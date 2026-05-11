package com.virinchi.workerservice.service;

import com.virinchi.workerservice.entity.ChunkStatus;
import com.virinchi.workerservice.entity.Job;
import com.virinchi.workerservice.entity.JobChunk;
import com.virinchi.workerservice.entity.JobStatus;
import com.virinchi.workerservice.entity.ValidationError;
import com.virinchi.workerservice.messaging.ChunkMessage;
import com.virinchi.workerservice.messaging.DlqMessage;
import com.virinchi.workerservice.repository.JobChunkRepository;
import com.virinchi.workerservice.repository.JobRepository;
import com.virinchi.workerservice.repository.ValidationErrorRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Value;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;


import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class ChunkProcessingService {

    private final JobRepository jobRepository;
    private final JobChunkRepository jobChunkRepository;
    private final ValidationErrorRepository validationErrorRepository;
    private final KafkaTemplate<String, DlqMessage> dlqMessageKafkaTemplate;
    private final KafkaTemplate<String, ChunkMessage> chunkMessageKafkaTemplate;
    private final MeterRegistry meterRegistry;

    @Value("${worker.instance-id:${spring.application.name}-${server.port}}")
    private String workerId;
    
    private static final int MAX_RETRIES = 3;
    private static final String JOB_CHUNKS_TOPIC = "job-chunks";
    private static final String JOB_CHUNKS_DLQ_TOPIC = "job-chunks-dlq";

    private Counter chunksCompletedCounter;
    private Counter chunksFailedCounter;
    private Counter rowsValidatedCounter;
    private Counter validationErrorsCounter;
    private Timer chunkProcessingTimer;
    

    @KafkaListener(
            topics = "job-chunks",
            groupId = "job-worker-group",
            containerFactory = "chunkMessageKafkaListenerContainerFactory"
    )
    @Transactional
    public void processChunk(ChunkMessage message) {
        JobChunk chunk = jobChunkRepository.findById(message.chunkId())
                .orElseThrow(() -> new IllegalArgumentException("Chunk not found: " + message.chunkId()));

        // Basic idempotency guard.
        // If Kafka redelivers a message for an already completed chunk, skip it.
        if (chunk.getStatus() == ChunkStatus.COMPLETED) {
            return;
        }

        long startNanos = System.nanoTime();
        chunk.setStatus(ChunkStatus.PROCESSING);
        chunk.setStartedAt(Instant.now());
        chunk.setLastError(null);
        chunk.setWorkerId(workerId);
        jobChunkRepository.save(chunk);

        // Idempotency hardening:
        // If this chunk was partially processed before a crash/retry,
        // remove old row-level errors before recomputing them.
        validationErrorRepository.deleteByChunkId(chunk.getId());

        try {
            ChunkValidationResult result = validateRowsInRange(
                    Path.of(message.filePath()),
                    message.jobId(),
                    message.chunkId(),
                    message.startRow(),
                    message.endRow()
            );

            chunk.setValidRows(result.validRows());
            chunk.setInvalidRows(result.invalidRows());

            if (!result.errors().isEmpty()) {
                validationErrorRepository.saveAll(result.errors());
            }

            chunk.setStatus(ChunkStatus.COMPLETED);
            chunk.setCompletedAt(Instant.now());
            chunk.setLastError(null);
            jobChunkRepository.save(chunk);

            chunksCompletedCounter.increment();
            rowsValidatedCounter.increment(result.validRows() + result.invalidRows());
            validationErrorsCounter.increment(result.errors().size());
            chunkProcessingTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);

            updateParentJob(message.jobId());

        } catch (Exception e) {
            int nextRetryCount = chunk.getRetryCount() + 1;

            chunk.setRetryCount(nextRetryCount);
            chunk.setLastError(e.getMessage());

            if (nextRetryCount >= MAX_RETRIES) {
                chunk.setStatus(ChunkStatus.FAILED_PERMANENT);
                chunk.setCompletedAt(Instant.now());
                jobChunkRepository.save(chunk);

                DlqMessage dlqMessage = new DlqMessage(
                    message.jobId(),
                    message.chunkId(),
                    message.filePath(),
                    message.startRow(),
                    message.endRow(),
                    nextRetryCount,
                    e.getMessage(),
                    Instant.now()
                );

                dlqMessageKafkaTemplate.send(
                    JOB_CHUNKS_DLQ_TOPIC,
                    message.chunkId().toString(),
                    dlqMessage
                );
                
                chunksFailedCounter.increment();
                chunkProcessingTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);

                updateParentJob(message.jobId());
                return;
            }

            chunk.setStatus(ChunkStatus.FAILED_RETRYABLE);
            jobChunkRepository.save(chunk);

            chunkMessageKafkaTemplate.send(
                JOB_CHUNKS_TOPIC,
                message.chunkId().toString(),
                message
            );
            
            return;
        }
    }

    private ChunkValidationResult validateRowsInRange(
            Path filePath,
            UUID jobId,
            UUID chunkId,
            long startRow,
            long endRow
    ) throws IOException {
        long validRows = 0;
        long invalidRows = 0;
        long currentDataRow = 0;

        List<ValidationError> errors = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            // Skip header
            reader.readLine();

            String line;
            while ((line = reader.readLine()) != null) {
                currentDataRow++;

                if (currentDataRow < startRow) {
                    continue;
                }

                if (currentDataRow > endRow) {
                    break;
                }

                List<ValidationError> rowErrors = validateRow(line, jobId, chunkId, currentDataRow);

                if (rowErrors.isEmpty()) {
                    validRows++;
                } else {
                    invalidRows++;
                    errors.addAll(rowErrors);
                }
            }
        }

        return new ChunkValidationResult(validRows, invalidRows, errors);
    }

    private List<ValidationError> validateRow(String line, UUID jobId, UUID chunkId, long rowNumber) {
        List<ValidationError> errors = new ArrayList<>();
        String[] fields = line.split(",", -1);

        if (fields.length < 8) {
            errors.add(buildError(
                    jobId,
                    chunkId,
                    rowNumber,
                    "row",
                    line,
                    "MALFORMED_ROW",
                    "Expected 8 columns but found " + fields.length
            ));
            return errors;
        }

        String pickupDatetime = fields[1];
        String dropoffDatetime = fields[2];
        String passengerCount = fields[3];
        String tripDistance = fields[4];
        String fareAmount = fields[5];
        String totalAmount = fields[6];
        String paymentType = fields[7];

        validateDatetimeOrder(errors, jobId, chunkId, rowNumber, pickupDatetime, dropoffDatetime);
        validateIntegerRange(errors, jobId, chunkId, rowNumber, "passenger_count", passengerCount, 1, 6);
        validatePositiveDecimal(errors, jobId, chunkId, rowNumber, "trip_distance", tripDistance);
        validateNonNegativeDecimal(errors, jobId, chunkId, rowNumber, "fare_amount", fareAmount);
        validateTotalAmount(errors, jobId, chunkId, rowNumber, fareAmount, totalAmount);
        validateIntegerRange(errors, jobId, chunkId, rowNumber, "payment_type", paymentType, 1, 6);

        return errors;
    }

    private void validateDatetimeOrder(
            List<ValidationError> errors,
            UUID jobId,
            UUID chunkId,
            long rowNumber,
            String pickupDatetime,
            String dropoffDatetime
    ) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime pickup = LocalDateTime.parse(pickupDatetime, formatter);
            LocalDateTime dropoff = LocalDateTime.parse(dropoffDatetime, formatter);

            if (!pickup.isBefore(dropoff)) {
                errors.add(buildError(
                        jobId,
                        chunkId,
                        rowNumber,
                        "dropoff_datetime",
                        dropoffDatetime,
                        "INVALID_TIME_RANGE",
                        "Dropoff datetime must be after pickup datetime"
                ));
            }
        } catch (Exception e) {
            errors.add(buildError(
                    jobId,
                    chunkId,
                    rowNumber,
                    "pickup_datetime/dropoff_datetime",
                    pickupDatetime + " / " + dropoffDatetime,
                    "INVALID_DATETIME",
                    "Datetime fields must use yyyy-MM-dd HH:mm:ss format"
            ));
        }
    }

    private void validateIntegerRange(
            List<ValidationError> errors,
            UUID jobId,
            UUID chunkId,
            long rowNumber,
            String fieldName,
            String value,
            int min,
            int max
    ) {
        try {
            int parsed = Integer.parseInt(value);

            if (parsed < min || parsed > max) {
                errors.add(buildError(
                        jobId,
                        chunkId,
                        rowNumber,
                        fieldName,
                        value,
                        "OUT_OF_RANGE",
                        fieldName + " must be between " + min + " and " + max
                ));
            }
        } catch (Exception e) {
            errors.add(buildError(
                    jobId,
                    chunkId,
                    rowNumber,
                    fieldName,
                    value,
                    "INVALID_INTEGER",
                    fieldName + " must be an integer"
            ));
        }
    }

    private void validatePositiveDecimal(
            List<ValidationError> errors,
            UUID jobId,
            UUID chunkId,
            long rowNumber,
            String fieldName,
            String value
    ) {
        try {
            BigDecimal parsed = new BigDecimal(value);

            if (parsed.compareTo(BigDecimal.ZERO) <= 0) {
                errors.add(buildError(
                        jobId,
                        chunkId,
                        rowNumber,
                        fieldName,
                        value,
                        "NOT_POSITIVE",
                        fieldName + " must be greater than zero"
                ));
            }
        } catch (Exception e) {
            errors.add(buildError(
                    jobId,
                    chunkId,
                    rowNumber,
                    fieldName,
                    value,
                    "INVALID_DECIMAL",
                    fieldName + " must be a decimal number"
            ));
        }
    }

    private void validateNonNegativeDecimal(
            List<ValidationError> errors,
            UUID jobId,
            UUID chunkId,
            long rowNumber,
            String fieldName,
            String value
    ) {
        try {
            BigDecimal parsed = new BigDecimal(value);

            if (parsed.compareTo(BigDecimal.ZERO) < 0) {
                errors.add(buildError(
                        jobId,
                        chunkId,
                        rowNumber,
                        fieldName,
                        value,
                        "NEGATIVE_AMOUNT",
                        fieldName + " must be non-negative"
                ));
            }
        } catch (Exception e) {
            errors.add(buildError(
                    jobId,
                    chunkId,
                    rowNumber,
                    fieldName,
                    value,
                    "INVALID_DECIMAL",
                    fieldName + " must be a decimal number"
            ));
        }
    }

    private void validateTotalAmount(
            List<ValidationError> errors,
            UUID jobId,
            UUID chunkId,
            long rowNumber,
            String fareAmount,
            String totalAmount
    ) {
        try {
            BigDecimal fare = new BigDecimal(fareAmount);
            BigDecimal total = new BigDecimal(totalAmount);

            if (total.compareTo(fare) < 0) {
                errors.add(buildError(
                        jobId,
                        chunkId,
                        rowNumber,
                        "total_amount",
                        totalAmount,
                        "TOTAL_LESS_THAN_FARE",
                        "total_amount must be greater than or equal to fare_amount"
                ));
            }
        } catch (Exception e) {
            errors.add(buildError(
                    jobId,
                    chunkId,
                    rowNumber,
                    "total_amount",
                    totalAmount,
                    "INVALID_DECIMAL",
                    "fare_amount and total_amount must be decimal numbers"
            ));
        }
    }

    private ValidationError buildError(
            UUID jobId,
            UUID chunkId,
            long rowNumber,
            String fieldName,
            String invalidValue,
            String errorCode,
            String errorMessage
    ) {
        return ValidationError.builder()
                .id(UUID.randomUUID())
                .jobId(jobId)
                .chunkId(chunkId)
                .rowNumber(rowNumber)
                .fieldName(fieldName)
                .invalidValue(invalidValue)
                .errorCode(errorCode)
                .errorMessage(errorMessage)
                .createdAt(Instant.now())
                .build();
    }

    private void updateParentJob(UUID jobId) {
        Job job = jobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));

        long completedChunks = jobChunkRepository.countByJobIdAndStatus(jobId, ChunkStatus.COMPLETED);
        long permanentlyFailedChunks = jobChunkRepository.countByJobIdAndStatus(jobId, ChunkStatus.FAILED_PERMANENT);

        job.setCompletedChunks((int) completedChunks);
        job.setFailedChunks((int) permanentlyFailedChunks);

        if (completedChunks + permanentlyFailedChunks == job.getTotalChunks()) {
            List<JobChunk> chunks = jobChunkRepository.findByJobId(jobId);

            long validRows = chunks.stream()
                    .mapToLong(JobChunk::getValidRows)
                    .sum();

            long invalidRows = chunks.stream()
                    .mapToLong(JobChunk::getInvalidRows)
                    .sum();

            job.setValidRows(validRows);
            job.setInvalidRows(invalidRows);
            job.setCompletedAt(Instant.now());

            if (permanentlyFailedChunks > 0) {
                job.setStatus(JobStatus.COMPLETED_WITH_ERRORS);
            } else {
                job.setStatus(JobStatus.COMPLETED);
            }
        }

        jobRepository.save(job);
    }

    @PostConstruct
    public void initMetrics() {
        this.chunksCompletedCounter = Counter.builder("worker.chunks.completed")
            .description("Number of chunks completed successfully")
            .register(meterRegistry);

        this.chunksFailedCounter = Counter.builder("worker.chunks.failed")
            .description("Number of chunks permanently failed")
            .register(meterRegistry);

        this.rowsValidatedCounter = Counter.builder("worker.rows.validated")
            .description("Number of rows validated by the worker")
            .register(meterRegistry);

        this.validationErrorsCounter = Counter.builder("worker.validation.errors")
            .description("Number of validation errors generated")
            .register(meterRegistry);

        this.chunkProcessingTimer = Timer.builder("worker.chunk.processing.duration")
            .description("Time taken to process a chunk")
            .register(meterRegistry);
}

    private record ChunkValidationResult(
            long validRows,
            long invalidRows,
            List<ValidationError> errors
    ) {
    }
}