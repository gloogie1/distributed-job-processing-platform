package com.virinchi.apiservice.service;

import com.virinchi.apiservice.dto.CreateJobRequest;
import com.virinchi.apiservice.dto.JobChunkResponse;
import com.virinchi.apiservice.dto.JobResponse;
import com.virinchi.apiservice.entity.ChunkStatus;
import com.virinchi.apiservice.entity.Job;
import com.virinchi.apiservice.entity.JobChunk;
import com.virinchi.apiservice.entity.JobStatus;
import com.virinchi.apiservice.messaging.ChunkMessage;
import com.virinchi.apiservice.repository.JobChunkRepository;
import com.virinchi.apiservice.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.virinchi.apiservice.dto.ValidationErrorResponse;
import com.virinchi.apiservice.entity.ValidationError;
import com.virinchi.apiservice.repository.ValidationErrorRepository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
public class JobService {

    private static final String JOB_CHUNKS_TOPIC = "job-chunks";

    private final JobRepository jobRepository;
    private final JobChunkRepository jobChunkRepository;
    private final ValidationErrorRepository validationErrorRepository;
    private final KafkaTemplate<String, ChunkMessage> kafkaTemplate;
    

    @Transactional
    public JobResponse createJob(CreateJobRequest request) {
        Path filePath = Path.of(request.filePath());

        if (!Files.exists(filePath)) {
            throw new IllegalArgumentException("File does not exist: " + request.filePath());
        }

        long totalRows = countDataRows(filePath);
        List<RowRange> ranges = splitIntoChunks(totalRows, request.chunkSize());

        UUID jobId = UUID.randomUUID();
        Instant now = Instant.now();

        Job job = Job.builder()
                .id(jobId)
                .filePath(request.filePath())
                .status(JobStatus.PENDING)
                .totalChunks(ranges.size())
                .completedChunks(0)
                .failedChunks(0)
                .totalRows(totalRows)
                .validRows(0)
                .invalidRows(0)
                .createdAt(now)
                .build();

        jobRepository.save(job);

        List<JobChunk> chunks = new ArrayList<>();

        for (RowRange range : ranges) {
            UUID chunkId = UUID.randomUUID();

            JobChunk chunk = JobChunk.builder()
                    .id(chunkId)
                    .jobId(jobId)
                    .filePath(request.filePath())
                    .startRow(range.startRow())
                    .endRow(range.endRow())
                    .status(ChunkStatus.PENDING)
                    .retryCount(0)
                    .validRows(0)
                    .invalidRows(0)
                    .createdAt(now)
                    .build();

            chunks.add(chunk);
        }

        jobChunkRepository.saveAll(chunks);

        for (JobChunk chunk : chunks) {
            ChunkMessage message = new ChunkMessage(
                    jobId,
                    chunk.getId(),
                    chunk.getFilePath(),
                    chunk.getStartRow(),
                    chunk.getEndRow()
            );

            kafkaTemplate.send(JOB_CHUNKS_TOPIC, chunk.getId().toString(), message);
        }

        job.setStatus(JobStatus.RUNNING);
        jobRepository.save(job);

        return toJobResponse(job);
    }

    public JobResponse getJob(UUID jobId) {
        Job job = jobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));

        return toJobResponse(job);
    }

    public List<JobChunkResponse> getJobChunks(UUID jobId) {
        return jobChunkRepository.findByJobId(jobId)
                .stream()
                .map(this::toJobChunkResponse)
                .toList();
    }

    public List<ValidationErrorResponse> getValidationErrors(UUID jobId) {
    return validationErrorRepository.findByJobIdOrderByRowNumberAsc(jobId)
            .stream()
            .map(this::toValidationErrorResponse)
            .toList();
    }

    private long countDataRows(Path filePath) {
        try (Stream<String> lines = Files.lines(filePath)) {
            long totalLines = lines.count();
            return Math.max(0, totalLines - 1); // subtract CSV header
        } catch (IOException e) {
            throw new RuntimeException("Failed to count rows in file: " + filePath, e);
        }
    }

    private List<RowRange> splitIntoChunks(long totalRows, long chunkSize) {
        List<RowRange> ranges = new ArrayList<>();

        long start = 1;

        while (start <= totalRows) {
            long end = Math.min(start + chunkSize - 1, totalRows);
            ranges.add(new RowRange(start, end));
            start = end + 1;
        }

        return ranges;
    }

    private JobResponse toJobResponse(Job job) {
        return new JobResponse(
                job.getId(),
                job.getFilePath(),
                job.getStatus(),
                job.getTotalChunks(),
                job.getCompletedChunks(),
                job.getFailedChunks(),
                job.getTotalRows(),
                job.getValidRows(),
                job.getInvalidRows(),
                job.getCreatedAt(),
                job.getCompletedAt()
        );
    }

    private JobChunkResponse toJobChunkResponse(JobChunk chunk) {
        return new JobChunkResponse(
                chunk.getId(),
                chunk.getJobId(),
                chunk.getFilePath(),
                chunk.getStartRow(),
                chunk.getEndRow(),
                chunk.getStatus(),
                chunk.getRetryCount(),
                chunk.getValidRows(),
                chunk.getInvalidRows(),
                chunk.getCreatedAt(),
                chunk.getStartedAt(),
                chunk.getCompletedAt(),
                chunk.getLastError()
        );
    }

    private ValidationErrorResponse toValidationErrorResponse(ValidationError error) {
    return new ValidationErrorResponse(
            error.getId(),
            error.getJobId(),
            error.getChunkId(),
            error.getRowNumber(),
            error.getFieldName(),
            error.getInvalidValue(),
            error.getErrorCode(),
            error.getErrorMessage(),
            error.getCreatedAt()
    );
}

    private record RowRange(long startRow, long endRow) {
    }
}