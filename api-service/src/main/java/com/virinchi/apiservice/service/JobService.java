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
import com.virinchi.apiservice.dto.ValidationErrorResponse;
import com.virinchi.apiservice.entity.ValidationError;
import com.virinchi.apiservice.repository.ValidationErrorRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.file.StandardOpenOption;
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
        Path sourceFilePath = Path.of(request.filePath());

        if (!Files.exists(sourceFilePath)) {
            throw new IllegalArgumentException("File does not exist: " + request.filePath());
        }

        UUID jobId = UUID.randomUUID();
        Instant now = Instant.now();

        ChunkCreationResult chunkCreationResult = createPhysicalChunkFiles(
            sourceFilePath,
            jobId,
            request.chunkSize()
        );

        Job job = Job.builder()
            .id(jobId)
            .filePath(request.filePath())
            .status(JobStatus.PENDING)
            .totalChunks(chunkCreationResult.chunks().size())
            .completedChunks(0)
            .failedChunks(0)
            .totalRows(chunkCreationResult.totalRows())
            .validRows(0)
            .invalidRows(0)
            .createdAt(now)
            .build();

        jobRepository.save(job);

        List<JobChunk> chunks = new ArrayList<>();

        for (ChunkFileInfo chunkFileInfo : chunkCreationResult.chunks()) {
            UUID chunkId = UUID.randomUUID();

            JobChunk chunk = JobChunk.builder()
                .id(chunkId)
                .jobId(jobId)
                .filePath(request.filePath())
                .chunkFilePath(chunkFileInfo.chunkFilePath())
                .startRow(chunkFileInfo.startRow())
                .endRow(chunkFileInfo.endRow())
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
                chunk.getChunkFilePath(),
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
        return jobChunkRepository.findByJobIdOrderByStartRowAsc(jobId)
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

    @Transactional(readOnly = true)
    public List<JobResponse> getRecentJobs() {
        return jobRepository.findTop25ByOrderByCreatedAtDesc()
            .stream()
            .map(this::toJobResponse)
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
                chunk.getChunkFilePath(),
                chunk.getStartRow(),
                chunk.getEndRow(),
                chunk.getStatus(),
                chunk.getRetryCount(),
                chunk.getWorkerId(),
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
    private ChunkCreationResult createPhysicalChunkFiles(
        Path sourceFilePath,
        UUID jobId,
        long chunkSize
    ) {
        List<ChunkFileInfo> chunkFiles = new ArrayList<>();

        Path chunksDir = sourceFilePath.getParent()
            .resolve("chunks")
            .resolve(jobId.toString());

        try {
            Files.createDirectories(chunksDir);

            try (BufferedReader reader = Files.newBufferedReader(sourceFilePath)) {
                String header = reader.readLine();

                if (header == null) {
                    return new ChunkCreationResult(0, chunkFiles);
                }

                long totalRows = 0;
                long currentChunkRowCount = 0;
                long chunkStartRow = 1;
                int chunkNumber = 1;

                BufferedWriter writer = null;
                Path currentChunkPath = null;

                try {
                    String line;

                    while ((line = reader.readLine()) != null) {
                        totalRows++;

                        if (writer == null || currentChunkRowCount >= chunkSize) {
                            if (writer != null) {
                                writer.close();

                                chunkFiles.add(new ChunkFileInfo(
                                    toContainerPath(currentChunkPath),
                                    chunkStartRow,
                                    totalRows - 1
                                ));
                            }

                            currentChunkPath = chunksDir.resolve(
                                String.format("chunk-%06d.csv", chunkNumber)
                            );

                            writer = Files.newBufferedWriter(
                                currentChunkPath,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.TRUNCATE_EXISTING
                            );

                            writer.write(header);
                            writer.newLine();

                            chunkStartRow = totalRows;
                            currentChunkRowCount = 0;
                            chunkNumber++;
                        }

                        writer.write(line);
                        writer.newLine();
                        currentChunkRowCount++;
                    }

                    if (writer != null) {
                        writer.close();

                        chunkFiles.add(new ChunkFileInfo(
                            toContainerPath(currentChunkPath),
                            chunkStartRow,
                            totalRows
                        ));
                    }

                    return new ChunkCreationResult(totalRows, chunkFiles);

                } finally {
                    if (writer != null) {
                        writer.close();
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create chunk files for: " + sourceFilePath, e);
        }
    }

    private String toContainerPath(Path path) {
        String normalized = path.toString().replace("\\", "/");

        int dataIndex = normalized.indexOf("/data/");
        if (dataIndex >= 0) {
            return normalized.substring(dataIndex);
        }

        return normalized;
    }

    private record ChunkCreationResult(
        long totalRows,
        List<ChunkFileInfo> chunks
    ) {
    }

    private record ChunkFileInfo(
        String chunkFilePath,
        long startRow,
        long endRow
    ) {
    }

    private record RowRange(long startRow, long endRow) {
    }
}