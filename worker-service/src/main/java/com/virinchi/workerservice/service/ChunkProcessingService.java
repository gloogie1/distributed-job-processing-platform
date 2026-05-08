package com.virinchi.workerservice.service;

import com.virinchi.workerservice.entity.ChunkStatus;
import com.virinchi.workerservice.entity.Job;
import com.virinchi.workerservice.entity.JobChunk;
import com.virinchi.workerservice.entity.JobStatus;
import com.virinchi.workerservice.messaging.ChunkMessage;
import com.virinchi.workerservice.repository.JobChunkRepository;
import com.virinchi.workerservice.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ChunkProcessingService {

    private final JobRepository jobRepository;
    private final JobChunkRepository jobChunkRepository;

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

        chunk.setStatus(ChunkStatus.PROCESSING);
        chunk.setStartedAt(Instant.now());
        jobChunkRepository.save(chunk);

        try {
            long processedRows = countRowsInRange(
                    Path.of(message.filePath()),
                    message.startRow(),
                    message.endRow()
            );

            chunk.setValidRows(processedRows);
            chunk.setInvalidRows(0);
            chunk.setStatus(ChunkStatus.COMPLETED);
            chunk.setCompletedAt(Instant.now());
            chunk.setLastError(null);
            jobChunkRepository.save(chunk);

            updateParentJob(message.jobId());

        } catch (Exception e) {
            chunk.setStatus(ChunkStatus.FAILED_RETRYABLE);
            chunk.setRetryCount(chunk.getRetryCount() + 1);
            chunk.setLastError(e.getMessage());
            jobChunkRepository.save(chunk);

            throw new RuntimeException("Failed to process chunk " + message.chunkId(), e);
        }
    }

    private long countRowsInRange(Path filePath, long startRow, long endRow) throws IOException {
        long processedRows = 0;
        long currentDataRow = 0;

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

                if (!line.isBlank()) {
                    processedRows++;
                }
            }
        }

        return processedRows;
    }

    private void updateParentJob(UUID jobId) {
        Job job = jobRepository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));

        long completedChunks = jobChunkRepository.countByJobIdAndStatus(jobId, ChunkStatus.COMPLETED);
        long permanentlyFailedChunks = jobChunkRepository.countByJobIdAndStatus(jobId, ChunkStatus.FAILED_PERMANENT);

        job.setCompletedChunks((int) completedChunks);
        job.setFailedChunks((int) permanentlyFailedChunks);

        if (completedChunks + permanentlyFailedChunks == job.getTotalChunks()) {
            long validRows = jobChunkRepository.findByJobId(jobId)
                    .stream()
                    .mapToLong(JobChunk::getValidRows)
                    .sum();

            long invalidRows = jobChunkRepository.findByJobId(jobId)
                    .stream()
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
}