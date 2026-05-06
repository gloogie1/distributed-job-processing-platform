package com.virinchi.apiservice.dto;

import com.virinchi.apiservice.entity.JobStatus;

import java.time.Instant;
import java.util.UUID;

public record JobResponse(
        UUID id,
        String filePath,
        JobStatus status,
        int totalChunks,
        int completedChunks,
        int failedChunks,
        long totalRows,
        long validRows,
        long invalidRows,
        Instant createdAt,
        Instant completedAt
) {
}