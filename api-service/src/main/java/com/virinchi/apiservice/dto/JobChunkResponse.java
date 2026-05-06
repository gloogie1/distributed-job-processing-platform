package com.virinchi.apiservice.dto;

import com.virinchi.apiservice.entity.ChunkStatus;

import java.time.Instant;
import java.util.UUID;

public record JobChunkResponse(
        UUID id,
        UUID jobId,
        String filePath,
        long startRow,
        long endRow,
        ChunkStatus status,
        int retryCount,
        long validRows,
        long invalidRows,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt,
        String lastError
) {
}