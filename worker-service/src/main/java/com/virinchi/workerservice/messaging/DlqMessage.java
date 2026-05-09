package com.virinchi.workerservice.messaging;

import java.time.Instant;
import java.util.UUID;

public record DlqMessage(
        UUID jobId,
        UUID chunkId,
        String filePath,
        long startRow,
        long endRow,
        int retryCount,
        String errorMessage,
        Instant failedAt
) {
}