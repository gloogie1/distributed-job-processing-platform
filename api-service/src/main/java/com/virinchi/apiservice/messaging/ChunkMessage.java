package com.virinchi.apiservice.messaging;

import java.util.UUID;

public record ChunkMessage(
        UUID jobId,
        UUID chunkId,
        String filePath,
        long startRow,
        long endRow
) {
}