package com.virinchi.workerservice.messaging;

import java.util.UUID;

public record ChunkMessage(
        UUID jobId,
        UUID chunkId,
        String filePath,
        String chunkFilePath,
        long startRow,
        long endRow
) {
}