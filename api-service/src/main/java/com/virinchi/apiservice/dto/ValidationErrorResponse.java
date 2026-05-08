package com.virinchi.apiservice.dto;

import java.time.Instant;
import java.util.UUID;

public record ValidationErrorResponse(
        UUID id,
        UUID jobId,
        UUID chunkId,
        long rowNumber,
        String fieldName,
        String invalidValue,
        String errorCode,
        String errorMessage,
        Instant createdAt
) {
}