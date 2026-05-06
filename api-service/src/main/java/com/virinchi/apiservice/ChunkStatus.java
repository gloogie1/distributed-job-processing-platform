package com.virinchi.apiservice.entity;

public enum ChunkStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED_RETRYABLE,
    FAILED_PERMANENT
}