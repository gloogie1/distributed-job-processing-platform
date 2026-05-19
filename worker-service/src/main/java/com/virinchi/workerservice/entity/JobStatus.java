package com.virinchi.workerservice.entity;

public enum JobStatus {
    PENDING,
    RUNNING,
    FINALIZING,
    COMPLETED,
    COMPLETED_WITH_ERRORS,
    FAILED
}