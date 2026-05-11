package com.virinchi.apiservice.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "job_chunks")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobChunk {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID jobId;

    @Column(nullable = false)
    private String filePath;

    @Column(nullable = false)
    private long startRow;

    @Column(nullable = false)
    private long endRow;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ChunkStatus status;

    private String workerId;
    private int retryCount;

    private long validRows;
    private long invalidRows;

    private Instant createdAt;
    private Instant startedAt;
    private Instant completedAt;

    private String lastError;
}