package com.virinchi.apiservice.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "jobs")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Job {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String filePath;

    @Column
    private String validFinalOutputPath;

    @Column
    private String invalidFinalOutputPath;

    @Column
    private String summaryOutputPath;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobStatus status;

    private int totalChunks;
    private int completedChunks;
    private int failedChunks;

    private long totalRows;
    private long validRows;
    private long invalidRows;

    private Instant createdAt;
    private Instant completedAt;
}