package com.virinchi.workerservice.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "validation_errors")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ValidationError {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID jobId;

    @Column(nullable = false)
    private UUID chunkId;

    @Column(nullable = false)
    private long rowNumber;

    @Column(nullable = false)
    private String fieldName;

    private String invalidValue;

    @Column(nullable = false)
    private String errorCode;

    @Column(nullable = false)
    private String errorMessage;

    private Instant createdAt;
}