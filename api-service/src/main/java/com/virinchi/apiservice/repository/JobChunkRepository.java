package com.virinchi.apiservice.repository;

import com.virinchi.apiservice.entity.JobChunk;
import com.virinchi.apiservice.entity.ChunkStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface JobChunkRepository extends JpaRepository<JobChunk, UUID> {

    List<JobChunk> findByJobIdOrderByStartRowAsc(UUID jobId);

    long countByJobIdAndStatus(UUID jobId, ChunkStatus status);
}