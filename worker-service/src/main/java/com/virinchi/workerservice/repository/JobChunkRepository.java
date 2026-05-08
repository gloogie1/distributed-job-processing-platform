package com.virinchi.workerservice.repository;

import com.virinchi.workerservice.entity.ChunkStatus;
import com.virinchi.workerservice.entity.JobChunk;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface JobChunkRepository extends JpaRepository<JobChunk, UUID> {

    List<JobChunk> findByJobId(UUID jobId);

    long countByJobIdAndStatus(UUID jobId, ChunkStatus status);
}