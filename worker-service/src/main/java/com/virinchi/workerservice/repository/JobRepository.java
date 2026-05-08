package com.virinchi.workerservice.repository;

import com.virinchi.workerservice.entity.Job;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {
}