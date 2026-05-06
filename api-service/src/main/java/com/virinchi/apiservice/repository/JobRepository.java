package com.virinchi.apiservice.repository;

import com.virinchi.apiservice.entity.Job;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {
}