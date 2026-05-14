package com.virinchi.apiservice.repository;

import com.virinchi.apiservice.entity.Job;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {

    List<Job> findTop25ByOrderByCreatedAtDesc();
}