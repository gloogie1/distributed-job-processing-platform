package com.virinchi.apiservice.repository;

import com.virinchi.apiservice.entity.ValidationError;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface ValidationErrorRepository extends JpaRepository<ValidationError, UUID> {

    List<ValidationError> findByJobIdOrderByRowNumberAsc(UUID jobId);
}