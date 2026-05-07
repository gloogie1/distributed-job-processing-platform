package com.virinchi.apiservice.controller;

import com.virinchi.apiservice.dto.CreateJobRequest;
import com.virinchi.apiservice.dto.JobChunkResponse;
import com.virinchi.apiservice.dto.JobResponse;
import com.virinchi.apiservice.service.JobService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;

    @PostMapping
    public JobResponse createJob(@Valid @RequestBody CreateJobRequest request) {
        return jobService.createJob(request);
    }

    @GetMapping("/{jobId}")
    public JobResponse getJob(@PathVariable UUID jobId) {
        return jobService.getJob(jobId);
    }

    @GetMapping("/{jobId}/chunks")
    public List<JobChunkResponse> getJobChunks(@PathVariable UUID jobId) {
        return jobService.getJobChunks(jobId);
    }
}