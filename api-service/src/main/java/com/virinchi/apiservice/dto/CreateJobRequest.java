package com.virinchi.apiservice.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CreateJobRequest(
        @NotBlank String filePath,

        @Min(1) long chunkSize
) {
}