# Distributed Job Processing Platform

A distributed CSV processing platform built with Spring Boot, Apache Kafka, PostgreSQL, Docker, React, Prometheus, and Grafana.

The system accepts large CSV processing jobs, splits the input into physical chunk files, distributes chunk work across Kafka-backed worker services, validates rows, writes processed output artifacts, tracks job/chunk state in PostgreSQL, and exposes both a React control dashboard and Prometheus/Grafana metrics.

## Performance Snapshot

> **Core benchmark:** Processed **9,554,778 NYC Yellow Taxi rows in 46 seconds** using **3 Docker worker replicas**, **96 chunks**, and final valid/invalid output generation.
>
> **Observed throughput:** ~**207,713 rows/sec**
>
> **Reliability checks:** Worker-kill testing completed successfully through Kafka consumer-group rebalancing, and controlled failure testing produced `FAILED_PERMANENT` chunks plus DLQ messages in `job-chunks-dlq`.

## Project Highlights

- Distributed worker architecture using Kafka consumer groups
- PostgreSQL-backed job and chunk state tracking
- Transactional outbox for reliable DB-to-Kafka chunk dispatch
- Physical chunk-file processing to avoid repeated full-file scans
- Retry and DLQ handling for failed chunks
- Idempotent chunk processing for safe retries/redelivery
- Per-chunk valid/invalid output files
- Final merged valid/invalid output artifacts for successful jobs
- React dashboard for job monitoring and control
- Prometheus/Grafana monitoring for runtime and worker metrics
- Benchmarked on a 9.55M-row NYC Yellow Taxi dataset

## Architecture

```text
                 ┌──────────────────────┐
                 │   React Dashboard     │
                 │   localhost:5173      │
                 └───────────┬──────────┘
                             │
                             v
┌─────────────────────────────────────────────────────┐
│                 API Service                         │
│                                                     │
│  POST /jobs                                         │
│   → create job                                      │
│   → split CSV into physical chunk files             │
│   → save job_chunks                                 │
│   → save outbox_events                              │
└───────────────┬───────────────────────┬─────────────┘
                │                       │
                v                       v
        ┌──────────────┐        ┌─────────────────┐
        │ PostgreSQL   │        │ Outbox Publisher│
        │ jobs         │        │ scheduled task  │
        │ job_chunks   │        └───────┬─────────┘
        │ outbox_events│                │
        │ validation   │                v
        │ errors       │        ┌────────────────┐
        └──────────────┘        │ Kafka Topics    │
                                │ job-chunks      │
                                │ job-chunks-dlq  │
                                └───────┬────────┘
                                        │
                                        v
                          ┌──────────────────────────┐
                          │ Worker Services           │
                          │ 3 Docker replicas         │
                          │                           │
                          │ consume chunk messages    │
                          │ validate rows             │
                          │ write valid/invalid files │
                          │ update chunk/job state    │
                          └───────────┬──────────────┘
                                      │
                                      v
                           ┌──────────────────────┐
                           │ Output Artifacts      │
                           │ /data/output/{jobId} │
                           └──────────────────────┘
```

## Tech Stack

| Layer | Technology |
|---|---|
| API | Spring Boot |
| Worker services | Spring Boot |
| Messaging | Apache Kafka |
| Database | PostgreSQL |
| Persistence | Spring Data JPA / Hibernate |
| Observability | Spring Boot Actuator, Micrometer, Prometheus, Grafana |
| Dashboard | React + Vite |
| Containerization | Docker Compose |
| Data format | CSV input/output |
| Benchmark dataset | NYC Yellow Taxi trip data |

## Core Features

### Job Creation

The API accepts a file path and chunk size:

```json
{
  "filePath": "/data/yellow_tripdata_2024_q1.csv",
  "chunkSize": 50000
}
```

The API then:

1. Reads the input CSV.
2. Splits it into physical chunk files under `/data/chunks/{jobId}/`.
3. Creates a `jobs` record.
4. Creates `job_chunks` records.
5. Creates `outbox_events` records.
6. Returns a running job response.

Job initialization currently performs physical chunk creation inside the `POST /jobs` request. This keeps the implementation simple for local Docker testing, but asynchronous job initialization is listed as a future improvement.

### Physical Chunk Files

Earlier versions used row ranges where each worker reopened and scanned the original CSV to find its assigned rows.

That was inefficient for large files.

The current design creates physical chunk files:

```text
/data/chunks/{jobId}/chunk-000001.csv
/data/chunks/{jobId}/chunk-000002.csv
/data/chunks/{jobId}/chunk-000003.csv
```

Workers now read only their assigned chunk file.

This avoids repeated full-file scans and significantly improves throughput.

### Transactional Outbox

The API does not directly publish Kafka messages during job creation.

Instead, it saves chunk messages into an `outbox_events` table in the same database transaction as the job and chunk records.

```text
single DB transaction:
  save job
  save job_chunks
  save outbox_events
```

A scheduled publisher then:

```text
read PENDING outbox events
publish to Kafka
mark events SENT
```

This prevents the failure case where job/chunk rows are saved but Kafka publishing fails halfway.

This implementation is designed for the local single-API deployment used in this project. A production multi-instance outbox publisher would usually add row claiming or `SKIP LOCKED` semantics to avoid multiple publishers sending the same pending event concurrently.

### Worker Processing

Workers consume `job-chunks` messages from Kafka.

For each chunk, the worker:

1. Marks the chunk as `PROCESSING`.
2. Deletes prior validation errors for idempotency.
3. Validates each row.
4. Writes valid rows to a per-chunk valid output file.
5. Writes invalid rows to a per-chunk invalid output file.
6. Stores sampled validation errors in PostgreSQL.
7. Marks the chunk as `COMPLETED`.
8. Updates the parent job.

### Validation Rules

The current row validation checks:

- `passenger_count` must be an integer between 1 and 6
- `trip_distance` must be greater than 0
- `fare_amount` must be non-negative
- `total_amount` must be greater than or equal to `fare_amount`
- `payment_type` must be an integer between 1 and 6
- `dropoff_datetime` must be after `pickup_datetime`

The validator is schema-specific and assumes normalized CSV rows for the selected taxi fields. It is not intended to be a fully general CSV parser for arbitrary quoted fields, escaped delimiters, or embedded newlines.

Validation errors are capped per chunk to avoid excessive PostgreSQL write amplification on dirty datasets.

Aggregate valid/invalid row counts remain accurate.

### Output Artifacts

Workers write per-chunk outputs:

```text
/data/output/{jobId}/valid/chunk-000001-valid.csv
/data/output/{jobId}/invalid/chunk-000001-invalid.csv
```

After all chunks complete successfully, the system enters a finalization phase and creates final merged outputs:

```text
/data/output/{jobId}/final/valid_rows.csv
/data/output/{jobId}/final/invalid_rows.csv
/data/output/{jobId}/final/summary.json
```

If one or more chunks fail permanently, the job is marked `COMPLETED_WITH_ERRORS` and failed chunk details are retained through chunk status, retry count, and DLQ messages instead of producing final merged job-level output files.

The final summary contains job-level counts and metadata.

Example:

```json
{
  "jobId": "5a64d54a-e7a4-48fa-83e1-f7b052c367db",
  "filePath": "/data/yellow_tripdata_2024_q1.csv",
  "status": "COMPLETED",
  "totalChunks": 96,
  "completedChunks": 96,
  "failedChunks": 0,
  "totalRows": 9554778,
  "validRows": 8480408,
  "invalidRows": 1074370
}
```

### Retry and DLQ

Chunk processing supports retries.

If a chunk fails:

```text
FAILED_RETRYABLE
→ retry
→ retry
→ retry
→ FAILED_PERMANENT
→ job-chunks-dlq
```

A controlled failure test using a config-gated force-fail input verified:

- Failed chunks retried 3 times.
- Chunks were marked `FAILED_PERMANENT`.
- Parent job was marked `COMPLETED_WITH_ERRORS`.
- DLQ messages were published to `job-chunks-dlq`.

### Idempotency

Chunk processing is designed to be safe under retry/redelivery.

Before recomputing a chunk, the worker removes previous validation errors for that chunk and overwrites output files rather than appending to them.

This prevents duplicate validation errors or duplicated output rows on Kafka redelivery.

The messaging model is best described as at-least-once delivery with idempotent chunk processing.

### Parent Job Aggregation

Multiple workers can finish chunks at the same time.

The worker uses a pessimistic lock when updating the parent job row to avoid race conditions where concurrent workers overwrite each other’s aggregate job status.

This prevents jobs from getting stuck in `RUNNING` even after all chunks have completed.

## Dashboard

The React dashboard provides application-level visibility.

URL:

```text
http://localhost:5173
```

Features:

- Submit jobs
- View recent jobs
- View job status and progress
- View total, valid, and invalid rows
- View duration and rows/sec
- View chunk status
- View worker distribution
- View validation error samples
- View output artifact paths

The dashboard is intended as the application control/monitoring plane.

## Kafka UI

Kafka UI is available at:

```text
http://localhost:8085
```

Use it to inspect:

- Kafka topics
- `job-chunks`
- `job-chunks-dlq`
- Messages
- Partitions
- Consumer groups

## Prometheus and Grafana

Prometheus:

```text
http://localhost:9090
```

Grafana:

```text
http://localhost:3000
```

Default Grafana login:

```text
username: admin
password: admin
```

Prometheus scrapes:

- API service actuator metrics
- Worker service actuator metrics

Useful PromQL queries:

```promql
worker_chunks_completed_total
```

```promql
worker_chunks_failed_total
```

```promql
worker_rows_validated_total
```

```promql
worker_validation_errors_total
```

```promql
rate(worker_rows_validated_total[1m])
```

```promql
worker_chunk_processing_duration_seconds_sum / worker_chunk_processing_duration_seconds_count
```

```promql
jvm_memory_used_bytes
```

```promql
hikaricp_connections_active
```

## Running Locally

### Prerequisites

- Docker Desktop
- Java 21
- Maven
- Node.js / npm
- Python 3, pandas, pyarrow for preparing the taxi dataset

### Start Backend Stack

```powershell
cd C:\projects\distributed-job-processing-platform
docker compose up -d --build --scale worker-service=3
```

This starts:

- PostgreSQL
- Kafka
- Kafka UI
- API service
- 3 worker service replicas
- Prometheus
- Grafana

Check containers:

```powershell
docker ps
```

### Start React Dashboard

```powershell
cd C:\projects\distributed-job-processing-platform\frontend
npm.cmd run dev
```

Open:

```text
http://localhost:5173
```

### Stop Backend Stack

```powershell
docker compose down
```

To also clear PostgreSQL data:

```powershell
docker compose down -v
```

## API Examples

### Submit Small Sample Job

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/jobs" `
  -ContentType "application/json" `
  -Body '{"filePath":"/data/sample_trips.csv","chunkSize":4}'
```

Expected sample result:

```text
totalRows   : 7
validRows   : 2
invalidRows : 5
```

### Submit Large Taxi Job

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/jobs" `
  -ContentType "application/json" `
  -Body '{"filePath":"/data/yellow_tripdata_2024_q1.csv","chunkSize":50000}'
```

### Get Job

```powershell
Invoke-RestMethod -Method Get `
  -Uri "http://localhost:8080/jobs/{jobId}"
```

### Get Chunks

```powershell
Invoke-RestMethod -Method Get `
  -Uri "http://localhost:8080/jobs/{jobId}/chunks"
```

### Get Validation Errors

```powershell
Invoke-RestMethod -Method Get `
  -Uri "http://localhost:8080/jobs/{jobId}/errors"
```

## Data Preparation

Generated taxi data files are not committed to Git.

The repository keeps only the small sample CSV. Large benchmark datasets should be generated locally.

Ignored generated paths include:

```text
sample-data/chunks/
sample-data/output/
sample-data/*.parquet
sample-data/*.csv
```

except:

```text
sample-data/sample_trips.csv
```

A Python preparation script is used to download/convert NYC Yellow Taxi parquet data into CSV format compatible with the validator.

Example output:

```text
sample-data/yellow_tripdata_2024_q1.csv
```

## Benchmark Results

### Large Dataset Benchmark

Dataset:

```text
NYC Yellow Taxi multi-month dataset
Rows: 9,554,778
Workers: 3 Docker worker replicas
Chunks: 96
Chunk size: 50,000
```

Observed result:

| Metric | Value |
|---|---:|
| Total rows | 9,554,778 |
| Valid rows | 8,480,408 |
| Invalid rows | 1,074,370 |
| Invalid % | 11.24% |
| Chunks | 96 |
| Workers | 3 |
| Duration | 46s |
| Throughput | ~207,713 rows/sec |
| Failed chunks | 0 |

### Chunk Size Tuning

Using the January 2024 NYC Yellow Taxi dataset with 2,964,624 rows and 3 Docker worker replicas:

| Chunk size | Chunks | Duration | Approx throughput |
|---:|---:|---:|---:|
| 5,000 | 593 | ~39s | ~76k rows/sec |
| 10,000 | 297 | ~24–29s | ~102k–124k rows/sec |
| 20,000 | 149 | ~18s | ~165k rows/sec |
| 75,000–100,000 | 30–40 | ~15–16s | ~185k–198k rows/sec |
| 250,000 | 12 | ~14s | ~212k rows/sec |
| Full file as one chunk | 1 | ~22s | ~135k rows/sec |

Observation:

```text
Very small chunks increased Kafka/outbox/database/file orchestration overhead.
One giant chunk removed parallelism.
Moderately large chunks gave the best balance between parallelism and overhead.
```

### Worker Scaling

On the larger dataset:

| Setup | Duration |
|---|---:|
| 1 worker | ~65s |
| 3 workers, 50k chunk size | ~52s |
| 3 workers, 100k/250k chunk size | ~46s |
| 3 workers, 5k chunk size | ~111s |

Observation:

```text
Worker scaling improved performance, but chunk size had a major effect.
Small chunks created too much orchestration overhead.
Larger chunks improved throughput by reducing per-chunk overhead while preserving enough parallelism.
```

## Failure Testing

### Worker Kill Test

A worker container was manually killed during a large running job.

Observed behavior:

```text
Kafka rebalanced partitions.
Remaining workers continued processing.
The job completed successfully.
Failed chunks remained 0.
```

This demonstrates that worker instance loss does not necessarily fail the job when Kafka can redeliver/reassign work.

### Controlled Failure / DLQ Test

A controlled failure mode was used to force chunk processing exceptions.

Observed behavior:

```text
2 chunks failed
each chunk retried 3 times
both chunks became FAILED_PERMANENT
parent job became COMPLETED_WITH_ERRORS
2 messages appeared in job-chunks-dlq
```

This verifies retry and DLQ behavior.

## Design Evolution

### Version 1: Row Range Chunking

Initial design:

```text
message = file path + start row + end row
worker opens original CSV
worker scans until assigned row range
```

Problem:

```text
Workers repeatedly scanned the same large CSV.
Later chunks required scanning most of the file.
```

### Version 2: Physical Chunk Files

Improved design:

```text
API creates physical chunk files
message = chunk file path
worker reads only assigned chunk file
```

Impact:

```text
Removed repeated full-file scans.
Improved large-file processing performance.
```

### Version 3: Validation Error Cap

After physical chunking, the bottleneck shifted to PostgreSQL writes because dirty chunks could produce tens of thousands of validation error rows.

Improvement:

```text
Store only sampled validation errors per chunk.
Keep aggregate valid/invalid counts accurate.
```

Impact:

```text
Reduced PostgreSQL write amplification.
Improved worker throughput.
```

### Version 4: Transactional Outbox

Direct Kafka publishing from job creation could leave the system inconsistent if DB writes succeeded but Kafka publishing failed.

Improvement:

```text
Save outbox events in the same DB transaction as job/chunk records.
Publish asynchronously from outbox_events.
```

Impact:

```text
Improved reliability of job dispatch.
Database remains the source of truth for work that still needs to be published.
```

### Version 5: Final Output Artifacts

Earlier versions tracked metadata and validation results but did not produce final processed datasets.

Improvement:

```text
Workers write per-chunk valid/invalid outputs.
Finalizer merges chunk outputs into final valid_rows.csv and invalid_rows.csv.
```

Impact:

```text
The platform now produces complete processed output artifacts, not only metadata.
```

## Known Limitations

- Job initialization is synchronous and performs physical chunk creation inside `POST /jobs`.
- The validator is schema-specific and assumes normalized CSV rows.
- The transactional outbox publisher is designed for the local single-API deployment used in this project.
- File-system writes and database writes are not part of a single atomic transaction, so failed runs may leave stale local chunk/output files.
- Kafka partition count limits the number of actively consuming workers in a consumer group.

## Repository Notes

Generated datasets and output artifacts are ignored by Git.

Do not commit:

```text
sample-data/chunks/
sample-data/output/
large taxi CSV files
parquet input files
frontend/node_modules/
frontend/dist/
```

Commit:

```text
source code
configuration
sample_trips.csv
README
monitoring configuration
```

## Future Improvements

Possible next steps:

- Testcontainers integration tests for Kafka/PostgreSQL end-to-end flows
- Prometheus/Grafana dashboard JSON export
- Kafka consumer lag endpoint or dashboard panel
- Flyway database migrations
- Object storage support for inputs/outputs
- Final output compression
- Job cancellation API
- Re-run failed chunks API
- Authentication for dashboard/API
- Worker registry/heartbeat table
- Kubernetes deployment manifests
- Asynchronous job initialization so `POST /jobs` returns quickly while physical chunk creation runs in the background
- Outbox row claiming / `SKIP LOCKED` support for multiple API publisher instances
- Robust CSV parsing using a dedicated CSV parser for quoted fields and escaped delimiters

## Current Status

The platform currently supports:

```text
large CSV job submission
physical chunking
Kafka-based distributed processing
transactional outbox dispatch
multi-worker scaling
row validation
retry + DLQ
idempotent chunk processing
per-chunk output files
final merged output files
React dashboard
Prometheus/Grafana monitoring
large-dataset benchmark testing
manual worker failure testing
controlled DLQ testing
```
