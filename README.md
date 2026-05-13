# Distributed Job Processing Platform

A Java Spring Boot distributed job-processing platform that splits CSV files into chunks, publishes chunk-processing tasks to Kafka, processes them with horizontally scalable worker instances, and stores job status, chunk status, validation results, retry state, and worker metrics in PostgreSQL.

This project is designed as a backend/distributed-systems portfolio project, not a notebook or data-analysis project.

## Tech Stack

- Java 21
- Spring Boot 3
- Spring Web
- Spring Data JPA
- Spring Kafka
- PostgreSQL
- Apache Kafka
- Docker Compose
- Maven
- Spring Boot Actuator
- Micrometer

## Architecture

```text
Client / curl / Postman
        |
        v
Spring Boot API Service
        |
        | creates job + chunk metadata
        v
PostgreSQL
        |
        | publishes chunk messages
        v
Kafka topic: job-chunks
        |
        | consumed by worker consumer group
        v
Spring Boot Worker Service(s)
        |
        | validates assigned CSV row ranges
        | updates chunk/job state
        | writes validation errors
        v
PostgreSQL
```

## Services

### API Service

The API service is responsible for:

- Accepting job submissions
- Splitting files into row-based chunks
- Creating job and chunk records in PostgreSQL
- Publishing chunk messages to Kafka
- Exposing job status, chunk status, and validation error endpoints

Runs on:

```text
http://localhost:8080
```

### Worker Service

The worker service is responsible for:

- Consuming chunk messages from Kafka
- Processing assigned row ranges
- Validating CSV rows
- Writing validation errors to PostgreSQL
- Updating chunk/job status
- Retrying failed chunks
- Publishing permanently failed chunks to a DLQ
- Exposing processing metrics through Actuator

Default port:

```text
http://localhost:8081
```

Multiple worker instances can be started on different ports while sharing the same Kafka consumer group.

## Infrastructure

Docker Compose starts:

- PostgreSQL
- Apache Kafka
- Kafka UI

Kafka UI:

```text
http://localhost:8085
```

PostgreSQL:

```text
host: localhost
port: 5432
database: jobsdb
username: jobuser
password: jobpass
```

## Kafka Topics

### `job-chunks`

Main topic for chunk-processing work.

The API publishes one message per chunk:

```json
{
  "jobId": "7756d8a2-04f4-4439-897d-ade1df0a38ce",
  "chunkId": "177a9242-01fb-45b4-a700-4c0769e18576",
  "filePath": "C:\\projects\\distributed-job-processing-platform\\sample-data\\sample_trips.csv",
  "startRow": 1,
  "endRow": 2
}
```

### `job-chunks-dlq`

Dead-letter topic for chunks that fail permanently after retry exhaustion.

DLQ messages include:

```json
{
  "jobId": "...",
  "chunkId": "...",
  "filePath": "...",
  "startRow": 1,
  "endRow": 2,
  "retryCount": 3,
  "errorMessage": "Failed to process chunk",
  "failedAt": "2026-05-09T21:33:42Z"
}
```

## Database Tables

### `jobs`

Stores parent job state:

- Job ID
- File path
- Status
- Total chunks
- Completed chunks
- Failed chunks
- Total rows
- Valid rows
- Invalid rows
- Created/completed timestamps

### `job_chunks`

Stores chunk-level state:

- Chunk ID
- Job ID
- File path
- Start row
- End row
- Status
- Retry count
- Worker ID
- Valid row count
- Invalid row count
- Error details
- Started/completed timestamps

### `validation_errors`

Stores row-level validation output:

- Job ID
- Chunk ID
- Row number
- Field name
- Invalid value
- Error code
- Error message
- Created timestamp

## Job Statuses

```text
PENDING
RUNNING
COMPLETED
COMPLETED_WITH_ERRORS
FAILED
```

## Chunk Statuses

```text
PENDING
PROCESSING
COMPLETED
FAILED_RETRYABLE
FAILED_PERMANENT
```

## Local Setup

### 1. Start infrastructure

```powershell
cd C:\projects\distributed-job-processing-platform
docker compose up -d
```

Check containers:

```powershell
docker ps
```

Expected containers:

```text
djp-postgres
djp-kafka
djp-kafka-ui
```

### 2. Start API service

```powershell
cd C:\projects\distributed-job-processing-platform\api-service
mvn spring-boot:run
```

Health check:

```text
http://localhost:8080/actuator/health
```

### 3. Start worker service

```powershell
cd C:\projects\distributed-job-processing-platform\worker-service
mvn spring-boot:run
```

Health check:

```text
http://localhost:8081/actuator/health
```

## Docker Compose Setup

The full system can be started with Docker Compose:
```powershell
docker compose up -d --build
```
This starts:

PostgreSQL
Kafka
Kafka UI
API service
Worker service

When using Docker Compose, sample files are mounted into the API and worker containers at:
```text
/data
```
So job requests should use container paths, for example:
```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/jobs" `
  -ContentType "application/json" `
  -Body '{"filePath":"/data/sample_trips.csv","chunkSize":1}'
```
Check job status:
```powershell
Invoke-RestMethod -Method Get `
  -Uri "http://localhost:8080/jobs/YOUR_JOB_ID"
```
Check chunks:
```powershell
Invoke-RestMethod -Method Get `
  -Uri "http://localhost:8080/jobs/YOUR_JOB_ID/chunks"
```

## API Endpoints

### Create job

```http
POST /jobs
```

Example:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/jobs" `
  -ContentType "application/json" `
  -Body '{"filePath":"C:\\projects\\distributed-job-processing-platform\\sample-data\\sample_trips.csv","chunkSize":2}'
```

Example response:

```text
id              : 7756d8a2-04f4-4439-897d-ade1df0a38ce
filePath        : C:\projects\distributed-job-processing-platform\sample-data\sample_trips.csv
status          : RUNNING
totalChunks     : 4
completedChunks : 0
failedChunks    : 0
totalRows       : 7
validRows       : 0
invalidRows     : 0
```

### Get job status

```http
GET /jobs/{jobId}
```

Example completed response:

```text
status          : COMPLETED
totalChunks     : 4
completedChunks : 4
failedChunks    : 0
totalRows       : 7
validRows       : 2
invalidRows     : 5
```

### Get job chunks

```http
GET /jobs/{jobId}/chunks
```

Example response:

```text
startRow    : 1
endRow      : 2
status      : COMPLETED
retryCount  : 0
workerId    : worker-8081
validRows   : 2
invalidRows : 0
```

### Get validation errors

```http
GET /jobs/{jobId}/errors
```

Example response:

```text
rowNumber    : 3
fieldName    : dropoff_datetime
invalidValue : 2024-01-01 11:50:00
errorCode    : INVALID_TIME_RANGE
errorMessage : Dropoff datetime must be after pickup datetime
```

## CSV Validation Rules

The worker validates each row using these rules:

- `pickup_datetime` must be before `dropoff_datetime`
- `passenger_count` must be between 1 and 6
- `trip_distance` must be greater than 0
- `fare_amount` must be non-negative
- `total_amount` must be greater than or equal to `fare_amount`
- `payment_type` must be between 1 and 6
- Rows must contain the expected number of columns

## Fault Tolerance

### Idempotent Chunk Processing

Kafka can redeliver messages. To avoid duplicate work:

- Completed chunks are skipped
- Permanently failed chunks are skipped
- Before reprocessing a chunk, existing validation errors for that chunk are deleted
- Validation output is recomputed for the chunk

This prevents duplicate validation error rows after retries or worker restarts.

### Explicit Retry Handling

The worker handles retries at the application level.

On failure:

```text
retryCount < 3
  -> mark chunk FAILED_RETRYABLE
  -> republish ChunkMessage to job-chunks
  -> return normally

retryCount >= 3
  -> mark chunk FAILED_PERMANENT
  -> publish DlqMessage to job-chunks-dlq
  -> update parent job
  -> return normally
```

This avoids relying on Spring Kafka's automatic retry loop and keeps retry state visible in PostgreSQL.

### Dead Letter Queue

Chunks that fail after the retry limit are published to:

```text
job-chunks-dlq
```

This allows failed work to be inspected without blocking the rest of the pipeline.

## Horizontal Worker Scaling

Workers use the same Kafka consumer group:

```text
job-worker-group
```

The `job-chunks` topic has 3 partitions, allowing up to 3 workers to process chunks in parallel.

Start multiple workers:

```powershell
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8081 --worker.instance-id=worker-8081"
```

```powershell
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8082 --worker.instance-id=worker-8082"
```

```powershell
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8083 --worker.instance-id=worker-8083"
```

Each processed chunk records the `workerId`, making worker distribution visible through:

```http
GET /jobs/{jobId}/chunks
```

## Observability

The worker exposes custom Micrometer metrics through Spring Boot Actuator.

Metrics endpoint:

```text
http://localhost:8081/actuator/metrics
```

Custom metrics:

```text
worker.chunks.completed
worker.chunks.failed
worker.rows.validated
worker.validation.errors
worker.chunk.processing.duration
```

Example:

```powershell
Invoke-RestMethod -Method Get `
  -Uri "http://localhost:8081/actuator/metrics/worker.chunks.completed"
```

Example output:

```text
worker.chunks.completed
COUNT = 7.0
```

## Example Metrics Output

After processing a 7-row sample file:

```text
worker.chunks.completed        : 7
worker.rows.validated          : 7
worker.validation.errors       : 5
worker.chunk.processing.duration COUNT: 7
```

## Current Limitations

- API and worker are run manually during development
- API and worker are not yet Dockerized
- No transactional outbox yet
- Kafka publishing happens directly after job/chunk creation
- No integration tests yet
- CSV parsing is simple and does not handle quoted commas
- Benchmarking section is still pending

## Planned Improvements

- Dockerize API and worker services
- Add benchmark results for 1 vs 2 vs 3 workers
- Add Transactional Outbox pattern for reliable Kafka publishing
- Add Testcontainers integration tests
- Add Prometheus/Grafana dashboard
- Replace simple CSV parsing with a robust CSV parser
- Add API pagination for validation errors
