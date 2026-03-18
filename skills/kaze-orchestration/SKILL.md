---
name: kaze-orchestration
description: Core orchestration logic for the Kaze Master node. Use when implementing gRPC service definitions, scheduling algorithms, or job lifecycle management in the Go backend.
---

# Kaze Orchestration (The Brain)

This skill defines the operational standards for the Kaze Master node, responsible for cluster coordination and job scheduling.

## 1. gRPC Service Contract
All communication between clients, masters, and workers must strictly follow the `kaze.proto` definitions.
- **Worker Management**: `RegisterWorker`, `Heartbeat`.
- **Job Management**: `SubmitJob`, `GetJobStatus`, `ListJobs`.
- **Observability**: `StreamLogs`.

## 2. Job Lifecycle (PostgreSQL)
Jobs must transition through explicit states in the database:
- `SUBMITTED`: Job received by Master.
- `QUEUED`: Job assigned to a priority queue.
- `ASSIGNED`: Job assigned to a specific Worker.
- `RUNNING`: Worker has started execution.
- `COMPLETED`: Job finished successfully.
- `FAILED`: Job encountered an error.
- `RETRYING`: Job failed but is within the retry limit.

## 3. Scheduling & Dispatching
The Master must implement resource-aware dispatching:
- **Round-Robin**: Default for uniform clusters.
- **Least-Load**: Prefer workers with the lowest CPU/RAM usage.
- **Tag-Matching**: Ensure jobs with specific requirements (e.g., `os: linux`) only run on capable nodes.

## 4. Fault Detection
- **Dead-Man's Switch**: Mark workers as `OFFLINE` if no heartbeat is received within 30 seconds.
- **Auto-Reassignment**: Jobs on offline workers must be automatically re-queued or failed based on their `RetryPolicy`.
