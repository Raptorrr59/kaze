---
name: kaze-worker-engine
description: Execution logic for Kaze Worker nodes. Use when implementing job executors, Docker integration, resource monitoring, or real-time log streaming from worker nodes.
---

# Kaze Worker Engine (The Muscle)

This skill governs how worker nodes execute tasks and report back to the Master.

## 1. Worker Lifecycle
- **Registration**: On startup, workers must register with the Master, reporting their `CPU`, `RAM`, `OS`, and `Tags`.
- **Heartbeat**: Send heartbeats every 5-10 seconds via gRPC. Include current CPU/Memory load.
- **Graceful Shutdown**: On `SIGTERM`, notify the Master to stop sending new jobs and (optionally) wait for current jobs to complete or checkpoint them.

## 2. Execution Drivers
Workers support multiple drivers for isolation and flexibility:
- **Shell Driver**: Executes simple commands using `os/exec`. Captures `stdout` and `stderr`.
- **Docker Driver**: Pulls images and runs tasks within containers using the Docker Engine API. Mandatory for isolated or environment-specific jobs.

## 3. Log Streaming & Observability
- **Streaming**: Pipe `stdout`/`stderr` in real-time back to the Master via a bi-directional gRPC stream.
- **Metrics**: Monitor and report per-job resource usage (CPU seconds, memory footprint).

## 4. Concurrency Control
- **Worker Pools**: Use a fixed-size worker pool (configurable) to prevent local resource exhaustion.
- **Cancellation**: Use `context.Context` to propagate job cancellations from the Master to the underlying execution process.
