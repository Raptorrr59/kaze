# Project 1: **"Kaze" (風) Distributed Task Orchestrator** - Project Details & Roadmap

## 1. Technical Specifications

### A. Core Architecture (The Orchestration Plane)
*   **Central Master (The Brain):**
    *   **Worker Registry:** Manages a dynamic pool of workers via gRPC heartbeats. Implements a "Dead-Man's Switch" logic to detect silent worker failures.
    *   **Job Store:** A transactional layer (PostgreSQL) that tracks every job's lifecycle from `SUBMITTED` to `ARCHIVED`.
    *   **Dispatcher:** Implements the scheduling algorithm (Round-Robin, Least-Load, or Resource-Aware) to assign tasks to the most suitable worker.
*   **Worker Nodes (The Muscle):**
    *   **Registration:** Workers self-announce their capabilities (CPU, RAM, OS, Tags) upon startup.
    *   **Execution Engine:** Supports multiple execution drivers:
        1.  **Shell Driver:** For simple scripts and binaries.
        2.  **Docker Driver:** For containerized isolation (pulling images, mounting volumes).
    *   **Resource Monitor:** Continuously reports local resource usage back to the master to prevent overloading.

### B. Scheduling Logic
*   **Task Types:**
    *   **One-off:** Immediate execution.
    *   **Scheduled:** Execute at a specific future timestamp (`ISO 8601`).
    *   **Cron-based:** Recurring execution using standard 5 or 6-field Cron expressions.
*   **Queue Management:** Priority-based queues. High-priority jobs can bypass the queue or (optionally) pre-empt running low-priority jobs.
*   **Concurrency Control:** Global and per-worker limits on concurrent tasks to ensure system stability.

### C. Communication & Protocol (gRPC)
*   **Protobuf Definitions:** Strict schema for all Master-Worker and Client-Master interactions.
*   **Bi-directional Streams:**
    *   **Heartbeat Stream:** Worker -> Master (health + stats).
    *   **Log Stream:** Worker -> Master (real-time `stdout`/`stderr` piping).
*   **Security:** Mandatory mTLS (Mutual TLS) for all cluster communication to prevent unauthorized worker joining or job sniffing.

### D. Fault Tolerance & Self-Healing
*   **Task Re-assignment:** If a worker goes offline, its `RUNNING` tasks are automatically marked for re-evaluation. Based on the job config, they are either re-queued on a different node or marked as `FAILED`.
*   **Retry Policy:** Configurable retry logic (e.g., "retry 3 times with exponential backoff").
*   **Idempotency:** Every job has a unique UUID. The system must ensure that a job is never executed more than once simultaneously (Distributed Locking via Redis).

---

## 2. Highly Detailed Objectives

### Phase 1: The Foundation (Week 1-2)
*   [x] **Protobuf Contract:** Define the `KazeService` including `RegisterWorker`, `SubmitJob`, `StreamLogs`, and `Heartbeat`.
*   [x] **Master Server:** Implement the gRPC server and a basic in-memory task queue.
*   [x] **Worker Client:** Implement the worker registration loop and a basic Shell Executor.
*   [x] **CLI (`kazectl`):** Create a basic CLI that can submit a "Hello World" task and list workers.

### Phase 2: Persistence & Reliability (Week 3-4)
*   [x] **PostgreSQL Integration:** Replace in-memory storage with a relational schema for persistence across restarts.
*   [x] **Cron Scheduler:** Implement a ticker-based scheduler that triggers jobs based on Cron expressions.
*   [x] **Fault Detection:** Implement the logic to mark workers as "Offline" after 3 missed heartbeats.
*   [x] **Docker Executor:** Implement the ability to pull a Docker image and run a task within a container using the Docker Engine API.

### Phase 3: Advanced Features & Observability (Week 5-6)
*   [x] **Real-time Log Streaming:** Pipe logs from the worker's container/shell through gRPC to the Master, allowing `kazectl logs -f <id>`.
*   [x] **Distributed Locking:** Integrate Redis to ensure only one Master can process the schedule (preparing for HA).
*   [x] **Metrics:** Export Prometheus metrics for queue depth, worker health, and task failure rates.
*   [x] **Clean Shutdown:** Ensure workers finish current tasks (or checkpoint them) before exiting on `SIGTERM`.

---

## 3. What You Will Learn (Deep Dive)
*   **Go Concurrency:** Mastering the `select` pattern, context cancellation, and worker pools.
*   **System Design:** Handling the "Fallacies of Distributed Computing" (network latency, partial failures).
*   **Production Go:** Using structured logging (zap/slog), environment-based config, and robust error handling.
*   **DevOps/Infra:** Interfacing with the Docker Socket and managing PostgreSQL/Redis at scale.
