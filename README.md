# Project 1: **"Kaze" (風) Distributed Task Orchestrator**
*A production-grade, high-concurrency backend system for scheduling and executing distributed jobs in Go.*

## Vision
Kaze is a distributed system designed to handle the orchestration of complex tasks across a cluster of heterogeneous worker nodes. It moves beyond simple task execution to provide a robust, self-healing environment capable of managing thousands of concurrent jobs with strict reliability and observability.

## High-Level Architecture
The system follows a Master-Worker architecture:
- **Central Master:** The "Brain" that manages worker registration, job persistence (PostgreSQL), and task dispatching.
- **Worker Nodes:** The "Muscle" that executes tasks using Shell or Docker drivers and reports resource usage.
- **Communication:** Powered by gRPC with bi-directional streaming for heartbeats and real-time logs.
- **Reliability:** Built-in fault tolerance, retry policies, and distributed locking via Redis.

---
For detailed technical specifications, roadmap, and learning objectives, see [PROJECT.md](./PROJECT.md).
