---
name: kaze-distributed-patterns
description: Essential distributed systems patterns for Kaze. Use when implementing cluster security (mTLS), distributed locking (Redis), or retry and fault tolerance logic.
---

# Kaze Distributed Patterns (The Glue)

This skill ensures the Kaze cluster is secure, reliable, and consistent across multiple nodes.

## 1. Cluster Security (mTLS)
All internal gRPC communication (Master-to-Worker, Client-to-Master) MUST use **Mutual TLS (mTLS)**.
- **Master Node**: Validates that connecting workers have certificates signed by the cluster CA.
- **Worker Node**: Validates the Master's identity before starting the registration loop.

## 2. Distributed Locking & Idempotency
- **Redis Integration**: Use Redis for distributed locking (`SET NX EX`) to ensure a job is only assigned to one worker at a time, even with multiple Masters.
- **UUIDs**: Every job must have a unique, deterministic ID (e.g., `UUID v4`) for deduplication.

## 3. Reliability & Retry Policies
- **Retry Strategy**: Implement exponential backoff (e.g., 2, 4, 8, 16 seconds) for job failures.
- **Circuit Breaker**: Workers should temporarily stop accepting new jobs if local system load (CPU/Memory) exceeds a critical threshold (e.g., 90%).

## 4. High Availability (HA)
- **Master Redundancy**: Design for multiple active Masters that synchronize state through the same Job Store (PostgreSQL) and Distributed Lock (Redis).
- **Consensus**: Use Redis-based leadership election or simple "First-to-Store" logic for periodic scheduling tasks (e.g., Cron jobs).
