package main

import (
	"context"
	"testing"

	pb "projects/kaze/proto"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestSchedulerBestFitAndTags(t *testing.T) {
	// 1. Setup in-memory SQLite database
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect sqlite database: %v", err)
	}

	// Auto-migrate tables
	if err := db.AutoMigrate(&Job{}, &Worker{}); err != nil {
		t.Fatalf("failed to auto-migrate: %v", err)
	}

	// 2. Initialize Master (without Redis and Cron for isolation in unit tests)
	master := &KazeMaster{
		db:      db,
		workers: make(map[string]*Worker),
	}

	// 3. Register Workers
	// Worker A: Small (2 CPU, 4GB RAM)
	_, err = master.RegisterWorker(context.Background(), &pb.WorkerInfo{
		WorkerId: "worker-A",
		Hostname: "node-a",
		CpuCount: 2,
		RamBytes: 4 * 1024 * 1024 * 1024,
		Tags:     map[string]string{},
	})
	if err != nil {
		t.Fatalf("failed to register worker A: %v", err)
	}

	// Worker B: Medium (8 CPU, 16GB RAM) + gpu tag
	_, err = master.RegisterWorker(context.Background(), &pb.WorkerInfo{
		WorkerId: "worker-B",
		Hostname: "node-b",
		CpuCount: 8,
		RamBytes: 16 * 1024 * 1024 * 1024,
		Tags:     map[string]string{"gpu": "true"},
	})
	if err != nil {
		t.Fatalf("failed to register worker B: %v", err)
	}

	// Worker C: Large (8 CPU, 32GB RAM)
	_, err = master.RegisterWorker(context.Background(), &pb.WorkerInfo{
		WorkerId: "worker-C",
		Hostname: "node-c",
		CpuCount: 8,
		RamBytes: 32 * 1024 * 1024 * 1024,
		Tags:     map[string]string{},
	})
	if err != nil {
		t.Fatalf("failed to register worker C: %v", err)
	}

	// 4. Test Scenario A: Submit job 1 (1 CPU, 1GB RAM)
	// Expect it to choose Worker A (Best-Fit: smallest capacity that still fits)
	j1Resp, err := master.SubmitJob(context.Background(), &pb.JobRequest{
		Command:       "sleep 1",
		RequiredCpu:   1.0,
		RequiredRamMb: 1024,
	})
	if err != nil {
		t.Fatalf("failed to submit job 1: %v", err)
	}

	master.dispatchJobsOnce()

	var j1 Job
	if err := db.First(&j1, "id = ?", j1Resp.JobId).Error; err != nil {
		t.Fatalf("failed to find job 1: %v", err)
	}
	if j1.Status != "ASSIGNED" || j1.WorkerID != "worker-A" {
		t.Errorf("Job 1: expected ASSIGNED to worker-A, got status=%s worker=%s", j1.Status, j1.WorkerID)
	}

	// 5. Test Scenario B: Submit job 2 (4 CPU, 4GB RAM)
	// Worker A (2 CPU total, 1 used) - doesn't fit
	// Worker B (8 CPU total, 0 used) - fits
	// Worker C (8 CPU total, 0 used) - fits, but larger capacity (32GB vs B's 16GB)
	// Expect it to choose Worker B (Best-Fit)
	j2Resp, err := master.SubmitJob(context.Background(), &pb.JobRequest{
		Command:       "sleep 2",
		RequiredCpu:   4.0,
		RequiredRamMb: 4096,
	})
	if err != nil {
		t.Fatalf("failed to submit job 2: %v", err)
	}

	master.dispatchJobsOnce()

	var j2 Job
	if err := db.First(&j2, "id = ?", j2Resp.JobId).Error; err != nil {
		t.Fatalf("failed to find job 2: %v", err)
	}
	if j2.Status != "ASSIGNED" || j2.WorkerID != "worker-B" {
		t.Errorf("Job 2: expected ASSIGNED to worker-B, got status=%s worker=%s", j2.Status, j2.WorkerID)
	}

	// 6. Test Scenario C: Submit job 3 (1 CPU, 1GB RAM, GPU=true)
	// Expect it to choose Worker B (since B is the only node with the tag "gpu=true")
	j3Resp, err := master.SubmitJob(context.Background(), &pb.JobRequest{
		Command:       "sleep 3",
		RequiredCpu:   1.0,
		RequiredRamMb: 1024,
		RequiredTags:  map[string]string{"gpu": "true"},
	})
	if err != nil {
		t.Fatalf("failed to submit job 3: %v", err)
	}

	master.dispatchJobsOnce()

	var j3 Job
	if err := db.First(&j3, "id = ?", j3Resp.JobId).Error; err != nil {
		t.Fatalf("failed to find job 3: %v", err)
	}
	if j3.Status != "ASSIGNED" || j3.WorkerID != "worker-B" {
		t.Errorf("Job 3: expected ASSIGNED to worker-B (gpu), got status=%s worker=%s", j3.Status, j3.WorkerID)
	}

	// 7. Test Scenario D: Submit job 4 that exceeds capacity (16 CPU)
	// Expect it to remain SUBMITTED or QUEUED (no assignment)
	j4Resp, err := master.SubmitJob(context.Background(), &pb.JobRequest{
		Command:       "sleep 4",
		RequiredCpu:   16.0,
		RequiredRamMb: 1024,
	})
	if err != nil {
		t.Fatalf("failed to submit job 4: %v", err)
	}

	master.dispatchJobsOnce()

	var j4 Job
	if err := db.First(&j4, "id = ?", j4Resp.JobId).Error; err != nil {
		t.Fatalf("failed to find job 4: %v", err)
	}
	if j4.Status != "SUBMITTED" || j4.WorkerID != "" {
		t.Errorf("Job 4: expected SUBMITTED/unassigned, got status=%s worker=%s", j4.Status, j4.WorkerID)
	}
}

func TestSchedulerAvailableCapacity(t *testing.T) {
	// Verify that dispatcher counts already running/assigned jobs as subtracting from capacity
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect sqlite database: %v", err)
	}
	db.AutoMigrate(&Job{}, &Worker{})

	master := &KazeMaster{
		db:      db,
		workers: make(map[string]*Worker),
	}

	// Register Worker A: Small (2 CPU, 4GB RAM)
	master.RegisterWorker(context.Background(), &pb.WorkerInfo{
		WorkerId: "worker-A",
		Hostname: "node-a",
		CpuCount: 2,
		RamBytes: 4 * 1024 * 1024 * 1024,
	})

	// Submit and assign Job 1 (1.5 CPU)
	j1Resp, _ := master.SubmitJob(context.Background(), &pb.JobRequest{
		Command:       "sleep 1",
		RequiredCpu:   1.5,
		RequiredRamMb: 1024,
	})
	master.dispatchJobsOnce()

	// Verify Job 1 assigned to Worker A
	var j1 Job
	db.First(&j1, "id = ?", j1Resp.JobId)
	if j1.WorkerID != "worker-A" {
		t.Fatalf("Job 1 not assigned to worker-A")
	}

	// Change job 1 status to RUNNING to verify it still counts
	db.Model(&j1).Update("status", "RUNNING")

	// Submit Job 2 (1.0 CPU). Remaining capacity on Worker A is 2.0 - 1.5 = 0.5 CPU.
	// Since 1.0 > 0.5, Job 2 should not fit on Worker A and remain unassigned.
	j2Resp, _ := master.SubmitJob(context.Background(), &pb.JobRequest{
		Command:       "sleep 2",
		RequiredCpu:   1.0,
		RequiredRamMb: 1024,
	})
	master.dispatchJobsOnce()

	var j2 Job
	db.First(&j2, "id = ?", j2Resp.JobId)
	if j2.Status != "SUBMITTED" || j2.WorkerID != "" {
		t.Errorf("Job 2 should not be assigned, got status=%s worker=%s", j2.Status, j2.WorkerID)
	}

	// Update Job 1 to COMPLETED to free up resources on Worker A
	db.Model(&j1).Update("status", "COMPLETED")

	// Dispatch again. Now Job 2 should be assigned to Worker A.
	master.dispatchJobsOnce()

	db.First(&j2, "id = ?", j2Resp.JobId)
	if j2.Status != "ASSIGNED" || j2.WorkerID != "worker-A" {
		t.Errorf("Job 2 should now be assigned to worker-A, got status=%s worker=%s", j2.Status, j2.WorkerID)
	}
}
