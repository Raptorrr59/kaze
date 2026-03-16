package main

import (
	"context"
	"log"
	"os"
	"os/exec"
	"time"

	pb "projects/kaze/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-1"
	}

	hostname, _ := os.Hostname()

	// 1. Connect to Master
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewKazeServiceClient(conn)

	// 2. Register with Master
	log.Printf("Worker %s registering...", workerID)
	resp, err := client.RegisterWorker(context.Background(), &pb.WorkerInfo{
		WorkerId: workerID,
		Hostname: hostname,
		CpuCount: 4,
		RamBytes: 8192,
	})
	if err != nil || !resp.Success {
		log.Fatalf("failed to register: %v", err)
	}
	log.Printf("Registered: %s", resp.Message)

	// 3. Start Heartbeat Loop
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			_, err := client.Heartbeat(context.Background(), &pb.HeartbeatRequest{
				WorkerId: workerID,
				CpuUsage: 0.1, // Mock stats for now
			})
			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
			}
		}
	}()

	log.Printf("Worker %s is ready and waiting for jobs...", workerID)
	
	// Phase 1: Simple loop to keep process alive. 
	// Real job pulling would be a stream or a long-poll.
	for {
		time.Sleep(1 * time.Hour)
	}
}

// executeShellCommand would be used in Phase 2
func executeShellCommand(cmdStr string) (string, error) {
	cmd := exec.Command("sh", "-c", cmdStr)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
