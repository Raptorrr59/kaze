package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	pb "projects/kaze/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Worker struct {
	id       string
	client   pb.KazeServiceClient
	docker   *client.Client
}

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
	grpcClient := pb.NewKazeServiceClient(conn)

	// 2. Initialize Docker Client
	dockerCli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Printf("Warning: Failed to initialize Docker client: %v. Docker tasks will fail.", err)
	}

	w := &Worker{
		id:     workerID,
		client: grpcClient,
		docker: dockerCli,
	}

	// 3. Register with Master
	log.Printf("Worker %s registering...", workerID)
	resp, err := w.client.RegisterWorker(context.Background(), &pb.WorkerInfo{
		WorkerId: workerID,
		Hostname: hostname,
		CpuCount: 4,
		RamBytes: 8192,
	})
	if err != nil || !resp.Success {
		log.Fatalf("failed to register: %v", err)
	}
	log.Printf("Registered: %s", resp.Message)

	// 4. Start Heartbeat Loop
	go w.heartbeatLoop()

	// 5. Start Job Polling Loop
	log.Printf("Worker %s is ready and waiting for jobs...", workerID)
	w.pollJobs()
}

func (w *Worker) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		_, err := w.client.Heartbeat(context.Background(), &pb.HeartbeatRequest{
			WorkerId: w.id,
			CpuUsage: 0.1, // Mock stats
		})
		if err != nil {
			log.Printf("Heartbeat failed: %v", err)
		}
	}
}

func (w *Worker) pollJobs() {
	ticker := time.NewTicker(3 * time.Second)
	for range ticker.C {
		resp, err := w.client.ListJobs(context.Background(), &pb.ListJobsRequest{})
		if err != nil {
			log.Printf("Failed to poll jobs: %v", err)
			continue
		}

		for _, job := range resp.Jobs {
			if job.Status == "ASSIGNED" {
				fullJob, err := w.client.GetJobStatus(context.Background(), &pb.GetJobStatusRequest{JobId: job.JobId})
				if err == nil && fullJob.Status == "ASSIGNED" {
					go w.executeJob(fullJob)
				}
			}
		}
	}
}

func (w *Worker) executeJob(job *pb.JobStatusResponse) {
	log.Printf("Executing job %s: %s", job.JobId, job.Command)
	
	w.client.UpdateJobStatus(context.Background(), &pb.UpdateJobStatusRequest{
		JobId:  job.JobId,
		Status: "RUNNING",
	})

	var result string
	var err error
	imageName := job.Image

	if imageName != "" && w.docker != nil {
		result, err = w.executeDocker(job.JobId, imageName, job.Command)
	} else {
		result, err = w.executeShell(job.Command)
	}

	status := "COMPLETED"
	if err != nil {
		log.Printf("Job %s failed: %v", job.JobId, err)
		status = "FAILED"
		result = err.Error()
	}

	w.client.UpdateJobStatus(context.Background(), &pb.UpdateJobStatusRequest{
		JobId:  job.JobId,
		Status: status,
		Result: result,
	})
}

func (w *Worker) executeShell(cmdStr string) (string, error) {
	cmd := exec.Command("sh", "-c", cmdStr)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (w *Worker) executeDocker(jobID, imageName, cmdStr string) (string, error) {
	ctx := context.Background()
	
	// Pull image
	reader, err := w.docker.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return "", err
	}
	defer reader.Close()
	io.Copy(io.Discard, reader)

	// Create container - Classic SDK signature: (ctx, config, hostConfig, networkingConfig, platform, name)
	createResp, err := w.docker.ContainerCreate(ctx, &container.Config{
		Image: imageName,
		Cmd:   []string{"sh", "-c", cmdStr},
	}, nil, nil, nil, "")
	if err != nil {
		return "", err
	}

	// Start container
	if err := w.docker.ContainerStart(ctx, createResp.ID, container.StartOptions{}); err != nil {
		return "", err
	}

	// Wait for container
	resultCh, errCh := w.docker.ContainerWait(ctx, createResp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return "", err
		}
	case <-resultCh:
	}

	// Get logs
	out, err := w.docker.ContainerLogs(ctx, createResp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return "", err
	}
	defer out.Close()

	logBytes, _ := io.ReadAll(out)
	
	// Cleanup
	w.docker.ContainerRemove(ctx, createResp.ID, container.RemoveOptions{})

	return string(logBytes), nil
}
