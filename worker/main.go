package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	pb "projects/kaze/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Worker struct {
	id         string
	client     pb.KazeServiceClient
	docker     *client.Client
	logStream  pb.KazeService_StreamLogsClient
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
		log.Printf("Warning: Failed to initialize Docker client: %v", err)
	}

	// 3. Initialize Log Stream
	logStream, err := grpcClient.StreamLogs(context.Background())
	if err != nil {
		log.Printf("Warning: Failed to initialize log stream: %v", err)
	}

	w := &Worker{
		id:        workerID,
		client:    grpcClient,
		docker:    dockerCli,
		logStream: logStream,
	}

	// 4. Register with Master
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

	// 5. Graceful Shutdown Handler
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Worker shutting down...")
		// Here we could notify master or wait for jobs
		os.Exit(0)
	}()

	// 6. Start Heartbeat Loop
	go w.heartbeatLoop()

	// 7. Start Job Polling Loop
	log.Printf("Worker %s is ready and waiting for jobs...", workerID)
	w.pollJobs()
}

func (w *Worker) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		_, err := w.client.Heartbeat(context.Background(), &pb.HeartbeatRequest{
			WorkerId: w.id,
			CpuUsage: 0.1,
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

	if job.Image != "" && w.docker != nil {
		result, err = w.executeDocker(job.JobId, job.Image, job.Command)
	} else {
		result, err = w.executeShell(job.JobId, job.Command)
	}

	status := "COMPLETED"
	if err != nil {
		status = "FAILED"
		result = err.Error()
	}

	w.client.UpdateJobStatus(context.Background(), &pb.UpdateJobStatusRequest{
		JobId:  job.JobId,
		Status: status,
		Result: result,
	})
}

func (w *Worker) sendLogFrame(jobID, data, streamType string) {
	if w.logStream == nil {
		return
	}
	w.logStream.Send(&pb.LogFrame{
		JobId:      jobID,
		WorkerId:   w.id,
		Data:       data,
		StreamType: streamType,
	})
}

func (w *Worker) executeShell(jobID, cmdStr string) (string, error) {
	cmd := exec.Command("sh", "-c", cmdStr)
	
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	
	if err := cmd.Start(); err != nil {
		return "", err
	}

	var fullOutput []byte
	outputMu := sync.Mutex{}

	streamToLogs := func(r io.Reader, streamType string) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			text := scanner.Text()
			w.sendLogFrame(jobID, text+"\n", streamType)
			outputMu.Lock()
			fullOutput = append(fullOutput, (text + "\n")...)
			outputMu.Unlock()
		}
	}

	go streamToLogs(stdout, "stdout")
	go streamToLogs(stderr, "stderr")

	err := cmd.Wait()
	return string(fullOutput), err
}

func (w *Worker) executeDocker(jobID, imageName, cmdStr string) (string, error) {
	ctx := context.Background()
	
	reader, err := w.docker.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return "", err
	}
	defer reader.Close()
	io.Copy(io.Discard, reader)

	createResp, err := w.docker.ContainerCreate(ctx, &container.Config{
		Image: imageName,
		Cmd:   []string{"sh", "-c", cmdStr},
	}, nil, nil, nil, "")
	if err != nil {
		return "", err
	}

	if err := w.docker.ContainerStart(ctx, createResp.ID, container.StartOptions{}); err != nil {
		return "", err
	}

	// Stream logs in real-time
	go func() {
		out, err := w.docker.ContainerLogs(ctx, createResp.ID, container.LogsOptions{
			ShowStdout: true, 
			ShowStderr: true, 
			Follow:     true,
		})
		if err != nil {
			return
		}
		defer out.Close()
		
		// Docker logs have a header we should ideally strip, but for simplicity:
		scanner := bufio.NewScanner(out)
		for scanner.Scan() {
			w.sendLogFrame(jobID, scanner.Text()+"\n", "stdout")
		}
	}()

	resultCh, errCh := w.docker.ContainerWait(ctx, createResp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return "", err
		}
	case <-resultCh:
	}

	// Final result capture
	out, err := w.docker.ContainerLogs(ctx, createResp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return "", err
	}
	defer out.Close()
	logBytes, _ := io.ReadAll(out)
	
	w.docker.ContainerRemove(ctx, createResp.ID, container.RemoveOptions{})
	return string(logBytes), nil
}
