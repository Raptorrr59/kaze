package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	pb "projects/kaze/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Worker struct {
	id         string
	client     pb.KazeServiceClient
	docker     *client.Client
	logStream  pb.KazeService_StreamLogsClient
	activeJobs sync.Map // Map[string]context.CancelFunc
	mu         sync.Mutex
	isDraining bool
}

func getSystemCapacity() (int32, int64, error) {
	cpuCores, err := cpu.Counts(true)
	if err != nil || cpuCores <= 0 {
		cpuCores = 4 // Fallback
	}

	vm, err := mem.VirtualMemory()
	var ramBytes int64
	if err != nil || vm.Total <= 0 {
		ramBytes = 8 * 1024 * 1024 * 1024 // Fallback 8GB
	} else {
		ramBytes = int64(vm.Total)
	}

	return int32(cpuCores), ramBytes, nil
}

func parseWorkerTags() map[string]string {
	tagsStr := os.Getenv("KAZE_WORKER_TAGS")
	tags := make(map[string]string)
	if tagsStr == "" {
		return tags
	}
	parts := strings.Split(tagsStr, ",")
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			tags[kv[0]] = kv[1]
		}
	}
	return tags
}

func getWorkerTLSCredentials() (credentials.TransportCredentials, error) {
	caCertFile := os.Getenv("KAZE_CA_CERT")
	if caCertFile == "" {
		caCertFile = "certs/ca.pem"
	}
	certFile := os.Getenv("KAZE_WORKER_CERT")
	if certFile == "" {
		certFile = "certs/worker.pem"
	}
	keyFile := os.Getenv("KAZE_WORKER_KEY")
	if keyFile == "" {
		keyFile = "certs/worker-key.pem"
	}

	clientCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load worker key pair: %v", err)
	}

	caCert, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %v", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA certificate to pool")
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      certPool,
		ServerName:   "localhost",
	}

	return credentials.NewTLS(config), nil
}

func main() {
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-1"
	}

	hostname, _ := os.Hostname()

	// 1. Connect to Master
	tlsCreds, err := getWorkerTLSCredentials()
	if err != nil {
		log.Fatalf("failed to load TLS credentials: %v", err)
	}

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(tlsCreds))
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
	
	cpuCores, ramBytes, _ := getSystemCapacity()
	tags := parseWorkerTags()

	resp, err := w.client.RegisterWorker(context.Background(), &pb.WorkerInfo{
		WorkerId: workerID,
		Hostname: hostname,
		CpuCount: cpuCores,
		RamBytes: ramBytes,
		Tags:     tags,
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
		log.Println("Worker initiating graceful shutdown...")

		w.mu.Lock()
		w.isDraining = true
		w.mu.Unlock()

		// Call DeregisterWorker RPC to Master
		deregCtx, deregCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer deregCancel()
		_, err := w.client.DeregisterWorker(deregCtx, &pb.DeregisterRequest{WorkerId: w.id})
		if err != nil {
			log.Printf("Warning: Failed to deregister from Master: %v", err)
		} else {
			log.Println("Deregistered from Master successfully.")
		}

		// Wait for currently executing tasks to complete
		log.Println("Waiting for running tasks to complete...")
		waitChan := make(chan struct{})
		go func() {
			for {
				count := 0
				w.activeJobs.Range(func(key, value interface{}) bool {
					count++
					return true
				})
				if count == 0 {
					close(waitChan)
					return
				}
				time.Sleep(500 * time.Millisecond)
			}
		}()

		select {
		case <-waitChan:
			log.Println("All tasks completed. Graceful shutdown finished.")
		case <-time.After(30 * time.Second):
			log.Println("Graceful shutdown timeout exceeded. Force-cancelling remaining tasks...")
			w.activeJobs.Range(func(key, value interface{}) bool {
				cancelFunc := value.(context.CancelFunc)
				cancelFunc()
				log.Printf("Force-cancelled task: %v", key)
				return true
			})
			time.Sleep(1 * time.Second)
		}

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
	// Initialize CPU metric check
	cpu.Percent(0, false)
	
	for range ticker.C {
		cpuPercents, err := cpu.Percent(0, false)
		var cpuUsage float32 = 0.1
		if err == nil && len(cpuPercents) > 0 {
			cpuUsage = float32(cpuPercents[0]) / 100.0
		}

		vm, err := mem.VirtualMemory()
		var ramUsageBytes int64 = 0
		if err == nil {
			ramUsageBytes = int64(vm.Used)
		}

		_, err = w.client.Heartbeat(context.Background(), &pb.HeartbeatRequest{
			WorkerId:      w.id,
			CpuUsage:      cpuUsage,
			RamUsageBytes: ramUsageBytes,
		})
		if err != nil {
			log.Printf("Heartbeat failed: %v", err)
		}
	}
}

func (w *Worker) pollJobs() {
	ticker := time.NewTicker(3 * time.Second)
	for range ticker.C {
		w.mu.Lock()
		draining := w.isDraining
		w.mu.Unlock()
		if draining {
			continue
		}

		resp, err := w.client.ListJobs(context.Background(), &pb.ListJobsRequest{})
		if err != nil {
			continue
		}

		for _, job := range resp.Jobs {
			if job.Status == "ASSIGNED" && job.WorkerId == w.id {
				w.mu.Lock()
				if w.isDraining {
					w.mu.Unlock()
					continue
				}
				w.mu.Unlock()

				fullJob, err := w.client.GetJobStatus(context.Background(), &pb.GetJobStatusRequest{JobId: job.JobId})
				if err == nil && fullJob.Status == "ASSIGNED" {
					go w.executeJob(fullJob)
				}
			}
		}
	}
}

func (w *Worker) executeJob(job *pb.JobStatusResponse) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w.activeJobs.Store(job.JobId, cancel)
	defer w.activeJobs.Delete(job.JobId)

	log.Printf("Executing job %s: %s", job.JobId, job.Command)
	
	_, err := w.client.UpdateJobStatus(ctx, &pb.UpdateJobStatusRequest{
		JobId:    job.JobId,
		Status:   "RUNNING",
		WorkerId: w.id,
	})
	if err != nil {
		log.Printf("Failed to update status to RUNNING for job %s: %v", job.JobId, err)
	}

	var result string

	if job.Image != "" && w.docker != nil {
		result, err = w.executeDocker(ctx, job.JobId, job.Image, job.Command)
	} else {
		result, err = w.executeShell(ctx, job.JobId, job.Command)
	}

	status := "COMPLETED"
	if err != nil {
		status = "FAILED"
		result = err.Error()
	}

	w.client.UpdateJobStatus(context.Background(), &pb.UpdateJobStatusRequest{
		JobId:    job.JobId,
		Status:   status,
		Result:   result,
		WorkerId: w.id,
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

func (w *Worker) executeShell(ctx context.Context, jobID, cmdStr string) (string, error) {
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdStr)
	
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

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		streamToLogs(stdout, "stdout")
		wg.Done()
	}()
	go func() {
		streamToLogs(stderr, "stderr")
		wg.Done()
	}()

	err := cmd.Wait()
	wg.Wait()
	return string(fullOutput), err
}

func (w *Worker) executeDocker(ctx context.Context, jobID, imageName, cmdStr string) (string, error) {
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

	defer func() {
		removeCtx := context.Background()
		w.docker.ContainerStop(removeCtx, createResp.ID, container.StopOptions{})
		w.docker.ContainerRemove(removeCtx, createResp.ID, container.RemoveOptions{Force: true})
	}()

	if err := w.docker.ContainerStart(ctx, createResp.ID, container.StartOptions{}); err != nil {
		return "", err
	}

	// Stream logs in real-time
	go func() {
		out, err := w.docker.ContainerLogs(context.Background(), createResp.ID, container.LogsOptions{
			ShowStdout: true, 
			ShowStderr: true, 
			Follow:     true,
		})
		if err != nil {
			return
		}
		defer out.Close()
		
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
	out, err := w.docker.ContainerLogs(context.Background(), createResp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return "", err
	}
	defer out.Close()
	logBytes, _ := io.ReadAll(out)
	
	return string(logBytes), nil
}
