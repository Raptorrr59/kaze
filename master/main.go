package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "projects/kaze/proto"
	"google.golang.org/grpc"
)

type Worker struct {
	pb.WorkerInfo
	LastHeartbeat time.Time
	Status        string
}

type KazeMaster struct {
	pb.UnimplementedKazeServiceServer
	mu       sync.RWMutex
	workers  map[string]*Worker
	jobQueue chan *pb.JobRequest
}

func NewKazeMaster() *KazeMaster {
	m := &KazeMaster{
		workers:  make(map[string]*Worker),
		jobQueue: make(chan *pb.JobRequest, 100),
	}
	go m.workerHealthCheck()
	return m
}

func (m *KazeMaster) RegisterWorker(ctx context.Context, info *pb.WorkerInfo) (*pb.RegisterResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("Registering worker: %s (%s)", info.WorkerId, info.Hostname)
	m.workers[info.WorkerId] = &Worker{
		WorkerInfo:    *info,
		LastHeartbeat: time.Now(),
		Status:        "ONLINE",
	}

	return &pb.RegisterResponse{Success: true, Message: "Registered successfully"}, nil
}

func (m *KazeMaster) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if w, ok := m.workers[req.WorkerId]; ok {
		w.LastHeartbeat = time.Now()
		w.Status = "ONLINE"
		return &pb.HeartbeatResponse{Ok: true}, nil
	}

	return &pb.HeartbeatResponse{Ok: false}, nil
}

func (m *KazeMaster) SubmitJob(ctx context.Context, req *pb.JobRequest) (*pb.JobResponse, error) {
	log.Printf("Received job: %s", req.Command)
	
	// For Phase 1, we just queue it. 
	// Real dispatching logic would happen in a separate goroutine.
	m.jobQueue <- req

	return &pb.JobResponse{
		JobId:  fmt.Sprintf("job-%d", time.Now().UnixNano()),
		Status: "QUEUED",
	}, nil
}

func (m *KazeMaster) workerHealthCheck() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		m.mu.Lock()
		for id, w := range m.workers {
			if time.Since(w.LastHeartbeat) > 30*time.Second {
				log.Printf("Worker %s timed out, marking OFFLINE", id)
				w.Status = "OFFLINE"
			}
		}
		m.mu.Unlock()
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterKazeServiceServer(s, NewKazeMaster())

	log.Printf("Kaze Master listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
