package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"strings"

	pb "projects/kaze/proto"
)

// --- METRICS ---

var (
	jobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kaze_jobs_total",
			Help: "Total number of jobs processed.",
		},
		[]string{"status"},
	)
	workersOnline = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kaze_workers_online_count",
			Help: "Current number of online workers.",
		},
	)
	queueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kaze_queue_depth",
			Help: "Current number of jobs in SUBMITTED or QUEUED status.",
		},
	)
)

func init() {
	prometheus.MustRegister(jobsTotal)
	prometheus.MustRegister(workersOnline)
	prometheus.MustRegister(queueDepth)
}

// --- MODELS ---

type Job struct {
	ID         string `gorm:"primaryKey"`
	Command    string
	Image      string
	Priority   int32
	Status     string
	Result     string
	RetryLimit int32
	RetryCount int32
	CronSpec   string
	WorkerID   string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type Worker struct {
	ID            string `gorm:"primaryKey"`
	Hostname      string
	Status        string
	CpuUsage      float32
	RamUsageBytes int64
	LastHeartbeat time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// --- MASTER SERVER ---

type KazeMaster struct {
	pb.UnimplementedKazeServiceServer
	db         *gorm.DB
	redis      *redis.Client
	cron       *cron.Cron
	mu         sync.RWMutex
	workers    map[string]*Worker
	
	// Log broadcasting: job_id -> map[subscriber_id]chan *pb.LogFrame
	logSubscribers sync.Map 
}

func NewKazeMaster(db *gorm.DB, rdb *redis.Client) *KazeMaster {
	m := &KazeMaster{
		db:      db,
		redis:   rdb,
		cron:    cron.New(),
		workers: make(map[string]*Worker),
	}
	
	var dbWorkers []Worker
	db.Find(&dbWorkers)
	for _, w := range dbWorkers {
		m.workers[w.ID] = &w
	}

	m.cron.Start()
	go m.workerHealthCheck()
	go m.jobDispatcher()
	go m.metricsUpdater()
	return m
}

func (m *KazeMaster) RegisterWorker(ctx context.Context, info *pb.WorkerInfo) (*pb.RegisterResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("Registering worker: %s (%s)", info.WorkerId, info.Hostname)
	
	worker := &Worker{
		ID:            info.WorkerId,
		Hostname:      info.Hostname,
		Status:        "ONLINE",
		LastHeartbeat: time.Now(),
	}

	if err := m.db.Save(worker).Error; err != nil {
		return nil, err
	}
	m.workers[worker.ID] = worker

	return &pb.RegisterResponse{Success: true, Message: "Registered successfully"}, nil
}

func (m *KazeMaster) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if w, ok := m.workers[req.WorkerId]; ok {
		w.LastHeartbeat = time.Now()
		w.Status = "ONLINE"
		w.CpuUsage = req.CpuUsage
		w.RamUsageBytes = req.RamUsageBytes
		
		m.db.Model(w).Updates(map[string]interface{}{
			"last_heartbeat":  w.LastHeartbeat,
			"status":          w.Status,
			"cpu_usage":       w.CpuUsage,
			"ram_usage_bytes": w.RamUsageBytes,
		})
		
		return &pb.HeartbeatResponse{Ok: true}, nil
	}

	return &pb.HeartbeatResponse{Ok: false}, nil
}

func (m *KazeMaster) ListWorkers(ctx context.Context, req *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	resp := &pb.ListWorkersResponse{}
	for _, w := range m.workers {
		resp.Workers = append(resp.Workers, &pb.WorkerStatus{
			WorkerId:          w.ID,
			Hostname:          w.Hostname,
			Status:            w.Status,
			CpuUsage:          w.CpuUsage,
			RamUsageBytes:     w.RamUsageBytes,
			LastHeartbeatUnix: w.LastHeartbeat.Unix(),
		})
	}

	return resp, nil
}

func (m *KazeMaster) SubmitJob(ctx context.Context, req *pb.JobRequest) (*pb.JobResponse, error) {
	jobID := uuid.New().String()
	log.Printf("Received job: %s (ID: %s)", req.Command, jobID)
	
	job := &Job{
		ID:         jobID,
		Command:    req.Command,
		Image:      req.Image,
		Priority:   req.Priority,
		Status:     "SUBMITTED",
		CronSpec:   req.CronSpec,
		RetryLimit: req.RetryLimit,
	}

	if err := m.db.Create(job).Error; err != nil {
		return nil, err
	}

	if req.CronSpec != "" {
		_, err := m.cron.AddFunc(req.CronSpec, func() {
			m.handleCronTrigger(job)
		})
		if err != nil {
			log.Printf("Failed to schedule cron job: %v", err)
			return nil, err
		}
	}

	return &pb.JobResponse{
		JobId:  jobID,
		Status: job.Status,
	}, nil
}

func (m *KazeMaster) GetJobStatus(ctx context.Context, req *pb.GetJobStatusRequest) (*pb.JobStatusResponse, error) {
	var job Job
	if err := m.db.First(&job, "id = ?", req.JobId).Error; err != nil {
		return nil, err
	}

	return &pb.JobStatusResponse{
		JobId:      job.ID,
		Command:    job.Command,
		Status:     job.Status,
		Result:     job.Result,
		RetryCount: job.RetryCount,
		CronSpec:   job.CronSpec,
		Image:      job.Image,
	}, nil
}

func (m *KazeMaster) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	var jobs []Job
	m.db.Find(&jobs)

	resp := &pb.ListJobsResponse{}
	for _, job := range jobs {
		resp.Jobs = append(resp.Jobs, &pb.JobStatusResponse{
			JobId:      job.ID,
			Command:    job.Command,
			Status:     job.Status,
			Result:     job.Result,
			RetryCount: job.RetryCount,
			CronSpec:   job.CronSpec,
			Image:      job.Image,
		})
	}
	return resp, nil
}

func (m *KazeMaster) UpdateJobStatus(ctx context.Context, req *pb.UpdateJobStatusRequest) (*pb.UpdateJobStatusResponse, error) {
	sanitizedResult := strings.ReplaceAll(req.Result, "\x00", "")

	err := m.db.Model(&Job{}).Where("id = ?", req.JobId).Updates(map[string]interface{}{
		"status": req.Status,
		"result": sanitizedResult,
	}).Error

	if err == nil {
		if req.Status == "COMPLETED" || req.Status == "FAILED" {
			jobsTotal.WithLabelValues(req.Status).Inc()
		}
	}

	if err != nil {
		return &pb.UpdateJobStatusResponse{Success: false}, err
	}
	return &pb.UpdateJobStatusResponse{Success: true}, nil
}

func (m *KazeMaster) StreamLogs(stream pb.KazeService_StreamLogsServer) error {
	for {
		frame, err := stream.Recv()
		if err != nil {
			return err
		}

		// Broadcast to all watchers of this job_id
		if subs, ok := m.logSubscribers.Load(frame.JobId); ok {
			subMap := subs.(*sync.Map)
			subMap.Range(func(key, value interface{}) bool {
				ch := value.(chan *pb.LogFrame)
				select {
				case ch <- frame:
				default:
					// If channel is full, drop frame or handle backpressure
				}
				return true
			})
		}
	}
}

func (m *KazeMaster) WatchLogs(req *pb.WatchLogsRequest, stream pb.KazeService_WatchLogsServer) error {
	subID := uuid.New().String()
	logChan := make(chan *pb.LogFrame, 100)

	actualSubs, _ := m.logSubscribers.LoadOrStore(req.JobId, &sync.Map{})
	subMap := actualSubs.(*sync.Map)
	subMap.Store(subID, logChan)
	
	defer func() {
		subMap.Delete(subID)
		// Cleanup map if empty?
	}()

	for {
		select {
		case frame := <-logChan:
			if err := stream.Send(frame); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// --- BACKGROUND LOGIC ---

func (m *KazeMaster) handleCronTrigger(jobTemplate *Job) {
	timestamp := time.Now().Truncate(time.Minute).Unix()
	lockKey := fmt.Sprintf("lock:cron:%s:%d", jobTemplate.ID, timestamp)
	
	ctx := context.Background()
	ok, err := m.redis.SetNX(ctx, lockKey, "locked", 2*time.Minute).Result()
	if err != nil || !ok {
		return
	}

	jobID := uuid.New().String()
	newJob := &Job{
		ID:         jobID,
		Command:    jobTemplate.Command,
		Image:      jobTemplate.Image,
		Priority:   jobTemplate.Priority,
		Status:     "SUBMITTED",
		RetryLimit: jobTemplate.RetryLimit,
	}
	m.db.Create(newJob)
	log.Printf("Cron triggered! New job ID: %s", jobID)
}

func (m *KazeMaster) workerHealthCheck() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		m.mu.Lock()
		for id, w := range m.workers {
			if time.Since(w.LastHeartbeat) > 30*time.Second {
				if w.Status != "OFFLINE" {
					log.Printf("Worker %s timed out, marking OFFLINE", id)
					w.Status = "OFFLINE"
					m.db.Model(w).Update("status", "OFFLINE")
					
					m.db.Model(&Job{}).Where("worker_id = ? AND status = ?", id, "RUNNING").
						Updates(map[string]interface{}{"status": "QUEUED", "worker_id": ""})
				}
			}
		}
		m.mu.Unlock()
	}
}

func (m *KazeMaster) jobDispatcher() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		var pendingJobs []Job
		m.db.Where("status IN ?", []string{"SUBMITTED", "QUEUED"}).Order("priority desc, created_at asc").Find(&pendingJobs)

		for _, job := range pendingJobs {
			m.mu.RLock()
			var targetWorker *Worker
			for _, w := range m.workers {
				if w.Status == "ONLINE" {
					targetWorker = w
					break 
				}
			}
			m.mu.RUnlock()

			if targetWorker != nil {
				log.Printf("Assigning job %s to worker %s", job.ID, targetWorker.ID)
				m.db.Model(&job).Updates(map[string]interface{}{
					"status":    "ASSIGNED",
					"worker_id": targetWorker.ID,
				})
			}
		}
	}
}

func (m *KazeMaster) metricsUpdater() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		m.mu.RLock()
		online := 0
		for _, w := range m.workers {
			if w.Status == "ONLINE" {
				online++
			}
		}
		m.mu.RUnlock()
		workersOnline.Set(float64(online))

		var count int64
		m.db.Model(&Job{}).Where("status IN ?", []string{"SUBMITTED", "QUEUED"}).Count(&count)
		queueDepth.Set(float64(count))
	}
}

func main() {
	// 1. Infrastructure (DB, Redis)
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "host=localhost user=kaze_user password=kaze_password dbname=kaze_db port=5432 sslmode=disable"
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}
	db.AutoMigrate(&Job{}, &Worker{})

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	// 2. Metrics Server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Metrics available at :9090/metrics")
		if err := http.ListenAndServe(":9090", nil); err != nil {
			log.Printf("Metrics server failed: %v", err)
		}
	}()

	// 3. gRPC Server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	master := NewKazeMaster(db, rdb)
	pb.RegisterKazeServiceServer(s, master)

	// 4. Graceful Shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down Kaze Master...")
		
		master.cron.Stop()
		s.GracefulStop()
		os.Exit(0)
	}()

	log.Printf("Kaze Master listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
