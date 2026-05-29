package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
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
	masterIsLeader = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kaze_master_is_leader",
			Help: "Indicates if this master instance is currently the cluster leader (1) or standby (0).",
		},
	)
)

func init() {
	prometheus.MustRegister(jobsTotal)
	prometheus.MustRegister(workersOnline)
	prometheus.MustRegister(queueDepth)
	prometheus.MustRegister(masterIsLeader)
}

// --- MODELS ---

type Job struct {
	ID            string `gorm:"primaryKey"`
	Command       string
	Image         string
	Priority      int32
	Status        string
	Result        string
	RetryLimit    int32
	RetryCount    int32
	CronSpec      string
	WorkerID      string
	RequiredCpu   float32
	RequiredRamMb int64
	RequiredTags  map[string]string `gorm:"serializer:json"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Worker struct {
	ID            string `gorm:"primaryKey"`
	Hostname      string
	Status        string
	CpuUsage      float32
	RamUsageBytes int64
	CpuCount      int32
	RamBytes      int64
	Tags          map[string]string `gorm:"serializer:json"`
	LastHeartbeat time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// --- MASTER SERVER ---

type cronEntryCache struct {
	entryID  cron.EntryID
	cronSpec string
	command  string
}

type KazeMaster struct {
	pb.UnimplementedKazeServiceServer
	db         *gorm.DB
	redis      *redis.Client
	id         string
	mu         sync.RWMutex
	workers    map[string]*Worker
	
	// Leadership variables
	isLeader     bool
	leaderCtx    context.Context
	leaderCancel context.CancelFunc
	leaderCron   *cron.Cron
	cronEntries  map[string]cronEntryCache // jobID -> cronEntryCache for tracking scheduled cron jobs
	
	// Log broadcasting: job_id -> map[subscriber_id]chan *pb.LogFrame
	logSubscribers sync.Map 
}

func NewKazeMaster(db *gorm.DB, rdb *redis.Client) *KazeMaster {
	m := &KazeMaster{
		db:          db,
		redis:       rdb,
		id:          "master-" + uuid.New().String(),
		workers:     make(map[string]*Worker),
		cronEntries: make(map[string]cronEntryCache),
	}
	
	var dbWorkers []Worker
	db.Find(&dbWorkers)
	for _, w := range dbWorkers {
		m.workers[w.ID] = &w
	}

	// Metrics update loop runs on all masters (online/standby)
	go m.metricsUpdater()

	// Start leadership election loop in the background
	go m.electionLoop()

	return m
}

func (m *KazeMaster) RegisterWorker(ctx context.Context, info *pb.WorkerInfo) (*pb.RegisterResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("Registering worker: %s (%s) with CPU: %d cores, RAM: %d MB, Tags: %v",
		info.WorkerId, info.Hostname, info.CpuCount, info.RamBytes/(1024*1024), info.Tags)
	
	worker := &Worker{
		ID:            info.WorkerId,
		Hostname:      info.Hostname,
		Status:        "ONLINE",
		LastHeartbeat: time.Now(),
		CpuCount:      info.CpuCount,
		RamBytes:      info.RamBytes,
		Tags:          info.Tags,
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

func (m *KazeMaster) DeregisterWorker(ctx context.Context, req *pb.DeregisterRequest) (*pb.DeregisterResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("Deregistering worker: %s", req.WorkerId)

	if w, ok := m.workers[req.WorkerId]; ok {
		w.Status = "OFFLINE"
		w.LastHeartbeat = time.Now()
		m.db.Model(w).Updates(map[string]interface{}{
			"status":         "OFFLINE",
			"last_heartbeat": w.LastHeartbeat,
		})

		// Reschedule running/assigned jobs
		err := m.db.Model(&Job{}).
			Where("worker_id = ? AND status IN ?", req.WorkerId, []string{"ASSIGNED", "RUNNING"}).
			Updates(map[string]interface{}{"status": "QUEUED", "worker_id": ""}).Error
		if err != nil {
			log.Printf("Failed to reschedule jobs for deregistered worker %s: %v", req.WorkerId, err)
		}

		return &pb.DeregisterResponse{Success: true}, nil
	}

	return &pb.DeregisterResponse{Success: false}, nil
}

// --- LEADER ELECTION & SYNC LOGIC ---

func (m *KazeMaster) electionLoop() {
	ticker := time.NewTicker(3 * time.Second)
	lockKey := "kaze:master:leader"
	ttl := 10 * time.Second

	for range ticker.C {
		ctx := context.Background()
		m.mu.RLock()
		leading := m.isLeader
		m.mu.RUnlock()

		if leading {
			// Lua script to renew the lock TTL atomically if we still own it
			renewScript := `
				if redis.call("get", KEYS[1]) == ARGV[1] then
					return redis.call("pexpire", KEYS[1], ARGV[2])
				else
					return 0
				end
			`
			res, err := m.redis.Eval(ctx, renewScript, []string{lockKey}, m.id, int(ttl.Milliseconds())).Int()
			if err != nil || res == 0 {
				log.Printf("Master %s failed to renew leadership lock. Demoting to standby...", m.id)
				m.stopLeading()
			}
		} else {
			// Try to acquire leadership lock
			ok, err := m.redis.SetNX(ctx, lockKey, m.id, ttl).Result()
			if err == nil && ok {
				m.startLeading()
			} else {
				// Ensure metric says we are standby
				masterIsLeader.Set(0)
			}
		}
	}
}

func (m *KazeMaster) startLeading() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isLeader {
		return
	}
	m.isLeader = true
	log.Printf("Master %s gained leadership! Starting leader loops...", m.id)
	masterIsLeader.Set(1)

	m.leaderCtx, m.leaderCancel = context.WithCancel(context.Background())
	m.leaderCron = cron.New()
	m.leaderCron.Start()

	m.cronEntries = make(map[string]cronEntryCache)

	go m.jobDispatcher(m.leaderCtx)
	go m.workerHealthCheck(m.leaderCtx)
	go m.cronSyncLoop(m.leaderCtx)
}

func (m *KazeMaster) stopLeading() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isLeader {
		return
	}
	m.isLeader = false
	log.Printf("Master %s lost leadership! Stopping leader loops...", m.id)
	masterIsLeader.Set(0)

	if m.leaderCron != nil {
		m.leaderCron.Stop()
		m.leaderCron = nil
	}

	if m.leaderCancel != nil {
		m.leaderCancel()
		m.leaderCancel = nil
	}
}

func (m *KazeMaster) cronSyncLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	// Run initial sync immediately
	m.syncCronJobs()

	for {
		select {
		case <-ticker.C:
			m.syncCronJobs()
		case <-ctx.Done():
			return
		}
	}
}

func (m *KazeMaster) syncCronJobs() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isLeader || m.leaderCron == nil {
		return
	}

	var dbCronJobs []Job
	if err := m.db.Where("cron_spec != ''").Find(&dbCronJobs).Error; err != nil {
		log.Printf("CronSync: failed to fetch cron jobs from database: %v", err)
		return
	}

	activeJobIDs := make(map[string]bool)

	for _, job := range dbCronJobs {
		activeJobIDs[job.ID] = true
		cache, exists := m.cronEntries[job.ID]

		if !exists {
			jobCopy := job
			entryID, err := m.leaderCron.AddFunc(job.CronSpec, func() {
				m.handleCronTrigger(&jobCopy)
			})
			if err != nil {
				log.Printf("CronSync: failed to schedule job %s with spec '%s': %v", job.ID, job.CronSpec, err)
				continue
			}
			m.cronEntries[job.ID] = cronEntryCache{
				entryID:  entryID,
				cronSpec: job.CronSpec,
				command:  job.Command,
			}
			log.Printf("CronSync: scheduled job %s ('%s') with spec '%s'", job.ID, job.Command, job.CronSpec)
		} else if cache.cronSpec != job.CronSpec || cache.command != job.Command {
			m.leaderCron.Remove(cache.entryID)
			
			jobCopy := job
			entryID, err := m.leaderCron.AddFunc(job.CronSpec, func() {
				m.handleCronTrigger(&jobCopy)
			})
			if err != nil {
				log.Printf("CronSync: failed to reschedule job %s: %v", job.ID, err)
				delete(m.cronEntries, job.ID)
				continue
			}
			m.cronEntries[job.ID] = cronEntryCache{
				entryID:  entryID,
				cronSpec: job.CronSpec,
				command:  job.Command,
			}
			log.Printf("CronSync: updated job %s schedule ('%s') to spec '%s'", job.ID, job.Command, job.CronSpec)
		}
	}

	for id, cache := range m.cronEntries {
		if !activeJobIDs[id] {
			m.leaderCron.Remove(cache.entryID)
			delete(m.cronEntries, id)
			log.Printf("CronSync: removed job %s from scheduler", id)
		}
	}
}

func (m *KazeMaster) Shutdown() {
	m.stopLeading()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	releaseScript := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
	m.redis.Eval(ctx, releaseScript, []string{"kaze:master:leader"}, m.id)
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
			CpuCount:          w.CpuCount,
			RamBytes:          w.RamBytes,
			Tags:              w.Tags,
		})
	}

	return resp, nil
}

func (m *KazeMaster) SubmitJob(ctx context.Context, req *pb.JobRequest) (*pb.JobResponse, error) {
	jobID := uuid.New().String()
	log.Printf("Received job: %s (ID: %s, CPU req: %.1f, RAM req: %d MB, Tags: %v)",
		req.Command, jobID, req.RequiredCpu, req.RequiredRamMb, req.RequiredTags)
	
	job := &Job{
		ID:            jobID,
		Command:       req.Command,
		Image:         req.Image,
		Priority:      req.Priority,
		Status:        "SUBMITTED",
		CronSpec:      req.CronSpec,
		RetryLimit:    req.RetryLimit,
		RequiredCpu:   req.RequiredCpu,
		RequiredRamMb: req.RequiredRamMb,
		RequiredTags:  req.RequiredTags,
	}

	if err := m.db.Create(job).Error; err != nil {
		return nil, err
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
		JobId:         job.ID,
		Command:       job.Command,
		Status:        job.Status,
		Result:        job.Result,
		RetryCount:    job.RetryCount,
		CronSpec:      job.CronSpec,
		Image:         job.Image,
		RequiredCpu:   job.RequiredCpu,
		RequiredRamMb: job.RequiredRamMb,
		RequiredTags:  job.RequiredTags,
		WorkerId:      job.WorkerID,
	}, nil
}

func (m *KazeMaster) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	var jobs []Job
	m.db.Find(&jobs)

	resp := &pb.ListJobsResponse{}
	for _, job := range jobs {
		resp.Jobs = append(resp.Jobs, &pb.JobStatusResponse{
			JobId:         job.ID,
			Command:       job.Command,
			Status:        job.Status,
			Result:        job.Result,
			RetryCount:    job.RetryCount,
			CronSpec:      job.CronSpec,
			Image:         job.Image,
			RequiredCpu:   job.RequiredCpu,
			RequiredRamMb: job.RequiredRamMb,
			RequiredTags:  job.RequiredTags,
			WorkerId:      job.WorkerID,
		})
	}
	return resp, nil
}

func (m *KazeMaster) UpdateJobStatus(ctx context.Context, req *pb.UpdateJobStatusRequest) (*pb.UpdateJobStatusResponse, error) {
	sanitizedResult := strings.ReplaceAll(req.Result, "\x00", "")

	var job Job
	if err := m.db.First(&job, "id = ?", req.JobId).Error; err != nil {
		return &pb.UpdateJobStatusResponse{Success: false}, err
	}

	if req.WorkerId != "" && job.WorkerID != req.WorkerId {
		log.Printf("Rejecting UpdateJobStatus for job %s: requested by worker %s, but job is currently assigned to worker %s",
			req.JobId, req.WorkerId, job.WorkerID)
		return &pb.UpdateJobStatusResponse{Success: false}, fmt.Errorf("job assigned to a different worker")
	}

	err := m.db.Model(&job).Updates(map[string]interface{}{
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

func (m *KazeMaster) workerHealthCheck(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.mu.Lock()
			for id, w := range m.workers {
				if time.Since(w.LastHeartbeat) > 30*time.Second {
					if w.Status != "OFFLINE" {
						log.Printf("Worker %s timed out, marking OFFLINE", id)
						w.Status = "OFFLINE"
						m.db.Model(w).Update("status", "OFFLINE")
						
						m.db.Model(&Job{}).Where("worker_id = ? AND status IN ?", id, []string{"ASSIGNED", "RUNNING"}).
							Updates(map[string]interface{}{"status": "QUEUED", "worker_id": ""})
					}
				}
			}
			m.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (m *KazeMaster) jobDispatcher(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.dispatchJobsOnce()
		case <-ctx.Done():
			return
		}
	}
}

func (m *KazeMaster) dispatchJobsOnce() {
	var pendingJobs []Job
	if err := m.db.Where("status IN ?", []string{"SUBMITTED", "QUEUED"}).Order("priority desc, created_at asc").Find(&pendingJobs).Error; err != nil {
		return
	}

	if len(pendingJobs) == 0 {
		return
	}

	// Fetch currently running/assigned jobs to compute resource allocation
	var activeJobs []Job
	if err := m.db.Where("status IN ?", []string{"ASSIGNED", "RUNNING"}).Find(&activeJobs).Error; err != nil {
		log.Printf("Dispatcher: failed to fetch active jobs: %v", err)
		return
	}

	// Calculate allocations per worker
	allocatedCpu := make(map[string]float32)
	allocatedRam := make(map[string]int64)
	for _, aj := range activeJobs {
		if aj.WorkerID != "" {
			allocatedCpu[aj.WorkerID] += aj.RequiredCpu
			allocatedRam[aj.WorkerID] += aj.RequiredRamMb * 1024 * 1024 // convert MB to bytes
		}
	}

	for _, job := range pendingJobs {
		m.mu.RLock()
		var eligibleWorkers []*Worker

		for _, w := range m.workers {
			if w.Status != "ONLINE" {
				continue
			}

			// Check CPU capacity
			availCpu := float32(w.CpuCount) - allocatedCpu[w.ID]
			if availCpu < job.RequiredCpu {
				continue
			}

			// Check RAM capacity
			availRam := w.RamBytes - allocatedRam[w.ID]
			reqRamBytes := job.RequiredRamMb * 1024 * 1024
			if availRam < reqRamBytes {
				continue
			}

			// Check Tag matching
			tagsMatch := true
			for k, v := range job.RequiredTags {
				wv, ok := w.Tags[k]
				if !ok || wv != v {
					tagsMatch = false
					break
				}
			}
			if !tagsMatch {
				continue
			}

			eligibleWorkers = append(eligibleWorkers, w)
		}
		m.mu.RUnlock()

		// Select the Best-Fit worker: smallest total capacity that still fits
		var targetWorker *Worker
		for _, w := range eligibleWorkers {
			if targetWorker == nil {
				targetWorker = w
			} else {
				// Compare total capacity: CPU count first, then RAM bytes
				if w.CpuCount < targetWorker.CpuCount {
					targetWorker = w
				} else if w.CpuCount == targetWorker.CpuCount && w.RamBytes < targetWorker.RamBytes {
					targetWorker = w
				}
			}
		}

		if targetWorker != nil {
			log.Printf("Assigning job %s (CPU req: %.1f, RAM req: %dMB) to worker %s (Total CPU: %d, Total RAM: %dMB)",
				job.ID, job.RequiredCpu, job.RequiredRamMb, targetWorker.ID, targetWorker.CpuCount, targetWorker.RamBytes/(1024*1024))
			
			err := m.db.Model(&job).Updates(map[string]interface{}{
				"status":    "ASSIGNED",
				"worker_id": targetWorker.ID,
			}).Error
			if err != nil {
				log.Printf("Dispatcher: failed to assign job %s: %v", job.ID, err)
				continue
			}

			// Update dynamically allocated map
			allocatedCpu[targetWorker.ID] += job.RequiredCpu
			allocatedRam[targetWorker.ID] += job.RequiredRamMb * 1024 * 1024
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

// --- TLS & AUTHENTICATION ---

func getMasterTLSCredentials() (credentials.TransportCredentials, error) {
	caCertFile := os.Getenv("KAZE_CA_CERT")
	if caCertFile == "" {
		caCertFile = "certs/ca.pem"
	}
	certFile := os.Getenv("KAZE_MASTER_CERT")
	if certFile == "" {
		certFile = "certs/master.pem"
	}
	keyFile := os.Getenv("KAZE_MASTER_KEY")
	if keyFile == "" {
		keyFile = "certs/master-key.pem"
	}

	serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load master key pair: %v", err)
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
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
		MinVersion:   tls.VersionTLS13,
	}

	return credentials.NewTLS(config), nil
}

func authorizeClient(ctx context.Context, fullMethod string) (context.Context, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "no peer info found")
	}

	tlsInfo, ok := pr.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "no TLS info found")
	}

	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return ctx, status.Error(codes.Unauthenticated, "no verified client certificate chain")
	}

	clientCert := tlsInfo.State.VerifiedChains[0][0]
	cn := clientCert.Subject.CommonName
	workerMethods := map[string]bool{
		"/kaze.KazeService/RegisterWorker":   true,
		"/kaze.KazeService/Heartbeat":        true,
		"/kaze.KazeService/UpdateJobStatus":  true,
		"/kaze.KazeService/StreamLogs":       true,
		"/kaze.KazeService/DeregisterWorker": true,
	}

	clientMethods := map[string]bool{
		"/kaze.KazeService/SubmitJob":   true,
		"/kaze.KazeService/ListWorkers": true,
		"/kaze.KazeService/WatchLogs":   true,
	}

	sharedMethods := map[string]bool{
		"/kaze.KazeService/ListJobs":     true,
		"/kaze.KazeService/GetJobStatus": true,
	}

	if workerMethods[fullMethod] {
		if cn != "worker" {
			return ctx, status.Errorf(codes.PermissionDenied, "role 'worker' required, client has CN '%s'", cn)
		}
	} else if clientMethods[fullMethod] {
		if cn != "client" {
			return ctx, status.Errorf(codes.PermissionDenied, "role 'client' required, client has CN '%s'", cn)
		}
	} else if sharedMethods[fullMethod] {
		if cn != "worker" && cn != "client" {
			return ctx, status.Errorf(codes.PermissionDenied, "role 'worker' or 'client' required, client has CN '%s'", cn)
		}
	} else {
		return ctx, status.Errorf(codes.PermissionDenied, "unknown method '%s'", fullMethod)
	}

	return ctx, nil
}

func unaryAuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	newCtx, err := authorizeClient(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	return handler(newCtx, req)
}

func streamAuthInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	_, err := authorizeClient(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	return handler(srv, ss)
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
	metricsAddr := os.Getenv("KAZE_METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Metrics available at %s/metrics", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, nil); err != nil {
			log.Printf("Metrics server failed: %v", err)
		}
	}()

	// 3. gRPC Server
	tlsCreds, err := getMasterTLSCredentials()
	if err != nil {
		log.Fatalf("failed to load TLS credentials: %v", err)
	}

	masterAddr := os.Getenv("KAZE_MASTER_ADDR")
	if masterAddr == "" {
		masterAddr = ":50051"
	}
	lis, err := net.Listen("tcp", masterAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.Creds(tlsCreds),
		grpc.UnaryInterceptor(unaryAuthInterceptor),
		grpc.StreamInterceptor(streamAuthInterceptor),
	)
	master := NewKazeMaster(db, rdb)
	pb.RegisterKazeServiceServer(s, master)

	// 4. Graceful Shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down Kaze Master...")
		
		master.Shutdown()
		s.GracefulStop()
		os.Exit(0)
	}()

	log.Printf("Kaze Master listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
