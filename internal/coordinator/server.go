package coordinator

import (
	"crypto/rand"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"fabrenetes/internal/api"
)

type Coordinator struct {
	mu         sync.Mutex
	workers    map[string]api.Worker
	services   map[string]api.Service
	managedVMs map[string]string
	vmCommands VMCommands
	timeout    time.Duration
	now        func() time.Time
}

func New(timeout time.Duration) *Coordinator {
	return &Coordinator{
		workers:  make(map[string]api.Worker),
		services: make(map[string]api.Service),
		timeout:  timeout,
		now:      time.Now,
	}
}

func (s *Coordinator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.dashboard)
	mux.Handle("GET /dashboard/", http.FileServerFS(dashboardFiles))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /workers", s.register)
	mux.HandleFunc("POST /workers/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("POST /workers/{id}/start", s.startWorker)
	mux.HandleFunc("POST /workers/{id}/stop", s.stopWorker)
	mux.HandleFunc("GET /workers", s.list)
	mux.HandleFunc("POST /services", s.createService)
	mux.HandleFunc("GET /services", s.listServices)
	mux.HandleFunc("DELETE /services/{name}", s.deleteService)
	mux.HandleFunc("POST /services/{name}/instances/{id}/stop", s.stopInstance)
	return mux
}

func (s *Coordinator) register(w http.ResponseWriter, r *http.Request) {
	var request api.RegisterRequest
	if !readJSON(w, r, &request) {
		return
	}
	if !api.ValidName(request.ID) || strings.TrimSpace(request.Hostname) == "" {
		writeError(w, http.StatusBadRequest, "valid id and hostname are required")
		return
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	s.mu.Lock()
	worker := api.Worker{
		ID:        request.ID,
		Hostname:  request.Hostname,
		IP:        ip,
		SessionID: rand.Text(),
		Status:    "ready",
		LastSeen:  s.now().UTC(),
	}
	s.workers[worker.ID] = worker
	for _, service := range s.services {
		for i := range service.Instances {
			instance := &service.Instances[i]
			if instance.WorkerID == worker.ID {
				instance.InstanceStatus = api.InstanceStatus{ID: instance.ID, Status: "unknown"}
			}
		}
	}
	s.assignPending()
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, worker)
}

func (s *Coordinator) heartbeat(w http.ResponseWriter, r *http.Request) {
	var request api.HeartbeatRequest
	if !readJSON(w, r, &request) {
		return
	}
	if strings.TrimSpace(request.SessionID) == "" {
		writeError(w, http.StatusBadRequest, "session_id is required")
		return
	}
	s.mu.Lock()
	worker, exists := s.workers[r.PathValue("id")]
	if !exists {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, "worker is not registered")
		return
	}
	if worker.SessionID != request.SessionID {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, "worker session has been replaced")
		return
	}
	instances := make(map[string]*api.Instance)
	for _, service := range s.services {
		for i := range service.Instances {
			instance := &service.Instances[i]
			instances[instance.ID] = instance
		}
	}
	seen := make(map[string]bool)
	for _, report := range request.Instances {
		instance := instances[report.ID]
		if report.ID == "" || seen[report.ID] || (instance != nil && instance.WorkerID != worker.ID) {
			s.mu.Unlock()
			writeError(w, http.StatusBadRequest, "instance reports must be unique and belong to this worker")
			return
		}
		if (report.Status != "running" && report.Status != "failed") || report.PID < 0 || report.Restarts < 0 || (report.Status == "running" && report.PID == 0) {
			s.mu.Unlock()
			writeError(w, http.StatusBadRequest, "instance report has an invalid status, PID, or restart count")
			return
		}
		if report.Port < 0 || report.Port > 65535 {
			s.mu.Unlock()
			writeError(w, http.StatusBadRequest, "instance report has an invalid port")
			return
		}
		seen[report.ID] = true
	}
	for _, report := range request.Instances {
		instance := instances[report.ID]
		if instance != nil {
			instance.InstanceStatus = report
		}
	}
	worker.LastSeen = s.now().UTC()
	worker.IP, _, _ = net.SplitHostPort(r.RemoteAddr)
	s.workers[worker.ID] = worker
	s.assignPending()
	assignments := s.assignments(worker.ID)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, api.HeartbeatResponse{Assignments: assignments})
}

func (s *Coordinator) list(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	now := s.now()
	workers := make([]api.Worker, 0, len(s.workers))
	for _, worker := range s.workers {
		if now.Sub(worker.LastSeen) >= s.timeout {
			worker.Status = "unreachable"
		}
		_, worker.CanStart = s.managedVMs[worker.ID]
		workers = append(workers, worker)
	}
	for id, ip := range s.managedVMs {
		_, registered := s.workers[id]
		if !registered {
			workers = append(workers, api.Worker{ID: id, Hostname: id, IP: ip, CanStart: true, Status: "unregistered"})
		}
	}
	s.mu.Unlock()
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	writeJSON(w, http.StatusOK, api.WorkersResponse{Workers: workers})
}

func readJSON(w http.ResponseWriter, r *http.Request, destination any) bool {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read request body")
		return false
	}
	err = json.Unmarshal(body, destination)
	if err != nil {
		writeError(w, http.StatusBadRequest, "request body must contain one valid JSON object")
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, api.ErrorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
