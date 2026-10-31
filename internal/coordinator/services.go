package coordinator

import (
	"crypto/rand"
	"net/http"
	"sort"
	"strings"

	"fabrenetes/internal/api"
)

func (s *Coordinator) createService(w http.ResponseWriter, r *http.Request) {
	var request api.ServiceSpec
	if !readJSON(w, r, &request) {
		return
	}
	if !api.ValidName(request.Name) || strings.TrimSpace(request.Command) == "" || request.Replicas < 1 {
		writeError(w, http.StatusBadRequest, "name, command, and a positive replica count are required")
		return
	}
	if request.Port < 0 || request.Port > 65535 {
		writeError(w, http.StatusBadRequest, "port must be between 1 and 65535 when specified")
		return
	}
	s.mu.Lock()
	_, exists := s.services[request.Name]
	if exists {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, "service already exists")
		return
	}
	service := api.Service{ServiceSpec: request, Instances: make([]api.Instance, request.Replicas)}
	for i := range service.Instances {
		service.Instances[i].InstanceStatus = api.InstanceStatus{ID: rand.Text(), Status: "pending"}
	}
	s.services[service.Name] = service
	s.assignPending()
	service = s.serviceState(s.services[service.Name])
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, service)
}

func (s *Coordinator) listServices(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	services := make([]api.Service, 0, len(s.services))
	for _, service := range s.services {
		services = append(services, s.serviceState(service))
	}
	s.mu.Unlock()
	sort.Slice(services, func(i, j int) bool { return services[i].Name < services[j].Name })
	writeJSON(w, http.StatusOK, api.ServicesResponse{Services: services})
}

func (s *Coordinator) deleteService(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	name := r.PathValue("name")
	_, exists := s.services[name]
	if !exists {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, "service does not exist")
		return
	}
	delete(s.services, name)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Coordinator) stopInstance(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	service, exists := s.services[r.PathValue("name")]
	if !exists {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, "service does not exist")
		return
	}
	for i := range service.Instances {
		instance := &service.Instances[i]
		if instance.ID != r.PathValue("id") {
			continue
		}
		if instance.WorkerID == "" || instance.Status != "running" || instance.PID <= 0 {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, "instance is not running")
			return
		}
		worker, registered := s.workers[instance.WorkerID]
		if !registered || s.now().Sub(worker.LastSeen) >= s.timeout {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, "worker is unreachable")
			return
		}
		instance.StopID = rand.Text()
		current := *instance
		s.mu.Unlock()
		writeJSON(w, http.StatusAccepted, current)
		return
	}
	s.mu.Unlock()
	writeError(w, http.StatusNotFound, "instance does not exist")
}

func (s *Coordinator) serviceState(service api.Service) api.Service {
	service.Instances = append([]api.Instance(nil), service.Instances...)
	now := s.now()
	for i := range service.Instances {
		instance := &service.Instances[i]
		if instance.WorkerID != "" && now.Sub(s.workers[instance.WorkerID].LastSeen) >= s.timeout {
			instance.Status = "unreachable"
		}
	}
	return service
}

func (s *Coordinator) assignPending() {
	workers := make([]string, 0, len(s.workers))
	now := s.now()
	for id, worker := range s.workers {
		if now.Sub(worker.LastSeen) < s.timeout {
			workers = append(workers, id)
		}
	}
	if len(workers) == 0 {
		return
	}
	sort.Strings(workers)
	counts := make(map[string]int)
	names := make([]string, 0, len(s.services))
	for name, service := range s.services {
		names = append(names, name)
		for _, instance := range service.Instances {
			if instance.WorkerID != "" {
				counts[instance.WorkerID]++
			}
		}
	}
	sort.Strings(names)
	for _, name := range names {
		service := s.services[name]
		for i := range service.Instances {
			instance := &service.Instances[i]
			if instance.WorkerID != "" {
				continue
			}
			chosen := workers[0]
			for _, id := range workers[1:] {
				if counts[id] < counts[chosen] {
					chosen = id
				}
			}
			instance.WorkerID = chosen
			counts[chosen]++
		}
	}
}

func (s *Coordinator) assignments(workerID string) []api.Assignment {
	assignments := make([]api.Assignment, 0)
	for _, service := range s.services {
		for _, instance := range service.Instances {
			if instance.WorkerID == workerID {
				assignments = append(assignments, api.Assignment{
					ID:      instance.ID,
					Service: service.Name,
					Command: service.Command,
					Args:    service.Args,
					Port:    service.Port,
					StopID:  instance.StopID,
				})
			}
		}
	}
	sort.Slice(assignments, func(i, j int) bool { return assignments[i].ID < assignments[j].ID })
	return assignments
}
