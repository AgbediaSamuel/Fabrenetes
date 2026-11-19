package coordinator

import (
	"crypto/rand"
	"net/http"
	"net/url"
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
	if request.Stateful && request.Replicas != 3 {
		writeError(w, http.StatusBadRequest, "stateful services require exactly three replicas")
		return
	}
	err := normalizePartitioning(&request)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if request.Package != "" {
		_, err := s.packagePath(request.Package)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
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
	if request.Stateful {
		workers := s.stateWorkers()
		if len(workers) != 3 {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, "stateful services need three available workers with distinct state addresses")
			return
		}
		service.State = &api.StateGroup{ID: rand.Text(), Partitioning: request.Partitioning, Partitions: request.Partitions}
		for i, worker := range workers {
			service.Instances[i].WorkerID = worker.ID
			service.State.Members = append(service.State.Members, api.StateMember{ID: worker.ID, Address: worker.StateAddress})
		}
	}
	services := copyServices(s.services)
	services[service.Name] = service
	s.assignPending(services)
	err = s.saveServices(services)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.services = services
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
	services := copyServices(s.services)
	delete(services, name)
	err := s.saveServices(services)
	if err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.services = services
	delete(s.nextInstance, name)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Coordinator) stopInstance(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	services := copyServices(s.services)
	service, exists := services[r.PathValue("name")]
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
		err := s.saveServices(services)
		if err != nil {
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.services = services
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
	service.Endpoint = "/apps/" + url.PathEscape(service.Name) + "/"
	service.PartitionStatus = s.partitionStatus(service)
	now := s.now()
	for i := range service.Instances {
		instance := &service.Instances[i]
		if instance.WorkerID != "" && now.Sub(s.workers[instance.WorkerID].LastSeen) >= s.timeout {
			instance.Status = "unreachable"
		}
	}
	return service
}

func (s *Coordinator) assignPending(services map[string]api.Service) bool {
	workers := make([]string, 0, len(s.workers))
	now := s.now()
	for id, worker := range s.workers {
		if now.Sub(worker.LastSeen) < s.timeout {
			workers = append(workers, id)
		}
	}
	if len(workers) == 0 {
		return false
	}
	sort.Strings(workers)
	counts := make(map[string]int)
	names := make([]string, 0, len(services))
	for name, service := range services {
		names = append(names, name)
		for _, instance := range service.Instances {
			if instance.WorkerID != "" {
				counts[instance.WorkerID]++
			}
		}
	}
	sort.Strings(names)
	changed := false
	for _, name := range names {
		service := services[name]
		if service.Stateful {
			continue
		}
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
			changed = true
		}
	}
	return changed
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
					Package: service.Package,
					Port:    service.Port,
					StopID:  instance.StopID,
					State:   service.State,
				})
			}
		}
	}
	sort.Slice(assignments, func(i, j int) bool { return assignments[i].ID < assignments[j].ID })
	return assignments
}
