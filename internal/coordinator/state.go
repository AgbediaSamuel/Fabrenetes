package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"fabrenetes/internal/api"
)

func (s *Coordinator) ConfigureData(directory string) error {
	if directory == "" {
		config, err := os.UserConfigDir()
		if err != nil {
			return err
		}
		directory = filepath.Join(config, "fabrenetes", "coordinator")
	}
	err := os.MkdirAll(directory, 0700)
	if err != nil {
		return fmt.Errorf("create coordinator data directory: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(directory, "services.json"))
	if errors.Is(err, os.ErrNotExist) {
		s.dataDir = directory
		return nil
	}
	if err != nil {
		return fmt.Errorf("read saved services: %w", err)
	}
	var services map[string]api.Service
	err = json.Unmarshal(data, &services)
	if err != nil || services == nil {
		return errors.New("saved services must contain a JSON object")
	}
	instances := make(map[string]bool)
	groups := make(map[string]bool)
	for name, service := range services {
		if !api.ValidName(name) || name != service.Name || strings.TrimSpace(service.Command) == "" || service.Replicas < 1 || len(service.Instances) != service.Replicas {
			return fmt.Errorf("invalid saved service %q", name)
		}
		if service.Stateful {
			err = normalizePartitioning(&service.ServiceSpec)
			if err != nil {
				return fmt.Errorf("invalid saved service %q: %w", name, err)
			}
			if service.State != nil {
				group := *service.State
				if group.Partitioning == "" {
					group.Partitioning = "none"
				}
				if group.Partitions == 0 {
					group.Partitions = 1
				}
				service.State = &group
			}
			err = validateStateGroup(service)
			if err != nil {
				return fmt.Errorf("invalid saved service %q: %w", name, err)
			}
			if groups[service.State.ID] {
				return fmt.Errorf("duplicate saved state group %q", service.State.ID)
			}
			groups[service.State.ID] = true
		} else {
			err = normalizePartitioning(&service.ServiceSpec)
			if err != nil || service.State != nil {
				return fmt.Errorf("stateless service %q has saved state configuration", name)
			}
		}
		service.Endpoint = ""
		service.PartitionStatus = nil
		for i := range service.Instances {
			instance := &service.Instances[i]
			if instance.ID == "" || instances[instance.ID] {
				return fmt.Errorf("invalid saved instance identity in service %q", name)
			}
			instances[instance.ID] = true
			status := "unknown"
			if instance.WorkerID == "" {
				status = "pending"
			}
			instance.InstanceStatus = api.InstanceStatus{ID: instance.ID, Status: status}
		}
		services[name] = service
	}
	s.services = services
	s.dataDir = directory
	return nil
}

func (s *Coordinator) saveServices(services map[string]api.Service) error {
	if s.dataDir == "" {
		return nil
	}
	data, err := json.MarshalIndent(services, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(s.dataDir, ".services-*")
	if err != nil {
		return fmt.Errorf("save services: %w", err)
	}
	defer os.Remove(file.Name())
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("save services: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("save services: %w", closeErr)
	}
	err = os.Rename(file.Name(), filepath.Join(s.dataDir, "services.json"))
	if err != nil {
		return fmt.Errorf("save services: %w", err)
	}
	return nil
}

func copyServices(services map[string]api.Service) map[string]api.Service {
	copy := make(map[string]api.Service, len(services))
	for name, service := range services {
		service.Instances = append([]api.Instance(nil), service.Instances...)
		copy[name] = service
	}
	return copy
}

func validStateAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || strings.ContainsAny(host, "/\\ \t\r\n?#@") {
		return false
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsUnspecified() {
		return false
	}
	number, err := strconv.Atoi(port)
	return err == nil && number > 0 && number <= 65535
}

func validateStateGroup(service api.Service) error {
	if service.Replicas != 3 || service.State == nil || !api.ValidName(service.State.ID) || !filepath.IsLocal(service.State.ID) || strings.ContainsAny(service.State.ID, "/\\") || len(service.State.Members) != 3 {
		return errors.New("stateful services require three replicas and a state group")
	}
	if service.State.Partitioning != service.Partitioning || service.State.Partitions != service.Partitions {
		return errors.New("state group partitioning must match its service")
	}
	members := make(map[string]bool)
	addresses := make(map[string]bool)
	for _, member := range service.State.Members {
		if !api.ValidName(member.ID) || members[member.ID] || !validStateAddress(member.Address) || addresses[member.Address] {
			return errors.New("state group members must have distinct worker identities and addresses")
		}
		members[member.ID] = true
		addresses[member.Address] = true
	}
	for _, instance := range service.Instances {
		if !members[instance.WorkerID] {
			return errors.New("stateful instances must match their state group members")
		}
		delete(members, instance.WorkerID)
	}
	return nil
}

func (s *Coordinator) stateWorkers() []api.Worker {
	counts := make(map[string]int)
	for _, service := range s.services {
		for _, instance := range service.Instances {
			counts[instance.WorkerID]++
		}
	}
	workers := make([]api.Worker, 0)
	for _, worker := range s.workers {
		if s.now().Sub(worker.LastSeen) < s.timeout && worker.StateAddress != "" {
			workers = append(workers, worker)
		}
	}
	sort.Slice(workers, func(i, j int) bool {
		if counts[workers[i].ID] == counts[workers[j].ID] {
			return workers[i].ID < workers[j].ID
		}
		return counts[workers[i].ID] < counts[workers[j].ID]
	})
	selected := make([]api.Worker, 0, 3)
	addresses := make(map[string]bool)
	for _, worker := range workers {
		if addresses[worker.StateAddress] {
			continue
		}
		selected = append(selected, worker)
		addresses[worker.StateAddress] = true
		if len(selected) == 3 {
			break
		}
	}
	return selected
}
