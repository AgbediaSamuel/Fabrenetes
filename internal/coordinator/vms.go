package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
)

type VM struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
}

type VMCommands struct {
	List string
	Run  string
}

func (s *Coordinator) ConfigureVMs(ctx context.Context, names []string, commands VMCommands) error {
	if len(names) == 0 {
		return nil
	}
	vms, err := commands.listVMs(ctx)
	if err != nil {
		return err
	}
	available := make(map[string]string)
	for _, vm := range vms {
		available[vm.Name] = vm.IP
	}
	managed := make(map[string]string)
	for _, name := range names {
		ip, exists := available[name]
		if !exists {
			return fmt.Errorf("VM %q does not exist", name)
		}
		managed[name] = ip
	}
	s.mu.Lock()
	s.managedVMs = managed
	s.vmCommands = commands
	s.mu.Unlock()
	return nil
}

func (s *Coordinator) startWorker(w http.ResponseWriter, r *http.Request) {
	s.controlWorker(w, r, "start")
}

func (s *Coordinator) stopWorker(w http.ResponseWriter, r *http.Request) {
	s.controlWorker(w, r, "stop")
}

func (s *Coordinator) controlWorker(w http.ResponseWriter, r *http.Request, action string) {
	id := r.PathValue("id")
	s.mu.Lock()
	ip, managed := s.managedVMs[id]
	commands := s.vmCommands
	s.mu.Unlock()
	if !managed {
		writeError(w, http.StatusNotFound, "worker is not a configured VM")
		return
	}
	err := commands.runOnVM(r.Context(), VM{Name: id, IP: ip}, "sudo", "systemctl", action, "fabrenetes-worker")
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if action == "stop" {
		s.mu.Lock()
		delete(s.workers, id)
		s.mu.Unlock()
	}
	w.WriteHeader(http.StatusNoContent)
}

func (commands VMCommands) listVMs(ctx context.Context) ([]VM, error) {
	if commands.List != "" {
		output, err := vmCommandOutput(ctx, commands.List)
		if err != nil {
			return nil, err
		}
		var vms []VM
		err = json.Unmarshal(output, &vms)
		if err != nil {
			return nil, fmt.Errorf("read VM list: %w", err)
		}
		return vms, nil
	}
	output, err := vmCommandOutput(ctx, "multipass", "list", "--format", "json")
	if err != nil {
		return nil, err
	}
	var inventory struct {
		List []struct {
			Name string   `json:"name"`
			IPv4 []string `json:"ipv4"`
		} `json:"list"`
	}
	err = json.Unmarshal(output, &inventory)
	if err != nil {
		return nil, fmt.Errorf("read Multipass VM list: %w", err)
	}
	vms := make([]VM, 0, len(inventory.List))
	for _, item := range inventory.List {
		vm := VM{Name: item.Name}
		if len(item.IPv4) > 0 {
			vm.IP = item.IPv4[0]
		}
		vms = append(vms, vm)
	}
	return vms, nil
}

func (commands VMCommands) runOnVM(ctx context.Context, vm VM, command ...string) error {
	program := commands.Run
	args := []string{vm.Name, vm.IP}
	if program == "" {
		program = "multipass"
		args = []string{"exec", vm.Name, "--"}
	}
	args = append(args, command...)
	_, err := vmCommandOutput(ctx, program, args...)
	return err
}

func vmCommandOutput(ctx context.Context, program string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, program, args...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			return nil, fmt.Errorf("%s: %w", program, err)
		}
		return nil, fmt.Errorf("%s: %w: %s", program, err, message)
	}
	return output, nil
}
