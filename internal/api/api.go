package api

import (
	"strings"
	"time"
)

type RegisterRequest struct {
	ID       string `json:"id"`
	Hostname string `json:"hostname"`
}

type HeartbeatRequest struct {
	SessionID string           `json:"session_id"`
	Instances []InstanceStatus `json:"instances,omitempty"`
}

type HeartbeatResponse struct {
	Assignments []Assignment `json:"assignments"`
}

type ServiceSpec struct {
	Name     string   `json:"name"`
	Command  string   `json:"command"`
	Args     []string `json:"args,omitempty"`
	Replicas int      `json:"replicas"`
	Port     int      `json:"port,omitempty"`
}

type Assignment struct {
	ID      string   `json:"id"`
	Service string   `json:"service"`
	Command string   `json:"command"`
	Args    []string `json:"args,omitempty"`
	Port    int      `json:"port,omitempty"`
	StopID  string   `json:"stop_id,omitempty"`
}

type InstanceStatus struct {
	ID       string `json:"id"`
	PID      int    `json:"pid"`
	Port     int    `json:"port,omitempty"`
	Status   string `json:"status"`
	Restarts int    `json:"restarts"`
	Error    string `json:"error,omitempty"`
}

type Instance struct {
	InstanceStatus
	WorkerID string `json:"worker_id"`
	StopID   string `json:"stop_id,omitempty"`
}

type Service struct {
	ServiceSpec
	Instances []Instance `json:"instances"`
}

type ServicesResponse struct {
	Services []Service `json:"services"`
}

type Worker struct {
	ID        string    `json:"id"`
	Hostname  string    `json:"hostname"`
	IP        string    `json:"ip"`
	CanStart  bool      `json:"can_start"`
	SessionID string    `json:"session_id"`
	Status    string    `json:"status"`
	LastSeen  time.Time `json:"last_seen"`
}

type WorkersResponse struct {
	Workers []Worker `json:"workers"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func ValidName(id string) bool {
	if strings.TrimSpace(id) == "" {
		return false
	}
	return id != "." && id != ".."
}
