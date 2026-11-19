package api

import (
	"hash/fnv"
	"strconv"
	"strings"
	"time"
)

type RegisterRequest struct {
	ID           string `json:"id"`
	Hostname     string `json:"hostname"`
	StateAddress string `json:"state_address,omitempty"`
}

type HeartbeatRequest struct {
	SessionID string           `json:"session_id"`
	Instances []InstanceStatus `json:"instances,omitempty"`
	State     []StateStatus    `json:"state,omitempty"`
}

type HeartbeatResponse struct {
	Assignments []Assignment `json:"assignments"`
}

type ServiceSpec struct {
	Name         string   `json:"name"`
	Command      string   `json:"command"`
	Args         []string `json:"args,omitempty"`
	Package      string   `json:"package,omitempty"`
	Replicas     int      `json:"replicas"`
	Port         int      `json:"port,omitempty"`
	Stateful     bool     `json:"stateful,omitempty"`
	Partitioning string   `json:"partitioning,omitempty"`
	Partitions   int      `json:"partitions,omitempty"`
}

type Assignment struct {
	ID      string      `json:"id"`
	Service string      `json:"service"`
	Command string      `json:"command"`
	Args    []string    `json:"args,omitempty"`
	Package string      `json:"package,omitempty"`
	Port    int         `json:"port,omitempty"`
	StopID  string      `json:"stop_id,omitempty"`
	State   *StateGroup `json:"state,omitempty"`
}

type StateGroup struct {
	ID           string        `json:"id"`
	Members      []StateMember `json:"members"`
	Partitioning string        `json:"partitioning,omitempty"`
	Partitions   int           `json:"partitions,omitempty"`
}

func (group StateGroup) PartitionCount() int {
	if group.Partitions == 0 {
		return 1
	}
	return group.Partitions
}

func (group StateGroup) PartitionID(index int) string {
	if group.PartitionCount() == 1 {
		return group.ID
	}
	return group.ID + "-" + strconv.Itoa(index)
}

func (group StateGroup) PartitionFor(key string) int {
	if group.Partitioning != "hash" {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum64() % uint64(group.PartitionCount()))
}

type StateStatus struct {
	GroupID   string `json:"group_id"`
	Partition int    `json:"partition"`
	Role      string `json:"role"`
	LeaderID  string `json:"leader_id,omitempty"`
	Term      uint64 `json:"term"`
	Available bool   `json:"available"`
}

type PartitionStatus struct {
	Index    int                  `json:"index"`
	LeaderID string               `json:"leader_id,omitempty"`
	Status   string               `json:"status"`
	Replicas []StateReplicaStatus `json:"replicas"`
}

type StateReplicaStatus struct {
	WorkerID string `json:"worker_id"`
	Role     string `json:"role"`
	Status   string `json:"status"`
}

type StateMember struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type Package struct {
	ID string `json:"id"`
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
	Instances       []Instance        `json:"instances"`
	State           *StateGroup       `json:"state,omitempty"`
	PartitionStatus []PartitionStatus `json:"partition_status,omitempty"`
	Endpoint        string            `json:"endpoint,omitempty"`
}

type ServicesResponse struct {
	Services []Service `json:"services"`
}

type Worker struct {
	ID           string    `json:"id"`
	Hostname     string    `json:"hostname"`
	IP           string    `json:"ip"`
	CanStart     bool      `json:"can_start"`
	SessionID    string    `json:"session_id"`
	Status       string    `json:"status"`
	LastSeen     time.Time `json:"last_seen"`
	StateAddress string    `json:"state_address,omitempty"`
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
