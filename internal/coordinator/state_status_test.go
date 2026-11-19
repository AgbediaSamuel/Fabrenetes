package coordinator

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestPartitionStatusUsesFreshConfirmedLeadership(t *testing.T) {
	server := New(10 * time.Second)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	handler := server.Handler()
	workers := stateWorkersForTest(t, handler)
	service := createService(t, handler, api.ServiceSpec{Name: "notes", Command: "notes", Replicas: 3, Stateful: true, Partitioning: "hash", Partitions: 2})
	for _, partition := range service.PartitionStatus {
		if partition.Status != "waiting" || len(partition.Replicas) != 3 {
			t.Fatalf("new partition: %+v", partition)
		}
	}
	report := func(partition int, role, leader string, term uint64, available bool) api.StateStatus {
		return api.StateStatus{GroupID: service.State.ID, Partition: partition, Role: role, LeaderID: leader, Term: term, Available: available}
	}
	heartbeatState(t, handler, workers[0], []api.StateStatus{report(0, "leader", workers[0].ID, 1, true), report(1, "follower", workers[2].ID, 1, false)})
	heartbeatState(t, handler, workers[1], []api.StateStatus{report(0, "follower", workers[0].ID, 1, false), report(1, "follower", workers[2].ID, 1, false)})
	heartbeatState(t, handler, workers[2], []api.StateStatus{report(0, "follower", workers[0].ID, 1, false), report(1, "leader", workers[2].ID, 1, true)})
	current := listServices(t, handler)[0]
	if current.PartitionStatus[0].Status != "available" || current.PartitionStatus[0].LeaderID != workers[0].ID || current.PartitionStatus[1].Status != "available" || current.PartitionStatus[1].LeaderID != workers[2].ID {
		t.Fatalf("confirmed leaders missing: %+v", current.PartitionStatus)
	}
	heartbeatState(t, handler, workers[1], []api.StateStatus{report(0, "candidate", "", 2, false)})
	current = listServices(t, handler)[0]
	if current.PartitionStatus[0].Status != "unavailable" || current.PartitionStatus[0].LeaderID != "" {
		t.Fatalf("old term still available: %+v", current.PartitionStatus[0])
	}
	heartbeatState(t, handler, workers[2], []api.StateStatus{report(0, "leader", workers[2].ID, 2, true)})
	current = listServices(t, handler)[0]
	if current.PartitionStatus[0].Status != "available" || current.PartitionStatus[0].LeaderID != workers[2].ID {
		t.Fatalf("new term leader missing: %+v", current.PartitionStatus[0])
	}
	heartbeatState(t, handler, workers[1], []api.StateStatus{report(0, "follower", workers[0].ID, 2, false)})
	current = listServices(t, handler)[0]
	if current.PartitionStatus[0].Status != "unavailable" || current.PartitionStatus[0].LeaderID != "" {
		t.Fatalf("conflicting leaders accepted: %+v", current.PartitionStatus[0])
	}
	heartbeatState(t, handler, workers[1], []api.StateStatus{report(0, "follower", workers[2].ID, 2, false)})
	registerStateWorker(t, handler, workers[2].ID, workers[2].StateAddress)
	current = listServices(t, handler)[0]
	if current.PartitionStatus[0].Status != "unavailable" || current.PartitionStatus[0].Replicas[2].Role != "unknown" {
		t.Fatalf("registration retained previous session reports: %+v", current.PartitionStatus[0])
	}
	now = now.Add(10 * time.Second)
	current = listServices(t, handler)[0]
	for _, partition := range current.PartitionStatus {
		if partition.Status != "unavailable" || partition.LeaderID != "" {
			t.Fatalf("expired report considered current: %+v", partition)
		}
		for _, replica := range partition.Replicas {
			if replica.Status != "unreachable" {
				t.Fatalf("expired replica: %+v", replica)
			}
		}
	}
}

func TestStateReportsValidateAtomicallyWithProcessReports(t *testing.T) {
	server := New(time.Minute)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	handler := server.Handler()
	workers := stateWorkersForTest(t, handler)
	service := createService(t, handler, api.ServiceSpec{Name: "notes", Command: "notes", Replicas: 3, Stateful: true})
	valid := api.StateStatus{GroupID: service.State.ID, Role: "leader", LeaderID: workers[0].ID, Term: 1, Available: true}
	heartbeatState(t, handler, workers[0], []api.StateStatus{valid})
	before := append([]api.StateStatus(nil), server.stateReports[workers[0].ID]...)
	now = now.Add(time.Second)
	for _, invalid := range []api.StateStatus{
		{GroupID: service.State.ID, Partition: 1, Role: "follower"},
		{GroupID: service.State.ID, Partition: -1, Role: "follower"},
		{GroupID: service.State.ID, Role: "Follower"},
		{GroupID: service.State.ID, Role: "follower", LeaderID: "outsider"},
		{GroupID: service.State.ID, Role: "follower", LeaderID: workers[0].ID, Term: 1, Available: true},
		{GroupID: service.State.ID, Role: "leader", LeaderID: workers[1].ID, Term: 1, Available: true},
		{GroupID: service.State.ID, Role: "leader", LeaderID: workers[0].ID, Available: true},
		{Role: "leader"},
	} {
		response := sendJSON(t, handler, http.MethodPost, "/workers/"+workers[0].ID+"/heartbeat", api.HeartbeatRequest{
			SessionID: workers[0].SessionID,
			Instances: []api.InstanceStatus{{ID: service.Instances[0].ID, PID: 123, Status: "running"}},
			State:     []api.StateStatus{invalid},
		})
		if response.Code != http.StatusBadRequest || !server.workers[workers[0].ID].LastSeen.Equal(workers[0].LastSeen) || server.services[service.Name].Instances[0].PID != 0 || !reflect.DeepEqual(before, server.stateReports[workers[0].ID]) {
			t.Fatalf("invalid state report partly applied: %d %+v", response.Code, invalid)
		}
	}
	response := sendJSON(t, handler, http.MethodPost, "/workers/"+workers[0].ID+"/heartbeat", api.HeartbeatRequest{SessionID: workers[0].SessionID, State: []api.StateStatus{valid, valid}})
	if response.Code != http.StatusBadRequest {
		t.Fatal("duplicate state report accepted")
	}
	outsider := registerStateWorker(t, handler, "outsider", "127.0.0.1:9104")
	response = sendJSON(t, handler, http.MethodPost, "/workers/outsider/heartbeat", api.HeartbeatRequest{SessionID: outsider.SessionID, State: []api.StateStatus{valid}})
	if response.Code != http.StatusBadRequest {
		t.Fatal("foreign state group report accepted")
	}
	response = sendRequest(handler, http.MethodDelete, "/services/notes", "")
	if response.Code != http.StatusNoContent {
		t.Fatal(response.Body.String())
	}
	heartbeatState(t, handler, workers[0], []api.StateStatus{valid})
}

func heartbeatState(t *testing.T, handler http.Handler, worker api.Worker, reports []api.StateStatus) {
	t.Helper()
	response := sendJSON(t, handler, http.MethodPost, "/workers/"+worker.ID+"/heartbeat", api.HeartbeatRequest{SessionID: worker.SessionID, State: reports})
	if response.Code != http.StatusOK {
		t.Fatalf("state heartbeat: %d %s", response.Code, response.Body.String())
	}
}
