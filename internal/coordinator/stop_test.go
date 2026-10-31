package coordinator

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestStopInstancePreservesServiceAndTargetsOneWorker(t *testing.T) {
	server := New(10 * time.Second)
	handler := server.Handler()
	workerA := registerWorker(t, handler, "worker-a")
	workerB := registerWorker(t, handler, "worker-b")
	service := createService(t, handler, api.ServiceSpec{Name: "clock", Command: "clock", Replicas: 2, Port: 9001})
	target := service.Instances[0]
	peer := service.Instances[1]
	heartbeatWorker(t, handler, workerA, []api.InstanceStatus{{ID: target.ID, PID: 123, Status: "running"}})
	heartbeatWorker(t, handler, workerB, []api.InstanceStatus{{ID: peer.ID, PID: 456, Status: "running"}})

	path := "/services/clock/instances/" + target.ID + "/stop"
	response := sendRequest(handler, http.MethodPost, path, "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("stop instance: %d %s", response.Code, response.Body.String())
	}
	var stopped api.Instance
	err := json.Unmarshal(response.Body.Bytes(), &stopped)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.ID != target.ID || stopped.WorkerID != workerA.ID || stopped.StopID == "" {
		t.Fatalf("unexpected stop response: %+v", stopped)
	}

	current := listServices(t, handler)[0]
	if current.Name != service.Name || current.Command != service.Command || current.Replicas != 2 || current.Port != 9001 || len(current.Instances) != 2 {
		t.Fatalf("stop changed the service: %+v", current)
	}
	if current.Instances[0].ID != target.ID || current.Instances[0].StopID != stopped.StopID || current.Instances[1].ID != peer.ID || current.Instances[1].StopID != "" {
		t.Fatalf("stop affected the wrong instance: %+v", current.Instances)
	}
	for range 2 {
		assignment := heartbeatWorker(t, handler, workerA, []api.InstanceStatus{{ID: target.ID, PID: 123, Status: "running"}}).Assignments[0]
		if assignment.ID != target.ID || assignment.StopID != stopped.StopID {
			t.Fatalf("stop request changed across heartbeats: %+v", assignment)
		}
	}
	other := heartbeatWorker(t, handler, workerB, nil).Assignments[0]
	if other.ID != peer.ID || other.StopID != "" {
		t.Fatalf("peer received a stop request: %+v", other)
	}

	response = sendRequest(handler, http.MethodPost, path, "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("second stop: %d %s", response.Code, response.Body.String())
	}
	next := heartbeatWorker(t, handler, workerA, nil).Assignments[0]
	if next.StopID == "" || next.StopID == stopped.StopID {
		t.Fatalf("new stop reused the previous request: %+v", next)
	}
}

func TestStopInstanceChecksServiceMembership(t *testing.T) {
	handler := New(10 * time.Second).Handler()
	service := createService(t, handler, api.ServiceSpec{Name: "clock", Command: "clock", Replicas: 1})
	createService(t, handler, api.ServiceSpec{Name: "other", Command: "clock", Replicas: 1})
	id := service.Instances[0].ID
	for _, path := range []string{
		"/services/missing/instances/" + id + "/stop",
		"/services/clock/instances/missing/stop",
		"/services/other/instances/" + id + "/stop",
	} {
		response := sendRequest(handler, http.MethodPost, path, "")
		if response.Code != http.StatusNotFound {
			t.Fatalf("unknown target %s: %d %s", path, response.Code, response.Body.String())
		}
	}
	response := sendRequest(handler, http.MethodPost, "/services/clock/instances/"+id+"/stop", "")
	if response.Code != http.StatusConflict {
		t.Fatalf("unassigned instance: %d %s", response.Code, response.Body.String())
	}
	if listServices(t, handler)[0].Instances[0].StopID != "" {
		t.Fatal("rejected request created a stop action")
	}
}

func TestStopInstanceRequiresRunningReachableWorker(t *testing.T) {
	for _, test := range []struct {
		name    string
		status  string
		advance time.Duration
	}{
		{name: "pending"},
		{name: "failed", status: "failed"},
		{name: "unreachable", status: "running", advance: 10 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
			server := New(10 * time.Second)
			server.now = func() time.Time { return now }
			handler := server.Handler()
			worker := registerWorker(t, handler, "worker-a")
			service := createService(t, handler, api.ServiceSpec{Name: "clock", Command: "clock", Replicas: 1})
			id := service.Instances[0].ID
			if test.status != "" {
				report := api.InstanceStatus{ID: id, Status: test.status}
				if test.status == "running" {
					report.PID = 123
				}
				heartbeatWorker(t, handler, worker, []api.InstanceStatus{report})
			}
			now = now.Add(test.advance)
			response := sendRequest(handler, http.MethodPost, "/services/clock/instances/"+id+"/stop", "")
			if response.Code != http.StatusConflict {
				t.Fatalf("unavailable instance: %d %s", response.Code, response.Body.String())
			}
			if listServices(t, handler)[0].Instances[0].StopID != "" {
				t.Fatal("rejected request created a stop action")
			}
		})
	}
}
