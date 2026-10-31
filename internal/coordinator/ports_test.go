package coordinator

import (
	"net/http"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestInstanceReportsPreserveActualPorts(t *testing.T) {
	for _, test := range []struct {
		name      string
		preferred int
		ports     [2]int
	}{
		{name: "different ports", preferred: 9001, ports: [2]int{9001, 9002}},
		{name: "unspecified", ports: [2]int{0, 0}},
		{name: "boundaries", preferred: 1, ports: [2]int{1, 65535}},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := New(10 * time.Second).Handler()
			worker := registerWorker(t, handler, "worker-a")
			service := createService(t, handler, api.ServiceSpec{
				Name: "clock", Command: "clock", Replicas: 2, Port: test.preferred,
			})
			reports := make([]api.InstanceStatus, len(service.Instances))
			for i, instance := range service.Instances {
				reports[i] = api.InstanceStatus{
					ID: instance.ID, PID: 123 + i, Port: test.ports[i], Status: "running",
				}
			}
			response := heartbeatWorker(t, handler, worker, reports)
			if len(response.Assignments) != 2 {
				t.Fatalf("unexpected assignments: %+v", response.Assignments)
			}
			for _, assignment := range response.Assignments {
				if assignment.Port != test.preferred {
					t.Fatalf("assignment port = %d; want preferred port %d", assignment.Port, test.preferred)
				}
			}
			current := listServices(t, handler)[0]
			if current.Port != test.preferred {
				t.Fatalf("service port = %d; want preferred port %d", current.Port, test.preferred)
			}
			for i, instance := range current.Instances {
				if instance.ID != reports[i].ID || instance.Port != test.ports[i] {
					t.Fatalf("instance report lost its actual port: %+v", instance)
				}
			}
		})
	}
}

func TestInvalidPortReportsDoNotPartiallyUpdateState(t *testing.T) {
	for _, port := range []int{-1, 65536} {
		now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
		server := New(10 * time.Second)
		server.now = func() time.Time { return now }
		handler := server.Handler()
		worker := registerWorker(t, handler, "worker-a")
		service := createService(t, handler, api.ServiceSpec{
			Name: "clock", Command: "clock", Replicas: 2, Port: 9001,
		})
		reports := []api.InstanceStatus{
			{ID: service.Instances[0].ID, PID: 123, Port: 9001, Status: "running"},
			{ID: service.Instances[1].ID, PID: 456, Port: 9002, Status: "running"},
		}
		heartbeatWorker(t, handler, worker, reports)
		before := listServices(t, handler)[0]
		lastSeen := server.workers[worker.ID].LastSeen
		now = now.Add(time.Second)
		reports[0].PID = 999
		reports[0].Port = 9003
		reports[1].Port = port
		response := sendJSON(t, handler, http.MethodPost, "/workers/worker-a/heartbeat", api.HeartbeatRequest{
			SessionID: worker.SessionID, Instances: reports,
		})
		if response.Code != http.StatusBadRequest {
			t.Fatalf("port %d accepted: %d %s", port, response.Code, response.Body.String())
		}
		if !server.workers[worker.ID].LastSeen.Equal(lastSeen) {
			t.Fatal("invalid port refreshed worker liveness")
		}
		current := listServices(t, handler)[0]
		for i, instance := range current.Instances {
			if instance.InstanceStatus != before.Instances[i].InstanceStatus {
				t.Fatalf("invalid port partly updated instance state: %+v", current.Instances)
			}
		}
	}
}
