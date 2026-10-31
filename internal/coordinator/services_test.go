package coordinator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestServicePlacementReportsAndDeletion(t *testing.T) {
	server := New(10 * time.Second)
	handler := server.Handler()
	workerB := registerWorker(t, handler, "worker-b")
	workerA := registerWorker(t, handler, "worker-a")
	service := createService(t, handler, api.ServiceSpec{Name: "echo", Command: "/bin/echo", Replicas: 2, Port: 9001})
	if len(service.Instances) != 2 || service.Instances[0].WorkerID != workerA.ID || service.Instances[1].WorkerID != workerB.ID {
		t.Fatalf("unexpected placement: %+v", service.Instances)
	}
	for _, worker := range []api.Worker{workerA, workerB} {
		first := heartbeatWorker(t, handler, worker, nil)
		second := heartbeatWorker(t, handler, worker, nil)
		if len(first.Assignments) != 1 || len(second.Assignments) != 1 || first.Assignments[0].ID != second.Assignments[0].ID {
			t.Fatalf("heartbeat changed assignments: %+v %+v", first, second)
		}
		assignment := first.Assignments[0]
		if assignment.Service != "echo" || assignment.Command != "/bin/echo" || assignment.Port != 9001 {
			t.Fatalf("unexpected executable assignment: %+v", assignment)
		}
		heartbeatWorker(t, handler, worker, []api.InstanceStatus{{ID: assignment.ID, PID: 123, Status: "running", Restarts: 1}})
	}
	current := listServices(t, handler)
	if current[0].Port != 9001 {
		t.Fatalf("service port was not stored: %+v", current[0])
	}
	for _, instance := range current[0].Instances {
		if instance.Status != "running" || instance.PID != 123 || instance.Restarts != 1 {
			t.Fatalf("process report was not stored: %+v", instance)
		}
	}
	duplicate := sendJSON(t, handler, http.MethodPost, "/services", service.ServiceSpec)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate service: %d %s", duplicate.Code, duplicate.Body.String())
	}
	response := sendRequest(handler, http.MethodDelete, "/services/echo", "")
	if response.Code != http.StatusNoContent || len(listServices(t, handler)) != 0 {
		t.Fatalf("delete service: %d %s", response.Code, response.Body.String())
	}
	for i, worker := range []api.Worker{workerA, workerB} {
		result := heartbeatWorker(t, handler, worker, []api.InstanceStatus{{ID: service.Instances[i].ID, PID: 123, Status: "running"}})
		if len(result.Assignments) != 0 {
			t.Fatalf("deleted service is still assigned: %+v", result.Assignments)
		}
	}
}

func TestPendingServiceAndWorkerSessionChanges(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	server := New(10 * time.Second)
	server.now = func() time.Time { return now }
	handler := server.Handler()
	service := createService(t, handler, api.ServiceSpec{Name: "echo", Command: "/bin/echo", Replicas: 1})
	if service.Instances[0].WorkerID != "" || service.Instances[0].Status != "pending" {
		t.Fatalf("service with no workers should wait: %+v", service)
	}
	worker := registerWorker(t, handler, "worker-a")
	assignment := heartbeatWorker(t, handler, worker, nil).Assignments[0]
	if assignment.ID != service.Instances[0].ID {
		t.Fatal("joining worker created another instance")
	}
	heartbeatWorker(t, handler, worker, []api.InstanceStatus{{ID: assignment.ID, PID: 123, Status: "running"}})
	now = now.Add(10 * time.Second)
	current := listServices(t, handler)[0].Instances[0]
	if current.Status != "unreachable" || current.WorkerID != worker.ID || current.ID != assignment.ID {
		t.Fatalf("expired worker should retain uncertain assignment: %+v", current)
	}
	registerWorker(t, handler, "worker-b")
	current = listServices(t, handler)[0].Instances[0]
	if current.WorkerID != worker.ID {
		t.Fatalf("assignment moved away from unreachable worker: %+v", current)
	}
	restarted := registerWorker(t, handler, worker.ID)
	current = listServices(t, handler)[0].Instances[0]
	if current.Status != "unknown" || current.PID != 0 || current.ID != assignment.ID {
		t.Fatalf("previous process state survived session replacement: %+v", current)
	}
	response := sendJSON(t, handler, http.MethodPost, "/workers/worker-a/heartbeat", api.HeartbeatRequest{
		SessionID: worker.SessionID, Instances: []api.InstanceStatus{{ID: assignment.ID, PID: 999, Status: "running"}},
	})
	if response.Code != http.StatusConflict {
		t.Fatalf("old session updated process state: %d %s", response.Code, response.Body.String())
	}
	heartbeatWorker(t, handler, restarted, []api.InstanceStatus{{ID: assignment.ID, Status: "failed", Error: "executable not found"}})
	current = listServices(t, handler)[0].Instances[0]
	if current.Status != "failed" || current.Error != "executable not found" {
		t.Fatalf("failed process is not inspectable: %+v", current)
	}
}

func TestInstanceReportsValidateBeforeUpdatingState(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	server := New(10 * time.Second)
	server.now = func() time.Time { return now }
	handler := server.Handler()
	workerA := registerWorker(t, handler, "worker-a")
	registerWorker(t, handler, "worker-b")
	service := createService(t, handler, api.ServiceSpec{Name: "echo", Command: "/bin/echo", Replicas: 2})
	own := api.InstanceStatus{ID: service.Instances[0].ID, PID: 123, Status: "running"}
	foreign := api.InstanceStatus{ID: service.Instances[1].ID, PID: 999, Status: "running"}
	now = now.Add(time.Second)
	for _, reports := range [][]api.InstanceStatus{{own, foreign}, {own, own}, {{ID: own.ID, Status: "running"}}} {
		response := sendJSON(t, handler, http.MethodPost, "/workers/worker-a/heartbeat", api.HeartbeatRequest{SessionID: workerA.SessionID, Instances: reports})
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid reports accepted: %d %s", response.Code, response.Body.String())
		}
		if !server.workers[workerA.ID].LastSeen.Equal(workerA.LastSeen) {
			t.Fatal("invalid heartbeat refreshed worker liveness")
		}
		for _, instance := range listServices(t, handler)[0].Instances {
			if instance.Status != "pending" || instance.PID != 0 {
				t.Fatalf("invalid heartbeat partly updated state: %+v", instance)
			}
		}
	}
}

func TestServiceRequiresPositiveReplicaCount(t *testing.T) {
	handler := New(10 * time.Second).Handler()
	for _, body := range []string{
		`{"name":"echo","command":"/bin/echo"}`,
		`{"name":".","command":"/bin/echo","replicas":1}`,
		`{"name":"missing-command","replicas":1}`,
		`{"name":"zero","command":"/bin/echo","replicas":0}`,
		`{"name":"negative","command":"/bin/echo","replicas":-1}`,
	} {
		response := sendRequest(handler, http.MethodPost, "/services", body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid service accepted: %d %s", response.Code, response.Body.String())
		}
	}
	if len(listServices(t, handler)) != 0 {
		t.Fatal("invalid service created instances")
	}
	service := createService(t, handler, api.ServiceSpec{Name: "echo", Command: "/bin/echo", Replicas: 1})
	if service.Replicas != 1 || len(service.Instances) != 1 {
		t.Fatalf("unexpected service: %+v", service)
	}
}

func TestServicePortValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		port   int
		status int
	}{
		{name: "unspecified", port: 0, status: http.StatusCreated},
		{name: "lowest", port: 1, status: http.StatusCreated},
		{name: "highest", port: 65535, status: http.StatusCreated},
		{name: "negative", port: -1, status: http.StatusBadRequest},
		{name: "above range", port: 65536, status: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := New(10 * time.Second).Handler()
			worker := registerWorker(t, handler, "worker-a")
			response := sendJSON(t, handler, http.MethodPost, "/services", api.ServiceSpec{
				Name: "clock", Command: "clock", Replicas: 1, Port: test.port,
			})
			if response.Code != test.status {
				t.Fatalf("create service: %d %s", response.Code, response.Body.String())
			}
			services := listServices(t, handler)
			assignments := heartbeatWorker(t, handler, worker, nil).Assignments
			if test.status == http.StatusBadRequest {
				if len(services) != 0 || len(assignments) != 0 {
					t.Fatal("invalid port created a service or assignment")
				}
				return
			}
			if len(services) != 1 || services[0].Port != test.port {
				t.Fatalf("service port not preserved: %+v", services)
			}
			if len(assignments) != 1 || assignments[0].Port != test.port {
				t.Fatalf("assignment port not preserved: %+v", assignments)
			}
		})
	}
}

func createService(t *testing.T, handler http.Handler, spec api.ServiceSpec) api.Service {
	t.Helper()
	response := sendJSON(t, handler, http.MethodPost, "/services", spec)
	if response.Code != http.StatusCreated {
		t.Fatalf("create service: %d %s", response.Code, response.Body.String())
	}
	var service api.Service
	err := json.Unmarshal(response.Body.Bytes(), &service)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func listServices(t *testing.T, handler http.Handler) []api.Service {
	t.Helper()
	response := sendRequest(handler, http.MethodGet, "/services", "")
	if response.Code != http.StatusOK {
		t.Fatalf("list services: %d %s", response.Code, response.Body.String())
	}
	var result api.ServicesResponse
	err := json.Unmarshal(response.Body.Bytes(), &result)
	if err != nil {
		t.Fatal(err)
	}
	return result.Services
}

func heartbeatWorker(t *testing.T, handler http.Handler, worker api.Worker, instances []api.InstanceStatus) api.HeartbeatResponse {
	t.Helper()
	response := sendJSON(t, handler, http.MethodPost, "/workers/"+worker.ID+"/heartbeat", api.HeartbeatRequest{SessionID: worker.SessionID, Instances: instances})
	if response.Code != http.StatusOK {
		t.Fatalf("heartbeat: %d %s", response.Code, response.Body.String())
	}
	var result api.HeartbeatResponse
	err := json.Unmarshal(response.Body.Bytes(), &result)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func sendJSON(t *testing.T, handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return sendRequest(handler, method, path, string(encoded))
}
