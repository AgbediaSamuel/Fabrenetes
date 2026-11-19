package coordinator

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestStatefulServicesUseThreeDistinctWorkers(t *testing.T) {
	server := New(10 * time.Second)
	handler := server.Handler()
	workers := []api.Worker{
		registerStateWorker(t, handler, "worker-a", "127.0.0.1:9101"),
		registerStateWorker(t, handler, "worker-b", "127.0.0.1:9102"),
		registerStateWorker(t, handler, "worker-c", "127.0.0.1:9103"),
		registerStateWorker(t, handler, "worker-d", "127.0.0.1:9104"),
	}
	createService(t, handler, api.ServiceSpec{Name: "busy", Command: "busy", Replicas: 2})
	service := createService(t, handler, api.ServiceSpec{Name: "dictionary", Command: "dictionary", Replicas: 3, Stateful: true})
	if service.State == nil || service.State.ID == "" || len(service.State.Members) != 3 {
		t.Fatalf("state group missing: %+v", service)
	}
	wantWorkers := []string{"worker-c", "worker-d", "worker-a"}
	for i, instance := range service.Instances {
		if instance.WorkerID != wantWorkers[i] || service.State.Members[i].ID != instance.WorkerID {
			t.Fatalf("unexpected stateful placement: %+v", service)
		}
	}
	for _, worker := range workers {
		for _, assignment := range heartbeatWorker(t, handler, worker, nil).Assignments {
			if assignment.Service == service.Name && !reflect.DeepEqual(assignment.State, service.State) {
				t.Fatalf("assignment lost group: %+v", assignment)
			}
		}
	}
	before := copyServices(server.services)
	registerStateWorker(t, handler, "worker-e", "127.0.0.1:9105")
	if !reflect.DeepEqual(server.services[service.Name].Instances, before[service.Name].Instances) {
		t.Fatal("joining worker changed fixed stateful membership")
	}
}

func TestStatefulServicesRequireThreeAvailableStateWorkers(t *testing.T) {
	server := New(10 * time.Second)
	handler := server.Handler()
	registerWorker(t, handler, "plain-worker")
	registerStateWorker(t, handler, "worker-a", "127.0.0.1:9101")
	registerStateWorker(t, handler, "worker-b", "127.0.0.1:9102")
	for _, replicas := range []int{1, 2, 4} {
		response := sendJSON(t, handler, http.MethodPost, "/services", api.ServiceSpec{Name: "dictionary", Command: "dictionary", Replicas: replicas, Stateful: true})
		if response.Code != http.StatusBadRequest {
			t.Fatalf("accepted stateful replica count %d: %d %s", replicas, response.Code, response.Body.String())
		}
	}
	response := sendJSON(t, handler, http.MethodPost, "/services", api.ServiceSpec{Name: "dictionary", Command: "dictionary", Replicas: 3, Stateful: true})
	if response.Code != http.StatusConflict || len(server.services) != 0 {
		t.Fatalf("accepted an incomplete state group: %d %s", response.Code, response.Body.String())
	}
	registerStateWorker(t, handler, "same-endpoint", "127.0.0.1:9102")
	response = sendJSON(t, handler, http.MethodPost, "/services", api.ServiceSpec{Name: "dictionary", Command: "dictionary", Replicas: 3, Stateful: true})
	if response.Code != http.StatusConflict {
		t.Fatal("two worker IDs sharing one endpoint counted as distinct replicas")
	}
	registerStateWorker(t, handler, "worker-c", "127.0.0.1:9103")
	createService(t, handler, api.ServiceSpec{Name: "dictionary", Command: "dictionary", Replicas: 3, Stateful: true})
}

func TestSavedServicesKeepStateIdentityAcrossCoordinatorRestart(t *testing.T) {
	directory := t.TempDir()
	server := New(10 * time.Second)
	err := server.ConfigureData(directory)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	worker := registerStateWorker(t, handler, "worker-a", "127.0.0.1:9101")
	registerStateWorker(t, handler, "worker-b", "127.0.0.1:9102")
	registerStateWorker(t, handler, "worker-c", "127.0.0.1:9103")
	service := createService(t, handler, api.ServiceSpec{Name: "dictionary", Command: "dictionary", Replicas: 3, Stateful: true})
	createService(t, handler, api.ServiceSpec{Name: "plain", Command: "plain", Replicas: 1})
	heartbeatWorker(t, handler, worker, []api.InstanceStatus{{ID: service.Instances[0].ID, PID: 123, Status: "running"}})
	response := sendRequest(handler, http.MethodPost, "/services/dictionary/instances/"+service.Instances[0].ID+"/stop", "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("stop instance: %d %s", response.Code, response.Body.String())
	}
	stopID := server.services[service.Name].Instances[0].StopID
	restarted := New(10 * time.Second)
	err = restarted.ConfigureData(directory)
	if err != nil {
		t.Fatal(err)
	}
	restored := restarted.services[service.Name]
	if len(restarted.services) != 2 || len(restarted.workers) != 0 || !reflect.DeepEqual(restored.State, service.State) {
		t.Fatalf("incorrect coordinator recovery: %+v", restored)
	}
	for i, instance := range restored.Instances {
		if instance.ID != service.Instances[i].ID || instance.WorkerID != service.Instances[i].WorkerID || instance.PID != 0 || instance.Status != "unknown" {
			t.Fatalf("incorrect restored instance: %+v", instance)
		}
	}
	if stopID == "" || restored.Instances[0].StopID != stopID {
		t.Fatal("coordinator restart changed the last stop request")
	}
	worker = registerStateWorker(t, restarted.Handler(), worker.ID, worker.StateAddress)
	assignments := heartbeatWorker(t, restarted.Handler(), worker, nil).Assignments
	var stateAssignment api.Assignment
	for _, assignment := range assignments {
		if assignment.Service == service.Name {
			stateAssignment = assignment
		}
	}
	if len(assignments) != 2 || stateAssignment.ID != service.Instances[0].ID || !reflect.DeepEqual(stateAssignment.State, service.State) {
		t.Fatalf("restored assignments changed: %+v", assignments)
	}
	response = sendRequest(restarted.Handler(), http.MethodDelete, "/services/dictionary", "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete restored service: %d %s", response.Code, response.Body.String())
	}
	deleted := New(10 * time.Second)
	err = deleted.ConfigureData(directory)
	if err != nil {
		t.Fatal(err)
	}
	_, exists := deleted.services[service.Name]
	if exists {
		t.Fatal("deleted service returned after restart")
	}
}

func TestServiceStorageFailuresDoNotPublishChanges(t *testing.T) {
	directory := t.TempDir()
	server := New(10 * time.Second)
	err := server.ConfigureData(directory)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	service := createService(t, handler, api.ServiceSpec{Name: "pending", Command: "pending", Replicas: 1})
	before, err := os.ReadFile(filepath.Join(directory, "services.json"))
	if err != nil {
		t.Fatal(err)
	}
	server.dataDir = filepath.Join(directory, "missing")
	response := sendJSON(t, handler, http.MethodPost, "/services", api.ServiceSpec{Name: "other", Command: "other", Replicas: 1})
	if response.Code != http.StatusInternalServerError || len(server.services) != 1 {
		t.Fatalf("failed create changed services: %d %+v", response.Code, server.services)
	}
	response = sendRequest(handler, http.MethodDelete, "/services/pending", "")
	if response.Code != http.StatusInternalServerError || len(server.services) != 1 {
		t.Fatalf("failed delete changed services: %d %+v", response.Code, server.services)
	}
	response = sendJSON(t, handler, http.MethodPost, "/workers", api.RegisterRequest{ID: "worker-a", Hostname: "worker-a"})
	if response.Code != http.StatusInternalServerError || len(server.workers) != 0 || server.services[service.Name].Instances[0].WorkerID != "" {
		t.Fatalf("failed placement changed assignments: %d %+v", response.Code, server.services)
	}
	after, err := os.ReadFile(filepath.Join(directory, "services.json"))
	if err != nil || string(before) != string(after) {
		t.Fatal("failed mutation changed saved services")
	}
	server.dataDir = directory
	registerWorker(t, handler, "worker-a")
	restored := New(10 * time.Second)
	err = restored.ConfigureData(directory)
	if err != nil {
		t.Fatal(err)
	}
	if restored.services[service.Name].Instances[0].WorkerID != "worker-a" || restored.services[service.Name].Instances[0].ID != service.Instances[0].ID {
		t.Fatal("successful placement was not persisted")
	}
}

func TestHeartbeatPlacementStorageFailureDoesNotPublishReports(t *testing.T) {
	server := New(10 * time.Second)
	directory := t.TempDir()
	err := server.ConfigureData(directory)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	handler := server.Handler()
	worker := registerWorker(t, handler, "worker-a")
	now = now.Add(10 * time.Second)
	service := createService(t, handler, api.ServiceSpec{Name: "pending", Command: "pending", Replicas: 1})
	server.dataDir = filepath.Join(directory, "missing")
	response := sendJSON(t, handler, http.MethodPost, "/workers/worker-a/heartbeat", api.HeartbeatRequest{SessionID: worker.SessionID})
	if response.Code != http.StatusInternalServerError || server.services[service.Name].Instances[0].WorkerID != "" || !server.workers[worker.ID].LastSeen.Equal(worker.LastSeen) {
		t.Fatalf("failed heartbeat published placement: %d %+v", response.Code, server.services)
	}
	server.dataDir = directory
	assignment := heartbeatWorker(t, handler, worker, nil).Assignments[0]
	if assignment.ID != service.Instances[0].ID {
		t.Fatal("placement retry replaced the instance identity")
	}
	server.dataDir = filepath.Join(directory, "missing")
	result := heartbeatWorker(t, handler, worker, []api.InstanceStatus{{ID: assignment.ID, Status: "running", PID: 123}})
	if len(result.Assignments) != 1 {
		t.Fatal("ordinary heartbeat unnecessarily required a disk write")
	}
}

func TestStateWorkerAddressesStayFixedWhileAssigned(t *testing.T) {
	server := New(10 * time.Second)
	handler := server.Handler()
	worker := registerStateWorker(t, handler, "worker-a", "127.0.0.1:9101")
	registerStateWorker(t, handler, "worker-b", "127.0.0.1:9102")
	registerStateWorker(t, handler, "worker-c", "127.0.0.1:9103")
	createService(t, handler, api.ServiceSpec{Name: "dictionary", Command: "dictionary", Replicas: 3, Stateful: true})
	for _, address := range []string{"", "127.0.0.1:9201"} {
		response := sendJSON(t, handler, http.MethodPost, "/workers", api.RegisterRequest{ID: worker.ID, Hostname: worker.Hostname, StateAddress: address})
		if response.Code != http.StatusConflict || server.workers[worker.ID].SessionID != worker.SessionID {
			t.Fatalf("changed membership address: %d %s", response.Code, response.Body.String())
		}
	}
	for _, address := range []string{"localhost", "localhost:0", "0.0.0.0:9091", "http://localhost:9091", "localhost:65536"} {
		response := sendJSON(t, handler, http.MethodPost, "/workers", api.RegisterRequest{ID: "invalid", Hostname: "invalid", StateAddress: address})
		if response.Code != http.StatusBadRequest {
			t.Fatalf("accepted invalid state address %q: %d", address, response.Code)
		}
	}
}

func TestInvalidSavedServicesDoNotStartAnEmptyCluster(t *testing.T) {
	directory := t.TempDir()
	err := os.WriteFile(filepath.Join(directory, "services.json"), []byte(`{"dictionary":{"name":"dictionary","command":"dictionary","replicas":3,"stateful":true,"instances":[]}}`), 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = New(10 * time.Second).ConfigureData(directory)
	if err == nil {
		t.Fatal("invalid saved state was silently discarded")
	}
}

func registerStateWorker(t *testing.T, handler http.Handler, id, address string) api.Worker {
	t.Helper()
	response := sendJSON(t, handler, http.MethodPost, "/workers", api.RegisterRequest{ID: id, Hostname: id, StateAddress: address})
	if response.Code != http.StatusCreated {
		t.Fatalf("register state worker: %d %s", response.Code, response.Body.String())
	}
	var worker api.Worker
	err := json.Unmarshal(response.Body.Bytes(), &worker)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}
