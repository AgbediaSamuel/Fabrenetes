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

func TestPartitioningIsChosenAtCreationAndSaved(t *testing.T) {
	for _, test := range []struct {
		name         string
		stateful     bool
		strategy     string
		count        int
		status       int
		wantStrategy string
		wantCount    int
	}{
		{name: "single", stateful: true, status: http.StatusCreated, wantStrategy: "none", wantCount: 1},
		{name: "hash", stateful: true, strategy: "hash", count: 3, status: http.StatusCreated, wantStrategy: "hash", wantCount: 3},
		{name: "hash-one", stateful: true, strategy: "hash", count: 1, status: http.StatusCreated, wantStrategy: "hash", wantCount: 1},
		{name: "none-many", stateful: true, strategy: "none", count: 2, status: http.StatusBadRequest},
		{name: "hash-zero", stateful: true, strategy: "hash", status: http.StatusBadRequest},
		{name: "hash-negative", stateful: true, strategy: "hash", count: -1, status: http.StatusBadRequest},
		{name: "unknown", stateful: true, strategy: "range", count: 2, status: http.StatusBadRequest},
		{name: "stateless", strategy: "hash", count: 2, status: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := New(time.Minute)
			directory := t.TempDir()
			err := server.ConfigureData(directory)
			if err != nil {
				t.Fatal(err)
			}
			handler := server.Handler()
			workers := stateWorkersForTest(t, handler)
			response := sendJSON(t, handler, http.MethodPost, "/services", api.ServiceSpec{
				Name: "notes", Command: "notes", Replicas: 3, Stateful: test.stateful, Partitioning: test.strategy, Partitions: test.count,
			})
			if response.Code != test.status {
				t.Fatalf("create: %d %s", response.Code, response.Body.String())
			}
			if test.status != http.StatusCreated {
				return
			}
			var created api.Service
			err = json.Unmarshal(response.Body.Bytes(), &created)
			if err != nil {
				t.Fatal(err)
			}
			if created.Partitioning != test.wantStrategy || created.Partitions != test.wantCount || created.State.PartitionCount() != test.wantCount || created.State.Partitioning != test.wantStrategy || len(created.PartitionStatus) != test.wantCount {
				t.Fatalf("incorrect layout: %+v", created)
			}
			assignment := heartbeatWorker(t, handler, workers[0], nil).Assignments[0]
			if !reflect.DeepEqual(assignment.State, created.State) {
				t.Fatalf("assignment layout changed: %+v", assignment)
			}
			restarted := New(time.Minute)
			err = restarted.ConfigureData(directory)
			if err != nil {
				t.Fatal(err)
			}
			restored := listServices(t, restarted.Handler())[0]
			if !reflect.DeepEqual(restored.State, created.State) || restored.Partitions != test.wantCount || restored.Partitioning != test.wantStrategy {
				t.Fatalf("restored layout changed: %+v", restored)
			}
		})
	}
}

func TestUnpartitionedSavedServicesKeepTheirDataIdentity(t *testing.T) {
	server := New(time.Minute)
	handler := server.Handler()
	stateWorkersForTest(t, handler)
	service := createService(t, handler, api.ServiceSpec{Name: "notes", Command: "notes", Replicas: 3, Stateful: true})
	service.Partitioning = ""
	service.Partitions = 0
	service.State.Partitioning = ""
	service.State.Partitions = 0
	data, err := json.Marshal(map[string]api.Service{service.Name: service})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	err = os.WriteFile(filepath.Join(directory, "services.json"), data, 0600)
	if err != nil {
		t.Fatal(err)
	}
	restarted := New(time.Minute)
	err = restarted.ConfigureData(directory)
	if err != nil {
		t.Fatal(err)
	}
	restored := restarted.services[service.Name]
	if restored.State.ID != service.State.ID || restored.State.PartitionID(0) != service.State.ID || restored.Partitioning != "none" || restored.Partitions != 1 {
		t.Fatalf("legacy dictionary was remapped: %+v", restored)
	}
}

func stateWorkersForTest(t *testing.T, handler http.Handler) []api.Worker {
	t.Helper()
	return []api.Worker{
		registerStateWorker(t, handler, "worker-a", "127.0.0.1:9101"),
		registerStateWorker(t, handler, "worker-b", "127.0.0.1:9102"),
		registerStateWorker(t, handler, "worker-c", "127.0.0.1:9103"),
	}
}
