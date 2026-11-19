package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fabrenetes/collections"
	"fabrenetes/internal/api"
	"fabrenetes/internal/coordinator"
)

func TestWorkflowApplicationProcess(t *testing.T) {
	if os.Getenv("test_workflow_process") != "1" {
		return
	}
	dictionary, err := collections.New("")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/value", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		switch r.Method {
		case http.MethodGet:
			var value json.RawMessage
			found, err := dictionary.Get(r.Context(), key, &value)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			if !found {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(value)
		case http.MethodPut:
			value, err := io.ReadAll(r.Body)
			if err != nil || !json.Valid(value) {
				http.Error(w, "valid JSON is required", http.StatusBadRequest)
				return
			}
			err = dictionary.Put(r.Context(), key, json.RawMessage(value))
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			err := dictionary.Delete(r.Context(), key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("App-Worker", os.Getenv("worker_id"))
		mux.ServeHTTP(w, r)
	})
	err = http.ListenAndServe(":"+os.Getenv("service_port"), handler)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPartitionedServiceWorkflow(t *testing.T) {
	t.Setenv("test_workflow_process", "1")
	directory := t.TempDir()
	current := coordinator.New(time.Second)
	err := current.ConfigureData(directory)
	if err != nil {
		t.Fatal(err)
	}
	var handler atomic.Value
	handler.Store(current.Handler())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.Load().(http.Handler).ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	type runningWorker struct {
		config Config
		cancel context.CancelFunc
		done   <-chan error
	}
	workers := make([]runningWorker, 3)
	t.Cleanup(func() {
		for _, worker := range workers {
			if worker.cancel != nil {
				worker.cancel()
			}
		}
		for _, worker := range workers {
			if worker.done != nil {
				err := awaitWorker(t, worker.done)
				if err != nil {
					t.Error(err)
				}
			}
		}
	})
	for i := range workers {
		config := testConfig(server.URL)
		config.ID = fmt.Sprintf("workflow-worker-%d", i)
		config.DataDir = t.TempDir()
		config.StateListen = "127.0.0.1:0"
		config.HeartbeatInterval = 20 * time.Millisecond
		workers[i].config = config
		workers[i].cancel, workers[i].done = startWorker(t, config)
	}
	var registered api.WorkersResponse
	waitForState(t, "workflow workers to register", func() bool {
		stateRequest(t, http.MethodGet, server.URL+"/workers", nil, http.StatusOK, &registered)
		return len(registered.Workers) == 3
	})
	for i := range workers {
		for _, worker := range registered.Workers {
			if worker.ID == workers[i].config.ID {
				workers[i].config.StateListen = worker.StateAddress
			}
		}
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	err = listener.Close()
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	spec := api.ServiceSpec{
		Name: "notes", Command: executable, Args: []string{"-test.run=^TestWorkflowApplicationProcess$"},
		Replicas: 3, Port: port, Stateful: true, Partitioning: "hash", Partitions: 2,
	}
	var created api.Service
	stateRequest(t, http.MethodPost, server.URL+"/services", spec, http.StatusCreated, &created)
	if created.State == nil || created.State.PartitionCount() != 2 || created.Endpoint != "/apps/notes/" {
		t.Fatalf("unexpected partitioned service: %+v", created)
	}
	ready := func() api.Service {
		var service api.Service
		waitForState(t, "application endpoints and partition reports", func() bool {
			service = stateService(t, server.URL)
			if !allStateInstancesRunning(service) || len(service.PartitionStatus) != 2 {
				return false
			}
			for _, partition := range service.PartitionStatus {
				if partition.Status != "available" || partition.LeaderID == "" {
					return false
				}
			}
			for _, instance := range service.Instances {
				status, headers, _, requestErr := workflowRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/health", instance.Port), nil)
				if requestErr != nil || status != http.StatusOK || headers.Get("App-Worker") != instance.WorkerID {
					return false
				}
			}
			return true
		})
		return service
	}
	before := ready()
	keys := make([]string, 2)
	for i := 0; keys[0] == "" || keys[1] == ""; i++ {
		key := fmt.Sprintf("message / %d", i)
		keys[created.State.PartitionFor(key)] = key
	}
	endpoint := func(key string) string {
		return server.URL + created.Endpoint + "value?key=" + url.QueryEscape(key)
	}
	for i, key := range keys {
		workflowPut(t, endpoint(key), fmt.Sprintf(`"value-%d"`, i))
	}
	seen := make(map[string]bool)
	for i := range 6 {
		status, headers, body, err := workflowRequest(http.MethodGet, endpoint(keys[i%2]), nil)
		if err != nil {
			t.Fatal(err)
		}
		if status != http.StatusOK || string(body) != fmt.Sprintf(`"value-%d"`, i%2) {
			t.Fatalf("balanced read returned %d: %s", status, body)
		}
		workerID := headers.Get("Fabrenetes-Worker")
		if workerID == "" || workerID != headers.Get("App-Worker") {
			t.Fatalf("proxy selected a different application: %+v", headers)
		}
		seen[workerID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("requests did not reach all three workers: %v", seen)
	}
	leaderID := before.PartitionStatus[0].LeaderID
	stopped := -1
	for i, worker := range workers {
		if worker.config.ID == leaderID {
			stopped = i
		}
	}
	if stopped < 0 {
		t.Fatalf("leader %q is not one of the workers", leaderID)
	}
	workers[stopped].cancel()
	err = awaitWorker(t, workers[stopped].done)
	workers[stopped].cancel = nil
	workers[stopped].done = nil
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, "stopped worker to become unreachable", func() bool {
		for _, instance := range stateService(t, server.URL).Instances {
			if instance.WorkerID == leaderID {
				return instance.Status == "unreachable"
			}
		}
		return false
	})
	for i, key := range keys {
		workflowPut(t, endpoint(key), fmt.Sprintf(`"after-failure-%d"`, i))
		workflowValue(t, endpoint(key), fmt.Sprintf(`"after-failure-%d"`, i))
	}
	for range 3 {
		status, headers, _, err := workflowRequest(http.MethodGet, endpoint(keys[0]), nil)
		if err != nil || status != http.StatusOK || headers.Get("Fabrenetes-Worker") == leaderID {
			t.Fatalf("proxy routed to stopped worker: status=%d, headers=%v, err=%v", status, headers, err)
		}
	}
	workers[stopped].cancel, workers[stopped].done = startWorker(t, workers[stopped].config)
	before = ready()
	for i, key := range keys {
		workflowValue(t, endpoint(key), fmt.Sprintf(`"after-failure-%d"`, i))
		local := "http://" + workers[stopped].config.StateListen + "/state/" + created.State.ID + "/values?key=" + url.QueryEscape(key)
		workflowValue(t, local, fmt.Sprintf(`"after-failure-%d"`, i))
	}
	restored := coordinator.New(time.Second)
	err = restored.ConfigureData(directory)
	if err != nil {
		t.Fatal(err)
	}
	handler.Store(restored.Handler())
	after := ready()
	if !reflect.DeepEqual(before.State, after.State) {
		t.Fatalf("coordinator restart changed the dictionary layout: before=%+v, after=%+v", before.State, after.State)
	}
	for i, instance := range before.Instances {
		if instance.ID != after.Instances[i].ID || instance.PID != after.Instances[i].PID {
			t.Fatalf("coordinator restart replaced an application: before=%+v, after=%+v", before.Instances, after.Instances)
		}
	}
	for i, key := range keys {
		workflowValue(t, endpoint(key), fmt.Sprintf(`"after-failure-%d"`, i))
	}
	stateRequest(t, http.MethodDelete, server.URL+"/services/notes", nil, http.StatusNoContent, nil)
	waitForState(t, "deleted workflow applications to stop", func() bool {
		for _, instance := range after.Instances {
			if syscall.Kill(instance.PID, 0) != syscall.ESRCH {
				return false
			}
		}
		return true
	})
	stateRequest(t, http.MethodPost, server.URL+"/services", spec, http.StatusCreated, &created)
	if created.State.ID == before.State.ID {
		t.Fatal("recreated service reused its old dictionary")
	}
	ready()
	status, _, body, err := workflowRequest(http.MethodGet, endpoint(keys[0]), nil)
	if err != nil || status != http.StatusNotFound {
		t.Fatalf("recreated service retained deleted state: status=%d, body=%s, err=%v", status, body, err)
	}
}

func workflowRequest(method, endpoint string, body []byte) (int, http.Header, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return response.StatusCode, response.Header, data, err
}

func workflowPut(t *testing.T, endpoint, value string) {
	t.Helper()
	waitForState(t, "workflow dictionary write", func() bool {
		status, _, body, err := workflowRequest(http.MethodPut, endpoint, []byte(value))
		if err != nil || status == http.StatusServiceUnavailable || status == http.StatusBadGateway {
			return false
		}
		if status != http.StatusNoContent {
			t.Fatalf("write returned %d: %s", status, body)
		}
		return true
	})
}

func workflowValue(t *testing.T, endpoint, expected string) {
	t.Helper()
	waitForState(t, "workflow dictionary value", func() bool {
		status, _, body, err := workflowRequest(http.MethodGet, endpoint, nil)
		if err != nil || status == http.StatusServiceUnavailable || status == http.StatusBadGateway {
			return false
		}
		if status != http.StatusOK || string(body) != expected {
			t.Fatalf("acknowledged value changed: status=%d, value=%s, want=%s", status, body, expected)
		}
		return true
	})
}
