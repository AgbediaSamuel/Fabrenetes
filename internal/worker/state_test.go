package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fabrenetes/collections"
	"fabrenetes/internal/api"
	"fabrenetes/internal/coordinator"
)

type stateProcessInfo struct {
	PID int    `json:"pid"`
	URL string `json:"url"`
}

func TestManagedStateProcess(t *testing.T) {
	if os.Getenv("test_state_process") != "1" {
		return
	}
	_, err := collections.New("")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(stateProcessInfo{PID: os.Getpid(), URL: os.Getenv("state_url")})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(os.Args[len(os.Args)-1], os.Getenv("worker_id")+".json")
	err = os.WriteFile(path, data, 0600)
	if err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestStatefulServiceRetainsValuesAcrossRestarts(t *testing.T) {
	t.Setenv("test_state_process", "1")
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
		config.ID = fmt.Sprintf("state-worker-%d", i)
		config.DataDir = t.TempDir()
		config.StateListen = "127.0.0.1:0"
		cancel, done := startWorker(t, config)
		workers[i] = runningWorker{config: config, cancel: cancel, done: done}
	}
	var registered api.WorkersResponse
	waitForState(t, "three workers to register", func() bool {
		stateRequest(t, http.MethodGet, server.URL+"/workers", nil, http.StatusOK, &registered)
		return len(registered.Workers) == 3
	})
	for i := range workers {
		for _, registeredWorker := range registered.Workers {
			if registeredWorker.ID == workers[i].config.ID {
				workers[i].config.StateListen = registeredWorker.StateAddress
			}
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ready := t.TempDir()
	var created api.Service
	stateRequest(t, http.MethodPost, server.URL+"/services", api.ServiceSpec{
		Name: "dictionary", Command: executable, Replicas: 3, Stateful: true,
		Args: []string{"-test.run=^TestManagedStateProcess$", "--", ready},
	}, http.StatusCreated, &created)
	if created.State == nil || len(created.State.Members) != 3 {
		t.Fatalf("service has no three-member state group: %+v", created)
	}
	var processes [3]stateProcessInfo
	waitForState(t, "applications to receive their state URLs", func() bool {
		for i, worker := range workers {
			data, readErr := os.ReadFile(filepath.Join(ready, worker.config.ID+".json"))
			if readErr != nil {
				return false
			}
			decodeErr := json.Unmarshal(data, &processes[i])
			if decodeErr != nil {
				return false
			}
			expected := "http://" + worker.config.StateListen + "/state/" + created.State.ID
			if processes[i].URL != expected {
				t.Fatalf("application state URL = %q, want %q", processes[i].URL, expected)
			}
		}
		return true
	})
	dictionaries := make([]*collections.Dictionary, len(workers))
	for i, process := range processes {
		dictionaries[i], err = collections.New(process.URL)
		if err != nil {
			t.Fatal(err)
		}
	}
	putStateValue(t, dictionaries[0], "first")
	checkStateValue(t, dictionaries[1], "first")

	var before api.Service
	waitForState(t, "running application reports", func() bool {
		before = stateService(t, server.URL)
		return allStateInstancesRunning(before)
	})
	var stopped api.Instance
	for _, instance := range before.Instances {
		if instance.WorkerID == workers[0].config.ID {
			stopped = instance
		}
	}
	stateRequest(t, http.MethodPost, server.URL+"/services/dictionary/instances/"+stopped.ID+"/stop", nil, http.StatusAccepted, nil)
	waitForState(t, "application process replacement", func() bool {
		for _, instance := range stateService(t, server.URL).Instances {
			if instance.ID == stopped.ID {
				return instance.Status == "running" && instance.PID != stopped.PID && instance.Restarts == 1
			}
		}
		return false
	})
	checkStateValue(t, dictionaries[0], "first")

	workers[0].cancel()
	err = awaitWorker(t, workers[0].done)
	workers[0].done = nil
	workers[0].cancel = nil
	if err != nil {
		t.Fatal(err)
	}
	putStateValue(t, dictionaries[1], "written while one worker was stopped")
	checkStateValue(t, dictionaries[2], "written while one worker was stopped")
	workers[0].cancel, workers[0].done = startWorker(t, workers[0].config)
	checkStateValue(t, dictionaries[0], "written while one worker was stopped")
	waitForState(t, "restarted worker application", func() bool {
		before = stateService(t, server.URL)
		return allStateInstancesRunning(before)
	})

	restored := coordinator.New(time.Second)
	err = restored.ConfigureData(directory)
	if err != nil {
		t.Fatal(err)
	}
	handler.Store(restored.Handler())
	var after api.Service
	waitForState(t, "worker reports after coordinator restart", func() bool {
		after = stateService(t, server.URL)
		return allStateInstancesRunning(after)
	})
	if !reflect.DeepEqual(before.State, after.State) {
		t.Fatalf("coordinator restart changed the state group: before=%+v, after=%+v", before.State, after.State)
	}
	for i, instance := range before.Instances {
		if instance.ID != after.Instances[i].ID || instance.PID != after.Instances[i].PID {
			t.Fatalf("coordinator restart replaced an application: before=%+v, after=%+v", instance, after.Instances[i])
		}
	}
	checkStateValue(t, dictionaries[0], "written while one worker was stopped")
	stateRequest(t, http.MethodDelete, server.URL+"/services/dictionary", nil, http.StatusNoContent, nil)
	waitForState(t, "deleted application processes to stop", func() bool {
		for _, instance := range after.Instances {
			if syscall.Kill(instance.PID, 0) != syscall.ESRCH {
				return false
			}
		}
		return true
	})
}

func stateRequest(t *testing.T, method, address string, value any, status int, destination any) {
	t.Helper()
	var body []byte
	var err error
	if value != nil {
		body, err = json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, address, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("%s %s returned %d, want %d: %s", method, address, response.StatusCode, status, data)
	}
	if destination != nil {
		err = json.Unmarshal(data, destination)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func stateService(t *testing.T, address string) api.Service {
	t.Helper()
	var response api.ServicesResponse
	stateRequest(t, http.MethodGet, address+"/services", nil, http.StatusOK, &response)
	if len(response.Services) != 1 {
		t.Fatalf("expected one service: %+v", response)
	}
	return response.Services[0]
}

func allStateInstancesRunning(service api.Service) bool {
	for _, instance := range service.Instances {
		if instance.Status != "running" || instance.PID == 0 {
			return false
		}
	}
	return len(service.Instances) == 3
}

func waitForState(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func putStateValue(t *testing.T, dictionary *collections.Dictionary, value string) {
	t.Helper()
	waitForState(t, "dictionary write", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := dictionary.Put(ctx, "message", value)
		return err == nil
	})
}

func checkStateValue(t *testing.T, dictionary *collections.Dictionary, expected string) {
	t.Helper()
	waitForState(t, "replicated dictionary value", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var value string
		found, err := dictionary.Get(ctx, "message", &value)
		if err != nil {
			return false
		}
		if !found || value != expected {
			t.Fatalf("acknowledged value changed: found=%v, value=%q, want=%q", found, value, expected)
		}
		return true
	})
}
