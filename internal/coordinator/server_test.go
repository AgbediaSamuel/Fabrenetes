package coordinator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestWorkerHeartbeatExpiryAndRecovery(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	server := New(10 * time.Second)
	server.now = func() time.Time { return now }
	handler := server.Handler()
	worker := registerWorker(t, handler, "worker-b")
	registerWorker(t, handler, "worker-a")
	if worker.SessionID == "" || worker.Status != "ready" || !worker.LastSeen.Equal(now) {
		t.Fatalf("unexpected registration: %+v", worker)
	}
	now = now.Add(8 * time.Second)
	response := sendRequest(handler, http.MethodPost, "/workers/worker-b/heartbeat", fmt.Sprintf(`{"session_id":%q}`, worker.SessionID))
	if response.Code != http.StatusOK {
		t.Fatalf("heartbeat: %d %s", response.Code, response.Body.String())
	}
	now = now.Add(2 * time.Second)
	workers := listWorkers(t, handler)
	if len(workers) != 2 || workers[0].ID != "worker-a" || workers[1].ID != "worker-b" {
		t.Fatalf("workers are not sorted: %+v", workers)
	}
	if workers[0].Status != "unreachable" || workers[1].Status != "ready" {
		t.Fatalf("heartbeat expiry was not reflected: %+v", workers)
	}
	response = sendRequest(handler, http.MethodPost, "/workers/worker-a/heartbeat", fmt.Sprintf(`{"session_id":%q}`, workers[0].SessionID))
	if response.Code != http.StatusOK || listWorkers(t, handler)[0].Status != "ready" {
		t.Fatalf("worker did not recover: %d %s", response.Code, response.Body.String())
	}
}

func TestRegistrationReplacesSession(t *testing.T) {
	server := New(10 * time.Second)
	handler := server.Handler()
	previous := registerWorker(t, handler, "worker-a")
	current := registerWorker(t, handler, "worker-a")
	if previous.SessionID == current.SessionID {
		t.Fatal("registration reused the previous session")
	}
	response := sendRequest(handler, http.MethodPost, "/workers/worker-a/heartbeat", fmt.Sprintf(`{"session_id":%q}`, previous.SessionID))
	if response.Code != http.StatusConflict {
		t.Fatalf("stale session: %d %s", response.Code, response.Body.String())
	}
	response = sendRequest(handler, http.MethodPost, "/workers/worker-a/heartbeat", fmt.Sprintf(`{"session_id":%q}`, current.SessionID))
	if response.Code != http.StatusOK || len(listWorkers(t, handler)) != 1 {
		t.Fatalf("current session failed: %d %s", response.Code, response.Body.String())
	}
	response = sendRequest(handler, http.MethodPost, "/workers/missing/heartbeat", fmt.Sprintf(`{"session_id":%q}`, current.SessionID))
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown worker: %d %s", response.Code, response.Body.String())
	}
}

func TestInvalidRequestsDoNotRegisterWorkers(t *testing.T) {
	server := New(10 * time.Second)
	handler := server.Handler()
	cases := []struct {
		name   string
		body   string
		status int
	}{
		{"empty", "", http.StatusBadRequest},
		{"malformed", `{"id":`, http.StatusBadRequest},
		{"missing hostname", `{"id":"worker-a"}`, http.StatusBadRequest},
		{"blank identity", `{"id":"  ","hostname":"host"}`, http.StatusBadRequest},
		{"dot identity", `{"id":".","hostname":"host"}`, http.StatusBadRequest},
		{"parent identity", `{"id":"..","hostname":"host"}`, http.StatusBadRequest},
		{"trailing value", `{"id":"worker-a","hostname":"host"} {}`, http.StatusBadRequest},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			response := sendRequest(handler, http.MethodPost, "/workers", test.body)
			if response.Code != test.status {
				t.Fatalf("got %d, want %d: %s", response.Code, test.status, response.Body.String())
			}
			var failure api.ErrorResponse
			err := json.Unmarshal(response.Body.Bytes(), &failure)
			if err != nil || failure.Error == "" {
				t.Fatalf("invalid error response: %s", response.Body.String())
			}
		})
	}
	workers := listWorkers(t, handler)
	if len(workers) != 0 {
		t.Fatalf("invalid requests registered workers: %+v", workers)
	}
}

func TestRegistrationAcceptsAdditionalFieldsAndLargeBodies(t *testing.T) {
	handler := New(10 * time.Second).Handler()
	hostname := strings.Repeat("x", 70_000)
	body := fmt.Sprintf(`{"id":"worker-a","hostname":%q,"description":"demo worker"}`, hostname)
	response := sendRequest(handler, http.MethodPost, "/workers", body)
	if response.Code != http.StatusCreated {
		t.Fatalf("register: %d %s", response.Code, response.Body.String())
	}
	workers := listWorkers(t, handler)
	if len(workers) != 1 || workers[0].Hostname != hostname {
		t.Fatal("registration did not preserve the supplied hostname")
	}
}

func registerWorker(t *testing.T, handler http.Handler, id string) api.Worker {
	t.Helper()
	response := sendRequest(handler, http.MethodPost, "/workers", fmt.Sprintf(`{"id":%q,"hostname":"test-host"}`, id))
	if response.Code != http.StatusCreated {
		t.Fatalf("register: %d %s", response.Code, response.Body.String())
	}
	var worker api.Worker
	err := json.Unmarshal(response.Body.Bytes(), &worker)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func listWorkers(t *testing.T, handler http.Handler) []api.Worker {
	t.Helper()
	response := sendRequest(handler, http.MethodGet, "/workers", "")
	if response.Code != http.StatusOK {
		t.Fatalf("list workers: %d %s", response.Code, response.Body.String())
	}
	var result api.WorkersResponse
	err := json.Unmarshal(response.Body.Bytes(), &result)
	if err != nil {
		t.Fatal(err)
	}
	return result.Workers
}

func sendRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	return response
}
