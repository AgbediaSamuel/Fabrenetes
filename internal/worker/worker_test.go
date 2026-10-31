package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestRunRegistersAndHeartbeats(t *testing.T) {
	heartbeats := make(chan string, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /workers", func(w http.ResponseWriter, r *http.Request) {
		var request api.RegisterRequest
		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			t.Errorf("decode registration: %v", err)
		}
		if request.ID != "worker/a" || request.Hostname != "node-a" {
			t.Errorf("unexpected registration: %+v", request)
		}
		respond(w, http.StatusCreated, api.Worker{ID: request.ID, SessionID: "session-a"})
	})
	mux.HandleFunc("POST /workers/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "worker/a" {
			t.Errorf("worker ID was not preserved: %q", r.PathValue("id"))
		}
		var request api.HeartbeatRequest
		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			t.Errorf("decode heartbeat: %v", err)
		}
		respond(w, http.StatusOK, api.HeartbeatResponse{Assignments: []api.Assignment{}})
		select {
		case heartbeats <- request.SessionID:
		default:
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	config := testConfig(server.URL)
	config.ID = "worker/a"
	cancel, done := startWorker(t, config)
	select {
	case sessionID := <-heartbeats:
		if sessionID != "session-a" {
			t.Fatalf("heartbeat session = %q", sessionID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not heartbeat")
	}
	cancel()
	err := awaitWorker(t, done)
	if err != nil {
		t.Fatalf("cancel worker: %v", err)
	}
}

func TestRunRecoversFromTemporaryFailuresAndLostRegistration(t *testing.T) {
	var registrations atomic.Int32
	var heartbeats atomic.Int32
	var mu sync.Mutex
	var sessions []string
	recovered := make(chan struct{}, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /workers", func(w http.ResponseWriter, r *http.Request) {
		switch registrations.Add(1) {
		case 1:
			respond(w, http.StatusServiceUnavailable, api.ErrorResponse{Error: "starting"})
		case 2:
			respond(w, http.StatusCreated, api.Worker{SessionID: "first"})
		default:
			respond(w, http.StatusCreated, api.Worker{SessionID: "second"})
		}
	})
	mux.HandleFunc("POST /workers/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var request api.HeartbeatRequest
		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			t.Errorf("decode heartbeat: %v", err)
		}
		mu.Lock()
		sessions = append(sessions, request.SessionID)
		mu.Unlock()
		switch heartbeats.Add(1) {
		case 1:
			respond(w, http.StatusServiceUnavailable, api.ErrorResponse{Error: "temporarily unavailable"})
		case 2:
			respond(w, http.StatusNotFound, api.ErrorResponse{Error: "unknown worker"})
		default:
			respond(w, http.StatusOK, api.HeartbeatResponse{Assignments: []api.Assignment{}})
			select {
			case recovered <- struct{}{}:
			default:
			}
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	cancel, done := startWorker(t, testConfig(server.URL))
	select {
	case <-recovered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not recover")
	}
	cancel()
	err := awaitWorker(t, done)
	if err != nil {
		t.Fatal(err)
	}
	if registrations.Load() != 3 {
		t.Fatalf("registrations = %d, want 3", registrations.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sessions) < 3 || sessions[0] != "first" || sessions[1] != "first" || sessions[2] != "second" {
		t.Fatalf("heartbeat sessions = %v", sessions)
	}
}

func TestRunStopsWhenSessionIsReplaced(t *testing.T) {
	var registrations atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /workers", func(w http.ResponseWriter, r *http.Request) {
		registrations.Add(1)
		respond(w, http.StatusCreated, api.Worker{SessionID: "first"})
	})
	mux.HandleFunc("POST /workers/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusConflict, api.ErrorResponse{Error: "stale session"})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	_, done := startWorker(t, testConfig(server.URL))
	err := awaitWorker(t, done)
	if err == nil || !strings.Contains(err.Error(), "session was replaced") {
		t.Fatalf("error = %v, want replaced session", err)
	}
	if registrations.Load() != 1 {
		t.Fatalf("worker stole identity by registering %d times", registrations.Load())
	}
}

func TestRunRejectsDotSegmentIdentities(t *testing.T) {
	for _, id := range []string{".", ".."} {
		t.Run(id, func(t *testing.T) {
			config := testConfig("http://127.0.0.1:1")
			config.ID = id
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := Run(ctx, config)
			if err == nil || !strings.Contains(err.Error(), "valid worker ID") {
				t.Fatalf("error = %v, want invalid identity", err)
			}
		})
	}
}

func TestRunCancelsInflightRequest(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	cancel, done := startWorker(t, testConfig(server.URL))
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not register")
	}
	cancel()
	err := awaitWorker(t, done)
	if err != nil {
		t.Fatalf("cancel inflight registration: %v", err)
	}
}

func TestPostReadsLargeAssignmentResponse(t *testing.T) {
	argument := strings.Repeat("x", 128<<10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, api.HeartbeatResponse{Assignments: []api.Assignment{{
			ID: "instance-a", Command: "echo", Args: []string{argument},
		}}})
	}))
	defer server.Close()
	var response api.HeartbeatResponse
	err := post(context.Background(), server.Client(), server.URL, api.HeartbeatRequest{}, http.StatusOK, &response)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Assignments) != 1 || len(response.Assignments[0].Args) != 1 || response.Assignments[0].Args[0] != argument {
		t.Fatal("assignment response was not read completely")
	}
}

func TestPostRejectsInvalidResponseJSON(t *testing.T) {
	for _, body := range []string{`{"assignments":`, `{"assignments":[]} {"assignments":[]}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, body)
			}))
			defer server.Close()
			var response api.HeartbeatResponse
			err := post(context.Background(), server.Client(), server.URL, api.HeartbeatRequest{}, http.StatusOK, &response)
			if err == nil {
				t.Fatal("invalid response JSON was accepted")
			}
		})
	}
}

func testConfig(coordinatorURL string) Config {
	return Config{
		ID: "worker-a", Hostname: "node-a", CoordinatorURL: coordinatorURL,
		HeartbeatInterval: 5 * time.Millisecond,
		RequestTimeout:    time.Second,
	}
}

func startWorker(t *testing.T, config Config) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, config)
	}()
	return cancel, done
}

func awaitWorker(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
		return nil
	}
}

func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}
