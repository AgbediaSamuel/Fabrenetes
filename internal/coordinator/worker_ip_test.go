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

func TestWorkerIPUsesConnectionAddress(t *testing.T) {
	cases := []struct {
		address string
		ip      string
	}{
		{"192.168.2.9:50422", "192.168.2.9"},
		{"[2001:db8::9]:50422", "2001:db8::9"},
	}
	for _, test := range cases {
		t.Run(test.ip, func(t *testing.T) {
			handler := New(5 * time.Second).Handler()
			response := workerRequestFrom(handler, "/workers", `{"id":"worker-a","hostname":"host-a","ip":"203.0.113.1"}`, test.address)
			if response.Code != http.StatusCreated {
				t.Fatalf("register: %d %s", response.Code, response.Body.String())
			}
			var worker api.Worker
			err := json.Unmarshal(response.Body.Bytes(), &worker)
			if err != nil {
				t.Fatal(err)
			}
			if worker.IP != test.ip || listWorkers(t, handler)[0].IP != test.ip {
				t.Fatalf("worker IP = %q, want %q", worker.IP, test.ip)
			}
		})
	}
}

func TestWorkerIPChangesOnlyWithAcceptedHeartbeat(t *testing.T) {
	handler := New(5 * time.Second).Handler()
	worker := registerWorker(t, handler, "worker-a")
	body := fmt.Sprintf(`{"session_id":%q}`, worker.SessionID)
	response := workerRequestFrom(handler, "/workers/worker-a/heartbeat", body, "192.168.2.10:51000")
	if response.Code != http.StatusOK || listWorkers(t, handler)[0].IP != "192.168.2.10" {
		t.Fatalf("heartbeat did not update worker IP: %d %s", response.Code, response.Body.String())
	}
	response = workerRequestFrom(handler, "/workers/worker-a/heartbeat", `{"session_id":"stale-session"}`, "203.0.113.1:51001")
	if response.Code != http.StatusConflict || listWorkers(t, handler)[0].IP != "192.168.2.10" {
		t.Fatalf("stale heartbeat changed worker IP: %d %s", response.Code, response.Body.String())
	}
}

func workerRequestFrom(handler http.Handler, path, body, address string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.RemoteAddr = address
	request.Header.Set("X-Forwarded-For", "203.0.113.2")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
