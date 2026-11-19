package coordinator

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestServiceAddressBalancesAndPreservesApplicationRequests(t *testing.T) {
	var calls [2]atomic.Int32
	backends := make([]*httptest.Server, 2)
	for index := range backends {
		backends[index] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls[index].Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			w.Header().Set("X-Application", "echo")
			w.Header().Set("Fabrenetes-Worker", "untrusted-app-header")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{
				"method": r.Method, "path": r.URL.Path, "escaped": r.URL.EscapedPath(),
				"query": r.URL.RawQuery, "body": string(body), "header": r.Header.Get("X-Example"),
			})
		}))
		defer backends[index].Close()
	}
	cluster, service, workers := routingFixture(t, backends)
	proxy := httptest.NewServer(cluster.Handler())
	defer proxy.Close()
	if service.Endpoint != "/apps/echo/" {
		t.Fatalf("service endpoint missing: %+v", service)
	}
	for index := range 6 {
		body := `{"message":"hello"}`
		request, err := http.NewRequest(http.MethodPatch, proxy.URL+service.Endpoint+"nested/a%2Fb?x=1&x=2", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Example", "preserved")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]string
		err = json.NewDecoder(response.Body).Decode(&result)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusCreated || response.Header.Get("Fabrenetes-Worker") != workers[index%2].ID || response.Header.Get("Fabrenetes-Instance") != service.Instances[index%2].ID || response.Header.Get("X-Application") != "echo" {
			t.Fatalf("incorrect selected application: %d %v", response.StatusCode, response.Header)
		}
		if result["method"] != http.MethodPatch || result["path"] != "/nested/a/b" || result["escaped"] != "/nested/a%2Fb" || result["query"] != "x=1&x=2" || result["body"] != body || result["header"] != "preserved" {
			t.Fatalf("request changed: %+v", result)
		}
	}
	if calls[0].Load() != 3 || calls[1].Load() != 3 {
		t.Fatalf("requests not balanced: %d %d", calls[0].Load(), calls[1].Load())
	}
	response, err := http.Get(proxy.URL + "/apps/echo")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result map[string]string
	err = json.NewDecoder(response.Body).Decode(&result)
	if err != nil || result["path"] != "/" {
		t.Fatalf("bare service address did not reach app root: %+v %v", result, err)
	}
}

func TestServiceAddressExcludesUnavailableInstances(t *testing.T) {
	backends := []*httptest.Server{
		httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("a")) })),
		httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("b")) })),
	}
	for _, backend := range backends {
		defer backend.Close()
	}
	cluster, service, workers := routingFixture(t, backends)
	now := time.Now()
	cluster.now = func() time.Time { return now }
	for id, worker := range cluster.workers {
		worker.LastSeen = now
		cluster.workers[id] = worker
	}
	handler := cluster.Handler()
	now = now.Add(time.Minute)
	response := sendRequest(handler, http.MethodGet, service.Endpoint, "")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expired workers routed: %d", response.Code)
	}
	heartbeatWorker(t, handler, workers[0], []api.InstanceStatus{{ID: service.Instances[0].ID, Status: "failed"}})
	response = sendRequest(handler, http.MethodGet, service.Endpoint, "")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed process routed: %d", response.Code)
	}
	heartbeatWorker(t, handler, workers[0], []api.InstanceStatus{{ID: service.Instances[0].ID, Status: "running", PID: 123, Port: backendPort(t, backends[0].URL)}})
	stored := cluster.workers[workers[0].ID]
	stored.IP = "127.0.0.1"
	cluster.workers[workers[0].ID] = stored
	for range 4 {
		response = sendRequest(handler, http.MethodGet, service.Endpoint, "")
		if response.Code != http.StatusOK || response.Body.String() != "a" || response.Header().Get("Fabrenetes-Worker") != workers[0].ID {
			t.Fatalf("request reached unavailable worker: %d %s", response.Code, response.Body.String())
		}
	}
	response = sendRequest(handler, http.MethodDelete, "/services/echo", "")
	if response.Code != http.StatusNoContent {
		t.Fatal(response.Body.String())
	}
	response = sendRequest(handler, http.MethodGet, service.Endpoint, "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("deleted service routed: %d", response.Code)
	}
}

func TestServiceAddressDoesNotReplayAWriteAfterApplicationDisconnects(t *testing.T) {
	var firstCalls atomic.Int32
	var secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		firstCalls.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		connection.Close()
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer second.Close()
	cluster, service, _ := routingFixture(t, []*httptest.Server{first, second})
	response := sendRequest(cluster.Handler(), http.MethodPost, service.Endpoint+"charge", "one write")
	if response.Code != http.StatusBadGateway || firstCalls.Load() != 1 || secondCalls.Load() != 0 {
		t.Fatalf("ambiguous write replayed: status=%d calls=%d,%d", response.Code, firstCalls.Load(), secondCalls.Load())
	}
	response = sendRequest(cluster.Handler(), http.MethodPost, service.Endpoint+"charge", "next write")
	if response.Code != http.StatusCreated || firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("next request failed: status=%d calls=%d,%d", response.Code, firstCalls.Load(), secondCalls.Load())
	}
}

func routingFixture(t *testing.T, backends []*httptest.Server) (*Coordinator, api.Service, []api.Worker) {
	t.Helper()
	cluster := New(time.Minute)
	handler := cluster.Handler()
	workers := []api.Worker{registerWorker(t, handler, "worker-a"), registerWorker(t, handler, "worker-b")}
	for _, worker := range workers {
		stored := cluster.workers[worker.ID]
		stored.IP = "127.0.0.1"
		cluster.workers[worker.ID] = stored
	}
	service := createService(t, handler, api.ServiceSpec{Name: "echo", Command: "echo", Replicas: 2})
	for index, worker := range workers {
		heartbeatWorker(t, handler, worker, []api.InstanceStatus{{ID: service.Instances[index].ID, PID: 123, Port: backendPort(t, backends[index].URL), Status: "running"}})
		stored := cluster.workers[worker.ID]
		stored.IP = "127.0.0.1"
		cluster.workers[worker.ID] = stored
	}
	return cluster, service, workers
}

func backendPort(t *testing.T, address string) int {
	t.Helper()
	parsed, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	return number
}
