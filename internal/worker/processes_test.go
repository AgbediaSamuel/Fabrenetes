package worker

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestWorkloadProcess(t *testing.T) {
	if os.Getenv("test_process") != "1" {
		return
	}
	if os.Getenv("test_listen") == "1" {
		listener, err := net.Listen("tcp4", ":"+os.Getenv("service_port"))
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
	}
	data := fmt.Sprintf("%d\n%s\n%s\n%s\n%s\n", os.Getpid(),
		os.Getenv("worker_id"), os.Getenv("service_name"), os.Getenv("instance_id"), os.Getenv("service_port"))
	err := os.WriteFile(os.Args[len(os.Args)-1], []byte(data), 0600)
	if err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestAssignmentsStartOnceAndStop(t *testing.T) {
	assignment := testAssignment(t)
	assignment.Port = 9001
	children := processes{workerID: "worker-a", instances: make(map[string]*process)}
	t.Cleanup(children.stop)
	children.apply([]api.Assignment{assignment})
	status := children.status()[0]
	if status.Status != "running" || status.PID == 0 || status.Port < assignment.Port {
		t.Fatalf("process did not start: %+v", status)
	}
	waitFor(t, func() bool {
		data, _ := os.ReadFile(assignment.Args[len(assignment.Args)-1])
		return string(data) == fmt.Sprintf("%d\nworker-a\ndemo\ninstance-a\n%d\n", status.PID, status.Port)
	})
	for range 5 {
		children.apply([]api.Assignment{assignment})
		current := children.status()[0]
		if current.PID != status.PID || current.Restarts != 0 {
			t.Fatalf("repeated assignment changed the process: %+v", current)
		}
	}
	children.apply(nil)
	if len(children.status()) != 0 || syscall.Kill(status.PID, 0) != syscall.ESRCH {
		t.Fatal("removed assignment left its process running")
	}
}

func TestExitedProcessRestarts(t *testing.T) {
	assignment := testAssignment(t)
	children := processes{workerID: "worker-a", instances: make(map[string]*process)}
	t.Cleanup(children.stop)
	children.apply([]api.Assignment{assignment})
	previous := children.status()[0]
	err := syscall.Kill(previous.PID, syscall.SIGKILL)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return children.status()[0].Status == "failed" })
	children.apply([]api.Assignment{assignment})
	current := children.status()[0]
	if current.Status != "running" || current.PID == previous.PID || current.PID == 0 || current.Restarts != 1 {
		t.Fatalf("process was not restarted: %+v", current)
	}
}

func TestStopRequestOnlyStopsSelectedInstance(t *testing.T) {
	first := testAssignment(t)
	first.StopID = "previous-stop"
	second := testAssignment(t)
	second.ID = "instance-b"
	assignments := []api.Assignment{first, second}
	children := processes{workerID: "worker-a", instances: make(map[string]*process)}
	t.Cleanup(children.stop)
	children.apply(assignments)
	waitFor(t, func() bool {
		_, firstErr := os.Stat(first.Args[len(first.Args)-1])
		_, secondErr := os.Stat(second.Args[len(second.Args)-1])
		return firstErr == nil && secondErr == nil
	})
	before := children.status()
	if before[0].Status != "running" || before[1].Status != "running" {
		t.Fatalf("processes did not start: %+v", before)
	}

	assignments[0].StopID = "stop-one"
	children.apply(assignments)
	waitFor(t, func() bool { return children.status()[0].Status == "failed" })
	peer := children.status()[1]
	if peer.Status != "running" || peer.PID != before[1].PID || peer.Restarts != 0 {
		t.Fatalf("stop request affected peer: %+v", peer)
	}
	children.apply(assignments)
	replacement := children.status()[0]
	if replacement.Status != "running" || replacement.PID == before[0].PID || replacement.PID == 0 || replacement.Restarts != 1 {
		t.Fatalf("stopped process was not restarted: %+v", replacement)
	}

	for range 5 {
		children.apply(assignments)
		current := children.status()
		if current[0].PID != replacement.PID || current[0].Status != "running" || current[0].Restarts != 1 {
			t.Fatalf("repeated stop request affected replacement: %+v", current[0])
		}
		if current[1].PID != before[1].PID || current[1].Restarts != 0 {
			t.Fatalf("repeated stop request affected peer: %+v", current[1])
		}
	}

	assignments[0].StopID = "stop-two"
	children.apply(assignments)
	waitFor(t, func() bool { return children.status()[0].Status == "failed" })
	children.apply(assignments)
	current := children.status()
	if current[0].Status != "running" || current[0].PID == replacement.PID || current[0].PID == 0 || current[0].Restarts != 2 {
		t.Fatalf("second stop request did not restart process: %+v", current[0])
	}
	if current[1].PID != before[1].PID || current[1].Restarts != 0 {
		t.Fatalf("second stop request affected peer: %+v", current[1])
	}
}

func TestStartFailureDoesNotRetry(t *testing.T) {
	assignment := testAssignment(t)
	executable := assignment.Command
	assignment.Command = filepath.Join(t.TempDir(), "missing")
	children := processes{workerID: "worker-a", instances: make(map[string]*process)}
	t.Cleanup(children.stop)
	children.apply([]api.Assignment{assignment})
	err := os.Symlink(executable, assignment.Command)
	if err != nil {
		t.Fatal(err)
	}
	children.apply([]api.Assignment{assignment})
	status := children.status()[0]
	if status.Status != "failed" || status.PID != 0 || status.Error == "" || status.Restarts != 0 {
		t.Fatalf("failed start was retried: %+v", status)
	}
}

func TestRunExecutesAssignmentsAndCleansUp(t *testing.T) {
	assignment := testAssignment(t)
	var desired atomic.Bool
	desired.Store(true)
	reports := make(chan []api.InstanceStatus, 16)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /workers", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusCreated, api.Worker{SessionID: "session-a"})
	})
	mux.HandleFunc("POST /workers/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var request api.HeartbeatRequest
		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			t.Errorf("decode heartbeat: %v", err)
		}
		response := api.HeartbeatResponse{Assignments: []api.Assignment{}}
		if desired.Load() {
			response.Assignments = append(response.Assignments, assignment)
		}
		respond(w, http.StatusOK, response)
		select {
		case reports <- request.Instances:
		default:
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	cancel, done := startWorker(t, testConfig(server.URL))
	defer func() {
		cancel()
		awaitWorker(t, done)
	}()
	var pid int
	waitFor(t, func() bool {
		select {
		case report := <-reports:
			if len(report) == 1 && report[0].Status == "running" {
				pid = report[0].PID
				return pid > 0
			}
		default:
		}
		return false
	})
	desired.Store(false)
	waitFor(t, func() bool { return syscall.Kill(pid, 0) == syscall.ESRCH })
	desired.Store(true)
	var replacement int
	waitFor(t, func() bool {
		select {
		case report := <-reports:
			if len(report) == 1 && report[0].Status == "running" && report[0].PID != pid {
				replacement = report[0].PID
				return replacement > 0
			}
		default:
		}
		return false
	})
	cancel()
	waitFor(t, func() bool { return syscall.Kill(replacement, 0) == syscall.ESRCH })
}

func testAssignment(t *testing.T) api.Assignment {
	t.Helper()
	t.Setenv("test_process", "1")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return api.Assignment{
		ID: "instance-a", Service: "demo", Command: executable,
		Args: []string{"-test.run=^TestWorkloadProcess$", "--", filepath.Join(t.TempDir(), "ready")},
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
