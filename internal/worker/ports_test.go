package worker

import (
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"fabrenetes/internal/api"
)

func TestInstancesSkipOccupiedAndReservedPorts(t *testing.T) {
	listener, err := net.Listen("tcp4", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	base := listener.Addr().(*net.TCPAddr).Port
	if base >= 65534 {
		t.Skip("OS-selected port leaves too few ports above it")
	}
	t.Setenv("test_listen", "1")
	first := testAssignment(t)
	first.Port = base
	second := testAssignment(t)
	second.ID = "instance-b"
	second.Port = base
	children := processes{workerID: "worker-a", instances: make(map[string]*process)}
	t.Cleanup(children.stop)
	children.apply([]api.Assignment{first, second})
	before := children.status()
	if before[0].Status != "running" || before[1].Status != "running" || before[0].Port <= base || before[1].Port <= base || before[0].Port == before[1].Port {
		t.Fatalf("instances did not receive separate available ports: %+v", before)
	}
	for i, assignment := range []api.Assignment{first, second} {
		waitFor(t, func() bool {
			data, _ := os.ReadFile(assignment.Args[len(assignment.Args)-1])
			return string(data) == fmt.Sprintf("%d\nworker-a\ndemo\n%s\n%d\n", before[i].PID, assignment.ID, before[i].Port)
		})
		available, err := portAvailable(before[i].Port)
		if err != nil || available {
			t.Fatalf("child did not bind reported port %d: available=%t, error=%v", before[i].Port, available, err)
		}
	}

	children.apply([]api.Assignment{second})
	third := testAssignment(t)
	third.ID = "instance-c"
	third.Port = base
	children.apply([]api.Assignment{second, third})
	current := children.status()
	if current[0].PID != before[1].PID || current[0].Port != before[1].Port || current[1].Port != before[0].Port {
		t.Fatalf("removed instance did not release its port: %+v", current)
	}
}

func TestRestartReusesOrReplacesItsPort(t *testing.T) {
	listener, err := net.Listen("tcp4", ":0")
	if err != nil {
		t.Fatal(err)
	}
	base := listener.Addr().(*net.TCPAddr).Port
	err = listener.Close()
	if err != nil {
		t.Fatal(err)
	}
	if base == 65535 {
		t.Skip("OS-selected port leaves no fallback port")
	}
	t.Setenv("test_listen", "1")
	assignment := testAssignment(t)
	assignment.Port = base
	children := processes{workerID: "worker-a", instances: make(map[string]*process)}
	t.Cleanup(children.stop)
	children.apply([]api.Assignment{assignment})
	before := children.status()[0]
	waitFor(t, func() bool {
		_, err := os.Stat(assignment.Args[len(assignment.Args)-1])
		return err == nil
	})
	err = syscall.Kill(before.PID, syscall.SIGKILL)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return children.status()[0].Status == "failed" })
	children.apply([]api.Assignment{assignment})
	after := children.status()[0]
	if after.Status != "running" || after.PID == before.PID || after.Port != before.Port || after.Restarts != 1 {
		t.Fatalf("restart changed the assigned port: %+v", after)
	}
	waitFor(t, func() bool {
		data, _ := os.ReadFile(assignment.Args[len(assignment.Args)-1])
		return string(data) == fmt.Sprintf("%d\nworker-a\ndemo\ninstance-a\n%d\n", after.PID, after.Port)
	})

	err = syscall.Kill(after.PID, syscall.SIGKILL)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return children.status()[0].Status == "failed" })
	occupied, err := net.Listen("tcp4", fmt.Sprintf(":%d", after.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	children.apply([]api.Assignment{assignment})
	replacement := children.status()[0]
	if replacement.Status != "running" || replacement.Port == after.Port || replacement.Port < base || replacement.Restarts != 2 {
		t.Fatalf("restart did not replace an occupied port: %+v", replacement)
	}
	waitFor(t, func() bool {
		data, _ := os.ReadFile(assignment.Args[len(assignment.Args)-1])
		return string(data) == fmt.Sprintf("%d\nworker-a\ndemo\ninstance-a\n%d\n", replacement.PID, replacement.Port)
	})
}

func TestPortAllocationOptionalAndExhausted(t *testing.T) {
	children := processes{instances: map[string]*process{
		"occupied": {status: api.InstanceStatus{PID: 1, Port: 65535}},
	}}
	withoutPort := &process{assignment: api.Assignment{ID: "no-port"}}
	port, err := children.availablePort(withoutPort)
	if err != nil || port != 0 {
		t.Fatalf("unrequested port allocation: port=%d error=%v", port, err)
	}
	lastPort := &process{assignment: api.Assignment{ID: "last-port", Port: 65535}}
	port, err = children.availablePort(lastPort)
	if err == nil || port != 0 {
		t.Fatalf("occupied final TCP port was allocated: port=%d error=%v", port, err)
	}
}

func TestStartFailureReleasesAllocatedPort(t *testing.T) {
	listener, err := net.Listen("tcp4", ":0")
	if err != nil {
		t.Fatal(err)
	}
	base := listener.Addr().(*net.TCPAddr).Port
	err = listener.Close()
	if err != nil {
		t.Fatal(err)
	}
	assignment := testAssignment(t)
	assignment.Port = base
	assignment.Command = "/missing/fabrenetes-test-command"
	children := processes{workerID: "worker-a", instances: make(map[string]*process)}
	t.Cleanup(children.stop)
	children.apply([]api.Assignment{assignment})
	failed := children.status()[0]
	if failed.Status != "failed" || failed.PID != 0 || failed.Port != 0 {
		t.Fatalf("failed start kept a port reservation: %+v", failed)
	}
	valid := testAssignment(t)
	valid.ID = "instance-b"
	valid.Port = base
	children.apply([]api.Assignment{assignment, valid})
	current := children.status()[1]
	if current.Status != "running" || current.Port != base {
		t.Fatalf("failed start prevented port reuse: %+v", current)
	}
}
