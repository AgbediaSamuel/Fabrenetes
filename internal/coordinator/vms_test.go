package coordinator

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestConfiguredVMsWaitForRegistration(t *testing.T) {
	fakeMultipass(t)
	server := New(5 * time.Second)
	err := server.ConfigureVMs(context.Background(), []string{"worker-b", "worker-a"}, VMCommands{})
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	workers := listWorkers(t, handler)
	if len(workers) != 2 || workers[0].ID != "worker-a" || workers[1].ID != "worker-b" {
		t.Fatalf("unexpected configured workers: %+v", workers)
	}
	for _, worker := range workers {
		if worker.Status != "unregistered" || !worker.CanStart || worker.IP == "" || !worker.LastSeen.IsZero() || worker.SessionID != "" {
			t.Fatalf("configured VM appeared registered: %+v", worker)
		}
	}
	if workers[0].IP != "192.168.2.9" || workers[1].IP != "192.168.2.10" {
		t.Fatalf("VM addresses were not retained: %+v", workers)
	}
	response := sendRequest(handler, http.MethodPost, "/workers/worker-a/heartbeat", `{"session_id":"previous-coordinator-session"}`)
	if response.Code != http.StatusNotFound {
		t.Fatalf("old agent must receive404 and re-register: %d %s", response.Code, response.Body.String())
	}
	service := createService(t, handler, api.ServiceSpec{Name: "clock", Command: "clock", Replicas: 1})
	if service.Instances[0].WorkerID != "" || service.Instances[0].Status != "pending" {
		t.Fatalf("an unregistered VM received an assignment: %+v", service)
	}
	registerWorker(t, handler, "worker-a")
	workers = listWorkers(t, handler)
	if len(workers) != 2 || workers[0].Status != "ready" || !workers[0].CanStart || workers[0].SessionID == "" {
		t.Fatalf("registration did not replace the displayed placeholder: %+v", workers)
	}
	service = listServices(t, handler)[0]
	if service.Instances[0].WorkerID != "worker-a" {
		t.Fatalf("registered worker did not receive pending work: %+v", service)
	}
}

func TestStartOnlyConfiguredWorkerAgents(t *testing.T) {
	log := fakeMultipass(t)
	server := New(5 * time.Second)
	err := server.ConfigureVMs(context.Background(), []string{"worker-a"}, VMCommands{})
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	registerWorker(t, handler, "worker-b")
	err = os.WriteFile(log, nil, 0600)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"start", "stop"} {
		for _, id := range []string{"worker-b", "missing"} {
			response := sendRequest(handler, http.MethodPost, "/workers/"+id+"/"+action, "")
			if response.Code != http.StatusNotFound {
				t.Fatalf("unmanaged %s %s: %d %s", action, id, response.Code, response.Body.String())
			}
		}
	}
	output, err := os.ReadFile(log)
	if err != nil || len(output) != 0 {
		t.Fatalf("an unmanaged worker invoked Multipass: %s, %v", output, err)
	}
	response := sendRequest(handler, http.MethodPost, "/workers/worker-a/start", "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("start configured worker: %d %s", response.Code, response.Body.String())
	}
	output, err = os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	want := "exec\nworker-a\n--\nsudo\nsystemctl\nstart\nfabrenetes-worker\n"
	if string(output) != want {
		t.Fatalf("unexpected worker command arguments: %q", output)
	}
	if listWorkers(t, handler)[0].Status != "unregistered" {
		t.Fatal("starting the service marked the worker ready before registration")
	}
	t.Setenv("fabrenetes_test_exec_error", "VM is not running")
	response = sendRequest(handler, http.MethodPost, "/workers/worker-a/start", "")
	if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), "VM is not running") {
		t.Fatalf("Multipass start error was not reported: %d %s", response.Code, response.Body.String())
	}
}

func TestStoppedWorkerKeepsAssignmentsUntilRestart(t *testing.T) {
	log := fakeMultipass(t)
	server := New(5 * time.Second)
	err := server.ConfigureVMs(context.Background(), []string{"worker-a"}, VMCommands{})
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	worker := registerWorker(t, handler, "worker-a")
	service := createService(t, handler, api.ServiceSpec{Name: "clock", Command: "clock", Replicas: 1})
	instanceID := service.Instances[0].ID
	body := fmt.Sprintf(`{"session_id":%q,"instances":[{"id":%q,"status":"running","pid":42}]}`, worker.SessionID, instanceID)
	response := sendRequest(handler, http.MethodPost, "/workers/worker-a/heartbeat", body)
	if response.Code != http.StatusOK {
		t.Fatalf("heartbeat: %d %s", response.Code, response.Body.String())
	}
	t.Setenv("fabrenetes_test_exec_error", "could not stop service")
	response = sendRequest(handler, http.MethodPost, "/workers/worker-a/stop", "")
	if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), "could not stop service") {
		t.Fatalf("stop failure: %d %s", response.Code, response.Body.String())
	}
	current := listWorkers(t, handler)[0]
	if current.SessionID != worker.SessionID || current.Status != "ready" || listServices(t, handler)[0].Instances[0].Status != "running" {
		t.Fatal("failed stop changed the registered worker or its instance")
	}
	t.Setenv("fabrenetes_test_exec_error", "")
	err = os.WriteFile(log, nil, 0600)
	if err != nil {
		t.Fatal(err)
	}
	response = sendRequest(handler, http.MethodPost, "/workers/worker-a/stop", "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("stop configured worker: %d %s", response.Code, response.Body.String())
	}
	output, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	want := "exec\nworker-a\n--\nsudo\nsystemctl\nstop\nfabrenetes-worker\n"
	if string(output) != want {
		t.Fatalf("unexpected stop command arguments: %q", output)
	}
	current = listWorkers(t, handler)[0]
	if current.Status != "unregistered" || !current.CanStart || current.IP != "192.168.2.9" || current.SessionID != "" {
		t.Fatalf("stopped worker did not return to its configured placeholder: %+v", current)
	}
	instance := listServices(t, handler)[0].Instances[0]
	if instance.ID != instanceID || instance.WorkerID != "worker-a" || instance.Status != "unreachable" {
		t.Fatalf("stopping the agent changed its assignment: %+v", instance)
	}
	pending := createService(t, handler, api.ServiceSpec{Name: "pending", Command: "clock", Replicas: 1})
	if pending.Instances[0].WorkerID != "" {
		t.Fatal("stopped worker received new work")
	}
	response = sendRequest(handler, http.MethodPost, "/workers/worker-a/heartbeat", body)
	if response.Code != http.StatusNotFound {
		t.Fatalf("stopped session heartbeat: %d %s", response.Code, response.Body.String())
	}
	worker = registerWorker(t, handler, "worker-a")
	services := listServices(t, handler)
	if services[0].Instances[0].ID != instanceID || services[0].Instances[0].Status != "unknown" || services[1].Instances[0].WorkerID != "worker-a" {
		t.Fatalf("re-registration did not recover existing and pending assignments: %+v", services)
	}
	body = fmt.Sprintf(`{"session_id":%q,"instances":[{"id":%q,"status":"running","pid":43}]}`, worker.SessionID, instanceID)
	response = sendRequest(handler, http.MethodPost, "/workers/worker-a/heartbeat", body)
	instance = listServices(t, handler)[0].Instances[0]
	if response.Code != http.StatusOK || instance.Status != "running" || instance.PID != 43 {
		t.Fatalf("restarted agent did not report its replacement process: %d %+v", response.Code, instance)
	}
}

func TestConfigureVMsValidatesInventory(t *testing.T) {
	for _, test := range []struct {
		name      string
		inventory string
		failure   string
		want      string
	}{
		{name: "invalid JSON", inventory: "invalid", want: "read Multipass VM list"},
		{name: "unknown VM", inventory: `{"list":[]}`, want: `VM "worker-a" does not exist`},
		{name: "command failed", failure: "daemon unavailable", want: "daemon unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fakeMultipass(t)
			t.Setenv("fabrenetes_test_inventory", test.inventory)
			t.Setenv("fabrenetes_test_list_error", test.failure)
			server := New(5 * time.Second)
			err := server.ConfigureVMs(context.Background(), []string{"worker-a"}, VMCommands{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got error %v, want %q", err, test.want)
			}
			if len(listWorkers(t, server.Handler())) != 0 {
				t.Fatal("failed configuration added workers")
			}
		})
	}
}

func TestNoVMConfigurationNeedsNoMultipass(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	server := New(5 * time.Second)
	err := server.ConfigureVMs(context.Background(), nil, VMCommands{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listWorkers(t, server.Handler())) != 0 {
		t.Fatal("VM configuration is not optional")
	}
}

func TestRunOnVMForwardsCommandArguments(t *testing.T) {
	log := fakeMultipass(t)
	err := (VMCommands{}).runOnVM(context.Background(), VM{Name: "worker-a"}, "printf", "%s", "hello world; $(literal)")
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	want := "exec\nworker-a\n--\nprintf\n%s\nhello world; $(literal)\n"
	if string(output) != want {
		t.Fatalf("unexpected VM command arguments: %q", output)
	}
}

func TestCustomVMCommands(t *testing.T) {
	dir := t.TempDir()
	list := filepath.Join(dir, "list machines")
	run := filepath.Join(dir, "run on machine")
	log := filepath.Join(dir, "arguments")
	err := os.WriteFile(list, []byte(`#!/bin/sh
printf 'Discovering machines\n' >&2
printf '[{"name":"custom-worker","ip":"10.0.0.8"}]'
`), 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(run, []byte(`#!/bin/sh
printf '%s\n' "$@" >> "$fabrenetes_test_log"
`), 0755)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", t.TempDir())
	t.Setenv("fabrenetes_test_log", log)
	commands := VMCommands{List: list, Run: run}
	server := New(5 * time.Second)
	err = server.ConfigureVMs(context.Background(), []string{"custom-worker"}, commands)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	workers := listWorkers(t, handler)
	if len(workers) != 1 || workers[0].ID != "custom-worker" || workers[0].IP != "10.0.0.8" {
		t.Fatalf("unexpected custom VM: %+v", workers)
	}
	for _, action := range []string{"start", "stop"} {
		response := sendRequest(handler, http.MethodPost, "/workers/custom-worker/"+action, "")
		if response.Code != http.StatusNoContent {
			t.Fatalf("custom %s: %d %s", action, response.Code, response.Body.String())
		}
	}
	err = commands.runOnVM(context.Background(), VM{Name: "custom-worker", IP: "10.0.0.8"}, "printf", "%s", "hello world; $(literal)")
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	want := "custom-worker\n10.0.0.8\nsudo\nsystemctl\nstart\nfabrenetes-worker\n" +
		"custom-worker\n10.0.0.8\nsudo\nsystemctl\nstop\nfabrenetes-worker\n" +
		"custom-worker\n10.0.0.8\nprintf\n%s\nhello world; $(literal)\n"
	if string(output) != want {
		t.Fatalf("unexpected custom runner arguments: %q", output)
	}
}

func TestCustomVMDiscoveryErrors(t *testing.T) {
	for _, test := range []struct {
		name   string
		script string
		want   string
	}{
		{name: "invalid JSON", script: "printf invalid", want: "read VM list"},
		{name: "command failed", script: "printf 'discovery failed' >&2; exit 1", want: "discovery failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			list := filepath.Join(t.TempDir(), "discover")
			err := os.WriteFile(list, []byte("#!/bin/sh\n"+test.script+"\n"), 0755)
			if err != nil {
				t.Fatal(err)
			}
			server := New(5 * time.Second)
			err = server.ConfigureVMs(context.Background(), []string{"custom-worker"}, VMCommands{List: list})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got error %v, want %q", err, test.want)
			}
			if len(listWorkers(t, server.Handler())) != 0 {
				t.Fatal("failed custom discovery added workers")
			}
		})
	}
}

func fakeMultipass(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "arguments")
	script := `#!/bin/sh
printf '%s\n' "$@" >> "$fabrenetes_test_log"
case "$1" in
  list)
    if [ -n "$fabrenetes_test_list_error" ]; then
      printf '%s\n' "$fabrenetes_test_list_error" >&2
      exit 1
    fi
    printf '%s' "$fabrenetes_test_inventory"
    ;;
  exec)
    if [ -n "$fabrenetes_test_exec_error" ]; then
      printf '%s\n' "$fabrenetes_test_exec_error" >&2
      exit 1
    fi
    ;;
  *) exit 1 ;;
esac
`
	err := os.WriteFile(filepath.Join(dir, "multipass"), []byte(script), 0755)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("fabrenetes_test_log", log)
	t.Setenv("fabrenetes_test_inventory", `{"list":[{"name":"worker-a","ipv4":["192.168.2.9"]},{"name":"worker-b","ipv4":["192.168.2.10"]},{"name":"unrelated","ipv4":[]}]}`)
	t.Setenv("fabrenetes_test_list_error", "")
	t.Setenv("fabrenetes_test_exec_error", "")
	return log
}
