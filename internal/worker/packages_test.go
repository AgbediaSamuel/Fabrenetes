package worker

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestPackagedRuntimeUsesPrivateDirectoriesAndRestarts(t *testing.T) {
	t.Setenv("test_process", "1")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	archive := makePackage(t,
		packageEntry{name: "runtime/runner", mode: 0755, data: runtime},
		packageEntry{name: "initial.txt", mode: 0644, data: []byte("original")},
	)
	id := packageID(archive)
	var downloads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/packages/"+id {
			t.Errorf("unexpected package path: %s", r.URL.Path)
		}
		downloads.Add(1)
		w.Write(archive)
	}))
	defer server.Close()
	store := &packageStore{baseURL: server.URL, dir: t.TempDir(), client: server.Client()}
	children := processes{workerID: "worker-a", instances: make(map[string]*process), ctx: context.Background(), packages: store}
	t.Cleanup(children.stop)
	first := api.Assignment{ID: "instance-a", Service: "demo", Package: id, Command: "./runtime/runner", Args: []string{"-test.run=^TestWorkloadProcess$", "--", "ready"}}
	second := first
	second.ID = "instance-b"
	assignments := []api.Assignment{first, second}
	children.apply(assignments)
	if children.status()[0].Status != "preparing" {
		t.Fatal("package launch did not report preparation")
	}
	waitFor(t, func() bool {
		children.apply(assignments)
		statuses := children.status()
		return statuses[0].Status == "running" && statuses[1].Status == "running"
	})
	before := children.status()
	firstDir := children.instances[first.ID].directory
	secondDir := children.instances[second.ID].directory
	if firstDir == secondDir || firstDir == "" || secondDir == "" {
		t.Fatalf("instances share their files: %q, %q", firstDir, secondDir)
	}
	for i, directory := range []string{firstDir, secondDir} {
		waitFor(t, func() bool {
			data, _ := os.ReadFile(filepath.Join(directory, "ready"))
			return string(data) == fmt.Sprintf("%d\nworker-a\ndemo\n%s\n\n", before[i].PID, assignments[i].ID)
		})
	}
	err = os.WriteFile(filepath.Join(firstDir, "initial.txt"), []byte("changed"), 0600)
	if err != nil {
		t.Fatal(err)
	}
	peerFile, err := os.ReadFile(filepath.Join(secondDir, "initial.txt"))
	if err != nil || string(peerFile) != "original" {
		t.Fatalf("one instance changed another's files: %q, %v", peerFile, err)
	}
	err = syscall.Kill(before[0].PID, syscall.SIGKILL)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return children.status()[0].Status == "failed" })
	children.apply(assignments)
	after := children.status()
	if after[0].PID == before[0].PID || after[0].Restarts != 1 || after[1].PID != before[1].PID {
		t.Fatalf("package recovery affected the wrong process: %+v", after)
	}
	if children.instances[first.ID].directory != firstDir || downloads.Load() != 1 {
		t.Fatalf("restart unpacked or downloaded the package again: directory=%q downloads=%d", children.instances[first.ID].directory, downloads.Load())
	}
	changed, err := os.ReadFile(filepath.Join(firstDir, "initial.txt"))
	if err != nil || string(changed) != "changed" {
		t.Fatalf("process restart replaced working files: %q, %v", changed, err)
	}
	children.apply([]api.Assignment{second})
	_, err = os.Stat(firstDir)
	if !os.IsNotExist(err) || children.status()[0].PID != before[1].PID {
		t.Fatalf("deleting one instance did not clean only its directory: %v", err)
	}
	children.apply(nil)
	_, err = os.Stat(secondDir)
	if !os.IsNotExist(err) {
		t.Fatalf("deleting the service left its working directory: %v", err)
	}
	_, err = os.Stat(filepath.Join(store.dir, "packages", id+".zip"))
	if err != nil {
		t.Fatalf("deleting an instance removed the reusable package: %v", err)
	}
}

func TestPackageDownloadKeepsHeartbeatsAndCancelsOnDeletion(t *testing.T) {
	archive := makePackage(t, packageEntry{name: "run", mode: 0755, data: []byte("#!/bin/sh\nexit 0\n")})
	id := packageID(archive)
	assignment := api.Assignment{ID: "instance-a", Service: "demo", Package: id, Command: "./run"}
	var desired atomic.Bool
	desired.Store(true)
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	reports := make(chan []api.InstanceStatus, 128)
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
	mux.HandleFunc("GET /packages/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(archive[:len(archive)/2])
		w.(http.Flusher).Flush()
		started <- struct{}{}
		<-r.Context().Done()
		canceled <- struct{}{}
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	config := testConfig(server.URL)
	config.DataDir = t.TempDir()
	config.HeartbeatInterval = 10 * time.Millisecond
	config.RequestTimeout = 200 * time.Millisecond
	cancel, done := startWorker(t, config)
	defer func() {
		cancel()
		awaitWorker(t, done)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("package download did not start")
	}
	for preparing := 0; preparing < 30; {
		select {
		case report := <-reports:
			if len(report) == 1 {
				if report[0].Status != "preparing" || report[0].PID != 0 {
					t.Fatalf("incomplete package started: %+v", report)
				}
				preparing++
			}
		case <-time.After(time.Second):
			t.Fatal("package download blocked heartbeats")
		}
	}
	desired.Store(false)
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("deleting the service did not cancel its download")
	}
	waitFor(t, func() bool {
		entries, _ := os.ReadDir(filepath.Join(config.DataDir, "packages"))
		return len(entries) == 0
	})
}

func TestPackagePreparationRejectsUnsafeOrCorruptFiles(t *testing.T) {
	for _, entry := range []packageEntry{
		{name: "../escaped", mode: 0644, data: []byte("escape")},
		{name: "/escaped", mode: 0644, data: []byte("escape")},
		{name: "link", mode: os.ModeSymlink | 0777, data: []byte("../escaped")},
		{name: "pipe", mode: os.ModeNamedPipe | 0600},
	} {
		t.Run(entry.name, func(t *testing.T) {
			archive := makePackage(t, entry)
			testInvalidPackage(t, archive, packageID(archive), "unsupported package path")
		})
	}
	t.Run("wrong checksum", func(t *testing.T) {
		archive := makePackage(t, packageEntry{name: "file", mode: 0644, data: []byte("file")})
		testInvalidPackage(t, archive, strings.Repeat("0", sha256.Size*2), "checksum")
	})
	t.Run("corrupt file", func(t *testing.T) {
		archive := makePackage(t, packageEntry{name: "file", mode: 0644, data: []byte("original-payload")})
		index := bytes.Index(archive, []byte("original-payload"))
		if index < 0 {
			t.Fatal("test archive did not contain stored payload")
		}
		archive[index] = 'x'
		testInvalidPackage(t, archive, packageID(archive), "checksum")
	})
}

func TestRemovingReadyPackageNeverLaunchesIt(t *testing.T) {
	archive := makePackage(t, packageEntry{name: "run", mode: 0755, data: []byte("#!/bin/sh\nexit 0\n")})
	id := packageID(archive)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(archive)
	}))
	defer server.Close()
	store := &packageStore{baseURL: server.URL, dir: t.TempDir(), client: server.Client()}
	children := processes{instances: make(map[string]*process), ctx: context.Background(), packages: store}
	t.Cleanup(children.stop)
	children.apply([]api.Assignment{{ID: "instance-a", Package: id, Command: "./run"}})
	waitFor(t, func() bool {
		entries, _ := os.ReadDir(filepath.Join(store.dir, "instances"))
		if len(entries) == 0 {
			return false
		}
		_, err := os.Stat(filepath.Join(store.dir, "instances", entries[0].Name(), "run"))
		return err == nil
	})
	children.apply(nil)
	children.stop()
	entries, err := os.ReadDir(filepath.Join(store.dir, "instances"))
	if err != nil || len(entries) != 0 || len(children.status()) != 0 {
		t.Fatalf("deleted preparation left a process or working directory: %v, %v", entries, err)
	}
}

func testInvalidPackage(t *testing.T, archive []byte, id, message string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(archive)
	}))
	defer server.Close()
	store := &packageStore{baseURL: server.URL, dir: t.TempDir(), client: server.Client()}
	children := processes{instances: make(map[string]*process), ctx: context.Background(), packages: store}
	t.Cleanup(children.stop)
	assignment := api.Assignment{ID: "instance-a", Package: id, Command: "/bin/true"}
	children.apply([]api.Assignment{assignment})
	waitFor(t, func() bool {
		children.apply([]api.Assignment{assignment})
		return children.status()[0].Status == "failed"
	})
	status := children.status()[0]
	if status.PID != 0 || !strings.Contains(status.Error, message) {
		t.Fatalf("invalid package was accepted: %+v", status)
	}
	entries, _ := os.ReadDir(filepath.Join(store.dir, "instances"))
	if len(entries) != 0 {
		t.Fatalf("invalid package left partially extracted files: %v", entries)
	}
	_, err := os.Stat(filepath.Join(store.dir, "escaped"))
	if !os.IsNotExist(err) {
		t.Fatalf("package wrote outside its instance directory: %v", err)
	}
}

type packageEntry struct {
	name string
	mode os.FileMode
	data []byte
}

func makePackage(t *testing.T, entries ...packageEntry) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Store}
		header.SetMode(entry.mode)
		file, err := writer.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write(entry.data)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func packageID(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
