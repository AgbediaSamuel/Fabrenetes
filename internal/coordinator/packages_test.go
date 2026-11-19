package coordinator

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"fabrenetes/internal/api"
)

func TestPackageUploadDownloadAndAssignment(t *testing.T) {
	directory := t.TempDir()
	server := New(5 * time.Second)
	err := server.ConfigurePackages(directory)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	data := applicationZIP(t)
	digest := sha256.Sum256(data)
	wantID := hex.EncodeToString(digest[:])
	for range 2 {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/packages", bytes.NewReader(data))
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("upload: %d %s", response.Code, response.Body.String())
		}
		var uploaded api.Package
		err = json.Unmarshal(response.Body.Bytes(), &uploaded)
		if err != nil || uploaded.ID != wantID {
			t.Fatalf("unexpected package: %+v %v", uploaded, err)
		}
	}
	files, err := os.ReadDir(directory)
	if err != nil || len(files) != 1 || files[0].Name() != wantID+".zip" {
		t.Fatalf("duplicate or partial packages left behind: %v %v", files, err)
	}
	response := sendRequest(handler, http.MethodGet, "/packages/"+wantID, "")
	if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), data) || response.Header().Get("Content-Type") != "application/zip" {
		t.Fatalf("download: %d %s", response.Code, response.Header())
	}
	worker := registerWorker(t, handler, "worker-a")
	response = sendJSON(t, handler, http.MethodPost, "/services", api.ServiceSpec{Name: "uppercase", Package: strings.ToUpper(wantID), Command: "./app", Replicas: 1})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("non-canonical package ID accepted: %d", response.Code)
	}
	service := createService(t, handler, api.ServiceSpec{
		Name: "clock", Package: wantID, Command: "./runtime/node", Args: []string{"server.js"}, Replicas: 1,
	})
	assignment := heartbeatWorker(t, handler, worker, nil).Assignments[0]
	if assignment.Package != wantID || assignment.Command != "./runtime/node" || len(assignment.Args) != 1 || assignment.Args[0] != "server.js" {
		t.Fatalf("package launch details not assigned: %+v", assignment)
	}
	heartbeatWorker(t, handler, worker, []api.InstanceStatus{{ID: service.Instances[0].ID, Status: "preparing"}})
	if listServices(t, handler)[0].Instances[0].Status != "preparing" {
		t.Fatal("package preparation status was not stored")
	}
	response = sendJSON(t, handler, http.MethodPost, "/workers/worker-a/heartbeat", api.HeartbeatRequest{
		SessionID: worker.SessionID, Instances: []api.InstanceStatus{{ID: service.Instances[0].ID, Status: "preparing", PID: 42}},
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("preparing instance with PID accepted: %d", response.Code)
	}
	restarted := New(5 * time.Second)
	err = restarted.ConfigurePackages(directory)
	if err != nil {
		t.Fatal(err)
	}
	response = sendRequest(restarted.Handler(), http.MethodGet, "/packages/"+wantID, "")
	if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), data) {
		t.Fatal("stored package was lost when the coordinator was recreated")
	}
}

func TestPackageErrorsDoNotCreateServicesOrPartialFiles(t *testing.T) {
	directory := t.TempDir()
	server := New(5 * time.Second)
	err := server.ConfigurePackages(directory)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	for _, reader := range []io.Reader{strings.NewReader("not a ZIP"), iotest.ErrReader(io.ErrUnexpectedEOF)} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/packages", reader)
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid upload: %d %s", response.Code, response.Body.String())
		}
	}
	files, err := os.ReadDir(directory)
	if err != nil || len(files) != 0 {
		t.Fatalf("invalid upload left files behind: %v %v", files, err)
	}
	for _, id := range []string{"invalid", strings.Repeat("0", sha256.Size*2)} {
		response := sendJSON(t, handler, http.MethodPost, "/services", api.ServiceSpec{Name: "missing", Package: id, Command: "./app", Replicas: 1})
		if response.Code != http.StatusBadRequest || len(listServices(t, handler)) != 0 {
			t.Fatalf("missing package created service: %d %s", response.Code, response.Body.String())
		}
		response = sendRequest(handler, http.MethodGet, "/packages/"+id, "")
		if response.Code != http.StatusNotFound {
			t.Fatalf("missing download: %d", response.Code)
		}
	}
}

func applicationZIP(t *testing.T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	file, err := archive.Create("server.js")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.WriteString(file, "console.log('clock')")
	if err != nil {
		t.Fatal(err)
	}
	err = archive.Close()
	if err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
