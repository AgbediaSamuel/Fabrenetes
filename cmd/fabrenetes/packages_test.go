package main

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fabrenetes/internal/api"
	"fabrenetes/internal/coordinator"
)

func TestCreateUploadsPackageAndKeepsLaunchArguments(t *testing.T) {
	cluster := coordinator.New(5 * time.Second)
	err := cluster.ConfigurePackages(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(cluster.Handler())
	defer server.Close()
	t.Setenv("coordinator_url", server.URL)
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
	path := filepath.Join(t.TempDir(), "clock package.zip")
	err = os.WriteFile(path, buffer.Bytes(), 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = run(context.Background(), []string{"create", "--name", "clock", "--package", path, "--replicas", "2", "./runtime/node", "server.js", "argument with spaces"})
	if err != nil {
		t.Fatal(err)
	}
	var response api.ServicesResponse
	err = coordinatorRequest(context.Background(), http.MethodGet, server.URL, "services", nil, &response)
	if err != nil {
		t.Fatal(err)
	}
	service := response.Services[0]
	if service.Package == "" || service.Command != "./runtime/node" || service.Replicas != 2 || len(service.Args) != 2 || service.Args[1] != "argument with spaces" {
		t.Fatalf("unexpected package service: %+v", service)
	}
	download, err := http.Get(server.URL + "/packages/" + service.Package)
	if err != nil {
		t.Fatal(err)
	}
	defer download.Body.Close()
	data, err := io.ReadAll(download.Body)
	if err != nil || !bytes.Equal(data, buffer.Bytes()) {
		t.Fatalf("uploaded package bytes differ: %v", err)
	}
}
