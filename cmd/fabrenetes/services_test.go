package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fabrenetes/internal/api"
	"fabrenetes/internal/coordinator"
)

func TestCreateService(t *testing.T) {
	for _, test := range []struct {
		name     string
		flags    []string
		replicas int
		port     int
	}{
		{name: "default", replicas: 1},
		{name: "explicit", flags: []string{"--replicas", "2"}, replicas: 2},
		{name: "port", flags: []string{"--port", "9001"}, replicas: 1, port: 9001},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(coordinator.New(5 * time.Second).Handler())
			defer server.Close()
			t.Setenv("coordinator_url", server.URL)

			args := append([]string{"create", "--name", "clock"}, test.flags...)
			args = append(args, "/usr/bin/node", "/tmp/time.js")
			err := run(context.Background(), args)
			if err != nil {
				t.Fatal(err)
			}

			var result api.ServicesResponse
			err = coordinatorRequest(context.Background(), http.MethodGet, server.URL, "services", nil, &result)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Services) != 1 {
				t.Fatalf("services = %v", result.Services)
			}
			service := result.Services[0]
			if service.Replicas != test.replicas || len(service.Instances) != test.replicas {
				t.Fatalf("service = %+v", service)
			}
			if service.Port != test.port {
				t.Fatalf("port = %d; want %d", service.Port, test.port)
			}
			if service.Command != "/usr/bin/node" || len(service.Args) != 1 || service.Args[0] != "/tmp/time.js" {
				t.Fatalf("command/args = %s %v", service.Command, service.Args)
			}
		})
	}
}

func TestCreateCoordinatorOverride(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(api.ErrorResponse{Error: "service already exists"})
	}))
	defer server.Close()
	t.Setenv("coordinator_url", "http://127.0.0.1:1")

	err := run(context.Background(), []string{"create", "--coordinator", server.URL, "--name", "clock", "/usr/bin/node"})
	if err == nil || err.Error() != "service already exists" {
		t.Fatalf("error = %v", err)
	}
}
