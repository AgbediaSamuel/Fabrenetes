package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fabrenetes/internal/api"
	"fabrenetes/internal/coordinator"
)

func TestCreateStatefulService(t *testing.T) {
	server := httptest.NewServer(coordinator.New(time.Minute).Handler())
	defer server.Close()
	for i := range 3 {
		var registered api.Worker
		err := coordinatorRequest(context.Background(), http.MethodPost, server.URL, "workers", api.RegisterRequest{
			ID: fmt.Sprintf("worker-%d", i), Hostname: "local", StateAddress: fmt.Sprintf("127.0.0.1:%d", 9101+i),
		}, &registered)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := run(context.Background(), []string{"create", "--coordinator", server.URL, "--name", "notes", "--stateful", "dictionary"})
	if err != nil {
		t.Fatal(err)
	}
	var response api.ServicesResponse
	err = coordinatorRequest(context.Background(), http.MethodGet, server.URL, "services", nil, &response)
	if err != nil {
		t.Fatal(err)
	}
	service := response.Services[0]
	if !service.Stateful || service.Replicas != 3 || service.State == nil || len(service.State.Members) != 3 {
		t.Fatalf("unexpected stateful service: %+v", service)
	}
	err = run(context.Background(), []string{"create", "--coordinator", server.URL, "--name", "invalid", "--stateful", "--replicas", "2", "dictionary"})
	if err == nil || !strings.Contains(err.Error(), "three replicas") {
		t.Fatalf("invalid stateful replica count: %v", err)
	}
}
