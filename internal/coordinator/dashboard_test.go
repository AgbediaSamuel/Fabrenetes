package coordinator

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDashboardRoute(t *testing.T) {
	handler := New(5 * time.Second).Handler()
	response := sendRequest(handler, http.MethodGet, "/", "")
	if response.Code != http.StatusOK || !strings.HasPrefix(response.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("dashboard: %d %s", response.Code, response.Header())
	}
	if !strings.Contains(response.Body.String(), "Create service") {
		t.Fatal("dashboard page is missing")
	}
	response = sendRequest(handler, http.MethodGet, "/missing", "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown route: %d", response.Code)
	}
}

func TestDashboardAssets(t *testing.T) {
	handler := New(5 * time.Second).Handler()
	for _, asset := range []struct {
		path        string
		contentType string
	}{
		{path: "/dashboard/styles.css", contentType: "text/css"},
		{path: "/dashboard/app.js", contentType: "javascript"},
	} {
		response := sendRequest(handler, http.MethodGet, asset.path, "")
		if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), asset.contentType) {
			t.Fatalf("asset %s: %d %s", asset.path, response.Code, response.Header())
		}
	}
}
