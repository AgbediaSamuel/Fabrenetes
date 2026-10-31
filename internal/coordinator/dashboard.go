package coordinator

import (
	"embed"
	"net/http"
)

//go:embed dashboard/*
var dashboardFiles embed.FS

func (*Coordinator) dashboard(w http.ResponseWriter, r *http.Request) {
	http.ServeFileFS(w, r, dashboardFiles, "dashboard/index.html")
}
