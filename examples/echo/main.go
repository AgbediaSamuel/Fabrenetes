package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
)

func main() {
	listen := flag.String("listen", ":9000", "HTTP address to listen on")
	flag.Parse()

	port := os.Getenv("service_port")
	if port != "" {
		host, _, err := net.SplitHostPort(*listen)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		*listen = net.JoinHostPort(host, port)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(struct {
			Worker  string `json:"worker"`
			Service string `json:"service"`
			PID     int    `json:"pid"`
			Message string `json:"message"`
		}{
			Worker:  os.Getenv("worker_id"),
			Service: os.Getenv("service_name"),
			PID:     os.Getpid(),
			Message: r.URL.Query().Get("message"),
		})
	})
	fmt.Printf("Echo service listening on %s (pid %d)\n", *listen, os.Getpid())
	err := http.ListenAndServe(*listen, mux)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
