package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"

	"fabrenetes/collections"
)

func main() {
	listen := flag.String("listen", ":9000", "HTTP address to listen on")
	flag.Parse()

	err := run(*listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(listen string) error {
	dictionary, err := collections.New("")
	if err != nil {
		return err
	}
	port := os.Getenv("service_port")
	if port != "" {
		host, _, err := net.SplitHostPort(listen)
		if err != nil {
			return err
		}
		listen = net.JoinHostPort(host, port)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /value", func(w http.ResponseWriter, r *http.Request) {
		var value json.RawMessage
		found, err := dictionary.Get(r.Context(), r.URL.Query().Get("key"), &value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if !found {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(value)
	})
	mux.HandleFunc("PUT /value", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || !json.Valid(body) {
			http.Error(w, "body must be valid JSON", http.StatusBadRequest)
			return
		}
		err = dictionary.Put(r.Context(), r.URL.Query().Get("key"), json.RawMessage(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /value", func(w http.ResponseWriter, r *http.Request) {
		err := dictionary.Delete(r.Context(), r.URL.Query().Get("key"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	fmt.Printf("Dictionary service listening on %s (pid %d)\n", listen, os.Getpid())
	return http.ListenAndServe(listen, mux)
}
