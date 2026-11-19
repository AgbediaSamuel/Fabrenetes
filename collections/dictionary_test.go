package collections

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestDictionaryValues(t *testing.T) {
	var mu sync.Mutex
	values := make(map[string]json.RawMessage)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/state/group/values" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		key := r.URL.Query().Get("key")
		switch r.Method {
		case http.MethodGet:
			value, exists := values[key]
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write(value)
		case http.MethodPut:
			if r.Header.Get("Content-Type") != "application/json" {
				t.Error("PUT content type must be application/json")
			}
			var value json.RawMessage
			err := json.NewDecoder(r.Body).Decode(&value)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			values[key] = value
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			delete(values, key)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	dictionary, err := New(server.URL + "/state/group/")
	if err != nil {
		t.Fatal(err)
	}
	key := "customer / 東京?+&"
	var actual any
	found, err := dictionary.Get(context.Background(), key, &actual)
	if err != nil || found {
		t.Fatalf("missing key: found=%v, err=%v", found, err)
	}
	for _, expected := range []any{nil, "hello", float64(42), map[string]any{"active": true, "items": []any{"a", "b"}}} {
		err = dictionary.Put(context.Background(), key, expected)
		if err != nil {
			t.Fatal(err)
		}
		found, err = dictionary.Get(context.Background(), key, &actual)
		if err != nil || !found || !reflect.DeepEqual(actual, expected) {
			t.Fatalf("value: got=%#v, want=%#v, found=%v, err=%v", actual, expected, found, err)
		}
	}
	err = dictionary.Delete(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	found, err = dictionary.Get(context.Background(), key, &actual)
	if err != nil || found {
		t.Fatalf("deleted key: found=%v, err=%v", found, err)
	}
	err = dictionary.Delete(context.Background(), key)
	if err != nil {
		t.Fatalf("deleting absent key: %v", err)
	}
}

func TestNewDictionaryURL(t *testing.T) {
	t.Setenv("state_url", "http://127.0.0.1:9091/state/group")
	dictionary, err := New("")
	if err != nil || dictionary.url != "http://127.0.0.1:9091/state/group" {
		t.Fatalf("environment URL: dictionary=%v, err=%v", dictionary, err)
	}
	t.Setenv("state_url", "")
	for _, address := range []string{"", "localhost:9091", "ftp://localhost/state/group", "http://localhost/?key=x", "http://localhost/?", "http://localhost/#group", "http://%"} {
		_, err = New(address)
		if err == nil {
			t.Errorf("accepted invalid state URL %q", address)
		}
	}
}

func TestDictionaryErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"no state leader is available"}`))
	}))
	defer server.Close()
	dictionary, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	var value any
	_, err = dictionary.Get(context.Background(), "key", &value)
	if err == nil || err.Error() != "no state leader is available" {
		t.Fatalf("GET error: %v", err)
	}
	err = dictionary.Put(context.Background(), "key", "value")
	if err == nil || err.Error() != "no state leader is available" {
		t.Fatalf("PUT error: %v", err)
	}
	err = dictionary.Delete(context.Background(), "key")
	if err == nil || err.Error() != "no state leader is available" {
		t.Fatalf("DELETE error: %v", err)
	}
	err = dictionary.Put(context.Background(), "key", make(chan int))
	if err == nil {
		t.Fatal("expected an error for a value that cannot be JSON encoded")
	}
}

func TestDictionaryInvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not JSON"))
	}))
	defer server.Close()
	dictionary, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	var value any
	_, err = dictionary.Get(context.Background(), "key", &value)
	if err == nil {
		t.Fatal("expected invalid response error")
	}
}

func TestDictionaryCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	dictionary, err := New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		var value any
		_, requestErr := dictionary.Get(ctx, "key", &value)
		result <- requestErr
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach server")
	}
	cancel()
	select {
	case err = <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled request did not return")
	}
}
