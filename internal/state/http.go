package state

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"fabrenetes/internal/api"

	"github.com/hashicorp/raft"
)

const forwardedHeader = "Fabrenetes-State-Forwarded"

func (m *Manager) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("CONNECT /state/{id}/raft", m.connect)
	mux.HandleFunc("GET /state/{id}/values", m.value)
	mux.HandleFunc("PUT /state/{id}/values", m.value)
	mux.HandleFunc("DELETE /state/{id}/values", m.value)
	return mux
}

func (m *Manager) find(id string) *replica {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.groups[id]
}

func (m *Manager) value(w http.ResponseWriter, request *http.Request) {
	key := request.URL.Query().Get("key")
	m.mu.RLock()
	definition, exists := m.dictionaries[request.PathValue("id")]
	var current *replica
	if exists {
		current = m.groups[definition.PartitionID(definition.PartitionFor(key))]
	}
	m.mu.RUnlock()
	if current == nil {
		writeError(w, http.StatusServiceUnavailable, "state group is unavailable")
		return
	}
	w.Header().Set("Fabrenetes-Partition", strconv.Itoa(current.partition))
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	if current.node.State() != raft.Leader {
		m.forward(w, request, current)
		return
	}
	if request.Method == http.MethodGet {
		err := waitFor(request.Context(), current.node.Barrier(0))
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		value, exists := current.dictionary.get(key)
		if !exists {
			writeError(w, http.StatusNotFound, "key does not exist")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(value)
		return
	}
	change := operation{Method: "delete", Key: key}
	if request.Method == http.MethodPut {
		value, err := io.ReadAll(request.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "could not read value")
			return
		}
		if !json.Valid(value) {
			writeError(w, http.StatusBadRequest, "value must be valid JSON")
			return
		}
		change.Method = "put"
		change.Value = value
	}
	payload, err := json.Marshal(change)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	future := current.node.Apply(payload, 0)
	err = waitFor(request.Context(), future)
	if err == nil {
		err, _ = future.Response().(error)
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *Manager) forward(w http.ResponseWriter, request *http.Request, current *replica) {
	_, leaderID := current.node.LeaderWithID()
	if leaderID == "" || request.Header.Get(forwardedHeader) != "" {
		writeError(w, http.StatusServiceUnavailable, "state leader is unavailable")
		return
	}
	var address string
	for _, member := range current.definition.Members {
		if member.ID == string(leaderID) {
			address = member.Address
			break
		}
	}
	if address == "" || address == m.address {
		writeError(w, http.StatusServiceUnavailable, "state leader is unavailable")
		return
	}
	endpoint := "http://" + address + "/state/" + url.PathEscape(current.definition.ID) + "/values?" + request.URL.RawQuery
	forwarded, err := http.NewRequestWithContext(request.Context(), request.Method, endpoint, request.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	forwarded.Header.Set(forwardedHeader, "1")
	forwarded.Header.Set("Content-Type", request.Header.Get("Content-Type"))
	response, err := m.client.Do(forwarded)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "state leader is unavailable")
		return
	}
	defer response.Body.Close()
	w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
	w.WriteHeader(response.StatusCode)
	io.Copy(w, response.Body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(api.ErrorResponse{Error: message})
}
