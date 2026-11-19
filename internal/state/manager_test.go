package state

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"fabrenetes/internal/api"

	"github.com/hashicorp/raft"
)

type testNode struct {
	id        string
	directory string
	address   string
	manager   *Manager
	server    *httptest.Server
}

func TestDictionaryReplicationAndRecovery(t *testing.T) {
	nodes, groups := startCluster(t, "dictionary", "other-dictionary")
	leader := awaitLeader(t, nodes, groups[0].ID)
	follower := nodes[(leader+1)%len(nodes)]
	value := `{"name":"Samuel","items":[1,2,3]}`
	expectRequest(t, follower, groups[0].ID, http.MethodPut, "profile / ?", value, http.StatusNoContent)
	expectRequest(t, follower, groups[0].ID, http.MethodGet, "profile / ?", "", http.StatusOK, value)
	expectRequest(t, follower, groups[0].ID, http.MethodGet, "missing", "", http.StatusNotFound)
	expectRequest(t, follower, groups[0].ID, http.MethodPut, "null-value", "null", http.StatusNoContent)
	expectRequest(t, follower, groups[0].ID, http.MethodGet, "null-value", "", http.StatusOK, "null")
	expectRequest(t, follower, groups[0].ID, http.MethodDelete, "null-value", "", http.StatusNoContent)
	expectRequest(t, follower, groups[0].ID, http.MethodDelete, "null-value", "", http.StatusNoContent)
	expectRequest(t, follower, groups[0].ID, http.MethodGet, "null-value", "", http.StatusNotFound)
	expectRequest(t, follower, groups[0].ID, http.MethodPut, "broken", "{", http.StatusBadRequest)
	expectRequest(t, follower, groups[0].ID, http.MethodPut, "", "1", http.StatusBadRequest)
	expectRequest(t, follower, "unknown-group", http.MethodGet, "profile", "", http.StatusServiceUnavailable)

	awaitLeader(t, nodes, groups[1].ID)
	expectRequest(t, follower, groups[1].ID, http.MethodPut, "profile / ?", `"another dictionary"`, http.StatusNoContent)
	expectRequest(t, follower, groups[0].ID, http.MethodGet, "profile / ?", "", http.StatusOK, value)

	err := nodes[leader].manager.groups[groups[0].ID].node.Snapshot().Error()
	if err != nil {
		t.Fatal(err)
	}
	stopTestNode(t, nodes[leader])
	awaitLeader(t, nodes, groups[0].ID)
	survivor := nodes[(leader+1)%len(nodes)]
	expectRequest(t, survivor, groups[0].ID, http.MethodGet, "profile / ?", "", http.StatusOK, value)
	expectRequest(t, survivor, groups[0].ID, http.MethodPut, "after-failure", "42", http.StatusNoContent)
	restartTestNode(t, nodes[leader], groups)
	awaitValue(t, nodes[leader], groups[0].ID, "after-failure", "42")

	for _, node := range nodes {
		stopTestNode(t, node)
	}
	for _, node := range nodes {
		restartTestNode(t, node, groups)
	}
	awaitLeader(t, nodes, groups[0].ID)
	awaitLeader(t, nodes, groups[1].ID)
	expectRequest(t, nodes[0], groups[0].ID, http.MethodGet, "profile / ?", "", http.StatusOK, value)
	expectRequest(t, nodes[0], groups[0].ID, http.MethodGet, "after-failure", "", http.StatusOK, "42")
	expectRequest(t, nodes[0], groups[1].ID, http.MethodGet, "profile / ?", "", http.StatusOK, `"another dictionary"`)

	err = nodes[0].manager.Apply(groups[:1])
	if err != nil {
		t.Fatal(err)
	}
	expectRequest(t, nodes[0], groups[1].ID, http.MethodGet, "profile / ?", "", http.StatusServiceUnavailable)
	err = nodes[0].manager.Apply(groups)
	if err != nil {
		t.Fatal(err)
	}
	awaitValue(t, nodes[0], groups[1].ID, "profile / ?", `"another dictionary"`)
}

func TestIsolatedLeaderCannotAcknowledgeOrRead(t *testing.T) {
	nodes, groups := startCluster(t, "dictionary")
	group := groups[0]
	leader := awaitLeader(t, nodes, group.ID)
	current := nodes[leader].manager.groups[group.ID]
	expectRequest(t, nodes[leader], group.ID, http.MethodPut, "value", "1", http.StatusNoContent)
	err := current.transport.Close()
	if err != nil {
		t.Fatal(err)
	}
	expectRequest(t, nodes[leader], group.ID, http.MethodPut, "isolated", "true", http.StatusServiceUnavailable)
	expectRequest(t, nodes[leader], group.ID, http.MethodGet, "value", "", http.StatusServiceUnavailable)
	remaining := []*testNode{nodes[(leader+1)%3], nodes[(leader+2)%3]}
	awaitLeader(t, remaining, group.ID)
	expectRequest(t, remaining[0], group.ID, http.MethodPut, "value", "2", http.StatusNoContent)
	expectRequest(t, remaining[0], group.ID, http.MethodGet, "isolated", "", http.StatusNotFound)
	stopTestNode(t, nodes[leader])
	restartTestNode(t, nodes[leader], groups)
	awaitValue(t, nodes[leader], group.ID, "value", "2")

	newLeader := awaitLeader(t, nodes, group.ID)
	for i, node := range nodes {
		if i != newLeader {
			stopTestNode(t, node)
		}
	}
	expectRequest(t, nodes[newLeader], group.ID, http.MethodGet, "value", "", http.StatusServiceUnavailable)
	expectRequest(t, nodes[newLeader], group.ID, http.MethodPut, "without-majority", "true", http.StatusServiceUnavailable)
}

func startCluster(t *testing.T, ids ...string) ([]*testNode, []api.StateGroup) {
	t.Helper()
	nodes := make([]*testNode, 3)
	for i := range nodes {
		nodes[i] = &testNode{id: fmt.Sprintf("worker-%d", i), directory: filepath.Join(t.TempDir(), "state")}
		startTestNode(t, nodes[i])
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			stopTestNode(t, node)
		}
	})
	groups := make([]api.StateGroup, 0, len(ids))
	for _, id := range ids {
		group := api.StateGroup{ID: id}
		for _, node := range nodes {
			group.Members = append(group.Members, api.StateMember{ID: node.id, Address: node.address})
		}
		groups = append(groups, group)
	}
	for _, node := range nodes {
		err := node.manager.Apply(groups)
		if err != nil {
			t.Fatal(err)
		}
	}
	return nodes, groups
}

func startTestNode(t *testing.T, node *testNode) {
	t.Helper()
	server := httptest.NewUnstartedServer(nil)
	if node.address != "" {
		server.Listener.Close()
		listener, err := net.Listen("tcp", node.address)
		if err != nil {
			t.Fatal(err)
		}
		server.Listener = listener
	}
	node.address = server.Listener.Addr().String()
	manager, err := New(node.id, node.directory, node.address)
	if err != nil {
		server.Listener.Close()
		t.Fatal(err)
	}
	server.Config.Handler = manager.Handler()
	server.Start()
	node.manager = manager
	node.server = server
}

func restartTestNode(t *testing.T, node *testNode, groups []api.StateGroup) {
	t.Helper()
	startTestNode(t, node)
	err := node.manager.Apply(groups)
	if err != nil {
		t.Fatal(err)
	}
}

func stopTestNode(t *testing.T, node *testNode) {
	t.Helper()
	if node.server == nil {
		return
	}
	err := node.manager.Close()
	if err != nil {
		t.Error(err)
	}
	node.server.Close()
	node.server = nil
}

func awaitLeader(t *testing.T, nodes []*testNode, group string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, node := range nodes {
			if node.server == nil {
				continue
			}
			current := node.manager.groups[group]
			if current != nil && current.node.State() == raft.Leader {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := waitFor(ctx, current.node.Barrier(0))
				cancel()
				known := err == nil
				for _, peer := range nodes {
					if peer.server != nil {
						_, leaderID := peer.manager.groups[group].node.LeaderWithID()
						known = known && string(leaderID) == node.id
					}
				}
				if known {
					return i
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("state group did not elect a leader")
	return -1
}

func awaitValue(t *testing.T, node *testNode, group, key, value string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		data, exists := node.manager.groups[group].dictionary.get(key)
		if exists && string(data) == value {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("worker %s did not recover %q", node.id, key)
}

func expectRequest(t *testing.T, node *testNode, group, method, key, body string, status int, expected ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	endpoint := node.server.URL + "/state/" + url.PathEscape(group) + "/values?key=" + url.QueryEscape(key)
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("%s via %s: got HTTP %d: %s, want %d", method, node.id, response.StatusCode, data, status)
	}
	if len(expected) > 0 && string(data) != expected[0] {
		t.Fatalf("got %s, want %s", data, expected[0])
	}
	return response.Header.Get("Fabrenetes-Partition")
}
