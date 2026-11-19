package state

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fabrenetes/internal/api"
)

func TestPartitionedDictionaryRoutesAndRecovers(t *testing.T) {
	nodes, group := startPartitionedCluster(t, 3)
	keys := make([]string, group.PartitionCount())
	for i := 0; ; i++ {
		key := fmt.Sprintf("key-%d", i)
		keys[group.PartitionFor(key)] = key
		complete := true
		for _, key := range keys {
			complete = complete && key != ""
		}
		if complete {
			break
		}
	}
	for partition, key := range keys {
		awaitLeader(t, nodes, group.PartitionID(partition))
		value := fmt.Sprintf(`{"partition":%d}`, partition)
		expectRequest(t, nodes[partition], group.ID, http.MethodPut, key, value, http.StatusNoContent)
		for _, node := range nodes {
			observed := expectRequest(t, node, group.ID, http.MethodGet, key, "", http.StatusOK, value)
			if observed != strconv.Itoa(partition) {
				t.Fatalf("key %q reported partition %q, want %d", key, observed, partition)
			}
			awaitValue(t, node, group.PartitionID(partition), key, value)
			for other := range group.PartitionCount() {
				if other == partition {
					continue
				}
				_, exists := node.manager.groups[group.PartitionID(other)].dictionary.get(key)
				if exists {
					t.Fatalf("key %q appeared in partition %d", key, other)
				}
			}
		}
	}
	for _, node := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		statuses := node.manager.Status(ctx)
		cancel()
		if len(statuses) != group.PartitionCount() {
			t.Fatalf("unexpected state status: %+v", statuses)
		}
		for partition, status := range statuses {
			if status.GroupID != group.ID || status.Partition != partition || status.Term == 0 || status.LeaderID == "" {
				t.Fatalf("partition identity or leadership missing: %+v", status)
			}
			if status.Role == "leader" {
				if !status.Available || status.LeaderID != node.id {
					t.Fatalf("leader did not verify its majority: %+v", status)
				}
			} else if status.Available {
				t.Fatalf("follower reported a verified leader: %+v", status)
			}
		}
	}
	leader := awaitLeader(t, nodes, group.PartitionID(0))
	err := nodes[leader].manager.groups[group.PartitionID(0)].node.Snapshot().Error()
	if err != nil {
		t.Fatal(err)
	}
	stopTestNode(t, nodes[leader])
	for partition, key := range keys {
		awaitLeader(t, nodes, group.PartitionID(partition))
		value := fmt.Sprintf(`{"updated":%d}`, partition)
		expectRequest(t, nodes[(leader+1)%len(nodes)], group.ID, http.MethodPut, key, value, http.StatusNoContent)
	}
	restartTestNode(t, nodes[leader], []api.StateGroup{group})
	for partition, key := range keys {
		awaitValue(t, nodes[leader], group.PartitionID(partition), key, fmt.Sprintf(`{"updated":%d}`, partition))
	}
	for _, node := range nodes {
		stopTestNode(t, node)
	}
	for _, node := range nodes {
		restartTestNode(t, node, []api.StateGroup{group})
	}
	for partition, key := range keys {
		awaitLeader(t, nodes, group.PartitionID(partition))
		expectRequest(t, nodes[0], group.ID, http.MethodGet, key, "", http.StatusOK, fmt.Sprintf(`{"updated":%d}`, partition))
	}
	expectRequest(t, nodes[1], group.ID, http.MethodDelete, keys[0], "", http.StatusNoContent)
	expectRequest(t, nodes[2], group.ID, http.MethodGet, keys[0], "", http.StatusNotFound)
}

func TestPartitionLayoutCannotChangeAfterRestart(t *testing.T) {
	nodes, group := startPartitionedCluster(t, 2)
	for partition := range group.PartitionCount() {
		awaitLeader(t, nodes, group.PartitionID(partition))
	}
	stopTestNode(t, nodes[0])
	startTestNode(t, nodes[0])
	for _, changed := range []api.StateGroup{
		{ID: group.ID, Members: group.Members, Partitioning: "hash", Partitions: 3},
		{ID: group.ID, Members: group.Members, Partitioning: "none", Partitions: 1},
	} {
		err := nodes[0].manager.Apply([]api.StateGroup{changed})
		if err == nil || !strings.Contains(err.Error(), "saved layout") {
			t.Fatalf("partition layout changed after restart: %v", err)
		}
		if len(nodes[0].manager.groups) != 0 {
			t.Fatal("rejected layout opened replacement partitions")
		}
	}
	err := nodes[0].manager.Apply([]api.StateGroup{group})
	if err != nil {
		t.Fatal(err)
	}
	for partition := range group.PartitionCount() {
		awaitLeader(t, nodes, group.PartitionID(partition))
	}
}

func TestLegacyDictionaryKeepsItsPartition(t *testing.T) {
	nodes, groups := startCluster(t, "legacy-dictionary")
	group := groups[0]
	awaitLeader(t, nodes, group.ID)
	expectRequest(t, nodes[0], group.ID, http.MethodPut, "message", `"retained"`, http.StatusNoContent)
	stopTestNode(t, nodes[0])
	identityDirectory := nodes[0].manager.groupDirectory(group.ID)
	err := os.Remove(filepath.Join(identityDirectory, "layout.json"))
	if err != nil {
		t.Fatal(err)
	}
	startTestNode(t, nodes[0])
	changed := group
	changed.Partitioning = "hash"
	changed.Partitions = 2
	err = nodes[0].manager.Apply([]api.StateGroup{changed})
	if err == nil || !strings.Contains(err.Error(), "cannot be repartitioned") {
		t.Fatalf("legacy dictionary was silently repartitioned: %v", err)
	}
	err = nodes[0].manager.Apply(groups)
	if err != nil {
		t.Fatal(err)
	}
	awaitValue(t, nodes[0], group.ID, "message", `"retained"`)
	expectRequest(t, nodes[0], group.ID, http.MethodGet, "message", "", http.StatusOK, `"retained"`)
}

func TestIsolatedLeaderStatusIsUnavailable(t *testing.T) {
	nodes, groups := startCluster(t, "dictionary")
	leader := awaitLeader(t, nodes, groups[0].ID)
	current := nodes[leader].manager.groups[groups[0].ID]
	err := current.transport.Close()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	statuses := nodes[leader].manager.Status(ctx)
	if len(statuses) != 1 || statuses[0].Available {
		t.Fatalf("isolated leader reported available state: %+v", statuses)
	}
}

func startPartitionedCluster(t *testing.T, partitions int) ([]*testNode, api.StateGroup) {
	t.Helper()
	nodes := make([]*testNode, 3)
	group := api.StateGroup{ID: "partitioned", Partitioning: "hash", Partitions: partitions}
	for i := range nodes {
		nodes[i] = &testNode{id: fmt.Sprintf("worker-%d", i), directory: filepath.Join(t.TempDir(), "state")}
		startTestNode(t, nodes[i])
		group.Members = append(group.Members, api.StateMember{ID: nodes[i].id, Address: nodes[i].address})
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			stopTestNode(t, node)
		}
	})
	for _, node := range nodes {
		err := node.manager.Apply([]api.StateGroup{group})
		if err != nil {
			t.Fatal(err)
		}
	}
	return nodes, group
}
