package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"fabrenetes/internal/api"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Manager struct {
	mu           sync.RWMutex
	workerID     string
	directory    string
	address      string
	groups       map[string]*replica
	dictionaries map[string]api.StateGroup
	client       *http.Client
	closed       bool
}

type replica struct {
	definition api.StateGroup
	dictionary *dictionary
	node       *raft.Raft
	transport  *raft.NetworkTransport
	store      *raftboltdb.BoltStore
	stream     *stream
	partition  int
}

func New(workerID, directory, advertisedAddress string) (*Manager, error) {
	err := validateAddress(advertisedAddress)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(directory, 0700)
	if err != nil {
		return nil, err
	}
	return &Manager{
		workerID: workerID, directory: directory, address: advertisedAddress,
		groups: make(map[string]*replica), dictionaries: make(map[string]api.StateGroup), client: &http.Client{},
	}, nil
}

func (m *Manager) Apply(definitions []api.StateGroup) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("state manager is closed")
	}
	desired := make(map[string]bool)
	dictionaries := make(map[string]api.StateGroup, len(definitions))
	for _, requested := range definitions {
		definition := normalizeGroup(requested)
		err := m.validateGroup(definition)
		if err != nil {
			return err
		}
		_, duplicate := dictionaries[definition.ID]
		if duplicate {
			return errors.New("state groups must be unique")
		}
		current, exists := m.dictionaries[definition.ID]
		if exists && !reflect.DeepEqual(current, definition) {
			return errors.New("state group membership and partitioning cannot change")
		}
		for partition := range definition.PartitionCount() {
			id := definition.PartitionID(partition)
			if desired[id] {
				return errors.New("state partition identities must be unique")
			}
			desired[id] = true
		}
		dictionaries[definition.ID] = definition
	}
	for _, definition := range dictionaries {
		_, exists := m.dictionaries[definition.ID]
		if !exists {
			err := m.saveLayout(definition)
			if err != nil {
				return err
			}
		}
	}
	for id, current := range m.groups {
		if !desired[id] {
			err := current.close()
			delete(m.groups, id)
			if err != nil {
				return err
			}
		}
	}
	for _, definition := range dictionaries {
		for partition := range definition.PartitionCount() {
			id := definition.PartitionID(partition)
			if m.groups[id] == nil {
				current, err := m.open(definition, partition)
				if err != nil {
					return fmt.Errorf("open state group %s: %w", id, err)
				}
				m.groups[id] = current
			}
		}
	}
	m.dictionaries = dictionaries
	return nil
}

func (m *Manager) Status(ctx context.Context) []api.StateStatus {
	m.mu.RLock()
	replicas := make([]*replica, 0, len(m.groups))
	for _, current := range m.groups {
		replicas = append(replicas, current)
	}
	m.mu.RUnlock()
	statuses := make([]api.StateStatus, len(replicas))
	var pending sync.WaitGroup
	for i, current := range replicas {
		pending.Add(1)
		go func() {
			defer pending.Done()
			role := current.node.State()
			term, _ := strconv.ParseUint(current.node.Stats()["term"], 10, 64)
			available := false
			if role == raft.Leader {
				err := waitFor(ctx, current.node.VerifyLeader())
				available = err == nil
			}
			role = current.node.State()
			_, leaderID := current.node.LeaderWithID()
			currentTerm, _ := strconv.ParseUint(current.node.Stats()["term"], 10, 64)
			status := api.StateStatus{
				GroupID: current.definition.ID, Partition: current.partition,
				Role: strings.ToLower(role.String()), LeaderID: string(leaderID), Term: currentTerm,
				Available: available && role == raft.Leader && string(leaderID) == m.workerID && term == currentTerm,
			}
			statuses[i] = status
		}()
	}
	pending.Wait()
	sort.Slice(statuses, func(i, j int) bool {
		if statuses[i].GroupID == statuses[j].GroupID {
			return statuses[i].Partition < statuses[j].Partition
		}
		return statuses[i].GroupID < statuses[j].GroupID
	})
	return statuses
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	var result error
	for id, current := range m.groups {
		result = errors.Join(result, current.close())
		delete(m.groups, id)
	}
	clear(m.dictionaries)
	m.client.CloseIdleConnections()
	return result
}

func (m *Manager) open(definition api.StateGroup, partition int) (*replica, error) {
	id := definition.PartitionID(partition)
	directory := m.groupDirectory(id)
	err := os.MkdirAll(directory, 0700)
	if err != nil {
		return nil, err
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(directory, "raft.db"))
	if err != nil {
		return nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(directory, 2, io.Discard)
	if err != nil {
		store.Close()
		return nil, err
	}
	configuration := raft.DefaultConfig()
	configuration.LocalID = raft.ServerID(m.workerID)
	configuration.LogOutput = io.Discard
	layer := &stream{
		address: m.address, groupID: id,
		connections: make(chan net.Conn), done: make(chan struct{}),
	}
	transport := raft.NewNetworkTransport(layer, 2, 3*time.Second, io.Discard)
	state := &dictionary{values: make(map[string]json.RawMessage)}
	existing, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, err
	}
	if !existing && definition.Members[0].ID == m.workerID {
		servers := make([]raft.Server, 0, len(definition.Members))
		for _, member := range definition.Members {
			servers = append(servers, raft.Server{
				ID: raft.ServerID(member.ID), Address: raft.ServerAddress(member.Address),
				Suffrage: raft.Voter,
			})
		}
		err = raft.BootstrapCluster(configuration, store, store, snapshots, transport, raft.Configuration{Servers: servers})
		if err != nil {
			transport.Close()
			store.Close()
			return nil, err
		}
	}
	node, err := raft.NewRaft(configuration, state, store, store, snapshots, transport)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, err
	}
	servers := node.GetConfiguration().Configuration().Servers
	if len(servers) > 0 && !sameMembers(servers, definition.Members) {
		node.Shutdown().Error()
		transport.Close()
		store.Close()
		return nil, errors.New("state group membership differs from its saved configuration")
	}
	return &replica{
		definition: definition, dictionary: state, node: node,
		transport: transport, store: store, stream: layer, partition: partition,
	}, nil
}

func sameMembers(servers []raft.Server, members []api.StateMember) bool {
	if len(servers) != len(members) {
		return false
	}
	for _, server := range servers {
		found := false
		for _, member := range members {
			if server.Suffrage == raft.Voter && string(server.ID) == member.ID && string(server.Address) == member.Address {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (r *replica) close() error {
	err := r.node.Shutdown().Error()
	err = errors.Join(err, r.transport.Close())
	return errors.Join(err, r.store.Close())
}

func (m *Manager) validateGroup(group api.StateGroup) error {
	if group.ID == "" || len(group.Members) != 3 {
		return errors.New("state groups require an ID and three members")
	}
	if group.Partitions < 1 || (group.Partitioning != "none" && group.Partitioning != "hash") || (group.Partitioning == "none" && group.Partitions != 1) {
		return errors.New("partitioning must be none with one partition, or hash with a positive partition count")
	}
	ids := make(map[string]bool)
	addresses := make(map[string]bool)
	local := false
	for _, member := range group.Members {
		if member.ID == "" || ids[member.ID] || addresses[member.Address] {
			return errors.New("state group members require unique IDs and addresses")
		}
		err := validateAddress(member.Address)
		if err != nil {
			return err
		}
		ids[member.ID] = true
		addresses[member.Address] = true
		if member.ID == m.workerID {
			if member.Address != m.address {
				return errors.New("state member address does not match this worker")
			}
			local = true
		}
	}
	if !local {
		return errors.New("state group does not contain this worker")
	}
	return nil
}

func validateAddress(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return errors.New("state address must contain a host and port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("state address requires a valid port")
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsUnspecified() {
		return errors.New("state address must be reachable by other workers")
	}
	return nil
}

func waitFor(ctx context.Context, future raft.Future) error {
	result := make(chan error, 1)
	go func() { result <- future.Error() }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
