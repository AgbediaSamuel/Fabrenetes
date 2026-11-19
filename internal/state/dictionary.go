package state

import (
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type operation struct {
	Method string          `json:"method"`
	Key    string          `json:"key"`
	Value  json.RawMessage `json:"value,omitempty"`
}

type dictionary struct {
	mu     sync.RWMutex
	values map[string]json.RawMessage
}

func (d *dictionary) Apply(entry *raft.Log) any {
	var change operation
	err := json.Unmarshal(entry.Data, &change)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	switch change.Method {
	case "put":
		if !json.Valid(change.Value) {
			return errors.New("value must be valid JSON")
		}
		d.values[change.Key] = append(json.RawMessage(nil), change.Value...)
	case "delete":
		delete(d.values, change.Key)
	default:
		return errors.New("unknown dictionary operation")
	}
	return nil
}

func (d *dictionary) get(key string) (json.RawMessage, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	value, exists := d.values[key]
	return append(json.RawMessage(nil), value...), exists
}

func (d *dictionary) Snapshot() (raft.FSMSnapshot, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	values := make(map[string]json.RawMessage, len(d.values))
	for key, value := range d.values {
		values[key] = append(json.RawMessage(nil), value...)
	}
	return &dictionarySnapshot{values: values}, nil
}

func (d *dictionary) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	values := make(map[string]json.RawMessage)
	err := json.NewDecoder(reader).Decode(&values)
	if err != nil {
		return err
	}
	if values == nil {
		return errors.New("dictionary snapshot must be an object")
	}
	d.mu.Lock()
	d.values = values
	d.mu.Unlock()
	return nil
}

type dictionarySnapshot struct {
	values map[string]json.RawMessage
}

func (s *dictionarySnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.values)
	if err != nil {
		sink.Cancel()
		return err
	}
	err = sink.Close()
	if err != nil {
		sink.Cancel()
	}
	return err
}

func (s *dictionarySnapshot) Release() {}
