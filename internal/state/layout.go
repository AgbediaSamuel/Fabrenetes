package state

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"fabrenetes/internal/api"
)

func normalizeGroup(group api.StateGroup) api.StateGroup {
	if group.Partitions == 0 {
		group.Partitions = 1
	}
	if group.Partitioning == "" {
		group.Partitioning = "none"
	}
	group.Members = append([]api.StateMember(nil), group.Members...)
	return group
}

func (m *Manager) groupDirectory(id string) string {
	identity := sha256.Sum256([]byte(id))
	return filepath.Join(m.directory, fmt.Sprintf("%x", identity))
}

func (m *Manager) saveLayout(group api.StateGroup) error {
	directory := m.groupDirectory(group.ID)
	err := os.MkdirAll(directory, 0700)
	if err != nil {
		return err
	}
	path := filepath.Join(directory, "layout.json")
	data, err := os.ReadFile(path)
	if err == nil {
		var saved api.StateGroup
		err = json.Unmarshal(data, &saved)
		if err != nil {
			return fmt.Errorf("read state layout: %w", err)
		}
		if !reflect.DeepEqual(normalizeGroup(saved), group) {
			return errors.New("state group membership and partitioning differ from its saved layout")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read state layout: %w", err)
	}
	if group.PartitionCount() != 1 {
		_, err = os.Stat(filepath.Join(directory, "raft.db"))
		if err == nil {
			return errors.New("existing single-partition state cannot be repartitioned")
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	data, err = json.Marshal(group)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".layout-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(file.Name(), path)
}
