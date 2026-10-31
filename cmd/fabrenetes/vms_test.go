package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCoordinatorUsesCustomVMDiscovery(t *testing.T) {
	dir := t.TempDir()
	command := filepath.Join(dir, "list vms")
	log := filepath.Join(dir, "discovery")
	t.Setenv("fabrenetes_test_discovery_log", log)
	script := `#!/bin/sh
printf 'discovered' > "$fabrenetes_test_discovery_log"
printf '%s' '[{"name":"other-worker","ip":"192.0.2.9"}]'
`
	err := os.WriteFile(command, []byte(script), 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = runCoordinator(context.Background(), []string{
		"--worker-vms", "worker-1",
		"--vm-list-command", command,
		"--vm-run-command", filepath.Join(dir, "run on vm"),
	})
	if err == nil || !strings.Contains(err.Error(), `VM "worker-1" does not exist`) {
		t.Fatalf("unexpected discovery error: %v", err)
	}
	output, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != "discovered" {
		t.Fatalf("custom discovery did not run: %q", output)
	}
}
