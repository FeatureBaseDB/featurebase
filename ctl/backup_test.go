// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestBackupCommand_Run(t *testing.T) {
	cm := NewBackupCommand(os.Stdin, os.Stdout, os.Stderr)
	cm.OutputDir = ""
	err := cm.Run(context.Background())
	if !errors.Is(err, UsageError) {
		t.Fatalf("expected usage error, got %v", err)
	}
	cm.OutputDir = "foo"
	cm.Concurrency = 0
	err = cm.Run(context.Background())
	if !errors.Is(err, UsageError) {
		t.Fatalf("expected usage error, got %v", err)
	}
	cm.Concurrency = 1
	cm.HeaderTimeoutStr = "until the cat wakes up"
	err = cm.Run(context.Background())
	if !errors.Is(err, UsageError) {
		t.Fatalf("expected usage error, got %v", err)
	}
}
