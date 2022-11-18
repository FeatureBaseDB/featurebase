// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestRestoreCommand_Run(t *testing.T) {
	cm := NewRestoreCommand(os.Stdin, os.Stdout, os.Stderr)
	cm.Path = ""
	err := cm.Run(context.Background())
	if !errors.Is(err, UsageError) {
		t.Fatalf("expected usage error, got %v", err)
	}
	cm.Path = "foo"
	cm.Concurrency = 0
	err = cm.Run(context.Background())
	if !errors.Is(err, UsageError) {
		t.Fatalf("expected usage error, got %v", err)
	}
}
