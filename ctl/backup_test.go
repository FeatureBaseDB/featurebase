// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/featurebasedb/featurebase/v3/logger"
)

func TestBackupCommand_Run(t *testing.T) {
	cmLog := logger.NewStandardLogger(io.Discard)
	cm := NewBackupCommand(cmLog)
	cm.OutputDir = ""
	err := cm.Run(context.Background())
	if !errors.Is(err, ErrUsage) {
		t.Fatalf("expected usage error, got %v", err)
	}
	cm.OutputDir = "foo"
	cm.Concurrency = 0
	err = cm.Run(context.Background())
	if !errors.Is(err, ErrUsage) {
		t.Fatalf("expected usage error, got %v", err)
	}
	cm.Concurrency = 1
	cm.HeaderTimeoutStr = "until the cat wakes up"
	err = cm.Run(context.Background())
	if !errors.Is(err, ErrUsage) {
		t.Fatalf("expected usage error, got %v", err)
	}
}
