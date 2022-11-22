// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/molecula/featurebase/v3/logger"
)

func TestRestoreCommand_Run(t *testing.T) {
	cmLog := logger.NewStandardLogger(io.Discard)
	cm := NewRestoreCommand(cmLog)
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
