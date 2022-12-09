// Copyright 2022 Molecula Corp. All rights reserved.
package ctl

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/featurebasedb/featurebase/v3/logger"
)

func TestKeygenCommand_Run(t *testing.T) {
	cmLog := logger.NewStandardLogger(os.Stderr)
	cm := NewKeygenCommand(cmLog)
	buf := &bytes.Buffer{}
	cm.stdout = buf
	err := cm.Run(context.Background())
	if err != nil {
		t.Fatalf("Keygen Run doesn't work: %s", err)
	}
	if !strings.Contains(buf.String(), "secret-key =") {
		t.Fatalf("Unexpected output: %s", buf.String())
	}
}
