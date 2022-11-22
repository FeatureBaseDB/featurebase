// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/logger"
)

func TestGenerateConfigCommand_Run(t *testing.T) {
	cmLog := logger.NewStandardLogger(os.Stderr)
	cm := NewGenerateConfigCommand(cmLog)
	buf := &bytes.Buffer{}
	cm.stdout = buf
	err := cm.Run(context.Background())
	if err != nil {
		t.Fatalf("Config Run doesn't work: %s", err)
	}
	if !strings.Contains(buf.String(), ":10101") {
		t.Fatalf("Unexpected config: %s", buf.String())
	}
}
