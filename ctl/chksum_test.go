// Copyright 2022 Molecula Corp. All rights reserved.
package ctl

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/test"
)

func TestChkSumCommand_Run(t *testing.T) {
	cmLog := logger.NewStandardLogger(os.Stderr)
	buf := &bytes.Buffer{}
	cm := NewChkSumCommand(cmLog, buf)

	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	cm.Host = cmd.API.Node().URI.HostPort()

	err := cm.Run(context.Background())
	if err != nil {
		t.Fatalf("ChkSum Run doesn't work: %s", err)
	}
	//ChkSumCommand is only used in executor_test.go, so for now we're just
	//making sure that it runs at all, not checking output.
}
