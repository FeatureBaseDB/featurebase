// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"os"
	"testing"

	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/spf13/cobra"
)

func TestBuildServerFlags(t *testing.T) {
	cm := &cobra.Command{}
	Server := server.NewCommand(os.Stderr)
	BuildServerFlags(cm, Server)
	if cm.Flags().Lookup("data-dir").Name == "" {
		t.Fatal("data-dir flag is required")
	}
	if cm.Flags().Lookup("log-path").Name == "" {
		t.Fatal("log-path flag is required")
	}
	if cm.Flags().Lookup("verchk-address").Name == "" {
		t.Fatal("verchk-address flag is required")
	}
	if cm.Flags().Lookup("uuid-file").Name == "" {
		t.Fatal("uuid-file flag is required")
	}
}
