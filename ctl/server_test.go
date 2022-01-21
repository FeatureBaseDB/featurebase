// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"bytes"
	"testing"

	"github.com/molecula/featurebase/v3/server"
	"github.com/spf13/cobra"
)

func TestBuildServerFlags(t *testing.T) {
	cm := &cobra.Command{}
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	Server := server.NewCommand(stdin, stdout, stderr)
	BuildServerFlags(cm, Server)
	if cm.Flags().Lookup("data-dir").Name == "" {
		t.Fatal("data-dir flag is required")
	}
	if cm.Flags().Lookup("log-path").Name == "" {
		t.Fatal("log-path flag is required")
	}
}
