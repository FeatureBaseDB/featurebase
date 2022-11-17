// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/test"
)

func TestExportCommand_Validation(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)

	cm := NewExportCommand(stdin, stdout, stderr)

	err := cm.Run(context.Background())
	if !errContains(err, pilosa.ErrIndexRequired) {
		t.Fatalf("wrong error, expected %q, got: '%s'", pilosa.ErrIndexRequired, err)
	}

	cm.Index = "i"
	err = cm.Run(context.Background())
	if !errContains(err, pilosa.ErrFieldRequired) {
		t.Fatalf("wrong error, expected %q, got: '%s'", pilosa.ErrFieldRequired, err)
	}
}

func TestExportCommand_Run(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewExportCommand(stdin, stdout, stderr)
	hostport := cmd.API.Node().URI.HostPort()
	cm.Host = hostport

	resp, err := http.DefaultClient.Do(test.MustNewHTTPRequest("POST", "http://"+hostport+"/index/i", strings.NewReader("")))
	if err != nil {
		t.Fatalf("making http request: %v", err)
	}
	resp.Body.Close()
	resp, err = http.DefaultClient.Do(test.MustNewHTTPRequest("POST", "http://"+hostport+"/index/i/field/f", strings.NewReader("")))
	if err != nil {
		t.Fatalf("making http request: %v", err)
	}
	resp.Body.Close()

	cm.Index = "i"
	cm.Field = "f"
	if err := cm.Run(context.Background()); err != nil {
		t.Fatalf("Export Run doesn't work: %s", err)
	}
}
