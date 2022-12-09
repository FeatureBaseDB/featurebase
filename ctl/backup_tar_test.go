package ctl

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/test"
)

func TestBackupTarCommand_Run(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	cmLog := logger.NewStandardLogger(io.Discard)
	cm := NewBackupTarCommand(cmLog)
	hostport := cmd.API.Node().URI.HostPort()
	cm.Host = hostport
	dir := t.TempDir()
	cm.OutputPath = filepath.Join(dir, "backuptest.tar")

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
	if err := cm.Run(context.Background()); err != nil {
		t.Fatalf("BackupTarCommand Run error: %s", err)
	}
}
