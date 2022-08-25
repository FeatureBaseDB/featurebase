package ctl

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/test"
)

func TestBackupTarCommand_Run(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewBackupTarCommand(stdin, stdout, stderr)
	hostport := cmd.API.Node().URI.HostPort()
	cm.Host = hostport
	cm.OutputPath = "-"

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
