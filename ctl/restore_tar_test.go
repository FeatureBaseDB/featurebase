package ctl

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/test"
)

func TestRestoreTarCommand_Run(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	cmLog := logger.NewStandardLogger(io.Discard)
	cm := NewRestoreTarCommand(cmLog)
	hostport := cmd.API.Node().URI.HostPort()
	cm.Host = hostport
	cm.Path = ""
	err := cm.Run(context.Background())
	if !errors.Is(err, UsageError) {
		t.Fatalf("expected usage error with empty path, got %v", err)
	}
	cm.Path = "nonexistent-file"
	err = cm.Run(context.Background())
	if _, ok := err.(*os.PathError); !ok {
		t.Fatalf("expected path error with nonexistent path, got %v", err)
	}
	// use stdin
	cm.Path = "-"

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

	if err := cm.Run(context.Background()); err != nil {
		t.Fatalf("RestoreTarCommand Run error: %s", err)
	}
}
