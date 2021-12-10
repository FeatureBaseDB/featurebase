package ctl

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/server"
)

func TestConfigCommand_Run(t *testing.T) {
	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()
	cm := NewConfigCommand(stdin, w, os.Stderr)
	cm.Config = server.NewConfig()

	err := cm.Run(context.Background())
	if err != nil {
		t.Fatalf("Config Run doesn't work: %s", err)
	}
	w.Close()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(buf.String(), ":10101") {
		t.Fatalf("Unexpected config: \n%s", buf.String())
	}
}
