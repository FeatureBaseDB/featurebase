// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/testhook"
)

func TestInspectCommand_Run(t *testing.T) {
	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()

	cm := NewInspectCommand(stdin, w, w)
	file, err := testhook.TempFile(t, "inspectTest")
	if err != nil {
		t.Fatalf("Error creating tempfile: %s", err)
	}
	_, err = file.Write([]byte("12358267538963"))
	if err != nil {
		t.Fatalf("writing to tempfile: %v", err)
	}
	file.Close()
	cm.Path = file.Name()
	err = cm.Run(context.Background())
	expectedError := "inspecting: "
	if !strings.Contains(err.Error(), expectedError) {
		t.Fatalf("expected error '%s', got '%v'", expectedError, err)
	}

	w.Close()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	if err != nil {
		t.Fatalf("copying data: %v", err)
	}
	if !strings.Contains(buf.String(), "inspecting bitmap...") {
		t.Fatalf("Inspect doesn't work: %s", err)
	}

	//	Todo: need correct roaring file for happy path
}
