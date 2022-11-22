// Copyright 2022 Molecula Corp. All rights reserved.
package ctl

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/logger"
)

func TestRBFPageCommand_Run(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		cmLog := logger.NewStandardLogger(os.Stderr)
		cmd := NewRBFPageCommand(cmLog)
		buf := &bytes.Buffer{}
		cmd.stdout = buf
		cmd.Path = filepath.Join("testdata", "ok")
		cmd.Pgnos = []uint32{0}
		if err := cmd.Run(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := buf.String(), "Type: meta"; !strings.Contains(got, want) {
			t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
		}
	})

	t.Run("ErrInvalidPageType", func(t *testing.T) {
		t.Skip("RBF panics if we do this")
		cmLog := logger.NewStandardLogger(os.Stderr)
		cmd := NewRBFPageCommand(cmLog)
		buf := &bytes.Buffer{}
		cmd.stdout = buf
		cmd.Path = filepath.Join("testdata", "err-invalid-page-type")
		cmd.Pgnos = []uint32{4}
		if err := cmd.Run(context.Background()); err == nil || err.Error() != `check failed` {
			t.Fatal(err)
		} else if got, want := buf.String(), `page not in-use & not free: pgno=4`+"\n"; got != want {
			t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
		}
	})
}
