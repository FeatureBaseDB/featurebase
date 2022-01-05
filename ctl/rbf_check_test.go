// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestRBFCheckCommand_Run(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		cmd := NewRBFCheckCommand(bytes.NewReader(nil), &stdout, &stderr)
		cmd.Path = filepath.Join("testdata", "rbf-check", "ok")
		if err := cmd.Run(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := stdout.String(), `ok`+"\n"; got != want {
			t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
		}
	})

	t.Run("ErrInvalidPageType", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		cmd := NewRBFCheckCommand(bytes.NewReader(nil), &stdout, &stderr)
		cmd.Path = filepath.Join("testdata", "rbf-check", "err-invalid-page-type")
		if err := cmd.Run(context.Background()); err == nil || err.Error() != `check failed` {
			t.Fatal(err)
		} else if got, want := stdout.String(), `page not in-use & not free: pgno=4`+"\n"; got != want {
			t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
		}
	})
}
