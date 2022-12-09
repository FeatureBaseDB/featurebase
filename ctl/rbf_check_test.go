// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/featurebasedb/featurebase/v3/logger"
)

func TestRBFCheckCommand_Run(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		cmLog := logger.NewStandardLogger(os.Stderr)
		cmd := NewRBFCheckCommand(cmLog)
		buf := &bytes.Buffer{}
		cmd.stdout = buf
		cmd.Path = filepath.Join("testdata", "ok")
		if err := cmd.Run(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := buf.String(), `ok`+"\n"; got != want {
			t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
		}
	})

	t.Run("ErrInvalidPageType", func(t *testing.T) {
		cmLog := logger.NewStandardLogger(os.Stderr)
		cmd := NewRBFCheckCommand(cmLog)
		buf := &bytes.Buffer{}
		cmd.stdout = buf
		cmd.Path = filepath.Join("testdata", "err-invalid-page-type")
		if err := cmd.Run(context.Background()); err == nil || err.Error() != `check failed` {
			t.Fatal(err)
		} else if got, want := buf.String(), `page not in-use & not free: pgno=4`+"\n"; got != want {
			t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
		}
	})
}
