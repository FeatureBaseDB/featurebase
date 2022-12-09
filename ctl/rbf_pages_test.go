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

func TestRBFPagesCommand_Run(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		want := `
ID       TYPE       EXTRA
======== ========== ====================
0        meta       pageN=4,walid=4,rootrec=1,freelist=2
1        rootrec    next=0
2        leaf       flags=x2,celln=0
3        leaf       flags=x2,celln=1
`[1:]

		cmLog := logger.NewStandardLogger(os.Stderr)
		cmd := NewRBFPagesCommand(cmLog)
		buf := &bytes.Buffer{}
		cmd.stdout = buf
		cmd.Path = filepath.Join("testdata", "ok")
		if err := cmd.Run(context.Background()); err != nil {
			t.Fatal(err)
		} else if got := buf.String(); got != want {
			t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
		}
	})

	t.Run("ErrInvalidPageType", func(t *testing.T) {
		want := `
ID       TYPE       EXTRA
======== ========== ====================
0        meta       pageN=5,walid=4,rootrec=1,freelist=2
1        rootrec    next=0
2        leaf       flags=x2,celln=0
3        leaf       flags=x2,celln=1
4        unknown [<nil>]
`[1:]

		cmLog := logger.NewStandardLogger(os.Stderr)
		cmd := NewRBFPagesCommand(cmLog)
		buf := &bytes.Buffer{}
		cmd.stdout = buf
		cmd.Path = filepath.Join("testdata", "err-invalid-page-type")
		if err := cmd.Run(context.Background()); err != nil {
			t.Fatal(err)
		} else if got := buf.String(); got != want {
			t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
		}
	})
}
