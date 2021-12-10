// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"testing"

	"github.com/molecula/featurebase/v2/testhook"
)

// mustOpenIndex returns a new, opened index at a temporary path. Panic on error.
func mustOpenIndex(tb testing.TB, opt IndexOptions) *Index {
	path, err := testhook.TempDirInDir(tb, *TempDir, "pilosa-index-")
	if err != nil {
		panic(err)
	}
	h := NewHolder(path, mustHolderConfig())
	index, err := h.CreateIndex("i", opt)
	testhook.Cleanup(tb, func() {
		h.Close()
	})

	if err != nil {
		panic(err)
	}

	index.keys = opt.Keys
	index.trackExistence = opt.TrackExistence

	return index
}

// reopen closes the index and reopens it.
func (i *Index) reopen() error {
	if err := i.Close(); err != nil {
		return err
	}
	if err := i.Open(); err != nil {
		return err
	}
	return nil
}
