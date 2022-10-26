// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"testing"
)

// mustOpenIndex returns a new, opened index at a temporary path. Panic on error.
func mustOpenIndex(tb testing.TB, opt IndexOptions) *Index {
	h := newTestHolder(tb)
	index, err := h.CreateIndex("i", "", opt)

	if err != nil {
		panic(err)
	}

	index.keys = opt.Keys
	index.trackExistence = opt.TrackExistence

	return index
}
