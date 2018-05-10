// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"io/ioutil"
	"os"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/boltdb"
)

// Holder is a test wrapper for pilosa.Holder.
type Holder struct {
	*pilosa.Holder
}

// NewHolder returns a new instance of Holder with a temporary path.
func NewHolder() *Holder {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	h := &Holder{Holder: pilosa.NewHolder()}
	h.Path = path
	h.Holder.NewAttrStore = boltdb.NewAttrStore

	return h
}

// MustOpenHolder creates and opens a holder at a temporary path. Panic on error.
func MustOpenHolder() *Holder {
	h := NewHolder()
	if err := h.Open(); err != nil {
		panic(err)
	}
	return h
}

// Close closes the holder and removes all underlying data.
func (h *Holder) Close() error {
	defer os.RemoveAll(h.Path)
	return h.Holder.Close()
}

// Reopen instantiates and opens a new holder.
// Note that the holder must be Closed first.
func (h *Holder) Reopen() error {
	path, logger := h.Path, h.Holder.Logger
	h.Holder = pilosa.NewHolder()
	h.Holder.Path = path
	h.Holder.Logger = logger
	h.Holder.NewAttrStore = boltdb.NewAttrStore
	if err := h.Holder.Open(); err != nil {
		return err
	}

	return nil
}

// MustCreateIndexIfNotExists returns a given index. Panic on error.
func (h *Holder) MustCreateIndexIfNotExists(index string, opt pilosa.IndexOptions) *Index {
	idx, err := h.Holder.CreateIndexIfNotExists(index, opt)
	if err != nil {
		panic(err)
	}
	return &Index{Index: idx}
}

// MustCreateFrameIfNotExists returns a given frame. Panic on error.
func (h *Holder) MustCreateFrameIfNotExists(index, frame string) *Frame {
	f, err := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{}).CreateFrameIfNotExists(frame, pilosa.FrameOptions{})
	if err != nil {
		panic(err)
	}
	return f
}

// MustCreateFragmentIfNotExists returns a given fragment. Panic on error.
func (h *Holder) MustCreateFragmentIfNotExists(index, frame, view string, slice uint64) *Fragment {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFrameIfNotExists(frame, pilosa.FrameOptions{})
	if err != nil {
		panic(err)
	}
	v, err := f.CreateViewIfNotExists(view)
	if err != nil {
		panic(err)
	}
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		panic(err)
	}
	return &Fragment{Fragment: frag}
}

// MustCreateRankedFragmentIfNotExists returns a given fragment with a ranked cache. Panic on error.
func (h *Holder) MustCreateRankedFragmentIfNotExists(index, frame, view string, slice uint64) *Fragment {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFrameIfNotExists(frame, pilosa.FrameOptions{CacheType: pilosa.CacheTypeRanked})
	if err != nil {
		panic(err)
	}
	v, err := f.CreateViewIfNotExists(view)
	if err != nil {
		panic(err)
	}
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		panic(err)
	}
	return &Fragment{Fragment: frag}
}
