package test

import (
	"bytes"
	"io/ioutil"
	"os"

	"github.com/pilosa/pilosa"
)

// Holder is a test wrapper for pilosa.Holder.
type Holder struct {
	*pilosa.Holder
	LogOutput bytes.Buffer
}

// NewHolder returns a new instance of Holder with a temporary path.
func NewHolder() *Holder {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	h := &Holder{Holder: pilosa.NewHolder()}
	h.Path = path
	h.Holder.LogOutput = &h.LogOutput

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
	path, logOutput := h.Path, h.Holder.LogOutput
	h.Holder = pilosa.NewHolder()
	h.Holder.Path = path
	h.Holder.LogOutput = logOutput
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
