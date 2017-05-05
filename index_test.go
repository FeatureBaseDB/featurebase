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

package pilosa_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/pilosa/pilosa"
)

// Ensure index can open and retrieve a frame.
func TestIndex_CreateFrameIfNotExists(t *testing.T) {
	index := MustOpenIndex()
	defer index.Close()

	// Create frame.
	f, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{})
	if err != nil {
		t.Fatal(err)
	} else if f == nil {
		t.Fatal("expected frame")
	}

	// Retrieve existing frame.
	other, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{})
	if err != nil {
		t.Fatal(err)
	} else if f.Frame != other.Frame {
		t.Fatal("frame mismatch")
	}

	if f.Frame != index.Frame("f") {
		t.Fatal("frame mismatch")
	}
}

func TestIndex_CreateFrame(t *testing.T) {
	// Ensure time quantum can be set appropriately on a new frame.
	t.Run("TimeQuantum", func(t *testing.T) {
		t.Run("Explicit", func(t *testing.T) {
			index := MustOpenIndex()
			defer index.Close()

			// Set index time quantum.
			if err := index.SetTimeQuantum(pilosa.TimeQuantum("YM")); err != nil {
				t.Fatal(err)
			}

			// Create frame with explicit quantum.
			f, err := index.CreateFrame("f", pilosa.FrameOptions{TimeQuantum: pilosa.TimeQuantum("YMDH")})
			if err != nil {
				t.Fatal(err)
			} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
				t.Fatalf("unexpected frame time quantum: %s", q)
			}
		})

		t.Run("Inherited", func(t *testing.T) {
			index := MustOpenIndex()
			defer index.Close()

			// Set index time quantum.
			if err := index.SetTimeQuantum(pilosa.TimeQuantum("YM")); err != nil {
				t.Fatal(err)
			}

			// Create frame.
			f, err := index.CreateFrame("f", pilosa.FrameOptions{})
			if err != nil {
				t.Fatal(err)
			} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YM") {
				t.Fatalf("unexpected frame time quantum: %s", q)
			}
		})
	})

	// Ensure frame cannot be created with a matching row label.
	t.Run("ErrColumnRowLabelEqual", func(t *testing.T) {
		t.Run("Explicit", func(t *testing.T) {
			index := MustOpenIndex()
			defer index.Close()

			_, err := index.CreateFrame("f", pilosa.FrameOptions{RowLabel: pilosa.DefaultColumnLabel})
			if err != pilosa.ErrColumnRowLabelEqual {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("Default", func(t *testing.T) {
			index := MustOpenIndex()
			defer index.Close()
			if err := index.SetColumnLabel(pilosa.DefaultRowLabel); err != nil {
				t.Fatal(err)
			}

			_, err := index.CreateFrame("f", pilosa.FrameOptions{})
			if err != pilosa.ErrColumnRowLabelEqual {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	})
}

// Ensure index can delete a frame.
func TestIndex_DeleteFrame(t *testing.T) {
	index := MustOpenIndex()
	defer index.Close()

	// Create frame.
	if _, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Delete frame & verify it's gone.
	if err := index.DeleteFrame("f"); err != nil {
		t.Fatal(err)
	} else if index.Frame("f") != nil {
		t.Fatal("expected nil frame")
	}

	// Delete again to make sure it doesn't error.
	if err := index.DeleteFrame("f"); err != nil {
		t.Fatal(err)
	}
}

// Ensure index can set the default time quantum.
func TestIndex_SetTimeQuantum(t *testing.T) {
	index := MustOpenIndex()
	defer index.Close()

	// Set & retrieve time quantum.
	if err := index.SetTimeQuantum(pilosa.TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	} else if q := index.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum: %s", q)
	}

	// Reload index and verify that it is persisted.
	if err := index.Reopen(); err != nil {
		t.Fatal(err)
	} else if q := index.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum (reopen): %s", q)
	}
}

// Index represents a test wrapper for pilosa.Index.
type Index struct {
	*pilosa.Index
}

// NewIndex returns a new instance of Index.
func NewIndex() *Index {
	path, err := ioutil.TempDir("", "pilosa-index-")
	if err != nil {
		panic(err)
	}
	index, err := pilosa.NewIndex(path, "i")
	if err != nil {
		panic(err)
	}
	return &Index{Index: index}
}

// MustOpenIndex returns a new, opened index at a temporary path. Panic on error.
func MustOpenIndex() *Index {
	index := NewIndex()
	if err := index.Open(); err != nil {
		panic(err)
	}
	return index
}

// Close closes the index and removes the underlying data.
func (i *Index) Close() error {
	defer os.RemoveAll(i.Path())
	return i.Index.Close()
}

// Reopen closes the index and reopens it.
func (i *Index) Reopen() error {
	var err error
	if err := i.Index.Close(); err != nil {
		return err
	}

	path, name := i.Path(), i.Name()
	i.Index, err = pilosa.NewIndex(path, name)
	if err != nil {
		return err
	}

	if err := i.Open(); err != nil {
		return err
	}
	return nil
}

// CreateFrame creates a frame with the given options.
func (i *Index) CreateFrame(name string, opt pilosa.FrameOptions) (*Frame, error) {
	f, err := i.Index.CreateFrame(name, opt)
	if err != nil {
		return nil, err
	}
	return &Frame{Frame: f}, nil
}

// CreateFrameIfNotExists creates a frame with the given options if it doesn't exist.
func (i *Index) CreateFrameIfNotExists(name string, opt pilosa.FrameOptions) (*Frame, error) {
	f, err := i.Index.CreateFrameIfNotExists(name, opt)
	if err != nil {
		return nil, err
	}
	return &Frame{Frame: f}, nil
}

// Ensure index can delete a frame.
func TestIndex_InvalidName(t *testing.T) {
	path, err := ioutil.TempDir("", "pilosa-index-")
	if err != nil {
		panic(err)
	}
	index, err := pilosa.NewIndex(path, "ABC")
	if index != nil {
		t.Fatalf("unexpected index name %s", index)
	}
}
