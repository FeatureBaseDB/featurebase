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
	"time"

	"github.com/pilosa/pilosa"
)

// Ensure frame can open and retrieve a view.
func TestFrame_CreateViewIfNotExists(t *testing.T) {
	f := MustOpenFrame()
	defer f.Close()

	// Create view.
	view, err := f.CreateViewIfNotExists("v")
	if err != nil {
		t.Fatal(err)
	} else if view == nil {
		t.Fatal("expected view")
	}

	// Retrieve existing view.
	view2, err := f.CreateViewIfNotExists("v")
	if err != nil {
		t.Fatal(err)
	} else if view != view2 {
		t.Fatal("view mismatch")
	}

	if view != f.View("v") {
		t.Fatal("view mismatch")
	}
}

// Ensure frame can set its time quantum.
func TestFrame_SetTimeQuantum(t *testing.T) {
	f := MustOpenFrame()
	defer f.Close()

	// Set & retrieve time quantum.
	if err := f.SetTimeQuantum(pilosa.TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum: %s", q)
	}

	// Reload frame and verify that it is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum (reopen): %s", q)
	}
}

func TestFrame_NameRestriction(t *testing.T) {
	path, err := ioutil.TempDir("", "pilosa-frame-")
	if err != nil {
		panic(err)
	}
	frame, err := pilosa.NewFrame(path, "i", ".meta")
	if frame != nil {
		t.Fatalf("unexpected frame name %s", err)
	}
}

// Frame represents a test wrapper for pilosa.Frame.
type Frame struct {
	*pilosa.Frame
}

// NewFrame returns a new instance of Frame d/0.
func NewFrame() *Frame {
	path, err := ioutil.TempDir("", "pilosa-frame-")
	if err != nil {
		panic(err)
	}
	frame, err := pilosa.NewFrame(path, "i", "f")
	if err != nil {
		panic(err)
	}
	return &Frame{Frame: frame}
}

// MustOpenFrame returns a new, opened frame at a temporary path. Panic on error.
func MustOpenFrame() *Frame {
	f := NewFrame()
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the frame and removes the underlying data.
func (f *Frame) Close() error {
	defer os.RemoveAll(f.Path())
	return f.Frame.Close()
}

// Reopen closes the index and reopens it.
func (f *Frame) Reopen() error {
	var err error
	if err := f.Frame.Close(); err != nil {
		return err
	}

	path, index, name := f.Path(), f.Index(), f.Name()
	f.Frame, err = pilosa.NewFrame(path, index, name)
	if err != nil {
		return err
	}

	if err := f.Open(); err != nil {
		return err
	}
	return nil
}

// MustSetBit sets a bit on the frame. Panic on error.
func (f *Frame) MustSetBit(view string, rowID, columnID uint64, t *time.Time) (changed bool) {
	changed, err := f.SetBit(view, rowID, columnID, t)
	if err != nil {
		panic(err)
	}
	return changed
}

// Ensure frame can set its cache
func TestFrame_SetCacheSize(t *testing.T) {
	f := MustOpenFrame()
	defer f.Close()
	cacheSize := uint32(100)

	// Set & retrieve frame cache size.
	if err := f.SetCacheSize(cacheSize); err != nil {
		t.Fatal(err)
	} else if q := f.CacheSize(); q != cacheSize {
		t.Fatalf("unexpected frame cache size: %d", q)
	}

	// Reload frame and verify that it is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if q := f.CacheSize(); q != cacheSize {
		t.Fatalf("unexpected frame cache size (reopen): %d", q)
	}
}
