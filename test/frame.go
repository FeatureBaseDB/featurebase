package test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
)

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
