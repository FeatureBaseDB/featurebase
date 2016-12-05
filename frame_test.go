package pilosa_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/pilosa/pilosa"
)

// Ensure frame can open and retrieve a fragment.
func TestFrame_CreateFragmentIfNotExists(t *testing.T) {
	f := MustOpenFrame()
	defer f.Close()

	// Create fragment.
	frag, err := f.CreateFragmentIfNotExists(100)
	if err != nil {
		t.Fatal(err)
	} else if frag == nil {
		t.Fatal("expected fragment")
	}

	// Retrieve existing fragment.
	frag2, err := f.CreateFragmentIfNotExists(100)
	if err != nil {
		t.Fatal(err)
	} else if frag != frag2 {
		t.Fatal("fragment mismatch")
	}

	if frag != f.Fragment(100) {
		t.Fatal("fragment mismatch")
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

	return &Frame{Frame: pilosa.NewFrame(path, "d", "f")}
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
