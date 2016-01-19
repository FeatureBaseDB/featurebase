package pilosa_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/umbel/pilosa"
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

// Ensure frame can set and retrieve bitmap attributes.
func TestFrame_BitmapAttrs(t *testing.T) {
	f := MustOpenFrame()
	defer f.Close()

	// Set attributes.
	if err := f.SetBitmapAttrs(1, map[string]interface{}{"A": float64(100)}); err != nil {
		t.Fatal(err)
	} else if err := f.SetBitmapAttrs(2, map[string]interface{}{"A": float64(200)}); err != nil {
		t.Fatal(err)
	} else if err := f.SetBitmapAttrs(1, map[string]interface{}{"B": "VALUE"}); err != nil {
		t.Fatal(err)
	}

	// Retrieve attributes for bitmap #1.
	if m, err := f.BitmapAttrs(1); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": float64(100), "B": "VALUE"}) {
		t.Fatalf("unexpected attrs(1): %#v", m)
	}

	// Retrieve attributes for bitmap #2.
	if m, err := f.BitmapAttrs(2); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": float64(200)}) {
		t.Fatalf("unexpected attrs(2): %#v", m)
	}
}

// Ensure frame returns a non-nil empty map if unset.
func TestFrame_BitmapAttrs_Empty(t *testing.T) {
	f := MustOpenFrame()
	defer f.Close()

	if m, err := f.BitmapAttrs(100); err != nil {
		t.Fatal(err)
	} else if m == nil || len(m) > 0 {
		t.Fatalf("unexpected attrs: %#v", m)
	}
}

// Ensure frame can unset attributes if explicitly set to nil.
func TestFrame_BitmapAttrs_Unset(t *testing.T) {
	f := MustOpenFrame()
	defer f.Close()

	// Set attributes.
	if err := f.SetBitmapAttrs(1, map[string]interface{}{"A": "X", "B": "Y"}); err != nil {
		t.Fatal(err)
	} else if err := f.SetBitmapAttrs(1, map[string]interface{}{"B": nil}); err != nil {
		t.Fatal(err)
	}

	// Verify attributes.
	if m, err := f.BitmapAttrs(1); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": "X"}) {
		t.Fatalf("unexpected attrs: %#v", m)
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
