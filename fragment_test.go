package pilosa_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/umbel/pilosa"
)

// SliceWidth is a helper reference to use when testing.
const SliceWidth = pilosa.SliceWidth

// Ensure a fragment can set a bit and retrieve it.
func TestFragment_SetBit(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Set bits on the fragment.
	if err := f.SetBit(120, 1); err != nil {
		t.Fatal(err)
	} else if err := f.SetBit(120, 6); err != nil {
		t.Fatal(err)
	} else if err := f.SetBit(121, 0); err != nil {
		t.Fatal(err)
	}

	// Verify counts on bitmaps.
	if n := f.Bitmap(120).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	} else if n := f.Bitmap(121).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.Bitmap(120).Count(); n != 2 {
		t.Fatalf("unexpected count (reopen): %d", n)
	} else if n := f.Bitmap(121).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can clear a set bit.
func TestFragment_ClearBit(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Set and then clear bits on the fragment.
	if err := f.SetBit(1000, 1); err != nil {
		t.Fatal(err)
	} else if err := f.SetBit(1000, 2); err != nil {
		t.Fatal(err)
	} else if err := f.ClearBit(1000, 1); err != nil {
		t.Fatal(err)
	}

	// Verify count on bitmap.
	if n := f.Bitmap(1000).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.Bitmap(1000).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Fragment is a test wrapper for pilosa.Fragment.
type Fragment struct {
	*pilosa.Fragment
}

// NewFragment returns a new instance of Fragment with a temporary path.
func NewFragment(db, frame string, slice uint64) *Fragment {
	file, err := ioutil.TempFile("", "pilosa-fragment-")
	if err != nil {
		panic(err)
	}
	file.Close()
	return &Fragment{Fragment: pilosa.NewFragment(file.Name(), db, frame, slice)}
}

// MustOpenFragment creates and opens an fragment at a temporary path. Panic on error.
func MustOpenFragment(db, frame string, slice uint64) *Fragment {
	f := NewFragment(db, frame, slice)
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the fragment and removes all underlying data.
func (f *Fragment) Close() error {
	defer os.Remove(f.Path())
	return f.Fragment.Close()
}

// Reopen closes the fragment and reopens it as a new instance.
func (f *Fragment) Reopen() error {
	path := f.Path()
	if err := f.Close(); err != nil {
		return err
	}

	f = &Fragment{Fragment: pilosa.NewFragment(path, f.DB(), f.Frame(), f.Slice())}
	if err := f.Open(); err != nil {
		return err
	}
	return nil
}

// MustSetBit sets a bit on a bitmap. Panic on error.
func (f *Fragment) MustSetBit(bitmapID, profileID uint64) {
	if err := f.SetBit(bitmapID, profileID); err != nil {
		panic(err)
	}
}

// MustClearBit clears a bit on a bitmap. Panic on error.
func (f *Fragment) MustClearBit(bitmapID, profileID uint64) {
	if err := f.ClearBit(bitmapID, profileID); err != nil {
		panic(err)
	}
}
