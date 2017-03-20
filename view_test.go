package pilosa_test

import (
	"io/ioutil"
	"os"

	"github.com/pilosa/pilosa"
)

// View is a test wrapper for pilosa.View.
type View struct {
	*pilosa.View
	BitmapAttrStore *AttrStore
}

// NewView returns a new instance of View with a temporary path.
func NewView(db, frame, name string) *View {
	file, err := ioutil.TempFile("", "pilosa-view-")
	if err != nil {
		panic(err)
	}
	file.Close()

	v := &View{
		View:            pilosa.NewView(file.Name(), db, frame, name),
		BitmapAttrStore: MustOpenAttrStore(),
	}
	v.View.BitmapAttrStore = v.BitmapAttrStore.AttrStore
	return v
}

// MustOpenView creates and opens an view at a temporary path. Panic on error.
func MustOpenView(db, frame, name string) *View {
	v := NewView(db, frame, name)
	if err := v.Open(); err != nil {
		panic(err)
	}
	return v
}

// Close closes the view and removes all underlying data.
func (v *View) Close() error {
	defer os.Remove(v.Path())
	defer v.BitmapAttrStore.Close()
	return v.View.Close()
}

// Reopen closes the view and reopens it as a new instance.
func (v *View) Reopen() error {
	path := v.Path()
	if err := v.View.Close(); err != nil {
		return err
	}

	v.View = pilosa.NewView(path, v.DB(), v.Frame(), v.Name())
	v.View.BitmapAttrStore = v.BitmapAttrStore.AttrStore
	if err := v.Open(); err != nil {
		return err
	}
	return nil
}

// MustSetBits sets bits on a bitmap. Panic on error.
// This function does not accept a timestamp or quantum.
func (v *View) MustSetBits(bitmapID uint64, profileIDs ...uint64) {
	for _, profileID := range profileIDs {
		if _, err := v.SetBit(bitmapID, profileID); err != nil {
			panic(err)
		}
	}
}

// MustClearBits clears bits on a bitmap. Panic on error.
func (v *View) MustClearBits(bitmapID uint64, profileIDs ...uint64) {
	for _, profileID := range profileIDs {
		if _, err := v.ClearBit(bitmapID, profileID); err != nil {
			panic(err)
		}
	}
}
