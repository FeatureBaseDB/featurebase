package pilosa_test

import (
	"io/ioutil"
	"os"

	"github.com/pilosa/pilosa"
)

// View is a test wrapper for pilosa.View.
type View struct {
	*pilosa.View
	RowAttrStore *AttrStore
}

// NewView returns a new instance of View with a temporary path.
func NewView(db, frame, name string) *View {
	file, err := ioutil.TempFile("", "pilosa-view-")
	if err != nil {
		panic(err)
	}
	file.Close()

	v := &View{
		View:         pilosa.NewView(file.Name(), db, frame, name, pilosa.DefaultCacheSize),
		RowAttrStore: MustOpenAttrStore(),
	}
	v.View.RowAttrStore = v.RowAttrStore.AttrStore
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
	defer v.RowAttrStore.Close()
	return v.View.Close()
}

// Reopen closes the view and reopens it as a new instance.
func (v *View) Reopen() error {
	path := v.Path()
	if err := v.View.Close(); err != nil {
		return err
	}

	v.View = pilosa.NewView(path, v.DB(), v.Frame(), v.Name(), pilosa.DefaultCacheSize)
	v.View.RowAttrStore = v.RowAttrStore.AttrStore
	if err := v.Open(); err != nil {
		return err
	}
	return nil
}

// MustSetBits sets bits on a row. Panic on error.
// This function does not accept a timestamp or quantum.
func (v *View) MustSetBits(rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		if _, err := v.SetBit(rowID, columnID); err != nil {
			panic(err)
		}
	}
}

// MustClearBits clears bits on a row. Panic on error.
func (v *View) MustClearBits(rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		if _, err := v.ClearBit(rowID, columnID); err != nil {
			panic(err)
		}
	}
}
