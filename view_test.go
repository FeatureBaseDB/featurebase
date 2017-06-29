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

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

// View is a test wrapper for pilosa.View.
type View struct {
	*pilosa.View
	RowAttrStore *test.AttrStore
}

// NewView returns a new instance of View with a temporary path.
func NewView(index, frame, name string) *View {
	file, err := ioutil.TempFile("", "pilosa-view-")
	if err != nil {
		panic(err)
	}
	file.Close()

	v := &View{
		View:         pilosa.NewView(file.Name(), index, frame, name, pilosa.DefaultCacheSize),
		RowAttrStore: test.MustOpenAttrStore(),
	}
	v.View.RowAttrStore = v.RowAttrStore.AttrStore
	return v
}

// MustOpenView creates and opens an view at a temporary path. Panic on error.
func MustOpenView(index, frame, name string) *View {
	v := NewView(index, frame, name)
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

	v.View = pilosa.NewView(path, v.Index(), v.Frame(), v.Name(), pilosa.DefaultCacheSize)
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
