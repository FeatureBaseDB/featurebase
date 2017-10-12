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

package pilosa

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

// View layout modes.
const (
	ViewStandard = "standard"
	ViewInverse  = "inverse"

	ViewFieldPrefix = "field_"
)

// IsValidView returns true if name is valid.
func IsValidView(name string) bool {
	return name == ViewStandard || name == ViewInverse
}

// View represents a container for frame data.
type View struct {
	mu    sync.RWMutex
	path  string
	index string
	frame string
	name  string

	cacheSize uint32

	// Fragments by slice.
	cacheType string // passed in by frame
	fragments map[uint64]*Fragment

	// maxSlice maintains this view's max slice in order to
	// prevent sending multiple `CreateSliceMessage` messages
	maxSlice uint64

	broadcaster Broadcaster
	stats       StatsClient

	RowAttrStore *AttrStore
	LogOutput    io.Writer
}

// NewView returns a new instance of View.
func NewView(path, index, frame, name string, cacheSize uint32) *View {
	return &View{
		path:      path,
		index:     index,
		frame:     frame,
		name:      name,
		cacheSize: cacheSize,

		cacheType: DefaultCacheType,
		fragments: make(map[uint64]*Fragment),

		broadcaster: NopBroadcaster,
		stats:       NopStatsClient,
		LogOutput:   ioutil.Discard,
	}
}

// Name returns the name the view was initialized with.
func (v *View) Name() string { return v.name }

// Index returns the index name the view was initialized with.
func (v *View) Index() string { return v.index }

// Frame returns the frame name the view was initialized with.
func (v *View) Frame() string { return v.frame }

// Path returns the path the view was initialized with.
func (v *View) Path() string { return v.path }

// Open opens and initializes the view.
func (v *View) Open() error {
	if err := func() error {
		// Ensure the view's path exists.
		if err := os.MkdirAll(v.path, 0777); err != nil {
			return err
		} else if err := os.MkdirAll(filepath.Join(v.path, "fragments"), 0777); err != nil {
			return err
		}

		if err := v.openFragments(); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		v.Close()
		return err
	}

	return nil
}

// openFragments opens and initializes the fragments inside the view.
func (v *View) openFragments() error {
	file, err := os.Open(filepath.Join(v.path, "fragments"))
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer file.Close()

	fis, err := file.Readdir(0)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		// Parse filename into integer.
		slice, err := strconv.ParseUint(filepath.Base(fi.Name()), 10, 64)
		if err != nil {
			continue
		}

		frag := v.newFragment(v.FragmentPath(slice), slice)
		if err := frag.Open(); err != nil {
			return fmt.Errorf("open fragment: slice=%d, err=%s", frag.Slice(), err)
		}
		frag.RowAttrStore = v.RowAttrStore
		v.fragments[frag.Slice()] = frag
	}

	return nil
}

// Close closes the view and its fragments.
func (v *View) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Close all fragments.
	for _, frag := range v.fragments {
		if err := frag.Close(); err != nil {
			return err
		}
	}
	v.fragments = make(map[uint64]*Fragment)

	return nil
}

// MaxSlice returns the max slice in the view.
func (v *View) MaxSlice() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()

	var max uint64
	for slice := range v.fragments {
		if slice > max {
			max = slice
		}
	}

	return max
}

// FragmentPath returns the path to a fragment in the view.
func (v *View) FragmentPath(slice uint64) string {
	return filepath.Join(v.path, "fragments", strconv.FormatUint(slice, 10))
}

// Fragment returns a fragment in the view by slice.
func (v *View) Fragment(slice uint64) *Fragment {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.fragment(slice)
}

func (v *View) fragment(slice uint64) *Fragment { return v.fragments[slice] }

// Fragments returns a list of all fragments in the view.
func (v *View) Fragments() []*Fragment {
	v.mu.Lock()
	defer v.mu.Unlock()

	other := make([]*Fragment, 0, len(v.fragments))
	for _, fragment := range v.fragments {
		other = append(other, fragment)
	}
	return other
}

// RecalculateCaches recalculates the cache on every fragment in the view.
func (v *View) RecalculateCaches() {
	for _, fragment := range v.Fragments() {
		fragment.RecalculateCache()
	}
}

// CreateFragmentIfNotExists returns a fragment in the view by slice.
func (v *View) CreateFragmentIfNotExists(slice uint64) (*Fragment, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.createFragmentIfNotExists(slice)
}

func (v *View) createFragmentIfNotExists(slice uint64) (*Fragment, error) {
	// Find fragment in cache first.
	if frag := v.fragments[slice]; frag != nil {
		return frag, nil
	}

	// Initialize and open fragment.
	frag := v.newFragment(v.FragmentPath(slice), slice)
	if err := frag.Open(); err != nil {
		return nil, err
	}
	frag.RowAttrStore = v.RowAttrStore

	// Broadcast a message that a new max slice was just created.
	if slice > v.maxSlice {
		v.maxSlice = slice

		// Send the create slice message to all nodes.
		err := v.broadcaster.SendAsync(
			&internal.CreateSliceMessage{
				Index:     v.index,
				Slice:     slice,
				IsInverse: IsInverseView(v.name),
			})
		if err != nil {
			return nil, err
		}
	}

	// Save to lookup.
	v.fragments[slice] = frag
	return frag, nil
}

func (v *View) newFragment(path string, slice uint64) *Fragment {
	frag := NewFragment(path, v.index, v.frame, v.name, slice)
	frag.CacheType = v.cacheType
	frag.CacheSize = v.cacheSize
	frag.LogOutput = v.LogOutput
	frag.stats = v.stats.WithTags(fmt.Sprintf("slice:%d", slice))
	return frag
}

// SetBit sets a bit within the view.
func (v *View) SetBit(rowID, columnID uint64) (changed bool, err error) {
	slice := columnID / SliceWidth
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		return changed, err
	}
	return frag.SetBit(rowID, columnID)
}

// ClearBit clears a bit within the view.
func (v *View) ClearBit(rowID, columnID uint64) (changed bool, err error) {
	slice := columnID / SliceWidth
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		return changed, err
	}
	return frag.ClearBit(rowID, columnID)
}

// FieldValue uses a column of bits to read a multi-bit value.
func (v *View) FieldValue(columnID uint64, bitDepth uint) (value uint64, exists bool, err error) {
	slice := columnID / SliceWidth
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		return value, exists, err
	}
	return frag.FieldValue(columnID, bitDepth)
}

// SetFieldValue uses a column of bits to set a multi-bit value.
func (v *View) SetFieldValue(columnID uint64, bitDepth uint, value uint64) (changed bool, err error) {
	slice := columnID / SliceWidth
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		return changed, err
	}
	return frag.SetFieldValue(columnID, bitDepth, value)
}

// FieldSum returns the sum & count of a field.
func (v *View) FieldSum(filter *Bitmap, bitDepth uint) (sum, count uint64, err error) {
	for _, f := range v.Fragments() {
		fsum, fcount, err := f.FieldSum(filter, bitDepth)
		if err != nil {
			return sum, count, err
		}
		sum += fsum
		count += fcount
	}
	return sum, count, nil
}

// FieldRange returns bitmaps with a field value encoding matching the predicate.
func (v *View) FieldRange(op pql.Token, bitDepth uint, predicate uint64) (*Bitmap, error) {
	bm := NewBitmap()
	for _, frag := range v.Fragments() {
		other, err := frag.FieldRange(op, bitDepth, predicate)
		if err != nil {
			return nil, err
		}
		bm = bm.Union(other)
	}
	return bm, nil
}

// FieldRangeBetween returns bitmaps with a field value encoding matching any
// value between predicateMin and predicateMax.
func (v *View) FieldRangeBetween(bitDepth uint, predicateMin, predicateMax uint64) (*Bitmap, error) {
	bm := NewBitmap()
	for _, frag := range v.Fragments() {
		other, err := frag.FieldRangeBetween(bitDepth, predicateMin, predicateMax)
		if err != nil {
			return nil, err
		}
		bm = bm.Union(other)
	}
	return bm, nil
}

// IsInverseView returns true if the view is used for storing an inverted representation.
func IsInverseView(name string) bool {
	return strings.HasPrefix(name, ViewInverse)
}

type viewSlice []*View

func (p viewSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p viewSlice) Len() int           { return len(p) }
func (p viewSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// ViewInfo represents schema information for a view.
type ViewInfo struct {
	Name string `json:"name"`
}

type viewInfoSlice []*ViewInfo

func (p viewInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p viewInfoSlice) Len() int           { return len(p) }
func (p viewInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }
