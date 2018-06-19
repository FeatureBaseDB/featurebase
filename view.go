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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
	"github.com/pkg/errors"
)

// View layout modes.
const (
	ViewStandard = "standard"

	viewBSIGroupPrefix = "bsig_"
)

// isValidView returns true if name is valid.
func isValidView(name string) bool {
	return name == ViewStandard
}

// View represents a container for field data.
type View struct {
	mu    sync.RWMutex
	path  string
	index string
	field string
	name  string

	cacheSize uint32

	// Fragments by slice.
	cacheType string // passed in by field
	fragments map[uint64]*Fragment

	// maxSlice maintains this view's max slice in order to
	// prevent sending multiple `CreateSliceMessage` messages
	maxSlice uint64

	broadcaster Broadcaster
	stats       StatsClient

	RowAttrStore AttrStore
	Logger       Logger
}

// NewView returns a new instance of View.
func NewView(path, index, field, name string, cacheSize uint32) *View {
	return &View{
		path:      path,
		index:     index,
		field:     field,
		name:      name,
		cacheSize: cacheSize,

		cacheType: defaultCacheType,
		fragments: make(map[uint64]*Fragment),

		broadcaster: NopBroadcaster,
		stats:       NopStatsClient,
		Logger:      NopLogger,
	}
}

// open opens and initializes the view.
func (v *View) open() error {

	// Never keep a cache for field views.
	if strings.HasPrefix(v.name, viewBSIGroupPrefix) {
		v.cacheType = CacheTypeNone
	}

	if err := func() error {
		// Ensure the view's path exists.
		if err := os.MkdirAll(v.path, 0777); err != nil {
			return errors.Wrap(err, "creating view directory")
		} else if err := os.MkdirAll(filepath.Join(v.path, "fragments"), 0777); err != nil {
			return errors.Wrap(err, "creating fragments directory")
		}

		if err := v.openFragments(); err != nil {
			return errors.Wrap(err, "opening fragments")
		}

		return nil
	}(); err != nil {
		v.close()
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
		return errors.Wrap(err, "opening fragments directory")
	}
	defer file.Close()

	fis, err := file.Readdir(0)
	if err != nil {
		return errors.Wrap(err, "reading fragments directory")
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

		frag := v.newFragment(v.fragmentPath(slice), slice)
		if err := frag.Open(); err != nil {
			return fmt.Errorf("open fragment: slice=%d, err=%s", frag.slice, err)
		}
		frag.RowAttrStore = v.RowAttrStore
		v.fragments[frag.slice] = frag
	}

	return nil
}

// close closes the view and its fragments.
func (v *View) close() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Close all fragments.
	for _, frag := range v.fragments {
		if err := frag.Close(); err != nil {
			return errors.Wrap(err, "closing fragment")
		}
	}
	v.fragments = make(map[uint64]*Fragment)

	return nil
}

// calculateMaxSlice returns the max slice in the view.
func (v *View) calculateMaxSlice() uint64 {
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

// fragmentPath returns the path to a fragment in the view.
func (v *View) fragmentPath(slice uint64) string {
	return filepath.Join(v.path, "fragments", strconv.FormatUint(slice, 10))
}

// Fragment returns a fragment in the view by slice.
func (v *View) Fragment(slice uint64) *Fragment {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.fragment(slice)
}

func (v *View) fragment(slice uint64) *Fragment { return v.fragments[slice] }

// allFragments returns a list of all fragments in the view.
func (v *View) allFragments() []*Fragment {
	v.mu.Lock()
	defer v.mu.Unlock()

	other := make([]*Fragment, 0, len(v.fragments))
	for _, fragment := range v.fragments {
		other = append(other, fragment)
	}
	return other
}

// recalculateCaches recalculates the cache on every fragment in the view.
func (v *View) recalculateCaches() {
	for _, fragment := range v.allFragments() {
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
	frag := v.newFragment(v.fragmentPath(slice), slice)
	if err := frag.Open(); err != nil {
		return nil, errors.Wrap(err, "opening fragment")
	}
	frag.RowAttrStore = v.RowAttrStore

	// Broadcast a message that a new max slice was just created.
	if slice > v.maxSlice {
		v.maxSlice = slice

		// Send the create slice message to all nodes.
		err := v.broadcaster.SendSync(
			&internal.CreateSliceMessage{
				Index: v.index,
				Slice: slice,
			})
		if err != nil {
			return nil, errors.Wrap(err, "sending createslice message")
		}
	}

	// Save to lookup.
	v.fragments[slice] = frag
	return frag, nil
}

func (v *View) newFragment(path string, slice uint64) *Fragment {
	frag := NewFragment(path, v.index, v.field, v.name, slice)
	frag.CacheType = v.cacheType
	frag.CacheSize = v.cacheSize
	frag.Logger = v.Logger
	frag.stats = v.stats.WithTags(fmt.Sprintf("slice:%d", slice))
	return frag
}

// deleteFragment removes the fragment from the view.
func (v *View) deleteFragment(slice uint64) error {

	fragment := v.fragments[slice]
	if fragment == nil {
		return ErrFragmentNotFound
	}

	v.Logger.Printf("delete fragment: (%s/%s/%s) %d", v.index, v.field, v.name, slice)

	// Close data files before deletion.
	if err := fragment.Close(); err != nil {
		return errors.Wrap(err, "closing fragment")
	}

	// Delete fragment file.
	if err := os.Remove(fragment.path); err != nil {
		return errors.Wrap(err, "deleting fragment file")
	}

	// Delete fragment cache file.
	if err := os.Remove(fragment.cachePath()); err != nil {
		v.Logger.Printf("no cache file to delete for slice %d", slice)
	}

	delete(v.fragments, slice)

	return nil
}

// row returns a row for a slice of the view.
func (v *View) row(rowID uint64) *Row {
	row := NewRow()
	for _, frag := range v.allFragments() {
		fr := frag.row(rowID)
		if fr == nil {
			continue
		}
		row.Merge(fr)
	}
	return row

}

// setBit sets a bit within the view.
func (v *View) setBit(rowID, columnID uint64) (changed bool, err error) {
	slice := columnID / SliceWidth
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		return changed, err
	}
	return frag.setBit(rowID, columnID)
}

// clearBit clears a bit within the view.
func (v *View) clearBit(rowID, columnID uint64) (changed bool, err error) {
	slice := columnID / SliceWidth
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		return changed, err
	}
	return frag.clearBit(rowID, columnID)
}

// value uses a column of bits to read a multi-bit value.
func (v *View) value(columnID uint64, bitDepth uint) (value uint64, exists bool, err error) {
	slice := columnID / SliceWidth
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		return value, exists, err
	}
	return frag.value(columnID, bitDepth)
}

// setValue uses a column of bits to set a multi-bit value.
func (v *View) setValue(columnID uint64, bitDepth uint, value uint64) (changed bool, err error) {
	slice := columnID / SliceWidth
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		return changed, err
	}
	return frag.setValue(columnID, bitDepth, value)
}

// sum returns the sum & count of a field.
func (v *View) sum(filter *Row, bitDepth uint) (sum, count uint64, err error) {
	for _, f := range v.allFragments() {
		fsum, fcount, err := f.sum(filter, bitDepth)
		if err != nil {
			return sum, count, err
		}
		sum += fsum
		count += fcount
	}
	return sum, count, nil
}

// min returns the min and count of a field.
func (v *View) min(filter *Row, bitDepth uint) (min, count uint64, err error) {
	var minHasValue bool
	for _, f := range v.allFragments() {
		fmin, fcount, err := f.min(filter, bitDepth)
		if err != nil {
			return min, count, err
		}
		// Don't consider a min based on zero columns.
		if fcount == 0 {
			continue
		}

		if !minHasValue {
			min = fmin
			minHasValue = true
			count += fcount
			continue
		}

		if fmin < min {
			min = fmin
			count += fcount
		}
	}
	return min, count, nil
}

// max returns the max and count of a field.
func (v *View) max(filter *Row, bitDepth uint) (max, count uint64, err error) {
	for _, f := range v.allFragments() {
		fmax, fcount, err := f.max(filter, bitDepth)
		if err != nil {
			return max, count, err
		}
		if fcount > 0 && fmax > max {
			max = fmax
			count += fcount
		}
	}
	return max, count, nil
}

// rangeOp returns rows with a field value encoding matching the predicate.
func (v *View) rangeOp(op pql.Token, bitDepth uint, predicate uint64) (*Row, error) {
	r := NewRow()
	for _, frag := range v.allFragments() {
		other, err := frag.rangeOp(op, bitDepth, predicate)
		if err != nil {
			return nil, err
		}
		r = r.Union(other)
	}
	return r, nil
}

// rangeBetween returns bitmaps with a field value encoding matching any
// value between predicateMin and predicateMax.
func (v *View) rangeBetween(bitDepth uint, predicateMin, predicateMax uint64) (*Row, error) {
	r := NewRow()
	for _, frag := range v.allFragments() {
		other, err := frag.rangeBetween(bitDepth, predicateMin, predicateMax)
		if err != nil {
			return nil, err
		}
		r = r.Union(other)
	}
	return r, nil
}

// ViewInfo represents schema information for a view.
type ViewInfo struct {
	Name string `json:"name"`
}

type viewInfoSlice []*ViewInfo

func (p viewInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p viewInfoSlice) Len() int           { return len(p) }
func (p viewInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }
