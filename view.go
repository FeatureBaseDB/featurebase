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

	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
)

// View layout modes.
const (
	viewStandard = "standard"

	viewBSIGroupPrefix = "bsig_"
)

// view represents a container for field data.
type view struct {
	mu    sync.RWMutex
	path  string
	index string
	field string
	name  string

	fieldType string
	cacheType string
	cacheSize uint32

	// Fragments by shard.
	fragments map[uint64]*fragment

	broadcaster  broadcaster
	stats        StatsClient
	rowAttrStore AttrStore
	logger       Logger
}

// newView returns a new instance of View.
func newView(path, index, field, name string, fieldOptions FieldOptions) *view {
	return &view{
		path:  path,
		index: index,
		field: field,
		name:  name,

		fieldType: fieldOptions.Type,
		cacheType: fieldOptions.CacheType,
		cacheSize: fieldOptions.CacheSize,

		fragments: make(map[uint64]*fragment),

		broadcaster: NopBroadcaster,
		stats:       NopStatsClient,
		logger:      NopLogger,
	}
}

// open opens and initializes the view.
func (v *view) open() error {

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
func (v *view) openFragments() error {
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
		shard, err := strconv.ParseUint(filepath.Base(fi.Name()), 10, 64)
		if err != nil {
			continue
		}

		frag := v.newFragment(v.fragmentPath(shard), shard)
		if err := frag.Open(); err != nil {
			return fmt.Errorf("open fragment: shard=%d, err=%s", frag.shard, err)
		}
		frag.RowAttrStore = v.rowAttrStore
		v.fragments[frag.shard] = frag
	}

	return nil
}

// close closes the view and its fragments.
func (v *view) close() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Close all fragments.
	for _, frag := range v.fragments {
		if err := frag.Close(); err != nil {
			return errors.Wrap(err, "closing fragment")
		}
	}
	v.fragments = make(map[uint64]*fragment)

	return nil
}

// availableShards returns a bitmap of shards which contain data.
func (v *view) availableShards() *roaring.Bitmap {
	v.mu.RLock()
	defer v.mu.RUnlock()

	b := roaring.NewBitmap()
	for shard := range v.fragments {
		b.Add(shard) // ignore error, no writer attached
	}
	return b
}

// fragmentPath returns the path to a fragment in the view.
func (v *view) fragmentPath(shard uint64) string {
	return filepath.Join(v.path, "fragments", strconv.FormatUint(shard, 10))
}

// Fragment returns a fragment in the view by shard.
func (v *view) Fragment(shard uint64) *fragment {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.fragment(shard)
}

func (v *view) fragment(shard uint64) *fragment { return v.fragments[shard] }

// allFragments returns a list of all fragments in the view.
func (v *view) allFragments() []*fragment {
	v.mu.Lock()
	defer v.mu.Unlock()

	other := make([]*fragment, 0, len(v.fragments))
	for _, fragment := range v.fragments {
		other = append(other, fragment)
	}
	return other
}

// recalculateCaches recalculates the cache on every fragment in the view.
func (v *view) recalculateCaches() {
	for _, fragment := range v.allFragments() {
		fragment.RecalculateCache()
	}
}

// CreateFragmentIfNotExists returns a fragment in the view by shard.
func (v *view) CreateFragmentIfNotExists(shard uint64) (*fragment, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.createFragmentIfNotExists(shard)
}

func (v *view) createFragmentIfNotExists(shard uint64) (*fragment, error) {
	// Find fragment in cache first.
	if frag := v.fragments[shard]; frag != nil {
		return frag, nil
	}

	// Initialize and open fragment.
	frag := v.newFragment(v.fragmentPath(shard), shard)
	if err := frag.Open(); err != nil {
		return nil, errors.Wrap(err, "opening fragment")
	}
	frag.RowAttrStore = v.rowAttrStore

	// Broadcast a message that a new max shard was just created.
	if err := v.broadcaster.SendSync(&CreateShardMessage{
		Index: v.index,
		Field: v.field,
		Shard: shard,
	}); err != nil {
		frag.close()
		return nil, errors.Wrap(err, "sending createshard message")
	}

	// Save to lookup.
	v.fragments[shard] = frag
	return frag, nil
}

func (v *view) newFragment(path string, shard uint64) *fragment {
	frag := newFragment(path, v.index, v.field, v.name, shard)
	frag.CacheType = v.cacheType
	frag.CacheSize = v.cacheSize
	frag.Logger = v.logger
	frag.stats = v.stats.WithTags(fmt.Sprintf("shard:%d", shard))
	if v.fieldType == FieldTypeMutex {
		frag.mutexVector = newRowsVector(frag)
	} else if v.fieldType == FieldTypeBool {
		frag.mutexVector = newBoolVector(frag)
	}
	return frag
}

// deleteFragment removes the fragment from the view.
func (v *view) deleteFragment(shard uint64) error {

	fragment := v.fragments[shard]
	if fragment == nil {
		return ErrFragmentNotFound
	}

	v.logger.Printf("delete fragment: (%s/%s/%s) %d", v.index, v.field, v.name, shard)

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
		v.logger.Printf("no cache file to delete for shard %d", shard)
	}

	delete(v.fragments, shard)

	return nil
}

// row returns a row for a shard of the view.
func (v *view) row(rowID uint64) *Row {
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
func (v *view) setBit(rowID, columnID uint64) (changed bool, err error) {
	shard := columnID / ShardWidth
	frag, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		return changed, err
	}
	return frag.setBit(rowID, columnID)
}

// clearBit clears a bit within the view.
func (v *view) clearBit(rowID, columnID uint64) (changed bool, err error) {
	shard := columnID / ShardWidth
	frag, found := v.fragments[shard]
	if !found {
		return false, nil
	}
	return frag.clearBit(rowID, columnID)
}

// value uses a column of bits to read a multi-bit value.
func (v *view) value(columnID uint64, bitDepth uint) (value uint64, exists bool, err error) {
	shard := columnID / ShardWidth
	frag, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		return value, exists, err
	}
	return frag.value(columnID, bitDepth)
}

// setValue uses a column of bits to set a multi-bit value.
func (v *view) setValue(columnID uint64, bitDepth uint, value uint64) (changed bool, err error) {
	shard := columnID / ShardWidth
	frag, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		return changed, err
	}
	return frag.setValue(columnID, bitDepth, value)
}

// sum returns the sum & count of a field.
func (v *view) sum(filter *Row, bitDepth uint) (sum, count uint64, err error) {
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
func (v *view) min(filter *Row, bitDepth uint) (min, count uint64, err error) {
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
func (v *view) max(filter *Row, bitDepth uint) (max, count uint64, err error) {
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
func (v *view) rangeOp(op pql.Token, bitDepth uint, predicate uint64) (*Row, error) {
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

// ViewInfo represents schema information for a view.
type ViewInfo struct {
	Name string `json:"name"`
}

type viewInfoSlice []*ViewInfo

func (p viewInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p viewInfoSlice) Len() int           { return len(p) }
func (p viewInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }
