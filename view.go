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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/stats"
	"github.com/molecula/featurebase/v2/testhook"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// View layout modes.
const (
	viewStandard = "standard"

	viewBSIGroupPrefix = "bsig_"
)

// view represents a container for field data.
type view struct {
	mu            sync.RWMutex
	path          string
	index         string
	field         string
	name          string
	qualifiedName string

	holder *Holder
	idx    *Index

	fieldType string
	cacheType string
	cacheSize uint32

	// Fragments by shard.
	fragments map[uint64]*fragment

	broadcaster broadcaster
	stats       stats.StatsClient

	knownShards       *roaring.Bitmap
	knownShardsCopied uint32
}

// newView returns a new instance of View.
func newView(holder *Holder, path, index, field, name string, fieldOptions FieldOptions) *view {
	PanicOn(ValidateName(name))

	return &view{
		path:          path,
		index:         index,
		field:         field,
		name:          name,
		qualifiedName: FormatQualifiedViewName(index, field, name),

		holder: holder,

		fieldType: fieldOptions.Type,
		cacheType: fieldOptions.CacheType,
		cacheSize: fieldOptions.CacheSize,

		fragments: make(map[uint64]*fragment),

		broadcaster: NopBroadcaster,
		stats:       stats.NopStatsClient,
		knownShards: roaring.NewSliceBitmap(),
	}
}

// addKnownShard adds a known shard to v, which you should only do when
// holding the lock -- but that's probably a given, since you're presumably
// calling it because you were potentially altering the shard list. Since
// you have the write lock, availableShards() can't be happening right now.
// Either it'll get the previous value or the next value of knownShards,
// and either is probably fine.
//
// This means that we only copy the (probably tiny) bitmap if we're
// modifying it after it's been read. If it never gets read, knownShardsCopied
// never changes. If it gets read, then we treat that one as immutable --
// we never modify it again, because the field code might be reading it, so
// we make a fresh copy. Since shards almost never change, the expected
// behavior is that we call addKnownShard a lot during initial startup,
// when knownShardsCopied is 0, and then after that calls to availableShards
// return that bitmap, and set knownShardsCopied to 1, but we rarely modify
// the list.
func (v *view) addKnownShard(shard uint64) {
	v.notifyIfNewShard(shard)
	if atomic.LoadUint32(&v.knownShardsCopied) == 1 {
		v.knownShards = v.knownShards.Clone()
		atomic.StoreUint32(&v.knownShardsCopied, 0)
	}
	_, err := v.knownShards.Add(shard)
	PanicOn(err)
}

// removeKnownShard removes a known shard from v. See the notes on addKnownShard.
func (v *view) removeKnownShard(shard uint64) {
	if atomic.LoadUint32(&v.knownShardsCopied) == 1 {
		v.knownShards = v.knownShards.Clone()
		atomic.StoreUint32(&v.knownShardsCopied, 0)
	}
	_, _ = v.knownShards.Remove(shard)
}

// openWithShardSet opens the view. Importantly, it
// only opens the fragments that have data. This saves
// a ton of time. If you have no data and want a new
// view, call view.openEmpty().
func (v *view) openWithShardSet(ss *shardSet) error {
	if v.knownShards == nil {
		v.knownShards = roaring.NewSliceBitmap()
	}

	// Never keep a cache for field views.
	if strings.HasPrefix(v.name, viewBSIGroupPrefix) {
		v.cacheType = CacheTypeNone
	}

	shards := ss.CloneMaybe()

	var frags []*fragment
	for shard := range shards {
		frag := v.newFragment(shard)
		frags = append(frags, frag)
		v.fragments[frag.shard] = frag
	}

	nGoro := runtime.NumCPU()
	if v.idx.holder.txf.TxType() != "roaring" {
		nGoro = nGoro / 4
	}
	if nGoro < 4 {
		nGoro = 4
	}
	pj := newParallelJobs(nGoro)
	for i := range frags {
		// create a new variable frag on each time through
		// the loop (instead of i, frag := range frags)
		// so that the closure run on the
		// goroutine has its own variable.
		frag := frags[i]
		accepted := pj.run(func(worker int) error {
			if err := frag.Open(); err != nil {
				return fmt.Errorf("open fragment: shard=%d, err=%s", frag.shard, err)
			}
			return nil
		})
		if !accepted {
			// have error/shutting down the pj, so stop
			break
		}
	}

	err := pj.waitForFinish()
	if err != nil {
		return err
	}

	// serial, not parallel, because no locking inside addKnownShard at the moment.
	// TODO(jea): is this slow on a cluster? can we optimize it
	// by running it on a goroutine in the background?
	for shard := range shards {
		v.addKnownShard(shard)
	}

	_ = testhook.Opened(v.holder.Auditor, v, nil)
	v.holder.Logger.Debugf("successfully opened index/field/view: %s/%s/%s", v.index, v.field, v.name)
	return nil
}

// openEmpty opens and initializes a new view that has no
// data. If you have data already, then use view.openWithShardSet()
func (v *view) openEmpty() error {
	if v.knownShards == nil {
		v.knownShards = roaring.NewSliceBitmap()
	}

	// Never keep a cache for field views.
	if strings.HasPrefix(v.name, viewBSIGroupPrefix) {
		v.cacheType = CacheTypeNone
	}

	if err := func() error {
		// Ensure the view's path exists.
		v.holder.Logger.Debugf("ensure view path exists: %s", v.path)
		err := os.MkdirAll(v.path, 0777)
		if err != nil {
			return errors.Wrap(err, "creating view directory")
		}
		err = os.MkdirAll(filepath.Join(v.path, "fragments"), 0777)
		if err != nil {
			return errors.Wrap(err, "creating fragments directory")
		}

		v.holder.Logger.Debugf("open fragments for index/field/view: %s/%s/%s", v.index, v.field, v.name)

		return nil
	}(); err != nil {
		v.close()
		return err
	}

	_ = testhook.Opened(v.holder.Auditor, v, nil)
	v.holder.Logger.Debugf("successfully opened index/field/view: %s/%s/%s", v.index, v.field, v.name)
	return nil
}

var workQueue = make(chan struct{}, runtime.NumCPU()*2)

// close closes the view and its fragments.
func (v *view) close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	defer func() {
		_ = testhook.Closed(v.holder.Auditor, v, nil)
	}()

	// Close all fragments.
	eg, ctx := errgroup.WithContext(context.Background())
fragLoop:
	for _, loopFrag := range v.fragments {
		select {
		case <-ctx.Done():
			break fragLoop
		default:
			frag := loopFrag
			workQueue <- struct{}{}
			eg.Go(func() error {
				defer func() {
					<-workQueue
				}()

				if err := frag.Close(); err != nil {
					return errors.Wrap(err, "closing fragment")
				}
				return nil
			})
		}
	}
	err := eg.Wait()
	v.fragments = make(map[uint64]*fragment)
	v.knownShards = nil
	return err
}

// flags returns a set of flags for the underlying fragments.
func (v *view) flags() byte {
	var flag byte
	if v.fieldType == FieldTypeInt || v.fieldType == FieldTypeDecimal || v.fieldType == FieldTypeTimestamp {
		flag |= roaringFlagBSIv2
	}
	return flag
}

// availableShards returns a bitmap of shards which contain data.
func (v *view) availableShards() *roaring.Bitmap {
	// A read lock prevents anything with the write lock from being
	// active, so anything that's calling add/removeKnownShard won't
	// be doing it here. But we do need to indicate that we came
	// through, but we don't want to block on a write lock. So we
	// use an atomic for that.
	v.mu.RLock()
	defer v.mu.RUnlock()
	atomic.StoreUint32(&v.knownShardsCopied, 1)
	return v.knownShards
}

// Fragment returns a fragment in the view by shard.
func (v *view) Fragment(shard uint64) *fragment {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.fragments[shard]
}

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

	// Find fragment in cache first.
	if frag := v.fragments[shard]; frag != nil {
		return frag, nil
	}

	// Initialize and open fragment.
	frag := v.newFragment(shard)
	if err := frag.Open(); err != nil {
		return nil, errors.Wrap(err, "opening fragment")
	}

	v.fragments[shard] = frag
	v.addKnownShard(shard)
	return frag, nil
}

func (v *view) notifyIfNewShard(shard uint64) {
	// if single node, don't bother serializing only to drop it b/c
	// we won't send to ourselves.
	srv, ok := v.broadcaster.(*Server)
	if ok && len(srv.cluster.Nodes()) == 1 {
		return
	}

	if v.knownShards.Contains(shard) { //checks the fields remoteShards bitmap to see if broadcast needed
		return
	}

	broadcastChan := make(chan struct{})

	go func() {
		err := v.holder.sendOrSpool(&CreateShardMessage{
			Index: v.index,
			Field: v.field,
			Shard: shard,
		})
		if err != nil {
			v.holder.Logger.Errorf("broadcasting create shard: %v", err)
		}
		close(broadcastChan)
	}()

	timer := time.NewTimer(50 * time.Millisecond)
	select {
	case <-broadcastChan:
		timer.Stop()
	case <-timer.C:
		v.holder.Logger.Debugf("broadcasting create shard took >50ms")
	}
}

func (v *view) newFragment(shard uint64) *fragment {
	fld := v.idx.Field(v.field)
	spec := fragSpec{
		index: v.idx,
		field: fld,
		view:  v,
	}
	if fld == nil {
		// The backup plan.
		// For tests that do incomplete setup, like making
		// a view without a field.
		spec.fieldstr = v.field
	}
	frag := newFragment(v.holder, spec, shard, v.flags())
	frag.CacheType = v.cacheType
	frag.CacheSize = v.cacheSize
	frag.stats = v.stats
	if v.fieldType == FieldTypeMutex {
		frag.mutexVector = newRowsVector(frag)
	} else if v.fieldType == FieldTypeBool {
		frag.mutexVector = newBoolVector(frag)
	}
	return frag
}

// deleteFragment removes the fragment from the view.
func (v *view) deleteFragment(shard uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	f := v.fragments[shard]
	if f == nil {
		return ErrFragmentNotFound
	}

	v.holder.Logger.Infof("delete fragment: (%s/%s/%s) %d", v.index, v.field, v.name, shard)

	idx := f.holder.Index(v.index)
	f.Close()
	if err := idx.holder.txf.DeleteFragmentFromStore(f.index(), f.field(), f.view(), f.shard, f); err != nil {
		return errors.Wrap(err, "DeleteFragment")
	}
	delete(v.fragments, shard)
	v.removeKnownShard(shard)

	return nil
}

// row returns a row for a shard of the view.
func (v *view) row(txOrig Tx, rowID uint64) (*Row, error) {
	row := NewRow()
	for _, frag := range v.allFragments() {

		tx := txOrig
		if NilInside(tx) {
			tx = v.idx.holder.txf.NewTx(Txo{Write: !writable, Index: v.idx, Fragment: frag, Shard: frag.shard})
			defer tx.Rollback()
		}

		fr, err := frag.row(tx, rowID)
		if err != nil {
			return nil, err
		} else if fr == nil {
			continue
		}
		row.Merge(fr)
	}
	return row, nil

}

// mutexCheck checks all available fragments for duplicate values. The return
// is map[column]map[shard][]values for collisions only.
func (v *view) mutexCheck(ctx context.Context, qcx *Qcx) (map[uint64]map[uint64][]uint64, error) {
	// We don't need the context, we just want the context-awareness on the error groups.
	// It would be nice if the inner functions could use this too...
	eg, _ := errgroup.WithContext(ctx)
	throttle := make(chan struct{}, runtime.NumCPU())
	frags := v.allFragments()
	results := make([]map[uint64][]uint64, len(frags))
	for i, frag := range frags {
		// local copies for the goroutine to use
		i, frag := i, frag
		eg.Go(func() error {
			// limit simultaneous parallel goroutines associated with this
			throttle <- struct{}{}
			defer func() {
				<-throttle
			}()
			tx, finisher, err := qcx.GetTx(Txo{Index: v.idx, Shard: frag.shard})
			if err != nil {
				return err
			}
			defer finisher(&err)
			results[i], err = frag.mutexCheck(tx)
			if err != nil {
				return err
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return nil, err
	}
	out := map[uint64]map[uint64][]uint64{}
	for i, result := range results {
		if len(result) == 0 {
			continue
		}
		out[frags[i].shard] = result
	}
	return out, nil
}

// setBit sets a bit within the view.
func (v *view) setBit(txOrig Tx, rowID, columnID uint64) (changed bool, err error) {
	shard := columnID / ShardWidth
	var frag *fragment
	frag, err = v.CreateFragmentIfNotExists(shard)
	if err != nil {
		return changed, err
	}

	tx := txOrig
	if NilInside(tx) {
		tx = v.idx.holder.txf.NewTx(Txo{Write: writable, Index: v.idx, Fragment: frag, Shard: shard})
		defer func() {
			if err == nil {
				PanicOn(tx.Commit())
			} else {
				tx.Rollback()
			}
		}()
	}
	return frag.setBit(tx, rowID, columnID)
}

// clearBit clears a bit within the view.
func (v *view) clearBit(txOrig Tx, rowID, columnID uint64) (changed bool, err error) {
	shard := columnID / ShardWidth
	frag := v.Fragment(shard)
	if frag == nil {
		return false, nil
	}

	tx := txOrig
	if NilInside(tx) {
		tx = v.idx.holder.txf.NewTx(Txo{Write: writable, Index: v.idx, Fragment: frag, Shard: shard})
		defer func() {
			if err == nil {
				PanicOn(tx.Commit())
			} else {
				tx.Rollback()
			}
		}()
	}

	return frag.clearBit(tx, rowID, columnID)
}

// value uses a column of bits to read a multi-bit value.
func (v *view) value(txOrig Tx, columnID uint64, bitDepth uint64) (value int64, exists bool, err error) {
	shard := columnID / ShardWidth
	frag, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		return value, exists, err
	}

	tx := txOrig
	if NilInside(tx) {
		tx = frag.idx.holder.txf.NewTx(Txo{Write: !writable, Index: frag.idx, Fragment: frag, Shard: frag.shard})
		defer tx.Rollback()
	}

	return frag.value(tx, columnID, bitDepth)
}

// setValue uses a column of bits to set a multi-bit value.
func (v *view) setValue(txOrig Tx, columnID uint64, bitDepth uint64, value int64) (changed bool, err error) {
	shard := columnID / ShardWidth
	frag, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		return changed, err
	}

	tx := txOrig
	if NilInside(tx) {
		tx = v.idx.holder.txf.NewTx(Txo{Write: writable, Index: v.idx, Fragment: frag, Shard: shard})
		defer func() {
			if err == nil {
				PanicOn(tx.Commit())
			} else {
				tx.Rollback()
			}
		}()
	}

	return frag.setValue(tx, columnID, bitDepth, value)
}

// clearValue removes a specific value assigned to columnID
func (v *view) clearValue(txOrig Tx, columnID uint64, bitDepth uint64, value int64) (changed bool, err error) {
	shard := columnID / ShardWidth
	frag := v.Fragment(shard)
	if frag == nil {
		return false, nil
	}

	tx := txOrig
	if NilInside(tx) {
		tx = v.idx.holder.txf.NewTx(Txo{Write: writable, Index: v.idx, Fragment: frag, Shard: shard})
		defer func() {
			if err == nil {
				PanicOn(tx.Commit())
			} else {
				tx.Rollback()
			}
		}()
	}
	return frag.clearValue(tx, columnID, bitDepth, value)
}

// rangeOp returns rows with a field value encoding matching the predicate.
func (v *view) rangeOp(qcx *Qcx, op pql.Token, bitDepth uint64, predicate int64) (_ *Row, err0 error) {
	r := NewRow()
	for _, frag := range v.allFragments() {

		tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: v.idx, Shard: frag.shard})
		if err != nil {
			return nil, err
		}
		defer finisher(&err0)

		other, err := frag.rangeOp(tx, op, bitDepth, predicate)
		if err != nil {
			return nil, err
		}
		r = r.Union(other)
	}
	return r, nil
}

func (v *view) bitDepth(shards []uint64) (uint64, error) {
	var maxBitDepth uint64

	for _, shard := range shards {
		frag, ok := v.fragments[shard]
		if !ok || frag == nil {
			continue
		}

		bd, err := frag.bitDepth()
		if err != nil {
			return 0, errors.Wrapf(err, "getting fragment(%d) bit depth", shard)
		}

		if bd > maxBitDepth {
			maxBitDepth = bd
		}
	}

	return maxBitDepth, nil
}

// ViewInfo represents schema information for a view.
type ViewInfo struct {
	Name string `json:"name"`
}

type viewInfoSlice []*ViewInfo

func (p viewInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p viewInfoSlice) Len() int           { return len(p) }
func (p viewInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// FormatQualifiedViewName generates a qualified name for the view to be used with Tx operations.
func FormatQualifiedViewName(index, field, view string) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00", index, field, view)
}
