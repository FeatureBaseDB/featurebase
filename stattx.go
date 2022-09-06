// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/featurebasedb/featurebase/v3/debugstats"
	"github.com/featurebasedb/featurebase/v3/roaring"
	txkey "github.com/featurebasedb/featurebase/v3/short_txkey"
	"github.com/featurebasedb/featurebase/v3/storage"
	"github.com/featurebasedb/featurebase/v3/vprint"
)

// statTx is useful to profile on a
// per method basis, and to play with
// read/write locking.
type statTx struct {
	b     Tx
	stats *callStats
}

// for now, just track call stats globally. But each statTx has
// a pointer to a callStats, so could be made per index or per shard, etc.
var globalCallStats = newCallStats()

type callStats struct {
	// protect elap
	mu sync.Mutex

	// track how much time each call took.
	elap map[kall]*elapsed
}

type elapsed struct {
	dur []float64
}

func newCallStats() *callStats {
	w := &callStats{}
	w.reset()
	return w
}

func (w *callStats) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.elap = make(map[kall]*elapsed)
	for i := kall(0); i < kLast; i++ {
		w.elap[i] = &elapsed{}
	}
}

func (c *callStats) report() (r string) {
	backend := storage.DefaultBackend
	r = fmt.Sprintf("callStats: (%v)\n", backend)
	c.mu.Lock()
	defer c.mu.Unlock()
	var lines []*debugstats.LineSorter
	for i := kall(0); i < kLast; i++ {
		slc := c.elap[i].dur
		n := len(slc)
		if n == 0 {
			continue
		}
		mean, sd, totaltm := computeMeanSd(slc)
		if n == 1 {
			sd = 0
			mean = slc[0]
			totaltm = slc[0]
		}
		line := fmt.Sprintf("  %20v  N=%8v   avg/op: %12v   sd: %12v  total: %12v\n", i.String(), n, time.Duration(mean), time.Duration(sd), time.Duration(totaltm))
		lines = append(lines, &debugstats.LineSorter{Line: line, Tot: totaltm})
	}
	sort.Sort(debugstats.SortByTot(lines))
	for i := range lines {
		r += lines[i].Line
	}

	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	r += fmt.Sprintf("\n m1.TotalAlloc = %v\n", m1.TotalAlloc)

	return
}

var NaN = math.NaN()

func computeMeanSd(slc []float64) (mean, sd, tot float64) {
	if len(slc) < 2 {
		return NaN, NaN, NaN
	}
	for _, v := range slc {
		tot += v
	}
	n := float64(len(slc))
	mean = tot / n

	variance := 0.0
	for _, v := range slc {
		tmp := (v - mean)
		variance += tmp * tmp
	}
	variance = variance / n // biased, but we don't care b/c we can have very small n
	sd = math.Sqrt(variance)
	if sd < 1e-8 {
		// sd is super close to zero, NaN out the z-score rather than +/- Inf
		sd = NaN
	}
	return
}

func (c *callStats) add(k kall, dur time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.elap[k]
	e.dur = append(e.dur, float64(dur))
}

func newStatTx(b Tx) *statTx {
	w := &statTx{
		b: b,

		// For now, just track call stats globally.
		// But this could be made per-Tx by making this be stats: newCallStats(),
		// for example.
		stats: globalCallStats,
	}
	return w
}

type kall int

// constants for kall argument to callStats.add()
const (
	kNewTxIterator kall = iota
	kImportRoaringBits
	kRollback
	kCommit
	kRoaringBitmap
	kContainer
	kPutContainer
	kRemoveContainer
	kAdd
	kRemove
	kContains
	kContainerIterator
	kForEach
	kForEachRange
	kCount
	kMax
	kMin
	kCountRange
	kOffsetRange
	kLast // mark the end, always keep this last. The following aren't tracked atm:
	kType
)

func (k kall) String() string {
	switch k {
	case kNewTxIterator:
		return "kNewTxIterator"
	case kImportRoaringBits:
		return "kImportRoaringBits"
	case kRollback:
		return "kRollback"
	case kCommit:
		return "kCommit"
	case kRoaringBitmap:
		return "kRoaringBitmap"
	case kContainer:
		return "kContainer"
	case kPutContainer:
		return "kPutContainer"
	case kRemoveContainer:
		return "kRemoveContainer"
	case kAdd:
		return "kAdd"
	case kRemove:
		return "kRemove"
	case kContains:
		return "kContains"
	case kContainerIterator:
		return "kContainerIterator"
	case kForEach:
		return "kForEach"
	case kForEachRange:
		return "kForEachRange"
	case kCount:
		return "kCount"
	case kMax:
		return "kMax"
	case kMin:
		return "kMin"
	case kCountRange:
		return "kCountRange"
	case kOffsetRange:
		return "kOffsetRange"
	case kLast:
		return "kLast"
	case kType:
		return "kType"
	}
	vprint.PanicOn(fmt.Sprintf("unknown kall '%v'", int(k)))
	return ""
}

var _ Tx = (*statTx)(nil)

func (c *statTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	me := kNewTxIterator

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()
	return c.b.NewTxIterator(index, field, view, shard)
}

func (c *statTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	me := kImportRoaringBits

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()
	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see ImportRoaringBits() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.ImportRoaringBits(index, field, view, shard, rit, clear, log, rowSize)
}

func (c *statTx) Rollback() {
	me := kRollback

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()
	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Rollback() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	c.b.Rollback()
}

func (c *statTx) Commit() error {
	me := kCommit

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Commit() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Commit()
}

func (c *statTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	me := kRoaringBitmap

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see RoaringBitmap() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.RoaringBitmap(index, field, view, shard)
}

func (c *statTx) Container(index, field, view string, shard uint64, key uint64) (ct *roaring.Container, err error) {
	me := kContainer

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Container() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Container(index, field, view, shard, key)
}

func (c *statTx) PutContainer(index, field, view string, shard uint64, key uint64, rc *roaring.Container) error {
	me := kPutContainer

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see PutContainer() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.PutContainer(index, field, view, shard, key, rc)
}

func (c *statTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {
	me := kRemoveContainer

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see RemoveContainer() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.RemoveContainer(index, field, view, shard, key)
}

func (c *statTx) Add(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	me := kAdd

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Add() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Add(index, field, view, shard, a...)
}

func (c *statTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	me := kRemove

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Remove() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Remove(index, field, view, shard, a...)
}

func (c *statTx) Contains(index, field, view string, shard uint64, key uint64) (exists bool, err error) {
	me := kContains

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Contains() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Contains(index, field, view, shard, key)
}

func (c *statTx) ContainerIterator(index, field, view string, shard uint64, firstRoaringContainerKey uint64) (citer roaring.ContainerIterator, found bool, err error) {
	me := kContainerIterator

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see ContainerIterator() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)
}

func (c *statTx) ApplyFilter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapFilter) (err error) {
	return GenericApplyFilter(c, index, field, view, shard, ckey, filter)
}

func (c *statTx) ApplyRewriter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapRewriter) (err error) {
	return c.b.ApplyRewriter(index, field, view, shard, ckey, filter)
}

func (c *statTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	me := kForEach

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see ForEach() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.ForEach(index, field, view, shard, fn)
}

func (c *statTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {
	me := kForEachRange

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see ForEachRange() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.ForEachRange(index, field, view, shard, start, end, fn)
}

func (c *statTx) Count(index, field, view string, shard uint64) (uint64, error) {
	me := kCount

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Count() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Count(index, field, view, shard)
}

func (c *statTx) Max(index, field, view string, shard uint64) (uint64, error) {
	me := kMax

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Max() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Max(index, field, view, shard)
}

func (c *statTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {
	me := kMin

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Min() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Min(index, field, view, shard)
}

func (c *statTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {
	me := kCountRange

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see CountRange() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.CountRange(index, field, view, shard, start, end)
}

func (c *statTx) OffsetRange(index, field, view string, shard, offset, start, end uint64) (other *roaring.Bitmap, err error) {
	me := kOffsetRange
	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see OffsetRange() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.OffsetRange(index, field, view, shard, offset, start, end)
}

func (c *statTx) Type() string {
	return c.b.Type()
}

func (c *statTx) GetSortedFieldViewList(idx *Index, shard uint64) (fvs []txkey.FieldView, err error) {
	return c.b.GetSortedFieldViewList(idx, shard)
}

func (tx *statTx) GetFieldSizeBytes(index, field string) (uint64, error) {
	return 0, nil
}
