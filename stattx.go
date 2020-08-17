// Copyright 2020 Pilosa Corp.
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
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/pilosa/pilosa/v2/roaring"
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

type LineSorter struct {
	Line string
	Tot  float64
}

type SortByTot []*LineSorter

func (p SortByTot) Len() int {
	return len(p)
}
func (p SortByTot) Less(i, j int) bool {
	return p[i].Tot < p[j].Tot
}
func (p SortByTot) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (c *callStats) report() (r string) {
	txsrc := os.Getenv("PILOSA_TXSRC")
	r = fmt.Sprintf("callStats: (%v)\n", txsrc)
	c.mu.Lock()
	defer c.mu.Unlock()
	var lines []*LineSorter
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
		lines = append(lines, &LineSorter{Line: line, Tot: totaltm})
	}
	sort.Sort(SortByTot(lines))
	for i := range lines {
		r += lines[i].Line
	}
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
	kIncrementOpN kall = iota
	kNewTxIterator
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
	kUnionInPlace
	kCountRange
	kOffsetRange
	kRoaringBitmapReader
	kSliceOfShards
	kLast // mark the end, always keep this last. The following aren't tracked atm:
	kType
	kDump
	kReadonly
	kPointer
	kUseRowCache
)

func (k kall) String() string {
	switch k {
	case kIncrementOpN:
		return "kIncrementOpN"
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
	case kUnionInPlace:
		return "kUnionInPlace"
	case kCountRange:
		return "kCountRange"
	case kOffsetRange:
		return "kOffsetRange"
	case kRoaringBitmapReader:
		return "kRoaringBitmapReader"
	case kSliceOfShards:
		return "kSliceOfShards"
	case kLast:
		return "kLast"
	case kType:
		return "kType"
	case kDump:
		return "kDump"
	case kReadonly:
		return "kReadonly"
	case kPointer:
		return "kPointer"
	case kUseRowCache:
		return "kUseRowCache"
	}
	panic(fmt.Sprintf("unknown kall '%v'", int(k)))
}

var _ = newStatTx // happy linter
var _ = kPointer
var _ = kUseRowCache
var _ = kType
var _ = kDump
var _ = kReadonly

var _ Tx = (*statTx)(nil)

//IncrementOpN
func (c *statTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {
	me := kIncrementOpN

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	c.b.IncrementOpN(index, field, view, shard, changedN)
}

func (c *statTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	me := kNewTxIterator

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()
	return c.b.NewTxIterator(index, field, view, shard)
}

func (c *statTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	me := kImportRoaringBits

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ImportRoaringBits() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.ImportRoaringBits(index, field, view, shard, rit, clear, log, rowSize, data)
}

func (c *statTx) Dump() {
	c.b.Dump()
}

func (c *statTx) Readonly() bool {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Readonly() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.Readonly()
}

func (tx *statTx) Pointer() string {
	return fmt.Sprintf("%p", tx)
}

func (c *statTx) Rollback() {
	me := kRollback

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Rollback() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see Commit() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see RoaringBitmap() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see Container() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see PutContainer() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see RemoveContainer() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.RemoveContainer(index, field, view, shard, key)
}

func (c *statTx) UseRowCache() bool {
	return c.b.UseRowCache()
}

func (c *statTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {
	me := kAdd

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Add() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.Add(index, field, view, shard, batched, a...)
}

func (c *statTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	me := kRemove

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Remove() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see Contains() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see ContainerIterator() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)
}

func (c *statTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	me := kForEach

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ForEach() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see ForEachRange() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see Count() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see Max() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see Min() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.Min(index, field, view, shard)
}

func (c *statTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {
	me := kUnionInPlace

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see UnionInPlace() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.UnionInPlace(index, field, view, shard, others...)
}

func (c *statTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {
	me := kCountRange

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see CountRange() panic '%v' at '%v'", r, stack())
			panic(r)
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
			AlwaysPrintf("see OffsetRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.OffsetRange(index, field, view, shard, offset, start, end)
}

func (c *statTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {
	me := kRoaringBitmapReader

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RoaringBitmapReader() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.RoaringBitmapReader(index, field, view, shard, fragmentPathForRoaring)
}

func (c *statTx) Type() string {
	return c.b.Type()
}

func (c *statTx) SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error) {
	me := kSliceOfShards

	t0 := time.Now()
	defer func() {
		c.stats.add(me, time.Since(t0))
	}()
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see SliceOfShards() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.SliceOfShards(index, field, view, optionalViewPath)
}
