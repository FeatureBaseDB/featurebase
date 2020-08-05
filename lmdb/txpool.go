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

// +build skip_building_lmdb_for_now

package pilosa

import (
	"fmt"
	"io"

	"github.com/pilosa/pilosa/v2/roaring"
)

// poolTx directs all Tx calls to a pre-made
// goroutine pool that are setup to do
// LMDB operations safely. Each of these
// goroutines has had runtime.LockOSThread()
// called, and we serialize write Tx onto
// a single writer goroutine.
type poolTx struct {
	w *LMDBWrapper
	b *LMDBTx
}

var _ = (*LMDBWrapper).newPoolTx // happy linter

func (w *LMDBWrapper) newPoolTx(write bool, initialIndexName string) (ptx *poolTx) {

	var tx *LMDBTx

	job := newLMDBJob(write, func(j *lmdbJob) {
		tx = w.NewLMDBTx(write, initialIndexName)
	})
	if suberr := w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}
	<-job.done

	return &poolTx{
		w: w,
		b: tx,
	}
}

var _ Tx = (*poolTx)(nil)

func (c *poolTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		c.b.IncrementOpN(index, field, view, shard, changedN)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
}

func (c *poolTx) NewTxIterator(index, field, view string, shard uint64) (rit *roaring.Iterator) {
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		rit = c.b.NewTxIterator(index, field, view, shard)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ImportRoaringBits() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()

	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		changed, rowSet, err = c.b.ImportRoaringBits(index, field, view, shard, rit, clear, log, rowSize, data)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) Dump() {
	c.b.Dump()
}

func (c *poolTx) Readonly() bool {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Readonly() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.Readonly()
}

func (tx *poolTx) Pointer() string {
	return fmt.Sprintf("%p", tx)
}

func (c *poolTx) Rollback() {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Rollback() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()

	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		c.b.Rollback()
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
}

func (c *poolTx) Commit() (err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Commit() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		err = c.b.Commit()
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) RoaringBitmap(index, field, view string, shard uint64) (rbm *roaring.Bitmap, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RoaringBitmap() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		rbm, err = c.b.RoaringBitmap(index, field, view, shard)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) Container(index, field, view string, shard uint64, key uint64) (ct *roaring.Container, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Container() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		ct, err = c.b.Container(index, field, view, shard, key)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) PutContainer(index, field, view string, shard uint64, key uint64, rc *roaring.Container) (err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see PutContainer() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		err = c.b.PutContainer(index, field, view, shard, key, rc)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) RemoveContainer(index, field, view string, shard uint64, key uint64) (err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RemoveContainer() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		err = c.b.RemoveContainer(index, field, view, shard, key)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) UseRowCache() bool {
	return c.b.UseRowCache()
}

func (c *poolTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Add() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		changeCount, err = c.b.Add(index, field, view, shard, batched, a...)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Remove() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		changeCount, err = c.b.Remove(index, field, view, shard, a...)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) Contains(index, field, view string, shard uint64, key uint64) (exists bool, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Contains() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		exists, err = c.b.Contains(index, field, view, shard, key)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) ContainerIterator(index, field, view string, shard uint64, firstRoaringContainerKey uint64) (citer roaring.ContainerIterator, found bool, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ContainerIterator() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		citer, found, err = c.b.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) (err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ForEach() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		err = c.b.ForEach(index, field, view, shard, fn)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) (err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ForEachRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		err = c.b.ForEachRange(index, field, view, shard, start, end, fn)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) Count(index, field, view string, shard uint64) (n uint64, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Count() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		n, err = c.b.Count(index, field, view, shard)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) Max(index, field, view string, shard uint64) (n uint64, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Max() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		n, err = c.b.Max(index, field, view, shard)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) Min(index, field, view string, shard uint64) (m uint64, found bool, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Min() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		m, found, err = c.b.Min(index, field, view, shard)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) (err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see UnionInPlace() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		err = c.b.UnionInPlace(index, field, view, shard, others...)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see CountRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		n, err = c.b.CountRange(index, field, view, shard, start, end)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) OffsetRange(index, field, view string, shard, offset, start, end uint64) (other *roaring.Bitmap, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see OffsetRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		other, err = c.b.OffsetRange(index, field, view, shard, offset, start, end)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RoaringBitmapReader() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		r, sz, err = c.b.RoaringBitmapReader(index, field, view, shard, fragmentPathForRoaring)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}

func (c *poolTx) Type() string {
	return c.b.Type()
}

func (c *poolTx) SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see SliceOfShards() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	job := newLMDBJob(c.b.write, func(j *lmdbJob) {
		sliceOfShards, err = c.b.SliceOfShards(index, field, view, optionalViewPath)
	})
	if suberr := c.w.submit(job); suberr != nil {
		AlwaysPrintf("submit job saw err '%v'", suberr)
		return
	}

	<-job.done
	return
}
