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

package roaring

import (
	"io"
)

type bTreeContainers struct {
	tree *tree

	lastKey       uint64
	lastContainer *Container
}

func newBTreeContainers() *bTreeContainers {
	return &bTreeContainers{
		tree: treeNew(),
	}
}

func NewBTreeBitmap(a ...uint64) *Bitmap {
	b := &Bitmap{
		Containers: newBTreeContainers(),
	}
	// TODO: We have no way to report this.
	_, _ = b.Add(a...)
	return b
}

func (btc *bTreeContainers) Get(key uint64) *Container {
	// Check the last* cache for same container.
	if key == btc.lastKey && btc.lastContainer != nil {
		return btc.lastContainer
	}

	var c *Container
	el, ok := btc.tree.Get(key)
	if ok {
		c = el
		btc.lastKey = key
		btc.lastContainer = c
	}
	return c
}

func (btc *bTreeContainers) Put(key uint64, c *Container) {
	// If a mapped container is added to the tree, reset the
	// lastContainer cache so that the cache is not pointing
	// at a read-only mmap.
	if c.Mapped() {
		btc.lastContainer = nil
	}
	btc.tree.Set(key, c)
}

func (u updater) update(oldV *Container, exists bool) (*Container, bool) {
	// update the existing container
	if exists {
		oldV.Update(u.typ, u.n, u.mapped)
		return oldV, false
	}
	cont := NewContainer()
	cont.typ = u.typ
	cont.n = u.n
	cont.mapped = u.mapped
	return cont, true
}

// this struct is added to prevent the closure locals from being escaped out to the heap
type updater struct {
	key    uint64
	n      int32
	typ    byte
	mapped bool
}

func (btc *bTreeContainers) PutContainerValues(key uint64, typ byte, n int, mapped bool) {
	a := updater{key, int32(n), typ, mapped}
	btc.tree.Put(key, a.update)
}

func (btc *bTreeContainers) Remove(key uint64) {
	btc.tree.Delete(key)
}

func (btc *bTreeContainers) GetOrCreate(key uint64) *Container {
	// Check the last* cache for same container.
	if key == btc.lastKey && btc.lastContainer != nil {
		return btc.lastContainer
	}

	btc.lastKey = key
	v, ok := btc.tree.Get(key)
	if !ok {
		cont := NewContainer()
		btc.tree.Set(key, cont)
		btc.lastContainer = cont
		return cont
	}

	btc.lastContainer = v
	return btc.lastContainer
}

func (btc *bTreeContainers) Count() (n uint64) {
	e, _ := btc.tree.Seek(0)
	_, c, err := e.Next()
	for err != io.EOF {
		n += uint64(c.n)
		_, c, err = e.Next()
	}
	return n
}

func (btc *bTreeContainers) Clone() Containers {
	nbtc := newBTreeContainers()

	itr, err := btc.tree.SeekFirst()
	if err == io.EOF {
		return nbtc
	}
	for {
		k, v, err := itr.Next()
		if err == io.EOF {
			break
		}
		nbtc.tree.Set(k, v.Clone())
	}
	return nbtc
}

func (btc *bTreeContainers) Last() (key uint64, c *Container) {
	if btc.tree.Len() == 0 {
		return 0, nil
	}
	k, v := btc.tree.Last()
	return k, v
}

func (btc *bTreeContainers) Size() int {
	return btc.tree.Len()
}

func (btc *bTreeContainers) Reset() {
	btc.tree = treeNew()
	btc.lastKey = 0
	btc.lastContainer = nil
}

func (btc *bTreeContainers) Iterator(key uint64) (citer ContainerIterator, found bool) {
	e, ok := btc.tree.Seek(key)
	if ok {
		found = true
	}

	return &btcIterator{
		e: e,
	}, found
}

func (btc *bTreeContainers) Repair() {
	e, _ := btc.tree.Seek(0)
	_, c, err := e.Next()
	for err != io.EOF {
		c.Repair()
		_, c, err = e.Next()
	}
}

type btcIterator struct {
	e   *enumerator
	key uint64
	val *Container
}

func (i *btcIterator) Next() bool {

	k, v, err := i.e.Next()
	if err == io.EOF {
		return false
	}
	i.key = k
	i.val = v
	return true
}

func (i *btcIterator) Value() (uint64, *Container) {
	if i.val == nil {
		return 0, nil
	}
	return i.key, i.val
}
