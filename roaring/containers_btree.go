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
	"fmt"
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
	if key == btc.lastKey {
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
	// If we don't do this, a Put on a container we just got from
	// Get can result in the tree containing a different container
	// than we'll get on next lookup.
	btc.lastKey, btc.lastContainer = key, c
	// If a mapped container is added to the tree, reset the
	// lastContainer cache so that the cache is not pointing
	// at a read-only mmap.
	if c.Mapped() {
		btc.lastKey = ^uint64(0)
	}
	btc.tree.Set(key, c)
}

func (u updater) update(oldV *Container, exists bool) (*Container, bool) {
	// update the existing container
	if exists {
		oldV = oldV.UpdateOrMake(u.typ, u.n, u.mapped)
		return oldV, true
	}
	cont := NewContainer()
	cont.setTyp(u.typ)
	cont.setN(u.n)
	cont.setMapped(u.mapped)
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
	if key == btc.lastKey {
		btc.lastKey = ^uint64(0)
		btc.lastContainer = nil
	}
}

func (btc *bTreeContainers) GetOrCreate(key uint64) *Container {
	// Check the last* cache for same container.
	if key == btc.lastKey {
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
		n += uint64(c.N())
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

func (btc *bTreeContainers) Freeze() Containers {
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
		nbtc.tree.Set(k, v.Freeze())
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
	// use a definitely-invalid key, so we can distinguish between "you
	// just looked that up, and it was a nil container" and "you have
	// never looked that up before."
	btc.lastKey = ^uint64(0)
	btc.lastContainer = nil
}

func (btc *bTreeContainers) ResetN(n int) {
	// we ignore n because it's impractical to preallocate the tree
	btc.Reset()
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

// Update calls fn (existing-container, existed), and expects
// (new-container, write). If write is true, the container is used to
// replace the given container.
func (btc *bTreeContainers) Update(key uint64, fn func(*Container, bool) (*Container, bool)) {
	btc.tree.Put(key, fn)
}

// UpdateEvery calls fn (existing-container, existed), and expects
// (new-container, write). If write is true, the container is used to
// replace the given container.
func (btc *bTreeContainers) UpdateEvery(fn func(uint64, *Container, bool) (*Container, bool)) {
	e, _ := btc.tree.Seek(0)
	// currently not handling the error from this, but in practice it has
	// to be io.EOF.
	_ = e.Every(fn)
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
	if roaringParanoia {
		if v == nil {
			panic(fmt.Sprintf("got nil container for key %d", k))
		}
	}
	i.key = k
	i.val = v
	return true
}

func (i *btcIterator) Value() (uint64, *Container) {
	return i.key, i.val
}
