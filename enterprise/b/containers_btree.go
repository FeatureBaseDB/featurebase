// Copyright (c) 2018 Pilosa Corp. All rights reserved.
//
// This file is part of Pilosa Enterprise Edition.
//
// Pilosa Enterprise Edition is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Pilosa Enterprise Edition is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with Pilosa Enterprise Edition.  If not, see <http://www.gnu.org/licenses/>.

package b

import (
	"io"

	"github.com/pilosa/pilosa/roaring"
)

func cmp(a, b uint64) int {
	return int(a - b)
}

type BTreeContainers struct {
	tree *Tree

	lastKey       uint64
	lastContainer *roaring.Container
}

func NewBTreeContainers() *BTreeContainers {
	return &BTreeContainers{
		tree: TreeNew(cmp),
	}
}

func NewBTreeBitmap(a ...uint64) *roaring.Bitmap {
	b := &roaring.Bitmap{
		Containers: NewBTreeContainers(),
	}
	b.Add(a...)
	return b
}

func (btc *BTreeContainers) Get(key uint64) *roaring.Container {
	// Check the last* cache for same container.
	if key == btc.lastKey && btc.lastContainer != nil {
		return btc.lastContainer
	}

	var c *roaring.Container
	el, ok := btc.tree.Get(key)
	if ok {
		c = el
		btc.lastKey = key
		btc.lastContainer = c
	}
	return c
}

func (btc *BTreeContainers) Put(key uint64, c *roaring.Container) {
	// If a mapped container is added to the tree, reset the
	// lastContainer cache so that the cache is not pointing
	// at a read-only mmap.
	if c.Mapped() {
		btc.lastContainer = nil
	}
	btc.tree.Set(key, c)
}

func (u updater) update(oldV *roaring.Container, exists bool) (*roaring.Container, bool) {
	// update the existing container
	if exists {
		oldV.Update(u.containerType, u.n, u.mapped)
		return oldV, false
	}
	cont := roaring.NewContainer()
	cont.Update(u.containerType, u.n, u.mapped)
	return cont, true
}

// this struct is added to prevent the closure locals from being escaped out to the heap
type updater struct {
	key           uint64
	containerType byte
	n             int
	mapped        bool
}

func (btc *BTreeContainers) PutContainerValues(key uint64, containerType byte, n int, mapped bool) {
	a := updater{key, containerType, n, mapped}
	btc.tree.Put(key, a.update)
}

func (btc *BTreeContainers) Remove(key uint64) {
	btc.tree.Delete(key)
}

func (btc *BTreeContainers) GetOrCreate(key uint64) *roaring.Container {
	// Check the last* cache for same container.
	if key == btc.lastKey && btc.lastContainer != nil {
		return btc.lastContainer
	}

	btc.lastKey = key
	v, ok := btc.tree.Get(key)
	if !ok {
		cont := roaring.NewContainer()
		btc.tree.Set(key, cont)
		btc.lastContainer = cont
		return cont
	}

	btc.lastContainer = v
	return btc.lastContainer
}

func (btc *BTreeContainers) Count() (n uint64) {
	e, _ := btc.tree.Seek(0)
	_, c, err := e.Next()
	for err != io.EOF {
		n += uint64(c.N())
		_, c, err = e.Next()
	}
	return
}

func (btc *BTreeContainers) Clone() roaring.Containers {
	nbtc := NewBTreeContainers()

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

func (btc *BTreeContainers) Last() (key uint64, c *roaring.Container) {
	if btc.tree.Len() == 0 {
		return 0, nil
	}
	k, v := btc.tree.Last()
	return k, v
}

func (btc *BTreeContainers) Size() int {
	return btc.tree.Len()
}

func (btc *BTreeContainers) Iterator(key uint64) (citer roaring.ContainerIterator, found bool) {
	e, ok := btc.tree.Seek(key)
	if ok {
		found = true
	}

	return &BTCIterator{
		e: e,
	}, found
}

type BTCIterator struct {
	e   *Enumerator
	key uint64
	val *roaring.Container
}

func (i *BTCIterator) Next() bool {

	k, v, err := i.e.Next()
	if err == io.EOF {
		return false
	}
	i.key = k
	i.val = v
	return true
}

func (i *BTCIterator) Value() (uint64, *roaring.Container) {
	if i.val == nil {
		return 0, nil
	}
	return i.key, i.val
}
