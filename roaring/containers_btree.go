// Copyright 2021 Molecula Corp. All rights reserved.
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
	// We have no way to report this.
	// Because we just created Bitmap, its OpWriter is nil, so there
	// is no code path which would cause Add() to return an error.
	// Therefore, it's safe to swallow this error.
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
	btc.tree.Set(key, c)
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
	_, _ = btc.tree.Put(key, fn)
	btc.lastKey = ^uint64(0)
	btc.lastContainer = nil
}

// UpdateEvery calls fn (existing-container, existed), and expects
// (new-container, write). If write is true, the container is used to
// replace the given container.
func (btc *bTreeContainers) UpdateEvery(fn func(uint64, *Container, bool) (*Container, bool)) {
	e, _ := btc.tree.Seek(0)
	// currently not handling the error from this, but in practice it has
	// to be io.EOF.
	_ = e.Every(fn)
	// invalidate cache.
	btc.lastKey = ^uint64(0)
	btc.lastContainer = nil
}

type btcIterator struct {
	e   *enumerator
	key uint64
	val *Container
}

func (i *btcIterator) Close() {}

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
