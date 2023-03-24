// Copyright 2023 Molecula Corp. All rights reserved.

package tstore

import (
	"github.com/featurebasedb/featurebase/v3/bufferpool"
)

type BTreeNode struct {
	page *bufferpool.Page
}

func (n *BTreeNode) slotCount() int {
	return int(n.page.ReadSlotCount())
}

func (n *BTreeNode) isLeaf() bool {
	return n.page.ReadPageType() == bufferpool.PAGE_TYPE_BTREE_LEAF
}

func (n *BTreeNode) takeReadLatch() {
	n.page.TakeReadLatch()
}

func (n *BTreeNode) releaseAnyLatch() {
	n.page.ReleaseAnyLatch()
}

func (n *BTreeNode) releaseReadLatch() {
	n.page.ReleaseReadLatch()
}

func (n *BTreeNode) takeWriteLatch() {
	n.page.TakeWriteLatch()
}

func (n *BTreeNode) releaseWriteLatch() {
	n.page.ReleaseWriteLatch()
}

func (n *BTreeNode) latchState() bufferpool.PageLatchState {
	return n.page.LatchState()
}

func (n *BTreeNode) findKey(key Sortable) (int, bool) {
	if n.latchState() == bufferpool.None {
		panic("unexpected latch state")
	}

	minIndex := 0
	onePastMaxIndex := int(n.page.ReadSlotCount())
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		slot := n.page.ReadPageSlot(int16(index))
		pl := slot.KeyPayload(n.page)
		keyAtIndex := Int(pl.KeyAsInt(n.page))
		if key.Equals(keyAtIndex) {
			return index, true
		}
		if key.Less(keyAtIndex) {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	return minIndex, false
}

func (n *BTreeNode) findNextPointer(key Sortable, objectID int32, shard int32) (bufferpool.PageID, error) {
	if n.latchState() == bufferpool.None {
		panic("unexpected latch state")
	}

	// debug
	// if n.page.ID().Page == 24 {
	// 	n.page.Dump("findNextPointer")
	// }
	// ---

	slotCount := int(n.page.ReadSlotCount())

	keyPosition, _ := n.findKey(key)
	if keyPosition == slotCount {
		// return the next pointer...
		return n.page.ReadNextPointer(), nil
	}
	// else return the pointer at the position returned from findKey
	slot := n.page.ReadPageSlot(int16(keyPosition))
	ipl := slot.InternalPayload(n.page)
	return ipl.ValueAsPagePointer(n.page), nil
}
