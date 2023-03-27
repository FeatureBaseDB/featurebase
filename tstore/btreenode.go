// Copyright 2023 Molecula Corp. All rights reserved.

package tstore

import (
	"github.com/featurebasedb/featurebase/v3/bufferpool"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
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

type BTreeNodeIterator struct {
	tree    *BTree
	node    *BTreeNode
	schema  types.Schema
	reverse bool
	cursor  int16
}

func NewBTreeNodeIterator(tree *BTree, initialNode *BTreeNode, reverse bool, schema types.Schema) *BTreeNodeIterator {
	return &BTreeNodeIterator{
		tree:    tree,
		node:    initialNode,
		schema:  schema,
		reverse: reverse,
		cursor:  0,
	}
}

func (i *BTreeNodeIterator) init() {
	if i.reverse {
		slotCount := i.node.slotCount()
		if slotCount > 0 {
			i.cursor = int16(slotCount)
		} else {
			i.cursor = 0
		}
	} else {
		panic("implement me")
	}
}

func (i *BTreeNodeIterator) Next() (Sortable, *BTreeTuple, error) {
	if i.cursor == 0 {
		i.init()
	}
	if i.reverse {
		if i.cursor == 0 {
			return nil, nil, nil
		}
		ci := i.cursor
		i.cursor -= 1
		return i.tree.getTuple(i.node, int(ci-1), i.schema)
	} else {
		panic("implement me")
	}
}

func (i *BTreeNodeIterator) Dispose() {
	i.node.releaseReadLatch()
}
