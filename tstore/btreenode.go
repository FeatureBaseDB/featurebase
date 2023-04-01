// Copyright 2023 Molecula Corp. All rights reserved.

package tstore

import (
	"io"

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
	tree   *BTree
	node   *BTreeNode
	schema types.Schema
	from   Sortable
	to     Sortable
	cursor int16
	stack  []stackItem
	item   *BTreeTuple
	//
	atend   bool
	atstart bool
	seeked  bool
}
type stackItem struct {
	n *BTreeNode
	i int
}

func NewBTreeNodeIterator(tree *BTree, initialNode *BTreeNode, from, to Sortable, schema types.Schema, cursor int) *BTreeNodeIterator {
	return &BTreeNodeIterator{
		tree:   tree,
		node:   initialNode,
		schema: schema,
		from:   from,
		to:     to,
		cursor: int16(cursor),
	}
}

func NewBTreeReverseNodeIterator(tree *BTree, initialNode *BTreeNode, from, to Sortable, schema types.Schema, cursor int) *BTreeReverseNodeIterator {
	return &BTreeReverseNodeIterator{
		BTreeNodeIterator: BTreeNodeIterator{
			tree:   tree,
			node:   initialNode,
			schema: schema,
			cursor: int16(cursor),
		},
	}
}

func (i *BTreeNodeIterator) init() {
	i.cursor = 0
}

func (i *BTreeNodeIterator) Next() (Sortable, *BTreeTuple, error) {
	if i.node.slotCount() == 0 {
		return nil, nil, io.EOF
	}
	ci := i.cursor
	i.cursor += 1
	if i.cursor >= int16(i.node.slotCount()) {
		// need to check the next page
		return nil, nil, io.EOF
	}
	key, tuple, err := i.tree.getTuple(i.node, int(ci), i.schema)
	if err != nil {
		return nil, nil, err
	}
	if key.Less(i.to) {
		return key, tuple, nil
	}
	return nil, nil, io.EOF
}

func (i *BTreeNodeIterator) Dispose() {
	i.node.releaseReadLatch()
}

type BTreeReverseNodeIterator struct {
	BTreeNodeIterator
}

func (i *BTreeReverseNodeIterator) init() {
	slotCount := i.node.slotCount()
	if slotCount > 0 {
		i.cursor = int16(slotCount)
	} else {
		i.cursor = 0
	}
}

func (i *BTreeReverseNodeIterator) Next() (Sortable, *BTreeTuple, error) {
	if i.cursor == 0 {
		i.init()
	}
	if i.node.slotCount() == 0 {
		return nil, nil, io.EOF
	}
	if i.cursor == 0 {
		return nil, nil, io.EOF
	}
	ci := i.cursor
	i.cursor -= 1
	key, tuple, err := i.tree.getTuple(i.node, int(ci), i.schema)
	if err != nil {
		return nil, nil, err
	}
	if i.from.Less(key) {
		return key, tuple, nil
	}
	return nil, nil, io.EOF
}

type BIterator interface {
	Next() (Sortable, *BTreeTuple, error)
}

type TstoreIterator interface {
	Next() bool
	Item() *BTreeTuple
}

type BTreeIterator struct {
	tree   *BTree
	node   *BTreeNode
	schema types.Schema
	from   Sortable
	to     Sortable
	cursor int16
	stack  []stackItem
	item   *BTreeTuple
	//
	atend   bool
	atstart bool
	seeked  bool
}

func NewBTreeIterator(tree *BTree, initialNode *BTreeNode, from, to Sortable, schema types.Schema) *BTreeIterator {
	return &BTreeIterator{
		tree:   tree,
		node:   initialNode,
		schema: schema,
		from:   from,
		to:     to,
		// cursor: int16(cursor),
	}
}

func (iter *BTreeIterator) Seek(key Sortable) bool {
	if iter.tree == nil {
		return false
	}
	iter.seeked = true
	iter.stack = iter.stack[:0]
	/*
		if iter.tree.root == nil {
			return false
		}
	*/
	n, _ := iter.tree.fetchNode(iter.tree.rootNode)

	for {
		i, found := n.findKey(key)
		iter.stack = append(iter.stack, stackItem{n, i})
		if found {
			_, iter.item, _ = iter.tree.getTuple(n, i, iter.schema)
			return true
		}
		if n.isLeaf() {
			iter.stack[len(iter.stack)-1].i--
			return iter.Next()
		}
		n = iter.nextNode()
	}
}

func (iter *BTreeIterator) First() bool {
	if iter.tree == nil {
		return false
	}
	iter.atend = false
	iter.atstart = false
	iter.seeked = true
	iter.stack = iter.stack[:0]
	/*
		if iter.tree.root == nil {
			return false
		}
	*/
	n, _ := iter.tree.fetchNode(iter.tree.rootNode)
	for {
		iter.stack = append(iter.stack, stackItem{n, 0})
		if n.isLeaf() {
			break
		}
		n = iter.nextNode()
	}
	s := iter.stack[len(iter.stack)-1]
	_, iter.item, _ = iter.tree.getTuple(n, int(s.i), iter.schema)
	return true
}

func (iter *BTreeIterator) nextNode() *BTreeNode {
	pn := iter.node.page.ReadNextPointer()
	if pn.Page != bufferpool.INVALID_PAGE {
		n, _ := iter.tree.fetchNode(pn)
		return n
	}
	return nil
}

func (iter *BTreeIterator) Item() *BTreeTuple {
	return iter.item
}

// TODO(twg) 2023/04/01 need to wire up the latches/pinning
func (iter *BTreeIterator) Next() bool {
	if iter.tree == nil {
		return false
	}
	if !iter.seeked {
		return iter.First()
	}
	if len(iter.stack) == 0 {
		if iter.atstart {
			return iter.First() && iter.Next()
		}
		return false
	}
	s := iter.stack[len(iter.stack)-1]
	s.i++
	if s.n.isLeaf() {
		if s.i == s.n.slotCount() {
			for {
				iter.stack = iter.stack[:len(iter.stack)-1]
				if len(iter.stack) == 0 {
					iter.atend = true
					return false
				}
				s = iter.stack[len(iter.stack)-1]
				if s.i < s.n.slotCount() {
					break
				}
			}
		}
	} else {
		n := iter.nextNode()
		for {
			iter.stack = append(iter.stack, stackItem{n, 0})
			if n.isLeaf() {
				break
			}
			n = iter.nextNode()
		}
	}
	s = iter.stack[len(iter.stack)-1]
	// ignoring the error and key for now
	_, iter.item, _ = iter.tree.getTuple(s.n, s.i, iter.schema)
	return true
}
