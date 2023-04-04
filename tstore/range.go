package tstore

import (
	"github.com/featurebasedb/featurebase/v3/bufferpool"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/vprint"
)

type stackItem struct {
	n *BTreeNode
	i int
}

type TstoreIterator interface {
	Next() bool
	Item() (*BTreeTuple, Sortable)
	Dispose()
}

type RangeIterator struct {
	tree   *BTree
	node   *BTreeNode
	schema types.Schema
	from   Sortable
	to     Sortable
	stack  []*stackItem
	item   *BTreeTuple
	key    Sortable
	//
	atend   bool
	atstart bool
	seeked  bool
	err     error
}

func (b *BTree) NewRangeIterator(first, last Sortable) (*RangeIterator, error) {
	root, err := b.fetchNode(b.rootNode)
	if err != nil {
		return nil, err
	}
	return NewRangeIterator(b, root, first, last, b.schema), nil
}

func NewRangeIterator(tree *BTree, initialNode *BTreeNode, from, to Sortable, schema types.Schema) *RangeIterator {
	return &RangeIterator{
		tree:   tree,
		node:   initialNode,
		schema: schema,
		from:   from,
		to:     to,
	}
}

func (iter *RangeIterator) Dump() {
	vprint.VV("id:%v sc:%v ls:%v", iter.node.page.ID(), iter.node.slotCount(), iter.node.latchState())
	for i := range iter.stack {
		item := iter.stack[i]
		vprint.VV("-->%v:%v:%v", item.n.page.ID(), item.n.latchState(), item.i)
	}
}

func (iter *RangeIterator) Dispose() {
	if iter.node.latchState() != bufferpool.None {
		iter.node.releaseReadLatch()
		iter.tree.unpin(iter.node)
	}
	for i := range iter.stack {
		item := iter.stack[i]
		if item.n.latchState() != bufferpool.None {
			item.n.releaseReadLatch()
			iter.tree.unpin(item.n)
		}
	}
}

func (iter *RangeIterator) Seek(key Sortable) bool {
	if iter.tree == nil {
		return false
	}
	iter.seeked = true
	iter.stack = iter.stack[:0]
	iter.node, iter.err = iter.tree.fetchNode(iter.tree.rootNode)
	if iter.err != nil {
		return false
	}
	iter.node.takeReadLatch()
	sc := iter.node.slotCount()
	if sc == 0 {
		return false
	}
	for {
		i, found := iter.node.findKey(key)
		iter.stack = append(iter.stack, &stackItem{iter.node, i})
		if found || i < sc {
			if iter.node.isLeaf() {
				// got to the leaf
				iter.stack[len(iter.stack)-1].i--

				return found
			} else {
				// need to descend
				slot := iter.node.page.ReadPageSlot(int16(i))
				ipl := slot.InternalPayload(iter.node.page)
				pn := bufferpool.PageID(ipl.ValueAsPagePointer(iter.node.page))
				iter.node, iter.err = iter.tree.fetchNode(pn)
				if iter.err != nil {
					return false
				}
				iter.node.takeReadLatch()
			}
		} else {
			iter.nextNode()
			if iter.err != nil {
				return false
			}
			iter.node.takeReadLatch()
		}

	}
}

func (iter *RangeIterator) First() bool {
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
	iter.node, iter.err = iter.tree.fetchNode(iter.tree.rootNode)
	if iter.err != nil {
		return false
	}
	iter.node.takeReadLatch()
	for {
		iter.stack = append(iter.stack, &stackItem{iter.node, 0})
		if iter.node.isLeaf() {
			break
		}
		iter.nextNode()
	}
	s := iter.stack[len(iter.stack)-1]
	iter.key, iter.item, iter.err = iter.tree.getTuple(iter.node, int(s.i), iter.schema)
	return iter.err == nil
}

func (iter *RangeIterator) nextNode() {
	pn := iter.node.page.ReadNextPointer()
	iter.node.releaseReadLatch()
	iter.tree.unpin(iter.node)
	if pn.Page != bufferpool.INVALID_PAGE {
		iter.node, iter.err = iter.tree.fetchNode(pn)
		return
	}
	iter.atend = true
}

func (iter *RangeIterator) Item() (*BTreeTuple, Sortable) {
	return iter.item, iter.key
}

func (iter *RangeIterator) Next() bool {
	if iter.tree == nil {
		return false
	}
	if !iter.seeked {
		if iter.from == Int(0) {
			return iter.First()
		}
		iter.Seek(iter.from)
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
				iter.node = s.n
				if !s.n.isLeaf() {
					return iter.Next()
				}
				if s.i < s.n.slotCount() {
					break
				}
			}
		}
	} else {
		if s.i == iter.node.slotCount() {
			iter.nextNode()
			if iter.err != nil {
				return false
			}
			iter.stack = append(iter.stack, &stackItem{iter.node, 0})
		} else {
			if s.i > iter.node.slotCount() {
				return false
			}
			slot := iter.node.page.ReadPageSlot(int16(s.i))
			ipl := slot.InternalPayload(iter.node.page)
			pn := bufferpool.PageID(ipl.ValueAsPagePointer(iter.node.page))
			iter.node, _ = iter.tree.fetchNode(pn)
			iter.node.takeReadLatch()
			iter.stack = append(iter.stack, &stackItem{iter.node, 0})
			if !iter.node.isLeaf() {
				return iter.Next()
			}
		}
	}

	s = iter.stack[len(iter.stack)-1]
	iter.key, iter.item, iter.err = iter.tree.getTuple(s.n, s.i, iter.schema)
	return iter.key.Less(iter.to) && iter.err == nil
}
