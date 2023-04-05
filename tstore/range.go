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
	item   *BTreeTuple
	key    Sortable
	//
	cursor int
	done   bool
	err    error
}

func (b *BTree) NewRangeIterator(first, last Sortable) (*RangeIterator, error) {
	root, err := b.fetchNode(b.rootNode)
	if err != nil {
		return nil, err
	}
	return NewRangeIterator(b, root, first, last, b.schema), nil
}

func NewRangeIterator(tree *BTree, initialNode *BTreeNode, from, to Sortable, schema types.Schema) *RangeIterator {
	r := &RangeIterator{
		tree:   tree,
		node:   initialNode,
		schema: schema,
		from:   from,
		to:     to,
	}
	r.Seek(from)
	return r
}

func (iter *RangeIterator) Dump() {
	vprint.VV("id:%v sc:%v ls:%v", iter.node.page.ID(), iter.node.slotCount(), iter.node.latchState())
}

func (iter *RangeIterator) Dispose() {
	if iter.node.latchState() != bufferpool.None {
		iter.node.releaseReadLatch()
		iter.tree.unpin(iter.node)
	}
}

func (iter *RangeIterator) Seek(key Sortable) {
	iter.node, iter.err = iter.tree.fetchNode(iter.tree.rootNode)
	if iter.err != nil {
		return
	}
	iter.node.takeReadLatch()
	iter.done, iter.err = iter.seek(iter.node, iter.schema, key)
}

func (iter *RangeIterator) seek(currentNode *BTreeNode, schema types.Schema, key Sortable) (bool, error) {
	if currentNode.isLeaf() {
		iter.node = currentNode
		c, _ := currentNode.findKey(key)
		iter.cursor = c - 1
		return c == currentNode.slotCount(), nil
	}
	nodePtr := iter.tree.findNextPointer(currentNode, key)
	node, err := iter.tree.fetchNode(nodePtr)
	if err != nil {
		return false, err
	}
	node.takeReadLatch()
	currentNode.releaseReadLatch()
	iter.tree.unpin(currentNode)
	return iter.seek(node, schema, key)
}

func (iter *RangeIterator) Item() (*BTreeTuple, Sortable) {
	return iter.item, iter.key
}

func (iter *RangeIterator) Next() bool {
	if iter.done {
		return false
	}
	iter.cursor++
	if iter.cursor == iter.node.slotCount() {
		page := iter.node.page.ReadNextPointer()
		iter.node.releaseReadLatch()
		iter.tree.unpin(iter.node)
		if page.Page == bufferpool.INVALID_PAGE { // at the end
			return false
		}
		n, err := iter.tree.fetchNode(page)
		iter.err = err
		if iter.err != nil {
			return false
		}
		iter.node = n
		iter.node.takeReadLatch()
		iter.cursor = 0
	}
	iter.key, iter.item, iter.err = iter.tree.getTuple(iter.node, int(iter.cursor), iter.schema)
	return iter.key.Less(iter.to) && iter.err == nil
}

// TODO(twg) 2023/04/05 untested just added while it was top of mind
func (iter *RangeIterator) Prev() bool {
	if iter.done {
		return false
	}
	iter.cursor--
	if iter.cursor < 0 {
		page := iter.node.page.ReadPrevPointer()
		iter.node.releaseReadLatch()
		iter.tree.unpin(iter.node)
		if page.Page == bufferpool.INVALID_PAGE { // at the end
			return false
		}
		n, err := iter.tree.fetchNode(page)
		iter.err = err
		if iter.err != nil {
			return false
		}
		iter.node = n
		iter.node.takeReadLatch()
		iter.cursor = iter.node.slotCount() - 1
	}
	iter.key, iter.item, iter.err = iter.tree.getTuple(iter.node, int(iter.cursor), iter.schema)
	return iter.from.Less(iter.key) && iter.err == nil
}
