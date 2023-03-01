// Copyright 2023 Molecula Corp. All rights reserved.

package tstore

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/featurebasedb/featurebase/v3/bufferpool"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/wireprotocol"
	"github.com/pkg/errors"
)

// TODO(pok)
// ▶ on create/alter table, update the schema version
// ▶ on open index run the aries recovery

// ▶ move latchState into Page ✅
// ▶ latch buffer pool ✅
// ▶ schema versioning on the page 🔧
//		▶ write initial schema version on the root page ✅
//		▶ write new schema versions on the root page
//		▶ handle schema version overflow

// ▶ put the insert into the split routine, so we don't have the do the insert after the fact

// - [x] Tuple format
//     - [ ] Tid (int64) - transaction id of last write
//     - [ ] Redo page ptr - pointer to last version of row
//     - [ ] schema version (int16)
//     - [ ] count of fields (int16)
//     - [ ] array of field offsets
//         - [ ] offset for each field (-1 for null)
//     - [ ] data payload
//         - [ ] for each column (len, only for variable columns) data

// - math that that works out max payload length for a tuple
//		- tries to key fanout min >= 4

// ▶ WAL
// 		▶  implement log buffers for wal files
// 		▶  implement write log event for PageID, event id etc.

// ▶ lazy writer on the buffer pool
// 		▶  implement a checkpoint that scans the pool and writes out dirty pages every minute or so

// ▶ MVCC versioning
// ▶ 3000+ columns (thanks Q2)

// ▶ handle nulls on insert
// ▶ backup/restore

// later

// ▶ do a test on concurrent inserts
// ▶ latch buffer I/Os

// BTree represents a b+tree structure used for storing and
// retrieving tuple data for a given shard in a table
type BTree struct {
	mu            sync.RWMutex
	schema        types.Schema
	schemaVersion int

	keysPerLeafPage     int64
	keysPerInternalPage int64

	objectID   int32
	shard      int32
	rootNode   bufferpool.PageID
	bufferpool *bufferpool.BufferPool
}

func NewBTree(maxKeySize int, objectID int32, shard int32, schema types.Schema, bpool *bufferpool.BufferPool) (*BTree, error) {
	// use key size to calculate keys per leaf and internal page
	if maxKeySize > bufferpool.MAX_KEY_ON_PAGE_SIZE {
		return nil, errors.Errorf("max key size exceeded")
	}

	sizeWithoutHeader := bufferpool.PAGE_SIZE - bufferpool.PAGE_SLOTS_START_OFFSET

	// for internal pages chunk size is:
	// keyLength 2
	// keyBytes maxKeySize
	// ptrValue 8
	chunkSize := int64(2 + maxKeySize + 8 + bufferpool.PAGE_SLOT_LENGTH)
	keysPerInternalPage := sizeWithoutHeader / (bufferpool.PAGE_SLOT_LENGTH + chunkSize)

	// for leaf pages chunk size is:
	// keyLength 2
	// keyBytes maxKeySize
	// rowPayloadLen 4
	// rowPayloadBytes [payload length]

	// get the payload length from the schema
	payLoadLength := 0
	for _, s := range schema {
		switch ty := s.Type.(type) {
		case *parser.DataTypeVarchar:
			payLoadLength += 4                  // offset or null
			payLoadLength += 4 + int(ty.Length) // actual data
		default:
			return nil, errors.Errorf("unsupported t-store data type '%T'", ty)
		}
	}

	chunkSize = int64(2 + maxKeySize + 1 + 2 + payLoadLength)
	keysPerLeafPage := int64(4)
	if payLoadLength <= bufferpool.MAX_PAYLOAD_CHUNK_SIZE_ON_PAGE {
		keysPerLeafPage = sizeWithoutHeader / (bufferpool.PAGE_SLOT_LENGTH + chunkSize)
	}

	tree := &BTree{
		objectID:            objectID,
		shard:               shard,
		bufferpool:          bpool,
		keysPerLeafPage:     keysPerLeafPage,
		keysPerInternalPage: keysPerInternalPage,
	}

	headerNode, err := tree.fetchNode(bufferpool.PageID{ObjectID: objectID, Shard: shard, Page: 0})
	if err != nil {
		return nil, err
	}
	headerNode.takeWriteLatch()
	defer headerNode.releaseWriteLatch()
	defer tree.unpin(headerNode)

	slot := headerNode.page.ReadPageSlot(0)
	// no need to protect updating this with a RWMutex yet
	ipl := slot.InternalPayload(headerNode.page)
	tree.rootNode = ipl.ValueAsPagePointer(headerNode.page)

	// see if the headerPage is in overflow
	nextHeader := headerNode.page.ReadNextPointer()
	if nextHeader.Page != bufferpool.INVALID_PAGE {
		// right now overflow is an error
		return nil, errors.Errorf("headerPage overflow")
	}
	// get the slot count from the headerPage
	slotCount := headerNode.page.ReadSlotCount()
	if slotCount > 1 {
		// we have schema versions
		slot = headerNode.page.ReadPageSlot(slotCount - 1)

		pl := slot.KeyPayload(headerNode.page)
		lpl := slot.LeafPayload(headerNode.page)

		latestVersion := int(pl.KeyAsInt(headerNode.page))

		b := lpl.ValueAsBytes(headerNode.page)

		rdr := bytes.NewReader(b)
		_, err = wireprotocol.ExpectToken(rdr, wireprotocol.TOKEN_SCHEMA_INFO)
		if err != nil {
			return nil, err
		}

		s, err := wireprotocol.ReadSchema(rdr)
		if err != nil {
			return nil, err
		}

		newSchema := false
		if len(s) != len(schema) {
			newSchema = true
		} else {
			for i, c := range s {
				if schema[i].ColumnName != c.ColumnName || schema[i].Type.BaseTypeName() != c.Type.BaseTypeName() {
					newSchema = true
					break
				}
			}
		}
		if newSchema {
			return nil, errors.Errorf("schema mismatch")
		}
		tree.schemaVersion = latestVersion
		tree.schema = schema
	} else {
		// no schema versions so write first one
		b, err := wireprotocol.WriteSchema(schema)
		if err != nil {
			return nil, err
		}

		err = tree.writeLeafEntryInSlot(headerNode, 1, Int(1).Bytes(), b)
		if err != nil {
			return nil, err
		}
		headerNode.page.WriteSlotCount(2)

		tree.schemaVersion = 1
		tree.schema = schema
		tree.bufferpool.FlushPage(headerNode.page.ID())
	}
	return tree, nil
}

// protocol for latching
// 		▶ latch parent node
// 		▶ get latch for childNode
// 		▶ Release latch for parent if “safe”.
// 			• A safe node is one that will not split or merge when updated.
// 				▶ Not full (on insertion)
// 				▶ More than half-full (on deletion)
//
//

//Latching for Search --> start at root with a read latch and go down; repeatedly,
// ▶ Acquire read latch on child
// ▶ Then unlatch parent

func (b *BTree) Search(currentNode *BTreeNode, k Sortable) (Sortable, *BTreeTuple) {
	if currentNode != nil {
		if currentNode.isLeaf() {
			defer currentNode.releaseReadLatch()
			defer b.unpin(currentNode)
			// search for the key
			i, found := currentNode.findKey(k)
			if found {
				slot := currentNode.page.ReadPageSlot(int16(i))
				lpl := slot.LeafPayload(currentNode.page)
				return k, NewBTreeTupleFromBytes(lpl.ValueAsBytes(currentNode.page), b.schema)
			}
			return nil, nil
		} else {
			nodePtr := b.findNextPointer(currentNode, k)
			node, _ := b.fetchNode(nodePtr)
			node.takeReadLatch()
			currentNode.releaseReadLatch()
			b.unpin(currentNode)
			return b.Search(node, k)
		}
	} else {
		n, _ := b.fetchNode(b.rootNode)
		n.takeReadLatch()
		return b.Search(n, k)
	}
}

//Latching for Insert --> start at root and go down, start at root with a read latch and go down; repeatedly,
// 		▶ latch parent node
// 		▶ get latch for childNode
// 			▶ if childNode is a leaf and will split and we only have a read latch, bail and start from the top in exclusive mode
// 		▶ release latch for parent if “safe”.
// 			• A safe node is one that will not split or merge when updated.
// 				▶ Not full (on insertion)

func (b *BTree) Insert(tup *BTreeTuple) error {
	// go get the root page from the buffer pool
	node, err := b.fetchNode(b.rootNode)
	if err != nil {
		return err
	}

	key := tup.keyValue()

	// special handling for the case where root is a leaf
	// if it is a leaf, we are always going to be writing
	// here so take a write latch
	if node.isLeaf() {
		node.takeWriteLatch()
	} else {
		node.takeReadLatch()
	}

	forceExclusive := false
	for {
		// does the root need to split?
		if b.isNodeFull(node) {

			// to do a split, we need a write latch so check to see if we have one
			// if not, we need to retry in exclusive mode
			if node.latchState() != bufferpool.Write {
				// release the read latch & take a write latch
				node.releaseAnyLatch()
				node.takeWriteLatch()
				// retry in exclusive mode
				forceExclusive = true
				continue
			}

			//split the node
			lhs, pivot, rhs, err := b.splitNode(node)
			if err != nil {
				return err
			}
			lhsPtr := lhs.page.ID()
			rhsPtr := rhs.page.ID()

			// decide which of the node to do the pending insert into
			// and release write latch an unpin on the other node
			var n *BTreeNode
			if key.Less(pivot) {
				n = lhs
				rhs.releaseWriteLatch()
				b.unpin(rhs)
			} else {
				n = rhs
				lhs.releaseWriteLatch()
				b.unpin(lhs)
			}

			// do the insert into the node
			err = b.insertNonFull(n, key, tup, forceExclusive)
			if err != nil {
				return err
			}

			// this is the root node splitting so handle that...
			err = b.handleRootNodeSplit(pivot, lhsPtr, rhsPtr)
			if err != nil {
				return err
			}

			return nil
		} else {
			err := b.insertNonFull(node, key, tup, forceExclusive)
			if err != nil {
				if err == ErrNeedsExclusive {
					// release the read latch & take a write latch
					node.releaseAnyLatch()
					node.takeWriteLatch()
					// retry in exclusive mode
					forceExclusive = true
					continue
				}
				return err
			}
			return nil
		}
	}
}

// private methods

func (b *BTree) fetchNode(pageID bufferpool.PageID) (*BTreeNode, error) {
	page, err := b.bufferpool.FetchPage(pageID)
	if err != nil {
		return nil, err
	}
	node := &BTreeNode{
		page: page,
	}
	return node, nil
}

func (b *BTree) unpin(node *BTreeNode) error {
	return b.bufferpool.UnpinPage(node.page.ID())
}

func (b *BTree) newOverflow() (*BTreeNode, error) {
	page, err := b.bufferpool.NewPage(b.objectID, b.shard)
	if err != nil {
		return nil, err
	}
	page.WritePageType(int16(bufferpool.PAGE_TYPE_BTREE_OVERFLOW))
	node := &BTreeNode{
		page: page,
	}
	return node, nil
}

func (b *BTree) newLeaf() (*BTreeNode, error) {
	page, err := b.bufferpool.NewPage(b.objectID, b.shard)
	if err != nil {
		return nil, err
	}
	page.WritePageType(int16(bufferpool.PAGE_TYPE_BTREE_LEAF))
	node := &BTreeNode{
		page: page,
	}
	return node, nil
}

func (b *BTree) newInternal() (*BTreeNode, error) {
	page, err := b.bufferpool.NewPage(b.objectID, b.shard)
	if err != nil {
		return nil, err
	}
	page.WritePageType(int16(bufferpool.PAGE_TYPE_BTREE_INTERNAL))
	node := &BTreeNode{
		page: page,
	}
	return node, nil
}

func (b *BTree) isNodeFull(n *BTreeNode) bool {
	if n.latchState() == bufferpool.None {
		panic("unexpected latch state")
	}

	if n.isLeaf() {
		sc := n.slotCount()
		if sc >= int(b.keysPerLeafPage) {
			return true
		}
	} else {
		sc := n.slotCount()
		if sc >= int(b.keysPerInternalPage) {
			return true
		}
	}
	return false
}

func (b *BTree) findNextPointer(node *BTreeNode, key Sortable) bufferpool.PageID {
	slotCount := int(node.page.ReadSlotCount())

	minIndex := 0
	onePastMaxIndex := slotCount
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		slot := node.page.ReadPageSlot(int16(index))
		pl := slot.KeyPayload(node.page)
		keyAtIndex := Int(pl.KeyAsInt(node.page))
		if key.Less(keyAtIndex) {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	if minIndex == slotCount {
		// we didn't find it so return the next pointer
		nextPtr := node.page.ReadNextPointer()
		return nextPtr
	} else {
		slot := node.page.ReadPageSlot(int16(minIndex))
		ipl := slot.InternalPayload(node.page)
		nextPtr := ipl.ValueAsPagePointer(node.page)
		return nextPtr
	}
}

func (b *BTree) setRootNode(newRootNode bufferpool.PageID) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// get the header node
	headerNode, err := b.fetchNode(bufferpool.PageID{ObjectID: b.objectID, Shard: b.shard, Page: 0})
	if err != nil {
		return err
	}
	headerNode.takeWriteLatch()
	defer headerNode.releaseWriteLatch()
	defer b.unpin(headerNode)

	// root page pointer is in slot 0
	slot := headerNode.page.ReadPageSlot(0)

	ipl := slot.InternalPayload(headerNode.page)

	// update the root page pointer
	ipl.PutPagePointer(headerNode.page, newRootNode)

	b.bufferpool.FlushPage(headerNode.page.ID())

	b.rootNode = newRootNode
	return nil
}

func (b *BTree) handleRootNodeSplit(pivot Sortable, lhsPtr bufferpool.PageID, rhsPtr bufferpool.PageID) error {
	// create a new root node
	newRoot, err := b.newInternal()
	if err != nil {
		return err
	}
	newRoot.takeWriteLatch()
	defer newRoot.releaseWriteLatch()
	defer b.unpin(newRoot)

	// add the pivot key pointing to the old root page
	b.insertInternalEntryAt(newRoot, 0, pivot, lhsPtr)

	// set the next ptr to point to newNode
	newRoot.page.WriteNextPointer(rhsPtr)

	b.setRootNode(newRoot.page.ID())
	return nil
}

func (b *BTree) compactLeafPage(node *BTreeNode) error {
	return nil
}

// this function handles overflow
func (b *BTree) writeLeafEntryInSlot(node *BTreeNode, slotNumber int16, keyBytes []byte, payloadBytes []byte) error {

	keyLength := len(keyBytes)
	payloadChunkLength := len(payloadBytes)

	payloadTotalLength := payloadChunkLength

	flags := 0

	if payloadChunkLength > bufferpool.MAX_PAYLOAD_CHUNK_SIZE_ON_PAGE {
		// we need to overflow to a new page, so set the fact we have to overflow
		flags = 1
		// cap the write size
		payloadChunkLength = bufferpool.MAX_PAYLOAD_CHUNK_SIZE_ON_PAGE
	}

	// double check we won't blow free space on page
	onPageSize := node.page.ComputeLeafPayloadTotalLength(keyLength, payloadChunkLength)
	if (onPageSize + bufferpool.PAGE_SLOT_LENGTH) > int32(node.page.FreeSpaceOnPage()) {
		err := b.compactLeafPage(node)
		if err != nil {
			return err
		}
		// check again
		if (onPageSize + bufferpool.PAGE_SLOT_LENGTH) > int32(node.page.FreeSpaceOnPage()) {
			// this shouldn't happen - if it does we have a logic error
			// in our assumptions about how many keys fit on a page
			// or saggitarius is rising in scorpio, or somesuch
			panic("page is full")
		}
	}

	// get the current freespace offset
	freeSpaceOffset := node.page.ReadFreeSpaceOffset()

	// compute the new free space offset for this page
	freeSpaceOffset -= int16(onPageSize)
	offset := freeSpaceOffset

	if flags == 1 {
		// we are going to overflow so allocate an overflow page
		overflowPage, err := b.newOverflow()
		if err != nil {
			return err
		}
		overflowPage.takeWriteLatch()
		defer overflowPage.releaseWriteLatch()
		defer b.unpin(overflowPage)

		bytesRemaining := payloadTotalLength
		lowWater := 0
		hiWater := payloadChunkLength
		// write the data on this page
		offset = node.page.WriteLeafPagePayloadHeader(offset, int16(keyLength), keyBytes, int8(flags), overflowPage.page.ID().Page, int32(payloadTotalLength))
		node.page.WriteLeafPagePayloadBytes(offset, int16(payloadChunkLength), payloadBytes[lowWater:hiWater])

		// now we've written payloadChunkLen bytes of payload to node.page, now write to overflow page
		bytesRemaining -= payloadChunkLength

		for bytesRemaining > 0 {
			overflowFreeSpace := int(overflowPage.page.FreeSpaceOnPage())

			var err error
			var nextOverflowPage *BTreeNode
			nextOverflowPtr := int64(0)
			if bytesRemaining > overflowFreeSpace {
				// we're gonna need another overflow page
				nextOverflowPage, err = b.newOverflow()
				if err != nil {
					return err
				}
				nextOverflowPage.takeWriteLatch()
				defer nextOverflowPage.releaseWriteLatch()
				defer b.unpin(nextOverflowPage)
				nextOverflowPtr = int64(nextOverflowPage.page.ID().Page)
			}

			lowWater = hiWater
			// set the payload chunk length to the free space on the page
			// less the 2 byte chunk length
			payloadChunkLength := overflowFreeSpace - 2
			hiWater += payloadChunkLength

			if nextOverflowPtr > 0 {
				overflowPage.page.WriteNextPointer(bufferpool.PageID{ObjectID: b.objectID, Shard: b.shard, Page: nextOverflowPtr})
			}

			overflowPage.page.WriteLeafPagePayloadBytes(bufferpool.PAGE_SLOTS_START_OFFSET, int16(payloadChunkLength), payloadBytes[lowWater:hiWater])
			bytesRemaining -= payloadChunkLength
			overflowPage = nextOverflowPage
		}

	} else {
		offset = node.page.WriteLeafPagePayloadHeader(offset, int16(keyLength), keyBytes, int8(flags), 0, int32(payloadTotalLength))
		node.page.WriteLeafPagePayloadBytes(offset, int16(payloadChunkLength), payloadBytes)
	}

	// update the free space offset on this page
	node.page.WriteFreeSpaceOffset(int16(freeSpaceOffset))

	// make a slot and write it
	slot := bufferpool.PageSlot{
		PayloadOffset: freeSpaceOffset,
	}
	node.page.WritePageSlot(slotNumber, slot)
	return nil
}

func (b *BTree) insertLeafEntryAt(node *BTreeNode, i int, key Sortable, tup *BTreeTuple) error {
	if node.latchState() != bufferpool.Write {
		panic("unexpected latch state")
	}

	// get the slot count
	slotCount := int(node.page.ReadSlotCount())

	// move all the slots after where we are going to insert
	for j := slotCount; j > i; j-- {
		sl := node.page.ReadPageSlot(int16(j - 1))
		node.page.WritePageSlot(int16(j), sl)
	}

	// put the payload together
	valueData, err := tup.Bytes()
	if err != nil {
		return err
	}

	// write to WAL here before we write to page!!!

	err = b.writeLeafEntryInSlot(node, int16(i), key.Bytes(), valueData)
	if err != nil {
		return err
	}

	// update the slot count
	slotCount++
	node.page.WriteSlotCount(int16(slotCount))

	return nil
}

func (b *BTree) writeInternalEntryInSlot(node *BTreeNode, slotNumber int16, keyBytes []byte, ptrValue int64) error {

	// get the current freespace offset
	freeSpaceOffset := node.page.ReadFreeSpaceOffset()

	keyLength := len(keyBytes)

	onPageSize := node.page.ComputeInternalPayloadTotalLength(keyLength)

	// compute the new free space offset for this page
	freeSpaceOffset -= int16(onPageSize)

	node.page.WriteInternalPagePayload(freeSpaceOffset, int16(keyLength), keyBytes, ptrValue)

	// update the free space offset on this page
	node.page.WriteFreeSpaceOffset(int16(freeSpaceOffset))

	// make a slot and write it
	slot := bufferpool.PageSlot{
		PayloadOffset: freeSpaceOffset,
	}
	node.page.WritePageSlot(slotNumber, slot)
	return nil
}

func (b *BTree) insertInternalEntryAt(node *BTreeNode, i int, key Sortable, pageID bufferpool.PageID) error {
	if node.latchState() != bufferpool.Write {
		panic("unexpected latch state")
	}

	// get the slot count
	slotCount := int(node.page.ReadSlotCount())

	// move all the slots after where we are going to insert
	for j := slotCount; j > i; j-- {
		sl := node.page.ReadPageSlot(int16(j - 1))
		node.page.WritePageSlot(int16(j), sl)
	}

	err := b.writeInternalEntryInSlot(node, int16(i), key.Bytes(), pageID.Page)
	if err != nil {
		return err
	}

	// update the slot count
	slotCount++
	node.page.WriteSlotCount(int16(slotCount))

	return nil
}

func (b *BTree) updatePointerEntryAt(node *BTreeNode, i int, value bufferpool.PageID) error {
	slot := node.page.ReadPageSlot(int16(i))
	ipl := slot.InternalPayload(node.page)
	return ipl.PutPagePointer(node.page, value)
}

func (b *BTree) splitNode(nodeToSplit *BTreeNode) (*BTreeNode, Sortable, *BTreeNode, error) {
	// TODO(pok) handle the sitch when inserting the new key and it ends up as the min key in rhs
	if nodeToSplit.isLeaf() {
		return b.splitLeafNode(nodeToSplit)
	} else {
		return b.splitInternalNode(nodeToSplit)
	}
}

func (b *BTree) insertNonFull(node *BTreeNode, key Sortable, tup *BTreeTuple, forceExclusive bool) error {
	if node.isLeaf() {
		// fmt.Printf("leaf insert on page %v (%v)\n", node.page.ID(), tup)

		if node.latchState() != bufferpool.Write {
			panic("unexpected latch state")
		}

		defer node.releaseWriteLatch()
		defer b.unpin(node)

		i, exists := node.findKey(key)
		if exists {
			return errors.Errorf("key violation")
		}

		err := b.insertLeafEntryAt(node, i, key, tup)
		if err != nil {
			return err
		}

		return nil
	} else {
		// its an internal node so follow the pointers
		childPtr, err := node.findNextPointer(key, b.objectID, b.shard)
		if err != nil {
			return err
		}
		childNode, err := b.fetchNode(childPtr)
		if err != nil {
			return err
		}

		// fmt.Printf("internal node search on page %v: key=%v, childptr=%v\n", node.page.ID(), key, childPtr)

		// we need to latch child node
		if forceExclusive {
			childNode.takeWriteLatch()
		} else {
			// we're not exclusive...

			// ...but given we're inserting, if the child node is a leaf
			// node we need to take a write latch on it
			if childNode.isLeaf() {
				childNode.takeWriteLatch()
			} else {
				childNode.takeReadLatch()
			}
		}

		// is the next node full? if so we need to split
		// we need to write to node (the parent), lhs and rhs
		if b.isNodeFull(childNode) {
			// we need to split, but if we only have a read latch on the
			// parent, we need to bail and retry
			if node.latchState() != bufferpool.Write {
				// release latch on node
				node.releaseAnyLatch()
				// release latch on childNode
				childNode.releaseAnyLatch()
				return ErrNeedsExclusive
			}

			slotCount := node.slotCount()

			lhs, pivot, rhs, err := b.splitNode(childNode)
			if err != nil {
				return err
			}

			// find out where we put the new pointer
			j, _ := node.findKey(pivot)

			// add the pivot key pointing to the lhs page
			b.insertInternalEntryAt(node, j, pivot, lhs.page.ID())

			// if not the last key seperator, adjust the pointer for the adjacent key seperator
			// if it is the last one, set the next pointer
			if j < slotCount {
				b.updatePointerEntryAt(node, j+1, rhs.page.ID())
			} else {
				//set the next ptr to point to the rhs page
				node.page.WriteNextPointer(rhs.page.ID())
			}

			node.releaseWriteLatch()
			b.unpin(node)

			// find out which node our key needs to go into
			if key.Less(pivot) {
				rhs.releaseWriteLatch()
				b.unpin(rhs)
				return b.insertNonFull(lhs, key, tup, forceExclusive)
			} else {
				lhs.releaseWriteLatch()
				b.unpin(lhs)
				return b.insertNonFull(rhs, key, tup, forceExclusive)
			}
		} else {
			// given the child node was not full, we can unlatch the parent here
			node.releaseAnyLatch()
			b.unpin(node)

			// now do the insert on the child node
			return b.insertNonFull(childNode, key, tup, forceExclusive)
		}
	}
}

func (b *BTree) splitLeafNode(nodeToSplit *BTreeNode) (*BTreeNode, Sortable, *BTreeNode, error) {
	// fmt.Printf("leaf split on page %v\n", nodeToSplit.page.ID())

	// where we are going to split
	slotCount := int(nodeToSplit.page.ReadSlotCount())
	splitPoint := slotCount / 2
	// get the node to split
	lhs := nodeToSplit
	// make a new node
	rhs, err := b.newLeaf()
	if err != nil {
		return nil, nil, nil, err
	}
	rhs.takeWriteLatch()

	seperationKeySlot := lhs.page.ReadPageSlot(int16(splitPoint))
	pl := seperationKeySlot.KeyPayload(lhs.page)
	seperationKey := Int(pl.KeyAsInt(lhs.page))

	leftSlotCount := int(lhs.page.ReadSlotCount())

	// find the split point
	var rightSlotCount = leftSlotCount - splitPoint

	//copy the data from left to right
	freeSpaceOffset := rhs.page.ReadFreeSpaceOffset()
	for i := 0; i < rightSlotCount; i++ {
		j := splitPoint + i

		// read the slot from the page
		s := lhs.page.ReadPageSlot(int16(j))
		// read the chunk from the page
		lbl := s.LeafPayload(lhs.page)
		c := lbl.GetPayloadReader(lhs.page)

		// mod the freespace offset based on the size of the chunk
		freeSpaceOffset -= int16(c.Length())
		// update the values in the slot
		s.PayloadOffset = freeSpaceOffset

		// write the chunk
		offset := rhs.page.WriteLeafPagePayloadHeader(int16(freeSpaceOffset), c.KeyLength, c.KeyBytes, c.Flags, c.OverflowPtr, c.PayloadTotalLength)
		rhs.page.WriteLeafPagePayloadBytes(offset, c.PayloadChunkLength, c.PayloadChunkBytes)
		//write the slot
		rhs.page.WritePageSlot(int16(i), s)

		// update the free space offset
		rhs.page.WriteFreeSpaceOffset(int16(freeSpaceOffset))
	}

	// set the new slotcounts
	lhs.page.WriteSlotCount(int16(splitPoint))
	rhs.page.WriteSlotCount(int16(rightSlotCount))

	// set sibling pointers
	rightPtr := lhs.page.ReadNextPointer()
	rhs.page.WriteNextPointer(rightPtr)
	lhs.page.WriteNextPointer(rhs.page.ID())
	rhs.page.WritePrevPointer(lhs.page.ID())

	return lhs, seperationKey, rhs, nil
}

func (b *BTree) splitInternalNode(nodeToSplit *BTreeNode) (*BTreeNode, Sortable, *BTreeNode, error) {
	// fmt.Printf("internal split on page %v\n", nodeToSplit.page.ID())

	// where we are going to split
	slotCount := int(nodeToSplit.page.ReadSlotCount())
	splitPoint := slotCount / 2

	// get the node to split
	lhs := nodeToSplit
	// make a new node
	rhs, err := b.newInternal()
	if err != nil {
		return nil, nil, nil, err
	}
	rhs.takeWriteLatch()

	seperationKeySlot := lhs.page.ReadPageSlot(int16(splitPoint - 1))
	pl := seperationKeySlot.KeyPayload(lhs.page)
	seperationKey := Int(pl.KeyAsInt(lhs.page))
	ipl := seperationKeySlot.InternalPayload(lhs.page)
	lhsNext := ipl.ValueAsPagePointer(lhs.page)

	leftSlotCount := int(lhs.page.ReadSlotCount())

	// find the split point
	var rightSlotCount = leftSlotCount - splitPoint

	//copy the data from left to right
	freeSpaceOffset := rhs.page.ReadFreeSpaceOffset()
	for i := 0; i < rightSlotCount; i++ {
		j := splitPoint + i

		// read the slot from the page
		s := lhs.page.ReadPageSlot(int16(j))
		// read the chunk from the page
		ipl := s.InternalPayload(lhs.page)
		c := ipl.InternalPageChunk(lhs.page)

		// mod the freespace offset based on the size of the chunk
		freeSpaceOffset -= int16(c.Length())
		// update the values in the slot
		s.PayloadOffset = freeSpaceOffset

		// write the chunk
		rhs.page.WriteInternalPageChunk(int16(freeSpaceOffset), c)
		//write the slot
		rhs.page.WritePageSlot(int16(i), s)

		// update the free space offset
		rhs.page.WriteFreeSpaceOffset(int16(freeSpaceOffset))
	}

	// set the new slotcounts
	lhs.page.WriteSlotCount(int16(splitPoint - 1))
	rhs.page.WriteSlotCount(int16(rightSlotCount))

	// set the next ptr pages
	rhs.page.WriteNextPointer(lhs.page.ReadNextPointer())
	lhs.page.WriteNextPointer(lhsNext)

	return lhs, seperationKey, rhs, nil
}

// func (b *BTree) Delete(x *BTreeNode, k int) {
// 	t := b.t
// 	i := 0
// 	for i < x.slotCount() && k > x.keys[i].Key {
// 		i += 1
// 	}
// 	if x.isLeaf {
// 		if i < x.slotCount() && x.keys[i].Key == k {
// 			x.keys = removePair(x.keys, i)
// 		}
// 		return
// 	}

// 	if i < x.slotCount() && x.keys[i].Key == k {
// 		b.deleteInternalNode(x, k, i)
// 	} else if len(x.child[i].keys) >= t {
// 		b.Delete(x.child[i], k)
// 	} else {
// 		if i != 0 && i+2 < len(x.child) {
// 			if len(x.child[i-1].keys) >= t {
// 				b.deleteSibling(x, i, i-1)
// 			} else if len(x.child[i+1].keys) >= t {
// 				b.deleteSibling(x, i, i+1)
// 			} else {
// 				b.deleteMerge(x, i, i+1)
// 			}
// 		} else if i == 0 {
// 			if len(x.child[i+1].keys) >= t {
// 				b.deleteSibling(x, i, i+1)
// 			} else {
// 				b.deleteMerge(x, i, i+1)
// 			}
// 		} else if i+1 == len(x.child) {
// 			if len(x.child[i-1].keys) >= t {
// 				b.deleteSibling(x, i, i-1)
// 			} else {
// 				b.deleteMerge(x, i, i-1)
// 			}
// 		}
// 		b.Delete(x.child[i], k)
// 	}

// }

// func (b *BTree) deleteInternalNode(x *BTreeNode, k int, i int) {
// 	t := b.t
// 	if x.isLeaf {
// 		if x.keys[i].Key == k {
// 			x.keys = removePair(x.keys, i)
// 			return
// 		}
// 		return
// 	}

// 	if len(x.child[i].keys) >= t {
// 		x.keys[i] = b.deletePredecessor(x.child[i])
// 		return
// 	} else if len(x.child[i+1].keys) >= t {
// 		x.keys[i] = b.deleteSuccessor(x.child[i+1])
// 		return
// 	} else {
// 		b.deleteMerge(x, i, i+1)
// 		b.deleteInternalNode(x.child[i], k, b.t-1)
// 	}
// }

// func (b *BTree) deletePredecessor(x *BTreeNode) *Pair {
// 	if x.isLeaf {
// 		k := x.keys[x.slotCount()-1]
// 		x.keys = removePair(x.keys, x.slotCount()-1)
// 		return k
// 	}
// 	n := x.slotCount() - 1
// 	if len(x.child[n].keys) >= b.t {
// 		b.deleteSibling(x, n, n+1)
// 	} else {
// 		b.deleteMerge(x, n, n+1)
// 	}
// 	return b.deletePredecessor(x.child[n])
// }

// func (b *BTree) deleteSuccessor(x *BTreeNode) *Pair {
// 	if x.isLeaf {
// 		k := x.keys[0]
// 		x.keys = removePair(x.keys, 0)
// 		return k
// 	}
// 	if len(x.child[1].keys) >= b.t {
// 		b.deleteSibling(x, 0, 1)
// 	} else {
// 		b.deleteMerge(x, 0, 1)
// 	}
// 	return b.deleteSuccessor(x.child[0])
// }

// func (b *BTree) deleteMerge(x *BTreeNode, i int, j int) {
// 	cnode := x.child[i]
// 	var new *BTreeNode

// 	if j > i {
// 		rsnode := x.child[j]
// 		cnode.keys = append(cnode.keys, x.keys[i])
// 		for q, k := range rsnode.keys {
// 			cnode.keys = append(cnode.keys, k)
// 			if len(rsnode.child) > 0 {
// 				cnode.child = append(cnode.child, rsnode.child[q])
// 			}
// 		}
// 		if len(rsnode.child) > 0 {
// 			cnode.child = append(cnode.child, rsnode.child[len(rsnode.child)-1])
// 			rsnode.child = removeNode(rsnode.child, len(rsnode.child)-1)
// 		}
// 		new = cnode
// 		x.keys = removePair(x.keys, i)
// 		x.child = removeNode(x.child, j)
// 	} else {
// 		lsnode := x.child[j]
// 		lsnode.keys = append(lsnode.keys, x.keys[j])
// 		for q, k := range cnode.keys {
// 			lsnode.keys = append(lsnode.keys, k)
// 			if len(lsnode.child) > 0 {
// 				lsnode.child = append(lsnode.child, cnode.child[q])
// 			}
// 		}
// 		new = lsnode
// 		x.keys = removePair(x.keys, j)
// 		x.child = removeNode(x.child, i)
// 	}

// 	if x == b.Root && x.slotCount() == 0 {
// 		b.Root = new
// 	}
// }

// func (b *BTree) deleteSibling(x *BTreeNode, i int, j int) {
// 	cnode := x.child[i]
// 	if i < j {
// 		rsnode := x.child[j]
// 		cnode.keys = append(cnode.keys, x.keys[i])
// 		x.keys[i] = rsnode.keys[0]

// 		if len(rsnode.child) > 0 {
// 			cnode.child = append(cnode.child, rsnode.child[0])
// 			rsnode.child = removeNode(rsnode.child, 0)
// 		}
// 		rsnode.keys = removePair(rsnode.keys, 0)
// 	} else {
// 		lsnode := x.child[j]
// 		cnode.keys = insertPair(cnode.keys, 0, x.keys[i-1])
// 		x.keys[i-1] = lsnode.keys[len(lsnode.keys)-1]
// 		lsnode.keys = removePair(lsnode.keys, len(lsnode.keys)-1)
// 		if len(lsnode.child) > 0 {
// 			insertNode(cnode.child, 0, nil)
// 			p := lsnode.child[len(lsnode.child)-1]
// 			lsnode.child = removeNode(lsnode.child, len(lsnode.child)-1)
// 			cnode.child = insertNode(cnode.child, 0, p)
// 		}
// 	}
// }

// func removeNode(a []*BTreeNode, s int) []*BTreeNode {
// 	return append(a[:s], a[s+1:]...)
// }

// func insertNode(a []*BTreeNode, index int, value *BTreeNode) []*BTreeNode {
// 	if len(a) == index { // nil or empty slice or after last element
// 		return append(a, value)
// 	}
// 	a = append(a[:index+1], a[index:]...) // index < len(a)
// 	a[index] = value
// 	return a
// }

// func removePair(a []*Pair, s int) []*Pair {
// 	return append(a[:s], a[s+1:]...)
// }

// func insertPair(a []*Pair, index int, value *Pair) []*Pair {
// 	if len(a) == index { // nil or empty slice or after last element
// 		return append(a, value)
// 	}
// 	a = append(a[:index+1], a[index:]...) // index < len(a)
// 	a[index] = value
// 	return a
// }

func (b *BTree) Dump(l int) {
	fmt.Printf("btree(keysPerLeafPage: %d, keysPerInternalPage: %d)\n", b.keysPerLeafPage, b.keysPerInternalPage)

	node, _ := b.fetchNode(b.rootNode)
	defer b.bufferpool.UnpinPage(node.page.ID())
	b.nodeDump(node, l)
}

func (b *BTree) nodeDump(node *BTreeNode, l int) {
	fmt.Printf("%snode(%d) --> leafNode: %v, slotCount: %d\n", fmt.Sprintf("%*s", l, ""), node.page.ID(), node.isLeaf(), node.page.ReadSlotCount())

	if node.isLeaf() {
		keys := ""
		sc := int16(node.slotCount())
		si := bufferpool.NewPageSlotIterator(node.page, 0)
		slot := si.Next()
		for slot != nil {
			pl := slot.KeyPayload(node.page)
			k := int(pl.KeyAsInt(node.page))
			keys += fmt.Sprintf("%d", k)
			if si.Cursor() < sc {
				keys += ", "
			}
			slot = si.Next()
		}
		fmt.Printf("%skeys [%s]\n", fmt.Sprintf("%*s", l+2, ""), keys)
	} else {
		fmt.Printf("%ssep-keys [\n", fmt.Sprintf("%*s", l+2, ""))
		si := bufferpool.NewPageSlotIterator(node.page, 0)
		slot := si.Next()
		for slot != nil {
			pl := slot.KeyPayload(node.page)
			k := int(pl.KeyAsInt(node.page))
			fmt.Printf("%s<%d\n", fmt.Sprintf("%*s", l+4, ""), k)
			ipl := slot.InternalPayload(node.page)
			pn := bufferpool.PageID(ipl.ValueAsPagePointer(node.page))
			cn, _ := b.fetchNode(pn)
			b.nodeDump(cn, l+6)
			b.bufferpool.UnpinPage(cn.page.ID())
			slot = si.Next()
		}
		pn := node.page.ReadNextPointer()
		if pn.Page != bufferpool.INVALID_PAGE {
			fmt.Printf("%s>=(next)\n", fmt.Sprintf("%*s", l+4, ""))
			cn, _ := b.fetchNode(pn)
			b.nodeDump(cn, l+6)
			b.bufferpool.UnpinPage(cn.page.ID())
		} else {
			fmt.Printf("%s>=(next MISSING!)\n", fmt.Sprintf("%*s", l+4, ""))
		}
		fmt.Printf("%s]\n", fmt.Sprintf("%*s", l+2, ""))
	}
}
