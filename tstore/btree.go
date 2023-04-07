// Copyright 2023 Molecula Corp. All rights reserved.

package tstore

import (
	"fmt"
	"sync"

	"github.com/featurebasedb/featurebase/v3/bufferpool"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

// TODO...

// ▶ WAL
// 		▶  implement log buffers for wal files in bufferpool
//			    in the buffer pool keep some buffers
// 		▶  implement writing of log
//				log every change to a page, before we write to the page
//		▶ log format
//				- previousLSN (LSN is the offset in the log file)
//				- TID
//				- record type
//				- PageID
//				- old data (undo) (+ offset and len) where applicable
//				- new data (redo) (+ offset and len)

// ▶ on open featurebase index run aries recovery (https://blog.acolyer.org/2016/01/08/aries/)

// ▶ lazy writer on the buffer pool
// 		▶  implement a checkpoint that scans the pool and writes out dirty pages periodically

// ▶ MVCC versioning

// ▶ handle updates (delete and an insert)

// ▶ handle deletes

// ▶ backup/restore

// Later....

// ▶ put the insert into the split routine, so we don't have the do the insert after the fact
// ▶ do a test on concurrent inserts
// ▶ smarter latching of buffer I/Os (currently serialized)

type TID int64

const (
	// the slot on the header page that points to the root page for data
	HEADER_SLOT_DATA_ROOT = 0
	// the slot on the header page that points to the root page for schema versions
	HEADER_SLOT_SCHEMA_ROOT = 1
)

// BTree represents a b+tree structure used for storing and
// retrieving tuple data for a given shard in a table
//
// BTrees for FeatureBase t-store data contain a header page
//   - slot 1 will be the pointer to the root page of the tuple data that is being stored in
//     this b-tree
//   - slot 2 of the header page will be a pointer to the root page for the schema data
//     the schema is stored as a tuple of schema version (int), and schema ([]byte)
//     like any other tuple
type BTree struct {
	mu            sync.RWMutex
	schema        types.Schema
	schemaVersion int

	keysPerLeafPage     int64
	keysPerInternalPage int64

	objectID   int32
	shard      int32
	rootNode   bufferpool.PageID
	schemaNode bufferpool.PageID
	bufferpool *bufferpool.BufferPool
}

// NewBTree creates a b+tree for a schema object denoted by objectID, and a shard denoted by shard. The maxKeySize specifies the
// size of the key to be used for this b-tree. Once the data is persisted to disk, it is a very bad idea to change this. schema specifies
// the schema used to write new data, this schema will be cheked against a history of schemas used for writes in this b+tree and if it differs
// from the current know schema version, it will be given a new version number (applying only to this b-tree) and stored. This version
// number will be stored with the tuple data. When data is read, the schema version is also read so that data in the page can be interpreted
// correctly.
func NewBTree(maxKeySize int, objectID int32, shard int32, schema types.Schema, bpool *bufferpool.BufferPool) (*BTree, error) {
	// use key size to calculate keys per leaf and internal page
	if maxKeySize > bufferpool.MAX_KEY_ON_PAGE_SIZE {
		return nil, errors.Errorf("max key size exceeded")
	}

	sizeWithoutHeader := bufferpool.PAGE_SIZE - bufferpool.PAGE_SLOTS_START_OFFSET

	// for internal pages size on page per key is:
	// keyLength 2
	// keyBytes maxKeySize
	// ptrValue 8
	chunkSize := int64( /* keyLength */ 2 + maxKeySize + /* ptrValue */ 8 + bufferpool.PAGE_SLOT_LENGTH)
	keysPerInternalPage := sizeWithoutHeader / (bufferpool.PAGE_SLOT_LENGTH + chunkSize)

	// for leaf pages size on page (worst case) is:
	// keyLength 2
	// keyBytes maxKeySize
	// flags 1
	// overflowPtr 8
	// rowPayloadTotalLen 4
	// rowPayloadChunkLen 2
	// rowPayloadBytes [payload length]

	// get the payload length from the schema
	payLoadLength := /* writeTID */ 8 + /* schemaVersion */ 2 + /* versionsPtr */ 8 + /* flags */ 1
	for _, s := range schema {
		switch ty := s.Type.(type) {
		case *parser.DataTypeVarchar:
			payLoadLength += 4                  // offset or null
			payLoadLength += 4 + int(ty.Length) // actual data
		case *parser.DataTypeVector:
			payLoadLength += 4                    // offset or null
			payLoadLength += 4 + int(ty.Length)*8 // actual data
		default:
			return nil, errors.Errorf("unsupported t-store data type '%T'", ty)
		}
	}

	chunkSize = int64( /* keyLength*/ 2 + maxKeySize + /* flags */ 1 + /* overflowPtr */ 8 + /* rowPayloadTotalLen */ 8 + /* rowPayloadChunkLen */ 2 + payLoadLength)
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
	headerNode.takeReadLatch()
	defer headerNode.releaseReadLatch()
	defer tree.unpin(headerNode)

	// sanity check
	slotCount := headerNode.page.ReadSlotCount()
	if slotCount != 2 {
		panic("inavlid slot count")
	}

	// get the root node for the data part of the b-tree
	slot := headerNode.page.ReadPageSlot(HEADER_SLOT_DATA_ROOT)
	ipl := slot.InternalPayload(headerNode.page)
	tree.rootNode = ipl.ValueAsPagePointer(headerNode.page)

	slot = headerNode.page.ReadPageSlot(HEADER_SLOT_SCHEMA_ROOT)
	ipl = slot.InternalPayload(headerNode.page)
	tree.schemaNode = ipl.ValueAsPagePointer(headerNode.page)

	tree.resolveSchema(schema)

	return tree, nil
}

// GetIterator constructs an iterator on the b+tree so that range scans may be performed or an error
func (b *BTree) GetIterator(from Sortable, to Sortable, reverse bool) (*BTreeNodeIterator, error) {
	n, err := b.fetchNode(b.rootNode)
	if err != nil {
		return nil, err
	}
	n.takeReadLatch()
	iter := b.getIterator(n, from, to, reverse, b.schema)
	return iter, nil
}

// Search finds a key k in the b+tree and returns the matching tuple. If no tuple is
// found, nil is returned
func (b *BTree) Search(k Sortable) (Sortable, *BTreeTuple, error) {
	// go get the root data page from the buffer pool
	n, err := b.fetchNode(b.rootNode)
	if err != nil {
		return nil, nil, err
	}
	n.takeReadLatch()
	return b.search(n, b.schema, k)
}

// Insert inserts a tuple into the b+tree. The key is assumed to be in the first column of the tuple.
// The function returns nill on success or an error.
//
// We use an optimistic latching model for inserts. (see insertNonFull() for more details)
func (b *BTree) Insert(tup *BTreeTuple) error {
	// go get the root data page from the buffer pool
	node, err := b.fetchNode(b.rootNode)
	if err != nil {
		return err
	}
	return b.insert(HEADER_SLOT_DATA_ROOT, node, tup, b.schema, b.schemaVersion)
}

// ====================================================================================================
// private methods

func (b *BTree) getIterator(node *BTreeNode, from Sortable, to Sortable, reverse bool, schema types.Schema) *BTreeNodeIterator {
	if reverse {
		// go to the right
		if node.isLeaf() {
			return NewBTreeNodeIterator(b, node, reverse, schema)
		} else {
			panic("implement me")
		}
	} else {
		// go to the left
		panic("implement me")
	}
}

// Latching for Search --> start at root with a read latch and go down; repeatedly,
// 		▶ acquire read latch on child
// 		▶ then unlatch parent

func (b *BTree) search(currentNode *BTreeNode, schema types.Schema, k Sortable) (Sortable, *BTreeTuple, error) {
	if currentNode.isLeaf() {
		defer currentNode.releaseReadLatch()
		defer b.unpin(currentNode)
		// search for the key
		i, found := currentNode.findKey(k)
		if found {
			return b.getTuple(currentNode, i, schema)
		}
		return nil, nil, nil
	} else {
		nodePtr := b.findNextPointer(currentNode, k)
		node, err := b.fetchNode(nodePtr)
		if err != nil {
			return nil, nil, err
		}
		node.takeReadLatch()
		currentNode.releaseReadLatch()
		b.unpin(currentNode)
		return b.search(node, schema, k)
	}
}

func (b *BTree) insert(headerPageRootSlot int, node *BTreeNode, tup *BTreeTuple, schema types.Schema, schemaVersion int) error {
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
			err = b.insertNonFull(n, key, tup, forceExclusive, schema, schemaVersion)
			if err != nil {
				return err
			}

			// this is the root node splitting so handle that...
			err = b.handleRootNodeSplit(headerPageRootSlot, pivot, lhsPtr, rhsPtr)
			if err != nil {
				return err
			}

			return nil
		} else {
			err := b.insertNonFull(node, key, tup, forceExclusive, schema, schemaVersion)
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

// getTuple reads a tuple off a page (and overflow pages). It will read the header of the tuple and decide (TODO) whether
// the tuple should be visible based on transaction ids or whether it is deleted
func (b *BTree) getTuple(node *BTreeNode, slotNumber int, schema types.Schema) (Sortable, *BTreeTuple, error) {
	// get the slot
	slot := node.page.ReadPageSlot(int16(slotNumber))

	// get the chunk off this leaf page
	kpl := slot.KeyPayload(node.page)
	lpl := slot.LeafPayload(node.page)
	rdr := lpl.GetPayloadReader(node.page)

	_ /*tupleHdr*/ = NewBTreeTupleHeaderFromBytes(rdr.PayloadChunkBytes)

	// TODO(pok) see if this tuple is visible to this TID, and is not deleted etc.

	// make a buffer to fit the payload
	payload := make([]byte, rdr.PayloadTotalLength)
	copy(payload, rdr.PayloadChunkBytes)
	bytesReceived := int32(rdr.PayloadChunkLength)
	if rdr.Flags == 1 {
		nextPtr := rdr.OverflowPtr
		for nextPtr != bufferpool.INVALID_PAGE {
			onode, err := b.fetchNode(bufferpool.PageID{ObjectID: b.objectID, Shard: b.shard, Page: nextPtr})
			if err != nil {
				return nil, nil, err
			}
			onode.takeReadLatch()
			// read the overflow bytes
			clen, cbytes := onode.page.ReadLeafPagePayloadBytes(bufferpool.PAGE_SLOTS_START_OFFSET)
			copy(payload[bytesReceived:], cbytes)
			bytesReceived += int32(clen)

			nextPtr = onode.page.ReadNextPointer().Page

			// release the latch and unpin
			onode.releaseReadLatch()
			b.unpin(onode)
		}
	}
	return Int(kpl.KeyAsInt(node.page)), NewBTreeTupleFromBytes(payload, schema), nil
}

// fetchNode gets the page specified by pageID from the buffer pool and wraps
// it in a BTreeNode. The page is pinned and read to use. It is the callers
// responsibiluty to unpin the page once they have finished with it.
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

// unpin is a convenience method to unpin the page wrapped by node
func (b *BTree) unpin(node *BTreeNode) error {
	return b.bufferpool.UnpinPage(node.page.ID())
}

// newOverflow creates a new overflow page and wraps it in a *BTreeNode
// The new page is pinned and read to use. It is the callers
// responsibiluty to unpin the page once they have finished with it.
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

// newOverflow creates a new leaf page and wraps it in a *BTreeNode
// The new page is pinned and read to use. It is the callers
// responsibiluty to unpin the page once they have finished with it.
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

// newOverflow creates a new internal page and wraps it in a *BTreeNode
// The new page is pinned and read to use. It is the callers
// responsibiluty to unpin the page once they have finished with it.
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

// isNodeFull returns true if a given node n is full and needs to
// be split. Being 'full' is a function of slot count and not
// free space on the page.
// The number of slots on the page is dictated by the page type and
// the values calculated during the b-tree creation based on the key
// size and the schema.
// It is possible for this function to return false and there to be
// no free space on the page - see compactLeafNode(), compactInternalNode()
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

// findNextPointer uses a binary search to locate the nearest key value and what
// pageID to jump to next during a search down the tree
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

// setRootNode writes the new root node page id into the appropriate slot
// in the header page of the b+tree, flushes the page and then updates the
// rootNode member on the BTree struct instance pointed to by b
func (b *BTree) setRootNode(headerPageRootSlot int, newRootNode bufferpool.PageID) error {
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

	// root page pointer is on the header page in headerPageRootSlot
	slot := headerNode.page.ReadPageSlot(int16(headerPageRootSlot))

	ipl := slot.InternalPayload(headerNode.page)

	// update the root page pointer
	ipl.PutPagePointer(headerNode.page, newRootNode)

	// TODO(pok) remove this once we have WAL
	b.bufferpool.FlushPage(headerNode.page.ID())

	// depending on which slot we have, update the cached page ptrs for root pages
	switch headerPageRootSlot {
	case HEADER_SLOT_DATA_ROOT:
		b.rootNode = newRootNode
	case HEADER_SLOT_SCHEMA_ROOT:
		b.schemaNode = newRootNode
	}
	return nil
}

// handleRootNodeSplit creates a new root node, inserts the seperator key pivot to point to the lhsPtr and
// the next pointer to point to the rhsPtr
func (b *BTree) handleRootNodeSplit(headerPageRootSlot int, pivot Sortable, lhsPtr bufferpool.PageID, rhsPtr bufferpool.PageID) error {
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

	b.setRootNode(headerPageRootSlot, newRoot.page.ID())
	return nil
}

// compactLeafPage compacts the data on a leaf page.
// When inserts happen, the free space pointer on the page is offset by the length of the data,
// the data is written to that offset and the free space pointer is decremented.
// We know how many keys we can fit on a page because we did the math when we created the BTree with the
// key length and the schema.
// When the page is split, we move half the keys on to the new page, but the data on the page that is split is
// is not rewritten for performance reasons. This means that even though there is plenty of room left for slots
// we may still have old data on the page taking up space. This function compacts the data on the page that is
// actually pointed to by slots on this page and rewrites the page, thereby freeing up space.
func (b *BTree) compactLeafPage(node *BTreeNode) error {
	// 1. make a scratch page
	// 2. iterate the slots on this page and copy data
	// 3. put the scratch page back over this one

	scratchPage := b.bufferpool.ScratchPage()
	scratchPage.WritePageType(bufferpool.PAGE_TYPE_BTREE_LEAF)
	scratchPage.WritePageNumber(node.page.ID().Page)
	freeSpaceOffset := scratchPage.ReadFreeSpaceOffset()

	slotCount := int(node.page.ReadSlotCount())

	for i := 0; i < slotCount; i++ {

		// read the slot from the page
		s := node.page.ReadPageSlot(int16(i))
		// read the chunk from the page
		lbl := s.LeafPayload(node.page)
		c := lbl.GetPayloadReader(node.page)

		// mod the freespace offset based on the size of the chunk
		freeSpaceOffset -= int16(c.Length())
		// update the values in the slot
		s.PayloadOffset = freeSpaceOffset

		// write the chunk
		offset := scratchPage.WriteLeafPagePayloadHeader(int16(freeSpaceOffset), c.KeyLength, c.KeyBytes, c.Flags, c.OverflowPtr, c.PayloadTotalLength)
		scratchPage.WriteLeafPagePayloadBytes(offset, c.PayloadChunkLength, c.PayloadChunkBytes)
		//write the slot
		scratchPage.WritePageSlot(int16(i), s)

		// update the free space offset
		scratchPage.WriteFreeSpaceOffset(int16(freeSpaceOffset))
	}
	// set the new slotcounts
	scratchPage.WriteSlotCount(int16(slotCount))

	// set sibling pointers
	scratchPage.WritePrevPointer(node.page.ReadPrevPointer())
	scratchPage.WriteNextPointer(node.page.ReadNextPointer())

	scratchPage.CopyPageTo(node.page)

	return nil
}

// compactInternalPage compacts the data on an internal page.
// see compactLeafPage() above
func (b *BTree) compactInternalPage(node *BTreeNode) error {
	// 1. make a scratch page
	// 2. iterate the slots on this page and copy data
	// 3. put the scratch page back over this one

	scratchPage := b.bufferpool.ScratchPage()
	scratchPage.WritePageType(bufferpool.PAGE_TYPE_BTREE_INTERNAL)
	scratchPage.WritePageNumber(node.page.ID().Page)
	freeSpaceOffset := scratchPage.ReadFreeSpaceOffset()

	slotCount := int(node.page.ReadSlotCount())

	for i := 0; i < slotCount; i++ {

		// read the slot from the page
		s := node.page.ReadPageSlot(int16(i))
		// read the chunk from the page
		ipl := s.InternalPayload(node.page)
		c := ipl.InternalPageChunk(node.page)

		// mod the freespace offset based on the size of the chunk
		freeSpaceOffset -= int16(c.Length())
		// update the values in the slot
		s.PayloadOffset = freeSpaceOffset

		// write the chunk
		scratchPage.WriteInternalPageChunk(int16(freeSpaceOffset), c)
		//write the slot
		scratchPage.WritePageSlot(int16(i), s)

		// update the free space offset
		scratchPage.WriteFreeSpaceOffset(int16(freeSpaceOffset))
	}
	// set the new slotcounts
	scratchPage.WriteSlotCount(int16(slotCount))

	// set sibling pointers
	scratchPage.WritePrevPointer(node.page.ReadPrevPointer())
	scratchPage.WriteNextPointer(node.page.ReadNextPointer())

	scratchPage.CopyPageTo(node.page)

	return nil
}

// writeLeafEntryInSlot handles writing a leaf entry into a slot on a leaf page.
// We already know what slotNumber we want to insert into.
// This function checks free space on the page and will compact if the write won't fit. It also handles overflowing data
// to overflow pages if the payload will not fit on this page.
func (b *BTree) writeLeafEntryInSlot(node *BTreeNode, slotNumber int16, keyBytes []byte, payloadBytes []byte) error {

	// calculate the size of the stuf we plan on writing
	keyLength := len(keyBytes)
	payloadChunkLength := len(payloadBytes)

	// set total length to the payload chunk length
	payloadTotalLength := payloadChunkLength

	// assume we aren't going to overflow
	flags := 0

	// if the payload chuck is more that we can fit on the page, adjust the size
	// for payload on this page and set the flag to indicate overflow
	if payloadChunkLength > bufferpool.MAX_PAYLOAD_CHUNK_SIZE_ON_PAGE {
		// we need to overflow to a new page, so set the fact we have to overflow
		flags = 1
		// cap the write size
		payloadChunkLength = bufferpool.MAX_PAYLOAD_CHUNK_SIZE_ON_PAGE
	}

	// if we are going to blow space, try to compact page
	// if we do blow space - we have larger math-y logic-y problems
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

	// move all the slots after where we are going to insert
	// get the current slot count
	for j := int(node.page.ReadSlotCount()); j > int(slotNumber); j-- {
		sl := node.page.ReadPageSlot(int16(j - 1))
		node.page.WritePageSlot(int16(j), sl)
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
			nextOverflowPtr := bufferpool.INVALID_PAGE
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
			overflowChunkLength := overflowFreeSpace - 2
			if overflowChunkLength > bytesRemaining {
				overflowChunkLength = bytesRemaining
			}
			hiWater += overflowChunkLength

			overflowPage.page.WriteNextPointer(bufferpool.PageID{ObjectID: b.objectID, Shard: b.shard, Page: nextOverflowPtr})

			overflowPage.page.WriteLeafPagePayloadBytes(bufferpool.PAGE_SLOTS_START_OFFSET, int16(payloadChunkLength), payloadBytes[lowWater:hiWater])
			bytesRemaining -= overflowChunkLength
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

// insertLeafEntryAt inserts a tuple at the slot denoted by keyPosition
func (b *BTree) insertLeafEntryAt(node *BTreeNode, keyPosition int, key Sortable, tup *BTreeTuple, schema types.Schema, schemaVersion int) error {
	if node.latchState() != bufferpool.Write {
		panic("unexpected latch state")
	}

	// read the slotcount first
	slotCount := int(node.page.ReadSlotCount())

	// put the payload together
	// TODO(pok) add TID and deal with existing rows (i.e. moving old versions to overflow pages)
	// TODO(pok) also should writers do the garbage collection for old rows that from commited transactions?
	valueData, err := tup.Bytes(0, schema, schemaVersion, bufferpool.INVALID_PAGE, false)
	if err != nil {
		return err
	}

	err = b.writeLeafEntryInSlot(node, int16(keyPosition), key.Bytes(), valueData)
	if err != nil {
		return err
	}

	// update the slot count
	slotCount++
	node.page.WriteSlotCount(int16(slotCount))
	return nil
}

// writeInternalEntryInSlot handles writing an internal entry into a slot on an internal page.
// We already know what slotNumber we want to insert into.
// This function checks free space on the page and will compact if the write won't fit.
func (b *BTree) writeInternalEntryInSlot(node *BTreeNode, slotNumber int16, keyBytes []byte, ptrValue int64) error {

	// get length info
	keyLength := len(keyBytes)
	onPageSize := node.page.ComputeInternalPayloadTotalLength(keyLength)

	// if we are going to blow space, try to compact page
	// if we do blow space - we have larger math-y logic-y problems
	if (onPageSize + bufferpool.PAGE_SLOT_LENGTH) > int32(node.page.FreeSpaceOnPage()) {
		err := b.compactInternalPage(node)
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

	// move all the slots after where we are going to insert
	for j := int(node.page.ReadSlotCount()); j > int(slotNumber); j-- {
		sl := node.page.ReadPageSlot(int16(j - 1))
		node.page.WritePageSlot(int16(j), sl)
	}

	// get the current freespace offset
	freeSpaceOffset := node.page.ReadFreeSpaceOffset()

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

// insertInternalEntryAt inserts a tuple at the slot denoted by keyPosition
func (b *BTree) insertInternalEntryAt(node *BTreeNode, keyPosition int, key Sortable, pageID bufferpool.PageID) error {
	if node.latchState() != bufferpool.Write {
		panic("unexpected latch state")
	}

	// get the slot count
	slotCount := int(node.page.ReadSlotCount())

	err := b.writeInternalEntryInSlot(node, int16(keyPosition), key.Bytes(), pageID.Page)
	if err != nil {
		return err
	}

	// update the slot count
	slotCount++
	node.page.WriteSlotCount(int16(slotCount))

	return nil
}

// updatePointerEntryAt updates a pointer entry that already exists on an internal node
func (b *BTree) updatePointerEntryAt(node *BTreeNode, slotNumber int, pagePtr bufferpool.PageID) error {
	slot := node.page.ReadPageSlot(int16(slotNumber))
	ipl := slot.InternalPayload(node.page)
	return ipl.PutPagePointer(node.page, pagePtr)
}

// splitNode calls the appropriate split function based on page type
func (b *BTree) splitNode(nodeToSplit *BTreeNode) (*BTreeNode, Sortable, *BTreeNode, error) {
	// TODO(pok) handle the sitch when inserting the new key and it ends up as the min key in rhs? We should probably test for this...
	if nodeToSplit.isLeaf() {
		return b.splitLeafNode(nodeToSplit)
	} else {
		return b.splitInternalNode(nodeToSplit)
	}
}

// insertNonFull handles insert operations.
// If the node is a leaf node, the data is simply inserted.
// If the node is an internal node, the key is used to find which child page pointer to follow. If the child node
// is full then pre-emptivly split it and then call insertNonFull on the appropriate child. If the child node is not
// full then call insertNonFull on the child node.
// Most of the complexity in this function is centered around latch-crabbing for concurrency.
//
// Latching for Insert --> start at root and go down, start at root with a read latch and go down; repeatedly,
// 		▶ latch parent node
// 		▶ get latch for childNode
// 			▶ if childNode is a leaf and will split and we only have a read latch, bail and start from the top in exclusive mode
// 		▶ release latch for parent if “safe”.
// 			• A safe node is one that will not split or merge when updated.
// 				▶ Not full (on insertion)

func (b *BTree) insertNonFull(node *BTreeNode, key Sortable, tup *BTreeTuple, forceExclusive bool, schema types.Schema, schemaVersion int) error {
	if node.isLeaf() {

		if node.latchState() != bufferpool.Write {
			panic("unexpected latch state")
		}

		defer node.releaseWriteLatch()
		defer b.unpin(node)

		i, exists := node.findKey(key)
		if exists {
			return errors.Errorf("key violation")
		}

		err := b.insertLeafEntryAt(node, i, key, tup, schema, schemaVersion)
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
				b.unpin(node)
				// release latch on childNode
				childNode.releaseAnyLatch()
				b.unpin(childNode)
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
				return b.insertNonFull(lhs, key, tup, forceExclusive, schema, schemaVersion)
			} else {
				lhs.releaseWriteLatch()
				b.unpin(lhs)
				return b.insertNonFull(rhs, key, tup, forceExclusive, schema, schemaVersion)
			}
		} else {
			// given the child node was not full, we can unlatch the parent here
			node.releaseAnyLatch()
			b.unpin(node)

			// now do the insert on the child node
			return b.insertNonFull(childNode, key, tup, forceExclusive, schema, schemaVersion)
		}
	}
}

// splitLeafNode handles splitting of a leaf node. It returns the original node, the seperator key on
// which the split happended and the new node, or an error.
func (b *BTree) splitLeafNode(nodeToSplit *BTreeNode) (*BTreeNode, Sortable, *BTreeNode, error) {

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

// splitInternalNode handles splitting of an internal node. It returns the original node, the seperator key on
// which the split happended and the new node, or an error.
func (b *BTree) splitInternalNode(nodeToSplit *BTreeNode) (*BTreeNode, Sortable, *BTreeNode, error) {
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
