/*
start with https://github.com/benbjohnson/immutable and specialize Map<uint32,int64>

Copyright 2019 Ben Johnson

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
package rbf

import (
	"math/bits"
)

// Size thresholds for each type of branch node.
const (
	maxArrayMapSize      = 8
	maxBitmapIndexedSize = 16
)

// Segment bit shifts within the map tree.
const (
	mapNodeBits = 5
	mapNodeSize = 1 << mapNodeBits
	mapNodeMask = mapNodeSize - 1
)

// PageMap represents an immutable hash map implementation. The map uses a Hasher
// to generate hashes and check for equality of key values.
//
// It is implemented as an Hash Array Mapped Trie.
type PageMap struct {
	size   int           // total number of key/value pairs
	root   mapNode       // root node of trie
	hasher *uint32Hasher // hasher implementation
}

// NewPageMap returns a new instance of PageMap. If hasher is nil, a default hasher
// implementation will automatically be chosen based on the first key added.
// Default hasher implementations only exist for int, string, and byte slice types.
func NewPageMap() *PageMap {
	return &PageMap{
		hasher: &uint32Hasher{},
	}
}

// Len returns the number of elements in the map.
func (m *PageMap) Len() int {
	return m.size
}

// clone returns a shallow copy of m.
func (m *PageMap) clone() *PageMap {
	other := *m
	return &other
}

// Get returns the value for a given key and a flag indicating whether the
// key exists. This flag distinguishes a zero value set on a key versus a
// non-existent key in the map.
func (m *PageMap) Get(key uint32) (value int64, ok bool) {
	if m.root == nil {
		return 0, false
	}
	keyHash := m.hasher.Hash(key)
	return m.root.get(key, 0, keyHash, m.hasher)
}

// Set returns a map with the key set to the new value. A nil value is allowed.
//
// This function will return a new map even if the updated value is the same as
// the existing value because PageMap does not track value equality.
func (m *PageMap) Set(key uint32, value int64) *PageMap {
	return m.set(key, value, false)
}

func (m *PageMap) set(key uint32, value int64, mutable bool) *PageMap {
	// Set a hasher on the first value if one does not already exist.
	hasher := m.hasher
	if hasher == nil {
		hasher = &uint32Hasher{}
	}

	// Generate copy if necessary.
	other := m
	if !mutable {
		other = m.clone()
	}
	other.hasher = hasher

	// If the map is empty, initialize with a simple array node.
	if m.root == nil {
		other.size = 1
		other.root = &mapArrayNode{entries: []mapEntry{{key: key, value: value}}}
		return other
	}

	// Otherwise copy the map and delegate insertion to the root.
	// Resized will return true if the key does not currently exist.
	var resized bool
	other.root = m.root.set(key, value, 0, hasher.Hash(key), hasher, mutable, &resized)
	if resized {
		other.size++
	}
	return other
}

// Delete returns a map with the given key removed.
// Removing a non-existent key will cause this method to return the same map.
func (m *PageMap) Delete(key uint32) *PageMap {
	return m.delete(key, false)
}

func (m *PageMap) delete(key uint32, mutable bool) *PageMap {
	// Return original map if no keys exist.
	if m.root == nil {
		return m
	}

	// If the delete did not change the node then return the original map.
	var resized bool
	newRoot := m.root.delete(key, 0, m.hasher.Hash(key), m.hasher, mutable, &resized)
	if !resized {
		return m
	}

	// Generate copy if necessary.
	other := m
	if !mutable {
		other = m.clone()
	}

	// Return copy of map with new root and decreased size.
	other.size = m.size - 1
	other.root = newRoot
	return other
}

// Iterator returns a new iterator for the map.
func (m *PageMap) Iterator() *PageMapIterator {
	itr := &PageMapIterator{m: m}
	itr.First()
	return itr
}

// PageMapBuilder represents an efficient builder for creating PageMaps.
type PageMapBuilder struct {
	m *PageMap // current state
}

// NewPageMapBuilder returns a new instance of PageMapBuilder.
func NewPageMapBuilder() *PageMapBuilder {
	return &PageMapBuilder{m: NewPageMap()}
}

// Map returns the underlying map. Only call once.
// Builder is invalid after call. Will panic on second invocation.
func (b *PageMapBuilder) Map() *PageMap {
	assert(b.m != nil) // "immutable.SortedPageMapBuilder.Map(): duplicate call to fetch map")
	m := b.m
	b.m = nil
	return m
}

// Len returns the number of elements in the underlying map.
func (b *PageMapBuilder) Len() int {
	assert(b.m != nil) // "immutable.PageMapBuilder: builder invalid after Map() invocation")
	return b.m.Len()
}

// Get returns the value for the given key.
func (b *PageMapBuilder) Get(key uint32) (value int64, ok bool) {
	assert(b.m != nil) //  "immutable.PageMapBuilder: builder invalid after Map() invocation")
	return b.m.Get(key)
}

// Set sets the value of the given key. See PageMap.Set() for additional details.
func (b *PageMapBuilder) Set(key uint32, value int64) {
	assert(b.m != nil) // "immutable.PageMapBuilder: builder invalid after Map() invocation")
	b.m = b.m.set(key, value, true)
}

// Delete removes the given key. See PageMap.Delete() for additional details.
func (b *PageMapBuilder) Delete(key uint32) {
	assert(b.m != nil) //  "immutable.PageMapBuilder: builder invalid after Map() invocation")
	b.m = b.m.delete(key, true)
}

// Iterator returns a new iterator for the underlying map.
func (b *PageMapBuilder) Iterator() *PageMapIterator {
	assert(b.m != nil) // "immutable.PageMapBuilder: builder invalid after Map() invocation")
	return b.m.Iterator()
}

// mapNode represents any node in the map tree.
type mapNode interface {
	get(key uint32, shift uint, keyHash uint32, h *uint32Hasher) (value int64, ok bool)
	set(key uint32, value int64, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode
	delete(key uint32, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode
}

var _ mapNode = (*mapArrayNode)(nil)
var _ mapNode = (*mapBitmapIndexedNode)(nil)
var _ mapNode = (*mapHashArrayNode)(nil)
var _ mapNode = (*mapValueNode)(nil)
var _ mapNode = (*mapHashCollisionNode)(nil)

// mapLeafNode represents a node that stores a single key hash at the leaf of the map tree.
type mapLeafNode interface {
	mapNode
	keyHashValue() uint32
}

var _ mapLeafNode = (*mapValueNode)(nil)
var _ mapLeafNode = (*mapHashCollisionNode)(nil)

// mapArrayNode is a map node that stores key/value pairs in a slice.
// Entries are stored in insertion order. An array node expands into a bitmap
// indexed node once a given threshold size is crossed.
type mapArrayNode struct {
	entries []mapEntry
}

// indexOf returns the entry index of the given key. Returns -1 if key not found.
func (n *mapArrayNode) indexOf(key uint32, h *uint32Hasher) int {
	for i := range n.entries {
		if n.entries[i].key == key {
			return i
		}
	}
	return -1
}

// get returns the value for the given key.
func (n *mapArrayNode) get(key uint32, shift uint, keyHash uint32, h *uint32Hasher) (value int64, ok bool) {
	i := n.indexOf(key, h)
	if i == -1 {
		return 0, false
	}
	return n.entries[i].value, true
}

// set inserts or updates the value for a given key. If the key is inserted and
// the new size crosses the max size threshold, a bitmap indexed node is returned.
func (n *mapArrayNode) set(key uint32, value int64, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode {
	idx := n.indexOf(key, h)

	// Mark as resized if the key doesn't exist.
	if idx == -1 {
		*resized = true
	}

	// If we are adding and it crosses the max size threshold, expand the node.
	// We do this by continually setting the entries to a value node and expanding.
	if idx == -1 && len(n.entries) >= maxArrayMapSize {
		var node mapNode = newMapValueNode(h.Hash(key), key, value)
		for _, entry := range n.entries {
			node = node.set(entry.key, entry.value, 0, h.Hash(entry.key), h, false, resized)
		}
		return node
	}

	// Update in-place if mutable.
	if mutable {
		if idx != -1 {
			n.entries[idx] = mapEntry{key, value}
		} else {
			n.entries = append(n.entries, mapEntry{key, value})
		}
		return n
	}

	// Update existing entry if a match is found.
	// Otherwise append to the end of the element list if it doesn't exist.
	var other mapArrayNode
	if idx != -1 {
		other.entries = make([]mapEntry, len(n.entries))
		copy(other.entries, n.entries)
		other.entries[idx] = mapEntry{key, value}
	} else {
		other.entries = make([]mapEntry, len(n.entries)+1)
		copy(other.entries, n.entries)
		other.entries[len(other.entries)-1] = mapEntry{key, value}
	}
	return &other
}

// delete removes the given key from the node. Returns the same node if key does
// not exist. Returns a nil node when removing the last entry.
func (n *mapArrayNode) delete(key uint32, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode {
	idx := n.indexOf(key, h)

	// Return original node if key does not exist.
	if idx == -1 {
		return n
	}
	*resized = true

	// Return nil if this node will contain no nodes.
	if len(n.entries) == 1 {
		return nil
	}

	// Update in-place, if mutable.
	if mutable {
		copy(n.entries[idx:], n.entries[idx+1:])
		n.entries[len(n.entries)-1] = mapEntry{}
		n.entries = n.entries[:len(n.entries)-1]
		return n
	}

	// Otherwise create a copy with the given entry removed.
	other := &mapArrayNode{entries: make([]mapEntry, len(n.entries)-1)}
	copy(other.entries[:idx], n.entries[:idx])
	copy(other.entries[idx:], n.entries[idx+1:])
	return other
}

// mapBitmapIndexedNode represents a map branch node with a variable number of
// node slots and indexed using a bitmap. Indexes for the node slots are
// calculated by counting the number of set bits before the target bit using popcount.
type mapBitmapIndexedNode struct {
	bitmap uint32
	nodes  []mapNode
}

// get returns the value for the given key.
func (n *mapBitmapIndexedNode) get(key uint32, shift uint, keyHash uint32, h *uint32Hasher) (value int64, ok bool) {
	bit := uint32(1) << ((keyHash >> shift) & mapNodeMask)
	if (n.bitmap & bit) == 0 {
		return 0, false
	}
	child := n.nodes[bits.OnesCount32(n.bitmap&(bit-1))]
	return child.get(key, shift+mapNodeBits, keyHash, h)
}

// set inserts or updates the value for the given key. If a new key is inserted
// and the size crosses the max size threshold then a hash array node is returned.
func (n *mapBitmapIndexedNode) set(key uint32, value int64, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode {
	// Extract the index for the bit segment of the key hash.
	keyHashFrag := (keyHash >> shift) & mapNodeMask

	// Determine the bit based on the hash index.
	bit := uint32(1) << keyHashFrag
	exists := (n.bitmap & bit) != 0

	// Mark as resized if the key doesn't exist.
	if !exists {
		*resized = true
	}

	// Find index of node based on popcount of bits before it.
	idx := bits.OnesCount32(n.bitmap & (bit - 1))

	// If the node already exists, delegate set operation to it.
	// If the node doesn't exist then create a simple value leaf node.
	var newNode mapNode
	if exists {
		newNode = n.nodes[idx].set(key, value, shift+mapNodeBits, keyHash, h, mutable, resized)
	} else {
		newNode = newMapValueNode(keyHash, key, value)
	}

	// Convert to a hash-array node once we exceed the max bitmap size.
	// Copy each node based on their bit position within the bitmap.
	if !exists && len(n.nodes) > maxBitmapIndexedSize {
		var other mapHashArrayNode
		for i := uint(0); i < uint(len(other.nodes)); i++ {
			if n.bitmap&(uint32(1)<<i) != 0 {
				other.nodes[i] = n.nodes[other.count]
				other.count++
			}
		}
		other.nodes[keyHashFrag] = newNode
		other.count++
		return &other
	}

	// Update in-place if mutable.
	if mutable {
		if exists {
			n.nodes[idx] = newNode
		} else {
			n.bitmap |= bit
			n.nodes = append(n.nodes, nil)
			copy(n.nodes[idx+1:], n.nodes[idx:])
			n.nodes[idx] = newNode
		}
		return n
	}

	// If node exists at given slot then overwrite it with new node.
	// Otherwise expand the node list and insert new node into appropriate position.
	other := &mapBitmapIndexedNode{bitmap: n.bitmap | bit}
	if exists {
		other.nodes = make([]mapNode, len(n.nodes))
		copy(other.nodes, n.nodes)
		other.nodes[idx] = newNode
	} else {
		other.nodes = make([]mapNode, len(n.nodes)+1)
		copy(other.nodes, n.nodes[:idx])
		other.nodes[idx] = newNode
		copy(other.nodes[idx+1:], n.nodes[idx:])
	}
	return other
}

// delete removes the key from the tree. If the key does not exist then the
// original node is returned. If removing the last child node then a nil is
// returned. Note that shrinking the node will not convert it to an array node.
func (n *mapBitmapIndexedNode) delete(key uint32, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode {
	bit := uint32(1) << ((keyHash >> shift) & mapNodeMask)

	// Return original node if key does not exist.
	if (n.bitmap & bit) == 0 {
		return n
	}

	// Find index of node based on popcount of bits before it.
	idx := bits.OnesCount32(n.bitmap & (bit - 1))

	// Delegate delete to child node.
	child := n.nodes[idx]
	newChild := child.delete(key, shift+mapNodeBits, keyHash, h, mutable, resized)

	// Return original node if key doesn't exist in child.
	if !*resized {
		return n
	}

	// Remove if returned child has been deleted.
	if newChild == nil {
		// If we won't have any children then return nil.
		if len(n.nodes) == 1 {
			return nil
		}

		// Update in-place if mutable.
		if mutable {
			n.bitmap ^= bit
			copy(n.nodes[idx:], n.nodes[idx+1:])
			n.nodes[len(n.nodes)-1] = nil
			n.nodes = n.nodes[:len(n.nodes)-1]
			return n
		}

		// Return copy with bit removed from bitmap and node removed from node list.
		other := &mapBitmapIndexedNode{bitmap: n.bitmap ^ bit, nodes: make([]mapNode, len(n.nodes)-1)}
		copy(other.nodes[:idx], n.nodes[:idx])
		copy(other.nodes[idx:], n.nodes[idx+1:])
		return other
	}

	// Generate copy, if necessary.
	other := n
	if !mutable {
		other = &mapBitmapIndexedNode{bitmap: n.bitmap, nodes: make([]mapNode, len(n.nodes))}
		copy(other.nodes, n.nodes)
	}

	// Update child.
	other.nodes[idx] = newChild
	return other
}

// mapHashArrayNode is a map branch node that stores nodes in a fixed length
// array. Child nodes are indexed by their index bit segment for the current depth.
type mapHashArrayNode struct {
	count uint                 // number of set nodes
	nodes [mapNodeSize]mapNode // child node slots, may contain empties
}

// clone returns a shallow copy of n.
func (n *mapHashArrayNode) clone() *mapHashArrayNode {
	other := *n
	return &other
}

// get returns the value for the given key.
func (n *mapHashArrayNode) get(key uint32, shift uint, keyHash uint32, h *uint32Hasher) (value int64, ok bool) {
	node := n.nodes[(keyHash>>shift)&mapNodeMask]
	if node == nil {
		return 0, false
	}
	return node.get(key, shift+mapNodeBits, keyHash, h)
}

// set returns a node with the value set for the given key.
func (n *mapHashArrayNode) set(key uint32, value int64, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode {
	idx := (keyHash >> shift) & mapNodeMask
	node := n.nodes[idx]

	// If node at index doesn't exist, create a simple value leaf node.
	// Otherwise delegate set to child node.
	var newNode mapNode
	if node == nil {
		*resized = true
		newNode = newMapValueNode(keyHash, key, value)
	} else {
		newNode = node.set(key, value, shift+mapNodeBits, keyHash, h, mutable, resized)
	}

	// Generate copy, if necessary.
	other := n
	if !mutable {
		other = n.clone()
	}

	// Update child node (and update size, if new).
	if node == nil {
		other.count++
	}
	other.nodes[idx] = newNode
	return other
}

// delete returns a node with the given key removed. Returns the same node if
// the key does not exist. If node shrinks to within bitmap-indexed size then
// converts to a bitmap-indexed node.
func (n *mapHashArrayNode) delete(key uint32, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode {
	idx := (keyHash >> shift) & mapNodeMask
	node := n.nodes[idx]

	// Return original node if child is not found.
	if node == nil {
		return n
	}

	// Return original node if child is unchanged.
	newNode := node.delete(key, shift+mapNodeBits, keyHash, h, mutable, resized)
	if !*resized {
		return n
	}

	// If we remove a node and drop below a threshold, convert back to bitmap indexed node.
	if newNode == nil && n.count <= maxBitmapIndexedSize {
		other := &mapBitmapIndexedNode{nodes: make([]mapNode, 0, n.count-1)}
		for i, child := range n.nodes {
			if child != nil && uint32(i) != idx {
				other.bitmap |= 1 << uint(i)
				other.nodes = append(other.nodes, child)
			}
		}
		return other
	}

	// Generate copy, if necessary.
	other := n
	if !mutable {
		other = n.clone()
	}

	// Return copy of node with child updated.
	other.nodes[idx] = newNode
	if newNode == nil {
		other.count--
	}
	return other
}

// mapValueNode represents a leaf node with a single key/value pair.
// A value node can be converted to a hash collision leaf node if a different
// key with the same keyHash is inserted.
type mapValueNode struct {
	keyHash uint32
	key     uint32
	value   int64
}

// newMapValueNode returns a new instance of mapValueNode.
func newMapValueNode(keyHash uint32, key uint32, value int64) *mapValueNode {
	return &mapValueNode{
		keyHash: keyHash,
		key:     key,
		value:   value,
	}
}

// keyHashValue returns the key hash for this node.
func (n *mapValueNode) keyHashValue() uint32 {
	return n.keyHash
}

// get returns the value for the given key.
func (n *mapValueNode) get(key uint32, shift uint, keyHash uint32, h *uint32Hasher) (value int64, ok bool) {
	if n.key != key {
		return 0, false
	}
	return n.value, true
}

// set returns a new node with the new value set for the key. If the key equals
// the node's key then a new value node is returned. If key is not equal to the
// node's key but has the same hash then a hash collision node is returned.
// Otherwise the nodes are merged into a branch node.
func (n *mapValueNode) set(key uint32, value int64, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode {
	// If the keys match then return a new value node overwriting the value.
	if n.key == key {
		// Update in-place if mutable.
		if mutable {
			n.value = value
			return n
		}
		// Otherwise return a new copy.
		return newMapValueNode(n.keyHash, key, value)
	}

	*resized = true

	// Recursively merge nodes together if key hashes are different.
	if n.keyHash != keyHash {
		return mergeIntoNode(n, shift, keyHash, key, value)
	}

	// Merge into collision node if hash matches.
	return &mapHashCollisionNode{keyHash: keyHash, entries: []mapEntry{
		{key: n.key, value: n.value},
		{key: key, value: value},
	}}
}

// delete returns nil if the key matches the node's key. Otherwise returns the original node.
func (n *mapValueNode) delete(key uint32, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode {
	// Return original node if the keys do not match.
	if n.key != key {
		return n
	}

	// Otherwise remove the node if keys do match.
	*resized = true
	return nil
}

// mapHashCollisionNode represents a leaf node that contains two or more key/value
// pairs with the same key hash. Single pairs for a hash are stored as value nodes.
type mapHashCollisionNode struct {
	keyHash uint32 // key hash for all entries
	entries []mapEntry
}

// keyHashValue returns the key hash for all entries on the node.
func (n *mapHashCollisionNode) keyHashValue() uint32 {
	return n.keyHash
}

// indexOf returns the index of the entry for the given key.
// Returns -1 if the key does not exist in the node.
func (n *mapHashCollisionNode) indexOf(key uint32, h *uint32Hasher) int {
	for i := range n.entries {
		if n.entries[i].key == key {
			return i
		}
	}
	return -1
}

// get returns the value for the given key.
func (n *mapHashCollisionNode) get(key uint32, shift uint, keyHash uint32, h *uint32Hasher) (value int64, ok bool) {
	for i := range n.entries {
		if n.entries[i].key == key {
			return n.entries[i].value, true
		}
	}
	return 0, false
}

// set returns a copy of the node with key set to the given value.
func (n *mapHashCollisionNode) set(key uint32, value int64, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode {
	// Merge node with key/value pair if this is not a hash collision.
	if n.keyHash != keyHash {
		*resized = true
		return mergeIntoNode(n, shift, keyHash, key, value)
	}

	// Update in-place if mutable.
	if mutable {
		if idx := n.indexOf(key, h); idx == -1 {
			*resized = true
			n.entries = append(n.entries, mapEntry{key, value})
		} else {
			n.entries[idx] = mapEntry{key, value}
		}
		return n
	}

	// Append to end of node if key doesn't exist & mark resized.
	// Otherwise copy nodes and overwrite at matching key index.
	other := &mapHashCollisionNode{keyHash: n.keyHash}
	if idx := n.indexOf(key, h); idx == -1 {
		*resized = true
		other.entries = make([]mapEntry, len(n.entries)+1)
		copy(other.entries, n.entries)
		other.entries[len(other.entries)-1] = mapEntry{key, value}
	} else {
		other.entries = make([]mapEntry, len(n.entries))
		copy(other.entries, n.entries)
		other.entries[idx] = mapEntry{key, value}
	}
	return other
}

// delete returns a node with the given key deleted. Returns the same node if
// the key does not exist. If removing the key would shrink the node to a single
// entry then a value node is returned.
func (n *mapHashCollisionNode) delete(key uint32, shift uint, keyHash uint32, h *uint32Hasher, mutable bool, resized *bool) mapNode {
	idx := n.indexOf(key, h)

	// Return original node if key is not found.
	if idx == -1 {
		return n
	}

	// Mark as resized if key exists.
	*resized = true

	// Convert to value node if we move to one entry.
	if len(n.entries) == 2 {
		return &mapValueNode{
			keyHash: n.keyHash,
			key:     n.entries[idx^1].key,
			value:   n.entries[idx^1].value,
		}
	}

	// Remove entry in-place if mutable.
	if mutable {
		copy(n.entries[idx:], n.entries[idx+1:])
		n.entries[len(n.entries)-1] = mapEntry{}
		n.entries = n.entries[:len(n.entries)-1]
		return n
	}

	// Return copy without entry if immutable.
	other := &mapHashCollisionNode{keyHash: n.keyHash, entries: make([]mapEntry, len(n.entries)-1)}
	copy(other.entries[:idx], n.entries[:idx])
	copy(other.entries[idx:], n.entries[idx+1:])
	return other
}

// mergeIntoNode merges a key/value pair into an existing node.
// Caller must verify that node's keyHash is not equal to keyHash.
func mergeIntoNode(node mapLeafNode, shift uint, keyHash uint32, key uint32, value int64) mapNode {
	idx1 := (node.keyHashValue() >> shift) & mapNodeMask
	idx2 := (keyHash >> shift) & mapNodeMask

	// Recursively build branch nodes to combine the node and its key.
	other := &mapBitmapIndexedNode{bitmap: (1 << idx1) | (1 << idx2)}
	if idx1 == idx2 {
		other.nodes = []mapNode{mergeIntoNode(node, shift+mapNodeBits, keyHash, key, value)}
	} else {
		if newNode := newMapValueNode(keyHash, key, value); idx1 < idx2 {
			other.nodes = []mapNode{node, newNode}
		} else {
			other.nodes = []mapNode{newNode, node}
		}
	}
	return other
}

// mapEntry represents a single key/value pair.
type mapEntry struct {
	key   uint32
	value int64
}

// PageMapIterator represents an iterator over a map's key/value pairs. Although
// map keys are not sorted, the iterator's order is deterministic.
type PageMapIterator struct {
	m *PageMap // source map

	stack [32]mapIteratorElem // search stack
	depth int                 // stack depth
}

// Done returns true if no more elements remain in the iterator.
func (itr *PageMapIterator) Done() bool {
	return itr.depth == -1
}

// First resets the iterator to the first key/value pair.
func (itr *PageMapIterator) First() {
	// Exit immediately if the map is empty.
	if itr.m.root == nil {
		itr.depth = -1
		return
	}

	// Initialize the stack to the left most element.
	itr.stack[0] = mapIteratorElem{node: itr.m.root}
	itr.depth = 0
	itr.first()
}

// Next returns the next key/value pair. Returns a nil key when no elements remain.
func (itr *PageMapIterator) Next() (key uint32, value int64, ok bool) {
	// Return nil key if iteration is done.
	if itr.Done() {
		ok = false
		return
	}

	// Retrieve current index & value. Current node is always a leaf.
	elem := &itr.stack[itr.depth]
	switch node := elem.node.(type) {
	case *mapArrayNode:
		entry := &node.entries[elem.index]
		key, value = entry.key, entry.value
	case *mapValueNode:
		key, value = node.key, node.value
	case *mapHashCollisionNode:
		entry := &node.entries[elem.index]
		key, value = entry.key, entry.value
	}

	// Move up stack until we find a node that has remaining position ahead
	// and move that element forward by one.
	itr.next()
	return key, value, true
}

// next moves to the next available key.
func (itr *PageMapIterator) next() {
	for ; itr.depth >= 0; itr.depth-- {
		elem := &itr.stack[itr.depth]

		switch node := elem.node.(type) {
		case *mapArrayNode:
			if elem.index < len(node.entries)-1 {
				elem.index++
				return
			}

		case *mapBitmapIndexedNode:
			if elem.index < len(node.nodes)-1 {
				elem.index++
				itr.stack[itr.depth+1].node = node.nodes[elem.index]
				itr.depth++
				itr.first()
				return
			}

		case *mapHashArrayNode:
			for i := elem.index + 1; i < len(node.nodes); i++ {
				if node.nodes[i] != nil {
					elem.index = i
					itr.stack[itr.depth+1].node = node.nodes[elem.index]
					itr.depth++
					itr.first()
					return
				}
			}

		case *mapValueNode:
			continue // always the last value, traverse up

		case *mapHashCollisionNode:
			if elem.index < len(node.entries)-1 {
				elem.index++
				return
			}
		}
	}
}

// first positions the stack left most index.
// Elements and indexes at and below the current depth are assumed to be correct.
func (itr *PageMapIterator) first() {
	for ; ; itr.depth++ {
		elem := &itr.stack[itr.depth]

		switch node := elem.node.(type) {
		case *mapBitmapIndexedNode:
			elem.index = 0
			itr.stack[itr.depth+1].node = node.nodes[0]

		case *mapHashArrayNode:
			for i := 0; i < len(node.nodes); i++ {
				if node.nodes[i] != nil { // find first node
					elem.index = i
					itr.stack[itr.depth+1].node = node.nodes[i]
					break
				}
			}

		default: // *mapArrayNode, mapLeafNode
			elem.index = 0
			return
		}
	}
}

// mapIteratorElem represents a node/index pair in the PageMapIterator stack.
type mapIteratorElem struct {
	node  mapNode
	index int
}
