package pilosa

// #cgo  CFLAGS:-mpopcnt

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa/internal"
	"github.com/yasushi-saito/rbtree"
)

const CounterMask = uint64(0xffffffffffffffff)

var CounterKey = int64(-1)

// Bitmap represents a bitmap broken up into Chunks.
// Internally it is represented as a red-black tree of chunks.
type Bitmap struct {
	tree   *rbtree.Tree
	bcount uint64
}

// NewBitmap returns a new instance of Bitmap.
func NewBitmap(bits ...uint64) *Bitmap {
	bm := &Bitmap{tree: rbtree.NewTree(rbtreeItemCompare)}
	for _, i := range bits {
		bm.setBit(i)
	}
	return bm
}

// Chunk returns the chunk within the bitmap.
// Returns nil if the chunk key does not exist.
func (b *Bitmap) Chunk(c *Chunk) *Chunk {
	if n := b.tree.Get(c); n != nil {
		return n.(*Chunk)
	}
	return nil
}

// AddChunk adds c to the bitmap.
func (b *Bitmap) AddChunk(c *Chunk) { b.tree.Insert(c) }

// Chunks returns a list of all chunks.
func (b *Bitmap) Chunks() []*Chunk {
	var a []*Chunk
	for itr := b.ChunkIterator(); !itr.Limit(); itr = itr.Next() {
		a = append(a, itr.Item().Clone())
	}
	return a
}

// ChunkIterator returns an iterator for looping over the bitmap's chunks.
func (b *Bitmap) ChunkIterator() *ChunkIterator {
	return &ChunkIterator{b.tree.Min()}
}

// Clone returns a copy of b.
func (b *Bitmap) Clone() *Bitmap {
	itr := b.ChunkIterator()
	other := NewBitmap()

	for {
		if itr.Limit() {
			break
		}

		other.AddChunk(itr.Item().Clone())
		itr = itr.Next()
	}
	return other
}

// Merge adds chunks from other to b.
// Chunks in b are overwritten if they exist in other.
func (b *Bitmap) Merge(other *Bitmap) {
	for itr := other.ChunkIterator(); !itr.Limit(); itr = itr.Next() {
		b.AddChunk(itr.Item().Clone())
	}
}

// IntersectionCount returns the number of intersections between b and other.
func (b *Bitmap) IntersectionCount(other *Bitmap) uint64 {
	itr0 := b.ChunkIterator()
	itr1 := other.ChunkIterator()

	results := uint64(0)
	for {
		if itr1.Limit() || itr0.Limit() {
			break
		} else if itr0.Item().Key < itr1.Item().Key {
			itr0 = itr0.Next()
		} else if itr0.Item().Key > itr1.Item().Key {
			itr1 = itr1.Next()
		} else if itr0.Item().Key == itr1.Item().Key {
			results += itr0.Item().Value.andcount(itr1.Item().Value)
			itr0 = itr0.Next()
			itr1 = itr1.Next()
		}
	}
	return results
}

// Intersect returns the itersection of b and other.
func (b *Bitmap) Intersect(other *Bitmap) *Bitmap {
	itr0 := b.ChunkIterator()
	itr1 := other.ChunkIterator()

	output := NewBitmap()
	for {
		if itr1.Limit() || itr0.Limit() {
			break
		} else if itr0.Item().Key < itr1.Item().Key {
			itr0 = itr0.Next()
		} else if itr0.Item().Key > itr1.Item().Key {
			itr1 = itr1.Next()
		} else if itr0.Item().Key == itr1.Item().Key {
			output.AddChunk(&Chunk{
				Key:   itr0.Item().Key,
				Value: itr0.Item().Value.intersect(itr1.Item().Value),
			})
			itr0 = itr0.Next()
			itr1 = itr1.Next()
		}
	}
	return output
}

// Invert returns a bitwise inversion of b.
func (b *Bitmap) Invert() *Bitmap {
	other := NewBitmap()
	for i := b.ChunkIterator(); !i.Limit(); i = i.Next() {
		other.AddChunk(&Chunk{
			Key:   i.Item().Key,
			Value: i.Item().Value.invert(),
		})
	}
	return other

}

// Union returns the bitwise union of b and other.
func (b *Bitmap) Union(other *Bitmap) *Bitmap {
	itr0 := b.ChunkIterator()
	itr1 := other.ChunkIterator()

	output := NewBitmap()
	eof := uint64(0xdeadbeef)

	for {
		if itr0.Limit() && itr1.Limit() {
			break
		} else if itr0.Limit() {
			if eof == itr1.Item().Key {
				break
			}
			output.AddChunk(&Chunk{itr1.Item().Key, itr1.Item().Value})
			eof = itr1.Item().Key
			itr1 = itr1.Next()
		} else if itr1.Limit() {
			if eof == itr0.Item().Key {
				break
			}
			output.AddChunk(&Chunk{itr0.Item().Key, itr0.Item().Value})
			eof = itr0.Item().Key
			itr0 = itr0.Next()
		} else if itr0.Item().Key < itr1.Item().Key {
			output.AddChunk(&Chunk{itr0.Item().Key, itr0.Item().Value})
			eof = itr0.Item().Key
			itr0 = itr0.Next()
		} else if itr0.Item().Key > itr1.Item().Key {
			output.AddChunk(&Chunk{itr1.Item().Key, itr1.Item().Value})
			eof = itr1.Item().Key
			itr1 = itr1.Next()
		} else if itr0.Item().Key == itr1.Item().Key {
			output.AddChunk(&Chunk{
				Key:   itr0.Item().Key,
				Value: itr0.Item().Value.union(itr1.Item().Value),
			})
			eof = itr0.Item().Key
			itr0 = itr0.Next()
			itr1 = itr1.Next()
		} else {
			panic("unreachable")
		}
	}
	return output
}

// Difference returns the diff of b and other.
func (b *Bitmap) Difference(other *Bitmap) *Bitmap {
	itr0 := b.ChunkIterator()
	itr1 := other.ChunkIterator()

	output := NewBitmap()
	for {
		if itr0.Limit() && itr1.Limit() {
			break
		} else if itr0.Limit() {
			break
		} else if itr1.Limit() {
			output.AddChunk(&Chunk{itr0.Item().Key, itr0.Item().Value})
			itr0 = itr0.Next()
		} else if itr0.Item().Key < itr1.Item().Key {
			output.AddChunk(&Chunk{itr0.Item().Key, itr0.Item().Value})
			itr0 = itr0.Next()
		} else if itr0.Item().Key > itr1.Item().Key {
			itr1 = itr1.Next()
		} else if itr0.Item().Key == itr1.Item().Key {
			chunk := &Chunk{
				Key:   itr0.Item().Key,
				Value: itr0.Item().Value.difference(itr1.Item().Value),
			}

			// Do not add if all bits are zeroed.
			if chunk.Value.bitcount() > 0 {
				output.AddChunk(chunk)
			}

			itr0 = itr0.Next()
			itr1 = itr1.Next()
		} else {
			panic("unreachable")
		}
	}
	return output
}

// ToRawCompressString returns a compressed, hex-encoded string of b.
func (b *Bitmap) ToRawCompressString() (string, int) {
	var bt bytes.Buffer
	buf := gzip.NewWriter(&bt)
	binary.Write(buf, binary.LittleEndian, uint64(b.tree.Len()))
	max_slice := 0
	for i := b.tree.Min(); !i.Limit(); i = i.Next() {
		obj := i.Item().(*Chunk)
		max_slice = int(obj.Key)
		binary.Write(buf, binary.LittleEndian, obj.Key)
		for _, v := range obj.Value {
			binary.Write(buf, binary.LittleEndian, v)
		}
	}
	buf.Flush()
	//buf.Close()
	max_slice = max_slice / 32
	return base64.StdEncoding.EncodeToString(bt.Bytes()), max_slice
}

// WriteTo writes the encoded bitmap to w.
func (b *Bitmap) WriteTo(w io.Writer) (n int64, err error) {
	// Wrap output in gzip compression.
	z := gzip.NewWriter(w)

	// Encode chunk count.
	enc := gob.NewEncoder(w)
	if err := enc.Encode(b.tree.Len()); err != nil {
		return 0, err
	}

	// Encode all chunks.
	for i := b.tree.Min(); !i.Limit(); i = i.Next() {
		if err := enc.Encode(i.Item().(*Chunk)); err != nil {
			return 0, err
		}
	}

	// Flush and close.
	if err := z.Close(); err != nil {
		return 0, err
	}

	return 0, nil
}

// ReadFrom reads encoded bitmap data from r into b.
func (b *Bitmap) ReadFrom(r io.Reader) (n int64, err error) {
	// Uncompress from gzip format.
	z, err := gzip.NewReader(r)
	if err != nil {
		return 0, err
	}
	dec := gob.NewDecoder(z)

	// Read size from data.
	var size int
	if err := dec.Decode(&size); err != nil {
		return 0, err
	}

	// Read chunks into bitmap.
	b.tree = rbtree.NewTree(rbtreeItemCompare)
	for i := 0; i < size; i++ {
		var chunk Chunk
		if err := dec.Decode(&chunk); err != nil {
			return 0, err
		}
		b.AddChunk(&chunk)
	}
	b.SetCount(b.BitCount())

	return 0, nil
}

// MarshalJSON returns a JSON-encoded byte slice of b.
func (b *Bitmap) MarshalJSON() ([]byte, error) {
	o := bitmapJSON{
		Chunks: make([]chunkJSON, 0, b.tree.Len()),
	}

	for itr := b.ChunkIterator(); !itr.Limit(); itr = itr.Next() {
		o.Chunks = append(o.Chunks, chunkJSON{Key: itr.Item().Key, Value: itr.Item().Value})
	}

	return json.Marshal(&o)
}

// MarshalBinary returns a gob-encoded byte slice of b.
func (b *Bitmap) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary decodes a gob-encoded byte slice into b.
func (b *Bitmap) UnmarshalBinary(data []byte) error {
	_, err := b.ReadFrom(bytes.NewReader(data))
	return err
}

// Bits returns the bits in b as a slice of ints.
func (b *Bitmap) Bits() []uint64 {
	result := make([]uint64, b.Count())

	x := 0
	for i := b.ChunkIterator(); !i.Limit(); i = i.Next() {
		item := i.Item()
		chunk := item.Key
		for bi, block := range item.Value {
			for bit := uint(0); bit < 64; bit++ {
				if (block & (1 << bit)) != 0 {
					idx := chunk << 11
					idx = idx | uint64((uint(bi)<<6)|bit)
					result[x] = idx
					x++
				}
			}
		}

	}
	return result
}

// setBit sets the i-th bit of the bitmap.
func (b *Bitmap) setBit(i uint64) (changed bool) {
	address := deref(i)

	chunk := b.Chunk(&Chunk{address.ChunkKey, make(Blocks, 32)})
	if chunk == nil {
		chunk = &Chunk{address.ChunkKey, make(Blocks, 32)}
		b.AddChunk(chunk)
	}

	changed = chunk.Value.setBit(address.BlockIndex, address.Bit)
	if changed {
		b.bcount++
	}

	return changed
}

// clearBit clears the i-th bit of the bitmap.
func (b *Bitmap) clearBit(i uint64) (changed bool) {
	address := deref(i)

	chunk := b.Chunk(&Chunk{address.ChunkKey, make(Blocks, 32)})
	if chunk == nil {
		return false
	}

	changed = chunk.Value.clearBit(address.BlockIndex, address.Bit)
	if changed && b.bcount > 0 {
		b.bcount--
	}

	return changed
}

// Len returns the number of chunks in b.
func (b *Bitmap) Len() int { return b.tree.Len() }

// SetCount sets the number of set bits in the bitmap.
func (b *Bitmap) SetCount(c uint64) { b.bcount = c }

// Count returns the number of set bits in the bitmap.
func (b *Bitmap) Count() uint64 { return b.bcount }

// BitCount calculates the number of set bits in the bitmap from raw chunk data.
func (b *Bitmap) BitCount() uint64 {
	var n uint64
	for i := b.ChunkIterator(); !i.Limit(); i = i.Next() {
		n += i.Item().Value.bitcount()
	}
	return n
}

// encodeBitmap converts b into its internal representation.
func encodeBitmap(b *Bitmap) *internal.Bitmap {
	pb := &internal.Bitmap{}
	for i := b.tree.Min(); !i.Limit(); i = i.Next() {
		pb.Chunks = append(pb.Chunks, encodeChunk(i.Item().(*Chunk)))
	}
	return pb
}

// decodeBitmap converts b from its internal representation.
func decodeBitmap(pb *internal.Bitmap) *Bitmap {
	b := NewBitmap()
	for _, chunk := range pb.GetChunks() {
		b.AddChunk(decodeChunk(chunk))
	}
	b.SetCount(b.BitCount())
	return b
}

// Union performs a union on a slice of bitmaps.
func Union(bitmaps []*Bitmap) *Bitmap {
	other := bitmaps[0]
	for _, bm := range bitmaps[1:] {
		other = other.Union(bm)
	}
	return other
}

// bitmapJSON is the JSON representation of Bitmap.
type bitmapJSON struct {
	Chunks []chunkJSON `json:"chunks"`
}

// Chunk represents a set of blocks in a Bitmap.
type Chunk struct {
	Key   uint64
	Value Blocks
}

// Clone returns a copy of c.
func (c *Chunk) Clone() *Chunk {
	return &Chunk{
		Key:   c.Key,
		Value: c.Value.copy(),
	}
}

// encodeChunks encodes c into its internal representation.
func encodeChunk(c *Chunk) *internal.Chunk {
	return &internal.Chunk{
		Key:   proto.Uint64(c.Key),
		Value: []uint64(c.Value),
	}
}

// decodeChunk decodes c from its internal representation.
func decodeChunk(pb *internal.Chunk) *Chunk {
	return &Chunk{
		Key:   pb.GetKey(),
		Value: Blocks(pb.GetValue()),
	}
}

// chunkJSON is the JSON representation of Chunk.
type chunkJSON struct {
	Key   uint64
	Value []uint64
}

// ChunkIterator represents an object for iterating over chunks in a bitmap.
type ChunkIterator struct {
	itr rbtree.Iterator
}

// Limit return true when the iterator is at the end of iteration.
func (r *ChunkIterator) Limit() bool {
	return r.itr.Limit()
}

// Next moves the iterator to the next chunk.
func (r *ChunkIterator) Next() *ChunkIterator {
	r.itr = r.itr.Next()
	return r
}

// Item returns the current item that the iterator is pointing at.
func (r *ChunkIterator) Item() *Chunk {
	if r.itr.Item() != nil {
		return r.itr.Item().(*Chunk)
	}
	return nil
}

func rbtreeItemCompare(a, b rbtree.Item) int {
	aKey, bKey := a.(*Chunk).Key, b.(*Chunk).Key
	if aKey < bKey {
		return -1
	} else if aKey > bKey {
		return 1
	}
	return 0
}

type Blocks []uint64

// NewBlocks returns a 32-length Block.
func NewBlocks() Blocks {
	return make(Blocks, 32)
}

func (a Blocks) bitcount() uint64 {
	return popcntSlice(a)
}

func (a Blocks) union(other Blocks) Blocks {
	ret := NewBlocks()
	for i, _ := range a {
		ret[i] = a[i] | other[i]
	}
	return ret
}

func (a Blocks) invert() Blocks {
	other := NewBlocks()
	for i, _ := range a {
		other[i] = ^a[i]
	}
	return other
}

func (a Blocks) copy() Blocks {
	other := NewBlocks()
	for i, _ := range a {
		other[i] = a[i]
	}
	return other
}

func (a Blocks) andcount(other Blocks) uint64 {
	return popcntAndSliceAsm(a, other)
}

func (a Blocks) intersect(other Blocks) Blocks {
	ret := NewBlocks()
	for i, _ := range a {
		ret[i] = a[i] & other[i]
	}
	return ret
}

func (a Blocks) difference(other Blocks) Blocks {
	ret := NewBlocks()
	for i, _ := range a {
		ret[i] = a[i] &^ other[i]
	}
	return ret
}

func (a Blocks) setBit(i uint8, bit uint8) (changed bool) {
	val := a[i] & (1 << bit)
	a[i] |= 1 << bit
	return val == 0
}

func (a Blocks) clearBit(i uint8, bit uint8) (changed bool) {
	val := a[i] & (1 << bit)
	a[i] &= ^(1 << bit)
	return val != 0
}

// Address represents a location for a given chunk/block/bit.
type Address struct {
	ChunkKey   uint64
	BlockIndex uint8
	Bit        uint8
}

func deref(pos uint64) Address {
	chunkKey := pos >> 11              // div by 2048
	offset := pos & 0x7FF              // mod by 2048
	blockIndex := uint8(offset >> 6)   // div by 64
	bit_offset := uint8(offset & 0x3F) // mod by 64
	return Address{chunkKey, blockIndex, bit_offset}
}
