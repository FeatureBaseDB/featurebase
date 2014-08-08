package index

// #cgo  CFLAGS:-mpopcnt

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/gob"
	"io/ioutil"
	"log"

	"github.com/yasushi-saito/rbtree"
)

const (
	MAX_HOT_SIZE = 50000
	BLOCK_SIZE   = 5000
	START_IDX    = MAX_HOT_SIZE - BLOCK_SIZE
	COUNTERMASK  = uint64(0xffffffffffffffff)
)

var (
	COUNTER_KEY int64
)

func init() {
	var dumb = COUNTERMASK
	COUNTER_KEY = int64(dumb)
}

//
type IntSet struct {
	set map[uint64]bool
}

func NewIntSet() *IntSet {
	x := new(IntSet)
	x.set = make(map[uint64]bool)
	return x
}

func (self *IntSet) Add(i uint64) bool {
	_, found := self.set[i]
	self.set[i] = true
	return !found //False if it existed already
}

func (self *IntSet) Contains(i uint64) bool {
	_, found := self.set[i]
	return found //true if it existed already
}

func (self *IntSet) Remove(i uint64) {
	delete(self.set, i)
}

func (self *IntSet) Size() int {
	return len(self.set)
}

//

/* ** native version turned out to be slower
func popcount(i uint64)uint64{
	val:= C.__builtin_popcountll(C.ulonglong(i))
	//x:= uint64(val)
	return uint64(val)
}
func popcount(x uint64) (n uint64) {
	// bit population count, see
	// http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel
	x -= (x >> 1) & 0x5555555555555555
	x = (x>>2)&0x3333333333333333 + x&0x3333333333333333
	x += x >> 4
	x &= 0x0f0f0f0f0f0f0f0f
	x *= 0x0101010101010101
	return uint64(x >> 56)
}
*/

type BlockArray struct {
	Block [32]uint64
}

func (s *BlockArray) bitcount() uint64 {
	var sum uint64
	for _, b := range (*s).Block {
		sum += popcount(b)
	}
	return sum
	//	return popcntSlice(s.Block)
}
func BlockArray_union(a *BlockArray, b *BlockArray) BlockArray {
	var o = BlockArray{}
	for i, _ := range a.Block {
		o.Block[i] = a.Block[i] | b.Block[i]
	}
	return o
}

func BlockArray_invert(a *BlockArray) BlockArray {
	var o = BlockArray{}
	for i, _ := range a.Block {
		o.Block[i] = ^a.Block[i]
	}
	return o
}

func BlockArray_copy(a *BlockArray) BlockArray {
	var o = BlockArray{}
	for i, _ := range a.Block {
		o.Block[i] = a.Block[i]
	}
	return o
}
func BlockArray_intersection(a *BlockArray, b *BlockArray) BlockArray {
	var o = BlockArray{}
	for i, _ := range a.Block {
		o.Block[i] = a.Block[i] & b.Block[i]
	}
	return o
}

func BlockArray_difference(a *BlockArray, b *BlockArray) BlockArray {
	var o = BlockArray{}
	for i, _ := range a.Block {
		o.Block[i] = a.Block[i] &^ b.Block[i]
	}
	return o
}

func (s *BlockArray) set_bit(BlockIndex uint8, bit uint8) bool {
	val := s.Block[BlockIndex] & (1 << bit)
	s.Block[BlockIndex] |= 1 << bit
	return val == 0
}

type Chunk struct {
	Key   uint64
	Value BlockArray
}
type Bitmap struct {
	nodes  *rbtree.Tree
	bcount uint64
}

func Compare(a uint64, b uint64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}
func Clone(a_bm IBitmap) IBitmap {

	var a = a_bm.Min()
	output := CreateRBBitmap()
	for {
		if a.Limit() {
			break
		}
		var a_node = a.Item()
		var o = BlockArray_copy(&a_node.Value)
		var o_node = &Chunk{a_node.Key, o}
		output.AddChunk(o_node)
		a = a.Next()
	}
	return output
}

func Intersection(a_bm IBitmap, b_bm IBitmap) IBitmap {
	var a = a_bm.Min()
	var b = b_bm.Min()
	defer a.Close()
	defer b.Close()
	output := CreateRBBitmap()

	for {
		if b.Limit() || a.Limit() {
			break
		} else if a.Item().Key < b.Item().Key {
			a = a.Next()
		} else if a.Item().Key > b.Item().Key {
			b = b.Next()
		} else if a.Item().Key == b.Item().Key {
			var a_node = a.Item()
			var b_node = b.Item().Value
			var o = BlockArray_intersection(&a_node.Value, &b_node)
			var o_node = &Chunk{a_node.Key, o}
			output.AddChunk(o_node)
			a = a.Next()
			b = b.Next()
		}
	}
	return output
}
func Invert(a_bm IBitmap) IBitmap {
	output := CreateRBBitmap()
	for i := a_bm.Min(); !i.Limit(); i = i.Next() {
		var node = i.Item()
		var o = BlockArray_invert(&node.Value)
		var o_node = &Chunk{node.Key, o}
		output.AddChunk(o_node)
	}
	return output

}
func NewBitmap() IBitmap {
	return CreateRBBitmap()
}

func Union(a_bm IBitmap, b_bm IBitmap) IBitmap {
	var a = a_bm.Min()
	var b = b_bm.Min()
	defer a.Close()
	defer b.Close()
	output := CreateRBBitmap()
	var o_last_Key = uint64(0xdeadbeef)

	for {
		if a.Limit() && b.Limit() {
			break
		} else if a.Limit() {
			if o_last_Key == b.Item().Key {
				break
			}
			var b_node = b.Item()
			var o_node = &Chunk{b_node.Key, b_node.Value}
			output.AddChunk(o_node)
			o_last_Key = o_node.Key
			b = b.Next()
		} else if b.Limit() {
			if o_last_Key == a.Item().Key {
				break
			}
			var a_node = a.Item()
			var o_node = &Chunk{a_node.Key, a_node.Value}
			output.AddChunk(o_node)
			o_last_Key = o_node.Key
			a = a.Next()
		} else if a.Item().Key < b.Item().Key {
			var a_node = a.Item()
			var o_node = &Chunk{a_node.Key, a_node.Value}
			output.AddChunk(o_node)
			o_last_Key = o_node.Key
			a = a.Next()
		} else if a.Item().Key > b.Item().Key {
			var b_node = b.Item()
			var o_node = &Chunk{b_node.Key, b_node.Value}
			output.AddChunk(o_node)
			o_last_Key = o_node.Key
			b = b.Next()
		} else if a.Item().Key == b.Item().Key {
			var a_node = a.Item()
			var b_node = b.Item().Value
			var o = BlockArray_union(&a_node.Value, &b_node)
			var o_node = &Chunk{a_node.Key, o}
			output.AddChunk(o_node)
			o_last_Key = o_node.Key
			a = a.Next()
			b = b.Next()
		} else {
			log.Println("NEVER SHOULD BE HERE")
			break
		}
	}
	return output
}

func Difference(a_bm IBitmap, b_bm IBitmap) IBitmap {
	var a = a_bm.Min()
	var b = b_bm.Min()
	defer a.Close()
	defer b.Close()
	output := CreateRBBitmap()
	var o_last_Key = uint64(0)

	if o_last_Key != 0 {
		o_last_Key = uint64(0)
	}

	for {
		if a.Limit() && b.Limit() {
			break
		} else if a.Limit() {
			break
		} else if b.Limit() {
			var a_node = a.Item()
			var o_node = &Chunk{a_node.Key, a_node.Value}
			output.AddChunk(o_node)
			o_last_Key = o_node.Key
			a = a.Next()
		} else if a.Item().Key < b.Item().Key {
			var a_node = a.Item()
			var o_node = &Chunk{a_node.Key, a_node.Value}
			output.AddChunk(o_node)
			o_last_Key = o_node.Key
			a = a.Next()
		} else if a.Item().Key > b.Item().Key {
			var b_node = b.Item()
			o_last_Key = b_node.Key
			b = b.Next()
		} else if a.Item().Key == b.Item().Key {
			var a_node = a.Item()
			//var b_node = BlockArray_invert(&b.Item().Value) //probably need to copy this out
			//var o = BlockArray_intersection(&a_node.Value, &b_node)

			var b_node = b.Item().Value
			var o = BlockArray_difference(&a_node.Value, &b_node)

			var o_node = &Chunk{a_node.Key, o}

			//could not add if all zero
			if o_node.Value.bitcount() > 0 {
				output.AddChunk(o_node)
			}
			o_last_Key = o_node.Key
			a = a.Next()
			b = b.Next()
		} else {
			log.Println("NEVER SHOULD BE HERE")
			break
		}
	}
	return output
}

type ChunkIterator interface {
	Limit() bool
	Item() *Chunk
	Next() ChunkIterator
	Dump()
	Close()
}
type RBNodeIterator struct {
	rbiterator rbtree.Iterator
}

func (r *RBNodeIterator) Limit() bool {
	return r.rbiterator.Limit()
}
func (r *RBNodeIterator) Next() ChunkIterator {
	r.rbiterator = r.rbiterator.Next()
	return r
}
func (r *RBNodeIterator) Dump() {
}

func (r *RBNodeIterator) Close() {
}
func (r *RBNodeIterator) Item() *Chunk {
	if r.rbiterator.Item() != nil {
		return r.rbiterator.Item().(*Chunk)
	}
	return nil
}
func GetChunk(bm IBitmap, ChunkKey uint64) *Chunk {
	look := &Chunk{ChunkKey, BlockArray{}}
	return bm.Get(look)
}

type IBitmap interface {
	AddChunk(*Chunk)
	Min() ChunkIterator
	Get(*Chunk) *Chunk
	Len() int
	Inc()
	Count() uint64
	SetCount(uint64)
	Bits() []uint64
	BuildFromBits(bits []uint64)
	ToBytes() []byte
	FromBytes([]byte)
	ToCompressString() string
	FromCompressString(string)
}

func NewRB() *rbtree.Tree {
	return rbtree.NewTree(func(a, b rbtree.Item) int { return Compare(a.(*Chunk).Key, b.(*Chunk).Key) })
}

func CreateRBBitmap() IBitmap {
	return &Bitmap{nodes: NewRB(), bcount: 0}
}

func (self *Bitmap) FromCompressString(str string) {
	compressed_data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		log.Println(err)
		return
	}
	reader, _ := gzip.NewReader(bytes.NewReader(compressed_data))
	data, _ := ioutil.ReadAll(reader)
	self.FromBytes(data)
}

func (self *Bitmap) ToCompressString() string {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(self.ToBytes())
	w.Flush()
	w.Close()
	return base64.StdEncoding.EncodeToString(b.Bytes())
}

func (self *Bitmap) AddChunk(a *Chunk) {
	self.nodes.Insert(a)
}
func (b *Bitmap) Min() ChunkIterator {
	return &RBNodeIterator{b.nodes.Min()}
}
func (b *Bitmap) Get(a *Chunk) *Chunk {
	n := b.nodes.Get(a)
	if n != nil {
		return n.(*Chunk)
	}
	return nil
}

func (b *Bitmap) ToBytes() []byte {
	var (
		buf bytes.Buffer
	)
	enc := gob.NewEncoder(&buf)
	enc.Encode(b.nodes.Len())
	c := 0
	for i := b.nodes.Min(); !i.Limit(); i = i.Next() {
		obj := i.Item().(*Chunk)
		enc.Encode(obj)
		c += 1
	}
	return buf.Bytes()
}

func (self *Bitmap) FromBytes(raw []byte) {
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)

	var size int
	dec.Decode(&size)
	self.nodes = NewRB()
	for i := 0; i < size; i++ {
		chunk := &Chunk{}
		dec.Decode(&chunk)
		self.AddChunk(chunk)
	}
	self.SetCount(BitCount(self))
}

func (b *Bitmap) BuildFromBits(bits []uint64) {
	for _, v := range bits {
		SetBit(b, v)
	}
}
func (b *Bitmap) Bits() []uint64 {
	result := make([]uint64, b.Count())

	x := 0
	for i := b.Min(); !i.Limit(); i = i.Next() {
		item := i.Item()
		chunk := item.Key
		for bi, block := range item.Value.Block {
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
func (b *Bitmap) Len() int {
	return b.nodes.Len()
}
func (b *Bitmap) Inc() {
	b.bcount += 1
}
func (b *Bitmap) SetCount(c uint64) {
	b.bcount = c
}

func (b *Bitmap) Count() uint64 {
	return b.bcount
}

type Address struct {
	ChunkKey   uint64
	BlockIndex uint8
	Bit        uint8
}

func deref(pos uint64) Address {
	ChunkKey := pos >> 11                     // div by 2048
	var bucket_offset = pos & 0x7FF           // mod by 2048
	BlockIndex := uint8(bucket_offset >> 6)   // div by 64
	bit_offset := uint8(bucket_offset & 0x3F) // mod by 64
	return Address{ChunkKey, BlockIndex, bit_offset}
}

func SetBit(b IBitmap, position uint64) (bool, *Chunk, Address) {
	//Chunk,Chunk_index,bit_offset :=deref(position)
	address := deref(position)

	item := GetChunk(b, address.ChunkKey)
	var node *Chunk
	if item == nil {
		node = &Chunk{address.ChunkKey, BlockArray{}}
		b.AddChunk(node)
	} else {
		node = item
	}
	data_changed := node.Value.set_bit(address.BlockIndex, address.Bit)
	if data_changed {
		b.Inc()
	}
	return data_changed, node, address
}
func BitCount(b IBitmap) uint64 {
	var total uint64
	total = 0
	i := b.Min()
	defer i.Close()
	for ; !i.Limit(); i = i.Next() {
		var item = i.Item()
		total += item.Value.bitcount()
	}
	return total
}
