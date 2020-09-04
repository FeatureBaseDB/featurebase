// Copyright 2020 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !amd64

package pilosa

// this is a stubbed out file to let 386 build. lmdb won't work well
// on 32-bit; not enough memory map address space.

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/pilosa/v2/roaring"
)

var _ = time.Now

// lmdbRegistrar facilitates shutdown
// of all the lmdb databases started under
// tests. Its needed because most tests don't cleanup
// the *Index(es) they create. But we still
// want to shutdown lmdbDB goroutines
// after tests run.
//
// It also allows opening the same path twice to
// result in sharing the same open database handle, and
// thus the same transactional guarantees.
//
type lmdbRegistrar struct {
	mu sync.Mutex
	mp map[*LMDBWrapper]bool

	path2db map[string]*LMDBWrapper
}

var globalLMDBReg *lmdbRegistrar = newLMDBTestRegistrar()

func newLMDBTestRegistrar() *lmdbRegistrar {

	return &lmdbRegistrar{
		mp:      make(map[*LMDBWrapper]bool),
		path2db: make(map[string]*LMDBWrapper),
	}
}

func (r *lmdbRegistrar) OpenDBWrapper(path0 string, doAllocZero bool) (DBWrapper, error) {
	panic("lmdb only available on 64-bit arch")
}

// register each lmdb created under tests, so we
// can clean them up. This is called by openLMDBWrapper() while
// holding the r.mu.Lock, since it needs to atomically
// check the registry and make a new instance only
// if one does not exist for its path, and otherwise
// return the existing instance.
func (r *lmdbRegistrar) unprotectedRegister(w *LMDBWrapper) {
	panic("lmdb only available on 64-bit arch")
}

// unregister removes w from r
func (r *lmdbRegistrar) unregister(w *LMDBWrapper) {
	panic("lmdb only available on 64-bit arch")
}

func DumpAllLMDB() {
	panic("lmdb only available on 64-bit arch")
}

// lmdbPath is a helper for determining the full directory
// in which the lmdb database will be stored.
func lmdbPath(path string) string {
	if !strings.HasSuffix(path, "-lmdb") {
		return path + "-lmdb"
	}
	return path
}

// openLMDBDB opens the database in the bpath directoy
// without deleting any prior content. Any LMDBDB
// database directory will have the "-lmdb" suffix.
//
// openLMDBDB will check the registry and make a new instance only
// if one does not exist for its bpath. Otherwise it returns
// the existing instance. This insures only one lmdbDB
// per bpath in this pilosa node.
func (r *lmdbRegistrar) openLMDBWrapper(path0 string) (*LMDBWrapper, error) {
	panic("lmdb only available on 64-bit arch")
}

var ErrShutdown = fmt.Errorf("shutting down")

// DeleteIndex deletes all the containers associated with
// the named index from the lmdb database.
func (w *LMDBWrapper) DeleteIndex(indexName string) error {
	panic("lmdb only available on 64-bit arch")
}

// statically confirm that LMDBTx satisfies the Tx interface.
var _ Tx = (*LMDBTx)(nil)

// LMDBWrapper provides the NewLMDBTx() method.
// Execute lmdbJob's via LMDBWrapper.submit(); these must
// be done by the lmdb goroutine worker pool.
type LMDBWrapper struct{}

func (w *LMDBWrapper) IsClosed() bool {
	panic("lmdb only available on 64-bit arch")
}

// NewLMDBTx produces LMDB based ACID transactions. If
// the transaction will modify data, then the write flag must be true.
// Read-only queries should set write to false, to allow more concurrency.
// Methods on a LMDBTx are thread-safe, and can be called from
// different goroutines.
//
// initialIndexName is optional. It is set by the TxFactory from the Txo
// options provided at the Tx creation point. It allows us to recognize
// and isolate cross-index queries more quickly. It can always be empty ""
// but when set is highly useful for debugging. It has no impact
// on transaction behavior.
//
func (w *LMDBWrapper) NewLMDBTx(write bool, initialIndexName string, frag *fragment) (tx *LMDBTx) {
	panic("lmdb only available on 64-bit arch")
}

// Close shuts down the LMDB database.
func (w *LMDBWrapper) Close() (err error) {
	panic("lmdb only available on 64-bit arch")
}

// LMDBTx wraps a lmdb.Txn and provides the Tx interface
// method implementations.
// The methods on LMDBTx are thread-safe, and can be called
// from different goroutines.
type LMDBTx struct{}

func (tx *LMDBTx) Type() string {
	panic("lmdb only available on 64-bit arch")
}

func (tx *LMDBTx) UseRowCache() bool {
	panic("lmdb only available on 64-bit arch")
}

func (tx *LMDBTx) Group() *TxGroup {
	panic("lmdb only available on 64-bit arch")
}

// Pointer gives us a memory address for the underlying transaction for debugging.
// It is public because we use it in roaring to report invalid container memory access
// outside of a transaction.
func (tx *LMDBTx) Pointer() string {
	panic("lmdb only available on 64-bit arch")
}

// Sn retreives the serial number of the Tx.
func (tx *LMDBTx) Sn() int64 {
	panic("lmdb only available on 64-bit arch")
}

// Rollback rolls back the transaction.
func (tx *LMDBTx) Rollback() {
	panic("lmdb only available on 64-bit arch")
}

// Commit commits the transaction to permanent storage.
// Commits can handle up to 100k updates to fragments
// at once, but not more. This is a LMDBDB imposed limit.
func (tx *LMDBTx) Commit() error {
	panic("lmdb only available on 64-bit arch")
}

// Readonly returns true iff the LMDBTx is read-only.
func (tx *LMDBTx) Readonly() bool {
	panic("lmdb only available on 64-bit arch")
}

// RoaringBitmap returns the roaring.Bitmap for all bits in the fragment.
func (tx *LMDBTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	panic("lmdb only available on 64-bit arch")
}

// Container returns the requested roaring.Container, selected by fragment and ckey
func (tx *LMDBTx) Container(index, field, view string, shard uint64, ckey uint64) (c *roaring.Container, err error) {
	panic("lmdb only available on 64-bit arch")
}

// PutContainer stores rc under the specified fragment and container ckey.
func (tx *LMDBTx) PutContainer(index, field, view string, shard uint64, ckey uint64, rc *roaring.Container) error {
	panic("lmdb only available on 64-bit arch")
}

// RemoveContainer deletes the container specified by the shard and container key ckey
func (tx *LMDBTx) RemoveContainer(index, field, view string, shard uint64, ckey uint64) error {
	panic("lmdb only available on 64-bit arch")
}

// Add sets all the a bits hot in the specified fragment.
func (tx *LMDBTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {
	panic("lmdb only available on 64-bit arch")

}

// Remove clears all the specified a bits in the chosen fragment.
func (tx *LMDBTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	panic("lmdb only available on 64-bit arch")
}

// Contains returns exists true iff the bit chosen by key is
// hot (set to 1) in specified fragment.
func (tx *LMDBTx) Contains(index, field, view string, shard uint64, key uint64) (exists bool, err error) {
	panic("lmdb only available on 64-bit arch")
}

func (tx *LMDBTx) SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error) {
	panic("lmdb only available on 64-bit arch")
}

// key is the container key for the first roaring Container
// roaring docs: Iterator returns a ContainterIterator which *after* a call to Next(), a call to Value() will
// return the first container at or after key. found will be true if a
// container is found at key.
//
// LMDBTx notes: We auto-stop at the end of this shard, not going beyond.
func (tx *LMDBTx) ContainerIterator(index, field, view string, shard uint64, firstRoaringContainerKey uint64) (citer roaring.ContainerIterator, found bool, err error) {
	panic("lmdb only available on 64-bit arch")

}

// LMDBIterator is the iterator returned from a LMDBTx.ContainerIterator() call.
// It implements the roaring.ContainerIterator interface.
type LMDBIterator struct{}

// NewLMDBIterator creates an iterator on tx that will
// only return badgerKeys that start with prefix.
func NewLMDBIterator(tx *LMDBTx, prefix []byte) (bi *LMDBIterator) {
	panic("lmdb only available on 64-bit arch")
}

// Close tells the database and transaction that the user is done
// with the iterator.
func (bi *LMDBIterator) Close() {
	panic("lmdb only available on 64-bit arch")
}

// Valid returns false if there are no more values in the iterator's range.
func (bi *LMDBIterator) Valid() bool {
	panic("lmdb only available on 64-bit arch")
}

// Seek allows the iterator to start at needle instead of the global begining.
func (bi *LMDBIterator) Seek(needle []byte) (ok bool) {
	panic("lmdb only available on 64-bit arch")
}

func (bi *LMDBIterator) ValidForPrefix(prefix []byte) bool {
	panic("lmdb only available on 64-bit arch")
}

func (bi *LMDBIterator) String() (r string) {
	panic("lmdb only available on 64-bit arch")
}

var oneByteSliceOfZero = []byte{0}

// Next advances the iterator.
func (bi *LMDBIterator) Next() (ok bool) {
	panic("lmdb only available on 64-bit arch")
}

// Value retrieves what is pointed at currently by the iterator.
func (bi *LMDBIterator) Value() (containerKey uint64, c *roaring.Container) {
	panic("lmdb only available on 64-bit arch")
}

// lmdbFinder implements roaring.IteratorFinder.
// It is used by LMDBTx.ForEach()
type lmdbFinder struct {
	tx        *LMDBTx
	index     string
	field     string
	view      string
	shard     uint64
	needClose []Closer
}

// FindIterator lets lmdbFinder implement the roaring.FindIterator interface.
func (bf *lmdbFinder) FindIterator(seek uint64) (roaring.ContainerIterator, bool) {
	panic("lmdb only available on 64-bit arch")
}

// Close closes all bf.needClose listed Closers.
func (bf *lmdbFinder) Close() {
	panic("lmdb only available on 64-bit arch")
}

// NewTxIterator returns a *roaring.Iterator that MUST have Close() called on it BEFORE
// the transaction Commits or Rollsback.
func (tx *LMDBTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	panic("lmdb only available on 64-bit arch")
}

// ForEach applies fn to each bitmap in the fragment.
func (tx *LMDBTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	panic("lmdb only available on 64-bit arch")
}

// ForEachRange applies fn on the selected range of bits on the chosen fragment.
func (tx *LMDBTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {
	panic("lmdb only available on 64-bit arch")
}

// Count operates on the full bitmap level, so it sums over all the containers
// in the bitmap.
func (tx *LMDBTx) Count(index, field, view string, shard uint64) (uint64, error) {
	panic("lmdb only available on 64-bit arch")
}

// Max is the maximum bit-value in your bitmap.
// Returns zero if the bitmap is empty. Odd, but this is what roaring.Max does.
func (tx *LMDBTx) Max(index, field, view string, shard uint64) (uint64, error) {
	panic("lmdb only available on 64-bit arch")
}

// Min returns the smallest bit set in the fragment. If no bit is hot,
// the second return argument is false.
func (tx *LMDBTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {
	panic("lmdb only available on 64-bit arch")
}

// CountRange returns the count of hot bits in the start, end range on the fragment.
// roaring.countRange counts the number of bits set between [start, end).
func (tx *LMDBTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {
	panic("lmdb only available on 64-bit arch")
}

// OffsetRange creates a new roaring.Bitmap to return in other. For all the
// hot bits in [start, endx) of the chosen fragment, it stores
// them into other but with offset added to their bit position.
// The primary client is doing this, using ShardWidth, already; see
// fragment.rowFromStorage() in fragment.go. For example:
//
//    data, err := tx.OffsetRange(f.index, f.field, f.view, f.shard,
//                     f.shard*ShardWidth, rowID*ShardWidth, (rowID+1)*ShardWidth)
//                     ^ offset            ^ start           ^ endx
//
// The start and endx arguments are container keys that have been shifted left by 16 bits;
// their highbits() will be taken to determine the actual container keys. This
// is done to conform to the roaring.OffsetRange() argument convention.
//
func (tx *LMDBTx) OffsetRange(index, field, view string, shard, offset, start, endx uint64) (other *roaring.Bitmap, err error) {
	panic("lmdb only available on 64-bit arch")
}

// IncrementOpN increments the tx opcount by changedN
func (tx *LMDBTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {
	panic("lmdb only available on 64-bit arch")
}

// ImportRoaringBits handles deletes by setting clear=true.
// rowSet[rowID] returns the number of bit changed on that rowID.
func (tx *LMDBTx) ImportRoaringBits(index, field, view string, shard uint64, itr roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	panic("lmdb only available on 64-bit arch")
}

func (tx *LMDBTx) toContainer(typ byte, v []byte) (r *roaring.Container) {
	panic("lmdb only available on 64-bit arch")
}

// StringifiedLMDBKeys returns a string with all the container
// keys available in lmdb.
func (w *LMDBWrapper) StringifiedLMDBKeys(optionalUseThisTx Tx) (r string) {
	panic("lmdb only available on 64-bit arch")
}

// countBitsSet returns the number of bits set (or "hot") in
// the roaring container value found by the txkey.Key()
// formatted bkey.
func (tx *LMDBTx) countBitsSet(bkey []byte) (n int) {
	panic("lmdb only available on 64-bit arch")
}

func (tx *LMDBTx) Dump() {
	panic("lmdb only available on 64-bit arch")
}

// stringifiedLMDBKeysTx reports all the lmdb keys and a
// corresponding blake3 hash viewable by txn within the entire
// lmdb database.
// It also reports how many bits are hot in the roaring container
// (how many bits are set, or 1 rather than 0).
//
// By convention, we must return the empty string if there
// are no keys present. The tests use this to confirm
// an empty database.
func stringifiedLMDBKeysTx(tx *LMDBTx) (r string) {
	panic("lmdb only available on 64-bit arch")
}

func (w *LMDBWrapper) DeleteField(index, field, fieldPath string) error {
	panic("lmdb only available on 64-bit arch")
}

func (w *LMDBWrapper) DeleteFragment(index, field, view string, shard uint64, frag interface{}) error {
	panic("lmdb only available on 64-bit arch")
}

func (w *LMDBWrapper) DeletePrefix(prefix []byte) error {
	panic("lmdb only available on 64-bit arch")
}

func (tx *LMDBTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {
	panic("lmdb only available on 64-bit arch")
}

// UnionInPlace unions all the others Bitmaps into a new Bitmap, and then writes it to the
// specified fragment.
func (tx *LMDBTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {
	panic("lmdb only available on 64-bit arch")
}

func (tx *LMDBTx) Options() Txo {
	panic("lmdb only available on 64-bit arch")
}
