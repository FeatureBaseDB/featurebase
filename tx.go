// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"github.com/molecula/featurebase/v3/roaring"
	txkey "github.com/molecula/featurebase/v3/short_txkey"
	//txkey "github.com/molecula/featurebase/v3/txkey"
)

// writable initializes Tx that update, use !writable for read-only.
const writable = true

// Tx providers offer transactional storage for high-level roaring.Bitmaps and
// low-level roaring.Containers.
//
// The common 4-tuple of (index, field, view, shard) jointly specify a fragment.
// A fragment conceptually holds one roaring.Bitmap.
//
// Within the fragment, the ckey or container-key is the uint64 that specifies
// the high 48-bits of the roaring.Bitmap 64-bit space.
// The ckey is used to retrieve a specific roaring.Container that
// is either a run, array, or raw bitmap. The roaring.Container is the
// low 16-bits of the roaring.Bitmap space. Its size is at most
// 8KB (2^16 bits / (8 bits / byte) == 8192 bytes).
//
// The grain of the transaction is guaranteed to be at least at the shard
// within one index. Therefore updates to any of the fields within
// the same shard will be atomically visible only once the transaction commits.
// Reads from another, concurrently open, transaction will not see updates
// that have not been committed.
type Tx interface {

	// Type returns "roaring", "rbf", or one of the other
	// Tx types at the top of txfactory.go
	Type() string

	// Rollback must be called at the end of read-only transactions. Either
	// Rollback or Commit must be called at the end of writable transactions.
	// It is safe to call Rollback multiple times, but it must be
	// called at least once to release resources. Any Rollback after
	// a Commit is ignored, so 'defer tx.Rollback()' should be commonly
	// written after starting a new transaction.
	//
	// If there is an error during internal Rollback processing,
	// this would be quite serious, and the underlying storage is
	// expected to panic. Hence there is no explicit error returned
	// from Rollback that needs to be checked.
	Rollback()

	// Commit makes the updates in the Tx visible to subsequent transactions.
	Commit() error

	// ContainerIterator loops over the containers in the conceptual
	// roaring.Bitmap for the specified fragment.
	// Calling Next() on the returned roaring.ContainerIterator gives
	// you a roaring.Container that is either run, array, or raw bitmap.
	// Return value 'found' is true when the ckey container was present.
	// ckey of 0 gives all containers (in the fragment).
	//
	// ContainerIterator must not have side-effects.
	//
	// citer.Close() must be called when the client is done using it.
	ContainerIterator(index, field, view string, shard uint64, ckey uint64) (citer roaring.ContainerIterator, found bool, err error)

	// ApplyFilter applies a roaring.BitmapFilter to a specified shard,
	// starting at the given container key. The filter's ConsiderData
	// method may be called with transient Container objects which *must
	// not* be retained or referenced after that function exits. Similarly,
	// their data must not be retained. If you need the data later, you
	// must copy it into some other memory.
	ApplyFilter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapFilter) (err error)

	// ApplyRewriter applies a roaring.BitmapRewriter to a specified shard,
	// starting at the given container key. The filter's ConsiderData
	// method may be called with transient Container objects which *must
	// not* be retained or referenced after that function exits. Similarly,
	// their data must not be retained. If you need the data later, you
	// must copy it into some other memory. However, it is safe to overwrite
	// the returned container; for instance, you can DifferenceInPlace on
	// it.
	ApplyRewriter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapRewriter) (err error)

	// RoaringBitmap retrieves the roaring.Bitmap for the entire shard.
	RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error)

	// Container returns the roaring.Container for the given ckey
	// (container-key or highbits) in the chosen fragment.
	Container(index, field, view string, shard uint64, ckey uint64) (*roaring.Container, error)

	// PutContainer stores c under the given ckey (container-key) in the specified fragment.
	PutContainer(index, field, view string, shard uint64, ckey uint64, c *roaring.Container) error

	// RemoveContainer deletes the roaring.Container under the given ckey (container-key)
	// in the specified fragment.
	RemoveContainer(index, field, view string, shard uint64, ckey uint64) error

	// Add adds the 'a' values to the Bitmap for the fragment.
	Add(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error)

	// Remove removes the 'a' values from the Bitmap for the fragment.
	Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error)

	// Contains tests if the uint64 v is stored in the fragment's Bitmap.
	Contains(index, field, view string, shard uint64, v uint64) (exists bool, err error)

	// Count returns the count of hot bits on the fragment.
	Count(index, field, view string, shard uint64) (uint64, error)

	// Max returns the maximum value set in the Bitmap for the fragment.
	Max(index, field, view string, shard uint64) (uint64, error)

	// Min returns the minimum value set in the Bitmap for the fragment.
	Min(index, field, view string, shard uint64) (uint64, bool, error)

	// CountRange returns the count of hot bits in the [start, end) range on the
	// fragment.
	CountRange(index, field, view string, shard uint64, start, end uint64) (uint64, error)

	// OffsetRange returns a *roaring.Bitmap containing the portion of the Bitmap for the fragment
	// which is specified by a combination of (offset, [start, end)).
	//
	// start  - The value at which to start reading. This must be the zero value
	//          of a container; i.e. [0, 65536, ...]
	// end    - The value at which to end reading. This must be the zero value
	//          of a container; i.e. [0, 65536, ...]
	// offset - The number of positions to shift the resulting bitmap. This must
	//          be the zero value of a container; i.e. [0, 65536, ...]
	//
	// For example, if (index, field, view, shard) represents the following bitmap:
	// [1, 2, 3, 65536, 65539]
	//
	// then the following results are achieved based on (offset, start, end):
	// (0, 0, 131072)          => [1, 2, 3, 65536, 65539]
	// (0, 65536, 131072)      => [0, 3]
	// (65536, 65536, 131072)  => [65536, 65539]
	// (262144, 65536, 131072) => [262144, 262147]
	//
	OffsetRange(index, field, view string, shard uint64, offset, start, end uint64) (*roaring.Bitmap, error)

	// ImportRoaringBits does efficient bulk import using rit, a roaring.RoaringIterator.
	//
	// See the roaring package for details of the RoaringIterator.
	//
	// If clear is true, the bits from rit are cleared, otherwise they are set in the
	// specifed fragment.
	//
	// ImportRoaringBits return values changed and rowSet may be inaccurate if
	// the data []byte is supplied (the RoaringTx implementation neglects this for speed).
	ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error)

	// GetSortedFieldViewList gets the set of FieldView(s)
	GetSortedFieldViewList(idx *Index, shard uint64) (fvs []txkey.FieldView, err error)

	GetFieldSizeBytes(index, field string) (uint64, error)
}

// GenericApplyFilter implements ApplyFilter in terms of tx.ContainerIterator,
// as a convenience if a Tx backend hasn't implemented this new function yet.
func GenericApplyFilter(tx Tx, index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapFilter) (err error) {
	iter, _, err := tx.ContainerIterator(index, field, view, shard, ckey)
	if err != nil {
		return err
	}
	// ApplyFilterToIterator closes the iterator for us.
	return roaring.ApplyFilterToIterator(filter, iter)
}
