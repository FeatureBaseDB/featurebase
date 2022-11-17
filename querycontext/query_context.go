// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package querycontext

import (
	"sort"
	"strings"

	"github.com/featurebasedb/featurebase/v3/roaring"
)

// QueryContext represents the lifespan of a query or similar thing which
// is accessing one or more backend databases. The individual databases
// are transactional; a transaction allows seeing consistent data (even
// if other things are may be writing to the database), keeps memory returned
// by the backend from being invalidated, and makes sets of changes take
// effect atomically.
//
// The QueryContext should not be closed until all access to data returned
// from queries is complete.
//
// The Error/Errorf methods tell the QueryContext that an error has occurred
// which should prevent it from committing. If you call either of them for
// a QueryContext, Commit() must fail. (It may yield the error provided,
// or another error which seemed important.) NewRead and NewWrite also fail
// once an error has been reported.
//
// A QueryContext is created with a parent context.Context, and will also
// fail, and refuse to commit, if that context is canceled before you try
// to commit.
type QueryContext interface {
	// NewRead requests a new QueryRead object for the indicated fragment.
	NewRead(IndexName, FieldName, ViewName, ShardID) (QueryRead, error)
	// NewWrite requests a new QueryWrite object for the indicated fragment.
	NewWrite(IndexName, FieldName, ViewName, ShardID) (QueryWrite, error)
	// Error sets a persistent error state and indicates that this QueryContext
	// must not commit its writes.
	Error(...interface{})
	// Errorf is a convenience function equivalent to Error(fmt.Errorf(...))
	Errorf(string, ...interface{})
	// Release releases resources held by this QueryContext without committing
	// writes. If writes have already been committed, they are not affected.
	// A release after a commit (or another release) is harmless.
	Release()
	// Commit attempts to commit writes, unless an error has already been
	// recorded or the parent context has been canceled. If it does not attempt
	// to commit writes, it reports the error that prevented it. Otherwise it
	// attempts the writes and reports an error if any errors occurred.
	// It is an error to try to commit twice or use the QueryContext after a
	// commit.
	Commit() error
}

// QueryRead represents read access to a fragment. When functions in
// this interface return an error, the error indicates a failed operation,
// such as an I/O error. Empty or nonexistent data is not an error.
// For example, the Container method can return a nil pointer if no such
// container exists, but would also return a nil error in that case. An
// error would be returned only if the attempt to determine whether the
// container exists failed for some reason.
type QueryRead interface {
	// ContainerIterator yields a container iterator starting at
	// the given key. The found bool return indicates whether that
	// exact container was present. The iterator's Close() function
	// must be called when done using it.
	ContainerIterator(ckey uint64) (citer roaring.ContainerIterator, found bool, err error)

	// ApplyFilter applies a roaring.BitmapFilter to the fragment, starting
	// at the given container key. The container objects passed to the
	// filter's ConsiderData method are transient objects; both the
	// container header and the data associated with the container can be
	// overwritten by the filter after each call. If you need the Container
	// objects, or the data they reference, after that method is called,
	// you must clone them.
	ApplyFilter(ckey uint64, filter roaring.BitmapFilter) (err error)

	// Container returns the *roaring.Container for the container key,
	// which may be a nil if the container isn't present. The container
	// returned is valid for the life of the query context.
	Container(ckey uint64) (*roaring.Container, error)

	// Contains determines whether the bit is set.
	Contains(v uint64) (exists bool, err error)

	// Count returns the count of bits set in the fragment.
	Count() (uint64, error)

	// Max returns the highest bit set in the fragment.
	Max() (uint64, error)

	// Min returns the lowest bit set in the fragment.
	Min() (uint64, bool, error)

	// CountRange returns the count of set bits in the range [start, end)
	// in this fragment. The lower bound is inclusive, the upper bound is
	// exclusive.
	CountRange(start, end uint64) (uint64, error)

	// OffsetRange returns a bitmap containing the containers covering the
	// range (in bits) from start (inclusive) to end (exclusive). Despite
	// the range being specified in bits, all three parameters must be multiples
	// of 65,536 (the size of a Container).
	//
	// The bits returned will have their offsets adjusted by (offset-start).
	// For instance, if start is 0, and offset is 65536, all bits will be
	// 65536 higher (which is to say, all container keys will be one higher
	// than they were in the fragment).
	//
	// OffsetRange is used to translate from a row of a fragment to a shard
	// of a database-wide Row. For instance:
	//
	// OffsetRange(3 * ShardWidth, 4 * ShardWidth, 7 * ShardWidth)
	//
	// would yield the third "row" of a fragment, with its container keys adjusted
	// to reflect the range covered by shard 7 of the index.
	//
	// The resulting bitmap is valid for the lifespan of the QueryContext.
	OffsetRange(offset, start, end uint64) (*roaring.Bitmap, error)

	// RoaringBitmap produces a roaring.Bitmap representing the entire fragment.
	// The resulting bitmap is valid for the lifespan of the QueryContext.
	RoaringBitmap() (*roaring.Bitmap, error)
}

// QueryWrite represents write access to a fragment. As with QueryRead,
// errors indicate an unexpected error. For instance, if you try to
// remove a container that doesn't exist, that's not an "error", but if
// you try to remove a container and get a disk write error or something
// like that, that's an error.
type QueryWrite interface {
	QueryRead

	// PutContainer stores c under the given key in the fragment.
	PutContainer(ckey uint64, c *roaring.Container) error

	// RemoveContainer deletes the roaring.Container under the given key
	// in the fragment.
	RemoveContainer(ckey uint64) error

	// Add sets the given bits in the fragment, and reports how many bits
	// actually changed.
	Add(a ...uint64) (changeCount int, err error)

	// Remove clears the given bits in the fragment, and reports how many
	// bits actually changed.
	Remove(a ...uint64) (changeCount int, err error)

	// ApplyRewriter applies a roaring.BitmapRewriter to a specified shard,
	// starting at the given container key. The filter's ConsiderData
	// method may be called with transient Container objects which *must
	// not* be retained or referenced after that function exits. Similarly,
	// their data must not be retained. If you need the data later, you
	// must copy it into some other memory. However, it is safe to overwrite
	// the returned container; for instance, you can DifferenceInPlace on
	// it.
	ApplyRewriter(ckey uint64, filter roaring.BitmapRewriter) (err error)

	// ImportRoaringBits does efficient bulk import using a roaring.RoaringIterator.
	//
	// See the roaring package for details of the RoaringIterator.
	//
	// If clear is true, the bits from rit are cleared, otherwise they are set in the
	// specifed fragment.
	ImportRoaringBits(rit roaring.RoaringIterator, clear bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error)
}

type IndexName string
type FieldName string
type ViewName string
type ShardID uint64

// QueryScope represents a possible set of things that can be written
// to. A QueryScope can in principle represent arbitrary patterns with
// special rules. However! Our system depends on using QueryScope
// objects to detect and prevent overlapping writes, to ensure that
// queries running in parallel won't deadlock against each other.
//
// So each TxStore can yield QueryScope objects, the Overlap semantics
// of which match the TxStore's database definitions. If two QueryScopes
// are considered to overlap, that means that there exist fragment
// identifiers such that each QueryScope returns true for Allowed on
// at least one of these fragment identifiers, and the TxStore's
// KeySplitter would produce the same database key for those fragment
// identifiers.
//
// The Add functions return the scope to allow things like
//
//	txs.NewWriteQueryContext(ctx, txs.Scope().AddIndex("i"))
//
// and chaining add operations in simple cases.
type QueryScope interface {
	// Allowed determines whether a specific fragment
	// is covered by this QueryScope.
	Allowed(IndexName, FieldName, ViewName, ShardID) bool

	// Overlap reports whether there are any overlaps between this
	// QueryScope object and another. An overlap exists wherever
	// calls to Allowed with the same parameters would return true for
	// both objects.
	Overlap(QueryScope) bool

	AddAll() QueryScope
	AddIndex(IndexName) QueryScope
	AddField(IndexName, FieldName) QueryScope
	AddIndexShards(IndexName, ...ShardID) QueryScope
	AddFieldShards(IndexName, FieldName, ...ShardID) QueryScope

	String() string
}

// indexShardQueryScope is a QueryScope which ignores fields and
// views, and provides a map from indexes to shards that are covered
// within those indexes. An empty shard list indicates all shards,
// an absent key indicates no shards. Shard lists are stored sorted.
type indexShardQueryScope struct {
	shards map[IndexName]shardList
	all    bool
}

var _ QueryScope = &indexShardQueryScope{}

func (i *indexShardQueryScope) String() string {
	var scopes []string
	for index, shards := range i.shards {
		if shards.all {
			scopes = append(scopes, string(index))
		} else {
			scopes = append(scopes, string(index+"#"))
		}
	}
	// ensure consistent order for reader benefit
	sort.Strings(scopes)
	return strings.Join(scopes, ",")
}

// AddAll adds the whole database
func (i *indexShardQueryScope) AddAll() QueryScope {
	i.all = true
	return i
}

// AddIndex adds the given index, with all shards writable.
func (i *indexShardQueryScope) AddIndex(index IndexName) QueryScope {
	if i.shards == nil {
		i.shards = map[IndexName]shardList{index: {all: true}}
		return i
	}
	i.shards[index] = shardList{all: true}
	return i
}

// AddIndexShards adds the given index for the given shards.
func (i *indexShardQueryScope) AddIndexShards(index IndexName, shards ...ShardID) QueryScope {
	if i.all {
		return i
	}
	if i.shards == nil {
		i.shards = map[IndexName]shardList{}
	}
	existing := i.shards[index]
	// We could at this point check whether anything previously existed, and
	// if not, just use a new {any: shards} shardlist, but we want to verify
	// shard lists are sorted.
	if existing.all {
		return i
	}
	for _, shard := range shards {
		existing.Add(shard)
	}
	i.shards[index] = existing
	return i
}

func (i *indexShardQueryScope) AddField(index IndexName, _ FieldName) QueryScope {
	return i.AddIndex(index)
}

func (i *indexShardQueryScope) AddFieldShards(index IndexName, field FieldName, shards ...ShardID) QueryScope {
	return i.AddIndexShards(index, shards...)
}

func (i *indexShardQueryScope) Allowed(index IndexName, _ FieldName, _ ViewName, shard ShardID) bool {
	shards, ok := i.shards[index]
	if !ok {
		return false
	}
	return shards.Allowed(shard)
}

func (i *indexShardQueryScope) Overlap(qw QueryScope) bool {
	// this panics if the other isn't also an indexShardQueryScope.
	// don't mix and match.
	other := qw.(*indexShardQueryScope)
	for index, shardList := range i.shards {
		if otherShards, ok := other.shards[index]; ok {
			if shardList.Overlap(otherShards) {
				return true
			}
		}
	}
	return false
}

// indexScope is to an index's fields as shardList is to
// a list of shards; `all` is the shardList of index-wide
// reservations, `any` is the map of fields to field-specific
// reservations.
type indexScope struct {
	all shardList
	any map[FieldName]shardList
}

// Overlap determines whether two index scopes overlap. This
// means they have shards in common between corresponding
// fields, or between anything and their index-wide shard lists.
func (i *indexScope) Overlap(other *indexScope) bool {
	// direct index<->index overlaps
	if i.all.Overlap(other.all) {
		return true
	}
	// our index-wide, their field-specific
	for _, otherShards := range other.any {
		if i.all.Overlap(otherShards) {
			return true
		}
	}
	for field, shards := range i.any {
		// our field-specific, their index-wide
		if other.all.Overlap(shards) {
			return true
		}
		// matching fields
		otherShards := other.any[field]
		if otherShards.Overlap(shards) {
			return true
		}
	}
	return false
}

func (scope *indexScope) AddField(field FieldName) {
	if scope.all.all {
		// We already cover everything.
		return
	}
	if scope.any == nil {
		scope.any = map[FieldName]shardList{field: {all: true}}
		return
	}
	scope.any[field] = shardList{all: true}
}

func (scope *indexScope) AddFieldShards(field FieldName, shards ...ShardID) {
	if scope.all.all {
		// We already cover everything.
		return
	}
	if scope.any == nil {
		scope.any = map[FieldName]shardList{}
	}
	existing, ok := scope.any[field]
	if !ok {
		existing = shardList{}
	}
	if existing.all {
		return
	}
	for _, shard := range shards {
		existing.Add(shard)
	}
	scope.any[field] = existing
}

// Complexity yields a small visual indicator of complexity of
// this scope.
// "": we actually cover nothing?
// *: we cover everything
// #: we cover some shards, nothing per-field
// /*: we cover some fields but not per-shard
// /#: we cover some shards of some fields
// #/*: we cover some shards and some whole fields
// #/#: we cover some shards index-wide and some shards of some fields
func (scope *indexScope) Complexity() string {
	if scope.all.all {
		return "*"
	}
	wholeIndex := len(scope.all.any) > 0
	wholeFields := false
	partialFields := false
	for _, shards := range scope.any {
		if shards.all {
			wholeFields = true
		} else if len(shards.any) > 0 {
			partialFields = true
		}
	}
	var result string
	if wholeIndex {
		result = "#"
	}
	if partialFields {
		return result + "/#"
	}
	if wholeFields {
		return result + "/*"
	}
	return result
}

// flexibleQueryScope is an experimental case which allows some indexes to be
// split into fields, while others aren't. it relies on a corresponding
// flexibleKeySplitter for the list of indexes which are always handled at
// a full index level.
//
// You can just add an entire index, even if it's unsplit. If an index is
// split, you can in principle add the whole index for some shards and just
// some fields or others, but if you want to do this, please don't.
// Hesitate to.
//
// If no flexibleKeySplitter is provided, every index is split.
type flexibleQueryScope struct {
	all      bool
	splitter *flexibleKeySplitter
	indexes  map[IndexName]*indexScope
}

var _ QueryScope = &flexibleQueryScope{}

func (i *flexibleQueryScope) String() string {
	if i.all {
		return "*"
	}
	descrs := make([]string, 0, len(i.indexes))
	for index, scope := range i.indexes {
		// Show the index's name plus something indicating the
		// approximate shape of the scope -- is it the whole index,
		// some shards, some fields, or what?
		descrs = append(descrs, string(index)+scope.Complexity())
	}
	sort.Strings(descrs)
	return strings.Join(descrs, ",")
}

// AddAll does what it sounds like.
func (i *flexibleQueryScope) AddAll() QueryScope {
	i.all = true
	return i
}

// AddIndex adds the given index, with all shards writable.
func (i *flexibleQueryScope) AddIndex(index IndexName) QueryScope {
	if i.all {
		return i
	}
	if i.indexes == nil {
		i.indexes = map[IndexName]*indexScope{index: {all: shardList{all: true}}}
		return i
	}
	i.indexes[index] = &indexScope{all: shardList{all: true}}
	return i
}

// AddIndexShards adds the given index for the given shards.
func (i *flexibleQueryScope) AddIndexShards(index IndexName, shards ...ShardID) QueryScope {
	if i.all {
		return i
	}
	if i.indexes == nil {
		i.indexes = map[IndexName]*indexScope{}
	}
	// We could at this point check whether anything previously existed, and
	// if not, just use a new {any: shards} shardlist, but we want to verify
	// shard lists are sorted.
	scope := i.indexes[index]
	if scope == nil {
		scope = &indexScope{}
		i.indexes[index] = scope
	}
	if scope.all.all {
		return i
	}
	for _, shard := range shards {
		scope.all.Add(shard)
	}
	return i
}

// AddField adds the given field, with all shards writable. If the field
// is in an unsplit index, the entire index is covered.
func (i *flexibleQueryScope) AddField(index IndexName, field FieldName) QueryScope {
	if i.all {
		return i
	}
	if i.splitter != nil {
		if _, ok := i.splitter.splitIndexes[index]; !ok {
			// ignore field because this index isn't split
			return i.AddIndex(index)
		}
	}
	if i.indexes == nil {
		i.indexes = make(map[IndexName]*indexScope)
	}
	scope := i.indexes[index]
	if scope == nil {
		scope = &indexScope{}
		i.indexes[index] = scope
	}
	scope.AddField(field)
	return i
}

// AddFieldShards adds the given index for the given shards.
func (i *flexibleQueryScope) AddFieldShards(index IndexName, field FieldName, shards ...ShardID) QueryScope {
	if i.all {
		return i
	}
	if i.splitter != nil {
		if _, ok := i.splitter.splitIndexes[index]; !ok {
			// ignore field because this index isn't split
			return i.AddIndexShards(index, shards...)
		}
	}
	if i.indexes == nil {
		i.indexes = make(map[IndexName]*indexScope)
	}
	scope := i.indexes[index]
	if scope == nil {
		scope = &indexScope{}
		i.indexes[index] = scope
	}
	scope.AddFieldShards(field, shards...)
	return i
}

func (i *flexibleQueryScope) Allowed(index IndexName, field FieldName, _ ViewName, shard ShardID) bool {
	if i.all {
		return true
	}
	if i.splitter != nil {
		// unsplit index: we can't have stored fields so we don't check them
		if _, ok := i.splitter.splitIndexes[index]; !ok {
			if shards, ok := i.indexes[index]; ok {
				return shards.all.Allowed(shard)
			}
			return false
		}
	}
	scope := i.indexes[index]
	if scope == nil {
		return false
	}
	// split index: check the index first, in case it's set, but don't fail if
	// it's not, because it might be in fields
	if scope.all.Allowed(shard) {
		return true
	}
	// why not just call Allowed on it directly? because it's a pointer-receiver
	// method and map entries aren't addressable.
	shards := scope.any[field]
	return shards.Allowed(shard)
}

func (i *flexibleQueryScope) Overlap(qw QueryScope) (out bool) {
	// this panics if the other isn't also an flexibleQueryScope.
	// don't mix and match.
	other := qw.(*flexibleQueryScope)
	// overlap occurs if there's an overlap of indexes, or of fields.
	// We compare indexes against the other side's corresponding fields,
	// and fields against the other side's corresponding indexes.
	if (i.all && len(other.indexes) > 0) || (other.all && len(i.indexes) > 0) {
		return true
	}
	for index, scope := range i.indexes {
		// if the other has this index as an unsplit index, overlap
		// there counts
		if otherScope, ok := other.indexes[index]; ok {
			if scope.Overlap(otherScope) {
				return true
			}
		}
	}
	return false
}

// shardList is a set of shards which can be either every shard
// or a provided list of shards. It should probably be named
// shardSet but we have one of those already that is for reasons
// not a good fit.
type shardList struct {
	all bool
	any []ShardID
}

// findShard returns the positive index at which shard was found
// in the shard list, or the negative index at which it would have
// been (and thus the insertion point for an add).
func (s *shardList) findShard(shard ShardID) int {
	l, h := 0, len(s.any)
	for h > l {
		m := (h + l) / 2
		if s.any[m] == shard {
			return m
		}
		if s.any[m] < shard {
			l = m + 1
		} else {
			h = m
		}
	}
	// in the single-item list case, if we're below the single item,
	// we ended with {l=h=0}, and if we're above it, we ended with
	// {l=h=1}. we want to return a negative value for all misses,
	// so we return -h -1. we could also use l. we couldn't use m,
	// because in the "above the single item" case, m was still 0
	// when we left the loop.
	return -h - 1
}

// Allowed indicates whether the given shard is currently included
// in the set.
func (s *shardList) Allowed(shard ShardID) bool {
	if s.all {
		return true
	}
	pos := s.findShard(shard)
	return pos >= 0
}

// Overlap determines whether two shard lists overlap.
func (s *shardList) Overlap(other shardList) bool {
	if s.all || other.all {
		return true
	}
	// an empty shard list doesn't overlap
	if len(s.any) == 0 || len(other.any) == 0 {
		return false
	}
	ours := s.any
	theirs := other.any
	o := 0
	for _, shard := range ours {
		for o < len(theirs) && theirs[o] < shard {
			o++
		}
		if o >= len(theirs) {
			return false
		}
		if theirs[o] == shard {
			return true
		}
	}
	return false
}

// Add adds the given shard to the shardlist, maintaining
// sorted order.
func (s *shardList) Add(shard ShardID) {
	if s.all {
		return
	}
	// short circuit for empty lists or the case where the
	// new item is the largest, so sorted lists are O(n)
	// instead of O(n log n).
	if len(s.any) == 0 {
		s.any = []ShardID{shard}
		return
	}
	if s.any[len(s.any)-1] < shard {
		s.any = append(s.any, shard)
		return
	}
	pos := s.findShard(shard)
	if pos >= 0 {
		return
	}
	// -1 -> 0, etc
	pos = -pos - 1
	s.any = append(s.any, 0)
	copy(s.any[pos+1:], s.any[pos:])
	s.any[pos] = shard
}
