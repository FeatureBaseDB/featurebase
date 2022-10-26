package querycontext

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/molecula/featurebase/v3/roaring"
)

type oopsieWrapper struct {
	tb       testing.TB
	expected int64
}

func (o *oopsieWrapper) expect(n int) {
	atomic.AddInt64(&o.expected, int64(n))
}

func (o *oopsieWrapper) oopsie(err error) error {
	o.tb.Helper()
	if err == nil {
		return nil
	}
	remaining := atomic.AddInt64(&o.expected, -1)
	if remaining < 0 {
		// we use Error here, not Fatal, so we can run in arbitrary goroutines
		o.tb.Error(err)
	}
	return err
}

// testTxStore is a wrapper which fails a test on unexpected
// errors.
type testTxStoreWrapper struct {
	inner TxStore
	oopsieWrapper
}

func newTestTxStoreWrapper(tb testing.TB, inner TxStore) *testTxStoreWrapper {
	return &testTxStoreWrapper{inner: inner, oopsieWrapper: oopsieWrapper{tb: tb}}
}

func (t *testTxStoreWrapper) Close() error {
	t.tb.Helper()
	return t.oopsie(t.inner.Close())
}

func (t *testTxStoreWrapper) NewQueryContext(ctx context.Context) (*testQueryContextWrapper, error) {
	t.tb.Helper()
	q, err := t.inner.NewQueryContext(ctx)
	return newTestQueryContextWrapper(t.tb, q), t.oopsie(err)
}

func (t *testTxStoreWrapper) NewWriteQueryContext(ctx context.Context, scope QueryScope) (*testQueryContextWrapper, error) {
	t.tb.Helper()
	q, err := t.inner.NewWriteQueryContext(ctx, scope)
	return newTestQueryContextWrapper(t.tb, q), t.oopsie(err)
}

func (t *testTxStoreWrapper) Scope() QueryScope {
	t.tb.Helper()
	return t.inner.Scope()
}

func (t *testTxStoreWrapper) dbPath(dbk dbKey) (string, error) {
	t.tb.Helper()
	p, err := t.inner.dbPath(dbk)
	return p, t.oopsie(err)
}

func (t *testTxStoreWrapper) keys(index IndexName, field FieldName, view ViewName, shard ShardID) (dbKey, fragKey) {
	t.tb.Helper()
	return t.inner.keys(index, field, view, shard)
}

type testQueryContextWrapper struct {
	inner QueryContext
	oopsieWrapper
}

func newTestQueryContextWrapper(tb testing.TB, inner QueryContext) *testQueryContextWrapper {
	return &testQueryContextWrapper{inner: inner, oopsieWrapper: oopsieWrapper{tb: tb}}
}

func (t *testQueryContextWrapper) Release() {
	t.tb.Helper()
	t.inner.Release()
}

func (t *testQueryContextWrapper) Commit() error {
	t.tb.Helper()
	return t.oopsie(t.inner.Commit())
}

func (t *testQueryContextWrapper) Error(args ...interface{}) {
	t.tb.Helper()
	t.inner.Error(args...)
}

func (t *testQueryContextWrapper) Errorf(msg string, args ...interface{}) {
	t.tb.Helper()
	t.inner.Errorf(msg, args...)
}

func (t *testQueryContextWrapper) NewRead(index IndexName, field FieldName, view ViewName, shard ShardID) (*testQueryReadWrapper, error) {
	t.tb.Helper()
	qr, err := t.inner.NewRead(index, field, view, shard)
	return newTestQueryReadWrapper(t.tb, qr), t.oopsie(err)
}

func (t *testQueryContextWrapper) NewWrite(index IndexName, field FieldName, view ViewName, shard ShardID) (*testQueryWriteWrapper, error) {
	t.tb.Helper()
	qw, err := t.inner.NewWrite(index, field, view, shard)
	return newTestQueryWriteWrapper(t.tb, qw), t.oopsie(err)
}

type testQueryReadWrapper struct {
	inner QueryRead
	oopsieWrapper
}

func newTestQueryReadWrapper(tb testing.TB, inner QueryRead) *testQueryReadWrapper {
	return &testQueryReadWrapper{inner: inner, oopsieWrapper: oopsieWrapper{tb: tb}}
}

func (t *testQueryReadWrapper) ContainerIterator(ckey uint64) (citer roaring.ContainerIterator, found bool, err error) {
	t.tb.Helper()
	citer, found, err = t.inner.ContainerIterator(ckey)
	return citer, found, t.oopsie(err)
}

func (t *testQueryReadWrapper) ApplyFilter(ckey uint64, filter roaring.BitmapFilter) (err error) {
	t.tb.Helper()
	return t.oopsie(t.inner.ApplyFilter(ckey, filter))
}

func (t *testQueryReadWrapper) Container(ckey uint64) (*roaring.Container, error) {
	t.tb.Helper()
	c, err := t.inner.Container(ckey)
	return c, t.oopsie(err)
}

func (t *testQueryReadWrapper) Contains(v uint64) (exists bool, err error) {
	t.tb.Helper()
	exists, err = t.inner.Contains(v)
	return exists, t.oopsie(err)
}

func (t *testQueryReadWrapper) Count() (uint64, error) {
	t.tb.Helper()
	c, err := t.inner.Count()
	return c, t.oopsie(err)
}

func (t *testQueryReadWrapper) Max() (uint64, error) {
	t.tb.Helper()
	m, err := t.inner.Max()
	return m, t.oopsie(err)
}

func (t *testQueryReadWrapper) Min() (uint64, bool, error) {
	t.tb.Helper()
	m, ok, err := t.inner.Min()
	return m, ok, t.oopsie(err)
}

func (t *testQueryReadWrapper) CountRange(start, end uint64) (uint64, error) {
	t.tb.Helper()
	c, err := t.inner.CountRange(start, end)
	return c, t.oopsie(err)
}

func (t *testQueryReadWrapper) OffsetRange(offset, start, end uint64) (*roaring.Bitmap, error) {
	t.tb.Helper()
	b, err := t.inner.OffsetRange(offset, start, end)
	return b, t.oopsie(err)
}

func (t *testQueryReadWrapper) RoaringBitmap() (*roaring.Bitmap, error) {
	t.tb.Helper()
	b, err := t.inner.RoaringBitmap()
	return b, t.oopsie(err)
}

type testQueryWriteWrapper struct {
	inner QueryWrite
	oopsieWrapper
}

func newTestQueryWriteWrapper(tb testing.TB, inner QueryWrite) *testQueryWriteWrapper {
	return &testQueryWriteWrapper{inner: inner, oopsieWrapper: oopsieWrapper{tb: tb}}
}

func (t *testQueryWriteWrapper) PutContainer(ckey uint64, c *roaring.Container) error {
	t.tb.Helper()
	return t.oopsie(t.inner.PutContainer(ckey, c))
}

func (t *testQueryWriteWrapper) RemoveContainer(ckey uint64) error {
	t.tb.Helper()
	return t.oopsie(t.inner.RemoveContainer(ckey))
}

func (t *testQueryWriteWrapper) Add(a ...uint64) (changeCount int, err error) {
	c, err := t.inner.Add(a...)
	t.tb.Helper()
	return c, t.oopsie(err)
}

func (t *testQueryWriteWrapper) Remove(a ...uint64) (changeCount int, err error) {
	c, err := t.inner.Remove(a...)
	t.tb.Helper()
	return c, t.oopsie(err)
}

func (t *testQueryWriteWrapper) ApplyRewriter(ckey uint64, filter roaring.BitmapRewriter) (err error) {
	t.tb.Helper()
	return t.oopsie(t.inner.ApplyRewriter(ckey, filter))
}

func (t *testQueryWriteWrapper) ImportRoaringBits(rit roaring.RoaringIterator, clear bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	c, r, err := t.inner.ImportRoaringBits(rit, clear, rowSize)
	t.tb.Helper()
	return c, r, t.oopsie(err)
}

// and we duplicate the QueryRead methods, because it's messy to try to embed a QueryRead wrapper
// in the QueryWrite wrapper and keep them sharing a single oopsieWrapper.

func (t *testQueryWriteWrapper) ContainerIterator(ckey uint64) (citer roaring.ContainerIterator, found bool, err error) {
	citer, found, err = t.inner.ContainerIterator(ckey)
	t.tb.Helper()
	return citer, found, t.oopsie(err)
}

func (t *testQueryWriteWrapper) ApplyFilter(ckey uint64, filter roaring.BitmapFilter) (err error) {
	t.tb.Helper()
	return t.oopsie(t.inner.ApplyFilter(ckey, filter))
}

func (t *testQueryWriteWrapper) Container(ckey uint64) (*roaring.Container, error) {
	c, err := t.inner.Container(ckey)
	t.tb.Helper()
	return c, t.oopsie(err)
}

func (t *testQueryWriteWrapper) Contains(v uint64) (exists bool, err error) {
	exists, err = t.inner.Contains(v)
	t.tb.Helper()
	return exists, t.oopsie(err)
}

func (t *testQueryWriteWrapper) Count() (uint64, error) {
	c, err := t.inner.Count()
	t.tb.Helper()
	return c, t.oopsie(err)
}

func (t *testQueryWriteWrapper) Max() (uint64, error) {
	m, err := t.inner.Max()
	t.tb.Helper()
	return m, t.oopsie(err)
}

func (t *testQueryWriteWrapper) Min() (uint64, bool, error) {
	m, ok, err := t.inner.Min()
	t.tb.Helper()
	return m, ok, t.oopsie(err)
}

func (t *testQueryWriteWrapper) CountRange(start, end uint64) (uint64, error) {
	c, err := t.inner.CountRange(start, end)
	t.tb.Helper()
	return c, t.oopsie(err)
}

func (t *testQueryWriteWrapper) OffsetRange(offset, start, end uint64) (*roaring.Bitmap, error) {
	b, err := t.inner.OffsetRange(offset, start, end)
	t.tb.Helper()
	return b, t.oopsie(err)
}

func (t *testQueryWriteWrapper) RoaringBitmap() (*roaring.Bitmap, error) {
	t.tb.Helper()
	b, err := t.inner.RoaringBitmap()
	return b, t.oopsie(err)
}
