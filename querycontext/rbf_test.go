package querycontext

import (
	"context"
	"testing"

	"github.com/molecula/featurebase/v3/roaring"
)

func TestRbfWrite(t *testing.T) {
	ctx := context.Background()
	txs := testTxStore(t, "foo", nil)
	q, _ := txs.NewWriteQueryContext(ctx, txs.Scope().AddIndex("i"))
	wr, _ := q.Write("i", "f", "v", 0)
	_, _ = wr.Add(23, 25)
	_, _ = wr.Remove(25)
	// putting nil and removing a missing container are no-ops
	_ = wr.PutContainer(2, nil)
	_ = wr.RemoveContainer(3)
	citer, _, _ := wr.ContainerIterator(3)
	if citer != nil {
		citer.Close()
	}
	_, _ = wr.Container(7)
	_, _ = wr.Count()
	_, _ = wr.Max()
	_, _, _ = wr.Min()
	_, _ = wr.CountRange(0, 65535)
	_, _ = wr.RoaringBitmap()
	_, _ = wr.OffsetRange(0, 0, 0)
	filter := roaring.NewBitmapColumnFilter(23)
	_ = wr.ApplyFilter(0, filter)
	// we're not testing ApplyRewriter and ImportRoaringBits because
	// they have a lot more setup to be testable, and in practice
	// they're all one-line functions anyway.
	_ = q.Commit()
	q, _ = txs.NewQueryContext(ctx)
	rd, _ := q.Read("i", "f", "v", 0)
	ok, _ := rd.Contains(23)
	if !ok {
		t.Fatalf("no 23")
	}
	txs.expect(1)
	err := txs.Close()
	if err == nil {
		t.Fatalf("shouldn't close a database while still open")
	}
	q.Release()
}
