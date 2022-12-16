package pilosa

import (
	"path/filepath"
	"testing"

	"github.com/featurebasedb/featurebase/v3/roaring"
	bolt "go.etcd.io/bbolt"
)

func TestGetFreeID(t *testing.T) {
	boltDir := t.TempDir()
	db, err := bolt.Open(filepath.Join(boltDir, "testDB"), 0600, nil)
	if err != nil {
		t.Fatalf("unexpected error opening test boltdb: %v", err)
	}
	defer db.Close()

	makeTestBucket := func(tx *bolt.Tx, b *roaring.Bitmap) *bolt.Bucket {
		if b == nil {
			t.Fatalf("unexpected nil bitmap")
		}
		free, err := tx.CreateBucketIfNotExists(bucketFree)
		if err != nil {
			t.Fatalf("unexpected error making freeBucket: %v", err)
		}
		buf, err := b.MarshalBinary()
		if err != nil {
			t.Fatalf("unexpected error marshaling bitmap (%v) to binary: %v", b, err)
		}
		if err := free.Put(freeKey, buf); err != nil {
			t.Fatalf("unexpected error adding data (%v) to freeBucket: %v", b, err)
		}
		return free
	}

	for name, test := range map[string]struct {
		bits *roaring.Bitmap
		want uint64
	}{
		"bucket is there, but nobody's home": {
			bits: roaring.NewBitmap(),
			want: 0,
		},
		"good bucket": {
			bits: roaring.NewBitmap(1, 2, 34, 55, 9000),
			want: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			tx, err := db.Begin(true)
			if err != nil {
				t.Fatalf("unexpected error starting bolt transaction: %v", err)
			}
			defer tx.Rollback()
			freeBucket := makeTestBucket(tx, test.bits)

			getter := newFreeIDGetter(freeBucket)
			defer getter.Close()
			if got := getter.GetFreeID(); got != test.want {
				t.Fatalf("expected %v got %v", test.want, got)
			}
		})
	}

	t.Run("CorrectOrdering", func(t *testing.T) {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatalf("unexpected error starting bolt transaction: %v", err)
		}
		defer tx.Rollback()

		bucket := makeTestBucket(tx, roaring.NewBitmap(1, 34, 2, 55, 9000))

		getter := newFreeIDGetter(bucket)
		defer getter.Close()
		for _, want := range []uint64{1, 2, 34, 55, 9000} {
			if got := getter.GetFreeID(); got != want {
				t.Fatalf("expected %v got %v", want, got)
			}
		}
		if got := getter.GetFreeID(); got != 0 {
			t.Fatalf("expected 0 got %v", got)
		}
	})

	t.Run("NotABitmap", func(t *testing.T) {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatalf("unexpected error starting bolt transaction: %v", err)
		}
		defer tx.Rollback()

		free, err := tx.CreateBucketIfNotExists(bucketFree)
		if err != nil {
			t.Fatalf("unexpected error making freeBucket: %v", err)
		}
		if err := free.Put(freeKey, []byte("this isn't right!")); err != nil {
			t.Fatalf("unexpected error adding data to freeBucket: %v", err)
		}
		getter := newFreeIDGetter(free)
		defer getter.Close()
		if got := getter.GetFreeID(); got != 0 {
			t.Fatalf("expected 0 got %v", got)
		}
	})
}
