// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"bytes"
	"sync"
	"testing"

	"github.com/molecula/featurebase/v3/roaring"
)

const countRangeMaxN = 8192

var countRangeSampleData []byte
var prepareCountRangeSampleData sync.Once

// The sample data for the counter is just a series of containers,
// each with cardinality equal to its container key.
func requireCountRangeSampleData(tb testing.TB) (*fragment, Tx) {
	prepareCountRangeSampleData.Do(func() {
		var arraySample [4096]uint16
		// This horrible hack relies on a quirk of roaring's internals: It'll
		// copy the bitmap if its length isn't exactly 1024. This lets us
		// request that each container get its own copy of the bitmap.
		var bitmapSample [1025]uint64
		for i := range arraySample {
			arraySample[i] = uint16(i * 2)
		}
		// Put corresponding bits in the bitmap...
		for i := 0; i < 4096/32; i++ {
			// bit 0 is 0x1, bit 2 is 0x4, so even-numbered bits
			// are 0x5555....
			bitmapSample[i] = 0x5555555555555555
		}
		bm := roaring.NewSliceBitmap()
		for n := 0; n < 4096 && n < countRangeMaxN; n++ {
			c := roaring.NewContainerArray(arraySample[:n])
			bm.Put(uint64(n), c)
		}
		// Start filling in the missing bits. This starts us out with
		// bitmap containers, but then eventually converts to things
		// that are more likely to be run containers. At the end of this,
		// we should have exactly the first 8,192 bits set, for a single
		// run of 8k.
		for n := 4096; n < 8192; n++ {
			c := roaring.NewContainerBitmapN(bitmapSample[:], int32(n))
			bm.Put(uint64(n), c)
			w := n - 4096
			bitmapSample[w/32] |= 1 << (((n % 32) * 2) + 1)
		}
		var asBytes bytes.Buffer
		n, err := bm.WriteTo(&asBytes)
		if err != nil {
			tb.Fatalf("writing bitmap: %v", err)
		}
		countRangeSampleData = asBytes.Bytes()
		tb.Logf("creating bitmap: %d containers, %d bytes of data", countRangeMaxN, n)
	})
	f, idx, tx := mustOpenFragment(tb)
	// Properly close this transaction, but not the next one we create that the
	// caller will be responsible for. The deferred callback will
	// be a nop if the Commit happened.
	defer tx.Rollback()
	err := f.importRoaringT(tx, countRangeSampleData, false)
	if err != nil {
		tb.Fatalf("importing sample data: %v", err)
	}
	err = tx.Commit()
	if err != nil {
		tb.Fatalf("committing sample data: %v", err)
	}
	tx = idx.holder.txf.NewTx(Txo{Write: false, Index: idx, Fragment: f, Shard: 0})
	return f, tx
}

func TestTx_CountRange(t *testing.T) {
	f, tx := requireCountRangeSampleData(t)
	defer f.Clean(t)
	defer tx.Rollback()
	// CountRange accesses the fragment without locking. Normally we only
	// call it from inside a fragment routine with locking. Otherwise, you
	// can have a race condition with snapshots, for instance.
	f.mu.Lock()
	defer f.mu.Unlock()

	expected := uint64(0)
	j := uint64(0)
	for i := uint64(0); i < countRangeMaxN; i += 7 {
		expected += i
		if i%4 == 3 {
			expected -= (j * 7) + 21
			j += 7
		}
		// Every other bit gets set, for a total of i bits in container
		// i, so they're all in the first (i*2) bits of the container.
		got, err := tx.CountRange("i", "f", "v", 0, uint64(j)<<16, (uint64(i)<<16)+(i*2))
		if err != nil {
			t.Fatalf("counting range: %v", err)
		}
		if got != expected {
			t.Fatalf("counting from container %d to %d, expected %d, got %d",
				j, i, expected, got)
		}
		// The -i here undoes the +i at the top of this loop.
		expected += (i * 7) + 21 - i
	}
}

func BenchmarkTx_CountRange(b *testing.B) {
	f, tx := requireCountRangeSampleData(b)
	defer f.Clean(b)
	defer tx.Rollback()

	for k := 0; k < b.N; k++ {
		expected := uint64(0)
		j := uint64(0)
		for i := uint64(0); i < countRangeMaxN; i += 7 {
			if i%4 == 3 {
				expected -= (j * 7) + 21
				j += 7
			}
			got, err := tx.CountRange("i", "f", "v", 0, uint64(j)<<16, uint64(i)<<16)
			if err != nil {
				b.Fatalf("counting range: %v", err)
			}
			if got != expected {
				b.Fatalf("counting from container %d to %d, expected %d, got %d",
					j, i, expected, got)
			}
			expected += (i * 7) + 21
		}
	}
}
