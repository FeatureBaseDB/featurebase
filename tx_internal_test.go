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

package pilosa

import (
	"bytes"
	"sync"
	"testing"

	"github.com/pilosa/pilosa/v2/roaring"
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
			arraySample[i] = uint16(i)
		}
		for i := 0; i < 4096/64; i++ {
			bitmapSample[i] = ^uint64(0)
		}
		bm := roaring.NewSliceBitmap()
		for n := 0; n < 4096 && n < countRangeMaxN; n++ {
			c := roaring.NewContainerArray(arraySample[:n])
			bm.Put(uint64(n), c)
		}
		for n := 4096; n < countRangeMaxN; n++ {
			c := roaring.NewContainerBitmapN(bitmapSample[:], int32(n))
			bm.Put(uint64(n), c)
			bitmapSample[n/64] |= 1 << (n % 64)
		}
		var asBytes bytes.Buffer
		n, err := bm.WriteTo(&asBytes)
		if err != nil {
			tb.Fatalf("writing bitmap: %v", err)
		}
		countRangeSampleData = asBytes.Bytes()
		tb.Logf("creating bitmap: %d containers, %d bytes of data", countRangeMaxN, n)
	})
	f, idx, tx := mustOpenFragment(tb, "i", "f", viewStandard, 0, "")
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

	expected := uint64(0)
	j := uint64(0)
	for i := uint64(0); i < countRangeMaxN; i += 7 {
		if i%4 == 3 {
			expected -= (j * 7) + 21
			j += 7
		}
		got, err := tx.CountRange("i", "f", viewStandard, 0, uint64(j)<<16, uint64(i)<<16)
		if err != nil {
			t.Fatalf("counting range: %v", err)
		}
		if got != expected {
			t.Fatalf("counting from container %d to %d, expected %d, got %d",
				j, i, expected, got)
		}
		expected += (i * 7) + 21
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
			got, err := tx.CountRange("i", "f", viewStandard, 0, uint64(j)<<16, uint64(i)<<16)
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
