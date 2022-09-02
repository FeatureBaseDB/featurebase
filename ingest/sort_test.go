// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ingest

import (
	"testing"
)

const sampleSize = 1000000

var sampleSortingData = createSampleFieldData()

func createSampleFieldData() *FieldOperation {
	fo := &FieldOperation{}
	fo.RecordIDs = make([]uint64, sampleSize)
	fo.Values = make([]uint64, sampleSize)
	fo.Signed = make([]int64, sampleSize)
	for i := range fo.RecordIDs {
		fo.RecordIDs[i] = (uint64(i) * 63 * 3456789) % 50000000
		fo.Values[i] = (uint64(i) * 6) % 8
		fo.Signed[i] = ((int64(i) * 17) % 15) - 8
	}
	return fo
}

func benchmarkOneSort(b *testing.B, fo *FieldOperation) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sortable := fo.clone()
		b.StartTimer()
		_ = sortable.SortToShards()
	}
}

func BenchmarkSortFieldOp(b *testing.B) {
	b.Run("full", func(b *testing.B) {
		f2 := *sampleSortingData
		benchmarkOneSort(b, &f2)
	})
	b.Run("nosign", func(b *testing.B) {
		f2 := *sampleSortingData
		f2.Signed = nil
		benchmarkOneSort(b, &f2)
	})
	b.Run("signonly", func(b *testing.B) {
		f2 := *sampleSortingData
		f2.Values = nil
		benchmarkOneSort(b, &f2)
	})
}

// func BenchmarkSort64(b *testing.B) {
// 	gen := func(n, width uint64) func() []uint64 {
// 		var data []uint64
// 		var once sync.Once
// 		return func() []uint64 {
// 			once.Do(func() {
// 				data = make([]uint64, n)
// 				var rng rand.PCGSource
// 				rng.Seed(9001)
// 				for i := range data {
// 					data[i] = rng.Uint64() % width
// 				}
// 			})
//
// 			return data
// 		}
// 	}
//
// 	algos := []struct {
// 		name string
// 		maxn uint64
// 		fn   func([]uint64) []uint64
// 	}{
// 		{
// 			name: "stdlib",
// 			maxn: 1024 * 1024 * 1024,
// 			fn:   stdSort,
// 		},
// 		{
// 			name: "heap",
// 			maxn: 1024 * 1024 * 1024,
// 			fn:   heapSort,
// 		},
// 		{
// 			name: "radix-insertion-mask",
// 			maxn: 1024 * 1024 * 1024,
// 			fn:   dedupSort64,
// 		},
// 	}
//
// 	widths := []struct {
// 		name  string
// 		width uint64
// 	}{
// 		{"64", 64},
// 		{"1K", 1024},
// 		{"64K", 64 * 1024},
// 		{"1M", 1024 * 1024},
// 		{"16M", 16 * 1024 * 1024},
// 		{"128M", 128 * 1024 * 1024},
// 		{"1B", 1024 * 1024 * 1024},
// 	}
//
// 	counts := []struct {
// 		name string
// 		n    uint64
// 	}{
// 		{"64", 64},
// 		{"1K", 1024},
// 		{"64K", 64 * 1024},
// 		{"1M", 1024 * 1024},
// 		{"4M", 4 * 1024 * 1024},
// 		{"16M", 16 * 1024 * 1024},
// 		{"64M", 64 * 1024 * 1024},
// 		{"256M", 256 * 1024 * 1024},
// 	}
//
// 	for _, width := range widths {
// 		width := width
// 		b.Run(width.name, func(b *testing.B) {
// 			for _, count := range counts {
// 				if count.n > width.width {
// 					continue
// 				}
//
// 				count := count
// 				b.Run(count.name, func(b *testing.B) {
// 					datasrc := gen(count.n, width.width)
// 					for _, alg := range algos {
// 						if count.n > alg.maxn {
// 							continue
// 						}
//
// 						alg := alg
// 						b.Run(alg.name, func(b *testing.B) {
// 							data := datasrc()
// 							buf := make([]uint64, len(data))
// 							b.SetBytes(8 * int64(len(buf)))
//
// 							b.StopTimer()
// 							b.ResetTimer()
//
// 							for i := 0; i < b.N; i++ {
// 								copy(buf, data)
// 								b.StartTimer()
// 								alg.fn(buf)
// 								b.StopTimer()
// 							}
// 						})
// 					}
// 				})
// 			}
// 		})
// 	}
// }
//
// func heapSort(data []uint64) []uint64 {
// 	for i, v := range data {
// 		for i > 0 && v > data[(i-1)/2] {
// 			data[i] = data[(i-1)/2]
// 			i = (i - 1) / 2
// 		}
// 		data[i] = v
// 	}
// 	{
// 		heap := data
// 		for len(heap) > 1 {
// 			heap[0], heap[len(heap)-1] = heap[len(heap)-1], heap[0]
// 			heap = heap[:len(heap)-1]
// 			i := 0
// 			for {
// 				max := i
// 				if r := 2*i + 1; r < len(heap) && heap[r] > heap[max] {
// 					max = r
// 				}
// 				if l := 2*i + 2; l < len(heap) && heap[l] > heap[max] {
// 					max = l
// 				}
// 				if max == i {
// 					break
// 				}
//
// 				heap[max], heap[i] = heap[i], heap[max]
// 				i = max
// 			}
// 		}
// 	}
//
// 	j := 1
// 	prev := data[0]
// 	for _, v := range data[1:] {
// 		if v == prev {
// 			continue
// 		}
//
// 		data[j] = v
// 		prev = v
// 	}
//
// 	return data[:j]
// }
//
// func stdSort(data []uint64) []uint64 {
// 	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })
//
// 	j := 1
// 	prev := data[0]
// 	for _, v := range data[1:] {
// 		if v == prev {
// 			continue
// 		}
//
// 		data[j] = v
// 		prev = v
// 	}
//
// 	return data[:j]
// }
