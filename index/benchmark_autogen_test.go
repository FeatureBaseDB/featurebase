package index

import (
	"math/rand"
	"testing"
)

const (
	c1 uint32 = 0xcc9e2d51
	c2 uint32 = 0x1b873593
)

func benchmarkDifferentCombinations(b *testing.B, op string, b1, b2 int, s1, s2 int) {
	m1 := NewBitmap()
	m2 := NewBitmap()

	bit := uint64(0)
	rand.Seed(int64(c1))
	for i := 0; i < b1; i++ {
		bit += uint64(rand.Intn(s1) + 1)
		SetBit(m1, bit)
	}

	bit = 0
	rand.Seed(int64(c2))
	for i := 0; i < b2; i++ {
		bit += uint64(rand.Intn(s1) + 1)
		SetBit(m2, bit)
	}

	var f func(a_bm IBitmap, b_bm IBitmap) IBitmap
	switch op {
	case "and":
		f = Intersection
	case "or":
		f = Union
	case "diff":
		f = Difference
	default:
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if f(m1, m2) == nil {
			b.Fatal("Problem with %s benchmark at i =", op, i)
		}
	}
}
func Benchmark_and_64_64_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 3, 3)
}

func Benchmark_and_64_64_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 3, 30)
}

func Benchmark_and_64_64_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 3, 300)
}

func Benchmark_and_64_64_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 3, 3000)
}

func Benchmark_and_64_64_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 3, 30000)
}

func Benchmark_and_64_64_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 30, 3)
}

func Benchmark_and_64_64_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 30, 30)
}

func Benchmark_and_64_64_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 30, 300)
}

func Benchmark_and_64_64_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 30, 3000)
}

func Benchmark_and_64_64_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 30, 30000)
}

func Benchmark_and_64_64_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 300, 3)
}

func Benchmark_and_64_64_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 300, 30)
}

func Benchmark_and_64_64_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 300, 300)
}

func Benchmark_and_64_64_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 300, 3000)
}

func Benchmark_and_64_64_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 300, 30000)
}

func Benchmark_and_64_64_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 3000, 3)
}

func Benchmark_and_64_64_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 3000, 30)
}

func Benchmark_and_64_64_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 3000, 300)
}

func Benchmark_and_64_64_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 3000, 3000)
}

func Benchmark_and_64_64_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 3000, 30000)
}

func Benchmark_and_64_64_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 30000, 3)
}

func Benchmark_and_64_64_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 30000, 30)
}

func Benchmark_and_64_64_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 30000, 300)
}

func Benchmark_and_64_64_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 30000, 3000)
}

func Benchmark_and_64_64_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 30000, 30000)
}

func Benchmark_and_64_1024_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 3, 3)
}

func Benchmark_and_64_1024_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 3, 30)
}

func Benchmark_and_64_1024_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 3, 300)
}

func Benchmark_and_64_1024_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 3, 3000)
}

func Benchmark_and_64_1024_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 3, 30000)
}

func Benchmark_and_64_1024_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 30, 3)
}

func Benchmark_and_64_1024_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 30, 30)
}

func Benchmark_and_64_1024_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 30, 300)
}

func Benchmark_and_64_1024_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 30, 3000)
}

func Benchmark_and_64_1024_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 30, 30000)
}

func Benchmark_and_64_1024_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 300, 3)
}

func Benchmark_and_64_1024_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 300, 30)
}

func Benchmark_and_64_1024_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 300, 300)
}

func Benchmark_and_64_1024_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 300, 3000)
}

func Benchmark_and_64_1024_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 300, 30000)
}

func Benchmark_and_64_1024_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 3000, 3)
}

func Benchmark_and_64_1024_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 3000, 30)
}

func Benchmark_and_64_1024_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 3000, 300)
}

func Benchmark_and_64_1024_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 3000, 3000)
}

func Benchmark_and_64_1024_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 3000, 30000)
}

func Benchmark_and_64_1024_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 30000, 3)
}

func Benchmark_and_64_1024_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 30000, 30)
}

func Benchmark_and_64_1024_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 30000, 300)
}

func Benchmark_and_64_1024_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 30000, 3000)
}

func Benchmark_and_64_1024_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 30000, 30000)
}

func Benchmark_and_64_65536_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 3, 3)
}

func Benchmark_and_64_65536_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 3, 30)
}

func Benchmark_and_64_65536_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 3, 300)
}

func Benchmark_and_64_65536_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 3, 3000)
}

func Benchmark_and_64_65536_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 3, 30000)
}

func Benchmark_and_64_65536_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 30, 3)
}

func Benchmark_and_64_65536_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 30, 30)
}

func Benchmark_and_64_65536_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 30, 300)
}

func Benchmark_and_64_65536_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 30, 3000)
}

func Benchmark_and_64_65536_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 30, 30000)
}

func Benchmark_and_64_65536_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 300, 3)
}

func Benchmark_and_64_65536_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 300, 30)
}

func Benchmark_and_64_65536_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 300, 300)
}

func Benchmark_and_64_65536_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 300, 3000)
}

func Benchmark_and_64_65536_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 300, 30000)
}

func Benchmark_and_64_65536_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 3000, 3)
}

func Benchmark_and_64_65536_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 3000, 30)
}

func Benchmark_and_64_65536_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 3000, 300)
}

func Benchmark_and_64_65536_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 3000, 3000)
}

func Benchmark_and_64_65536_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 3000, 30000)
}

func Benchmark_and_64_65536_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 30000, 3)
}

func Benchmark_and_64_65536_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 30000, 30)
}

func Benchmark_and_64_65536_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 30000, 300)
}

func Benchmark_and_64_65536_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 30000, 3000)
}

func Benchmark_and_64_65536_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 30000, 30000)
}

func Benchmark_and_1024_64_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 3, 3)
}

func Benchmark_and_1024_64_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 3, 30)
}

func Benchmark_and_1024_64_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 3, 300)
}

func Benchmark_and_1024_64_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 3, 3000)
}

func Benchmark_and_1024_64_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 3, 30000)
}

func Benchmark_and_1024_64_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 30, 3)
}

func Benchmark_and_1024_64_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 30, 30)
}

func Benchmark_and_1024_64_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 30, 300)
}

func Benchmark_and_1024_64_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 30, 3000)
}

func Benchmark_and_1024_64_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 30, 30000)
}

func Benchmark_and_1024_64_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 300, 3)
}

func Benchmark_and_1024_64_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 300, 30)
}

func Benchmark_and_1024_64_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 300, 300)
}

func Benchmark_and_1024_64_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 300, 3000)
}

func Benchmark_and_1024_64_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 300, 30000)
}

func Benchmark_and_1024_64_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 3000, 3)
}

func Benchmark_and_1024_64_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 3000, 30)
}

func Benchmark_and_1024_64_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 3000, 300)
}

func Benchmark_and_1024_64_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 3000, 3000)
}

func Benchmark_and_1024_64_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 3000, 30000)
}

func Benchmark_and_1024_64_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 30000, 3)
}

func Benchmark_and_1024_64_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 30000, 30)
}

func Benchmark_and_1024_64_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 30000, 300)
}

func Benchmark_and_1024_64_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 30000, 3000)
}

func Benchmark_and_1024_64_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 30000, 30000)
}

func Benchmark_and_1024_1024_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 3, 3)
}

func Benchmark_and_1024_1024_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 3, 30)
}

func Benchmark_and_1024_1024_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 3, 300)
}

func Benchmark_and_1024_1024_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 3, 3000)
}

func Benchmark_and_1024_1024_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 3, 30000)
}

func Benchmark_and_1024_1024_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 30, 3)
}

func Benchmark_and_1024_1024_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 30, 30)
}

func Benchmark_and_1024_1024_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 30, 300)
}

func Benchmark_and_1024_1024_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 30, 3000)
}

func Benchmark_and_1024_1024_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 30, 30000)
}

func Benchmark_and_1024_1024_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 300, 3)
}

func Benchmark_and_1024_1024_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 300, 30)
}

func Benchmark_and_1024_1024_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 300, 300)
}

func Benchmark_and_1024_1024_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 300, 3000)
}

func Benchmark_and_1024_1024_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 300, 30000)
}

func Benchmark_and_1024_1024_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 3000, 3)
}

func Benchmark_and_1024_1024_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 3000, 30)
}

func Benchmark_and_1024_1024_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 3000, 300)
}

func Benchmark_and_1024_1024_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 3000, 3000)
}

func Benchmark_and_1024_1024_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 3000, 30000)
}

func Benchmark_and_1024_1024_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 30000, 3)
}

func Benchmark_and_1024_1024_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 30000, 30)
}

func Benchmark_and_1024_1024_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 30000, 300)
}

func Benchmark_and_1024_1024_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 30000, 3000)
}

func Benchmark_and_1024_1024_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 30000, 30000)
}

func Benchmark_and_1024_65536_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 3, 3)
}

func Benchmark_and_1024_65536_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 3, 30)
}

func Benchmark_and_1024_65536_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 3, 300)
}

func Benchmark_and_1024_65536_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 3, 3000)
}

func Benchmark_and_1024_65536_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 3, 30000)
}

func Benchmark_and_1024_65536_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 30, 3)
}

func Benchmark_and_1024_65536_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 30, 30)
}

func Benchmark_and_1024_65536_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 30, 300)
}

func Benchmark_and_1024_65536_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 30, 3000)
}

func Benchmark_and_1024_65536_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 30, 30000)
}

func Benchmark_and_1024_65536_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 300, 3)
}

func Benchmark_and_1024_65536_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 300, 30)
}

func Benchmark_and_1024_65536_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 300, 300)
}

func Benchmark_and_1024_65536_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 300, 3000)
}

func Benchmark_and_1024_65536_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 300, 30000)
}

func Benchmark_and_1024_65536_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 3000, 3)
}

func Benchmark_and_1024_65536_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 3000, 30)
}

func Benchmark_and_1024_65536_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 3000, 300)
}

func Benchmark_and_1024_65536_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 3000, 3000)
}

func Benchmark_and_1024_65536_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 3000, 30000)
}

func Benchmark_and_1024_65536_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 30000, 3)
}

func Benchmark_and_1024_65536_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 30000, 30)
}

func Benchmark_and_1024_65536_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 30000, 300)
}

func Benchmark_and_1024_65536_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 30000, 3000)
}

func Benchmark_and_1024_65536_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 30000, 30000)
}

func Benchmark_and_65536_64_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 3, 3)
}

func Benchmark_and_65536_64_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 3, 30)
}

func Benchmark_and_65536_64_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 3, 300)
}

func Benchmark_and_65536_64_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 3, 3000)
}

func Benchmark_and_65536_64_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 3, 30000)
}

func Benchmark_and_65536_64_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 30, 3)
}

func Benchmark_and_65536_64_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 30, 30)
}

func Benchmark_and_65536_64_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 30, 300)
}

func Benchmark_and_65536_64_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 30, 3000)
}

func Benchmark_and_65536_64_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 30, 30000)
}

func Benchmark_and_65536_64_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 300, 3)
}

func Benchmark_and_65536_64_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 300, 30)
}

func Benchmark_and_65536_64_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 300, 300)
}

func Benchmark_and_65536_64_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 300, 3000)
}

func Benchmark_and_65536_64_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 300, 30000)
}

func Benchmark_and_65536_64_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 3000, 3)
}

func Benchmark_and_65536_64_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 3000, 30)
}

func Benchmark_and_65536_64_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 3000, 300)
}

func Benchmark_and_65536_64_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 3000, 3000)
}

func Benchmark_and_65536_64_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 3000, 30000)
}

func Benchmark_and_65536_64_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 30000, 3)
}

func Benchmark_and_65536_64_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 30000, 30)
}

func Benchmark_and_65536_64_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 30000, 300)
}

func Benchmark_and_65536_64_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 30000, 3000)
}

func Benchmark_and_65536_64_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 30000, 30000)
}

func Benchmark_and_65536_1024_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 3, 3)
}

func Benchmark_and_65536_1024_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 3, 30)
}

func Benchmark_and_65536_1024_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 3, 300)
}

func Benchmark_and_65536_1024_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 3, 3000)
}

func Benchmark_and_65536_1024_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 3, 30000)
}

func Benchmark_and_65536_1024_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 30, 3)
}

func Benchmark_and_65536_1024_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 30, 30)
}

func Benchmark_and_65536_1024_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 30, 300)
}

func Benchmark_and_65536_1024_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 30, 3000)
}

func Benchmark_and_65536_1024_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 30, 30000)
}

func Benchmark_and_65536_1024_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 300, 3)
}

func Benchmark_and_65536_1024_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 300, 30)
}

func Benchmark_and_65536_1024_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 300, 300)
}

func Benchmark_and_65536_1024_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 300, 3000)
}

func Benchmark_and_65536_1024_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 300, 30000)
}

func Benchmark_and_65536_1024_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 3000, 3)
}

func Benchmark_and_65536_1024_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 3000, 30)
}

func Benchmark_and_65536_1024_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 3000, 300)
}

func Benchmark_and_65536_1024_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 3000, 3000)
}

func Benchmark_and_65536_1024_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 3000, 30000)
}

func Benchmark_and_65536_1024_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 30000, 3)
}

func Benchmark_and_65536_1024_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 30000, 30)
}

func Benchmark_and_65536_1024_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 30000, 300)
}

func Benchmark_and_65536_1024_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 30000, 3000)
}

func Benchmark_and_65536_1024_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 30000, 30000)
}

func Benchmark_and_65536_65536_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 3, 3)
}

func Benchmark_and_65536_65536_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 3, 30)
}

func Benchmark_and_65536_65536_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 3, 300)
}

func Benchmark_and_65536_65536_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 3, 3000)
}

func Benchmark_and_65536_65536_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 3, 30000)
}

func Benchmark_and_65536_65536_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 30, 3)
}

func Benchmark_and_65536_65536_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 30, 30)
}

func Benchmark_and_65536_65536_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 30, 300)
}

func Benchmark_and_65536_65536_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 30, 3000)
}

func Benchmark_and_65536_65536_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 30, 30000)
}

func Benchmark_and_65536_65536_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 300, 3)
}

func Benchmark_and_65536_65536_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 300, 30)
}

func Benchmark_and_65536_65536_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 300, 300)
}

func Benchmark_and_65536_65536_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 300, 3000)
}

func Benchmark_and_65536_65536_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 300, 30000)
}

func Benchmark_and_65536_65536_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 3000, 3)
}

func Benchmark_and_65536_65536_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 3000, 30)
}

func Benchmark_and_65536_65536_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 3000, 300)
}

func Benchmark_and_65536_65536_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 3000, 3000)
}

func Benchmark_and_65536_65536_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 3000, 30000)
}

func Benchmark_and_65536_65536_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 30000, 3)
}

func Benchmark_and_65536_65536_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 30000, 30)
}

func Benchmark_and_65536_65536_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 30000, 300)
}

func Benchmark_and_65536_65536_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 30000, 3000)
}

func Benchmark_and_65536_65536_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 30000, 30000)
}

func Benchmark_or_64_64_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 3, 3)
}

func Benchmark_or_64_64_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 3, 30)
}

func Benchmark_or_64_64_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 3, 300)
}

func Benchmark_or_64_64_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 3, 3000)
}

func Benchmark_or_64_64_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 3, 30000)
}

func Benchmark_or_64_64_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 30, 3)
}

func Benchmark_or_64_64_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 30, 30)
}

func Benchmark_or_64_64_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 30, 300)
}

func Benchmark_or_64_64_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 30, 3000)
}

func Benchmark_or_64_64_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 30, 30000)
}

func Benchmark_or_64_64_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 300, 3)
}

func Benchmark_or_64_64_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 300, 30)
}

func Benchmark_or_64_64_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 300, 300)
}

func Benchmark_or_64_64_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 300, 3000)
}

func Benchmark_or_64_64_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 300, 30000)
}

func Benchmark_or_64_64_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 3000, 3)
}

func Benchmark_or_64_64_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 3000, 30)
}

func Benchmark_or_64_64_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 3000, 300)
}

func Benchmark_or_64_64_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 3000, 3000)
}

func Benchmark_or_64_64_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 3000, 30000)
}

func Benchmark_or_64_64_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 30000, 3)
}

func Benchmark_or_64_64_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 30000, 30)
}

func Benchmark_or_64_64_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 30000, 300)
}

func Benchmark_or_64_64_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 30000, 3000)
}

func Benchmark_or_64_64_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 30000, 30000)
}

func Benchmark_or_64_1024_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 3, 3)
}

func Benchmark_or_64_1024_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 3, 30)
}

func Benchmark_or_64_1024_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 3, 300)
}

func Benchmark_or_64_1024_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 3, 3000)
}

func Benchmark_or_64_1024_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 3, 30000)
}

func Benchmark_or_64_1024_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 30, 3)
}

func Benchmark_or_64_1024_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 30, 30)
}

func Benchmark_or_64_1024_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 30, 300)
}

func Benchmark_or_64_1024_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 30, 3000)
}

func Benchmark_or_64_1024_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 30, 30000)
}

func Benchmark_or_64_1024_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 300, 3)
}

func Benchmark_or_64_1024_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 300, 30)
}

func Benchmark_or_64_1024_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 300, 300)
}

func Benchmark_or_64_1024_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 300, 3000)
}

func Benchmark_or_64_1024_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 300, 30000)
}

func Benchmark_or_64_1024_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 3000, 3)
}

func Benchmark_or_64_1024_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 3000, 30)
}

func Benchmark_or_64_1024_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 3000, 300)
}

func Benchmark_or_64_1024_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 3000, 3000)
}

func Benchmark_or_64_1024_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 3000, 30000)
}

func Benchmark_or_64_1024_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 30000, 3)
}

func Benchmark_or_64_1024_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 30000, 30)
}

func Benchmark_or_64_1024_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 30000, 300)
}

func Benchmark_or_64_1024_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 30000, 3000)
}

func Benchmark_or_64_1024_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 30000, 30000)
}

func Benchmark_or_64_65536_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 3, 3)
}

func Benchmark_or_64_65536_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 3, 30)
}

func Benchmark_or_64_65536_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 3, 300)
}

func Benchmark_or_64_65536_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 3, 3000)
}

func Benchmark_or_64_65536_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 3, 30000)
}

func Benchmark_or_64_65536_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 30, 3)
}

func Benchmark_or_64_65536_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 30, 30)
}

func Benchmark_or_64_65536_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 30, 300)
}

func Benchmark_or_64_65536_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 30, 3000)
}

func Benchmark_or_64_65536_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 30, 30000)
}

func Benchmark_or_64_65536_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 300, 3)
}

func Benchmark_or_64_65536_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 300, 30)
}

func Benchmark_or_64_65536_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 300, 300)
}

func Benchmark_or_64_65536_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 300, 3000)
}

func Benchmark_or_64_65536_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 300, 30000)
}

func Benchmark_or_64_65536_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 3000, 3)
}

func Benchmark_or_64_65536_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 3000, 30)
}

func Benchmark_or_64_65536_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 3000, 300)
}

func Benchmark_or_64_65536_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 3000, 3000)
}

func Benchmark_or_64_65536_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 3000, 30000)
}

func Benchmark_or_64_65536_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 30000, 3)
}

func Benchmark_or_64_65536_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 30000, 30)
}

func Benchmark_or_64_65536_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 30000, 300)
}

func Benchmark_or_64_65536_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 30000, 3000)
}

func Benchmark_or_64_65536_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 30000, 30000)
}

func Benchmark_or_1024_64_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 3, 3)
}

func Benchmark_or_1024_64_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 3, 30)
}

func Benchmark_or_1024_64_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 3, 300)
}

func Benchmark_or_1024_64_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 3, 3000)
}

func Benchmark_or_1024_64_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 3, 30000)
}

func Benchmark_or_1024_64_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 30, 3)
}

func Benchmark_or_1024_64_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 30, 30)
}

func Benchmark_or_1024_64_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 30, 300)
}

func Benchmark_or_1024_64_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 30, 3000)
}

func Benchmark_or_1024_64_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 30, 30000)
}

func Benchmark_or_1024_64_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 300, 3)
}

func Benchmark_or_1024_64_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 300, 30)
}

func Benchmark_or_1024_64_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 300, 300)
}

func Benchmark_or_1024_64_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 300, 3000)
}

func Benchmark_or_1024_64_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 300, 30000)
}

func Benchmark_or_1024_64_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 3000, 3)
}

func Benchmark_or_1024_64_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 3000, 30)
}

func Benchmark_or_1024_64_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 3000, 300)
}

func Benchmark_or_1024_64_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 3000, 3000)
}

func Benchmark_or_1024_64_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 3000, 30000)
}

func Benchmark_or_1024_64_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 30000, 3)
}

func Benchmark_or_1024_64_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 30000, 30)
}

func Benchmark_or_1024_64_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 30000, 300)
}

func Benchmark_or_1024_64_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 30000, 3000)
}

func Benchmark_or_1024_64_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 30000, 30000)
}

func Benchmark_or_1024_1024_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 3, 3)
}

func Benchmark_or_1024_1024_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 3, 30)
}

func Benchmark_or_1024_1024_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 3, 300)
}

func Benchmark_or_1024_1024_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 3, 3000)
}

func Benchmark_or_1024_1024_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 3, 30000)
}

func Benchmark_or_1024_1024_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 30, 3)
}

func Benchmark_or_1024_1024_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 30, 30)
}

func Benchmark_or_1024_1024_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 30, 300)
}

func Benchmark_or_1024_1024_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 30, 3000)
}

func Benchmark_or_1024_1024_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 30, 30000)
}

func Benchmark_or_1024_1024_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 300, 3)
}

func Benchmark_or_1024_1024_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 300, 30)
}

func Benchmark_or_1024_1024_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 300, 300)
}

func Benchmark_or_1024_1024_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 300, 3000)
}

func Benchmark_or_1024_1024_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 300, 30000)
}

func Benchmark_or_1024_1024_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 3000, 3)
}

func Benchmark_or_1024_1024_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 3000, 30)
}

func Benchmark_or_1024_1024_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 3000, 300)
}

func Benchmark_or_1024_1024_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 3000, 3000)
}

func Benchmark_or_1024_1024_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 3000, 30000)
}

func Benchmark_or_1024_1024_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 30000, 3)
}

func Benchmark_or_1024_1024_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 30000, 30)
}

func Benchmark_or_1024_1024_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 30000, 300)
}

func Benchmark_or_1024_1024_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 30000, 3000)
}

func Benchmark_or_1024_1024_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 30000, 30000)
}

func Benchmark_or_1024_65536_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 3, 3)
}

func Benchmark_or_1024_65536_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 3, 30)
}

func Benchmark_or_1024_65536_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 3, 300)
}

func Benchmark_or_1024_65536_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 3, 3000)
}

func Benchmark_or_1024_65536_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 3, 30000)
}

func Benchmark_or_1024_65536_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 30, 3)
}

func Benchmark_or_1024_65536_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 30, 30)
}

func Benchmark_or_1024_65536_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 30, 300)
}

func Benchmark_or_1024_65536_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 30, 3000)
}

func Benchmark_or_1024_65536_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 30, 30000)
}

func Benchmark_or_1024_65536_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 300, 3)
}

func Benchmark_or_1024_65536_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 300, 30)
}

func Benchmark_or_1024_65536_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 300, 300)
}

func Benchmark_or_1024_65536_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 300, 3000)
}

func Benchmark_or_1024_65536_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 300, 30000)
}

func Benchmark_or_1024_65536_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 3000, 3)
}

func Benchmark_or_1024_65536_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 3000, 30)
}

func Benchmark_or_1024_65536_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 3000, 300)
}

func Benchmark_or_1024_65536_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 3000, 3000)
}

func Benchmark_or_1024_65536_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 3000, 30000)
}

func Benchmark_or_1024_65536_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 30000, 3)
}

func Benchmark_or_1024_65536_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 30000, 30)
}

func Benchmark_or_1024_65536_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 30000, 300)
}

func Benchmark_or_1024_65536_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 30000, 3000)
}

func Benchmark_or_1024_65536_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 30000, 30000)
}

func Benchmark_or_65536_64_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 3, 3)
}

func Benchmark_or_65536_64_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 3, 30)
}

func Benchmark_or_65536_64_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 3, 300)
}

func Benchmark_or_65536_64_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 3, 3000)
}

func Benchmark_or_65536_64_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 3, 30000)
}

func Benchmark_or_65536_64_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 30, 3)
}

func Benchmark_or_65536_64_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 30, 30)
}

func Benchmark_or_65536_64_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 30, 300)
}

func Benchmark_or_65536_64_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 30, 3000)
}

func Benchmark_or_65536_64_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 30, 30000)
}

func Benchmark_or_65536_64_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 300, 3)
}

func Benchmark_or_65536_64_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 300, 30)
}

func Benchmark_or_65536_64_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 300, 300)
}

func Benchmark_or_65536_64_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 300, 3000)
}

func Benchmark_or_65536_64_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 300, 30000)
}

func Benchmark_or_65536_64_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 3000, 3)
}

func Benchmark_or_65536_64_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 3000, 30)
}

func Benchmark_or_65536_64_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 3000, 300)
}

func Benchmark_or_65536_64_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 3000, 3000)
}

func Benchmark_or_65536_64_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 3000, 30000)
}

func Benchmark_or_65536_64_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 30000, 3)
}

func Benchmark_or_65536_64_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 30000, 30)
}

func Benchmark_or_65536_64_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 30000, 300)
}

func Benchmark_or_65536_64_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 30000, 3000)
}

func Benchmark_or_65536_64_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 30000, 30000)
}

func Benchmark_or_65536_1024_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 3, 3)
}

func Benchmark_or_65536_1024_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 3, 30)
}

func Benchmark_or_65536_1024_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 3, 300)
}

func Benchmark_or_65536_1024_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 3, 3000)
}

func Benchmark_or_65536_1024_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 3, 30000)
}

func Benchmark_or_65536_1024_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 30, 3)
}

func Benchmark_or_65536_1024_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 30, 30)
}

func Benchmark_or_65536_1024_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 30, 300)
}

func Benchmark_or_65536_1024_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 30, 3000)
}

func Benchmark_or_65536_1024_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 30, 30000)
}

func Benchmark_or_65536_1024_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 300, 3)
}

func Benchmark_or_65536_1024_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 300, 30)
}

func Benchmark_or_65536_1024_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 300, 300)
}

func Benchmark_or_65536_1024_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 300, 3000)
}

func Benchmark_or_65536_1024_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 300, 30000)
}

func Benchmark_or_65536_1024_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 3000, 3)
}

func Benchmark_or_65536_1024_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 3000, 30)
}

func Benchmark_or_65536_1024_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 3000, 300)
}

func Benchmark_or_65536_1024_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 3000, 3000)
}

func Benchmark_or_65536_1024_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 3000, 30000)
}

func Benchmark_or_65536_1024_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 30000, 3)
}

func Benchmark_or_65536_1024_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 30000, 30)
}

func Benchmark_or_65536_1024_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 30000, 300)
}

func Benchmark_or_65536_1024_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 30000, 3000)
}

func Benchmark_or_65536_1024_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 30000, 30000)
}

func Benchmark_or_65536_65536_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 3, 3)
}

func Benchmark_or_65536_65536_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 3, 30)
}

func Benchmark_or_65536_65536_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 3, 300)
}

func Benchmark_or_65536_65536_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 3, 3000)
}

func Benchmark_or_65536_65536_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 3, 30000)
}

func Benchmark_or_65536_65536_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 30, 3)
}

func Benchmark_or_65536_65536_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 30, 30)
}

func Benchmark_or_65536_65536_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 30, 300)
}

func Benchmark_or_65536_65536_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 30, 3000)
}

func Benchmark_or_65536_65536_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 30, 30000)
}

func Benchmark_or_65536_65536_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 300, 3)
}

func Benchmark_or_65536_65536_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 300, 30)
}

func Benchmark_or_65536_65536_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 300, 300)
}

func Benchmark_or_65536_65536_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 300, 3000)
}

func Benchmark_or_65536_65536_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 300, 30000)
}

func Benchmark_or_65536_65536_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 3000, 3)
}

func Benchmark_or_65536_65536_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 3000, 30)
}

func Benchmark_or_65536_65536_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 3000, 300)
}

func Benchmark_or_65536_65536_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 3000, 3000)
}

func Benchmark_or_65536_65536_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 3000, 30000)
}

func Benchmark_or_65536_65536_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 30000, 3)
}

func Benchmark_or_65536_65536_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 30000, 30)
}

func Benchmark_or_65536_65536_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 30000, 300)
}

func Benchmark_or_65536_65536_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 30000, 3000)
}

func Benchmark_or_65536_65536_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 30000, 30000)
}

func Benchmark_diff_64_64_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 3, 3)
}

func Benchmark_diff_64_64_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 3, 30)
}

func Benchmark_diff_64_64_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 3, 300)
}

func Benchmark_diff_64_64_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 3, 3000)
}

func Benchmark_diff_64_64_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 3, 30000)
}

func Benchmark_diff_64_64_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 30, 3)
}

func Benchmark_diff_64_64_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 30, 30)
}

func Benchmark_diff_64_64_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 30, 300)
}

func Benchmark_diff_64_64_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 30, 3000)
}

func Benchmark_diff_64_64_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 30, 30000)
}

func Benchmark_diff_64_64_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 300, 3)
}

func Benchmark_diff_64_64_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 300, 30)
}

func Benchmark_diff_64_64_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 300, 300)
}

func Benchmark_diff_64_64_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 300, 3000)
}

func Benchmark_diff_64_64_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 300, 30000)
}

func Benchmark_diff_64_64_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 3000, 3)
}

func Benchmark_diff_64_64_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 3000, 30)
}

func Benchmark_diff_64_64_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 3000, 300)
}

func Benchmark_diff_64_64_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 3000, 3000)
}

func Benchmark_diff_64_64_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 3000, 30000)
}

func Benchmark_diff_64_64_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 30000, 3)
}

func Benchmark_diff_64_64_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 30000, 30)
}

func Benchmark_diff_64_64_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 30000, 300)
}

func Benchmark_diff_64_64_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 30000, 3000)
}

func Benchmark_diff_64_64_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 30000, 30000)
}

func Benchmark_diff_64_1024_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 3, 3)
}

func Benchmark_diff_64_1024_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 3, 30)
}

func Benchmark_diff_64_1024_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 3, 300)
}

func Benchmark_diff_64_1024_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 3, 3000)
}

func Benchmark_diff_64_1024_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 3, 30000)
}

func Benchmark_diff_64_1024_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 30, 3)
}

func Benchmark_diff_64_1024_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 30, 30)
}

func Benchmark_diff_64_1024_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 30, 300)
}

func Benchmark_diff_64_1024_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 30, 3000)
}

func Benchmark_diff_64_1024_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 30, 30000)
}

func Benchmark_diff_64_1024_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 300, 3)
}

func Benchmark_diff_64_1024_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 300, 30)
}

func Benchmark_diff_64_1024_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 300, 300)
}

func Benchmark_diff_64_1024_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 300, 3000)
}

func Benchmark_diff_64_1024_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 300, 30000)
}

func Benchmark_diff_64_1024_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 3000, 3)
}

func Benchmark_diff_64_1024_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 3000, 30)
}

func Benchmark_diff_64_1024_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 3000, 300)
}

func Benchmark_diff_64_1024_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 3000, 3000)
}

func Benchmark_diff_64_1024_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 3000, 30000)
}

func Benchmark_diff_64_1024_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 30000, 3)
}

func Benchmark_diff_64_1024_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 30000, 30)
}

func Benchmark_diff_64_1024_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 30000, 300)
}

func Benchmark_diff_64_1024_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 30000, 3000)
}

func Benchmark_diff_64_1024_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 30000, 30000)
}

func Benchmark_diff_64_65536_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 3, 3)
}

func Benchmark_diff_64_65536_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 3, 30)
}

func Benchmark_diff_64_65536_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 3, 300)
}

func Benchmark_diff_64_65536_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 3, 3000)
}

func Benchmark_diff_64_65536_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 3, 30000)
}

func Benchmark_diff_64_65536_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 30, 3)
}

func Benchmark_diff_64_65536_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 30, 30)
}

func Benchmark_diff_64_65536_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 30, 300)
}

func Benchmark_diff_64_65536_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 30, 3000)
}

func Benchmark_diff_64_65536_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 30, 30000)
}

func Benchmark_diff_64_65536_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 300, 3)
}

func Benchmark_diff_64_65536_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 300, 30)
}

func Benchmark_diff_64_65536_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 300, 300)
}

func Benchmark_diff_64_65536_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 300, 3000)
}

func Benchmark_diff_64_65536_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 300, 30000)
}

func Benchmark_diff_64_65536_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 3000, 3)
}

func Benchmark_diff_64_65536_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 3000, 30)
}

func Benchmark_diff_64_65536_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 3000, 300)
}

func Benchmark_diff_64_65536_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 3000, 3000)
}

func Benchmark_diff_64_65536_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 3000, 30000)
}

func Benchmark_diff_64_65536_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 30000, 3)
}

func Benchmark_diff_64_65536_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 30000, 30)
}

func Benchmark_diff_64_65536_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 30000, 300)
}

func Benchmark_diff_64_65536_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 30000, 3000)
}

func Benchmark_diff_64_65536_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 30000, 30000)
}

func Benchmark_diff_1024_64_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 3, 3)
}

func Benchmark_diff_1024_64_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 3, 30)
}

func Benchmark_diff_1024_64_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 3, 300)
}

func Benchmark_diff_1024_64_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 3, 3000)
}

func Benchmark_diff_1024_64_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 3, 30000)
}

func Benchmark_diff_1024_64_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 30, 3)
}

func Benchmark_diff_1024_64_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 30, 30)
}

func Benchmark_diff_1024_64_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 30, 300)
}

func Benchmark_diff_1024_64_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 30, 3000)
}

func Benchmark_diff_1024_64_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 30, 30000)
}

func Benchmark_diff_1024_64_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 300, 3)
}

func Benchmark_diff_1024_64_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 300, 30)
}

func Benchmark_diff_1024_64_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 300, 300)
}

func Benchmark_diff_1024_64_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 300, 3000)
}

func Benchmark_diff_1024_64_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 300, 30000)
}

func Benchmark_diff_1024_64_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 3000, 3)
}

func Benchmark_diff_1024_64_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 3000, 30)
}

func Benchmark_diff_1024_64_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 3000, 300)
}

func Benchmark_diff_1024_64_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 3000, 3000)
}

func Benchmark_diff_1024_64_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 3000, 30000)
}

func Benchmark_diff_1024_64_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 30000, 3)
}

func Benchmark_diff_1024_64_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 30000, 30)
}

func Benchmark_diff_1024_64_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 30000, 300)
}

func Benchmark_diff_1024_64_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 30000, 3000)
}

func Benchmark_diff_1024_64_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 30000, 30000)
}

func Benchmark_diff_1024_1024_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 3, 3)
}

func Benchmark_diff_1024_1024_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 3, 30)
}

func Benchmark_diff_1024_1024_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 3, 300)
}

func Benchmark_diff_1024_1024_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 3, 3000)
}

func Benchmark_diff_1024_1024_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 3, 30000)
}

func Benchmark_diff_1024_1024_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 30, 3)
}

func Benchmark_diff_1024_1024_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 30, 30)
}

func Benchmark_diff_1024_1024_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 30, 300)
}

func Benchmark_diff_1024_1024_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 30, 3000)
}

func Benchmark_diff_1024_1024_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 30, 30000)
}

func Benchmark_diff_1024_1024_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 300, 3)
}

func Benchmark_diff_1024_1024_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 300, 30)
}

func Benchmark_diff_1024_1024_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 300, 300)
}

func Benchmark_diff_1024_1024_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 300, 3000)
}

func Benchmark_diff_1024_1024_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 300, 30000)
}

func Benchmark_diff_1024_1024_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 3000, 3)
}

func Benchmark_diff_1024_1024_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 3000, 30)
}

func Benchmark_diff_1024_1024_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 3000, 300)
}

func Benchmark_diff_1024_1024_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 3000, 3000)
}

func Benchmark_diff_1024_1024_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 3000, 30000)
}

func Benchmark_diff_1024_1024_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 30000, 3)
}

func Benchmark_diff_1024_1024_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 30000, 30)
}

func Benchmark_diff_1024_1024_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 30000, 300)
}

func Benchmark_diff_1024_1024_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 30000, 3000)
}

func Benchmark_diff_1024_1024_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 30000, 30000)
}

func Benchmark_diff_1024_65536_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 3, 3)
}

func Benchmark_diff_1024_65536_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 3, 30)
}

func Benchmark_diff_1024_65536_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 3, 300)
}

func Benchmark_diff_1024_65536_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 3, 3000)
}

func Benchmark_diff_1024_65536_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 3, 30000)
}

func Benchmark_diff_1024_65536_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 30, 3)
}

func Benchmark_diff_1024_65536_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 30, 30)
}

func Benchmark_diff_1024_65536_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 30, 300)
}

func Benchmark_diff_1024_65536_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 30, 3000)
}

func Benchmark_diff_1024_65536_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 30, 30000)
}

func Benchmark_diff_1024_65536_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 300, 3)
}

func Benchmark_diff_1024_65536_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 300, 30)
}

func Benchmark_diff_1024_65536_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 300, 300)
}

func Benchmark_diff_1024_65536_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 300, 3000)
}

func Benchmark_diff_1024_65536_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 300, 30000)
}

func Benchmark_diff_1024_65536_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 3000, 3)
}

func Benchmark_diff_1024_65536_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 3000, 30)
}

func Benchmark_diff_1024_65536_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 3000, 300)
}

func Benchmark_diff_1024_65536_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 3000, 3000)
}

func Benchmark_diff_1024_65536_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 3000, 30000)
}

func Benchmark_diff_1024_65536_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 30000, 3)
}

func Benchmark_diff_1024_65536_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 30000, 30)
}

func Benchmark_diff_1024_65536_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 30000, 300)
}

func Benchmark_diff_1024_65536_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 30000, 3000)
}

func Benchmark_diff_1024_65536_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 30000, 30000)
}

func Benchmark_diff_65536_64_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 3, 3)
}

func Benchmark_diff_65536_64_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 3, 30)
}

func Benchmark_diff_65536_64_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 3, 300)
}

func Benchmark_diff_65536_64_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 3, 3000)
}

func Benchmark_diff_65536_64_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 3, 30000)
}

func Benchmark_diff_65536_64_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 30, 3)
}

func Benchmark_diff_65536_64_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 30, 30)
}

func Benchmark_diff_65536_64_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 30, 300)
}

func Benchmark_diff_65536_64_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 30, 3000)
}

func Benchmark_diff_65536_64_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 30, 30000)
}

func Benchmark_diff_65536_64_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 300, 3)
}

func Benchmark_diff_65536_64_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 300, 30)
}

func Benchmark_diff_65536_64_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 300, 300)
}

func Benchmark_diff_65536_64_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 300, 3000)
}

func Benchmark_diff_65536_64_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 300, 30000)
}

func Benchmark_diff_65536_64_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 3000, 3)
}

func Benchmark_diff_65536_64_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 3000, 30)
}

func Benchmark_diff_65536_64_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 3000, 300)
}

func Benchmark_diff_65536_64_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 3000, 3000)
}

func Benchmark_diff_65536_64_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 3000, 30000)
}

func Benchmark_diff_65536_64_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 30000, 3)
}

func Benchmark_diff_65536_64_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 30000, 30)
}

func Benchmark_diff_65536_64_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 30000, 300)
}

func Benchmark_diff_65536_64_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 30000, 3000)
}

func Benchmark_diff_65536_64_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 30000, 30000)
}

func Benchmark_diff_65536_1024_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 3, 3)
}

func Benchmark_diff_65536_1024_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 3, 30)
}

func Benchmark_diff_65536_1024_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 3, 300)
}

func Benchmark_diff_65536_1024_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 3, 3000)
}

func Benchmark_diff_65536_1024_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 3, 30000)
}

func Benchmark_diff_65536_1024_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 30, 3)
}

func Benchmark_diff_65536_1024_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 30, 30)
}

func Benchmark_diff_65536_1024_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 30, 300)
}

func Benchmark_diff_65536_1024_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 30, 3000)
}

func Benchmark_diff_65536_1024_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 30, 30000)
}

func Benchmark_diff_65536_1024_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 300, 3)
}

func Benchmark_diff_65536_1024_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 300, 30)
}

func Benchmark_diff_65536_1024_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 300, 300)
}

func Benchmark_diff_65536_1024_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 300, 3000)
}

func Benchmark_diff_65536_1024_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 300, 30000)
}

func Benchmark_diff_65536_1024_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 3000, 3)
}

func Benchmark_diff_65536_1024_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 3000, 30)
}

func Benchmark_diff_65536_1024_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 3000, 300)
}

func Benchmark_diff_65536_1024_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 3000, 3000)
}

func Benchmark_diff_65536_1024_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 3000, 30000)
}

func Benchmark_diff_65536_1024_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 30000, 3)
}

func Benchmark_diff_65536_1024_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 30000, 30)
}

func Benchmark_diff_65536_1024_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 30000, 300)
}

func Benchmark_diff_65536_1024_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 30000, 3000)
}

func Benchmark_diff_65536_1024_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 30000, 30000)
}

func Benchmark_diff_65536_65536_3_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 3, 3)
}

func Benchmark_diff_65536_65536_3_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 3, 30)
}

func Benchmark_diff_65536_65536_3_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 3, 300)
}

func Benchmark_diff_65536_65536_3_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 3, 3000)
}

func Benchmark_diff_65536_65536_3_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 3, 30000)
}

func Benchmark_diff_65536_65536_30_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 30, 3)
}

func Benchmark_diff_65536_65536_30_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 30, 30)
}

func Benchmark_diff_65536_65536_30_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 30, 300)
}

func Benchmark_diff_65536_65536_30_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 30, 3000)
}

func Benchmark_diff_65536_65536_30_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 30, 30000)
}

func Benchmark_diff_65536_65536_300_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 300, 3)
}

func Benchmark_diff_65536_65536_300_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 300, 30)
}

func Benchmark_diff_65536_65536_300_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 300, 300)
}

func Benchmark_diff_65536_65536_300_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 300, 3000)
}

func Benchmark_diff_65536_65536_300_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 300, 30000)
}

func Benchmark_diff_65536_65536_3000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 3000, 3)
}

func Benchmark_diff_65536_65536_3000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 3000, 30)
}

func Benchmark_diff_65536_65536_3000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 3000, 300)
}

func Benchmark_diff_65536_65536_3000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 3000, 3000)
}

func Benchmark_diff_65536_65536_3000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 3000, 30000)
}

func Benchmark_diff_65536_65536_30000_3(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 30000, 3)
}

func Benchmark_diff_65536_65536_30000_30(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 30000, 30)
}

func Benchmark_diff_65536_65536_30000_300(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 30000, 300)
}

func Benchmark_diff_65536_65536_30000_3000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 30000, 3000)
}

func Benchmark_diff_65536_65536_30000_30000(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 30000, 30000)
}
