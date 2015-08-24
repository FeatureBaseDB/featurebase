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
		m1.SetBit(bit)
	}

	bit = 0
	rand.Seed(int64(c2))
	for i := 0; i < b2; i++ {
		bit += uint64(rand.Intn(s1) + 1)
		m2.SetBit(bit)
	}

	var f func(b_bm *Bitmap) *Bitmap
	switch op {
	case "and":
		f = m1.Intersection
	case "or":
		f = m1.Union
	case "diff":
		f = m1.Difference
	default:
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if f(m2) == nil {
			b.Fatalf("Problem with %s benchmark at i = %d", op, i)
		}
	}
}
func Benchmark_and_64_64_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 64, 64)
}

func Benchmark_and_64_64_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 64, 128)
}

func Benchmark_and_64_64_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 64, 256)
}

func Benchmark_and_64_64_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 64, 512)
}

func Benchmark_and_64_64_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 64, 1024)
}

func Benchmark_and_64_64_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 64, 8192)
}

func Benchmark_and_64_64_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 64, 16384)
}

func Benchmark_and_64_64_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 64, 32768)
}

func Benchmark_and_64_64_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 64, 65536)
}

func Benchmark_and_64_64_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 128, 64)
}

func Benchmark_and_64_64_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 128, 128)
}

func Benchmark_and_64_64_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 128, 256)
}

func Benchmark_and_64_64_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 128, 512)
}

func Benchmark_and_64_64_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 128, 1024)
}

func Benchmark_and_64_64_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 128, 8192)
}

func Benchmark_and_64_64_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 128, 16384)
}

func Benchmark_and_64_64_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 128, 32768)
}

func Benchmark_and_64_64_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 128, 65536)
}

func Benchmark_and_64_64_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 256, 64)
}

func Benchmark_and_64_64_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 256, 128)
}

func Benchmark_and_64_64_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 256, 256)
}

func Benchmark_and_64_64_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 256, 512)
}

func Benchmark_and_64_64_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 256, 1024)
}

func Benchmark_and_64_64_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 256, 8192)
}

func Benchmark_and_64_64_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 256, 16384)
}

func Benchmark_and_64_64_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 256, 32768)
}

func Benchmark_and_64_64_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 256, 65536)
}

func Benchmark_and_64_64_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 512, 64)
}

func Benchmark_and_64_64_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 512, 128)
}

func Benchmark_and_64_64_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 512, 256)
}

func Benchmark_and_64_64_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 512, 512)
}

func Benchmark_and_64_64_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 512, 1024)
}

func Benchmark_and_64_64_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 512, 8192)
}

func Benchmark_and_64_64_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 512, 16384)
}

func Benchmark_and_64_64_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 512, 32768)
}

func Benchmark_and_64_64_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 512, 65536)
}

func Benchmark_and_64_64_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 1024, 64)
}

func Benchmark_and_64_64_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 1024, 128)
}

func Benchmark_and_64_64_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 1024, 256)
}

func Benchmark_and_64_64_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 1024, 512)
}

func Benchmark_and_64_64_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 1024, 1024)
}

func Benchmark_and_64_64_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 1024, 8192)
}

func Benchmark_and_64_64_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 1024, 16384)
}

func Benchmark_and_64_64_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 1024, 32768)
}

func Benchmark_and_64_64_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 1024, 65536)
}

func Benchmark_and_64_64_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 8192, 64)
}

func Benchmark_and_64_64_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 8192, 128)
}

func Benchmark_and_64_64_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 8192, 256)
}

func Benchmark_and_64_64_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 8192, 512)
}

func Benchmark_and_64_64_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 8192, 1024)
}

func Benchmark_and_64_64_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 8192, 8192)
}

func Benchmark_and_64_64_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 8192, 16384)
}

func Benchmark_and_64_64_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 8192, 32768)
}

func Benchmark_and_64_64_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 8192, 65536)
}

func Benchmark_and_64_64_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 16384, 64)
}

func Benchmark_and_64_64_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 16384, 128)
}

func Benchmark_and_64_64_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 16384, 256)
}

func Benchmark_and_64_64_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 16384, 512)
}

func Benchmark_and_64_64_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 16384, 1024)
}

func Benchmark_and_64_64_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 16384, 8192)
}

func Benchmark_and_64_64_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 16384, 16384)
}

func Benchmark_and_64_64_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 16384, 32768)
}

func Benchmark_and_64_64_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 16384, 65536)
}

func Benchmark_and_64_64_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 32768, 64)
}

func Benchmark_and_64_64_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 32768, 128)
}

func Benchmark_and_64_64_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 32768, 256)
}

func Benchmark_and_64_64_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 32768, 512)
}

func Benchmark_and_64_64_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 32768, 1024)
}

func Benchmark_and_64_64_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 32768, 8192)
}

func Benchmark_and_64_64_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 32768, 16384)
}

func Benchmark_and_64_64_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 32768, 32768)
}

func Benchmark_and_64_64_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 32768, 65536)
}

func Benchmark_and_64_64_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 65536, 64)
}

func Benchmark_and_64_64_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 65536, 128)
}

func Benchmark_and_64_64_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 65536, 256)
}

func Benchmark_and_64_64_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 65536, 512)
}

func Benchmark_and_64_64_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 65536, 1024)
}

func Benchmark_and_64_64_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 65536, 8192)
}

func Benchmark_and_64_64_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 65536, 16384)
}

func Benchmark_and_64_64_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 65536, 32768)
}

func Benchmark_and_64_64_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 64, 65536, 65536)
}

func Benchmark_and_64_1024_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 64, 64)
}

func Benchmark_and_64_1024_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 64, 128)
}

func Benchmark_and_64_1024_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 64, 256)
}

func Benchmark_and_64_1024_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 64, 512)
}

func Benchmark_and_64_1024_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 64, 1024)
}

func Benchmark_and_64_1024_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 64, 8192)
}

func Benchmark_and_64_1024_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 64, 16384)
}

func Benchmark_and_64_1024_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 64, 32768)
}

func Benchmark_and_64_1024_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 64, 65536)
}

func Benchmark_and_64_1024_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 128, 64)
}

func Benchmark_and_64_1024_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 128, 128)
}

func Benchmark_and_64_1024_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 128, 256)
}

func Benchmark_and_64_1024_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 128, 512)
}

func Benchmark_and_64_1024_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 128, 1024)
}

func Benchmark_and_64_1024_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 128, 8192)
}

func Benchmark_and_64_1024_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 128, 16384)
}

func Benchmark_and_64_1024_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 128, 32768)
}

func Benchmark_and_64_1024_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 128, 65536)
}

func Benchmark_and_64_1024_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 256, 64)
}

func Benchmark_and_64_1024_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 256, 128)
}

func Benchmark_and_64_1024_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 256, 256)
}

func Benchmark_and_64_1024_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 256, 512)
}

func Benchmark_and_64_1024_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 256, 1024)
}

func Benchmark_and_64_1024_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 256, 8192)
}

func Benchmark_and_64_1024_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 256, 16384)
}

func Benchmark_and_64_1024_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 256, 32768)
}

func Benchmark_and_64_1024_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 256, 65536)
}

func Benchmark_and_64_1024_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 512, 64)
}

func Benchmark_and_64_1024_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 512, 128)
}

func Benchmark_and_64_1024_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 512, 256)
}

func Benchmark_and_64_1024_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 512, 512)
}

func Benchmark_and_64_1024_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 512, 1024)
}

func Benchmark_and_64_1024_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 512, 8192)
}

func Benchmark_and_64_1024_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 512, 16384)
}

func Benchmark_and_64_1024_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 512, 32768)
}

func Benchmark_and_64_1024_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 512, 65536)
}

func Benchmark_and_64_1024_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 1024, 64)
}

func Benchmark_and_64_1024_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 1024, 128)
}

func Benchmark_and_64_1024_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 1024, 256)
}

func Benchmark_and_64_1024_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 1024, 512)
}

func Benchmark_and_64_1024_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 1024, 1024)
}

func Benchmark_and_64_1024_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 1024, 8192)
}

func Benchmark_and_64_1024_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 1024, 16384)
}

func Benchmark_and_64_1024_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 1024, 32768)
}

func Benchmark_and_64_1024_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 1024, 65536)
}

func Benchmark_and_64_1024_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 8192, 64)
}

func Benchmark_and_64_1024_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 8192, 128)
}

func Benchmark_and_64_1024_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 8192, 256)
}

func Benchmark_and_64_1024_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 8192, 512)
}

func Benchmark_and_64_1024_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 8192, 1024)
}

func Benchmark_and_64_1024_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 8192, 8192)
}

func Benchmark_and_64_1024_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 8192, 16384)
}

func Benchmark_and_64_1024_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 8192, 32768)
}

func Benchmark_and_64_1024_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 8192, 65536)
}

func Benchmark_and_64_1024_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 16384, 64)
}

func Benchmark_and_64_1024_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 16384, 128)
}

func Benchmark_and_64_1024_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 16384, 256)
}

func Benchmark_and_64_1024_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 16384, 512)
}

func Benchmark_and_64_1024_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 16384, 1024)
}

func Benchmark_and_64_1024_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 16384, 8192)
}

func Benchmark_and_64_1024_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 16384, 16384)
}

func Benchmark_and_64_1024_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 16384, 32768)
}

func Benchmark_and_64_1024_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 16384, 65536)
}

func Benchmark_and_64_1024_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 32768, 64)
}

func Benchmark_and_64_1024_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 32768, 128)
}

func Benchmark_and_64_1024_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 32768, 256)
}

func Benchmark_and_64_1024_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 32768, 512)
}

func Benchmark_and_64_1024_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 32768, 1024)
}

func Benchmark_and_64_1024_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 32768, 8192)
}

func Benchmark_and_64_1024_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 32768, 16384)
}

func Benchmark_and_64_1024_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 32768, 32768)
}

func Benchmark_and_64_1024_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 32768, 65536)
}

func Benchmark_and_64_1024_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 65536, 64)
}

func Benchmark_and_64_1024_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 65536, 128)
}

func Benchmark_and_64_1024_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 65536, 256)
}

func Benchmark_and_64_1024_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 65536, 512)
}

func Benchmark_and_64_1024_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 65536, 1024)
}

func Benchmark_and_64_1024_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 65536, 8192)
}

func Benchmark_and_64_1024_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 65536, 16384)
}

func Benchmark_and_64_1024_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 65536, 32768)
}

func Benchmark_and_64_1024_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 1024, 65536, 65536)
}

func Benchmark_and_64_65536_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 64, 64)
}

func Benchmark_and_64_65536_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 64, 128)
}

func Benchmark_and_64_65536_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 64, 256)
}

func Benchmark_and_64_65536_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 64, 512)
}

func Benchmark_and_64_65536_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 64, 1024)
}

func Benchmark_and_64_65536_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 64, 8192)
}

func Benchmark_and_64_65536_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 64, 16384)
}

func Benchmark_and_64_65536_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 64, 32768)
}

func Benchmark_and_64_65536_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 64, 65536)
}

func Benchmark_and_64_65536_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 128, 64)
}

func Benchmark_and_64_65536_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 128, 128)
}

func Benchmark_and_64_65536_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 128, 256)
}

func Benchmark_and_64_65536_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 128, 512)
}

func Benchmark_and_64_65536_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 128, 1024)
}

func Benchmark_and_64_65536_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 128, 8192)
}

func Benchmark_and_64_65536_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 128, 16384)
}

func Benchmark_and_64_65536_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 128, 32768)
}

func Benchmark_and_64_65536_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 128, 65536)
}

func Benchmark_and_64_65536_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 256, 64)
}

func Benchmark_and_64_65536_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 256, 128)
}

func Benchmark_and_64_65536_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 256, 256)
}

func Benchmark_and_64_65536_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 256, 512)
}

func Benchmark_and_64_65536_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 256, 1024)
}

func Benchmark_and_64_65536_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 256, 8192)
}

func Benchmark_and_64_65536_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 256, 16384)
}

func Benchmark_and_64_65536_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 256, 32768)
}

func Benchmark_and_64_65536_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 256, 65536)
}

func Benchmark_and_64_65536_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 512, 64)
}

func Benchmark_and_64_65536_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 512, 128)
}

func Benchmark_and_64_65536_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 512, 256)
}

func Benchmark_and_64_65536_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 512, 512)
}

func Benchmark_and_64_65536_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 512, 1024)
}

func Benchmark_and_64_65536_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 512, 8192)
}

func Benchmark_and_64_65536_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 512, 16384)
}

func Benchmark_and_64_65536_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 512, 32768)
}

func Benchmark_and_64_65536_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 512, 65536)
}

func Benchmark_and_64_65536_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 1024, 64)
}

func Benchmark_and_64_65536_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 1024, 128)
}

func Benchmark_and_64_65536_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 1024, 256)
}

func Benchmark_and_64_65536_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 1024, 512)
}

func Benchmark_and_64_65536_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 1024, 1024)
}

func Benchmark_and_64_65536_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 1024, 8192)
}

func Benchmark_and_64_65536_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 1024, 16384)
}

func Benchmark_and_64_65536_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 1024, 32768)
}

func Benchmark_and_64_65536_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 1024, 65536)
}

func Benchmark_and_64_65536_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 8192, 64)
}

func Benchmark_and_64_65536_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 8192, 128)
}

func Benchmark_and_64_65536_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 8192, 256)
}

func Benchmark_and_64_65536_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 8192, 512)
}

func Benchmark_and_64_65536_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 8192, 1024)
}

func Benchmark_and_64_65536_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 8192, 8192)
}

func Benchmark_and_64_65536_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 8192, 16384)
}

func Benchmark_and_64_65536_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 8192, 32768)
}

func Benchmark_and_64_65536_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 8192, 65536)
}

func Benchmark_and_64_65536_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 16384, 64)
}

func Benchmark_and_64_65536_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 16384, 128)
}

func Benchmark_and_64_65536_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 16384, 256)
}

func Benchmark_and_64_65536_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 16384, 512)
}

func Benchmark_and_64_65536_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 16384, 1024)
}

func Benchmark_and_64_65536_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 16384, 8192)
}

func Benchmark_and_64_65536_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 16384, 16384)
}

func Benchmark_and_64_65536_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 16384, 32768)
}

func Benchmark_and_64_65536_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 16384, 65536)
}

func Benchmark_and_64_65536_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 32768, 64)
}

func Benchmark_and_64_65536_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 32768, 128)
}

func Benchmark_and_64_65536_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 32768, 256)
}

func Benchmark_and_64_65536_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 32768, 512)
}

func Benchmark_and_64_65536_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 32768, 1024)
}

func Benchmark_and_64_65536_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 32768, 8192)
}

func Benchmark_and_64_65536_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 32768, 16384)
}

func Benchmark_and_64_65536_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 32768, 32768)
}

func Benchmark_and_64_65536_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 32768, 65536)
}

func Benchmark_and_64_65536_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 65536, 64)
}

func Benchmark_and_64_65536_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 65536, 128)
}

func Benchmark_and_64_65536_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 65536, 256)
}

func Benchmark_and_64_65536_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 65536, 512)
}

func Benchmark_and_64_65536_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 65536, 1024)
}

func Benchmark_and_64_65536_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 65536, 8192)
}

func Benchmark_and_64_65536_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 65536, 16384)
}

func Benchmark_and_64_65536_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 65536, 32768)
}

func Benchmark_and_64_65536_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 64, 65536, 65536, 65536)
}

func Benchmark_and_1024_64_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 64, 64)
}

func Benchmark_and_1024_64_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 64, 128)
}

func Benchmark_and_1024_64_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 64, 256)
}

func Benchmark_and_1024_64_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 64, 512)
}

func Benchmark_and_1024_64_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 64, 1024)
}

func Benchmark_and_1024_64_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 64, 8192)
}

func Benchmark_and_1024_64_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 64, 16384)
}

func Benchmark_and_1024_64_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 64, 32768)
}

func Benchmark_and_1024_64_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 64, 65536)
}

func Benchmark_and_1024_64_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 128, 64)
}

func Benchmark_and_1024_64_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 128, 128)
}

func Benchmark_and_1024_64_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 128, 256)
}

func Benchmark_and_1024_64_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 128, 512)
}

func Benchmark_and_1024_64_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 128, 1024)
}

func Benchmark_and_1024_64_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 128, 8192)
}

func Benchmark_and_1024_64_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 128, 16384)
}

func Benchmark_and_1024_64_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 128, 32768)
}

func Benchmark_and_1024_64_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 128, 65536)
}

func Benchmark_and_1024_64_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 256, 64)
}

func Benchmark_and_1024_64_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 256, 128)
}

func Benchmark_and_1024_64_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 256, 256)
}

func Benchmark_and_1024_64_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 256, 512)
}

func Benchmark_and_1024_64_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 256, 1024)
}

func Benchmark_and_1024_64_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 256, 8192)
}

func Benchmark_and_1024_64_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 256, 16384)
}

func Benchmark_and_1024_64_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 256, 32768)
}

func Benchmark_and_1024_64_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 256, 65536)
}

func Benchmark_and_1024_64_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 512, 64)
}

func Benchmark_and_1024_64_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 512, 128)
}

func Benchmark_and_1024_64_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 512, 256)
}

func Benchmark_and_1024_64_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 512, 512)
}

func Benchmark_and_1024_64_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 512, 1024)
}

func Benchmark_and_1024_64_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 512, 8192)
}

func Benchmark_and_1024_64_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 512, 16384)
}

func Benchmark_and_1024_64_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 512, 32768)
}

func Benchmark_and_1024_64_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 512, 65536)
}

func Benchmark_and_1024_64_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 1024, 64)
}

func Benchmark_and_1024_64_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 1024, 128)
}

func Benchmark_and_1024_64_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 1024, 256)
}

func Benchmark_and_1024_64_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 1024, 512)
}

func Benchmark_and_1024_64_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 1024, 1024)
}

func Benchmark_and_1024_64_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 1024, 8192)
}

func Benchmark_and_1024_64_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 1024, 16384)
}

func Benchmark_and_1024_64_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 1024, 32768)
}

func Benchmark_and_1024_64_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 1024, 65536)
}

func Benchmark_and_1024_64_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 8192, 64)
}

func Benchmark_and_1024_64_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 8192, 128)
}

func Benchmark_and_1024_64_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 8192, 256)
}

func Benchmark_and_1024_64_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 8192, 512)
}

func Benchmark_and_1024_64_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 8192, 1024)
}

func Benchmark_and_1024_64_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 8192, 8192)
}

func Benchmark_and_1024_64_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 8192, 16384)
}

func Benchmark_and_1024_64_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 8192, 32768)
}

func Benchmark_and_1024_64_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 8192, 65536)
}

func Benchmark_and_1024_64_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 16384, 64)
}

func Benchmark_and_1024_64_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 16384, 128)
}

func Benchmark_and_1024_64_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 16384, 256)
}

func Benchmark_and_1024_64_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 16384, 512)
}

func Benchmark_and_1024_64_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 16384, 1024)
}

func Benchmark_and_1024_64_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 16384, 8192)
}

func Benchmark_and_1024_64_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 16384, 16384)
}

func Benchmark_and_1024_64_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 16384, 32768)
}

func Benchmark_and_1024_64_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 16384, 65536)
}

func Benchmark_and_1024_64_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 32768, 64)
}

func Benchmark_and_1024_64_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 32768, 128)
}

func Benchmark_and_1024_64_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 32768, 256)
}

func Benchmark_and_1024_64_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 32768, 512)
}

func Benchmark_and_1024_64_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 32768, 1024)
}

func Benchmark_and_1024_64_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 32768, 8192)
}

func Benchmark_and_1024_64_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 32768, 16384)
}

func Benchmark_and_1024_64_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 32768, 32768)
}

func Benchmark_and_1024_64_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 32768, 65536)
}

func Benchmark_and_1024_64_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 65536, 64)
}

func Benchmark_and_1024_64_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 65536, 128)
}

func Benchmark_and_1024_64_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 65536, 256)
}

func Benchmark_and_1024_64_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 65536, 512)
}

func Benchmark_and_1024_64_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 65536, 1024)
}

func Benchmark_and_1024_64_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 65536, 8192)
}

func Benchmark_and_1024_64_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 65536, 16384)
}

func Benchmark_and_1024_64_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 65536, 32768)
}

func Benchmark_and_1024_64_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 64, 65536, 65536)
}

func Benchmark_and_1024_1024_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 64, 64)
}

func Benchmark_and_1024_1024_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 64, 128)
}

func Benchmark_and_1024_1024_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 64, 256)
}

func Benchmark_and_1024_1024_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 64, 512)
}

func Benchmark_and_1024_1024_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 64, 1024)
}

func Benchmark_and_1024_1024_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 64, 8192)
}

func Benchmark_and_1024_1024_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 64, 16384)
}

func Benchmark_and_1024_1024_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 64, 32768)
}

func Benchmark_and_1024_1024_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 64, 65536)
}

func Benchmark_and_1024_1024_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 128, 64)
}

func Benchmark_and_1024_1024_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 128, 128)
}

func Benchmark_and_1024_1024_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 128, 256)
}

func Benchmark_and_1024_1024_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 128, 512)
}

func Benchmark_and_1024_1024_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 128, 1024)
}

func Benchmark_and_1024_1024_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 128, 8192)
}

func Benchmark_and_1024_1024_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 128, 16384)
}

func Benchmark_and_1024_1024_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 128, 32768)
}

func Benchmark_and_1024_1024_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 128, 65536)
}

func Benchmark_and_1024_1024_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 256, 64)
}

func Benchmark_and_1024_1024_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 256, 128)
}

func Benchmark_and_1024_1024_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 256, 256)
}

func Benchmark_and_1024_1024_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 256, 512)
}

func Benchmark_and_1024_1024_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 256, 1024)
}

func Benchmark_and_1024_1024_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 256, 8192)
}

func Benchmark_and_1024_1024_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 256, 16384)
}

func Benchmark_and_1024_1024_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 256, 32768)
}

func Benchmark_and_1024_1024_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 256, 65536)
}

func Benchmark_and_1024_1024_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 512, 64)
}

func Benchmark_and_1024_1024_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 512, 128)
}

func Benchmark_and_1024_1024_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 512, 256)
}

func Benchmark_and_1024_1024_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 512, 512)
}

func Benchmark_and_1024_1024_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 512, 1024)
}

func Benchmark_and_1024_1024_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 512, 8192)
}

func Benchmark_and_1024_1024_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 512, 16384)
}

func Benchmark_and_1024_1024_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 512, 32768)
}

func Benchmark_and_1024_1024_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 512, 65536)
}

func Benchmark_and_1024_1024_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 1024, 64)
}

func Benchmark_and_1024_1024_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 1024, 128)
}

func Benchmark_and_1024_1024_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 1024, 256)
}

func Benchmark_and_1024_1024_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 1024, 512)
}

func Benchmark_and_1024_1024_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 1024, 1024)
}

func Benchmark_and_1024_1024_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 1024, 8192)
}

func Benchmark_and_1024_1024_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 1024, 16384)
}

func Benchmark_and_1024_1024_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 1024, 32768)
}

func Benchmark_and_1024_1024_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 1024, 65536)
}

func Benchmark_and_1024_1024_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 8192, 64)
}

func Benchmark_and_1024_1024_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 8192, 128)
}

func Benchmark_and_1024_1024_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 8192, 256)
}

func Benchmark_and_1024_1024_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 8192, 512)
}

func Benchmark_and_1024_1024_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 8192, 1024)
}

func Benchmark_and_1024_1024_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 8192, 8192)
}

func Benchmark_and_1024_1024_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 8192, 16384)
}

func Benchmark_and_1024_1024_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 8192, 32768)
}

func Benchmark_and_1024_1024_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 8192, 65536)
}

func Benchmark_and_1024_1024_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 16384, 64)
}

func Benchmark_and_1024_1024_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 16384, 128)
}

func Benchmark_and_1024_1024_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 16384, 256)
}

func Benchmark_and_1024_1024_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 16384, 512)
}

func Benchmark_and_1024_1024_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 16384, 1024)
}

func Benchmark_and_1024_1024_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 16384, 8192)
}

func Benchmark_and_1024_1024_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 16384, 16384)
}

func Benchmark_and_1024_1024_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 16384, 32768)
}

func Benchmark_and_1024_1024_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 16384, 65536)
}

func Benchmark_and_1024_1024_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 32768, 64)
}

func Benchmark_and_1024_1024_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 32768, 128)
}

func Benchmark_and_1024_1024_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 32768, 256)
}

func Benchmark_and_1024_1024_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 32768, 512)
}

func Benchmark_and_1024_1024_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 32768, 1024)
}

func Benchmark_and_1024_1024_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 32768, 8192)
}

func Benchmark_and_1024_1024_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 32768, 16384)
}

func Benchmark_and_1024_1024_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 32768, 32768)
}

func Benchmark_and_1024_1024_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 32768, 65536)
}

func Benchmark_and_1024_1024_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 65536, 64)
}

func Benchmark_and_1024_1024_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 65536, 128)
}

func Benchmark_and_1024_1024_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 65536, 256)
}

func Benchmark_and_1024_1024_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 65536, 512)
}

func Benchmark_and_1024_1024_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 65536, 1024)
}

func Benchmark_and_1024_1024_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 65536, 8192)
}

func Benchmark_and_1024_1024_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 65536, 16384)
}

func Benchmark_and_1024_1024_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 65536, 32768)
}

func Benchmark_and_1024_1024_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 1024, 65536, 65536)
}

func Benchmark_and_1024_65536_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 64, 64)
}

func Benchmark_and_1024_65536_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 64, 128)
}

func Benchmark_and_1024_65536_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 64, 256)
}

func Benchmark_and_1024_65536_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 64, 512)
}

func Benchmark_and_1024_65536_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 64, 1024)
}

func Benchmark_and_1024_65536_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 64, 8192)
}

func Benchmark_and_1024_65536_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 64, 16384)
}

func Benchmark_and_1024_65536_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 64, 32768)
}

func Benchmark_and_1024_65536_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 64, 65536)
}

func Benchmark_and_1024_65536_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 128, 64)
}

func Benchmark_and_1024_65536_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 128, 128)
}

func Benchmark_and_1024_65536_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 128, 256)
}

func Benchmark_and_1024_65536_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 128, 512)
}

func Benchmark_and_1024_65536_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 128, 1024)
}

func Benchmark_and_1024_65536_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 128, 8192)
}

func Benchmark_and_1024_65536_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 128, 16384)
}

func Benchmark_and_1024_65536_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 128, 32768)
}

func Benchmark_and_1024_65536_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 128, 65536)
}

func Benchmark_and_1024_65536_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 256, 64)
}

func Benchmark_and_1024_65536_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 256, 128)
}

func Benchmark_and_1024_65536_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 256, 256)
}

func Benchmark_and_1024_65536_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 256, 512)
}

func Benchmark_and_1024_65536_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 256, 1024)
}

func Benchmark_and_1024_65536_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 256, 8192)
}

func Benchmark_and_1024_65536_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 256, 16384)
}

func Benchmark_and_1024_65536_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 256, 32768)
}

func Benchmark_and_1024_65536_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 256, 65536)
}

func Benchmark_and_1024_65536_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 512, 64)
}

func Benchmark_and_1024_65536_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 512, 128)
}

func Benchmark_and_1024_65536_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 512, 256)
}

func Benchmark_and_1024_65536_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 512, 512)
}

func Benchmark_and_1024_65536_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 512, 1024)
}

func Benchmark_and_1024_65536_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 512, 8192)
}

func Benchmark_and_1024_65536_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 512, 16384)
}

func Benchmark_and_1024_65536_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 512, 32768)
}

func Benchmark_and_1024_65536_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 512, 65536)
}

func Benchmark_and_1024_65536_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 1024, 64)
}

func Benchmark_and_1024_65536_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 1024, 128)
}

func Benchmark_and_1024_65536_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 1024, 256)
}

func Benchmark_and_1024_65536_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 1024, 512)
}

func Benchmark_and_1024_65536_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 1024, 1024)
}

func Benchmark_and_1024_65536_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 1024, 8192)
}

func Benchmark_and_1024_65536_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 1024, 16384)
}

func Benchmark_and_1024_65536_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 1024, 32768)
}

func Benchmark_and_1024_65536_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 1024, 65536)
}

func Benchmark_and_1024_65536_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 8192, 64)
}

func Benchmark_and_1024_65536_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 8192, 128)
}

func Benchmark_and_1024_65536_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 8192, 256)
}

func Benchmark_and_1024_65536_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 8192, 512)
}

func Benchmark_and_1024_65536_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 8192, 1024)
}

func Benchmark_and_1024_65536_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 8192, 8192)
}

func Benchmark_and_1024_65536_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 8192, 16384)
}

func Benchmark_and_1024_65536_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 8192, 32768)
}

func Benchmark_and_1024_65536_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 8192, 65536)
}

func Benchmark_and_1024_65536_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 16384, 64)
}

func Benchmark_and_1024_65536_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 16384, 128)
}

func Benchmark_and_1024_65536_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 16384, 256)
}

func Benchmark_and_1024_65536_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 16384, 512)
}

func Benchmark_and_1024_65536_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 16384, 1024)
}

func Benchmark_and_1024_65536_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 16384, 8192)
}

func Benchmark_and_1024_65536_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 16384, 16384)
}

func Benchmark_and_1024_65536_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 16384, 32768)
}

func Benchmark_and_1024_65536_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 16384, 65536)
}

func Benchmark_and_1024_65536_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 32768, 64)
}

func Benchmark_and_1024_65536_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 32768, 128)
}

func Benchmark_and_1024_65536_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 32768, 256)
}

func Benchmark_and_1024_65536_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 32768, 512)
}

func Benchmark_and_1024_65536_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 32768, 1024)
}

func Benchmark_and_1024_65536_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 32768, 8192)
}

func Benchmark_and_1024_65536_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 32768, 16384)
}

func Benchmark_and_1024_65536_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 32768, 32768)
}

func Benchmark_and_1024_65536_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 32768, 65536)
}

func Benchmark_and_1024_65536_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 65536, 64)
}

func Benchmark_and_1024_65536_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 65536, 128)
}

func Benchmark_and_1024_65536_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 65536, 256)
}

func Benchmark_and_1024_65536_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 65536, 512)
}

func Benchmark_and_1024_65536_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 65536, 1024)
}

func Benchmark_and_1024_65536_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 65536, 8192)
}

func Benchmark_and_1024_65536_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 65536, 16384)
}

func Benchmark_and_1024_65536_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 65536, 32768)
}

func Benchmark_and_1024_65536_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 1024, 65536, 65536, 65536)
}

func Benchmark_and_65536_64_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 64, 64)
}

func Benchmark_and_65536_64_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 64, 128)
}

func Benchmark_and_65536_64_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 64, 256)
}

func Benchmark_and_65536_64_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 64, 512)
}

func Benchmark_and_65536_64_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 64, 1024)
}

func Benchmark_and_65536_64_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 64, 8192)
}

func Benchmark_and_65536_64_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 64, 16384)
}

func Benchmark_and_65536_64_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 64, 32768)
}

func Benchmark_and_65536_64_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 64, 65536)
}

func Benchmark_and_65536_64_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 128, 64)
}

func Benchmark_and_65536_64_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 128, 128)
}

func Benchmark_and_65536_64_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 128, 256)
}

func Benchmark_and_65536_64_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 128, 512)
}

func Benchmark_and_65536_64_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 128, 1024)
}

func Benchmark_and_65536_64_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 128, 8192)
}

func Benchmark_and_65536_64_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 128, 16384)
}

func Benchmark_and_65536_64_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 128, 32768)
}

func Benchmark_and_65536_64_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 128, 65536)
}

func Benchmark_and_65536_64_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 256, 64)
}

func Benchmark_and_65536_64_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 256, 128)
}

func Benchmark_and_65536_64_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 256, 256)
}

func Benchmark_and_65536_64_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 256, 512)
}

func Benchmark_and_65536_64_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 256, 1024)
}

func Benchmark_and_65536_64_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 256, 8192)
}

func Benchmark_and_65536_64_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 256, 16384)
}

func Benchmark_and_65536_64_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 256, 32768)
}

func Benchmark_and_65536_64_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 256, 65536)
}

func Benchmark_and_65536_64_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 512, 64)
}

func Benchmark_and_65536_64_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 512, 128)
}

func Benchmark_and_65536_64_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 512, 256)
}

func Benchmark_and_65536_64_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 512, 512)
}

func Benchmark_and_65536_64_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 512, 1024)
}

func Benchmark_and_65536_64_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 512, 8192)
}

func Benchmark_and_65536_64_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 512, 16384)
}

func Benchmark_and_65536_64_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 512, 32768)
}

func Benchmark_and_65536_64_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 512, 65536)
}

func Benchmark_and_65536_64_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 1024, 64)
}

func Benchmark_and_65536_64_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 1024, 128)
}

func Benchmark_and_65536_64_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 1024, 256)
}

func Benchmark_and_65536_64_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 1024, 512)
}

func Benchmark_and_65536_64_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 1024, 1024)
}

func Benchmark_and_65536_64_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 1024, 8192)
}

func Benchmark_and_65536_64_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 1024, 16384)
}

func Benchmark_and_65536_64_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 1024, 32768)
}

func Benchmark_and_65536_64_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 1024, 65536)
}

func Benchmark_and_65536_64_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 8192, 64)
}

func Benchmark_and_65536_64_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 8192, 128)
}

func Benchmark_and_65536_64_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 8192, 256)
}

func Benchmark_and_65536_64_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 8192, 512)
}

func Benchmark_and_65536_64_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 8192, 1024)
}

func Benchmark_and_65536_64_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 8192, 8192)
}

func Benchmark_and_65536_64_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 8192, 16384)
}

func Benchmark_and_65536_64_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 8192, 32768)
}

func Benchmark_and_65536_64_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 8192, 65536)
}

func Benchmark_and_65536_64_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 16384, 64)
}

func Benchmark_and_65536_64_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 16384, 128)
}

func Benchmark_and_65536_64_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 16384, 256)
}

func Benchmark_and_65536_64_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 16384, 512)
}

func Benchmark_and_65536_64_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 16384, 1024)
}

func Benchmark_and_65536_64_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 16384, 8192)
}

func Benchmark_and_65536_64_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 16384, 16384)
}

func Benchmark_and_65536_64_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 16384, 32768)
}

func Benchmark_and_65536_64_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 16384, 65536)
}

func Benchmark_and_65536_64_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 32768, 64)
}

func Benchmark_and_65536_64_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 32768, 128)
}

func Benchmark_and_65536_64_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 32768, 256)
}

func Benchmark_and_65536_64_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 32768, 512)
}

func Benchmark_and_65536_64_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 32768, 1024)
}

func Benchmark_and_65536_64_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 32768, 8192)
}

func Benchmark_and_65536_64_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 32768, 16384)
}

func Benchmark_and_65536_64_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 32768, 32768)
}

func Benchmark_and_65536_64_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 32768, 65536)
}

func Benchmark_and_65536_64_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 65536, 64)
}

func Benchmark_and_65536_64_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 65536, 128)
}

func Benchmark_and_65536_64_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 65536, 256)
}

func Benchmark_and_65536_64_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 65536, 512)
}

func Benchmark_and_65536_64_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 65536, 1024)
}

func Benchmark_and_65536_64_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 65536, 8192)
}

func Benchmark_and_65536_64_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 65536, 16384)
}

func Benchmark_and_65536_64_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 65536, 32768)
}

func Benchmark_and_65536_64_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 64, 65536, 65536)
}

func Benchmark_and_65536_1024_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 64, 64)
}

func Benchmark_and_65536_1024_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 64, 128)
}

func Benchmark_and_65536_1024_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 64, 256)
}

func Benchmark_and_65536_1024_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 64, 512)
}

func Benchmark_and_65536_1024_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 64, 1024)
}

func Benchmark_and_65536_1024_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 64, 8192)
}

func Benchmark_and_65536_1024_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 64, 16384)
}

func Benchmark_and_65536_1024_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 64, 32768)
}

func Benchmark_and_65536_1024_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 64, 65536)
}

func Benchmark_and_65536_1024_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 128, 64)
}

func Benchmark_and_65536_1024_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 128, 128)
}

func Benchmark_and_65536_1024_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 128, 256)
}

func Benchmark_and_65536_1024_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 128, 512)
}

func Benchmark_and_65536_1024_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 128, 1024)
}

func Benchmark_and_65536_1024_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 128, 8192)
}

func Benchmark_and_65536_1024_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 128, 16384)
}

func Benchmark_and_65536_1024_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 128, 32768)
}

func Benchmark_and_65536_1024_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 128, 65536)
}

func Benchmark_and_65536_1024_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 256, 64)
}

func Benchmark_and_65536_1024_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 256, 128)
}

func Benchmark_and_65536_1024_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 256, 256)
}

func Benchmark_and_65536_1024_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 256, 512)
}

func Benchmark_and_65536_1024_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 256, 1024)
}

func Benchmark_and_65536_1024_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 256, 8192)
}

func Benchmark_and_65536_1024_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 256, 16384)
}

func Benchmark_and_65536_1024_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 256, 32768)
}

func Benchmark_and_65536_1024_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 256, 65536)
}

func Benchmark_and_65536_1024_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 512, 64)
}

func Benchmark_and_65536_1024_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 512, 128)
}

func Benchmark_and_65536_1024_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 512, 256)
}

func Benchmark_and_65536_1024_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 512, 512)
}

func Benchmark_and_65536_1024_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 512, 1024)
}

func Benchmark_and_65536_1024_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 512, 8192)
}

func Benchmark_and_65536_1024_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 512, 16384)
}

func Benchmark_and_65536_1024_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 512, 32768)
}

func Benchmark_and_65536_1024_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 512, 65536)
}

func Benchmark_and_65536_1024_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 1024, 64)
}

func Benchmark_and_65536_1024_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 1024, 128)
}

func Benchmark_and_65536_1024_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 1024, 256)
}

func Benchmark_and_65536_1024_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 1024, 512)
}

func Benchmark_and_65536_1024_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 1024, 1024)
}

func Benchmark_and_65536_1024_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 1024, 8192)
}

func Benchmark_and_65536_1024_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 1024, 16384)
}

func Benchmark_and_65536_1024_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 1024, 32768)
}

func Benchmark_and_65536_1024_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 1024, 65536)
}

func Benchmark_and_65536_1024_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 8192, 64)
}

func Benchmark_and_65536_1024_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 8192, 128)
}

func Benchmark_and_65536_1024_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 8192, 256)
}

func Benchmark_and_65536_1024_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 8192, 512)
}

func Benchmark_and_65536_1024_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 8192, 1024)
}

func Benchmark_and_65536_1024_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 8192, 8192)
}

func Benchmark_and_65536_1024_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 8192, 16384)
}

func Benchmark_and_65536_1024_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 8192, 32768)
}

func Benchmark_and_65536_1024_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 8192, 65536)
}

func Benchmark_and_65536_1024_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 16384, 64)
}

func Benchmark_and_65536_1024_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 16384, 128)
}

func Benchmark_and_65536_1024_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 16384, 256)
}

func Benchmark_and_65536_1024_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 16384, 512)
}

func Benchmark_and_65536_1024_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 16384, 1024)
}

func Benchmark_and_65536_1024_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 16384, 8192)
}

func Benchmark_and_65536_1024_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 16384, 16384)
}

func Benchmark_and_65536_1024_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 16384, 32768)
}

func Benchmark_and_65536_1024_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 16384, 65536)
}

func Benchmark_and_65536_1024_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 32768, 64)
}

func Benchmark_and_65536_1024_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 32768, 128)
}

func Benchmark_and_65536_1024_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 32768, 256)
}

func Benchmark_and_65536_1024_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 32768, 512)
}

func Benchmark_and_65536_1024_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 32768, 1024)
}

func Benchmark_and_65536_1024_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 32768, 8192)
}

func Benchmark_and_65536_1024_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 32768, 16384)
}

func Benchmark_and_65536_1024_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 32768, 32768)
}

func Benchmark_and_65536_1024_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 32768, 65536)
}

func Benchmark_and_65536_1024_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 65536, 64)
}

func Benchmark_and_65536_1024_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 65536, 128)
}

func Benchmark_and_65536_1024_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 65536, 256)
}

func Benchmark_and_65536_1024_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 65536, 512)
}

func Benchmark_and_65536_1024_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 65536, 1024)
}

func Benchmark_and_65536_1024_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 65536, 8192)
}

func Benchmark_and_65536_1024_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 65536, 16384)
}

func Benchmark_and_65536_1024_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 65536, 32768)
}

func Benchmark_and_65536_1024_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 1024, 65536, 65536)
}

func Benchmark_and_65536_65536_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 64, 64)
}

func Benchmark_and_65536_65536_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 64, 128)
}

func Benchmark_and_65536_65536_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 64, 256)
}

func Benchmark_and_65536_65536_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 64, 512)
}

func Benchmark_and_65536_65536_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 64, 1024)
}

func Benchmark_and_65536_65536_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 64, 8192)
}

func Benchmark_and_65536_65536_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 64, 16384)
}

func Benchmark_and_65536_65536_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 64, 32768)
}

func Benchmark_and_65536_65536_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 64, 65536)
}

func Benchmark_and_65536_65536_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 128, 64)
}

func Benchmark_and_65536_65536_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 128, 128)
}

func Benchmark_and_65536_65536_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 128, 256)
}

func Benchmark_and_65536_65536_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 128, 512)
}

func Benchmark_and_65536_65536_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 128, 1024)
}

func Benchmark_and_65536_65536_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 128, 8192)
}

func Benchmark_and_65536_65536_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 128, 16384)
}

func Benchmark_and_65536_65536_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 128, 32768)
}

func Benchmark_and_65536_65536_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 128, 65536)
}

func Benchmark_and_65536_65536_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 256, 64)
}

func Benchmark_and_65536_65536_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 256, 128)
}

func Benchmark_and_65536_65536_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 256, 256)
}

func Benchmark_and_65536_65536_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 256, 512)
}

func Benchmark_and_65536_65536_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 256, 1024)
}

func Benchmark_and_65536_65536_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 256, 8192)
}

func Benchmark_and_65536_65536_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 256, 16384)
}

func Benchmark_and_65536_65536_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 256, 32768)
}

func Benchmark_and_65536_65536_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 256, 65536)
}

func Benchmark_and_65536_65536_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 512, 64)
}

func Benchmark_and_65536_65536_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 512, 128)
}

func Benchmark_and_65536_65536_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 512, 256)
}

func Benchmark_and_65536_65536_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 512, 512)
}

func Benchmark_and_65536_65536_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 512, 1024)
}

func Benchmark_and_65536_65536_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 512, 8192)
}

func Benchmark_and_65536_65536_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 512, 16384)
}

func Benchmark_and_65536_65536_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 512, 32768)
}

func Benchmark_and_65536_65536_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 512, 65536)
}

func Benchmark_and_65536_65536_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 1024, 64)
}

func Benchmark_and_65536_65536_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 1024, 128)
}

func Benchmark_and_65536_65536_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 1024, 256)
}

func Benchmark_and_65536_65536_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 1024, 512)
}

func Benchmark_and_65536_65536_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 1024, 1024)
}

func Benchmark_and_65536_65536_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 1024, 8192)
}

func Benchmark_and_65536_65536_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 1024, 16384)
}

func Benchmark_and_65536_65536_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 1024, 32768)
}

func Benchmark_and_65536_65536_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 1024, 65536)
}

func Benchmark_and_65536_65536_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 8192, 64)
}

func Benchmark_and_65536_65536_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 8192, 128)
}

func Benchmark_and_65536_65536_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 8192, 256)
}

func Benchmark_and_65536_65536_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 8192, 512)
}

func Benchmark_and_65536_65536_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 8192, 1024)
}

func Benchmark_and_65536_65536_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 8192, 8192)
}

func Benchmark_and_65536_65536_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 8192, 16384)
}

func Benchmark_and_65536_65536_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 8192, 32768)
}

func Benchmark_and_65536_65536_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 8192, 65536)
}

func Benchmark_and_65536_65536_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 16384, 64)
}

func Benchmark_and_65536_65536_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 16384, 128)
}

func Benchmark_and_65536_65536_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 16384, 256)
}

func Benchmark_and_65536_65536_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 16384, 512)
}

func Benchmark_and_65536_65536_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 16384, 1024)
}

func Benchmark_and_65536_65536_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 16384, 8192)
}

func Benchmark_and_65536_65536_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 16384, 16384)
}

func Benchmark_and_65536_65536_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 16384, 32768)
}

func Benchmark_and_65536_65536_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 16384, 65536)
}

func Benchmark_and_65536_65536_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 32768, 64)
}

func Benchmark_and_65536_65536_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 32768, 128)
}

func Benchmark_and_65536_65536_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 32768, 256)
}

func Benchmark_and_65536_65536_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 32768, 512)
}

func Benchmark_and_65536_65536_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 32768, 1024)
}

func Benchmark_and_65536_65536_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 32768, 8192)
}

func Benchmark_and_65536_65536_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 32768, 16384)
}

func Benchmark_and_65536_65536_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 32768, 32768)
}

func Benchmark_and_65536_65536_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 32768, 65536)
}

func Benchmark_and_65536_65536_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 65536, 64)
}

func Benchmark_and_65536_65536_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 65536, 128)
}

func Benchmark_and_65536_65536_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 65536, 256)
}

func Benchmark_and_65536_65536_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 65536, 512)
}

func Benchmark_and_65536_65536_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 65536, 1024)
}

func Benchmark_and_65536_65536_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 65536, 8192)
}

func Benchmark_and_65536_65536_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 65536, 16384)
}

func Benchmark_and_65536_65536_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 65536, 32768)
}

func Benchmark_and_65536_65536_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "and", 65536, 65536, 65536, 65536)
}

func Benchmark_or_64_64_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 64, 64)
}

func Benchmark_or_64_64_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 64, 128)
}

func Benchmark_or_64_64_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 64, 256)
}

func Benchmark_or_64_64_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 64, 512)
}

func Benchmark_or_64_64_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 64, 1024)
}

func Benchmark_or_64_64_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 64, 8192)
}

func Benchmark_or_64_64_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 64, 16384)
}

func Benchmark_or_64_64_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 64, 32768)
}

func Benchmark_or_64_64_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 64, 65536)
}

func Benchmark_or_64_64_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 128, 64)
}

func Benchmark_or_64_64_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 128, 128)
}

func Benchmark_or_64_64_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 128, 256)
}

func Benchmark_or_64_64_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 128, 512)
}

func Benchmark_or_64_64_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 128, 1024)
}

func Benchmark_or_64_64_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 128, 8192)
}

func Benchmark_or_64_64_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 128, 16384)
}

func Benchmark_or_64_64_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 128, 32768)
}

func Benchmark_or_64_64_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 128, 65536)
}

func Benchmark_or_64_64_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 256, 64)
}

func Benchmark_or_64_64_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 256, 128)
}

func Benchmark_or_64_64_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 256, 256)
}

func Benchmark_or_64_64_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 256, 512)
}

func Benchmark_or_64_64_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 256, 1024)
}

func Benchmark_or_64_64_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 256, 8192)
}

func Benchmark_or_64_64_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 256, 16384)
}

func Benchmark_or_64_64_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 256, 32768)
}

func Benchmark_or_64_64_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 256, 65536)
}

func Benchmark_or_64_64_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 512, 64)
}

func Benchmark_or_64_64_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 512, 128)
}

func Benchmark_or_64_64_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 512, 256)
}

func Benchmark_or_64_64_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 512, 512)
}

func Benchmark_or_64_64_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 512, 1024)
}

func Benchmark_or_64_64_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 512, 8192)
}

func Benchmark_or_64_64_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 512, 16384)
}

func Benchmark_or_64_64_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 512, 32768)
}

func Benchmark_or_64_64_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 512, 65536)
}

func Benchmark_or_64_64_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 1024, 64)
}

func Benchmark_or_64_64_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 1024, 128)
}

func Benchmark_or_64_64_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 1024, 256)
}

func Benchmark_or_64_64_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 1024, 512)
}

func Benchmark_or_64_64_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 1024, 1024)
}

func Benchmark_or_64_64_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 1024, 8192)
}

func Benchmark_or_64_64_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 1024, 16384)
}

func Benchmark_or_64_64_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 1024, 32768)
}

func Benchmark_or_64_64_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 1024, 65536)
}

func Benchmark_or_64_64_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 8192, 64)
}

func Benchmark_or_64_64_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 8192, 128)
}

func Benchmark_or_64_64_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 8192, 256)
}

func Benchmark_or_64_64_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 8192, 512)
}

func Benchmark_or_64_64_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 8192, 1024)
}

func Benchmark_or_64_64_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 8192, 8192)
}

func Benchmark_or_64_64_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 8192, 16384)
}

func Benchmark_or_64_64_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 8192, 32768)
}

func Benchmark_or_64_64_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 8192, 65536)
}

func Benchmark_or_64_64_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 16384, 64)
}

func Benchmark_or_64_64_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 16384, 128)
}

func Benchmark_or_64_64_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 16384, 256)
}

func Benchmark_or_64_64_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 16384, 512)
}

func Benchmark_or_64_64_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 16384, 1024)
}

func Benchmark_or_64_64_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 16384, 8192)
}

func Benchmark_or_64_64_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 16384, 16384)
}

func Benchmark_or_64_64_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 16384, 32768)
}

func Benchmark_or_64_64_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 16384, 65536)
}

func Benchmark_or_64_64_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 32768, 64)
}

func Benchmark_or_64_64_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 32768, 128)
}

func Benchmark_or_64_64_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 32768, 256)
}

func Benchmark_or_64_64_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 32768, 512)
}

func Benchmark_or_64_64_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 32768, 1024)
}

func Benchmark_or_64_64_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 32768, 8192)
}

func Benchmark_or_64_64_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 32768, 16384)
}

func Benchmark_or_64_64_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 32768, 32768)
}

func Benchmark_or_64_64_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 32768, 65536)
}

func Benchmark_or_64_64_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 65536, 64)
}

func Benchmark_or_64_64_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 65536, 128)
}

func Benchmark_or_64_64_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 65536, 256)
}

func Benchmark_or_64_64_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 65536, 512)
}

func Benchmark_or_64_64_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 65536, 1024)
}

func Benchmark_or_64_64_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 65536, 8192)
}

func Benchmark_or_64_64_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 65536, 16384)
}

func Benchmark_or_64_64_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 65536, 32768)
}

func Benchmark_or_64_64_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 64, 65536, 65536)
}

func Benchmark_or_64_1024_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 64, 64)
}

func Benchmark_or_64_1024_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 64, 128)
}

func Benchmark_or_64_1024_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 64, 256)
}

func Benchmark_or_64_1024_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 64, 512)
}

func Benchmark_or_64_1024_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 64, 1024)
}

func Benchmark_or_64_1024_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 64, 8192)
}

func Benchmark_or_64_1024_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 64, 16384)
}

func Benchmark_or_64_1024_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 64, 32768)
}

func Benchmark_or_64_1024_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 64, 65536)
}

func Benchmark_or_64_1024_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 128, 64)
}

func Benchmark_or_64_1024_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 128, 128)
}

func Benchmark_or_64_1024_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 128, 256)
}

func Benchmark_or_64_1024_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 128, 512)
}

func Benchmark_or_64_1024_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 128, 1024)
}

func Benchmark_or_64_1024_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 128, 8192)
}

func Benchmark_or_64_1024_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 128, 16384)
}

func Benchmark_or_64_1024_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 128, 32768)
}

func Benchmark_or_64_1024_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 128, 65536)
}

func Benchmark_or_64_1024_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 256, 64)
}

func Benchmark_or_64_1024_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 256, 128)
}

func Benchmark_or_64_1024_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 256, 256)
}

func Benchmark_or_64_1024_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 256, 512)
}

func Benchmark_or_64_1024_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 256, 1024)
}

func Benchmark_or_64_1024_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 256, 8192)
}

func Benchmark_or_64_1024_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 256, 16384)
}

func Benchmark_or_64_1024_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 256, 32768)
}

func Benchmark_or_64_1024_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 256, 65536)
}

func Benchmark_or_64_1024_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 512, 64)
}

func Benchmark_or_64_1024_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 512, 128)
}

func Benchmark_or_64_1024_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 512, 256)
}

func Benchmark_or_64_1024_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 512, 512)
}

func Benchmark_or_64_1024_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 512, 1024)
}

func Benchmark_or_64_1024_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 512, 8192)
}

func Benchmark_or_64_1024_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 512, 16384)
}

func Benchmark_or_64_1024_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 512, 32768)
}

func Benchmark_or_64_1024_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 512, 65536)
}

func Benchmark_or_64_1024_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 1024, 64)
}

func Benchmark_or_64_1024_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 1024, 128)
}

func Benchmark_or_64_1024_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 1024, 256)
}

func Benchmark_or_64_1024_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 1024, 512)
}

func Benchmark_or_64_1024_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 1024, 1024)
}

func Benchmark_or_64_1024_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 1024, 8192)
}

func Benchmark_or_64_1024_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 1024, 16384)
}

func Benchmark_or_64_1024_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 1024, 32768)
}

func Benchmark_or_64_1024_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 1024, 65536)
}

func Benchmark_or_64_1024_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 8192, 64)
}

func Benchmark_or_64_1024_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 8192, 128)
}

func Benchmark_or_64_1024_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 8192, 256)
}

func Benchmark_or_64_1024_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 8192, 512)
}

func Benchmark_or_64_1024_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 8192, 1024)
}

func Benchmark_or_64_1024_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 8192, 8192)
}

func Benchmark_or_64_1024_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 8192, 16384)
}

func Benchmark_or_64_1024_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 8192, 32768)
}

func Benchmark_or_64_1024_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 8192, 65536)
}

func Benchmark_or_64_1024_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 16384, 64)
}

func Benchmark_or_64_1024_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 16384, 128)
}

func Benchmark_or_64_1024_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 16384, 256)
}

func Benchmark_or_64_1024_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 16384, 512)
}

func Benchmark_or_64_1024_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 16384, 1024)
}

func Benchmark_or_64_1024_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 16384, 8192)
}

func Benchmark_or_64_1024_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 16384, 16384)
}

func Benchmark_or_64_1024_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 16384, 32768)
}

func Benchmark_or_64_1024_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 16384, 65536)
}

func Benchmark_or_64_1024_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 32768, 64)
}

func Benchmark_or_64_1024_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 32768, 128)
}

func Benchmark_or_64_1024_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 32768, 256)
}

func Benchmark_or_64_1024_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 32768, 512)
}

func Benchmark_or_64_1024_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 32768, 1024)
}

func Benchmark_or_64_1024_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 32768, 8192)
}

func Benchmark_or_64_1024_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 32768, 16384)
}

func Benchmark_or_64_1024_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 32768, 32768)
}

func Benchmark_or_64_1024_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 32768, 65536)
}

func Benchmark_or_64_1024_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 65536, 64)
}

func Benchmark_or_64_1024_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 65536, 128)
}

func Benchmark_or_64_1024_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 65536, 256)
}

func Benchmark_or_64_1024_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 65536, 512)
}

func Benchmark_or_64_1024_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 65536, 1024)
}

func Benchmark_or_64_1024_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 65536, 8192)
}

func Benchmark_or_64_1024_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 65536, 16384)
}

func Benchmark_or_64_1024_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 65536, 32768)
}

func Benchmark_or_64_1024_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 1024, 65536, 65536)
}

func Benchmark_or_64_65536_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 64, 64)
}

func Benchmark_or_64_65536_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 64, 128)
}

func Benchmark_or_64_65536_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 64, 256)
}

func Benchmark_or_64_65536_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 64, 512)
}

func Benchmark_or_64_65536_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 64, 1024)
}

func Benchmark_or_64_65536_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 64, 8192)
}

func Benchmark_or_64_65536_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 64, 16384)
}

func Benchmark_or_64_65536_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 64, 32768)
}

func Benchmark_or_64_65536_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 64, 65536)
}

func Benchmark_or_64_65536_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 128, 64)
}

func Benchmark_or_64_65536_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 128, 128)
}

func Benchmark_or_64_65536_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 128, 256)
}

func Benchmark_or_64_65536_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 128, 512)
}

func Benchmark_or_64_65536_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 128, 1024)
}

func Benchmark_or_64_65536_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 128, 8192)
}

func Benchmark_or_64_65536_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 128, 16384)
}

func Benchmark_or_64_65536_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 128, 32768)
}

func Benchmark_or_64_65536_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 128, 65536)
}

func Benchmark_or_64_65536_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 256, 64)
}

func Benchmark_or_64_65536_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 256, 128)
}

func Benchmark_or_64_65536_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 256, 256)
}

func Benchmark_or_64_65536_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 256, 512)
}

func Benchmark_or_64_65536_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 256, 1024)
}

func Benchmark_or_64_65536_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 256, 8192)
}

func Benchmark_or_64_65536_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 256, 16384)
}

func Benchmark_or_64_65536_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 256, 32768)
}

func Benchmark_or_64_65536_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 256, 65536)
}

func Benchmark_or_64_65536_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 512, 64)
}

func Benchmark_or_64_65536_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 512, 128)
}

func Benchmark_or_64_65536_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 512, 256)
}

func Benchmark_or_64_65536_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 512, 512)
}

func Benchmark_or_64_65536_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 512, 1024)
}

func Benchmark_or_64_65536_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 512, 8192)
}

func Benchmark_or_64_65536_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 512, 16384)
}

func Benchmark_or_64_65536_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 512, 32768)
}

func Benchmark_or_64_65536_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 512, 65536)
}

func Benchmark_or_64_65536_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 1024, 64)
}

func Benchmark_or_64_65536_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 1024, 128)
}

func Benchmark_or_64_65536_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 1024, 256)
}

func Benchmark_or_64_65536_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 1024, 512)
}

func Benchmark_or_64_65536_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 1024, 1024)
}

func Benchmark_or_64_65536_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 1024, 8192)
}

func Benchmark_or_64_65536_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 1024, 16384)
}

func Benchmark_or_64_65536_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 1024, 32768)
}

func Benchmark_or_64_65536_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 1024, 65536)
}

func Benchmark_or_64_65536_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 8192, 64)
}

func Benchmark_or_64_65536_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 8192, 128)
}

func Benchmark_or_64_65536_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 8192, 256)
}

func Benchmark_or_64_65536_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 8192, 512)
}

func Benchmark_or_64_65536_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 8192, 1024)
}

func Benchmark_or_64_65536_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 8192, 8192)
}

func Benchmark_or_64_65536_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 8192, 16384)
}

func Benchmark_or_64_65536_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 8192, 32768)
}

func Benchmark_or_64_65536_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 8192, 65536)
}

func Benchmark_or_64_65536_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 16384, 64)
}

func Benchmark_or_64_65536_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 16384, 128)
}

func Benchmark_or_64_65536_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 16384, 256)
}

func Benchmark_or_64_65536_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 16384, 512)
}

func Benchmark_or_64_65536_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 16384, 1024)
}

func Benchmark_or_64_65536_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 16384, 8192)
}

func Benchmark_or_64_65536_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 16384, 16384)
}

func Benchmark_or_64_65536_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 16384, 32768)
}

func Benchmark_or_64_65536_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 16384, 65536)
}

func Benchmark_or_64_65536_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 32768, 64)
}

func Benchmark_or_64_65536_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 32768, 128)
}

func Benchmark_or_64_65536_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 32768, 256)
}

func Benchmark_or_64_65536_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 32768, 512)
}

func Benchmark_or_64_65536_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 32768, 1024)
}

func Benchmark_or_64_65536_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 32768, 8192)
}

func Benchmark_or_64_65536_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 32768, 16384)
}

func Benchmark_or_64_65536_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 32768, 32768)
}

func Benchmark_or_64_65536_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 32768, 65536)
}

func Benchmark_or_64_65536_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 65536, 64)
}

func Benchmark_or_64_65536_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 65536, 128)
}

func Benchmark_or_64_65536_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 65536, 256)
}

func Benchmark_or_64_65536_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 65536, 512)
}

func Benchmark_or_64_65536_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 65536, 1024)
}

func Benchmark_or_64_65536_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 65536, 8192)
}

func Benchmark_or_64_65536_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 65536, 16384)
}

func Benchmark_or_64_65536_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 65536, 32768)
}

func Benchmark_or_64_65536_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 64, 65536, 65536, 65536)
}

func Benchmark_or_1024_64_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 64, 64)
}

func Benchmark_or_1024_64_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 64, 128)
}

func Benchmark_or_1024_64_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 64, 256)
}

func Benchmark_or_1024_64_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 64, 512)
}

func Benchmark_or_1024_64_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 64, 1024)
}

func Benchmark_or_1024_64_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 64, 8192)
}

func Benchmark_or_1024_64_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 64, 16384)
}

func Benchmark_or_1024_64_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 64, 32768)
}

func Benchmark_or_1024_64_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 64, 65536)
}

func Benchmark_or_1024_64_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 128, 64)
}

func Benchmark_or_1024_64_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 128, 128)
}

func Benchmark_or_1024_64_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 128, 256)
}

func Benchmark_or_1024_64_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 128, 512)
}

func Benchmark_or_1024_64_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 128, 1024)
}

func Benchmark_or_1024_64_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 128, 8192)
}

func Benchmark_or_1024_64_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 128, 16384)
}

func Benchmark_or_1024_64_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 128, 32768)
}

func Benchmark_or_1024_64_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 128, 65536)
}

func Benchmark_or_1024_64_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 256, 64)
}

func Benchmark_or_1024_64_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 256, 128)
}

func Benchmark_or_1024_64_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 256, 256)
}

func Benchmark_or_1024_64_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 256, 512)
}

func Benchmark_or_1024_64_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 256, 1024)
}

func Benchmark_or_1024_64_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 256, 8192)
}

func Benchmark_or_1024_64_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 256, 16384)
}

func Benchmark_or_1024_64_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 256, 32768)
}

func Benchmark_or_1024_64_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 256, 65536)
}

func Benchmark_or_1024_64_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 512, 64)
}

func Benchmark_or_1024_64_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 512, 128)
}

func Benchmark_or_1024_64_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 512, 256)
}

func Benchmark_or_1024_64_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 512, 512)
}

func Benchmark_or_1024_64_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 512, 1024)
}

func Benchmark_or_1024_64_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 512, 8192)
}

func Benchmark_or_1024_64_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 512, 16384)
}

func Benchmark_or_1024_64_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 512, 32768)
}

func Benchmark_or_1024_64_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 512, 65536)
}

func Benchmark_or_1024_64_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 1024, 64)
}

func Benchmark_or_1024_64_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 1024, 128)
}

func Benchmark_or_1024_64_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 1024, 256)
}

func Benchmark_or_1024_64_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 1024, 512)
}

func Benchmark_or_1024_64_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 1024, 1024)
}

func Benchmark_or_1024_64_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 1024, 8192)
}

func Benchmark_or_1024_64_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 1024, 16384)
}

func Benchmark_or_1024_64_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 1024, 32768)
}

func Benchmark_or_1024_64_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 1024, 65536)
}

func Benchmark_or_1024_64_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 8192, 64)
}

func Benchmark_or_1024_64_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 8192, 128)
}

func Benchmark_or_1024_64_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 8192, 256)
}

func Benchmark_or_1024_64_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 8192, 512)
}

func Benchmark_or_1024_64_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 8192, 1024)
}

func Benchmark_or_1024_64_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 8192, 8192)
}

func Benchmark_or_1024_64_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 8192, 16384)
}

func Benchmark_or_1024_64_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 8192, 32768)
}

func Benchmark_or_1024_64_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 8192, 65536)
}

func Benchmark_or_1024_64_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 16384, 64)
}

func Benchmark_or_1024_64_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 16384, 128)
}

func Benchmark_or_1024_64_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 16384, 256)
}

func Benchmark_or_1024_64_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 16384, 512)
}

func Benchmark_or_1024_64_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 16384, 1024)
}

func Benchmark_or_1024_64_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 16384, 8192)
}

func Benchmark_or_1024_64_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 16384, 16384)
}

func Benchmark_or_1024_64_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 16384, 32768)
}

func Benchmark_or_1024_64_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 16384, 65536)
}

func Benchmark_or_1024_64_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 32768, 64)
}

func Benchmark_or_1024_64_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 32768, 128)
}

func Benchmark_or_1024_64_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 32768, 256)
}

func Benchmark_or_1024_64_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 32768, 512)
}

func Benchmark_or_1024_64_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 32768, 1024)
}

func Benchmark_or_1024_64_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 32768, 8192)
}

func Benchmark_or_1024_64_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 32768, 16384)
}

func Benchmark_or_1024_64_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 32768, 32768)
}

func Benchmark_or_1024_64_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 32768, 65536)
}

func Benchmark_or_1024_64_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 65536, 64)
}

func Benchmark_or_1024_64_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 65536, 128)
}

func Benchmark_or_1024_64_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 65536, 256)
}

func Benchmark_or_1024_64_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 65536, 512)
}

func Benchmark_or_1024_64_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 65536, 1024)
}

func Benchmark_or_1024_64_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 65536, 8192)
}

func Benchmark_or_1024_64_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 65536, 16384)
}

func Benchmark_or_1024_64_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 65536, 32768)
}

func Benchmark_or_1024_64_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 64, 65536, 65536)
}

func Benchmark_or_1024_1024_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 64, 64)
}

func Benchmark_or_1024_1024_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 64, 128)
}

func Benchmark_or_1024_1024_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 64, 256)
}

func Benchmark_or_1024_1024_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 64, 512)
}

func Benchmark_or_1024_1024_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 64, 1024)
}

func Benchmark_or_1024_1024_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 64, 8192)
}

func Benchmark_or_1024_1024_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 64, 16384)
}

func Benchmark_or_1024_1024_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 64, 32768)
}

func Benchmark_or_1024_1024_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 64, 65536)
}

func Benchmark_or_1024_1024_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 128, 64)
}

func Benchmark_or_1024_1024_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 128, 128)
}

func Benchmark_or_1024_1024_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 128, 256)
}

func Benchmark_or_1024_1024_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 128, 512)
}

func Benchmark_or_1024_1024_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 128, 1024)
}

func Benchmark_or_1024_1024_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 128, 8192)
}

func Benchmark_or_1024_1024_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 128, 16384)
}

func Benchmark_or_1024_1024_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 128, 32768)
}

func Benchmark_or_1024_1024_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 128, 65536)
}

func Benchmark_or_1024_1024_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 256, 64)
}

func Benchmark_or_1024_1024_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 256, 128)
}

func Benchmark_or_1024_1024_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 256, 256)
}

func Benchmark_or_1024_1024_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 256, 512)
}

func Benchmark_or_1024_1024_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 256, 1024)
}

func Benchmark_or_1024_1024_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 256, 8192)
}

func Benchmark_or_1024_1024_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 256, 16384)
}

func Benchmark_or_1024_1024_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 256, 32768)
}

func Benchmark_or_1024_1024_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 256, 65536)
}

func Benchmark_or_1024_1024_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 512, 64)
}

func Benchmark_or_1024_1024_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 512, 128)
}

func Benchmark_or_1024_1024_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 512, 256)
}

func Benchmark_or_1024_1024_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 512, 512)
}

func Benchmark_or_1024_1024_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 512, 1024)
}

func Benchmark_or_1024_1024_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 512, 8192)
}

func Benchmark_or_1024_1024_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 512, 16384)
}

func Benchmark_or_1024_1024_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 512, 32768)
}

func Benchmark_or_1024_1024_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 512, 65536)
}

func Benchmark_or_1024_1024_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 1024, 64)
}

func Benchmark_or_1024_1024_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 1024, 128)
}

func Benchmark_or_1024_1024_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 1024, 256)
}

func Benchmark_or_1024_1024_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 1024, 512)
}

func Benchmark_or_1024_1024_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 1024, 1024)
}

func Benchmark_or_1024_1024_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 1024, 8192)
}

func Benchmark_or_1024_1024_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 1024, 16384)
}

func Benchmark_or_1024_1024_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 1024, 32768)
}

func Benchmark_or_1024_1024_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 1024, 65536)
}

func Benchmark_or_1024_1024_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 8192, 64)
}

func Benchmark_or_1024_1024_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 8192, 128)
}

func Benchmark_or_1024_1024_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 8192, 256)
}

func Benchmark_or_1024_1024_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 8192, 512)
}

func Benchmark_or_1024_1024_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 8192, 1024)
}

func Benchmark_or_1024_1024_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 8192, 8192)
}

func Benchmark_or_1024_1024_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 8192, 16384)
}

func Benchmark_or_1024_1024_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 8192, 32768)
}

func Benchmark_or_1024_1024_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 8192, 65536)
}

func Benchmark_or_1024_1024_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 16384, 64)
}

func Benchmark_or_1024_1024_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 16384, 128)
}

func Benchmark_or_1024_1024_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 16384, 256)
}

func Benchmark_or_1024_1024_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 16384, 512)
}

func Benchmark_or_1024_1024_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 16384, 1024)
}

func Benchmark_or_1024_1024_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 16384, 8192)
}

func Benchmark_or_1024_1024_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 16384, 16384)
}

func Benchmark_or_1024_1024_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 16384, 32768)
}

func Benchmark_or_1024_1024_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 16384, 65536)
}

func Benchmark_or_1024_1024_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 32768, 64)
}

func Benchmark_or_1024_1024_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 32768, 128)
}

func Benchmark_or_1024_1024_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 32768, 256)
}

func Benchmark_or_1024_1024_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 32768, 512)
}

func Benchmark_or_1024_1024_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 32768, 1024)
}

func Benchmark_or_1024_1024_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 32768, 8192)
}

func Benchmark_or_1024_1024_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 32768, 16384)
}

func Benchmark_or_1024_1024_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 32768, 32768)
}

func Benchmark_or_1024_1024_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 32768, 65536)
}

func Benchmark_or_1024_1024_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 65536, 64)
}

func Benchmark_or_1024_1024_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 65536, 128)
}

func Benchmark_or_1024_1024_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 65536, 256)
}

func Benchmark_or_1024_1024_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 65536, 512)
}

func Benchmark_or_1024_1024_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 65536, 1024)
}

func Benchmark_or_1024_1024_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 65536, 8192)
}

func Benchmark_or_1024_1024_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 65536, 16384)
}

func Benchmark_or_1024_1024_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 65536, 32768)
}

func Benchmark_or_1024_1024_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 1024, 65536, 65536)
}

func Benchmark_or_1024_65536_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 64, 64)
}

func Benchmark_or_1024_65536_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 64, 128)
}

func Benchmark_or_1024_65536_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 64, 256)
}

func Benchmark_or_1024_65536_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 64, 512)
}

func Benchmark_or_1024_65536_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 64, 1024)
}

func Benchmark_or_1024_65536_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 64, 8192)
}

func Benchmark_or_1024_65536_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 64, 16384)
}

func Benchmark_or_1024_65536_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 64, 32768)
}

func Benchmark_or_1024_65536_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 64, 65536)
}

func Benchmark_or_1024_65536_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 128, 64)
}

func Benchmark_or_1024_65536_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 128, 128)
}

func Benchmark_or_1024_65536_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 128, 256)
}

func Benchmark_or_1024_65536_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 128, 512)
}

func Benchmark_or_1024_65536_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 128, 1024)
}

func Benchmark_or_1024_65536_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 128, 8192)
}

func Benchmark_or_1024_65536_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 128, 16384)
}

func Benchmark_or_1024_65536_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 128, 32768)
}

func Benchmark_or_1024_65536_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 128, 65536)
}

func Benchmark_or_1024_65536_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 256, 64)
}

func Benchmark_or_1024_65536_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 256, 128)
}

func Benchmark_or_1024_65536_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 256, 256)
}

func Benchmark_or_1024_65536_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 256, 512)
}

func Benchmark_or_1024_65536_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 256, 1024)
}

func Benchmark_or_1024_65536_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 256, 8192)
}

func Benchmark_or_1024_65536_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 256, 16384)
}

func Benchmark_or_1024_65536_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 256, 32768)
}

func Benchmark_or_1024_65536_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 256, 65536)
}

func Benchmark_or_1024_65536_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 512, 64)
}

func Benchmark_or_1024_65536_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 512, 128)
}

func Benchmark_or_1024_65536_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 512, 256)
}

func Benchmark_or_1024_65536_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 512, 512)
}

func Benchmark_or_1024_65536_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 512, 1024)
}

func Benchmark_or_1024_65536_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 512, 8192)
}

func Benchmark_or_1024_65536_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 512, 16384)
}

func Benchmark_or_1024_65536_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 512, 32768)
}

func Benchmark_or_1024_65536_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 512, 65536)
}

func Benchmark_or_1024_65536_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 1024, 64)
}

func Benchmark_or_1024_65536_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 1024, 128)
}

func Benchmark_or_1024_65536_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 1024, 256)
}

func Benchmark_or_1024_65536_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 1024, 512)
}

func Benchmark_or_1024_65536_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 1024, 1024)
}

func Benchmark_or_1024_65536_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 1024, 8192)
}

func Benchmark_or_1024_65536_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 1024, 16384)
}

func Benchmark_or_1024_65536_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 1024, 32768)
}

func Benchmark_or_1024_65536_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 1024, 65536)
}

func Benchmark_or_1024_65536_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 8192, 64)
}

func Benchmark_or_1024_65536_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 8192, 128)
}

func Benchmark_or_1024_65536_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 8192, 256)
}

func Benchmark_or_1024_65536_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 8192, 512)
}

func Benchmark_or_1024_65536_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 8192, 1024)
}

func Benchmark_or_1024_65536_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 8192, 8192)
}

func Benchmark_or_1024_65536_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 8192, 16384)
}

func Benchmark_or_1024_65536_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 8192, 32768)
}

func Benchmark_or_1024_65536_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 8192, 65536)
}

func Benchmark_or_1024_65536_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 16384, 64)
}

func Benchmark_or_1024_65536_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 16384, 128)
}

func Benchmark_or_1024_65536_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 16384, 256)
}

func Benchmark_or_1024_65536_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 16384, 512)
}

func Benchmark_or_1024_65536_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 16384, 1024)
}

func Benchmark_or_1024_65536_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 16384, 8192)
}

func Benchmark_or_1024_65536_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 16384, 16384)
}

func Benchmark_or_1024_65536_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 16384, 32768)
}

func Benchmark_or_1024_65536_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 16384, 65536)
}

func Benchmark_or_1024_65536_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 32768, 64)
}

func Benchmark_or_1024_65536_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 32768, 128)
}

func Benchmark_or_1024_65536_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 32768, 256)
}

func Benchmark_or_1024_65536_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 32768, 512)
}

func Benchmark_or_1024_65536_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 32768, 1024)
}

func Benchmark_or_1024_65536_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 32768, 8192)
}

func Benchmark_or_1024_65536_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 32768, 16384)
}

func Benchmark_or_1024_65536_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 32768, 32768)
}

func Benchmark_or_1024_65536_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 32768, 65536)
}

func Benchmark_or_1024_65536_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 65536, 64)
}

func Benchmark_or_1024_65536_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 65536, 128)
}

func Benchmark_or_1024_65536_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 65536, 256)
}

func Benchmark_or_1024_65536_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 65536, 512)
}

func Benchmark_or_1024_65536_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 65536, 1024)
}

func Benchmark_or_1024_65536_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 65536, 8192)
}

func Benchmark_or_1024_65536_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 65536, 16384)
}

func Benchmark_or_1024_65536_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 65536, 32768)
}

func Benchmark_or_1024_65536_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 1024, 65536, 65536, 65536)
}

func Benchmark_or_65536_64_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 64, 64)
}

func Benchmark_or_65536_64_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 64, 128)
}

func Benchmark_or_65536_64_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 64, 256)
}

func Benchmark_or_65536_64_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 64, 512)
}

func Benchmark_or_65536_64_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 64, 1024)
}

func Benchmark_or_65536_64_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 64, 8192)
}

func Benchmark_or_65536_64_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 64, 16384)
}

func Benchmark_or_65536_64_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 64, 32768)
}

func Benchmark_or_65536_64_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 64, 65536)
}

func Benchmark_or_65536_64_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 128, 64)
}

func Benchmark_or_65536_64_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 128, 128)
}

func Benchmark_or_65536_64_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 128, 256)
}

func Benchmark_or_65536_64_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 128, 512)
}

func Benchmark_or_65536_64_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 128, 1024)
}

func Benchmark_or_65536_64_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 128, 8192)
}

func Benchmark_or_65536_64_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 128, 16384)
}

func Benchmark_or_65536_64_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 128, 32768)
}

func Benchmark_or_65536_64_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 128, 65536)
}

func Benchmark_or_65536_64_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 256, 64)
}

func Benchmark_or_65536_64_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 256, 128)
}

func Benchmark_or_65536_64_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 256, 256)
}

func Benchmark_or_65536_64_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 256, 512)
}

func Benchmark_or_65536_64_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 256, 1024)
}

func Benchmark_or_65536_64_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 256, 8192)
}

func Benchmark_or_65536_64_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 256, 16384)
}

func Benchmark_or_65536_64_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 256, 32768)
}

func Benchmark_or_65536_64_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 256, 65536)
}

func Benchmark_or_65536_64_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 512, 64)
}

func Benchmark_or_65536_64_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 512, 128)
}

func Benchmark_or_65536_64_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 512, 256)
}

func Benchmark_or_65536_64_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 512, 512)
}

func Benchmark_or_65536_64_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 512, 1024)
}

func Benchmark_or_65536_64_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 512, 8192)
}

func Benchmark_or_65536_64_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 512, 16384)
}

func Benchmark_or_65536_64_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 512, 32768)
}

func Benchmark_or_65536_64_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 512, 65536)
}

func Benchmark_or_65536_64_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 1024, 64)
}

func Benchmark_or_65536_64_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 1024, 128)
}

func Benchmark_or_65536_64_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 1024, 256)
}

func Benchmark_or_65536_64_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 1024, 512)
}

func Benchmark_or_65536_64_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 1024, 1024)
}

func Benchmark_or_65536_64_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 1024, 8192)
}

func Benchmark_or_65536_64_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 1024, 16384)
}

func Benchmark_or_65536_64_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 1024, 32768)
}

func Benchmark_or_65536_64_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 1024, 65536)
}

func Benchmark_or_65536_64_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 8192, 64)
}

func Benchmark_or_65536_64_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 8192, 128)
}

func Benchmark_or_65536_64_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 8192, 256)
}

func Benchmark_or_65536_64_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 8192, 512)
}

func Benchmark_or_65536_64_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 8192, 1024)
}

func Benchmark_or_65536_64_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 8192, 8192)
}

func Benchmark_or_65536_64_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 8192, 16384)
}

func Benchmark_or_65536_64_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 8192, 32768)
}

func Benchmark_or_65536_64_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 8192, 65536)
}

func Benchmark_or_65536_64_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 16384, 64)
}

func Benchmark_or_65536_64_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 16384, 128)
}

func Benchmark_or_65536_64_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 16384, 256)
}

func Benchmark_or_65536_64_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 16384, 512)
}

func Benchmark_or_65536_64_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 16384, 1024)
}

func Benchmark_or_65536_64_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 16384, 8192)
}

func Benchmark_or_65536_64_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 16384, 16384)
}

func Benchmark_or_65536_64_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 16384, 32768)
}

func Benchmark_or_65536_64_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 16384, 65536)
}

func Benchmark_or_65536_64_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 32768, 64)
}

func Benchmark_or_65536_64_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 32768, 128)
}

func Benchmark_or_65536_64_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 32768, 256)
}

func Benchmark_or_65536_64_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 32768, 512)
}

func Benchmark_or_65536_64_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 32768, 1024)
}

func Benchmark_or_65536_64_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 32768, 8192)
}

func Benchmark_or_65536_64_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 32768, 16384)
}

func Benchmark_or_65536_64_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 32768, 32768)
}

func Benchmark_or_65536_64_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 32768, 65536)
}

func Benchmark_or_65536_64_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 65536, 64)
}

func Benchmark_or_65536_64_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 65536, 128)
}

func Benchmark_or_65536_64_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 65536, 256)
}

func Benchmark_or_65536_64_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 65536, 512)
}

func Benchmark_or_65536_64_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 65536, 1024)
}

func Benchmark_or_65536_64_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 65536, 8192)
}

func Benchmark_or_65536_64_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 65536, 16384)
}

func Benchmark_or_65536_64_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 65536, 32768)
}

func Benchmark_or_65536_64_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 64, 65536, 65536)
}

func Benchmark_or_65536_1024_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 64, 64)
}

func Benchmark_or_65536_1024_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 64, 128)
}

func Benchmark_or_65536_1024_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 64, 256)
}

func Benchmark_or_65536_1024_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 64, 512)
}

func Benchmark_or_65536_1024_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 64, 1024)
}

func Benchmark_or_65536_1024_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 64, 8192)
}

func Benchmark_or_65536_1024_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 64, 16384)
}

func Benchmark_or_65536_1024_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 64, 32768)
}

func Benchmark_or_65536_1024_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 64, 65536)
}

func Benchmark_or_65536_1024_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 128, 64)
}

func Benchmark_or_65536_1024_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 128, 128)
}

func Benchmark_or_65536_1024_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 128, 256)
}

func Benchmark_or_65536_1024_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 128, 512)
}

func Benchmark_or_65536_1024_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 128, 1024)
}

func Benchmark_or_65536_1024_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 128, 8192)
}

func Benchmark_or_65536_1024_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 128, 16384)
}

func Benchmark_or_65536_1024_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 128, 32768)
}

func Benchmark_or_65536_1024_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 128, 65536)
}

func Benchmark_or_65536_1024_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 256, 64)
}

func Benchmark_or_65536_1024_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 256, 128)
}

func Benchmark_or_65536_1024_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 256, 256)
}

func Benchmark_or_65536_1024_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 256, 512)
}

func Benchmark_or_65536_1024_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 256, 1024)
}

func Benchmark_or_65536_1024_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 256, 8192)
}

func Benchmark_or_65536_1024_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 256, 16384)
}

func Benchmark_or_65536_1024_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 256, 32768)
}

func Benchmark_or_65536_1024_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 256, 65536)
}

func Benchmark_or_65536_1024_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 512, 64)
}

func Benchmark_or_65536_1024_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 512, 128)
}

func Benchmark_or_65536_1024_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 512, 256)
}

func Benchmark_or_65536_1024_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 512, 512)
}

func Benchmark_or_65536_1024_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 512, 1024)
}

func Benchmark_or_65536_1024_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 512, 8192)
}

func Benchmark_or_65536_1024_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 512, 16384)
}

func Benchmark_or_65536_1024_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 512, 32768)
}

func Benchmark_or_65536_1024_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 512, 65536)
}

func Benchmark_or_65536_1024_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 1024, 64)
}

func Benchmark_or_65536_1024_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 1024, 128)
}

func Benchmark_or_65536_1024_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 1024, 256)
}

func Benchmark_or_65536_1024_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 1024, 512)
}

func Benchmark_or_65536_1024_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 1024, 1024)
}

func Benchmark_or_65536_1024_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 1024, 8192)
}

func Benchmark_or_65536_1024_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 1024, 16384)
}

func Benchmark_or_65536_1024_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 1024, 32768)
}

func Benchmark_or_65536_1024_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 1024, 65536)
}

func Benchmark_or_65536_1024_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 8192, 64)
}

func Benchmark_or_65536_1024_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 8192, 128)
}

func Benchmark_or_65536_1024_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 8192, 256)
}

func Benchmark_or_65536_1024_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 8192, 512)
}

func Benchmark_or_65536_1024_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 8192, 1024)
}

func Benchmark_or_65536_1024_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 8192, 8192)
}

func Benchmark_or_65536_1024_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 8192, 16384)
}

func Benchmark_or_65536_1024_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 8192, 32768)
}

func Benchmark_or_65536_1024_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 8192, 65536)
}

func Benchmark_or_65536_1024_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 16384, 64)
}

func Benchmark_or_65536_1024_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 16384, 128)
}

func Benchmark_or_65536_1024_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 16384, 256)
}

func Benchmark_or_65536_1024_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 16384, 512)
}

func Benchmark_or_65536_1024_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 16384, 1024)
}

func Benchmark_or_65536_1024_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 16384, 8192)
}

func Benchmark_or_65536_1024_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 16384, 16384)
}

func Benchmark_or_65536_1024_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 16384, 32768)
}

func Benchmark_or_65536_1024_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 16384, 65536)
}

func Benchmark_or_65536_1024_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 32768, 64)
}

func Benchmark_or_65536_1024_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 32768, 128)
}

func Benchmark_or_65536_1024_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 32768, 256)
}

func Benchmark_or_65536_1024_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 32768, 512)
}

func Benchmark_or_65536_1024_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 32768, 1024)
}

func Benchmark_or_65536_1024_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 32768, 8192)
}

func Benchmark_or_65536_1024_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 32768, 16384)
}

func Benchmark_or_65536_1024_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 32768, 32768)
}

func Benchmark_or_65536_1024_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 32768, 65536)
}

func Benchmark_or_65536_1024_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 65536, 64)
}

func Benchmark_or_65536_1024_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 65536, 128)
}

func Benchmark_or_65536_1024_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 65536, 256)
}

func Benchmark_or_65536_1024_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 65536, 512)
}

func Benchmark_or_65536_1024_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 65536, 1024)
}

func Benchmark_or_65536_1024_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 65536, 8192)
}

func Benchmark_or_65536_1024_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 65536, 16384)
}

func Benchmark_or_65536_1024_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 65536, 32768)
}

func Benchmark_or_65536_1024_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 1024, 65536, 65536)
}

func Benchmark_or_65536_65536_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 64, 64)
}

func Benchmark_or_65536_65536_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 64, 128)
}

func Benchmark_or_65536_65536_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 64, 256)
}

func Benchmark_or_65536_65536_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 64, 512)
}

func Benchmark_or_65536_65536_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 64, 1024)
}

func Benchmark_or_65536_65536_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 64, 8192)
}

func Benchmark_or_65536_65536_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 64, 16384)
}

func Benchmark_or_65536_65536_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 64, 32768)
}

func Benchmark_or_65536_65536_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 64, 65536)
}

func Benchmark_or_65536_65536_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 128, 64)
}

func Benchmark_or_65536_65536_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 128, 128)
}

func Benchmark_or_65536_65536_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 128, 256)
}

func Benchmark_or_65536_65536_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 128, 512)
}

func Benchmark_or_65536_65536_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 128, 1024)
}

func Benchmark_or_65536_65536_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 128, 8192)
}

func Benchmark_or_65536_65536_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 128, 16384)
}

func Benchmark_or_65536_65536_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 128, 32768)
}

func Benchmark_or_65536_65536_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 128, 65536)
}

func Benchmark_or_65536_65536_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 256, 64)
}

func Benchmark_or_65536_65536_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 256, 128)
}

func Benchmark_or_65536_65536_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 256, 256)
}

func Benchmark_or_65536_65536_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 256, 512)
}

func Benchmark_or_65536_65536_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 256, 1024)
}

func Benchmark_or_65536_65536_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 256, 8192)
}

func Benchmark_or_65536_65536_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 256, 16384)
}

func Benchmark_or_65536_65536_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 256, 32768)
}

func Benchmark_or_65536_65536_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 256, 65536)
}

func Benchmark_or_65536_65536_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 512, 64)
}

func Benchmark_or_65536_65536_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 512, 128)
}

func Benchmark_or_65536_65536_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 512, 256)
}

func Benchmark_or_65536_65536_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 512, 512)
}

func Benchmark_or_65536_65536_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 512, 1024)
}

func Benchmark_or_65536_65536_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 512, 8192)
}

func Benchmark_or_65536_65536_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 512, 16384)
}

func Benchmark_or_65536_65536_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 512, 32768)
}

func Benchmark_or_65536_65536_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 512, 65536)
}

func Benchmark_or_65536_65536_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 1024, 64)
}

func Benchmark_or_65536_65536_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 1024, 128)
}

func Benchmark_or_65536_65536_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 1024, 256)
}

func Benchmark_or_65536_65536_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 1024, 512)
}

func Benchmark_or_65536_65536_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 1024, 1024)
}

func Benchmark_or_65536_65536_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 1024, 8192)
}

func Benchmark_or_65536_65536_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 1024, 16384)
}

func Benchmark_or_65536_65536_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 1024, 32768)
}

func Benchmark_or_65536_65536_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 1024, 65536)
}

func Benchmark_or_65536_65536_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 8192, 64)
}

func Benchmark_or_65536_65536_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 8192, 128)
}

func Benchmark_or_65536_65536_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 8192, 256)
}

func Benchmark_or_65536_65536_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 8192, 512)
}

func Benchmark_or_65536_65536_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 8192, 1024)
}

func Benchmark_or_65536_65536_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 8192, 8192)
}

func Benchmark_or_65536_65536_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 8192, 16384)
}

func Benchmark_or_65536_65536_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 8192, 32768)
}

func Benchmark_or_65536_65536_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 8192, 65536)
}

func Benchmark_or_65536_65536_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 16384, 64)
}

func Benchmark_or_65536_65536_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 16384, 128)
}

func Benchmark_or_65536_65536_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 16384, 256)
}

func Benchmark_or_65536_65536_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 16384, 512)
}

func Benchmark_or_65536_65536_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 16384, 1024)
}

func Benchmark_or_65536_65536_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 16384, 8192)
}

func Benchmark_or_65536_65536_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 16384, 16384)
}

func Benchmark_or_65536_65536_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 16384, 32768)
}

func Benchmark_or_65536_65536_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 16384, 65536)
}

func Benchmark_or_65536_65536_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 32768, 64)
}

func Benchmark_or_65536_65536_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 32768, 128)
}

func Benchmark_or_65536_65536_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 32768, 256)
}

func Benchmark_or_65536_65536_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 32768, 512)
}

func Benchmark_or_65536_65536_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 32768, 1024)
}

func Benchmark_or_65536_65536_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 32768, 8192)
}

func Benchmark_or_65536_65536_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 32768, 16384)
}

func Benchmark_or_65536_65536_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 32768, 32768)
}

func Benchmark_or_65536_65536_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 32768, 65536)
}

func Benchmark_or_65536_65536_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 65536, 64)
}

func Benchmark_or_65536_65536_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 65536, 128)
}

func Benchmark_or_65536_65536_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 65536, 256)
}

func Benchmark_or_65536_65536_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 65536, 512)
}

func Benchmark_or_65536_65536_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 65536, 1024)
}

func Benchmark_or_65536_65536_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 65536, 8192)
}

func Benchmark_or_65536_65536_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 65536, 16384)
}

func Benchmark_or_65536_65536_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 65536, 32768)
}

func Benchmark_or_65536_65536_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "or", 65536, 65536, 65536, 65536)
}

func Benchmark_diff_64_64_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 64, 64)
}

func Benchmark_diff_64_64_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 64, 128)
}

func Benchmark_diff_64_64_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 64, 256)
}

func Benchmark_diff_64_64_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 64, 512)
}

func Benchmark_diff_64_64_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 64, 1024)
}

func Benchmark_diff_64_64_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 64, 8192)
}

func Benchmark_diff_64_64_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 64, 16384)
}

func Benchmark_diff_64_64_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 64, 32768)
}

func Benchmark_diff_64_64_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 64, 65536)
}

func Benchmark_diff_64_64_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 128, 64)
}

func Benchmark_diff_64_64_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 128, 128)
}

func Benchmark_diff_64_64_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 128, 256)
}

func Benchmark_diff_64_64_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 128, 512)
}

func Benchmark_diff_64_64_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 128, 1024)
}

func Benchmark_diff_64_64_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 128, 8192)
}

func Benchmark_diff_64_64_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 128, 16384)
}

func Benchmark_diff_64_64_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 128, 32768)
}

func Benchmark_diff_64_64_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 128, 65536)
}

func Benchmark_diff_64_64_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 256, 64)
}

func Benchmark_diff_64_64_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 256, 128)
}

func Benchmark_diff_64_64_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 256, 256)
}

func Benchmark_diff_64_64_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 256, 512)
}

func Benchmark_diff_64_64_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 256, 1024)
}

func Benchmark_diff_64_64_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 256, 8192)
}

func Benchmark_diff_64_64_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 256, 16384)
}

func Benchmark_diff_64_64_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 256, 32768)
}

func Benchmark_diff_64_64_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 256, 65536)
}

func Benchmark_diff_64_64_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 512, 64)
}

func Benchmark_diff_64_64_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 512, 128)
}

func Benchmark_diff_64_64_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 512, 256)
}

func Benchmark_diff_64_64_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 512, 512)
}

func Benchmark_diff_64_64_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 512, 1024)
}

func Benchmark_diff_64_64_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 512, 8192)
}

func Benchmark_diff_64_64_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 512, 16384)
}

func Benchmark_diff_64_64_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 512, 32768)
}

func Benchmark_diff_64_64_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 512, 65536)
}

func Benchmark_diff_64_64_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 1024, 64)
}

func Benchmark_diff_64_64_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 1024, 128)
}

func Benchmark_diff_64_64_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 1024, 256)
}

func Benchmark_diff_64_64_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 1024, 512)
}

func Benchmark_diff_64_64_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 1024, 1024)
}

func Benchmark_diff_64_64_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 1024, 8192)
}

func Benchmark_diff_64_64_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 1024, 16384)
}

func Benchmark_diff_64_64_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 1024, 32768)
}

func Benchmark_diff_64_64_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 1024, 65536)
}

func Benchmark_diff_64_64_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 8192, 64)
}

func Benchmark_diff_64_64_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 8192, 128)
}

func Benchmark_diff_64_64_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 8192, 256)
}

func Benchmark_diff_64_64_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 8192, 512)
}

func Benchmark_diff_64_64_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 8192, 1024)
}

func Benchmark_diff_64_64_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 8192, 8192)
}

func Benchmark_diff_64_64_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 8192, 16384)
}

func Benchmark_diff_64_64_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 8192, 32768)
}

func Benchmark_diff_64_64_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 8192, 65536)
}

func Benchmark_diff_64_64_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 16384, 64)
}

func Benchmark_diff_64_64_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 16384, 128)
}

func Benchmark_diff_64_64_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 16384, 256)
}

func Benchmark_diff_64_64_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 16384, 512)
}

func Benchmark_diff_64_64_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 16384, 1024)
}

func Benchmark_diff_64_64_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 16384, 8192)
}

func Benchmark_diff_64_64_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 16384, 16384)
}

func Benchmark_diff_64_64_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 16384, 32768)
}

func Benchmark_diff_64_64_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 16384, 65536)
}

func Benchmark_diff_64_64_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 32768, 64)
}

func Benchmark_diff_64_64_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 32768, 128)
}

func Benchmark_diff_64_64_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 32768, 256)
}

func Benchmark_diff_64_64_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 32768, 512)
}

func Benchmark_diff_64_64_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 32768, 1024)
}

func Benchmark_diff_64_64_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 32768, 8192)
}

func Benchmark_diff_64_64_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 32768, 16384)
}

func Benchmark_diff_64_64_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 32768, 32768)
}

func Benchmark_diff_64_64_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 32768, 65536)
}

func Benchmark_diff_64_64_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 65536, 64)
}

func Benchmark_diff_64_64_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 65536, 128)
}

func Benchmark_diff_64_64_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 65536, 256)
}

func Benchmark_diff_64_64_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 65536, 512)
}

func Benchmark_diff_64_64_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 65536, 1024)
}

func Benchmark_diff_64_64_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 65536, 8192)
}

func Benchmark_diff_64_64_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 65536, 16384)
}

func Benchmark_diff_64_64_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 65536, 32768)
}

func Benchmark_diff_64_64_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 64, 65536, 65536)
}

func Benchmark_diff_64_1024_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 64, 64)
}

func Benchmark_diff_64_1024_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 64, 128)
}

func Benchmark_diff_64_1024_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 64, 256)
}

func Benchmark_diff_64_1024_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 64, 512)
}

func Benchmark_diff_64_1024_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 64, 1024)
}

func Benchmark_diff_64_1024_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 64, 8192)
}

func Benchmark_diff_64_1024_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 64, 16384)
}

func Benchmark_diff_64_1024_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 64, 32768)
}

func Benchmark_diff_64_1024_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 64, 65536)
}

func Benchmark_diff_64_1024_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 128, 64)
}

func Benchmark_diff_64_1024_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 128, 128)
}

func Benchmark_diff_64_1024_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 128, 256)
}

func Benchmark_diff_64_1024_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 128, 512)
}

func Benchmark_diff_64_1024_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 128, 1024)
}

func Benchmark_diff_64_1024_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 128, 8192)
}

func Benchmark_diff_64_1024_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 128, 16384)
}

func Benchmark_diff_64_1024_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 128, 32768)
}

func Benchmark_diff_64_1024_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 128, 65536)
}

func Benchmark_diff_64_1024_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 256, 64)
}

func Benchmark_diff_64_1024_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 256, 128)
}

func Benchmark_diff_64_1024_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 256, 256)
}

func Benchmark_diff_64_1024_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 256, 512)
}

func Benchmark_diff_64_1024_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 256, 1024)
}

func Benchmark_diff_64_1024_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 256, 8192)
}

func Benchmark_diff_64_1024_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 256, 16384)
}

func Benchmark_diff_64_1024_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 256, 32768)
}

func Benchmark_diff_64_1024_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 256, 65536)
}

func Benchmark_diff_64_1024_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 512, 64)
}

func Benchmark_diff_64_1024_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 512, 128)
}

func Benchmark_diff_64_1024_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 512, 256)
}

func Benchmark_diff_64_1024_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 512, 512)
}

func Benchmark_diff_64_1024_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 512, 1024)
}

func Benchmark_diff_64_1024_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 512, 8192)
}

func Benchmark_diff_64_1024_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 512, 16384)
}

func Benchmark_diff_64_1024_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 512, 32768)
}

func Benchmark_diff_64_1024_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 512, 65536)
}

func Benchmark_diff_64_1024_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 1024, 64)
}

func Benchmark_diff_64_1024_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 1024, 128)
}

func Benchmark_diff_64_1024_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 1024, 256)
}

func Benchmark_diff_64_1024_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 1024, 512)
}

func Benchmark_diff_64_1024_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 1024, 1024)
}

func Benchmark_diff_64_1024_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 1024, 8192)
}

func Benchmark_diff_64_1024_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 1024, 16384)
}

func Benchmark_diff_64_1024_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 1024, 32768)
}

func Benchmark_diff_64_1024_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 1024, 65536)
}

func Benchmark_diff_64_1024_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 8192, 64)
}

func Benchmark_diff_64_1024_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 8192, 128)
}

func Benchmark_diff_64_1024_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 8192, 256)
}

func Benchmark_diff_64_1024_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 8192, 512)
}

func Benchmark_diff_64_1024_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 8192, 1024)
}

func Benchmark_diff_64_1024_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 8192, 8192)
}

func Benchmark_diff_64_1024_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 8192, 16384)
}

func Benchmark_diff_64_1024_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 8192, 32768)
}

func Benchmark_diff_64_1024_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 8192, 65536)
}

func Benchmark_diff_64_1024_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 16384, 64)
}

func Benchmark_diff_64_1024_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 16384, 128)
}

func Benchmark_diff_64_1024_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 16384, 256)
}

func Benchmark_diff_64_1024_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 16384, 512)
}

func Benchmark_diff_64_1024_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 16384, 1024)
}

func Benchmark_diff_64_1024_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 16384, 8192)
}

func Benchmark_diff_64_1024_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 16384, 16384)
}

func Benchmark_diff_64_1024_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 16384, 32768)
}

func Benchmark_diff_64_1024_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 16384, 65536)
}

func Benchmark_diff_64_1024_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 32768, 64)
}

func Benchmark_diff_64_1024_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 32768, 128)
}

func Benchmark_diff_64_1024_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 32768, 256)
}

func Benchmark_diff_64_1024_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 32768, 512)
}

func Benchmark_diff_64_1024_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 32768, 1024)
}

func Benchmark_diff_64_1024_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 32768, 8192)
}

func Benchmark_diff_64_1024_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 32768, 16384)
}

func Benchmark_diff_64_1024_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 32768, 32768)
}

func Benchmark_diff_64_1024_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 32768, 65536)
}

func Benchmark_diff_64_1024_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 65536, 64)
}

func Benchmark_diff_64_1024_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 65536, 128)
}

func Benchmark_diff_64_1024_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 65536, 256)
}

func Benchmark_diff_64_1024_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 65536, 512)
}

func Benchmark_diff_64_1024_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 65536, 1024)
}

func Benchmark_diff_64_1024_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 65536, 8192)
}

func Benchmark_diff_64_1024_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 65536, 16384)
}

func Benchmark_diff_64_1024_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 65536, 32768)
}

func Benchmark_diff_64_1024_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 1024, 65536, 65536)
}

func Benchmark_diff_64_65536_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 64, 64)
}

func Benchmark_diff_64_65536_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 64, 128)
}

func Benchmark_diff_64_65536_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 64, 256)
}

func Benchmark_diff_64_65536_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 64, 512)
}

func Benchmark_diff_64_65536_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 64, 1024)
}

func Benchmark_diff_64_65536_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 64, 8192)
}

func Benchmark_diff_64_65536_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 64, 16384)
}

func Benchmark_diff_64_65536_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 64, 32768)
}

func Benchmark_diff_64_65536_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 64, 65536)
}

func Benchmark_diff_64_65536_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 128, 64)
}

func Benchmark_diff_64_65536_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 128, 128)
}

func Benchmark_diff_64_65536_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 128, 256)
}

func Benchmark_diff_64_65536_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 128, 512)
}

func Benchmark_diff_64_65536_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 128, 1024)
}

func Benchmark_diff_64_65536_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 128, 8192)
}

func Benchmark_diff_64_65536_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 128, 16384)
}

func Benchmark_diff_64_65536_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 128, 32768)
}

func Benchmark_diff_64_65536_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 128, 65536)
}

func Benchmark_diff_64_65536_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 256, 64)
}

func Benchmark_diff_64_65536_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 256, 128)
}

func Benchmark_diff_64_65536_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 256, 256)
}

func Benchmark_diff_64_65536_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 256, 512)
}

func Benchmark_diff_64_65536_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 256, 1024)
}

func Benchmark_diff_64_65536_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 256, 8192)
}

func Benchmark_diff_64_65536_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 256, 16384)
}

func Benchmark_diff_64_65536_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 256, 32768)
}

func Benchmark_diff_64_65536_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 256, 65536)
}

func Benchmark_diff_64_65536_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 512, 64)
}

func Benchmark_diff_64_65536_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 512, 128)
}

func Benchmark_diff_64_65536_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 512, 256)
}

func Benchmark_diff_64_65536_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 512, 512)
}

func Benchmark_diff_64_65536_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 512, 1024)
}

func Benchmark_diff_64_65536_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 512, 8192)
}

func Benchmark_diff_64_65536_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 512, 16384)
}

func Benchmark_diff_64_65536_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 512, 32768)
}

func Benchmark_diff_64_65536_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 512, 65536)
}

func Benchmark_diff_64_65536_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 1024, 64)
}

func Benchmark_diff_64_65536_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 1024, 128)
}

func Benchmark_diff_64_65536_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 1024, 256)
}

func Benchmark_diff_64_65536_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 1024, 512)
}

func Benchmark_diff_64_65536_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 1024, 1024)
}

func Benchmark_diff_64_65536_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 1024, 8192)
}

func Benchmark_diff_64_65536_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 1024, 16384)
}

func Benchmark_diff_64_65536_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 1024, 32768)
}

func Benchmark_diff_64_65536_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 1024, 65536)
}

func Benchmark_diff_64_65536_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 8192, 64)
}

func Benchmark_diff_64_65536_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 8192, 128)
}

func Benchmark_diff_64_65536_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 8192, 256)
}

func Benchmark_diff_64_65536_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 8192, 512)
}

func Benchmark_diff_64_65536_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 8192, 1024)
}

func Benchmark_diff_64_65536_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 8192, 8192)
}

func Benchmark_diff_64_65536_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 8192, 16384)
}

func Benchmark_diff_64_65536_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 8192, 32768)
}

func Benchmark_diff_64_65536_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 8192, 65536)
}

func Benchmark_diff_64_65536_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 16384, 64)
}

func Benchmark_diff_64_65536_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 16384, 128)
}

func Benchmark_diff_64_65536_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 16384, 256)
}

func Benchmark_diff_64_65536_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 16384, 512)
}

func Benchmark_diff_64_65536_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 16384, 1024)
}

func Benchmark_diff_64_65536_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 16384, 8192)
}

func Benchmark_diff_64_65536_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 16384, 16384)
}

func Benchmark_diff_64_65536_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 16384, 32768)
}

func Benchmark_diff_64_65536_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 16384, 65536)
}

func Benchmark_diff_64_65536_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 32768, 64)
}

func Benchmark_diff_64_65536_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 32768, 128)
}

func Benchmark_diff_64_65536_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 32768, 256)
}

func Benchmark_diff_64_65536_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 32768, 512)
}

func Benchmark_diff_64_65536_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 32768, 1024)
}

func Benchmark_diff_64_65536_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 32768, 8192)
}

func Benchmark_diff_64_65536_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 32768, 16384)
}

func Benchmark_diff_64_65536_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 32768, 32768)
}

func Benchmark_diff_64_65536_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 32768, 65536)
}

func Benchmark_diff_64_65536_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 65536, 64)
}

func Benchmark_diff_64_65536_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 65536, 128)
}

func Benchmark_diff_64_65536_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 65536, 256)
}

func Benchmark_diff_64_65536_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 65536, 512)
}

func Benchmark_diff_64_65536_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 65536, 1024)
}

func Benchmark_diff_64_65536_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 65536, 8192)
}

func Benchmark_diff_64_65536_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 65536, 16384)
}

func Benchmark_diff_64_65536_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 65536, 32768)
}

func Benchmark_diff_64_65536_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 64, 65536, 65536, 65536)
}

func Benchmark_diff_1024_64_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 64, 64)
}

func Benchmark_diff_1024_64_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 64, 128)
}

func Benchmark_diff_1024_64_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 64, 256)
}

func Benchmark_diff_1024_64_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 64, 512)
}

func Benchmark_diff_1024_64_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 64, 1024)
}

func Benchmark_diff_1024_64_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 64, 8192)
}

func Benchmark_diff_1024_64_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 64, 16384)
}

func Benchmark_diff_1024_64_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 64, 32768)
}

func Benchmark_diff_1024_64_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 64, 65536)
}

func Benchmark_diff_1024_64_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 128, 64)
}

func Benchmark_diff_1024_64_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 128, 128)
}

func Benchmark_diff_1024_64_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 128, 256)
}

func Benchmark_diff_1024_64_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 128, 512)
}

func Benchmark_diff_1024_64_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 128, 1024)
}

func Benchmark_diff_1024_64_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 128, 8192)
}

func Benchmark_diff_1024_64_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 128, 16384)
}

func Benchmark_diff_1024_64_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 128, 32768)
}

func Benchmark_diff_1024_64_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 128, 65536)
}

func Benchmark_diff_1024_64_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 256, 64)
}

func Benchmark_diff_1024_64_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 256, 128)
}

func Benchmark_diff_1024_64_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 256, 256)
}

func Benchmark_diff_1024_64_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 256, 512)
}

func Benchmark_diff_1024_64_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 256, 1024)
}

func Benchmark_diff_1024_64_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 256, 8192)
}

func Benchmark_diff_1024_64_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 256, 16384)
}

func Benchmark_diff_1024_64_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 256, 32768)
}

func Benchmark_diff_1024_64_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 256, 65536)
}

func Benchmark_diff_1024_64_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 512, 64)
}

func Benchmark_diff_1024_64_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 512, 128)
}

func Benchmark_diff_1024_64_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 512, 256)
}

func Benchmark_diff_1024_64_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 512, 512)
}

func Benchmark_diff_1024_64_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 512, 1024)
}

func Benchmark_diff_1024_64_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 512, 8192)
}

func Benchmark_diff_1024_64_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 512, 16384)
}

func Benchmark_diff_1024_64_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 512, 32768)
}

func Benchmark_diff_1024_64_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 512, 65536)
}

func Benchmark_diff_1024_64_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 1024, 64)
}

func Benchmark_diff_1024_64_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 1024, 128)
}

func Benchmark_diff_1024_64_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 1024, 256)
}

func Benchmark_diff_1024_64_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 1024, 512)
}

func Benchmark_diff_1024_64_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 1024, 1024)
}

func Benchmark_diff_1024_64_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 1024, 8192)
}

func Benchmark_diff_1024_64_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 1024, 16384)
}

func Benchmark_diff_1024_64_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 1024, 32768)
}

func Benchmark_diff_1024_64_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 1024, 65536)
}

func Benchmark_diff_1024_64_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 8192, 64)
}

func Benchmark_diff_1024_64_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 8192, 128)
}

func Benchmark_diff_1024_64_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 8192, 256)
}

func Benchmark_diff_1024_64_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 8192, 512)
}

func Benchmark_diff_1024_64_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 8192, 1024)
}

func Benchmark_diff_1024_64_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 8192, 8192)
}

func Benchmark_diff_1024_64_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 8192, 16384)
}

func Benchmark_diff_1024_64_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 8192, 32768)
}

func Benchmark_diff_1024_64_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 8192, 65536)
}

func Benchmark_diff_1024_64_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 16384, 64)
}

func Benchmark_diff_1024_64_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 16384, 128)
}

func Benchmark_diff_1024_64_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 16384, 256)
}

func Benchmark_diff_1024_64_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 16384, 512)
}

func Benchmark_diff_1024_64_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 16384, 1024)
}

func Benchmark_diff_1024_64_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 16384, 8192)
}

func Benchmark_diff_1024_64_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 16384, 16384)
}

func Benchmark_diff_1024_64_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 16384, 32768)
}

func Benchmark_diff_1024_64_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 16384, 65536)
}

func Benchmark_diff_1024_64_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 32768, 64)
}

func Benchmark_diff_1024_64_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 32768, 128)
}

func Benchmark_diff_1024_64_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 32768, 256)
}

func Benchmark_diff_1024_64_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 32768, 512)
}

func Benchmark_diff_1024_64_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 32768, 1024)
}

func Benchmark_diff_1024_64_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 32768, 8192)
}

func Benchmark_diff_1024_64_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 32768, 16384)
}

func Benchmark_diff_1024_64_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 32768, 32768)
}

func Benchmark_diff_1024_64_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 32768, 65536)
}

func Benchmark_diff_1024_64_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 65536, 64)
}

func Benchmark_diff_1024_64_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 65536, 128)
}

func Benchmark_diff_1024_64_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 65536, 256)
}

func Benchmark_diff_1024_64_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 65536, 512)
}

func Benchmark_diff_1024_64_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 65536, 1024)
}

func Benchmark_diff_1024_64_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 65536, 8192)
}

func Benchmark_diff_1024_64_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 65536, 16384)
}

func Benchmark_diff_1024_64_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 65536, 32768)
}

func Benchmark_diff_1024_64_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 64, 65536, 65536)
}

func Benchmark_diff_1024_1024_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 64, 64)
}

func Benchmark_diff_1024_1024_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 64, 128)
}

func Benchmark_diff_1024_1024_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 64, 256)
}

func Benchmark_diff_1024_1024_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 64, 512)
}

func Benchmark_diff_1024_1024_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 64, 1024)
}

func Benchmark_diff_1024_1024_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 64, 8192)
}

func Benchmark_diff_1024_1024_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 64, 16384)
}

func Benchmark_diff_1024_1024_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 64, 32768)
}

func Benchmark_diff_1024_1024_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 64, 65536)
}

func Benchmark_diff_1024_1024_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 128, 64)
}

func Benchmark_diff_1024_1024_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 128, 128)
}

func Benchmark_diff_1024_1024_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 128, 256)
}

func Benchmark_diff_1024_1024_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 128, 512)
}

func Benchmark_diff_1024_1024_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 128, 1024)
}

func Benchmark_diff_1024_1024_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 128, 8192)
}

func Benchmark_diff_1024_1024_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 128, 16384)
}

func Benchmark_diff_1024_1024_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 128, 32768)
}

func Benchmark_diff_1024_1024_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 128, 65536)
}

func Benchmark_diff_1024_1024_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 256, 64)
}

func Benchmark_diff_1024_1024_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 256, 128)
}

func Benchmark_diff_1024_1024_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 256, 256)
}

func Benchmark_diff_1024_1024_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 256, 512)
}

func Benchmark_diff_1024_1024_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 256, 1024)
}

func Benchmark_diff_1024_1024_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 256, 8192)
}

func Benchmark_diff_1024_1024_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 256, 16384)
}

func Benchmark_diff_1024_1024_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 256, 32768)
}

func Benchmark_diff_1024_1024_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 256, 65536)
}

func Benchmark_diff_1024_1024_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 512, 64)
}

func Benchmark_diff_1024_1024_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 512, 128)
}

func Benchmark_diff_1024_1024_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 512, 256)
}

func Benchmark_diff_1024_1024_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 512, 512)
}

func Benchmark_diff_1024_1024_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 512, 1024)
}

func Benchmark_diff_1024_1024_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 512, 8192)
}

func Benchmark_diff_1024_1024_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 512, 16384)
}

func Benchmark_diff_1024_1024_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 512, 32768)
}

func Benchmark_diff_1024_1024_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 512, 65536)
}

func Benchmark_diff_1024_1024_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 1024, 64)
}

func Benchmark_diff_1024_1024_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 1024, 128)
}

func Benchmark_diff_1024_1024_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 1024, 256)
}

func Benchmark_diff_1024_1024_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 1024, 512)
}

func Benchmark_diff_1024_1024_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 1024, 1024)
}

func Benchmark_diff_1024_1024_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 1024, 8192)
}

func Benchmark_diff_1024_1024_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 1024, 16384)
}

func Benchmark_diff_1024_1024_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 1024, 32768)
}

func Benchmark_diff_1024_1024_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 1024, 65536)
}

func Benchmark_diff_1024_1024_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 8192, 64)
}

func Benchmark_diff_1024_1024_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 8192, 128)
}

func Benchmark_diff_1024_1024_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 8192, 256)
}

func Benchmark_diff_1024_1024_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 8192, 512)
}

func Benchmark_diff_1024_1024_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 8192, 1024)
}

func Benchmark_diff_1024_1024_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 8192, 8192)
}

func Benchmark_diff_1024_1024_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 8192, 16384)
}

func Benchmark_diff_1024_1024_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 8192, 32768)
}

func Benchmark_diff_1024_1024_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 8192, 65536)
}

func Benchmark_diff_1024_1024_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 16384, 64)
}

func Benchmark_diff_1024_1024_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 16384, 128)
}

func Benchmark_diff_1024_1024_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 16384, 256)
}

func Benchmark_diff_1024_1024_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 16384, 512)
}

func Benchmark_diff_1024_1024_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 16384, 1024)
}

func Benchmark_diff_1024_1024_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 16384, 8192)
}

func Benchmark_diff_1024_1024_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 16384, 16384)
}

func Benchmark_diff_1024_1024_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 16384, 32768)
}

func Benchmark_diff_1024_1024_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 16384, 65536)
}

func Benchmark_diff_1024_1024_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 32768, 64)
}

func Benchmark_diff_1024_1024_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 32768, 128)
}

func Benchmark_diff_1024_1024_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 32768, 256)
}

func Benchmark_diff_1024_1024_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 32768, 512)
}

func Benchmark_diff_1024_1024_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 32768, 1024)
}

func Benchmark_diff_1024_1024_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 32768, 8192)
}

func Benchmark_diff_1024_1024_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 32768, 16384)
}

func Benchmark_diff_1024_1024_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 32768, 32768)
}

func Benchmark_diff_1024_1024_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 32768, 65536)
}

func Benchmark_diff_1024_1024_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 65536, 64)
}

func Benchmark_diff_1024_1024_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 65536, 128)
}

func Benchmark_diff_1024_1024_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 65536, 256)
}

func Benchmark_diff_1024_1024_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 65536, 512)
}

func Benchmark_diff_1024_1024_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 65536, 1024)
}

func Benchmark_diff_1024_1024_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 65536, 8192)
}

func Benchmark_diff_1024_1024_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 65536, 16384)
}

func Benchmark_diff_1024_1024_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 65536, 32768)
}

func Benchmark_diff_1024_1024_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 1024, 65536, 65536)
}

func Benchmark_diff_1024_65536_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 64, 64)
}

func Benchmark_diff_1024_65536_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 64, 128)
}

func Benchmark_diff_1024_65536_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 64, 256)
}

func Benchmark_diff_1024_65536_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 64, 512)
}

func Benchmark_diff_1024_65536_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 64, 1024)
}

func Benchmark_diff_1024_65536_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 64, 8192)
}

func Benchmark_diff_1024_65536_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 64, 16384)
}

func Benchmark_diff_1024_65536_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 64, 32768)
}

func Benchmark_diff_1024_65536_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 64, 65536)
}

func Benchmark_diff_1024_65536_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 128, 64)
}

func Benchmark_diff_1024_65536_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 128, 128)
}

func Benchmark_diff_1024_65536_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 128, 256)
}

func Benchmark_diff_1024_65536_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 128, 512)
}

func Benchmark_diff_1024_65536_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 128, 1024)
}

func Benchmark_diff_1024_65536_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 128, 8192)
}

func Benchmark_diff_1024_65536_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 128, 16384)
}

func Benchmark_diff_1024_65536_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 128, 32768)
}

func Benchmark_diff_1024_65536_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 128, 65536)
}

func Benchmark_diff_1024_65536_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 256, 64)
}

func Benchmark_diff_1024_65536_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 256, 128)
}

func Benchmark_diff_1024_65536_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 256, 256)
}

func Benchmark_diff_1024_65536_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 256, 512)
}

func Benchmark_diff_1024_65536_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 256, 1024)
}

func Benchmark_diff_1024_65536_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 256, 8192)
}

func Benchmark_diff_1024_65536_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 256, 16384)
}

func Benchmark_diff_1024_65536_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 256, 32768)
}

func Benchmark_diff_1024_65536_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 256, 65536)
}

func Benchmark_diff_1024_65536_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 512, 64)
}

func Benchmark_diff_1024_65536_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 512, 128)
}

func Benchmark_diff_1024_65536_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 512, 256)
}

func Benchmark_diff_1024_65536_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 512, 512)
}

func Benchmark_diff_1024_65536_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 512, 1024)
}

func Benchmark_diff_1024_65536_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 512, 8192)
}

func Benchmark_diff_1024_65536_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 512, 16384)
}

func Benchmark_diff_1024_65536_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 512, 32768)
}

func Benchmark_diff_1024_65536_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 512, 65536)
}

func Benchmark_diff_1024_65536_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 1024, 64)
}

func Benchmark_diff_1024_65536_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 1024, 128)
}

func Benchmark_diff_1024_65536_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 1024, 256)
}

func Benchmark_diff_1024_65536_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 1024, 512)
}

func Benchmark_diff_1024_65536_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 1024, 1024)
}

func Benchmark_diff_1024_65536_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 1024, 8192)
}

func Benchmark_diff_1024_65536_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 1024, 16384)
}

func Benchmark_diff_1024_65536_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 1024, 32768)
}

func Benchmark_diff_1024_65536_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 1024, 65536)
}

func Benchmark_diff_1024_65536_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 8192, 64)
}

func Benchmark_diff_1024_65536_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 8192, 128)
}

func Benchmark_diff_1024_65536_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 8192, 256)
}

func Benchmark_diff_1024_65536_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 8192, 512)
}

func Benchmark_diff_1024_65536_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 8192, 1024)
}

func Benchmark_diff_1024_65536_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 8192, 8192)
}

func Benchmark_diff_1024_65536_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 8192, 16384)
}

func Benchmark_diff_1024_65536_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 8192, 32768)
}

func Benchmark_diff_1024_65536_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 8192, 65536)
}

func Benchmark_diff_1024_65536_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 16384, 64)
}

func Benchmark_diff_1024_65536_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 16384, 128)
}

func Benchmark_diff_1024_65536_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 16384, 256)
}

func Benchmark_diff_1024_65536_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 16384, 512)
}

func Benchmark_diff_1024_65536_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 16384, 1024)
}

func Benchmark_diff_1024_65536_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 16384, 8192)
}

func Benchmark_diff_1024_65536_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 16384, 16384)
}

func Benchmark_diff_1024_65536_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 16384, 32768)
}

func Benchmark_diff_1024_65536_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 16384, 65536)
}

func Benchmark_diff_1024_65536_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 32768, 64)
}

func Benchmark_diff_1024_65536_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 32768, 128)
}

func Benchmark_diff_1024_65536_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 32768, 256)
}

func Benchmark_diff_1024_65536_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 32768, 512)
}

func Benchmark_diff_1024_65536_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 32768, 1024)
}

func Benchmark_diff_1024_65536_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 32768, 8192)
}

func Benchmark_diff_1024_65536_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 32768, 16384)
}

func Benchmark_diff_1024_65536_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 32768, 32768)
}

func Benchmark_diff_1024_65536_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 32768, 65536)
}

func Benchmark_diff_1024_65536_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 65536, 64)
}

func Benchmark_diff_1024_65536_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 65536, 128)
}

func Benchmark_diff_1024_65536_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 65536, 256)
}

func Benchmark_diff_1024_65536_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 65536, 512)
}

func Benchmark_diff_1024_65536_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 65536, 1024)
}

func Benchmark_diff_1024_65536_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 65536, 8192)
}

func Benchmark_diff_1024_65536_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 65536, 16384)
}

func Benchmark_diff_1024_65536_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 65536, 32768)
}

func Benchmark_diff_1024_65536_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 1024, 65536, 65536, 65536)
}

func Benchmark_diff_65536_64_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 64, 64)
}

func Benchmark_diff_65536_64_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 64, 128)
}

func Benchmark_diff_65536_64_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 64, 256)
}

func Benchmark_diff_65536_64_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 64, 512)
}

func Benchmark_diff_65536_64_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 64, 1024)
}

func Benchmark_diff_65536_64_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 64, 8192)
}

func Benchmark_diff_65536_64_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 64, 16384)
}

func Benchmark_diff_65536_64_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 64, 32768)
}

func Benchmark_diff_65536_64_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 64, 65536)
}

func Benchmark_diff_65536_64_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 128, 64)
}

func Benchmark_diff_65536_64_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 128, 128)
}

func Benchmark_diff_65536_64_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 128, 256)
}

func Benchmark_diff_65536_64_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 128, 512)
}

func Benchmark_diff_65536_64_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 128, 1024)
}

func Benchmark_diff_65536_64_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 128, 8192)
}

func Benchmark_diff_65536_64_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 128, 16384)
}

func Benchmark_diff_65536_64_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 128, 32768)
}

func Benchmark_diff_65536_64_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 128, 65536)
}

func Benchmark_diff_65536_64_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 256, 64)
}

func Benchmark_diff_65536_64_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 256, 128)
}

func Benchmark_diff_65536_64_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 256, 256)
}

func Benchmark_diff_65536_64_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 256, 512)
}

func Benchmark_diff_65536_64_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 256, 1024)
}

func Benchmark_diff_65536_64_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 256, 8192)
}

func Benchmark_diff_65536_64_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 256, 16384)
}

func Benchmark_diff_65536_64_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 256, 32768)
}

func Benchmark_diff_65536_64_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 256, 65536)
}

func Benchmark_diff_65536_64_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 512, 64)
}

func Benchmark_diff_65536_64_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 512, 128)
}

func Benchmark_diff_65536_64_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 512, 256)
}

func Benchmark_diff_65536_64_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 512, 512)
}

func Benchmark_diff_65536_64_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 512, 1024)
}

func Benchmark_diff_65536_64_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 512, 8192)
}

func Benchmark_diff_65536_64_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 512, 16384)
}

func Benchmark_diff_65536_64_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 512, 32768)
}

func Benchmark_diff_65536_64_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 512, 65536)
}

func Benchmark_diff_65536_64_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 1024, 64)
}

func Benchmark_diff_65536_64_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 1024, 128)
}

func Benchmark_diff_65536_64_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 1024, 256)
}

func Benchmark_diff_65536_64_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 1024, 512)
}

func Benchmark_diff_65536_64_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 1024, 1024)
}

func Benchmark_diff_65536_64_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 1024, 8192)
}

func Benchmark_diff_65536_64_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 1024, 16384)
}

func Benchmark_diff_65536_64_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 1024, 32768)
}

func Benchmark_diff_65536_64_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 1024, 65536)
}

func Benchmark_diff_65536_64_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 8192, 64)
}

func Benchmark_diff_65536_64_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 8192, 128)
}

func Benchmark_diff_65536_64_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 8192, 256)
}

func Benchmark_diff_65536_64_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 8192, 512)
}

func Benchmark_diff_65536_64_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 8192, 1024)
}

func Benchmark_diff_65536_64_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 8192, 8192)
}

func Benchmark_diff_65536_64_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 8192, 16384)
}

func Benchmark_diff_65536_64_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 8192, 32768)
}

func Benchmark_diff_65536_64_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 8192, 65536)
}

func Benchmark_diff_65536_64_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 16384, 64)
}

func Benchmark_diff_65536_64_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 16384, 128)
}

func Benchmark_diff_65536_64_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 16384, 256)
}

func Benchmark_diff_65536_64_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 16384, 512)
}

func Benchmark_diff_65536_64_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 16384, 1024)
}

func Benchmark_diff_65536_64_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 16384, 8192)
}

func Benchmark_diff_65536_64_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 16384, 16384)
}

func Benchmark_diff_65536_64_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 16384, 32768)
}

func Benchmark_diff_65536_64_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 16384, 65536)
}

func Benchmark_diff_65536_64_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 32768, 64)
}

func Benchmark_diff_65536_64_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 32768, 128)
}

func Benchmark_diff_65536_64_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 32768, 256)
}

func Benchmark_diff_65536_64_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 32768, 512)
}

func Benchmark_diff_65536_64_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 32768, 1024)
}

func Benchmark_diff_65536_64_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 32768, 8192)
}

func Benchmark_diff_65536_64_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 32768, 16384)
}

func Benchmark_diff_65536_64_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 32768, 32768)
}

func Benchmark_diff_65536_64_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 32768, 65536)
}

func Benchmark_diff_65536_64_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 65536, 64)
}

func Benchmark_diff_65536_64_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 65536, 128)
}

func Benchmark_diff_65536_64_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 65536, 256)
}

func Benchmark_diff_65536_64_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 65536, 512)
}

func Benchmark_diff_65536_64_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 65536, 1024)
}

func Benchmark_diff_65536_64_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 65536, 8192)
}

func Benchmark_diff_65536_64_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 65536, 16384)
}

func Benchmark_diff_65536_64_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 65536, 32768)
}

func Benchmark_diff_65536_64_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 64, 65536, 65536)
}

func Benchmark_diff_65536_1024_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 64, 64)
}

func Benchmark_diff_65536_1024_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 64, 128)
}

func Benchmark_diff_65536_1024_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 64, 256)
}

func Benchmark_diff_65536_1024_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 64, 512)
}

func Benchmark_diff_65536_1024_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 64, 1024)
}

func Benchmark_diff_65536_1024_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 64, 8192)
}

func Benchmark_diff_65536_1024_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 64, 16384)
}

func Benchmark_diff_65536_1024_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 64, 32768)
}

func Benchmark_diff_65536_1024_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 64, 65536)
}

func Benchmark_diff_65536_1024_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 128, 64)
}

func Benchmark_diff_65536_1024_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 128, 128)
}

func Benchmark_diff_65536_1024_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 128, 256)
}

func Benchmark_diff_65536_1024_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 128, 512)
}

func Benchmark_diff_65536_1024_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 128, 1024)
}

func Benchmark_diff_65536_1024_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 128, 8192)
}

func Benchmark_diff_65536_1024_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 128, 16384)
}

func Benchmark_diff_65536_1024_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 128, 32768)
}

func Benchmark_diff_65536_1024_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 128, 65536)
}

func Benchmark_diff_65536_1024_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 256, 64)
}

func Benchmark_diff_65536_1024_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 256, 128)
}

func Benchmark_diff_65536_1024_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 256, 256)
}

func Benchmark_diff_65536_1024_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 256, 512)
}

func Benchmark_diff_65536_1024_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 256, 1024)
}

func Benchmark_diff_65536_1024_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 256, 8192)
}

func Benchmark_diff_65536_1024_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 256, 16384)
}

func Benchmark_diff_65536_1024_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 256, 32768)
}

func Benchmark_diff_65536_1024_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 256, 65536)
}

func Benchmark_diff_65536_1024_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 512, 64)
}

func Benchmark_diff_65536_1024_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 512, 128)
}

func Benchmark_diff_65536_1024_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 512, 256)
}

func Benchmark_diff_65536_1024_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 512, 512)
}

func Benchmark_diff_65536_1024_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 512, 1024)
}

func Benchmark_diff_65536_1024_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 512, 8192)
}

func Benchmark_diff_65536_1024_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 512, 16384)
}

func Benchmark_diff_65536_1024_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 512, 32768)
}

func Benchmark_diff_65536_1024_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 512, 65536)
}

func Benchmark_diff_65536_1024_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 1024, 64)
}

func Benchmark_diff_65536_1024_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 1024, 128)
}

func Benchmark_diff_65536_1024_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 1024, 256)
}

func Benchmark_diff_65536_1024_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 1024, 512)
}

func Benchmark_diff_65536_1024_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 1024, 1024)
}

func Benchmark_diff_65536_1024_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 1024, 8192)
}

func Benchmark_diff_65536_1024_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 1024, 16384)
}

func Benchmark_diff_65536_1024_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 1024, 32768)
}

func Benchmark_diff_65536_1024_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 1024, 65536)
}

func Benchmark_diff_65536_1024_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 8192, 64)
}

func Benchmark_diff_65536_1024_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 8192, 128)
}

func Benchmark_diff_65536_1024_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 8192, 256)
}

func Benchmark_diff_65536_1024_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 8192, 512)
}

func Benchmark_diff_65536_1024_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 8192, 1024)
}

func Benchmark_diff_65536_1024_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 8192, 8192)
}

func Benchmark_diff_65536_1024_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 8192, 16384)
}

func Benchmark_diff_65536_1024_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 8192, 32768)
}

func Benchmark_diff_65536_1024_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 8192, 65536)
}

func Benchmark_diff_65536_1024_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 16384, 64)
}

func Benchmark_diff_65536_1024_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 16384, 128)
}

func Benchmark_diff_65536_1024_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 16384, 256)
}

func Benchmark_diff_65536_1024_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 16384, 512)
}

func Benchmark_diff_65536_1024_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 16384, 1024)
}

func Benchmark_diff_65536_1024_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 16384, 8192)
}

func Benchmark_diff_65536_1024_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 16384, 16384)
}

func Benchmark_diff_65536_1024_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 16384, 32768)
}

func Benchmark_diff_65536_1024_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 16384, 65536)
}

func Benchmark_diff_65536_1024_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 32768, 64)
}

func Benchmark_diff_65536_1024_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 32768, 128)
}

func Benchmark_diff_65536_1024_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 32768, 256)
}

func Benchmark_diff_65536_1024_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 32768, 512)
}

func Benchmark_diff_65536_1024_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 32768, 1024)
}

func Benchmark_diff_65536_1024_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 32768, 8192)
}

func Benchmark_diff_65536_1024_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 32768, 16384)
}

func Benchmark_diff_65536_1024_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 32768, 32768)
}

func Benchmark_diff_65536_1024_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 32768, 65536)
}

func Benchmark_diff_65536_1024_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 65536, 64)
}

func Benchmark_diff_65536_1024_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 65536, 128)
}

func Benchmark_diff_65536_1024_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 65536, 256)
}

func Benchmark_diff_65536_1024_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 65536, 512)
}

func Benchmark_diff_65536_1024_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 65536, 1024)
}

func Benchmark_diff_65536_1024_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 65536, 8192)
}

func Benchmark_diff_65536_1024_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 65536, 16384)
}

func Benchmark_diff_65536_1024_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 65536, 32768)
}

func Benchmark_diff_65536_1024_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 1024, 65536, 65536)
}

func Benchmark_diff_65536_65536_64_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 64, 64)
}

func Benchmark_diff_65536_65536_64_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 64, 128)
}

func Benchmark_diff_65536_65536_64_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 64, 256)
}

func Benchmark_diff_65536_65536_64_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 64, 512)
}

func Benchmark_diff_65536_65536_64_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 64, 1024)
}

func Benchmark_diff_65536_65536_64_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 64, 8192)
}

func Benchmark_diff_65536_65536_64_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 64, 16384)
}

func Benchmark_diff_65536_65536_64_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 64, 32768)
}

func Benchmark_diff_65536_65536_64_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 64, 65536)
}

func Benchmark_diff_65536_65536_128_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 128, 64)
}

func Benchmark_diff_65536_65536_128_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 128, 128)
}

func Benchmark_diff_65536_65536_128_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 128, 256)
}

func Benchmark_diff_65536_65536_128_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 128, 512)
}

func Benchmark_diff_65536_65536_128_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 128, 1024)
}

func Benchmark_diff_65536_65536_128_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 128, 8192)
}

func Benchmark_diff_65536_65536_128_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 128, 16384)
}

func Benchmark_diff_65536_65536_128_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 128, 32768)
}

func Benchmark_diff_65536_65536_128_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 128, 65536)
}

func Benchmark_diff_65536_65536_256_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 256, 64)
}

func Benchmark_diff_65536_65536_256_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 256, 128)
}

func Benchmark_diff_65536_65536_256_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 256, 256)
}

func Benchmark_diff_65536_65536_256_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 256, 512)
}

func Benchmark_diff_65536_65536_256_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 256, 1024)
}

func Benchmark_diff_65536_65536_256_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 256, 8192)
}

func Benchmark_diff_65536_65536_256_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 256, 16384)
}

func Benchmark_diff_65536_65536_256_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 256, 32768)
}

func Benchmark_diff_65536_65536_256_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 256, 65536)
}

func Benchmark_diff_65536_65536_512_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 512, 64)
}

func Benchmark_diff_65536_65536_512_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 512, 128)
}

func Benchmark_diff_65536_65536_512_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 512, 256)
}

func Benchmark_diff_65536_65536_512_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 512, 512)
}

func Benchmark_diff_65536_65536_512_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 512, 1024)
}

func Benchmark_diff_65536_65536_512_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 512, 8192)
}

func Benchmark_diff_65536_65536_512_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 512, 16384)
}

func Benchmark_diff_65536_65536_512_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 512, 32768)
}

func Benchmark_diff_65536_65536_512_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 512, 65536)
}

func Benchmark_diff_65536_65536_1024_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 1024, 64)
}

func Benchmark_diff_65536_65536_1024_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 1024, 128)
}

func Benchmark_diff_65536_65536_1024_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 1024, 256)
}

func Benchmark_diff_65536_65536_1024_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 1024, 512)
}

func Benchmark_diff_65536_65536_1024_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 1024, 1024)
}

func Benchmark_diff_65536_65536_1024_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 1024, 8192)
}

func Benchmark_diff_65536_65536_1024_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 1024, 16384)
}

func Benchmark_diff_65536_65536_1024_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 1024, 32768)
}

func Benchmark_diff_65536_65536_1024_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 1024, 65536)
}

func Benchmark_diff_65536_65536_8192_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 8192, 64)
}

func Benchmark_diff_65536_65536_8192_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 8192, 128)
}

func Benchmark_diff_65536_65536_8192_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 8192, 256)
}

func Benchmark_diff_65536_65536_8192_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 8192, 512)
}

func Benchmark_diff_65536_65536_8192_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 8192, 1024)
}

func Benchmark_diff_65536_65536_8192_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 8192, 8192)
}

func Benchmark_diff_65536_65536_8192_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 8192, 16384)
}

func Benchmark_diff_65536_65536_8192_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 8192, 32768)
}

func Benchmark_diff_65536_65536_8192_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 8192, 65536)
}

func Benchmark_diff_65536_65536_16384_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 16384, 64)
}

func Benchmark_diff_65536_65536_16384_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 16384, 128)
}

func Benchmark_diff_65536_65536_16384_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 16384, 256)
}

func Benchmark_diff_65536_65536_16384_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 16384, 512)
}

func Benchmark_diff_65536_65536_16384_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 16384, 1024)
}

func Benchmark_diff_65536_65536_16384_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 16384, 8192)
}

func Benchmark_diff_65536_65536_16384_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 16384, 16384)
}

func Benchmark_diff_65536_65536_16384_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 16384, 32768)
}

func Benchmark_diff_65536_65536_16384_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 16384, 65536)
}

func Benchmark_diff_65536_65536_32768_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 32768, 64)
}

func Benchmark_diff_65536_65536_32768_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 32768, 128)
}

func Benchmark_diff_65536_65536_32768_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 32768, 256)
}

func Benchmark_diff_65536_65536_32768_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 32768, 512)
}

func Benchmark_diff_65536_65536_32768_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 32768, 1024)
}

func Benchmark_diff_65536_65536_32768_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 32768, 8192)
}

func Benchmark_diff_65536_65536_32768_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 32768, 16384)
}

func Benchmark_diff_65536_65536_32768_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 32768, 32768)
}

func Benchmark_diff_65536_65536_32768_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 32768, 65536)
}

func Benchmark_diff_65536_65536_65536_64(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 65536, 64)
}

func Benchmark_diff_65536_65536_65536_128(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 65536, 128)
}

func Benchmark_diff_65536_65536_65536_256(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 65536, 256)
}

func Benchmark_diff_65536_65536_65536_512(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 65536, 512)
}

func Benchmark_diff_65536_65536_65536_1024(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 65536, 1024)
}

func Benchmark_diff_65536_65536_65536_8192(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 65536, 8192)
}

func Benchmark_diff_65536_65536_65536_16384(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 65536, 16384)
}

func Benchmark_diff_65536_65536_65536_32768(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 65536, 32768)
}

func Benchmark_diff_65536_65536_65536_65536(b *testing.B) {
	benchmarkDifferentCombinations(b, "diff", 65536, 65536, 65536, 65536)
}
