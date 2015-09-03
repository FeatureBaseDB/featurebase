package pilosa_test

import (
	"math/rand"
	"testing"

	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/statsd"
	"github.com/umbel/pilosa/storage/mem"
)

var (
	size      int
	membrand  *pilosa.Brand
	cassbrand *pilosa.Brand
)

func init() {
	size = 1000

	statsd.Setup()
	// SetupCassandra()

	membrand = pilosa.NewBrand("db", "frame", 0, mem.NewStorage(), size, size, 0)
	for i := uint64(0); i < uint64(size); i++ {
		membrand.SetBit(i, 0, 1)
	}
	// cassbrand = NewBrand("db", "frame", 0, NewCassStorage(), size, size, 0)
	// for i := uint64(0); i < uint64(size); i++ {
	// 	cassbrand.SetBit(i, 0, 1)
	// }
}

func benchmarkBrand(b *testing.B, size int, fill int, brand *pilosa.Brand) {
	println(b.N)
	for i := 0; i < b.N; i++ {
		bid := rand.Int() % size
		profile := uint64(i % fill)
		brand.SetBit(uint64(bid), profile, 1)
	}
}

func BenchmarkBrandMemSetBitL2(b *testing.B) { benchmarkBrand(b, size, 1024*64, membrand) }
func BenchmarkBrandCasSetBitL2(b *testing.B) { benchmarkBrand(b, size, 1024*64, cassbrand) }
