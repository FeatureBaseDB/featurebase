package index

import (
	//	"fmt"
	"math/rand"
	"testing"
	//	"time"

	//	. "github.com/smartystreets/goconvey/convey"
)

var (
	size      int
	membrand  *Brand
	cassbrand *Brand
)

func init() {
	println("GO")
	size = 1000

	membrand = NewBrand("db", "frame", 0, NewMemoryStorage(), size, size, 0)
	for i := uint64(0); i < uint64(size); i++ {
		membrand.SetBit(i, 0, 1)
	}
	cassbrand = NewBrand("db", "frame", 0, NewCassStorage(), size, size, 0)
	for i := uint64(0); i < uint64(size); i++ {
		cassbrand.SetBit(i, 0, 1)
	}
}

/*
func TestBitmaps(t *testing.T) {
	Convey("function BitCount should equal method bm.Count()", t, func() {
		bm := CreateRBBitmap()
		for i := uint64(0); i < uint64(4096); i++ {
			SetBit(bm, i)
		}
		bc1 := BitCount(bm)
		bc2 := bm.Count()
		So(bc1, ShouldEqual, bc2)
		So(bc1, ShouldEqual, 4096)
	})
	Convey("function Difference 1 and not 0 => true ", t, func() {
		bm1 := CreateRBBitmap()
		bm2 := CreateRBBitmap()
		SetBit(bm1, 1)
		//SetBit(bm2,2)
		all := Difference(bm1, bm2)
		res := BitCount(all)

		So(1, ShouldEqual, res)
	})

	Convey("function Difference 1 and not 0 => true ", t, func() {
		bm1 := CreateRBBitmap()
		bm2 := CreateRBBitmap()
		SetBit(bm1, 1)
		SetBit(bm1, 2)
		SetBit(bm1, 3)
		SetBit(bm1, 4)
		SetBit(bm2, 3)
		//SetBit(bm2,2)
		all := Difference(bm1, bm2)
		res := BitCount(all)

		So(3, ShouldEqual, res)
	})

	Convey("UNION even + odd equal 4096 ", t, func() {
		even := CreateRBBitmap()
		for i := uint64(0); i < uint64(4096); i += 2 {
			SetBit(even, i)
		}

		odd := CreateRBBitmap()
		for i := uint64(1); i < uint64(4096); i += 2 {
			SetBit(odd, i)
		}
		all := Union(even, odd)
		total_bits := BitCount(all)

		So(total_bits, ShouldEqual, 4096)
	})

	Convey("Intersection even - odd equal 0 ", t, func() {
		even := CreateRBBitmap()
		for i := uint64(0); i < uint64(4096); i += 2 {
			SetBit(even, i)
		}

		odd := CreateRBBitmap()
		for i := uint64(1); i < uint64(4096); i += 2 {
			SetBit(odd, i)
		}
		all := Intersection(even, odd)
		total_bits := BitCount(all)

		So(total_bits, ShouldEqual, 0)

	})

	Convey("Bitcount< 1s ", t, func() {
		all := CreateRBBitmap()
		for i := uint64(0); i < uint64(65536); i++ {
			SetBit(all, i)
		}
		start := time.Now()
		BitCount(all)
		So(start, ShouldHappenWithin, time.Duration(1)*time.Millisecond, time.Now())
	})

	Convey("Compressed ", t, func() {
		all := CreateRBBitmap()
		for i := uint64(0); i < uint64(4096); i++ {
			SetBit(all, i)
		}
		cs := all.ToCompressString()
		fmt.Println(cs)
		bm := CreateRBBitmap()
		bm.FromCompressString(cs)
		So(BitCount(all), ShouldEqual, BitCount(bm))
	})

	Convey("AndCount ", t, func() {
		a := CreateRBBitmap()
		for i := uint64(0); i < uint64(4096); i++ {
			SetBit(a, i)
		}
		b := CreateRBBitmap()
		for i := uint64(0); i < uint64(8192); i++ {
			SetBit(b, i)
		}
		c1 := IntersectionCount(a, b)
		c := Intersection(a, b)
		So(c1, ShouldEqual, BitCount(c))
	})

}
*/
func benchmark_(b *testing.B, size int, fill int, brand *Brand) {

	println(b.N)
	for i := 0; i < b.N; i++ {
		bid := rand.Int() % size
		profile := uint64(i % fill)
		brand.SetBit(uint64(bid), profile, 1)
	}
}
func BenchmarkBrandMemSetBitL2(b *testing.B) {
	benchmark_(b, size, 1024*64, membrand)
}
func BenchmarkBrandCasSetBitL2(b *testing.B) {
	benchmark_(b, size, 1024*64, cassbrand)
}

/*
func BenchmarkSetBit(b *testing.B) {
	// run the Fib function b.N times
	a := CreateRBBitmap()
	for n := 0; n < b.N; n++ {
		SetBit(a, uint64(n))
	}
}
*/
