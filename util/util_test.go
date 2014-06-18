package util

import (
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	. "github.com/smartystreets/goconvey/convey"
)

/*
var (
	array [1000000]int
	muid  = make(map[SUUID]int)
	muuid = make(map[*GUID]int)
	r     int
)

func init() {
	for i, _ := range array {
		muid[Id()] = i
		id := util.RandomUUID()
		muuid[&id] = i
	}

}
*/
func TestId(t *testing.T) {
	Convey("Test Small", t, func() {
		s := "1"
		//s := "0000000000000001"
		//s := "000000000000001"
		b2 := Hex_to_SUUID(s)
		So(1, ShouldEqual, b2)
	})
	Convey("Basic Usage", t, func() {
		bc1 := Id()
		println(SUUID_to_Hex(bc1))
		println(SUUID_to_Hex(bc1))
		bc2 := Id()
		println(SUUID_to_Hex(bc2))
		So(bc1, ShouldNotEqual, bc2)
	})
	Convey("Hex Encoded Usage", t, func() {
		b1 := Id()
		s := SUUID_to_Hex(b1)
		b2 := Hex_to_SUUID(s)
		So(b1, ShouldEqual, b2)
	})
	Convey("Gen 10", t, func() {
		for i := 0; i < 10; i++ {
			bc1 := Id()
			println(SUUID_to_Hex(bc1))
		}
		So(1, ShouldEqual, 1)
	})

}

func BenchmarkId(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		Id()
	}
}

func BenchmarkUUID(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		gocql.RandomUUID()
	}
}
func TestGUID(t *testing.T) {
	fmt.Println(RandomUUID().String())

}

/*
func BenchmarkLookupId(b *testing.B) {
	x := Id()
	for i := 0; i < b.N; i++ {
		if a, found := muid[x]; found {
			muid[x] = a + 1
		}
	}
}
func BenchmarkLookupUUID(b *testing.B) {
	x := util.RandomUUID()
	for i := 0; i < b.N; i++ {
		if a, found := muuid[&x]; found {
			muuid[&x] = a + 1
		}
	}
}
*/
