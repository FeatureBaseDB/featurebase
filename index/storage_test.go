package index

import (
	"fmt"
	"net"
	"testing"
	"time"

	//	"io/ioutil"
	//   "time"
	. "github.com/smartystreets/goconvey/convey"
)

func TestStorage(t *testing.T) {
	db := "db"
	frame := "main"
	slice := 0
	filter := 10
	bitmap_id := uint64(1234)
	/*	Convey("KV ", t, func() {
			storage, _ := NewKVStorage("/tmp/", 0, db)
			bm := storage.Fetch(bitmap_id, db, slice)
			SetBit(bm, 0)
			SetBit(bm, 1)
			SetBit(bm, 2)
			storage.Store(int64(bitmap_id), db, frame, slice, filter, bm.(*Bitmap))
			bm2, _ := storage.Fetch(bitmap_id, db, slice)
			So(BitCount(bm), ShouldEqual, BitCount(bm2))
			So(BitCount(bm), ShouldEqual, bm.Count())
			So(BitCount(bm), ShouldEqual, 3)

		})
	*/
	c, err := net.DialTimeout("tcp", "127.0.0.1:9042", 100*time.Millisecond)
	if err != nil {
		fmt.Println("NO cassandra. Skipping test.")
	} else {
		c.Close()
		Convey("cassandra", t, func() {
			fmt.Println("GO")
			storage := NewCassStorage("127.0.0.1", "hotbox")

			fmt.Println("FETCH")
			bm, _ := storage.Fetch(bitmap_id, db, frame, slice)
			SetBit(bm, 0)
			SetBit(bm, 1)
			SetBit(bm, 2)
			fmt.Println("STORE")
			storage.Store(int64(bitmap_id), db, frame, slice, filter, bm.(*Bitmap))
			fmt.Println("FETCH")
			bm2, _ := storage.Fetch(bitmap_id, db, frame, slice)
			So(BitCount(bm), ShouldEqual, BitCount(bm2))
			So(BitCount(bm), ShouldEqual, bm.Count())
			So(BitCount(bm), ShouldEqual, 3)

		})
	}

}
