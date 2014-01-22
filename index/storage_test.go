package index

import (
	"testing"

	//	"io/ioutil"
	//   "time"
	. "github.com/smartystreets/goconvey/convey"
)

func TestStorage(t *testing.T) {
	db := "db"
	slice := 0
	bitmap_id := uint64(1234)
	Convey("KV ", t, func() {
		storage, _ := NewKVStorage("/tmp/", 0, db)
		bm := storage.Fetch(bitmap_id, db, slice)
		SetBit(bm, 0)
		SetBit(bm, 1)
		SetBit(bm, 2)
		storage.Store(int64(bitmap_id), db, slice, bm.(*Bitmap))
		bm2 := storage.Fetch(bitmap_id, db, slice)
		So(BitCount(bm), ShouldEqual, BitCount(bm2))
		So(BitCount(bm), ShouldEqual, bm.Count())
		So(BitCount(bm), ShouldEqual, 3)

	})
	/*
		Convey("cassandra", t, func() {
			storage, _ := NewCassStorage("127.0.0.1", "hotbox")

			bm := storage.Fetch(bitmap_id, db, slice)
			SetBit(bm, 0)
			SetBit(bm, 1)
			SetBit(bm, 2)
			storage.Store(int64(bitmap_id), db, slice, bm.(*Bitmap))
			bm2 := storage.Fetch(bitmap_id, db, slice)
			So(BitCount(bm), ShouldEqual, BitCount(bm2))
			So(BitCount(bm), ShouldEqual, bm.Count())
			So(BitCount(bm), ShouldEqual, 3)

		})
	*/

}
