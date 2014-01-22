package index

import (
	"testing"

	//	"io/ioutil"
	//   "time"
	"pilosa/util"
	. "github.com/smartystreets/goconvey/convey"
)

func TestFragment(t *testing.T) {

	//id := util.Id()
	general := util.Hex_to_SUUID("1")
	brand := util.Hex_to_SUUID("2")
	dummy := NewFragmentContainer()
	dummy.AddFragment("25", "general", 0, general)
	dummy.AddFragment("25", "Brand", 0, brand)

	Convey("Get ", t, func() {
		bh, _ := dummy.Get(general, 1234)
		So(bh, ShouldNotEqual, 0)
	})

	Convey("SetBit/Count 1 1", t, func() {
		//	bh, _ := dummy.Get(id, 1234)
		bi1 := uint64(1234)
		changed, _ := dummy.SetBit(general, bi1, 1)
		So(changed, ShouldEqual, true)
		changed, _ = dummy.SetBit(general, bi1, 1)
		So(changed, ShouldEqual, false)
		bh, _ := dummy.Get(general, bi1)
		num, _ := dummy.Count(general, bh)
		So(num, ShouldEqual, 1)
	})

	Convey("Union/Intersect", t, func() {
		bi1 := uint64(1234)
		bi2 := uint64(4321)

		dummy.SetBit(general, bi2, 65537) //set_bit creates the bitmap

		bh1, _ := dummy.Get(general, bi1)
		bh2, _ := dummy.Get(general, bi2)

		handles := []BitmapHandle{bh1, bh2}
		result, _ := dummy.Union(general, handles)

		num, _ := dummy.Count(general, result)
		So(num, ShouldEqual, 2)
		result, _ = dummy.Intersect(general, handles)

		num, _ = dummy.Count(general, result)
		So(num, ShouldEqual, 0)
	})
	Convey("Union Empty", t, func() {

		bi1 := uint64(1234)
		bh1, _ := dummy.Get(general, bi1)
		bh2, _ := dummy.Empty(general) //set_bit creates the bitmap

		handles := []BitmapHandle{bh1, bh2}
		result, _ := dummy.Union(general, handles)

		num, _ := dummy.Count(general, result)

		So(num, ShouldEqual, 1)
	})
	Convey("Bytes", t, func() {
		bi1 := uint64(1234)
		bh1, _ := dummy.Get(general, bi1)
		before, _ := dummy.Count(general, bh1)

		bytes, _ := dummy.GetBytes(general, bh1)
		bh2, _ := dummy.FromBytes(general, bytes)

		after, _ := dummy.Count(general, bh2)
		So(before, ShouldEqual, after)
	})
	Convey("Empty ", t, func() {
		bh, _ := dummy.Empty(general)
		before, _ := dummy.Count(general, bh)
		So(before, ShouldEqual, 0)
	})

	Convey("GetList ", t, func() {
		bhs, _ := dummy.GetList(general, []uint64{1234, 4321, 789})
		result, _ := dummy.Union(general, bhs)
		num, _ := dummy.Count(general, result)
		So(num, ShouldEqual, 2)
	})

	Convey("Brand SetBit Small", t, func() {
		bi1 := uint64(1029)
		for x := uint64(0); x < 1000; x++ {
			dummy.SetBit(brand, bi1, x)
		}
		So(1, ShouldEqual, 1)
	})

	/*
		Convey("Brand SetBit Big", t, func() {
			bi1 := uint64(1231)
			bi2 := uint64(1232)
			bi3 := uint64(1233)
			bi4 := uint64(1234)
			for x := uint64(0); x < 60000; x++ {
				if x < 100 {
					dummy.SetBit(brand, bi1, x)
					dummy.SetBit(brand, bi4, x)
				}
				if x < 500 {
					dummy.SetBit(brand, bi2, x)
				}
				if x%3 == 0 && x < 1000 {
					dummy.SetBit(brand, bi3, x)
				}
				if x > 700 && x < 1000 {
					dummy.SetBit(brand, bi4, x)
				}
				if x > 1000 {
					dummy.SetBit(brand, x, x)
				}
			}
			bh1, _ := dummy.Get(brand, bi1)
			//	dummy.Rank()
			log.Println(dummy.TopN(brand, bh1, 4))
			log.Println(dummy.Stats(brand))
			So(1, ShouldEqual, 1)
		})
			Convey("Brand TopN", t, func() {
				max_brands := uint64(5000)
				for i := uint64(0); i < max_brands; i++ {
					for x := uint64(0); x < i; x = x + 1 {
						dummy.SetBit(brand, uint64(i), x)
					}

				}
				bh1, _ := dummy.Get(brand, uint64(4999))
				log.Println(dummy.TopN(brand, bh1, 4))

				So(1, ShouldEqual, 1)
			})
	*/
	Convey("Clear ", t, func() {
		res, _ := dummy.Clear(general)
		So(res, ShouldEqual, true)
	})

	Convey("store ", t, func() {
		b := uint64(1029)
		compressed := "H4sIAAAJbogA/2JmYRBQ+9/IzMjI6pxRmpfN+L+JgZGJkdk7tZKRjYGRNSwxpzSV8X8LAwOD8v9moDIup5z85GzHoqLESpAwI1AjWITxfxtQjdT/VqAIV7SxUWxpZl6JmQlImJGN0YGB4R+j+v8mJkaFH/8h4B+M8X+UgcwAhZTm/yZgMCLCarC4bbAxYGHFNBpWBBmwsGIeDSuCDFhYsYyGFUEGLKxYR8OKIAMWVmyjYUWQwcDwfyYwqNgHLKjk4Q7BBAAAAAD//wEAAP//QNipzzcJAAA="
		dummy.LoadBitmap(brand, b, compressed)
		bh1, _ := dummy.Get(brand, b)
		before, _ := dummy.Count(brand, bh1)
		So(15228, ShouldEqual, before)
	})

}
