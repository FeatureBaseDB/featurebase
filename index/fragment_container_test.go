package index

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/umbel/pilosa/util"
)

func TestFragment(t *testing.T) {

	//id := util.Id()
	general := util.Hex_to_SUUID("1")
	brand := util.Hex_to_SUUID("2")
	dummy := NewFragmentContainer()
	dummy.AddFragment("25", "general", 0, general)
	dummy.AddFragment("25", "b.n", 0, brand)

	Convey("Get ", t, func() {
		bh, _ := dummy.Get(general, 1234)
		So(bh, ShouldNotEqual, 0)
	})

	Convey("SetBit/Count 1 1", t, func() {
		//	bh, _ := dummy.Get(id, 1234)
		bi1 := uint64(1234)
		changed, _ := dummy.SetBit(general, bi1, 1, 0)
		So(changed, ShouldEqual, true)
		changed, _ = dummy.SetBit(general, bi1, 1, 0)
		So(changed, ShouldEqual, false)
		bh, _ := dummy.Get(general, bi1)
		num, _ := dummy.Count(general, bh)
		So(num, ShouldEqual, 1)
	})

	Convey("Union/Intersect/Difference", t, func() {
		bi1 := uint64(1234)
		bi2 := uint64(4321)

		dummy.SetBit(general, bi2, 65537, 0) //set_bit creates the bitmap

		bh1, _ := dummy.Get(general, bi1)
		bh2, _ := dummy.Get(general, bi2)

		handles := []BitmapHandle{bh1, bh2}
		result, _ := dummy.Union(general, handles)

		num, _ := dummy.Count(general, result)
		So(num, ShouldEqual, 2)
		result, _ = dummy.Intersect(general, handles)

		num, _ = dummy.Count(general, result)
		So(num, ShouldEqual, 0)

		result, _ = dummy.Difference(general, handles)
		num, _ = dummy.Count(general, result)
		So(num, ShouldEqual, 1)

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
			dummy.SetBit(brand, bi1, x, 0)
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
	*/
	Convey("Brand TopN", t, func() {
		dummy.SetBit(brand, uint64(1), 1, 2)
		dummy.SetBit(brand, uint64(1), 2, 2)
		dummy.SetBit(brand, uint64(1), 3, 2)
		dummy.SetBit(brand, uint64(2), 1, 2)
		dummy.SetBit(brand, uint64(2), 2, 2)
		dummy.SetBit(brand, uint64(3), 1, 2)
		bh1, _ := dummy.Get(brand, uint64(1))
		c := []uint64{2}
		results, _ := dummy.TopN(brand, bh1, 4, c)
		pair := results[0]
		So(pair.Key, ShouldEqual, 1)
		So(pair.Count, ShouldEqual, 3)
	})
	Convey("Clear ", t, func() {
		res, _ := dummy.Clear(general)
		So(res, ShouldEqual, true)
	})
	Convey("store ", t, func() {
		b := uint64(1029)
		//	compressed := "H4sIAAAJbogA/2JmYRBQ+9/IzMjI6pxRmpfN+L+JgZGJkdk7tZKRjYGRNSwxpzSV8X8LAwOD8v9moDIup5z85GzHoqLESpAwI1AjWITxfxtQjdT/VqAIV7SxUWxpZl6JmQlImJGN0YGB4R+j+v8mJkaFH/8h4B+M8X+UgcwAhZTm/yZgMCLCarC4bbAxYGHFNBpWBBmwsGIeDSuCDFhYsYyGFUEGLKxYR8OKIAMWVmyjYUWQwcDwfyYwqNgHLKjk4Q7BBAAAAAD//wEAAP//QNipzzcJAAA="
		//		compressed := "H4sIAAAJbogA/2JmYWBS+9/IzMjI6pxRmpfN+L+JgZGJkdk7tZKRjYGRNSwxpzSV8X8LAwOD8v9moDIup5z85GzHoqLESpAwI1AjWITxfxtQjdT/VqAIV7SxUWxpZl6JmQlImJGN0YGBweN/E+M/RgdGRoUf/6EAk/GbGUQy4AUAAAAA//8BAAD//2vjG9ezAAAA"
		compressed := "H4sIAAAJbogA/2JmYWBR+9/IzMjI6pxRmpfN+L+JgZGJkdk7tZKRjYGRNSwxpzSV8X8LAwOD8v9moDIup5z85GzHoqLESpAwI1AjWITxfxtQjdj/ViZGRo7o2NLMvBIzE5Ag0BiGf4zq/5uYGBV+/IeCUQZWBiikNP83AQN1NKwIMRgYAAAAAP//AQAA//9U05AivAIAAA=="
		dummy.LoadBitmap(brand, b, compressed, 0)
		bh1, _ := dummy.Get(brand, b)
		before, _ := dummy.Count(brand, bh1)
		So(4096, ShouldEqual, before)
	})

}
