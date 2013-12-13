package index

import (
	"testing"

	//	"io/ioutil"
	//   "time"
	"github.com/nu7hatch/gouuid"
	. "github.com/smartystreets/goconvey/convey"
)

func TestServer(t *testing.T) {

	id, _ := uuid.NewV4()
	dummy := NewFragmentContainer()
	dummy.AddFragment("general", "25", 0, id)

	Convey("Get ", t, func() {
		bh, _ := dummy.Get(id, 1234)
		So(bh, ShouldNotEqual, 0)
	})

	Convey("SetBit/Count 1 1", t, func() {
		bh, _ := dummy.Get(id, 1234)
		changed, _ := dummy.SetBit(id, bh, 1)
		So(changed, ShouldEqual, true)
		changed, _ = dummy.SetBit(id, bh, 1)
		So(changed, ShouldEqual, false)
		num, _ := dummy.Count(id, bh)
		So(num, ShouldEqual, 1)
	})

	Convey("Union/Intersect", t, func() {
		bh1, _ := dummy.Get(id, 1234)
		//		dummy.SetBit(id, bh1, 1)

		bh2, _ := dummy.Get(id, 4321)
		dummy.SetBit(id, bh2, 2)

		handles := []BitmapHandle{bh1, bh2}
		result, _ := dummy.Union(id, handles)

		num, _ := dummy.Count(id, result)
		So(num, ShouldEqual, 2)
		result, _ = dummy.Intersect(id, handles)

		num, _ = dummy.Count(id, result)
		So(num, ShouldEqual, 0)
	})
}
