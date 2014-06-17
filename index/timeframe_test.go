package index

import (
	"fmt"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTimeFrame(t *testing.T) {

	//print get_YMD_id(2014,3,28,1234)
	Convey("Test ID", t, func() {
		const shortForm = "2006-01-02 15:04"
		x, _ := time.Parse(shortForm, "1970-01-01 00:00:00")
		fmt.Println(x)

		m := GetTimeIds(uint64(15027), x, YMD)
		spew.Dump(m)
		fmt.Println("OK")
		So(1, ShouldEqual, 1)
	})
	Convey("Test 1H", t, func() {
		const shortForm = "2006-01-02 15:04"
		t1, _ := time.Parse(shortForm, "2014-01-02 10:03")
		t2, _ := time.Parse(shortForm, "2014-01-02 11:03")
		m := GetRange(t1, t2, uint64(1))
		So(len(m), ShouldEqual, 1)
	})
	Convey("Test 2H", t, func() {
		const shortForm = "2006-01-02 15:04"
		t1, _ := time.Parse(shortForm, "2014-01-02 10:03")
		t2, _ := time.Parse(shortForm, "2014-01-02 12:03")
		m := GetRange(t1, t2, uint64(1))
		So(len(m), ShouldEqual, 2)
	})

	Convey("Test 24H", t, func() {
		const shortForm = "2006-01-02 15:04"
		t1, _ := time.Parse(shortForm, "2014-01-02 12:03")
		t2, _ := time.Parse(shortForm, "2014-01-03 12:03")
		m := GetRange(t1, t2, uint64(1))
		So(len(m), ShouldEqual, 24)
	})
	Convey("Test 1D", t, func() {
		const shortForm = "2006-01-02 15:04"
		t1, _ := time.Parse(shortForm, "2014-01-02 00:00")
		t2, _ := time.Parse(shortForm, "2014-01-03 00:00")
		m := GetRange(t1, t2, uint64(1))
		So(len(m), ShouldEqual, 1)
	})

	Convey("Test 1D1H", t, func() {
		const shortForm = "2006-01-02 15:04"
		t1, _ := time.Parse(shortForm, "2014-01-02 00:00")
		t2, _ := time.Parse(shortForm, "2014-01-03 01:00")
		m := GetRange(t1, t2, uint64(1))
		So(len(m), ShouldEqual, 2)
	})

	Convey("Test 1H1D", t, func() {
		const shortForm = "2006-01-02 15:04"
		t1, _ := time.Parse(shortForm, "2014-01-02 23:00")
		t2, _ := time.Parse(shortForm, "2014-01-04 00:00")
		m := GetRange(t1, t2, uint64(1))
		So(len(m), ShouldEqual, 2)
	})

	Convey("Test 1H1D1H", t, func() {
		const shortForm = "2006-01-02 15:04"
		t1, _ := time.Parse(shortForm, "2014-01-02 23:00")
		t2, _ := time.Parse(shortForm, "2014-01-04 01:00")
		m := GetRange(t1, t2, uint64(1))
		So(len(m), ShouldEqual, 3)
	})

	Convey("Test 1Y", t, func() {
		const shortForm = "2006-01-02 15:04"
		t1, _ := time.Parse(shortForm, "2014-01-01 00:00")
		t2, _ := time.Parse(shortForm, "2015-01-01 00:00")
		m := GetRange(t1, t2, uint64(1))
		So(len(m), ShouldEqual, 1)
	})
	Convey("Test 1H1D1M", t, func() {
		const shortForm = "2006-01-02 15:04"
		t1, _ := time.Parse(shortForm, "2014-01-30 23:00")
		t2, _ := time.Parse(shortForm, "2014-03-01 00:00")
		m := GetRange(t1, t2, uint64(1))
		So(len(m), ShouldEqual, 3)
	})

	Convey("Test 1H1D1MD1H1", t, func() {
		const shortForm = "2006-01-02 15:04"
		t1, _ := time.Parse(shortForm, "2014-01-30 23:00")
		t2, _ := time.Parse(shortForm, "2014-03-02 01:00")
		m := GetRange(t1, t2, uint64(1))
		So(len(m), ShouldEqual, 5)
	})

}
