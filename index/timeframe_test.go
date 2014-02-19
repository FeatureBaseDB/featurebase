package index

import (
	"fmt"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTimeFrame(t *testing.T) {

	Convey("Test ID", t, func() {
		const shortForm = "2006-01-02 15:04"
		x, _ := time.Parse(shortForm, "2014-01-01 10:03")
		fmt.Println(x)

		m := getTimeIds(uint64(1), x, YMDH, 2014)
		spew.Dump(m)
		So(1, ShouldEqual, 1)
	})

}
