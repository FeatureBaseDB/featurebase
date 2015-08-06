package hold

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/umbel/pilosa/util"
)

func TestHoldChan(t *testing.T) {

	Hold := Holder{make(map[util.GUID]holdchan), make(chan gethold), make(chan delhold)}
	go Hold.Run()

	Convey("set then get", t, func() {
		id := util.RandomUUID()
		Hold.Set(&id, "derp", 10)
		derp, _ := Hold.Get(&id, 10)
		So(derp, ShouldEqual, "derp")
	})
	Convey("get then set", t, func() {
		id := util.RandomUUID()
		go func() {
			Hold.Set(&id, "derpsy", 10)
		}()
		time.Sleep(time.Second / 10)
		derp, _ := Hold.Get(&id, 10)
		So(derp, ShouldEqual, "derpsy")
	})
}
