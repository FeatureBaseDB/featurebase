package hold

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"tux21b.org/v1/gocql/uuid"
)

func TestHoldChan(t *testing.T) {

	Hold := Holder{make(map[uuid.UUID]holdchan), make(chan gethold), make(chan delhold)}
	go Hold.Run()

	Convey("set then get", t, func() {
		id := uuid.RandomUUID()
		Hold.Set(&id, "derp", 10)
		derp, _ := Hold.Get(&id, 10)
		So(derp, ShouldEqual, "derp")
	})
	Convey("get then set", t, func() {
		id := uuid.RandomUUID()
		go func() {
			time.Sleep(time.Second / 10)
			Hold.Set(&id, "derpsy", 10)
		}()
		derp, _ := Hold.Get(&id, 10)
		So(derp, ShouldEqual, "derpsy")
	})
}
