package hold

import (
	"testing"
	"time"

	"github.com/gocql/gocql/uuid"

	. "github.com/smartystreets/goconvey/convey"
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
			Hold.Set(&id, "derpsy", 10)
		}()
		time.Sleep(time.Second / 10)
		derp, _ := Hold.Get(&id, 10)
		So(derp, ShouldEqual, "derpsy")
	})
}
