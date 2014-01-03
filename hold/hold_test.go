package hold

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"tux21b.org/v1/gocql/uuid"
)

func TestHoldChan(t *testing.T) {
	Convey("set then get", t, func() {
		id := uuid.RandomUUID()
		Hold.Set(&id, "derp")
		derp := Hold.Get(&id)
		So(derp, ShouldEqual, "derp")
	})
	Convey("get then set", t, func() {
		id := uuid.RandomUUID()
		go func() {
			time.Sleep(time.Second / 10)
			Hold.Set(&id, "derpsy")
		}()
		derp := Hold.Get(&id)
		So(derp, ShouldEqual, "derpsy")
	})
}
