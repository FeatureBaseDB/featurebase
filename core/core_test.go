package core

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCompile(t *testing.T) {
	Convey("has asm", t, func() {

		So(canCompile(), ShouldEqual, true)
	})
}
