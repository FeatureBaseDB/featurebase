package cruncher

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCruncher(t *testing.T) {
	Convey("Basic Cruncher Tests", t, func() {
		spew.Dump("cruncher test")
	})
}
