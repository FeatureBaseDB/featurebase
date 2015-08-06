package db

import (
	"log"
	"testing"

	"github.com/davecgh/go-spew/spew"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/umbel/pilosa/util"
)

func TestTopology(t *testing.T) {
	Convey("Basic DB structures", t, func() {
		log.Println("topology test")

		cluster := NewCluster()
		database := cluster.GetOrCreateDatabase("main")

		frame := database.GetOrCreateFrame("general")
		slice := database.GetOrCreateSlice(0)

		fragment_id := util.Id()
		spew.Dump(fragment_id)
		database.GetOrCreateFragment(frame, slice, fragment_id)

		//	spew.Dump(database)
		//	spew.Dump("DONE")

	})
}
