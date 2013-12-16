package db

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/nu7hatch/gouuid"
	. "github.com/smartystreets/goconvey/convey"
	"log"
	"testing"
)

func TestTopology(t *testing.T) {
	Convey("Basic DB structures", t, func() {
		log.Println("topology test")

		cluster := NewCluster()
		database := cluster.GetOrCreateDatabase("main")

		frame := database.GetOrCreateFrame("general")
		slice := database.GetOrCreateSlice(0)

		fragment_id, _ := uuid.ParseHex("6a9aea17-2915-4eb4-858f-a8d7d4dc0a1e")
		spew.Dump(fragment_id)
		database.GetOrCreateFragment(frame, slice, fragment_id)

		spew.Dump(database)
		spew.Dump("DONE")

	})
}
