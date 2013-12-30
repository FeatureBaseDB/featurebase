package query

import (
	"pilosa/db"
	"pilosa/util"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/nu7hatch/gouuid"
	. "github.com/smartystreets/goconvey/convey"
)

func TestQueryPlanner(t *testing.T) {
	Convey("Basic query plan", t, func() {

		bm1 := db.Bitmap{10, "general"}
		inputs1 := []QueryInput{&bm1}
		query1 := Query{"get", inputs1, 0}

		bm2 := db.Bitmap{20, "general"}
		inputs2 := []QueryInput{&bm2}
		query2 := Query{"get", inputs2, 0}

		inputs := []QueryInput{&query1, &query2}
		query := Query{"union", inputs, 0}
		/*

			bm1 := db.Bitmap{10, "general"}
			inputs1 := []QueryInput{&bm1}
			query1 := Query{"get", inputs1}
			query := query1
		*/

		// create an empty database
		cluster := db.NewCluster()
		database := cluster.GetOrCreateDatabase("main")
		frame := database.GetOrCreateFrame("general")

		slice1 := database.GetOrCreateSlice(0)
		fragment_id1 := util.Id()
		fragment1 := database.GetOrCreateFragment(frame, slice1, fragment_id1)
		process_id1, _ := uuid.NewV4()
		process1 := db.NewProcess(process_id1)
		process1.SetHost("----192.1.1.0----")
		fragment1.SetProcess(process1)

		slice2 := database.GetOrCreateSlice(1)
		fragment_id2 := util.Id()
		fragment2 := database.GetOrCreateFragment(frame, slice2, fragment_id2)
		process_id2, _ := uuid.NewV4()
		process2 := db.NewProcess(process_id2)
		process2.SetHost("----192.1.1.1----")
		fragment2.SetProcess(process2)

		qplanner := QueryPlanner{Database: database}
		destination := db.Process{}

		id, _ := uuid.NewV4()
		qp := qplanner.Plan(&query, id, &destination)

		for i, qs := range *qp {
			spew.Dump(i, qs, qs.inputs)
			spew.Dump("**************************************************************")
		}
	})
}
