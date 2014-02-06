package query

import (
	"pilosa/db"
	"pilosa/util"
	"testing"

	"github.com/davecgh/go-spew/spew"
	. "github.com/smartystreets/goconvey/convey"
	"tux21b.org/v1/gocql/uuid"
)

func TestQueryPlanner(t *testing.T) {
	Convey("Basic query plan", t, func() {

		id1 := uuid.RandomUUID()
		bm1 := db.Bitmap{10, "general"}
		inputs1 := []QueryInput{&bm1}
		query1 := Query{&id1, "get", inputs1, 0, 0}

		id2 := uuid.RandomUUID()
		bm2 := db.Bitmap{20, "general"}
		inputs2 := []QueryInput{&bm2}
		query2 := Query{&id2, "get", inputs2, 0, 0}

		id3 := uuid.RandomUUID()
		inputs := []QueryInput{&query1, &query2}
		query := Query{&id3, "union", inputs, 0, 0}
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
		process_id1 := uuid.RandomUUID()
		process1 := db.NewProcess(&process_id1)
		process1.SetHost("----192.1.1.0----")
		fragment1.SetProcess(process1)

		slice2 := database.GetOrCreateSlice(1)
		fragment_id2 := util.Id()
		fragment2 := database.GetOrCreateFragment(frame, slice2, fragment_id2)
		process_id2 := uuid.RandomUUID()
		process2 := db.NewProcess(&process_id2)
		process2.SetHost("----192.1.1.1----")
		fragment2.SetProcess(process2)

		qplanner := QueryPlanner{Database: database}
		destination := fragment1.GetLocation()

		id := uuid.RandomUUID()
		qp := qplanner.Plan(&query, &id, destination)

		for i, qs := range *qp {
			//spew.Dump(i, qs, qs.inputs)
			spew.Dump(i, qs)
			spew.Dump("**************************************************************")
		}
	})
}
