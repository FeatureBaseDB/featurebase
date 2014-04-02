package query

import (
	"pilosa/db"
	"pilosa/util"
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"tux21b.org/v1/gocql/uuid"
)

func basic_database() (*db.Database, *db.Fragment) {
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
	return database, fragment1
}

func TestQueryPlanner(t *testing.T) {
	Convey("Union query plan", t, func() {

		id1 := uuid.RandomUUID()
		query1 := Query{Id: &id1, Operation: "get", Args: map[string]interface{}{"id": uint64(10), "frame": "general"}}

		id2 := uuid.RandomUUID()
		query2 := Query{Id: &id2, Operation: "get", Args: map[string]interface{}{"id": uint64(20), "frame": "general"}}

		id3 := uuid.RandomUUID()
		query := Query{Id: &id3, Operation: "union", Subqueries: []Query{query1, query2}}

		database, fragment1 := basic_database()

		qplanner := QueryPlanner{Database: database, Query: &query}
		destination := fragment1.GetLocation()

		id := uuid.RandomUUID()
		qp := *qplanner.Plan(&query, &id, destination)

		So(len(qp), ShouldEqual, 7)
		So(qp[0].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[0].(GetQueryStep).Slice, ShouldEqual, 0)
		So(*(qp[0].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "general", 0})
		So(qp[1].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[1].(GetQueryStep).Slice, ShouldEqual, 0)
		So(*(qp[1].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{20, "general", 0})
		So(qp[2].(UnionQueryStep).Operation, ShouldEqual, "union")
		So(qp[2].(UnionQueryStep).Inputs, ShouldResemble, []*uuid.UUID{
			qp[0].(GetQueryStep).Id,
			qp[1].(GetQueryStep).Id,
		})
		So(qp[3].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[3].(GetQueryStep).Slice, ShouldEqual, 1)
		So(*(qp[3].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "general", 0})
		So(qp[4].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[4].(GetQueryStep).Slice, ShouldEqual, 1)
		So(*(qp[4].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{20, "general", 0})
		So(qp[5].(UnionQueryStep).Operation, ShouldEqual, "union")
		So(qp[5].(UnionQueryStep).Inputs, ShouldResemble, []*uuid.UUID{
			qp[3].(GetQueryStep).Id,
			qp[4].(GetQueryStep).Id,
		})
		So(qp[6].(CatQueryStep).Operation, ShouldEqual, "cat")
		So(qp[6].(CatQueryStep).Inputs, ShouldResemble, []*uuid.UUID{
			qp[2].(UnionQueryStep).Id,
			qp[5].(UnionQueryStep).Id,
		})
	})
	Convey("Get query plan - including parsing", t, func() {

		query := QueryForPQL("get(10,general)")

		database, fragment1 := basic_database()

		qplanner := QueryPlanner{Database: database, Query: query}
		destination := fragment1.GetLocation()

		id := uuid.RandomUUID()
		qp := *qplanner.Plan(query, &id, destination)

		So(len(qp), ShouldEqual, 3)
		So(qp[0].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[0].(GetQueryStep).Slice, ShouldEqual, 0)
		So(*(qp[0].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "general", 0})
		So(qp[1].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[1].(GetQueryStep).Slice, ShouldEqual, 1)
		So(*(qp[1].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "general", 0})
		So(qp[2].(CatQueryStep).Operation, ShouldEqual, "cat")
		So(qp[2].(CatQueryStep).Inputs, ShouldResemble, []*uuid.UUID{
			qp[0].(GetQueryStep).Id,
			qp[1].(GetQueryStep).Id,
		})
	})
	Convey("Union query plan - including parsing", t, func() {

		query := QueryForPQL("union(get(10, general), get(20, general))")

		database, fragment1 := basic_database()

		qplanner := QueryPlanner{Database: database, Query: query}
		destination := fragment1.GetLocation()

		id := uuid.RandomUUID()
		qp := *qplanner.Plan(query, &id, destination)

		So(len(qp), ShouldEqual, 7)
		So(qp[0].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[0].(GetQueryStep).Slice, ShouldEqual, 0)
		So(*(qp[0].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "general", 0})
		So(qp[1].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[1].(GetQueryStep).Slice, ShouldEqual, 0)
		So(*(qp[1].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{20, "general", 0})
		So(qp[2].(UnionQueryStep).Operation, ShouldEqual, "union")
		So(qp[2].(UnionQueryStep).Inputs, ShouldResemble, []*uuid.UUID{
			qp[0].(GetQueryStep).Id,
			qp[1].(GetQueryStep).Id,
		})
		So(qp[3].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[3].(GetQueryStep).Slice, ShouldEqual, 1)
		So(*(qp[3].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "general", 0})
		So(qp[4].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[4].(GetQueryStep).Slice, ShouldEqual, 1)
		So(*(qp[4].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{20, "general", 0})
		So(qp[5].(UnionQueryStep).Operation, ShouldEqual, "union")
		So(qp[5].(UnionQueryStep).Inputs, ShouldResemble, []*uuid.UUID{
			qp[3].(GetQueryStep).Id,
			qp[4].(GetQueryStep).Id,
		})
		So(qp[6].(CatQueryStep).Operation, ShouldEqual, "cat")
		So(qp[6].(CatQueryStep).Inputs, ShouldResemble, []*uuid.UUID{
			qp[2].(UnionQueryStep).Id,
			qp[5].(UnionQueryStep).Id,
		})
	})
	Convey("Set query plan - including parsing", t, func() {
		query := QueryForPQL("set(10, general, 0, 100)")

		database, fragment1 := basic_database()

		qplanner := QueryPlanner{Database: database, Query: query}
		destination := fragment1.GetLocation()

		id := uuid.RandomUUID()
		qp := *qplanner.Plan(query, &id, destination)
		So(len(qp), ShouldEqual, 1)
		So(qp[0].(SetQueryStep).Operation, ShouldEqual, "set")
		So(qp[0].(SetQueryStep).ProfileId, ShouldEqual, 100)
		So(*(qp[0].(SetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "general", 0})
	})
	Convey("Top-n query plan - including parsing", t, func() {
		query := QueryForPQL("top-n(get(10, general), [1,2,3], 50)")

		database, fragment1 := basic_database()

		qplanner := QueryPlanner{Database: database, Query: query}
		destination := fragment1.GetLocation()

		id := uuid.RandomUUID()
		qp := *qplanner.Plan(query, &id, destination)
		So(len(qp), ShouldEqual, 5)
		So(qp[0].(GetQueryStep).Operation, ShouldEqual, "get")
		So(*(qp[0].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "general", 0})
		So(qp[1].(*TopNQueryStep).Operation, ShouldEqual, "top-n")
		So(qp[1].(*TopNQueryStep).Input, ShouldEqual, qp[0].(GetQueryStep).Id)
		So(qp[1].(*TopNQueryStep).N, ShouldEqual, 50)
		So(qp[2].(GetQueryStep).Operation, ShouldEqual, "get")
		So(*(qp[2].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "general", 0})
		So(qp[3].(*TopNQueryStep).Operation, ShouldEqual, "top-n")
		So(qp[3].(*TopNQueryStep).Input, ShouldEqual, qp[2].(GetQueryStep).Id)
		So(qp[3].(*TopNQueryStep).N, ShouldEqual, 50)
	})
}
