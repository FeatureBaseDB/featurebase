package query

import (
	"log"
	"pilosa/db"
	"pilosa/util"
	"testing"

	"github.com/davecgh/go-spew/spew"
	. "github.com/smartystreets/goconvey/convey"
)

func basic_database() (*db.Database, *db.Fragment) {
	// create an empty database
	cluster := db.NewCluster()
	database := cluster.GetOrCreateDatabase("main")
	frame := database.GetOrCreateFrame("default")

	slice1 := database.GetOrCreateSlice(0)
	fragment_id1 := util.Id()
	fragment1 := database.GetOrCreateFragment(frame, slice1, fragment_id1)
	process_id1 := util.RandomUUID()
	process1 := db.NewProcess(&process_id1)
	process1.SetHost("----192.1.1.0----")
	fragment1.SetProcess(process1)

	slice2 := database.GetOrCreateSlice(1)
	fragment_id2 := util.Id()
	fragment2 := database.GetOrCreateFragment(frame, slice2, fragment_id2)
	process_id2 := util.RandomUUID()
	process2 := db.NewProcess(&process_id2)
	process2.SetHost("----192.1.1.1----")
	fragment2.SetProcess(process2)
	return database, fragment1
}

func TestQueryPlanner(t *testing.T) {
	Convey("Union query plan", t, func() {

		id1 := util.RandomUUID()
		query1 := Query{Id: &id1, Operation: "get", Args: map[string]interface{}{"id": uint64(10), "frame": "default"}}

		id2 := util.RandomUUID()
		query2 := Query{Id: &id2, Operation: "get", Args: map[string]interface{}{"id": uint64(20), "frame": "default"}}

		id3 := util.RandomUUID()
		query := Query{Id: &id3, Operation: "union", Subqueries: []Query{query1, query2}}

		database, fragment1 := basic_database()

		qplanner := QueryPlanner{Database: database, Query: &query}
		destination := fragment1.GetLocation()

		id := util.RandomUUID()
		log.Println(id)
		qpp, err := qplanner.Plan(&query, &id, destination)
		So(err, ShouldEqual, nil)

		qp := *qpp

		So(len(qp), ShouldEqual, 7)
		So(qp[0].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[0].(GetQueryStep).Slice, ShouldEqual, 0)
		So(*(qp[0].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "default", 0})
		So(qp[1].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[1].(GetQueryStep).Slice, ShouldEqual, 0)
		So(*(qp[1].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{20, "default", 0})
		So(qp[2].(UnionQueryStep).Operation, ShouldEqual, "union")
		So(qp[2].(UnionQueryStep).Inputs, ShouldResemble, []*util.GUID{
			qp[0].(GetQueryStep).Id,
			qp[1].(GetQueryStep).Id,
		})
		So(qp[3].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[3].(GetQueryStep).Slice, ShouldEqual, 1)
		So(*(qp[3].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "default", 0})
		So(qp[4].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[4].(GetQueryStep).Slice, ShouldEqual, 1)
		So(*(qp[4].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{20, "default", 0})
		So(qp[5].(UnionQueryStep).Operation, ShouldEqual, "union")
		So(qp[5].(UnionQueryStep).Inputs, ShouldResemble, []*util.GUID{
			qp[3].(GetQueryStep).Id,
			qp[4].(GetQueryStep).Id,
		})
		So(qp[6].(CatQueryStep).Operation, ShouldEqual, "cat")
		So(qp[6].(CatQueryStep).Inputs, ShouldResemble, []*util.GUID{
			qp[2].(UnionQueryStep).Id,
			qp[5].(UnionQueryStep).Id,
		})
	})
	Convey("Get query plan - including parsing", t, func() {

		query, err := QueryForPQL("get(10,default)")
		So(err, ShouldEqual, nil)

		database, fragment1 := basic_database()

		qplanner := QueryPlanner{Database: database, Query: query}
		destination := fragment1.GetLocation()

		id := util.RandomUUID()
		qpp, err := qplanner.Plan(query, &id, destination)
		So(err, ShouldEqual, nil)
		qp := *qpp

		So(len(qp), ShouldEqual, 3)
		So(qp[0].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[0].(GetQueryStep).Slice, ShouldEqual, 0)
		So(*(qp[0].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "default", 0})
		So(qp[1].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[1].(GetQueryStep).Slice, ShouldEqual, 1)
		So(*(qp[1].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "default", 0})
		So(qp[2].(CatQueryStep).Operation, ShouldEqual, "cat")
		So(qp[2].(CatQueryStep).Inputs, ShouldResemble, []*util.GUID{
			qp[0].(GetQueryStep).Id,
			qp[1].(GetQueryStep).Id,
		})
	})
	Convey("Union query plan - including parsing", t, func() {

		query, err := QueryForPQL("union(get(10, default), get(20, default))")
		So(err, ShouldEqual, nil)

		database, fragment1 := basic_database()

		qplanner := QueryPlanner{Database: database, Query: query}
		destination := fragment1.GetLocation()

		id := util.RandomUUID()
		qpp, err := qplanner.Plan(query, &id, destination)
		So(err, ShouldEqual, nil)
		qp := *qpp

		So(len(qp), ShouldEqual, 7)
		So(qp[0].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[0].(GetQueryStep).Slice, ShouldEqual, 0)
		So(*(qp[0].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "default", 0})
		So(qp[1].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[1].(GetQueryStep).Slice, ShouldEqual, 0)
		So(*(qp[1].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{20, "default", 0})
		So(qp[2].(UnionQueryStep).Operation, ShouldEqual, "union")
		So(qp[2].(UnionQueryStep).Inputs, ShouldResemble, []*util.GUID{
			qp[0].(GetQueryStep).Id,
			qp[1].(GetQueryStep).Id,
		})
		So(qp[3].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[3].(GetQueryStep).Slice, ShouldEqual, 1)
		So(*(qp[3].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "default", 0})
		So(qp[4].(GetQueryStep).Operation, ShouldEqual, "get")
		So(qp[4].(GetQueryStep).Slice, ShouldEqual, 1)
		So(*(qp[4].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{20, "default", 0})
		So(qp[5].(UnionQueryStep).Operation, ShouldEqual, "union")
		So(qp[5].(UnionQueryStep).Inputs, ShouldResemble, []*util.GUID{
			qp[3].(GetQueryStep).Id,
			qp[4].(GetQueryStep).Id,
		})
		So(qp[6].(CatQueryStep).Operation, ShouldEqual, "cat")
		So(qp[6].(CatQueryStep).Inputs, ShouldResemble, []*util.GUID{
			qp[2].(UnionQueryStep).Id,
			qp[5].(UnionQueryStep).Id,
		})
	})
	Convey("Set query plan - including parsing", t, func() {
		query, err := QueryForPQL("set(10, default, 0, 100)")
		So(err, ShouldEqual, nil)

		database, fragment1 := basic_database()

		qplanner := QueryPlanner{Database: database, Query: query}
		destination := fragment1.GetLocation()

		id := util.RandomUUID()
		qpp, err := qplanner.Plan(query, &id, destination)
		So(err, ShouldEqual, nil)
		qp := *qpp
		So(len(qp), ShouldEqual, 1)
		So(qp[0].(SetQueryStep).Operation, ShouldEqual, "set")
		So(qp[0].(SetQueryStep).ProfileId, ShouldEqual, 100)
		So(*(qp[0].(SetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "default", 0})
	})
	Convey("Top-n query plan - including parsing", t, func() {
		query, err := QueryForPQL("top-n(get(10, default), default, 50,[1,2,3])")
		So(err, ShouldEqual, nil)

		database, fragment1 := basic_database()
		qplanner := QueryPlanner{Database: database, Query: query}
		destination := fragment1.GetLocation()
		id := util.RandomUUID()
		qpp, err := qplanner.Plan(query, &id, destination)

		So(err, ShouldEqual, nil)
		qp := *qpp
		So(err, ShouldEqual, nil)
		So(len(qp), ShouldEqual, 5)
		So(qp[0].(GetQueryStep).Operation, ShouldEqual, "get")
		So(*(qp[0].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "default", 0})
		So(qp[1].(*TopNQueryStep).Operation, ShouldEqual, "top-n")
		So(qp[1].(*TopNQueryStep).Input, ShouldEqual, qp[0].(GetQueryStep).Id)
		So(qp[1].(*TopNQueryStep).N, ShouldEqual, 50)
		So(qp[1].(*TopNQueryStep).Filters, ShouldResemble, []uint64{1, 2, 3})
		So(qp[2].(GetQueryStep).Operation, ShouldEqual, "get")
		So(*(qp[2].(GetQueryStep).Bitmap), ShouldResemble, db.Bitmap{10, "default", 0})
		So(qp[3].(*TopNQueryStep).Operation, ShouldEqual, "top-n")
		So(qp[3].(*TopNQueryStep).Input, ShouldEqual, qp[2].(GetQueryStep).Id)
		So(qp[3].(*TopNQueryStep).N, ShouldEqual, 50)
	})
	Convey("All query plan - including parsing", t, func() {
		query, err := QueryForPQL("top-n(all(), default, 50, [1,2,3])")
		So(err, ShouldEqual, nil)

		database, fragment1 := basic_database()
		qplanner := QueryPlanner{Database: database, Query: query}
		destination := fragment1.GetLocation()
		id := util.RandomUUID()
		qpp, err := qplanner.Plan(query, &id, destination)

		So(err, ShouldEqual, nil)
		qp := *qpp
		So(err, ShouldEqual, nil)
		So(len(qp), ShouldEqual, 3)
		So(qp[0].(*TopNQueryStep).Operation, ShouldEqual, "top-n")
		So(qp[0].(*TopNQueryStep).Input, ShouldEqual, nil)
		So(qp[0].(*TopNQueryStep).N, ShouldEqual, 50)
		So(qp[0].(*TopNQueryStep).Filters, ShouldResemble, []uint64{1, 2, 3})
	})
	Convey("Get query plan - including parsing", t, func() {

		_, err := QueryForPQL("count()")
		So(err, ShouldNotEqual, nil)
		_, err = QueryForPQL("count(intersect())")
		So(err, ShouldNotEqual, nil)
		_, err = QueryForPQL("count(get(10, default))")
		So(err, ShouldEqual, nil)
		_, err = QueryForPQL("count(union())")
		So(err, ShouldNotEqual, nil)
	})

	Convey("Stash including parsing", t, func() {
		qp, err := QueryForPQL("stash(union(get(10,default),get(20,default)))")
		So(err, ShouldEqual, nil)

		database, fragment1 := basic_database()
		qplanner := QueryPlanner{Database: database, Query: qp}
		destination := fragment1.GetLocation()
		id := util.RandomUUID()
		qpp, _ := qplanner.Plan(qp, &id, destination)
		p := *qpp

		So(len(p), ShouldNotEqual, 0)
		log.Println(len(p))
		spew.Dump(p[0])
		//So(p[0].(StashQueryStep).Operation, ShouldEqual, "stash")
	})

}
