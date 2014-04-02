package query

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
)

func TestQueryParser(t *testing.T) {
	Convey("Basic parse - get()", t, func() {
		tokens, err := Lex("get(10)")
		So(err, ShouldBeNil)

		query, err := Parse(tokens)
		So(err, ShouldBeNil)

		So(query.Operation, ShouldEqual, "get")
		So(query.Args, ShouldResemble, map[string]interface{}{"id": uint64(10), "frame": "general"})
	})
	Convey("Basic parse - set()", t, func() {
		tokens, err := Lex("set(10, general, 0, 20)")
		So(err, ShouldBeNil)

		query, err := Parse(tokens)
		So(err, ShouldBeNil)

		So(query.Operation, ShouldEqual, "set")
		So(query.Args, ShouldResemble, map[string]interface{}{"id": uint64(10), "frame": "general", "filter": int(0), "profile_id": uint64(20)})
	})
	Convey("Basic nested query parse", t, func() {
		tokens, err := Lex("union(get(10,general), get(11,brand), get(12))")
		So(err, ShouldBeNil)

		query, err := Parse(tokens)
		So(err, ShouldBeNil)

		So(query.Operation, ShouldEqual, "union")
		So(len(query.Subqueries), ShouldEqual, 3)

		So(query.Subqueries[0].Operation, ShouldEqual, "get")
		So(query.Subqueries[0].Args, ShouldResemble, map[string]interface{}{"id": uint64(10), "frame": "general"})
		So(query.Subqueries[1].Operation, ShouldEqual, "get")
		So(query.Subqueries[1].Args, ShouldResemble, map[string]interface{}{"id": uint64(11), "frame": "brand"})
		So(query.Subqueries[2].Operation, ShouldEqual, "get")
		So(query.Subqueries[2].Args, ShouldResemble, map[string]interface{}{"id": uint64(12), "frame": "general"})
	})
	Convey("Keyword args", t, func() {
		tokens, err := Lex("get(id=10)")
		So(err, ShouldBeNil)

		query, err := Parse(tokens)
		So(err, ShouldBeNil)

		So(query.Operation, ShouldEqual, "get")
		So(query.Args, ShouldResemble, map[string]interface{}{"id": uint64(10), "frame": "general"})
	})
	Convey("Keyword args - multiple", t, func() {
		tokens, err := Lex("get(id=10, frame=brands)")
		So(err, ShouldBeNil)

		query, err := Parse(tokens)
		So(err, ShouldBeNil)

		So(query.Operation, ShouldEqual, "get")
		So(query.Args, ShouldResemble, map[string]interface{}{"id": uint64(10), "frame": "brands"})
	})
	Convey("Lists", t, func() {
		tokens, err := Lex("top-n(get(10, general), [1,2,3], 50)")
		So(err, ShouldBeNil)

		query, err := Parse(tokens)
		So(err, ShouldBeNil)

		So(query.Operation, ShouldEqual, "top-n")
		So(query.Args, ShouldResemble, map[string]interface{}{"ids": []uint64{1, 2, 3}, "n": 50})

		So(len(query.Subqueries), ShouldEqual, 1)
		So(query.Subqueries[0].Operation, ShouldEqual, "get")
		So(query.Subqueries[0].Args, ShouldResemble, map[string]interface{}{"id": uint64(10), "frame": "general"})
	})
}
