package query

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestQuery(t *testing.T) {
	Convey("Bracketed Lists 1", t, func() {
		tokens, err := Lex("plugin(get(88, general), [get(12, general), get(13, general)])")
		So(err, ShouldBeNil)
		filter, filters := TokensToFilterStrings(tokens)
		So(filter, ShouldEqual, "get(88,general)")
		So(filters, ShouldResemble, []string{"get(12,general)", "get(13,general)"})
	})
	Convey("Bracketed Lists 2", t, func() {
		tokens, err := Lex("plugin(intersect(get(88, general, [0]), get(77, b.n)), [get(12, general), get(13, general)])")
		So(err, ShouldBeNil)
		filter, filters := TokensToFilterStrings(tokens)
		So(filter, ShouldEqual, "intersect(get(88,general,[0]),get(77,b.n))")
		So(filters, ShouldResemble, []string{"get(12,general)", "get(13,general)"})
	})
	Convey("Bracketed Lists 3", t, func() {
		tokens, err := Lex("plugin(intersect(get(88, general, [0]), get(77, b.n)))")
		So(err, ShouldBeNil)
		filter, filters := TokensToFilterStrings(tokens)
		So(filter, ShouldEqual, "intersect(get(88,general,[0]),get(77,b.n))")
		var empty []string
		So(filters, ShouldResemble, empty)
	})
}
