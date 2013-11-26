package query

import (
    "testing"
    . "github.com/smartystreets/goconvey/convey"
)

func TestParser(t *testing.T) {
        Convey("Basic parsing", t, func() {
                tokens := Lex("get(10)")
                So(len(tokens), ShouldEqual, 4)
                So(tokens[0].Text, ShouldEqual, "get")
                So(tokens[0].Type, ShouldEqual, TYPE_FUNC)
                So(tokens[1].Text, ShouldEqual, "(")
                So(tokens[1].Type, ShouldEqual, TYPE_LP)
                So(tokens[2].Text, ShouldEqual, "10")
                So(tokens[2].Type, ShouldEqual, TYPE_ID)
                So(tokens[3].Text, ShouldEqual, ")")
                So(tokens[3].Type, ShouldEqual, TYPE_RP)
        })
}
