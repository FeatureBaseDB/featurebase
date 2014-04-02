package query

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
)

func TestLexer(t *testing.T) {
	Convey("Basic lexical analysis", t, func() {
		var tokens []Token
		var err error

		tokens, err = Lex("get(10)")
		So(err, ShouldBeNil)
		So(len(tokens), ShouldEqual, 4)
		So(tokens, ShouldResemble, []Token{
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"10", TYPE_VALUE},
			{")", TYPE_RP},
		})

		tokens, err = Lex("get(id=10)")
		So(err, ShouldBeNil)
		So(tokens, ShouldResemble, []Token{
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"id", TYPE_KEYWORD},
			{"=", TYPE_EQUALS},
			{"10", TYPE_VALUE},
			{")", TYPE_RP},
		})

		tokens, err = Lex("get(id=10, frame=brand)")
		So(err, ShouldBeNil)
		So(tokens, ShouldResemble, []Token{
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"id", TYPE_KEYWORD},
			{"=", TYPE_EQUALS},
			{"10", TYPE_VALUE},
			{",", TYPE_COMMA},
			{"frame", TYPE_KEYWORD},
			{"=", TYPE_EQUALS},
			{"brand", TYPE_VALUE},
			{")", TYPE_RP},
		})

		tokens, err = Lex("union(get(10))")
		So(err, ShouldBeNil)
		So(tokens, ShouldResemble, []Token{
			{"union", TYPE_FUNC},
			{"(", TYPE_LP},
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"10", TYPE_VALUE},
			{")", TYPE_RP},
			{")", TYPE_RP},
		})

		tokens, err = Lex("intersect(get(10), get(11), get(12))")
		So(err, ShouldBeNil)
		So(tokens, ShouldResemble, []Token{
			{"intersect", TYPE_FUNC},
			{"(", TYPE_LP},
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"10", TYPE_VALUE},
			{")", TYPE_RP},
			{",", TYPE_COMMA},
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"11", TYPE_VALUE},
			{")", TYPE_RP},
			{",", TYPE_COMMA},
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"12", TYPE_VALUE},
			{")", TYPE_RP},
			{")", TYPE_RP},
		})

		tokens, err = Lex("intersect(get(10), get(11), concat(get(12),get(14)))")
		So(err, ShouldBeNil)
		So(tokens, ShouldResemble, []Token{
			{"intersect", TYPE_FUNC},
			{"(", TYPE_LP},
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"10", TYPE_VALUE},
			{")", TYPE_RP},
			{",", TYPE_COMMA},
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"11", TYPE_VALUE},
			{")", TYPE_RP},
			{",", TYPE_COMMA},
			{"concat", TYPE_FUNC},
			{"(", TYPE_LP},
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"12", TYPE_VALUE},
			{")", TYPE_RP},
			{",", TYPE_COMMA},
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"14", TYPE_VALUE},
			{")", TYPE_RP},
			{")", TYPE_RP},
			{")", TYPE_RP},
		})

		tokens, err = Lex("concat(get(1, brand),get(2))")
		So(err, ShouldBeNil)
		So(tokens, ShouldResemble, []Token{
			{"concat", TYPE_FUNC},
			{"(", TYPE_LP},
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"1", TYPE_VALUE},
			{",", TYPE_COMMA},
			{"brand", TYPE_VALUE},
			{")", TYPE_RP},
			{",", TYPE_COMMA},
			{"get", TYPE_FUNC},
			{"(", TYPE_LP},
			{"2", TYPE_VALUE},
			{")", TYPE_RP},
			{")", TYPE_RP},
		})

		tokens, err = Lex("set(1, 987)")
		So(err, ShouldBeNil)
		So(tokens, ShouldResemble, []Token{
			{"set", TYPE_FUNC},
			{"(", TYPE_LP},
			{"1", TYPE_VALUE},
			{",", TYPE_COMMA},
			{"987", TYPE_VALUE},
			{")", TYPE_RP},
		})

		tokens, err = Lex("set(1, general, 987)")
		So(err, ShouldBeNil)
		So(tokens, ShouldResemble, []Token{
			{"set", TYPE_FUNC},
			{"(", TYPE_LP},
			{"1", TYPE_VALUE},
			{",", TYPE_COMMA},
			{"general", TYPE_VALUE},
			{",", TYPE_COMMA},
			{"987", TYPE_VALUE},
			{")", TYPE_RP},
		})

		tokens, err = Lex("top-n(get(10), 8)")
		So(tokens, ShouldResemble, []Token{
			Token{"top-n", TYPE_FUNC},
			Token{"(", TYPE_LP},
			Token{"get", TYPE_FUNC},
			Token{"(", TYPE_LP},
			Token{"10", TYPE_VALUE},
			Token{")", TYPE_RP},
			Token{",", TYPE_COMMA},
			Token{"8", TYPE_VALUE},
			Token{")", TYPE_RP},
		})

		tokens, err = Lex("top-n(get(10, general), [1,2,3])")
		So(tokens, ShouldResemble, []Token{
			Token{"top-n", TYPE_FUNC},
			Token{"(", TYPE_LP},
			Token{"get", TYPE_FUNC},
			Token{"(", TYPE_LP},
			Token{"10", TYPE_VALUE},
			Token{",", TYPE_COMMA},
			Token{"general", TYPE_VALUE},
			Token{")", TYPE_RP},
			Token{",", TYPE_COMMA},
			Token{"[", TYPE_LB},
			Token{"1", TYPE_VALUE},
			Token{",", TYPE_COMMA},
			Token{"2", TYPE_VALUE},
			Token{",", TYPE_COMMA},
			Token{"3", TYPE_VALUE},
			Token{"]", TYPE_RB},
			Token{")", TYPE_RP},
		})
	})
}
