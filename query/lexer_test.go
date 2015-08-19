package query_test

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa/query"
)

// Ensure a simple function and value can be lexed.
func TestLexer_Lex_FuncValue(t *testing.T) {
	if tokens, err := query.Lex("get(10)"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"10", query.TYPE_VALUE},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure a simple function, keyword, & value can be lexed.
func TestLexer_Lex_FuncKeywordValue(t *testing.T) {
	if tokens, err := query.Lex("get(id=10)"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"id", query.TYPE_KEYWORD},
		{"=", query.TYPE_EQUALS},
		{"10", query.TYPE_VALUE},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure a more complex function can be lexed.
func TestLexer_Lex_FuncComplex(t *testing.T) {
	if tokens, err := query.Lex("get(id=10, frame=brand)"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"id", query.TYPE_KEYWORD},
		{"=", query.TYPE_EQUALS},
		{"10", query.TYPE_VALUE},
		{",", query.TYPE_COMMA},
		{"frame", query.TYPE_KEYWORD},
		{"=", query.TYPE_EQUALS},
		{"brand", query.TYPE_VALUE},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure that nested functions can be lexed.
func TestLexer_Lex_FuncNested(t *testing.T) {
	if tokens, err := query.Lex("union(get(10))"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"union", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"10", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure that a list of nested functions can be lexed.
func TestLexer_Lex_FuncNestedList(t *testing.T) {
	if tokens, err := query.Lex("intersect(get(10), get(11), get(12))"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"intersect", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"10", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{",", query.TYPE_COMMA},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"11", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{",", query.TYPE_COMMA},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"12", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure that complex nested functions can be lexed.
func TestLexer_Lex_FuncNestedComplex(t *testing.T) {
	if tokens, err := query.Lex("intersect(get(10), get(11), concat(get(12),get(14)))"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"intersect", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"10", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{",", query.TYPE_COMMA},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"11", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{",", query.TYPE_COMMA},
		{"concat", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"12", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{",", query.TYPE_COMMA},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"14", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{")", query.TYPE_RP},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure that complex nested functions can be lexed.
func TestLexer_Lex_FuncNestedComplex2(t *testing.T) {
	if tokens, err := query.Lex("concat(get(1, brand),get(2))"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"concat", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"1", query.TYPE_VALUE},
		{",", query.TYPE_COMMA},
		{"brand", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{",", query.TYPE_COMMA},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"2", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure that a set function can be lexed.
func TestLexer_Lex_Set1(t *testing.T) {
	if tokens, err := query.Lex("set(1, 987)"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"set", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"1", query.TYPE_VALUE},
		{",", query.TYPE_COMMA},
		{"987", query.TYPE_VALUE},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure that a set function can be lexed.
func TestLexer_Lex_Set2(t *testing.T) {
	if tokens, err := query.Lex("set(1, general, 987)"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"set", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"1", query.TYPE_VALUE},
		{",", query.TYPE_COMMA},
		{"general", query.TYPE_VALUE},
		{",", query.TYPE_COMMA},
		{"987", query.TYPE_VALUE},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure that a TopN function can be lexed.
func TestLexer_Lex_TopN1(t *testing.T) {
	if tokens, err := query.Lex("top-n(get(10), 8)"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"top-n", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"10", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{",", query.TYPE_COMMA},
		{"8", query.TYPE_VALUE},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure that a TopN function can be lexed.
func TestLexer_Lex_TopN2(t *testing.T) {
	if tokens, err := query.Lex("top-n(get(10, general), [1,2,3])"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"top-n", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"10", query.TYPE_VALUE},
		{",", query.TYPE_COMMA},
		{"general", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{",", query.TYPE_COMMA},
		{"[", query.TYPE_LB},
		{"1", query.TYPE_VALUE},
		{",", query.TYPE_COMMA},
		{"2", query.TYPE_VALUE},
		{",", query.TYPE_COMMA},
		{"3", query.TYPE_VALUE},
		{"]", query.TYPE_RB},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// Ensure that a plugin function can be lexed.
func TestLexer_Lex_Plugin(t *testing.T) {
	if tokens, err := query.Lex("plugin(get(10, general), [get(11, general)])"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(tokens, []query.Token{
		{"plugin", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"10", query.TYPE_VALUE},
		{",", query.TYPE_COMMA},
		{"general", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{",", query.TYPE_COMMA},
		{"[", query.TYPE_LB},
		{"get", query.TYPE_FUNC},
		{"(", query.TYPE_LP},
		{"11", query.TYPE_VALUE},
		{",", query.TYPE_COMMA},
		{"general", query.TYPE_VALUE},
		{")", query.TYPE_RP},
		{"]", query.TYPE_RB},
		{")", query.TYPE_RP},
	}) {
		t.Fatalf("unexpected tokens:\n\n%s", spew.Sprint(tokens))
	}
}

// MustLex lexes s and returns a set of tokens. Panic on error.
func MustLex(s string) []query.Token {
	a, err := query.Lex(s)
	if err != nil {
		panic(err)
	}
	return a
}
