package query_test

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa/query"
)

func TestTokensToFilterStrings1(t *testing.T) {
	filter, filters := query.TokensToFilterStrings(MustLex("plugin(get(88, general), [get(12, general), get(13, general)])"))
	if filter != "get(88,general)" {
		t.Fatalf("unexpected filter: %s", filter)
	} else if !reflect.DeepEqual(filters, []string{"get(12,general)", "get(13,general)"}) {
		t.Fatalf("unexpected filters: %s", spew.Sprint(filters))
	}
}

func TestTokensToFilterStrings2(t *testing.T) {
	filter, filters := query.TokensToFilterStrings(MustLex("plugin(intersect(get(88, general, [0]), get(77, b.n)), [get(12, general), get(13, general)])"))
	if filter != "intersect(get(88,general,[0]),get(77,b.n))" {
		t.Fatalf("unexpected filter: %s", filter)
	} else if !reflect.DeepEqual(filters, []string{"get(12,general)", "get(13,general)"}) {
		t.Fatalf("unexpected filters: %s", spew.Sprint(filters))
	}
}

func TestTokensToFilterStrings3(t *testing.T) {
	filter, filters := query.TokensToFilterStrings(MustLex("plugin(intersect(get(88, general, [0]), get(77, b.n)))"))
	if filter != "intersect(get(88,general,[0]),get(77,b.n))" {
		t.Fatalf("unexpected filter: %s", filter)
	} else if !reflect.DeepEqual(filters, []string(nil)) {
		t.Fatalf("unexpected filters: %s", spew.Sprint(filters))
	}
}
