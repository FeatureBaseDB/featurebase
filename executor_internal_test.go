package pilosa

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/pilosa/pilosa/pql"
)

func TestExecutor_TranslateGroupByCall(t *testing.T) {
	e := &executor{
		Holder: NewHolder(),
	}
	e.Holder.Path, _ = ioutil.TempDir("", "")
	err := e.Holder.Open()
	if err != nil {
		t.Fatalf("opening holder: %v", err)
	}

	e.TranslateStore = e.Holder.translateFile
	tf, _ := ioutil.TempFile("", "")
	e.Holder.translateFile.Path = tf.Name()
	err = e.Holder.translateFile.Open()
	if err != nil {
		t.Fatalf("opening translateFile: %v", err)
	}

	idx, err := e.Holder.CreateIndex("i", IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}

	_, erra := idx.CreateField("ak", OptFieldKeys())
	_, errb := idx.CreateField("b")
	_, errc := idx.CreateField("ck", OptFieldKeys())
	if erra != nil || errb != nil || errc != nil {
		t.Fatalf("creating fields %v, %v, %v", erra, errb, errc)
	}

	_, erra = e.TranslateStore.TranslateRowsToUint64("i", "ak", []string{"la"})
	_, errb = e.TranslateStore.TranslateRowsToUint64("i", "ck", []string{"ha"})
	if erra != nil || errb != nil {
		t.Fatalf("translating rows %v, %v", erra, errb)
	}

	query, err := pql.ParseString(`GroupBy(Rows(field=ak), Rows(field=b), Rows(field=ck), previous=["la", 0, "ha"])`)
	if err != nil {
		t.Fatalf("parsing query: %v", err)
	}
	c := query.Calls[0]
	err = e.translateGroupByCall("i", idx, c)
	if err != nil {
		t.Fatalf("translating call: %v", err)
	}
	if len(c.Args["previous"].([]interface{})) != 3 {
		t.Fatalf("unexpected length for 'previous' arg %v", c.Args["previous"])
	}
	for i, v := range c.Args["previous"].([]interface{}) {
		if !isInt(v) {
			t.Fatalf("expected all items in previous to be ints, but '%v' at index %d is %[1]T", v, i)
		}
	}

	errTests := []struct {
		pql string
		err string
	}{
		{
			pql: `GroupBy(Rows(field=notfound), previous=1)`,
			err: "'previous' argument must be list",
		},
		{
			pql: `GroupBy(Rows(field=ak), previous=["la", 0])`,
			err: "mismatched lengths",
		},
		{
			pql: `GroupBy(Rows(field=ak), previous=[1])`,
			err: "prev value must be a string",
		},
		{
			pql: `GroupBy(Rows(field=notfound), previous=[1])`,
			err: ErrFieldNotFound.Error(),
		},
		// TODO: an unknown key will actually allocate an id. this is probably bad.
		// {
		// 	pql: `GroupBy(Rows(field=ak), previous=["zoop"])`,
		// 	err: "translating row key '",
		// },
		{
			pql: `GroupBy(Rows(field=b), previous=["la"])`,
			err: "which doesn't use string keys",
		},
	}

	for i, test := range errTests {
		t.Run(fmt.Sprintf("#%d_%s", i, test.err), func(t *testing.T) {
			query, err := pql.ParseString(test.pql)
			if err != nil {
				t.Fatalf("parsing query: %v", err)
			}
			c := query.Calls[0]
			err = e.translateGroupByCall("i", idx, c)
			if err == nil {
				t.Fatalf("expected error, but translated call is '%s", c)
			}
			if !strings.Contains(err.Error(), test.err) {
				t.Fatalf("expected '%s', got '%v'", test.err, err)
			}
		})
	}
}

func isInt(a interface{}) bool {
	switch a.(type) {
	case int, int64, uint, uint64:
		return true
	default:
		return false
	}
}

func TestFilterWithLimit(t *testing.T) {
	f := filterWithLimit(5)

	for i := uint64(0); i < 5; i++ {
		include, done := f(i, i*(1<<shardVsContainerExponent), nil)
		if done {
			t.Fatalf("limit filter ended early on iteration %d", i)
		}
		if !include {
			t.Fatalf("limit filter should always include until done")
		}
	}
	inc, done := f(5, 5*(1<<shardVsContainerExponent)+1, nil)
	if !done {
		t.Fatalf("limit filter should have been done, but got inc: %v done: %v", inc, done)
	}
}

func TestFilterWithRows(t *testing.T) {
	tests := []struct {
		rows     []uint64
		callWith []uint64
		expect   [][2]bool
	}{
		{
			rows:     []uint64{},
			callWith: []uint64{0},
			expect:   [][2]bool{{false, true}},
		},
		{
			rows:     []uint64{0},
			callWith: []uint64{0},
			expect:   [][2]bool{{true, true}},
		},
		{
			rows:     []uint64{1},
			callWith: []uint64{0, 2},
			expect:   [][2]bool{{false, false}, {false, true}},
		},
		{
			rows:     []uint64{0},
			callWith: []uint64{1, 2},
			expect:   [][2]bool{{false, true}, {false, true}},
		},
		{
			rows:     []uint64{3, 9},
			callWith: []uint64{1, 2, 3, 10},
			expect:   [][2]bool{{false, false}, {false, false}, {true, false}, {false, true}},
		},
		{
			rows:     []uint64{0, 1, 2},
			callWith: []uint64{0, 1, 2},
			expect:   [][2]bool{{true, false}, {true, false}, {true, true}},
		},
	}

	for num, test := range tests {
		t.Run(fmt.Sprintf("%d_%v_with_%v", num, test.rows, test.callWith), func(t *testing.T) {
			if len(test.callWith) != len(test.expect) {
				t.Fatalf("Badly specified test - must expect the same number of values as calls.")
			}
			f := filterWithRows(test.rows)
			for i, id := range test.callWith {
				inc, done := f(id, 0, nil)
				if inc != test.expect[i][0] || done != test.expect[i][1] {
					t.Fatalf("Calling with %d\nexp: %v,%v\ngot: %v,%v", id, test.expect[i][0], test.expect[i][1], inc, done)
				}
			}
		})
	}

}

func TestFieldRowMarshalJSON(t *testing.T) {
	fr := FieldRow{
		Field:  "blah",
		RowID:  0,
		RowKey: "ha",
	}
	b, err := json.Marshal(fr)
	if err != nil {
		t.Fatalf("marshalling fieldrow: %v", err)
	}
	if string(b) != `{"field":"blah","rowKey":"ha"}` {
		t.Fatalf("unexpected json: %s", b)
	}

	fr = FieldRow{
		Field:  "blah",
		RowID:  2,
		RowKey: "",
	}
	b, err = json.Marshal(fr)
	if err != nil {
		t.Fatalf("marshalling fieldrow: %v", err)
	}
	if string(b) != `{"field":"blah","rowID":2}` {
		t.Fatalf("unexpected json: %s", b)
	}
}
