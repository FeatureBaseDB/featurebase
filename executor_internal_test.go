package pilosa

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/testhook"
)

func TestExecutor_TranslateRowsOnBool(t *testing.T) {
	path, _ := testhook.TempDirInDir(t, *TempDir, "pilosa-executor-")
	holder := NewHolder(path, mustHolderConfig())
	defer holder.Close()

	e := &executor{
		Holder:  holder,
		Cluster: NewTestCluster(t, 1),
	}
	if err := e.Holder.Open(); err != nil {
		t.Fatalf("opening holder: %v", err)
	}

	idx, err := e.Holder.CreateIndex("i", IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}

	shard := uint64(0)
	tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard})
	defer tx.Rollback()

	fb, errb := idx.CreateField("b", OptFieldTypeBool())
	_, errbk := idx.CreateField("bk", OptFieldTypeBool(), OptFieldKeys())
	if errb != nil || errbk != nil {
		t.Fatalf("creating fields %v, %v", errb, errbk)
	}

	_, err1 := fb.SetBit(tx, 1, 1, nil)
	_, err2 := fb.SetBit(tx, 2, 2, nil)
	_, err3 := fb.SetBit(tx, 3, 3, nil)
	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("setting bit %v, %v, %v", err1, err2, err3)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		pql string
	}{
		{pql: "Rows(b)"},
		{pql: "GroupBy(Rows(b))"},
		{pql: "Set(4, b=true)"},
	}

	for _, test := range tests {
		t.Run(test.pql, func(t *testing.T) {
			query, err := pql.ParseString(test.pql)
			if err != nil {
				t.Fatalf("parsing query: %v", err)
			}

			c := query.Calls[0]
			colTranslations, rowTranslations, err := e.preTranslate(context.Background(), "i", c)
			if err != nil {
				t.Fatalf("pre-translating call: %v", err)
			}
			_, err = e.translateCall(c, "i", colTranslations, rowTranslations)
			if err != nil {
				t.Fatalf("translating call: %v", err)
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

func TestExecutor_GroupCountCondition(t *testing.T) {
	t.Run("satisfiesCondition", func(t *testing.T) {
		type condCheck struct {
			cond string
			exp  bool
		}
		tests := []struct {
			groupCount GroupCount
			checks     []condCheck
		}{
			{
				groupCount: GroupCount{Count: 100},
				checks: []condCheck{
					{cond: "count == 99", exp: false},
					{cond: "count != 99", exp: true},
					{cond: "count < 99", exp: false},
					{cond: "count <= 99", exp: false},
					{cond: "count > 99", exp: true},
					{cond: "count >= 99", exp: true},

					{cond: "count == 100", exp: true},
					{cond: "count != 100", exp: false},
					{cond: "count < 100", exp: false},
					{cond: "count <= 100", exp: true},
					{cond: "count > 100", exp: false},
					{cond: "count >= 100", exp: true},

					{cond: "count == 101", exp: false},
					{cond: "count != 101", exp: true},
					{cond: "count < 101", exp: true},
					{cond: "count <= 101", exp: true},
					{cond: "count > 101", exp: false},
					{cond: "count >= 101", exp: false},

					{cond: "98 < count < 100", exp: false},
					{cond: "98 < count <= 100", exp: true},
					{cond: "98 < count < 101", exp: true},
					{cond: "100 <= count < 102", exp: true},
					{cond: "100 < count < 102", exp: false},
					{cond: "98 <= count <= 102", exp: true},
				},
			},
			{
				groupCount: GroupCount{Agg: 100},
				checks: []condCheck{
					{cond: "sum == 99", exp: false},
					{cond: "sum != 99", exp: true},
					{cond: "sum < 99", exp: false},
					{cond: "sum <= 99", exp: false},
					{cond: "sum > 99", exp: true},
					{cond: "sum >= 99", exp: true},

					{cond: "sum == 100", exp: true},
					{cond: "sum != 100", exp: false},
					{cond: "sum < 100", exp: false},
					{cond: "sum <= 100", exp: true},
					{cond: "sum > 100", exp: false},
					{cond: "sum >= 100", exp: true},

					{cond: "sum == 101", exp: false},
					{cond: "sum != 101", exp: true},
					{cond: "sum < 101", exp: true},
					{cond: "sum <= 101", exp: true},
					{cond: "sum > 101", exp: false},
					{cond: "sum >= 101", exp: false},

					{cond: "98 < sum < 100", exp: false},
					{cond: "98 < sum <= 100", exp: true},
					{cond: "98 < sum < 101", exp: true},
					{cond: "100 <= sum < 102", exp: true},
					{cond: "100 < sum < 102", exp: false},
					{cond: "98 <= sum <= 102", exp: true},
				},
			},
			{
				groupCount: GroupCount{Agg: -100},
				checks: []condCheck{
					{cond: "sum == -99", exp: false},
					{cond: "sum != -99", exp: true},
					{cond: "sum < -99", exp: true},
					{cond: "sum <= -99", exp: true},
					{cond: "sum > -99", exp: false},
					{cond: "sum >= -99", exp: false},

					{cond: "sum == -100", exp: true},
					{cond: "sum != -100", exp: false},
					{cond: "sum < -100", exp: false},
					{cond: "sum <= -100", exp: true},
					{cond: "sum > -100", exp: false},
					{cond: "sum >= -100", exp: true},

					{cond: "sum == -101", exp: false},
					{cond: "sum != -101", exp: true},
					{cond: "sum < -101", exp: false},
					{cond: "sum <= -101", exp: false},
					{cond: "sum > -101", exp: true},
					{cond: "sum >= -101", exp: true},

					{cond: "-100 < sum < -98", exp: false},
					{cond: "-100 <= sum < -98", exp: true},
					{cond: "-101 < sum < -98", exp: true},
					{cond: "-102 < sum <= -100", exp: true},
					{cond: "-102 < sum < -100", exp: false},
					{cond: "-102 <= sum <= -98", exp: true},
				},
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test (#%d):", i), func(t *testing.T) {
				for j, check := range test.checks {
					t.Run(fmt.Sprintf("check (#%d):", j), func(t *testing.T) {

						query, err := pql.ParseString(fmt.Sprintf("GroupBy(Rows(a), having=Condition(%s))", check.cond))
						if err != nil {
							t.Fatalf("parsing query: %v", err)
						}
						c := query.Calls[0]
						having := c.Args["having"].(*pql.Call)

						var got bool
						for subj, cond := range having.Args {
							switch subj {
							case "count", "sum":
								condition, ok := cond.(*pql.Condition)
								if !ok {
									t.Fatalf("not a valid condition")
								}
								got = test.groupCount.satisfiesCondition(subj, condition)
							}
						}

						if got != check.exp {
							t.Fatalf("expected: %v, but got: %v", check.exp, got)
						}
					})
				}
			})
		}
	})
}

func TestValCountComparisons(t *testing.T) {
	tests := []struct {
		name       string
		vc         ValCount
		other      ValCount
		expLarger  ValCount
		expSmaller ValCount
	}{
		{
			name: "zero",
		},
		{
			name:       "ints",
			vc:         ValCount{Val: 10, Count: 1},
			other:      ValCount{Val: 3, Count: 2},
			expLarger:  ValCount{Val: 10, Count: 1},
			expSmaller: ValCount{Val: 3, Count: 2},
		},
		{
			name:       "floats",
			vc:         ValCount{FloatVal: 10.2, Count: 1},
			other:      ValCount{FloatVal: 3.4, Count: 2},
			expLarger:  ValCount{FloatVal: 10.2, Count: 1},
			expSmaller: ValCount{FloatVal: 3.4, Count: 2},
		},
		{
			name:       "intsEquality",
			vc:         ValCount{Val: 10, Count: 1},
			other:      ValCount{Val: 10, Count: 2},
			expLarger:  ValCount{Val: 10, Count: 3},
			expSmaller: ValCount{Val: 10, Count: 3},
		},
		{
			name:       "floatsEquality",
			vc:         ValCount{FloatVal: 10.7, Count: 1},
			other:      ValCount{FloatVal: 10.7, Count: 2},
			expLarger:  ValCount{FloatVal: 10.7, Count: 3},
			expSmaller: ValCount{FloatVal: 10.7, Count: 3},
		},
		{
			name:       "timestampEquality",
			vc:         ValCount{Val: -17782800, TimestampVal: time.Unix(0, -17782800*int64(time.Second)), Count: 1},
			other:      ValCount{Val: -17782800, TimestampVal: time.Unix(0, -17782800*int64(time.Second)), Count: 1},
			expLarger:  ValCount{Val: -17782800, TimestampVal: time.Unix(0, -17782800*int64(time.Second)), Count: 2},
			expSmaller: ValCount{Val: -17782800, TimestampVal: time.Unix(0, -17782800*int64(time.Second)), Count: 2},
		},
		{
			name:       "timestamp",
			vc:         ValCount{Val: -17782800, TimestampVal: time.Unix(0, -17782800*int64(time.Second)), Count: 1},
			other:      ValCount{Val: 1587399600, TimestampVal: time.Unix(0, 1587399600*int64(time.Second)), Count: 1},
			expLarger:  ValCount{Val: 1587399600, TimestampVal: time.Unix(0, 1587399600*int64(time.Second)), Count: 1},
			expSmaller: ValCount{Val: -17782800, TimestampVal: time.Unix(0, -17782800*int64(time.Second)), Count: 1},
		},
	}

	for i, test := range tests {
		t.Run(test.name+strconv.Itoa(i), func(t *testing.T) {
			gotLarger := test.vc.larger(test.other)
			if gotLarger != test.expLarger {
				t.Fatalf("larger failed, expected:\n%+v\ngot:\n%+v", test.expLarger, gotLarger)
			}

			gotSmaller := test.vc.smaller(test.other)
			if gotSmaller != test.expSmaller {
				t.Fatalf("smaller failed, expected:\n%+v\ngot:\n%+v", test.expSmaller, gotSmaller)
			}
		})
	}
}

func TestToNegInt64(t *testing.T) {
	tests := []struct {
		u64      uint64
		i64      int64
		overflow bool
	}{
		{
			u64: uint64(1 << 63),
			i64: int64(-1 << 63),
		},
		{
			u64: uint64(1<<63) - 1,
			i64: int64(-1<<63) + 1,
		},
		{
			u64:      uint64(1<<63) + 1,
			overflow: true,
		},
	}

	for _, tc := range tests {
		val, err := toNegInt64(tc.u64)
		if err != nil && !tc.overflow {
			t.Fatalf("error: %+v, expected: %+v", err, tc)
		}

		if val != tc.i64 {
			t.Fatalf("Expected: %+v, Got: %+v", tc.i64, val)
		}
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		u64      uint64
		i64      int64
		overflow bool
	}{
		{
			u64: uint64(1<<63) - 1,
			i64: 1<<63 - 1,
		},
		{
			u64: uint64(0),
			i64: 0,
		},
		{
			u64:      uint64(1 << 63),
			overflow: true,
		},
		{
			u64:      1<<64 - 1,
			overflow: true,
		},
	}

	for _, tc := range tests {
		val, err := toInt64(tc.u64)
		if err != nil && !tc.overflow {
			t.Fatalf("error: %+v, expected: %+v", err, tc)
		}

		if val != tc.i64 {
			t.Fatalf("Expected: %+v, Got: %+v", tc.i64, val)
		}
	}
}

func TestGetSorter(t *testing.T) {
	tests := []struct {
		sortSpec string
		expGCS   *groupCountSorter
		expErr   string
	}{
		{
			sortSpec: "count asc",
			expGCS:   &groupCountSorter{fields: []int{-1}, order: []order{asc}},
		},
		{
			sortSpec: "count    asc",
			expGCS:   &groupCountSorter{fields: []int{-1}, order: []order{asc}},
		},
		{
			sortSpec: "  count asc",
			expGCS:   &groupCountSorter{fields: []int{-1}, order: []order{asc}},
		},
		{
			sortSpec: "  count asc    ",
			expGCS:   &groupCountSorter{fields: []int{-1}, order: []order{asc}},
		},
		{
			sortSpec: "count",
			expGCS:   &groupCountSorter{fields: []int{-1}, order: []order{desc}},
		},
		{
			sortSpec: "sum asc",
			expGCS:   &groupCountSorter{fields: []int{-2}, order: []order{asc}},
		},
		{
			sortSpec: "aggregate asc",
			expGCS:   &groupCountSorter{fields: []int{-2}, order: []order{asc}},
		},
		{
			sortSpec: "boondoggle asc",
			expErr:   "sorting is only supported on count, aggregate, or sum, not 'boondoggle'",
		},
		{
			sortSpec: "sum asc, count desc",
			expGCS:   &groupCountSorter{fields: []int{-2, -1}, order: []order{asc, desc}},
		},
		{
			sortSpec: "count asc, sum desc",
			expGCS:   &groupCountSorter{fields: []int{-1, -2}, order: []order{asc, desc}},
		},
		{
			sortSpec: " count  asc ,  sum  desc ",
			expGCS:   &groupCountSorter{fields: []int{-1, -2}, order: []order{asc, desc}},
		},
		{
			sortSpec: " count  asc ,  sum  desc blah",
			expErr:   "parsing sort directive: 'sum  desc blah': too many elements",
		},
		{
			sortSpec: "count asc, sum fesc",
			expErr:   "unknown sort direction 'fesc'",
		},
		{
			sortSpec: " , sum fesc",
			expErr:   "invalid sorting directive: ''",
		},
		{
			// weird and useless, but I guess fine?
			sortSpec: "count  asc,count asc ",
			expGCS:   &groupCountSorter{fields: []int{-1, -1}, order: []order{asc, asc}},
		},
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("%s_%d", tst.sortSpec, i), func(t *testing.T) {
			gcs, err := getSorter(tst.sortSpec)
			if err != nil {
				if tst.expErr == "" {
					t.Errorf("unexpected error: %v", err)
					return
				}
				if tst.expErr != err.Error() {
					t.Errorf("mismatched errors got: '%v', exp: '%s'", err, tst.expErr)
				}
				return
			}
			if !reflect.DeepEqual(gcs, tst.expGCS) {
				t.Errorf("exp:\n%+v\ngot:\n%v\n", tst.expGCS, gcs)
			}

		})
	}
}

func TestExecutorSafeCopyDistinctTimestamp(t *testing.T) {
	result := DistinctTimestamp{Values: []string{"test", "test"}, Name: "test"}
	results := make([]interface{}, 1)
	results[0] = result

	response := QueryResponse{Results: results, Err: nil, Profile: nil}
	copied := safeCopy(response)
	if !reflect.DeepEqual(copied.Results, response.Results) {
		t.Fatalf("Did not copy results. got %+v, want %+v", copied.Results, response.Results)
	}
}
