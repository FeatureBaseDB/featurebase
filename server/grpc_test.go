// Copyright 2021 Molecula Corp. All rights reserved.
package server_test

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/pql"
	pb "github.com/molecula/featurebase/v2/proto"
	"github.com/molecula/featurebase/v2/server"
	"github.com/molecula/featurebase/v2/sql"
	"github.com/molecula/featurebase/v2/test"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestGRPC(t *testing.T) {
	t.Run("ToTable", func(t *testing.T) {
		type expHeader struct {
			name     string
			dataType string
		}

		type expColumn interface{}

		va, vb := int64(-11), int64(-12)
		tests := []struct {
			result     interface{}
			expHeaders []expHeader
			expColumns [][]expColumn
		}{
			// Row (uint64)
			{
				pilosa.NewRow(10, 11, 12),
				[]expHeader{
					{"_id", "uint64"},
				},
				[][]expColumn{
					{uint64(10)},
					{uint64(11)},
					{uint64(12)},
				},
			},
			// Row (string)
			{
				&pilosa.Row{Keys: []string{"ten", "eleven", "twelve"}},
				[]expHeader{
					{"_id", "string"},
				},
				[][]expColumn{
					{"ten"},
					{"eleven"},
					{"twelve"},
				},
			},
			// PairField (uint64)
			{
				pilosa.PairField{
					Pair:  pilosa.Pair{ID: 10, Count: 123},
					Field: "fld",
				},
				[]expHeader{
					{"fld", "uint64"},
					{"count", "uint64"},
				},
				[][]expColumn{
					{uint64(10), uint64(123)},
				},
			},
			// Pair (string)
			{
				pilosa.PairField{
					Pair:  pilosa.Pair{Key: "ten", Count: 123},
					Field: "fld",
				},
				[]expHeader{
					{"fld", "string"},
					{"count", "uint64"},
				},
				[][]expColumn{
					{string("ten"), uint64(123)},
				},
			},
			// *PairsField (uint64)
			{
				&pilosa.PairsField{
					Pairs: []pilosa.Pair{
						{ID: 10, Count: 123},
						{ID: 11, Count: 456},
					},
					Field: "fld",
				},
				[]expHeader{
					{"fld", "uint64"},
					{"count", "uint64"},
				},
				[][]expColumn{
					{uint64(10), uint64(123)},
					{uint64(11), uint64(456)},
				},
			},
			// *PairsField (string)
			{
				&pilosa.PairsField{
					Pairs: []pilosa.Pair{
						{Key: "ten", Count: 123},
						{Key: "eleven", Count: 456},
					},
					Field: "fld",
				},
				[]expHeader{
					{"fld", "string"},
					{"count", "uint64"},
				},
				[][]expColumn{
					{"ten", uint64(123)},
					{"eleven", uint64(456)},
				},
			},
			// []GroupCount (uint64)
			{
				pilosa.NewGroupCounts("", []pilosa.GroupCount{
					{
						Group: []pilosa.FieldRow{
							{Field: "a", RowID: 10},
							{Field: "b", RowID: 11},
						},
						Count: 123,
					},
					{
						Group: []pilosa.FieldRow{
							{Field: "a", RowID: 10},
							{Field: "b", RowID: 12},
						},
						Count: 456,
					},
					{
						Group: []pilosa.FieldRow{
							{Field: "va", Value: &va},
							{Field: "vb", Value: &vb},
						},
						Count: 789,
					},
				}...),
				[]expHeader{
					{"a", "uint64"},
					{"b", "uint64"},
					{"count", "uint64"},
				},
				[][]expColumn{
					{uint64(10), uint64(11), uint64(123)},
					{uint64(10), uint64(12), uint64(456)},
					{int64(va), int64(vb), uint64(789)},
				},
			},
			// []GroupCount (string) + sum
			{
				pilosa.NewGroupCounts("sum", []pilosa.GroupCount{
					{
						Group: []pilosa.FieldRow{
							{Field: "a", RowKey: "ten"},
							{Field: "b", RowKey: "eleven"},
						},
						Count: 123,
					},
					{
						Group: []pilosa.FieldRow{
							{Field: "a", RowKey: "ten"},
							{Field: "b", RowKey: "twelve"},
						},
						Count: 456,
					},
				}...),
				[]expHeader{
					{"a", "string"},
					{"b", "string"},
					{"count", "uint64"},
					{"sum", "int64"},
				},
				[][]expColumn{
					{"ten", "eleven", uint64(123), int64(0)},
					{"ten", "twelve", uint64(456), int64(0)},
				},
			},
			// RowIdentifiers (uint64)
			{
				pilosa.RowIdentifiers{
					Rows: []uint64{10, 11, 12},
				},
				[]expHeader{
					{"", "uint64"}, // This is blank because we don't expose RowIdentifiers.field, so we have no way to set it for tests.
				},
				[][]expColumn{
					{uint64(10)},
					{uint64(11)},
					{uint64(12)},
				},
			},
			// RowIdentifiers (string)
			{
				pilosa.RowIdentifiers{
					Keys: []string{"ten", "eleven", "twelve"},
				},
				[]expHeader{
					{"", "string"}, // This is blank because we don't expose RowIdentifiers.field, so we have no way to set it for tests.
				},
				[][]expColumn{
					{"ten"},
					{"eleven"},
					{"twelve"},
				},
			},
			// uint64
			{
				uint64(123),
				[]expHeader{
					{"count", "uint64"},
				},
				[][]expColumn{
					{uint64(123)},
				},
			},
			// bool
			{
				true,
				[]expHeader{
					{"result", "bool"},
				},
				[][]expColumn{
					{true},
				},
			},
			// ValCount
			{
				pilosa.ValCount{Val: 1, Count: 1},
				[]expHeader{{"value", "int64"}, {"count", "int64"}},
				[][]expColumn{{int64(1), int64(1)}},
			},
			{
				pilosa.ValCount{FloatVal: 1.24, Count: 1},
				[]expHeader{{"value", "float64"}, {"count", "int64"}},
				[][]expColumn{{float64(1.24), int64(1)}},
			},
			// SignedRow
			{
				pilosa.SignedRow{
					Neg: pilosa.NewRow(13, 14, 15),
					Pos: pilosa.NewRow(10, 11, 12),
				},
				[]expHeader{
					{"", "int64"},
				},
				[][]expColumn{
					{int64(-15)},
					{int64(-14)},
					{int64(-13)},
					{int64(10)},
					{int64(11)},
					{int64(12)},
				},
			},
		}

		for ti, test := range tests {
			toTabler, err := server.ToTablerWrapper(test.result)
			if err != nil {
				t.Fatal(err)
			}
			table, err := toTabler.ToTable()
			if err != nil {
				t.Fatal(err)
			}

			// Ensure headers match.
			headers := table.GetHeaders()
			if len(headers) < len(test.expHeaders) {
				t.Fatalf("test %d expected %d headers, got %d, first missing header %q",
					ti, len(test.expHeaders), len(headers), test.expHeaders[len(headers)].name)
			}
			for i, header := range headers {
				if header.Name != test.expHeaders[i].name {
					t.Fatalf("test %d expected header name: %s, but got: %s", ti, test.expHeaders[i].name, header.Name)
				}
				if header.Datatype != test.expHeaders[i].dataType {
					t.Fatalf("test %d expected header data type: %s, but got: %s", ti, test.expHeaders[i].dataType, header.Datatype)
				}
			}

			// Ensure column data matches.
			for i, row := range table.GetRows() {
				columns := row.GetColumns()
				if len(columns) != len(test.expColumns[i]) {
					t.Fatalf("test %d expected %d columns, got %d in row %d",
						ti, len(test.expColumns[i]), len(columns), i)
				}
				for j, column := range columns {
					switch v := test.expColumns[i][j].(type) {
					case string:
						val := column.GetStringVal()
						if val != v {
							t.Fatalf("test %d expected column val: %v, but got: %v", ti, v, val)
						}
					case uint64:
						val := column.GetUint64Val()
						if val != v {
							t.Fatalf("test %d expected column val: %v, but got: %v", ti, v, val)
						}
					case bool:
						val := column.GetBoolVal()
						if val != v {
							t.Fatalf("test %d expected column val: %v, but got: %v", ti, v, val)
						}
					case int64:
						val := column.GetInt64Val()
						if val != v {
							t.Fatalf("test %d expected column val: %v but got: %v", ti, v, val)
						}
					case float64:
						val := column.GetFloat64Val()
						if val != v {
							t.Fatalf("test %d expected column val: %v but got: %v", ti, v, val)
						}
					default:
						t.Fatalf("test %d has unhandled data type: %T", ti, v)
					}
				}
			}
		}
	})
}

func TestQueryPQLUnary(t *testing.T) {
	m := test.RunCommand(t)
	defer m.Close()

	i := m.MustCreateIndex(t, "i", pilosa.IndexOptions{})
	m.MustCreateField(t, i.Name(), "f", pilosa.OptFieldKeys())
	gh := server.NewGRPCHandler(m.API)

	stream := &MockServerTransportStream{}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)

	resp, err := gh.QueryPQLUnary(ctx, &pb.QueryPQLRequest{
		Index: i.Name(),
		Pql:   `Set(0, f="zero")`,
	})
	if err != nil {
		// Unary query should work
		t.Fatal(err)
	}

	if resp.Duration == 0 {
		t.Fatal("duration not recorded")
	}
	duration, err := stream.GetDuration()
	if duration == 0 || err != nil {
		t.Fatal("duration header not recorded")
	}

	_, err = gh.QueryPQLUnary(ctx, &pb.QueryPQLRequest{
		Index: i.Name(),
		Pql:   `Set(1, f="one") Set(2, f="two")`,
	})
	staterr := status.Convert(err)
	if staterr == nil || staterr.Code() != codes.InvalidArgument {
		// QueryPQLUnary handles exactly one query
		t.Fatalf("expected error: InvalidArgument, got: %v", err)
	}
}

func TestQueryPQL(t *testing.T) {
	// TODO: Replace TestQueryPQL and TestQueryPQLUnary with table-driven test, like TestQuerySQL
	m := test.RunCommand(t)
	defer m.Close()

	i := m.MustCreateIndex(t, "i", pilosa.IndexOptions{Keys: false, TrackExistence: true})
	m.MustCreateField(t, i.Name(), "f", pilosa.OptFieldKeys())
	gh := server.NewGRPCHandler(m.API)

	mock := &mockPilosa_QuerySQLServer{}

	err := gh.QueryPQL(&pb.QueryPQLRequest{
		Index: i.Name(),
		Pql:   `Set(0, f="zero")`,
	}, mock)
	if err != nil {
		t.Fatal(err)
	}

	duration, err := mock.GetDuration()
	if duration == 0 || err != nil {
		t.Fatal("duration header not recorded")
	}

	if len(mock.Results) != 1 {
		t.Fatal("expecting one result")
	}

	if len(mock.Results[0].Headers) != 1 {
		t.Fatal("expecting one header")
	}

	if len(mock.Results[0].Columns) != 1 {
		t.Fatal("expecting one column")
	}

	if mock.Results[0].Duration == 0 {
		t.Fatal("expecting non-zero duration")
	}

	// Set second value so that All() returns more than one result
	err = gh.QueryPQL(&pb.QueryPQLRequest{
		Index: i.Name(),
		Pql:   `Set(1, f="zero")`,
	}, mock)
	if err != nil {
		t.Fatal(err)
	}

	mock.clearResults()

	err = gh.QueryPQL(&pb.QueryPQLRequest{
		Index: i.Name(),
		Pql:   `All()`,
	}, mock)
	if err != nil {
		t.Fatal(err)
	}

	if len(mock.Results) != 2 {
		t.Fatal("expecting two results")
	}

	if len(mock.Results[0].Headers) != 1 {
		t.Fatal("expecting one header")
	}

	if len(mock.Results[0].Columns) != 1 {
		t.Fatal("expecting one column")
	}

	if len(mock.Results[1].Headers) != 0 {
		t.Fatal("expecting no headers on second result")
	}

	if len(mock.Results[1].Columns) != 1 {
		t.Fatal("expecting one column on second result")
	}

	if mock.Results[0].Duration == 0 {
		t.Fatal("expecting non-zero duration")
	}

	if mock.Results[1].Duration != 0 {
		t.Fatal("expecting zero duration on second result")
	}
}

type (
	tableResponse struct {
		headers []columnInfo
		rows    []row
	}
	columnInfo struct {
		name     string
		datatype string
	}
	row struct {
		columns []columnResponse
	}
	columnResponse interface{}
)

func TestQuerySQL(t *testing.T) {

	stream := &MockServerTransportStream{}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)

	gh, tearDownFunc := setUpTestQuerySQLUnary(ctx, t)
	defer tearDownFunc()

	tests := []struct {
		sql string
		exp tableResponse
		eq  func(tableResponse, tableResponse) error
	}{
		{
			// Extract(Limit(All(), limit=100, offset=0),Rows(age))
			sql: "select age from grouper",
			exp: tableResponse{
				headers: []columnInfo{
					{"age", "int64"},
				},
				rows: []row{
					{[]columnResponse{int64(27)}},
					{[]columnResponse{int64(16)}},
					{[]columnResponse{int64(19)}},
					{[]columnResponse{int64(27)}},
					{[]columnResponse{int64(16)}},
					{[]columnResponse{int64(34)}},
					{[]columnResponse{int64(27)}},
					{[]columnResponse{int64(16)}},
					{[]columnResponse{int64(16)}},
					{[]columnResponse{int64(31)}},
				},
			},
			eq: equal,
		},
		{
			// Extract(Limit(ConstRow(columns=[2]), limit=100, offset=0),Rows(age),Rows(color),Rows(height),Rows(score))
			sql: "select * from grouper where _id=2",
			exp: tableResponse{
				headers: []columnInfo{
					{"_id", "uint64"},
					{"age", "int64"},
					{"color", "[]string"},
					{"height", "int64"},
					{"score", "int64"},
				},
				rows: []row{
					{[]columnResponse{uint64(2), int64(16), []string{"blue"}, int64(30), int64(-8)}},
				},
			},
			eq: equal,
		},
		{
			// Extract(Limit(ConstRow(columns=[2]), limit=100, offset=0),Rows(age),Rows(color),Rows(height),Rows(score))
			sql: "select * from grouper",
			exp: tableResponse{
				headers: []columnInfo{
					{"_id", "uint64"},
					{"age", "int64"},
					{"color", "[]string"},
					{"height", "int64"},
					{"score", "int64"},
				},
				rows: []row{
					{[]columnResponse{uint64(1), int64(27), []string{"blue"}, int64(20), int64(-10)}},
					{[]columnResponse{uint64(2), int64(16), []string{"blue"}, int64(30), int64(-8)}},
					{[]columnResponse{uint64(3), int64(19), []string{"red"}, int64(40), int64(6)}},
					{[]columnResponse{uint64(4), int64(27), []string{"green"}, int64(50), int64(0)}},
					{[]columnResponse{uint64(5), int64(16), []string{"blue"}, int64(60), int64(-2)}},
					{[]columnResponse{uint64(6), int64(34), []string{"blue"}, int64(70), int64(100)}},
					{[]columnResponse{uint64(7), int64(27), []string{"blue"}, int64(80), int64(0)}},
					{[]columnResponse{uint64(8), int64(16), []string{}, int64(90), int64(-13)}},
					{[]columnResponse{uint64(9), int64(16), []string{"red"}, int64(100), int64(80)}},
					{[]columnResponse{uint64(10), int64(31), []string{"red"}, int64(110), int64(-2)}},
				},
			},
			eq: equal,
		},
		// join
		{
			// Count(Intersect(All(),Distinct(Row(grouperid!=null),index='joiner',field='grouperid')))
			sql: "select count(*) from grouper g INNER JOIN joiner j ON g._id = j.grouperid",
			exp: tableResponse{
				headers: []columnInfo{
					{"count(*)", "uint64"},
				},
				rows: []row{
					{[]columnResponse{uint64(8)}},
				},
			},
			eq: equal,
		},
		{
			// Intersect(All(),Distinct(Row(grouperid!=null),index='joiner',field='grouperid'))
			sql: "select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid",
			exp: tableResponse{
				headers: []columnInfo{{"_id", "uint64"}},
				rows: []row{
					{[]columnResponse{uint64(1)}},
					{[]columnResponse{uint64(2)}},
					{[]columnResponse{uint64(3)}},
					{[]columnResponse{uint64(5)}},
					{[]columnResponse{uint64(6)}},
					{[]columnResponse{uint64(7)}},
					{[]columnResponse{uint64(8)}},
					{[]columnResponse{uint64(9)}},
				},
			},
			eq: equalUnordered,
		},
		{
			// Intersect(Row(color='red'),Distinct(Row(grouperid!=null),index='joiner',field='grouperid'))
			sql: "select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where g.color = 'red'",
			exp: tableResponse{
				headers: []columnInfo{{"_id", "uint64"}},
				rows: []row{
					{[]columnResponse{uint64(3)}},
					{[]columnResponse{uint64(9)}},
				},
			},
			eq: equalUnordered,
		},
		{
			// Intersect(Row(color='red'),Distinct(Row(jointype=2),index='joiner',field='grouperid'))
			sql: "select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where g.color = 'red' and j.jointype = 2",
			exp: tableResponse{
				headers: []columnInfo{{"_id", "uint64"}},
				rows: []row{
					{[]columnResponse{uint64(3)}},
					{[]columnResponse{uint64(9)}},
				},
			},
			eq: equalUnordered,
		},
		// order by
		{
			// Distinct(Row(score!=null),index='grouper',field='score')
			sql: "select distinct score from grouper order by score asc",
			exp: tableResponse{
				headers: []columnInfo{{"score", "int64"}},
				rows: []row{
					{[]columnResponse{int64(-13)}},
					{[]columnResponse{int64(-10)}},
					{[]columnResponse{int64(-8)}},
					{[]columnResponse{int64(-2)}},
					{[]columnResponse{int64(0)}},
					{[]columnResponse{int64(6)}},
					{[]columnResponse{int64(80)}},
					{[]columnResponse{int64(100)}},
				},
			},
			eq: equal,
		},
		{
			// Distinct(Row(score!=null),index='grouper',field='score')
			sql: "select distinct score from grouper order by score desc",
			exp: tableResponse{
				headers: []columnInfo{{"score", "int64"}},
				rows: []row{
					{[]columnResponse{int64(100)}},
					{[]columnResponse{int64(80)}},
					{[]columnResponse{int64(6)}},
					{[]columnResponse{int64(0)}},
					{[]columnResponse{int64(-2)}},
					{[]columnResponse{int64(-8)}},
					{[]columnResponse{int64(-10)}},
					{[]columnResponse{int64(-13)}},
				},
			},
			eq: equal,
		},
		{
			// Distinct(Row(score!=null),index='grouper',field='score')
			sql: "select distinct score from grouper order by score asc limit 5",
			exp: tableResponse{
				headers: []columnInfo{{"score", "int64"}},
				rows: []row{
					{[]columnResponse{int64(-13)}},
					{[]columnResponse{int64(-10)}},
					{[]columnResponse{int64(-8)}},
					{[]columnResponse{int64(-2)}},
					{[]columnResponse{int64(0)}},
				},
			},
			eq: equal,
		},

		{
			// Distinct(Row(score!=null),index='grouper',field='score')
			sql: "select distinct score from grouper order by score desc limit 5",
			exp: tableResponse{
				headers: []columnInfo{{"score", "int64"}},
				rows: []row{
					{[]columnResponse{int64(100)}},
					{[]columnResponse{int64(80)}},
					{[]columnResponse{int64(6)}},
					{[]columnResponse{int64(0)}},
					{[]columnResponse{int64(-2)}},
				},
			},
			eq: equal,
		},

		// distinct
		{
			// Distinct(Row(score!=null),index='grouper',field='score')
			sql: "select distinct score from grouper",
			exp: tableResponse{
				headers: []columnInfo{{"score", "int64"}},
				rows: []row{
					{[]columnResponse{int64(-13)}},
					{[]columnResponse{int64(-10)}},
					{[]columnResponse{int64(-8)}},
					{[]columnResponse{int64(-2)}},
					{[]columnResponse{int64(0)}},
					{[]columnResponse{int64(6)}},
					{[]columnResponse{int64(80)}},
					{[]columnResponse{int64(100)}},
				},
			},
			eq: equalUnordered,
		},
		{

			// Distinct(Row(height!=null),index='grouper',field='height')
			sql: "select distinct height from grouper",
			exp: tableResponse{
				headers: []columnInfo{{"height", "int64"}},
				rows: []row{
					{[]columnResponse{int64(20)}},
					{[]columnResponse{int64(30)}},
					{[]columnResponse{int64(40)}},
					{[]columnResponse{int64(50)}},
					{[]columnResponse{int64(60)}},
					{[]columnResponse{int64(70)}},
					{[]columnResponse{int64(80)}},
					{[]columnResponse{int64(90)}},
					{[]columnResponse{int64(100)}},
					{[]columnResponse{int64(110)}},
				},
			},
			eq: equalUnordered,
		},

		// groupby
		{
			// GroupBy(Rows(field='age'),limit=100)
			sql: "select age as yrs, count(*) as cnt from grouper group by age",
			exp: tableResponse{
				headers: []columnInfo{
					{"yrs", "int64"},
					{"cnt", "uint64"},
				},
				rows: []row{
					{[]columnResponse{int64(16), uint64(4)}},
					{[]columnResponse{int64(19), uint64(1)}},
					{[]columnResponse{int64(27), uint64(3)}},
					{[]columnResponse{int64(31), uint64(1)}},
					{[]columnResponse{int64(34), uint64(1)}},
				},
			},
			eq: equalUnordered,
		},
		{
			// GroupBy(Rows(field='age'),Rows(field='color'),limit=100)
			sql: "select age, color, count(*) from grouper group by age, color",
			exp: tableResponse{
				headers: []columnInfo{
					{"age", "int64"},
					{"color", "string"},
					{"count(*)", "uint64"},
				},
				rows: []row{
					{[]columnResponse{int64(16), "blue", uint64(2)}},
					{[]columnResponse{int64(16), "red", uint64(1)}},
					{[]columnResponse{int64(19), "red", uint64(1)}},
					{[]columnResponse{int64(27), "blue", uint64(2)}},
					{[]columnResponse{int64(27), "green", uint64(1)}},
					{[]columnResponse{int64(31), "red", uint64(1)}},
					{[]columnResponse{int64(34), "blue", uint64(1)}},
				},
			},
			eq: equalUnordered,
		},
		{
			// GroupBy(Rows(field='age'),Rows(field='color'),limit=100,filter=Row(age=27),aggregate=Sum(field='height'))
			sql: "select age, color, sum(height) from grouper where age = 27 group by age, color",
			exp: tableResponse{
				headers: []columnInfo{
					{"age", "int64"},
					{"color", "string"},
					{"sum(height)", "int64"},
				},
				rows: []row{
					{[]columnResponse{int64(27), "blue", int64(100)}},
					{[]columnResponse{int64(27), "green", int64(50)}},
				},
			},
			eq: equalUnordered,
		},
		{
			// GroupBy(Rows(field='age'),limit=100,having=Condition(count>1))
			sql: "select age, count(*) from grouper group by age having count > 1",
			exp: tableResponse{
				headers: []columnInfo{
					{"age", "int64"},
					{"count(*)", "uint64"},
				},
				rows: []row{
					{[]columnResponse{int64(16), uint64(4)}},
					{[]columnResponse{int64(27), uint64(3)}},
				},
			},
			eq: equalUnordered,
		},
		{
			// GroupBy(Rows(field='age'),limit=100,having=Condition(1<=count<=3))
			sql: "select age, count(*) from grouper group by age having count between 1 and 3",
			exp: tableResponse{
				headers: []columnInfo{
					{"age", "int64"},
					{"count(*)", "uint64"},
				},
				rows: []row{
					{[]columnResponse{int64(19), uint64(1)}},
					{[]columnResponse{int64(27), uint64(3)}},
					{[]columnResponse{int64(31), uint64(1)}},
					{[]columnResponse{int64(34), uint64(1)}},
				},
			},
			eq: equalUnordered,
		},
		{
			// GroupBy(Rows(field='age'),limit=3)
			sql: "select age, count(*) as cnt from grouper group by age order by cnt desc, age desc limit 3",
			exp: tableResponse{
				headers: []columnInfo{
					{"age", "int64"},
					{"cnt", "uint64"},
				},
				rows: []row{
					{[]columnResponse{int64(16), uint64(4)}},
					{[]columnResponse{int64(27), uint64(3)}},
					{[]columnResponse{int64(19), uint64(1)}},
				},
			},
			eq: equal,
		},
		{
			sql: "show tables",
			exp: tableResponse{
				headers: []columnInfo{
					{"Table", "string"},
				},
				rows: []row{
					{[]columnResponse{"delete_me"}},
					{[]columnResponse{"grouper"}},
					{[]columnResponse{"joiner"}},
				},
			},
			eq: equal,
		},
		{
			sql: "show fields from grouper",
			exp: tableResponse{
				headers: []columnInfo{
					{"Field", "string"},
					{"Type", "string"},
				},
				rows: []row{
					{[]columnResponse{"age", "int"}},
					{[]columnResponse{"color", "keyed-set"}},
					{[]columnResponse{"height", "int"}},
					{[]columnResponse{"score", "int"}},
				},
			},
			eq: equal,
		},
		{
			sql: "drop table delete_me",
			exp: tableResponse{
				headers: []columnInfo{},
				rows:    []row{},
			},
			eq: equal,
		},
		{
			sql: "show tables",
			exp: tableResponse{
				headers: []columnInfo{
					{"Table", "string"},
				},
				rows: []row{
					{[]columnResponse{"grouper"}},
					{[]columnResponse{"joiner"}},
				},
			},
			eq: equal,
		},
		// The following cases test different paths within the `case *sqlparser.AndExpr`
		// of extract.go by providing different WHERE conditions.
		{
			// len(left) == 2 && len(right) == 1
			// right[0].table == left[0].table
			sql: "select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where g.color = 'red' and j.jointype = 2 and g.age = 16",
			exp: tableResponse{
				headers: []columnInfo{{"_id", "uint64"}},
				rows: []row{
					{[]columnResponse{uint64(9)}},
				},
			},
			eq: equalUnordered,
		},
		{
			// len(left) == 2 && len(right) == 1
			// right[0].table == left[1].table {
			sql: "select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where j.jointype = 2 and g.color = 'red' and g.age = 16",
			exp: tableResponse{
				headers: []columnInfo{{"_id", "uint64"}},
				rows: []row{
					{[]columnResponse{uint64(9)}},
				},
			},
			eq: equalUnordered,
		},
		{
			// len(left) == 1 && len(right) == 1 && left[0].table != right[0].table
			sql: "select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where g.color = 'red' and g.age = 16 and j.jointype = 2",
			exp: tableResponse{
				headers: []columnInfo{{"_id", "uint64"}},
				rows: []row{
					{[]columnResponse{uint64(9)}},
				},
			},
			eq: equalUnordered,
		},
	}

	for i, test := range tests {
		t.Run("test-"+strconv.Itoa(i), func(t *testing.T) {
			resp, err := gh.QuerySQLUnary(ctx, &pb.QuerySQLRequest{Sql: test.sql})
			if err != nil {
				t.Fatalf("sql: %s, error: %v", test.sql, err)
			} else {
				if resp.Duration == 0 {
					t.Fatal("duration not recorded")
				}
				duration, err := stream.GetDuration()
				if duration == 0 || err != nil {
					t.Fatal("duration header not recorded")
				}
				stream.ClearMD()
				tr := toTableResponse(resp)
				if err := test.eq(test.exp, tr); err != nil {
					t.Fatalf("sql: %s, error: %+v", test.sql, err)
				}
			}
		})
		t.Run("test-"+strconv.Itoa(i)+"-streaming", func(t *testing.T) {
			if strings.HasPrefix(test.sql, "drop table") {
				t.Skip("drop statements can only run once")
			}
			mock := &mockPilosa_QuerySQLServer{}
			err := gh.QuerySQL(&pb.QuerySQLRequest{Sql: test.sql}, mock)
			if err != nil {
				t.Fatalf("sql: %s, error: %v", test.sql, err)
			} else {
				if mock.Results[0].Duration == 0 {
					t.Fatal("duration not recorded")
				}
				duration, err := mock.GetDuration()
				if duration == 0 || err != nil {
					t.Fatal("duration header not recorded")
				}
				if len(mock.Results) > 1 && mock.Results[1].Duration != 0 {
					t.Fatal("duration on second result expected to be zero")
				}
				// TODO: test result values
			}
		})
	}
}

func TestQuerySQLUnaryWithError(t *testing.T) {

	stream := &MockServerTransportStream{}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
	gh, tearDownFunc := setUpTestQuerySQLUnary(ctx, t)
	defer tearDownFunc()

	tests := []struct {
		sql string
		err error
	}{
		{

			sql: "select * from index_not_found",
			err: pilosa.ErrIndexNotFound,
		},
		{

			sql: "select field_not_found from grouper",
			err: pilosa.ErrFieldNotFound,
		},
		{

			sql: "select * from grouper, index_not_found",
			err: sql.ErrUnsupportedQuery,
		},
		{

			sql: "select _id, age, field_not_found from grouper",
			err: pilosa.ErrFieldNotFound,
		},
	}

	for i, test := range tests {
		t.Run("test-"+strconv.Itoa(i), func(t *testing.T) {
			_, err := gh.QuerySQLUnary(ctx, &pb.QuerySQLRequest{Sql: test.sql})
			if err == nil {
				t.Fatalf("sql: %s, expected error: %v", test.sql, test.err)
			} else if errors.Cause(err) != test.err {
				t.Fatalf("sql: %s, expected error: %v, got: %v", test.sql, test.err, err)
			}
		})
	}
}

func TestCRUDIndexes(t *testing.T) {
	m := test.RunCommand(t)
	defer m.Close()

	stream := &MockServerTransportStream{}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
	gh := server.NewGRPCHandler(m.API)

	t.Run("CreateIndex", func(t *testing.T) {
		// Try CreateIndex for testindex1
		_, err := gh.CreateIndex(ctx, &pb.CreateIndexRequest{Name: "testindex1", Keys: true})
		if err != nil {
			t.Fatal(err)
		}

		schema, err := m.API.Schema(ctx, false)
		if err != nil {
			t.Fatal("Getting schema error", err)
		}
		if len(schema) != 1 {
			t.Fatal("Schema should include one index")
		}
		if schema[0].Name != "testindex1" {
			t.Fatal("Index name not set correctly")
		}
		if schema[0].Options.Keys != true {
			t.Fatal("Index Keys not set correctly")
		}
		if schema[0].Options.TrackExistence != true {
			t.Fatal("Index TrackExistence should be true when created by gRPC")
		}

		// Try CreateIndex for testindex2
		_, err = gh.CreateIndex(ctx, &pb.CreateIndexRequest{Name: "testindex2"})
		if err != nil {
			t.Fatal(err)
		}

		schema, err = m.API.Schema(ctx, false)
		if err != nil {
			t.Fatal("Getting schema error", err)
		}

		if len(schema) != 2 {
			t.Fatal("Schema should include two indexes")
		}

		_ = m.API.DeleteIndex(ctx, "testindex1")

		schema, err = m.API.Schema(ctx, false)
		if err != nil {
			t.Fatal("Getting schema error", err)
		}

		if len(schema) != 1 {
			t.Fatal("Schema should include one index")
		}
		if schema[0].Name != "testindex2" {
			t.Fatal("Index name not set correctly")
		}
		if schema[0].Options.Keys != false {
			t.Fatal("Index Keys not set correctly")
		}

		// Check errors for CreateIndex: create index with same name
		_, err = gh.CreateIndex(ctx, &pb.CreateIndexRequest{Name: "testindex2"})
		errStatus, _ := status.FromError(err)
		if errStatus.Code() != codes.AlreadyExists {
			t.Fatalf("Error code should be codes.AlreadyExists, but is %v", errStatus.Code())
		}

		// Check errors for CreateIndex: create index with no name
		_, err = gh.CreateIndex(ctx, &pb.CreateIndexRequest{Name: ""})
		errStatus, _ = status.FromError(err)
		if errStatus.Code() != codes.FailedPrecondition {
			t.Fatalf("Error code should be codes.Unknown, but is %v", errStatus.Code())
		}

		// Check errors for CreateIndex: create index with invalid name
		_, err = gh.CreateIndex(ctx, &pb.CreateIndexRequest{Name: "💩"})
		errStatus, _ = status.FromError(err)
		if errStatus.Code() != codes.FailedPrecondition {
			t.Fatalf("Error code should be codes.FailedPrecondition, but is %v", errStatus.Code())
		}

		_ = m.API.DeleteIndex(ctx, "testindex2")
	})

	t.Run("GetIndex", func(t *testing.T) {
		_, err := m.API.CreateIndex(ctx, "testindex1", pilosa.IndexOptions{})
		if err != nil {
			t.Fatal(err)
		}

		// Check GetIndex for testindex1
		resp, err := gh.GetIndex(ctx, &pb.GetIndexRequest{Name: "testindex1"})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Index.Name != "testindex1" {
			t.Fatalf("Index name does not match: %s", resp.Index.Name)
		}

		// Check errors for GetIndex: get index that doesn't exist
		_, err = gh.GetIndex(ctx, &pb.GetIndexRequest{Name: "wrongname"})
		errStatus, _ := status.FromError(err)
		if errStatus.Code() != codes.NotFound {
			t.Fatalf("Error code should be codes.NotFound, but is %v", errStatus.Code())
		}

		// Check errors for GetIndex: get index with invalid name
		_, err = gh.GetIndex(ctx, &pb.GetIndexRequest{Name: "💩"})
		errStatus, _ = status.FromError(err)
		if errStatus.Code() != codes.NotFound {
			t.Fatalf("Error code should be codes.NotFound, but is %v", errStatus.Code())
		}
		_ = m.API.DeleteIndex(ctx, "testindex1")
	})

	t.Run("GetIndexes", func(t *testing.T) {
		_, err := m.API.CreateIndex(ctx, "testindex1", pilosa.IndexOptions{})
		if err != nil {
			t.Fatal(err)
		}

		// Check GetIndexes
		resp2, err := gh.GetIndexes(ctx, &pb.GetIndexesRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp2.Indexes) != 1 && resp2.Indexes[0].Name != "testindex1" {
			t.Fatalf("GetIndexes did not produce the correct result set: %v", resp2.Indexes)
		}

		_, err = m.API.CreateIndex(ctx, "testindex2", pilosa.IndexOptions{})
		if err != nil {
			t.Fatal(err)
		}

		// Check GetIndexes again
		resp, err := gh.GetIndexes(ctx, &pb.GetIndexesRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Indexes) != 2 {
			t.Fatalf("GetIndexes did not produce the correct result set: %v", resp.Indexes)
		}
		_ = m.API.DeleteIndex(ctx, "testindex1")
		_ = m.API.DeleteIndex(ctx, "testindex2")
	})

	t.Run("DeleteIndexes", func(t *testing.T) {
		_, err := m.API.CreateIndex(ctx, "testindex1", pilosa.IndexOptions{})
		if err != nil {
			t.Fatal(err)
		}

		// Try to delete index
		_, err = gh.DeleteIndex(ctx, &pb.DeleteIndexRequest{Name: "testindex1"})
		if err != nil {
			t.Fatal(err)
		}

		schema, err := m.API.Schema(ctx, false)
		if err != nil {
			t.Fatal("Getting schema error", err)
		}

		if len(schema) != 0 {
			t.Fatal("Schema should include no index")
		}

		// Try to delete non-existing index
		_, err = gh.DeleteIndex(ctx, &pb.DeleteIndexRequest{Name: "doesnotexist"})
		errStatus, _ := status.FromError(err)
		if errStatus.Code() != codes.NotFound {
			t.Fatalf("Error code should be codes.NotFound, but is %v", errStatus.Code())
		}
	})
}

func setUpTestQuerySQLUnary(ctx context.Context, t *testing.T) (gh *server.GRPCHandler, tearDownFunc func()) {
	t.Helper()

	m := test.RunCommand(t)
	gh = server.NewGRPCHandler(m.API)

	// grouper
	grouper := m.MustCreateIndex(t, "grouper", pilosa.IndexOptions{Keys: false, TrackExistence: true})
	m.MustCreateField(t, grouper.Name(), "color", pilosa.OptFieldKeys())
	for id, color := range map[int]string{
		1:  "blue",
		2:  "blue",
		5:  "blue",
		6:  "blue",
		7:  "blue",
		3:  "red",
		9:  "red",
		10: "red",
		4:  "green",
	} {
		if _, err := gh.QueryPQLUnary(ctx, &pb.QueryPQLRequest{
			Index: grouper.Name(),
			Pql:   fmt.Sprintf(`Set(%d, color="%s")`, id, color),
		}); err != nil {
			t.Fatal(err)
		}
	}
	m.MustCreateField(t, grouper.Name(), "score", pilosa.OptFieldTypeInt(-1000, 1000))
	for id, score := range map[int]int{
		1:  -10,
		2:  -8,
		3:  6,
		4:  0,
		5:  -2,
		6:  100,
		7:  0,
		8:  -13,
		9:  80,
		10: -2,
	} {
		if _, err := gh.QueryPQLUnary(ctx, &pb.QueryPQLRequest{
			Index: grouper.Name(),
			Pql:   fmt.Sprintf(`Set(%d, score=%d)`, id, score),
		}); err != nil {
			t.Fatal(err)
		}
	}
	m.MustCreateField(t, grouper.Name(), "age", pilosa.OptFieldTypeInt(0, 100))
	for id, age := range map[int]int{
		2:  16,
		5:  16,
		8:  16,
		9:  16,
		3:  19,
		1:  27,
		4:  27,
		7:  27,
		10: 31,
		6:  34,
	} {
		if _, err := gh.QueryPQLUnary(ctx, &pb.QueryPQLRequest{
			Index: grouper.Name(),
			Pql:   fmt.Sprintf(`Set(%d, age=%d)`, id, age),
		}); err != nil {
			t.Fatal(err)
		}
	}

	m.MustCreateField(t, grouper.Name(), "height", pilosa.OptFieldTypeInt(0, 1000))
	for id, height := range map[int]int{
		1:  20,
		2:  30,
		3:  40,
		4:  50,
		5:  60,
		6:  70,
		7:  80,
		8:  90,
		9:  100,
		10: 110,
	} {
		if _, err := gh.QueryPQLUnary(ctx, &pb.QueryPQLRequest{
			Index: grouper.Name(),
			Pql:   fmt.Sprintf(`Set(%d, height=%d)`, id, height),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// joiner
	joiner := m.MustCreateIndex(t, "joiner", pilosa.IndexOptions{TrackExistence: true})
	m.MustCreateField(t, joiner.Name(), "grouperid", pilosa.OptFieldTypeInt(0, 1000), pilosa.OptFieldForeignIndex(grouper.Name()))
	m.MustCreateField(t, joiner.Name(), "jointype", pilosa.OptFieldTypeInt(-1000, 1000))
	for id, grouperid := range map[int]int{
		1:  1,
		2:  2,
		3:  5,
		4:  6,
		5:  7,
		6:  3,
		7:  8,
		8:  9,
		9:  1,
		10: 2,
	} {
		if _, err := gh.QueryPQLUnary(ctx, &pb.QueryPQLRequest{
			Index: joiner.Name(),
			Pql:   fmt.Sprintf(`Set(%d, grouperid=%d)`, id, grouperid),
		}); err != nil {
			t.Fatal(err)
		}
	}
	for id, jointype := range map[int]int{
		1:  1,
		2:  1,
		3:  1,
		4:  1,
		5:  1,
		6:  2,
		7:  2,
		8:  2,
		9:  3,
		10: 3,
	} {
		if _, err := gh.QueryPQLUnary(ctx, &pb.QueryPQLRequest{
			Index: joiner.Name(),
			Pql:   fmt.Sprintf(`Set(%d, jointype=%d)`, id, jointype),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// delete_me
	m.MustCreateIndex(t, "delete_me", pilosa.IndexOptions{TrackExistence: true})

	return gh, func() {
		if err := m.API.DeleteIndex(ctx, joiner.Name()); err != nil {
			panic(err)
		}
		if err := m.API.DeleteIndex(ctx, grouper.Name()); err != nil {
			panic(err)
		}
		if err := m.Close(); err != nil {
			panic(err)
		}
	}
}

func toTableResponse(resp *pb.TableResponse) tableResponse {
	tr := tableResponse{
		headers: make([]columnInfo, len(resp.Headers)),
		rows:    make([]row, len(resp.Rows)),
	}

	for i, h := range resp.Headers {
		tr.headers[i] = columnInfo{
			name:     h.Name,
			datatype: h.Datatype,
		}
	}

	for i, r := range resp.Rows {
		tr.rows[i].columns = make([]columnResponse, len(r.Columns))
		for j, c := range r.Columns {

			switch v := c.GetColumnVal().(type) {
			case *pb.ColumnResponse_StringVal:
				tr.rows[i].columns[j] = v.StringVal
			case *pb.ColumnResponse_Uint64Val:
				tr.rows[i].columns[j] = v.Uint64Val
			case *pb.ColumnResponse_Int64Val:
				tr.rows[i].columns[j] = v.Int64Val
			case *pb.ColumnResponse_BoolVal:
				tr.rows[i].columns[j] = v.BoolVal
			case *pb.ColumnResponse_BlobVal:
				tr.rows[i].columns[j] = v.BlobVal
			case *pb.ColumnResponse_Uint64ArrayVal:
				tr.rows[i].columns[j] = v.Uint64ArrayVal.Vals
			case *pb.ColumnResponse_StringArrayVal:
				tr.rows[i].columns[j] = v.StringArrayVal.Vals
			case *pb.ColumnResponse_Float64Val:
				tr.rows[i].columns[j] = v.Float64Val
			case *pb.ColumnResponse_DecimalVal:
				tr.rows[i].columns[j] = pql.NewDecimal(v.DecimalVal.Value, v.DecimalVal.Scale)
			default:
				tr.rows[i].columns[j] = nil
			}
		}
	}

	return tr
}

func equal(exp tableResponse, got tableResponse) error {
	if !reflect.DeepEqual(exp, got) {
		return fmt.Errorf("got: %+v %[1]T, but expected: %+v", got, exp)
	}
	return nil
}

func equalUnordered(exp tableResponse, got tableResponse) error {
	if len(exp.headers) != len(got.headers) || !reflect.DeepEqual(exp.headers, got.headers) {
		return fmt.Errorf("header does not match: got %+v, but expected %+v", got.headers, exp.headers)
	}

	if len(exp.rows) != len(got.rows) {
		return fmt.Errorf("rows count does not match: got %+v, but expected %+v", len(got.rows), len(exp.rows))
	}
	for _, er := range exp.rows {
		for j, gr := range got.rows {
			if reflect.DeepEqual(er.columns, gr.columns) {
				got.rows[j] = got.rows[len(got.rows)-1]
				got.rows = got.rows[:len(got.rows)-1]
				break
			}
		}
	}
	if len(got.rows) > 0 {
		return fmt.Errorf("got incorrect rows: %+v", got.rows)
	}
	return nil
}

type MockServerTransportStream struct {
	header metadata.MD
}

func (stream *MockServerTransportStream) Method() string {
	return ""
}

func (stream *MockServerTransportStream) SetHeader(md metadata.MD) error {
	// Should probably merge md with value of stream.header, but this works since we have only one metadata value
	stream.header = md
	return nil
}

func (stream *MockServerTransportStream) SendHeader(md metadata.MD) error {
	stream.header = md
	return nil
}

func (stream *MockServerTransportStream) SetTrailer(md metadata.MD) error {
	return nil
}

func (stream *MockServerTransportStream) GetDuration() (int, error) {
	duration, ok := stream.header["duration"]
	if ok {
		return strconv.Atoi(duration[0])
	}
	return 0, errors.New("duration not recorded")
}

func (stream *MockServerTransportStream) ClearMD() {
	stream.header = metadata.New(map[string]string{})
}

type mockPilosa_QuerySQLServer struct {
	MockServerTransportStream
	pb.Pilosa_QuerySQLServer
	Results []*pb.RowResponse
}

func (m *mockPilosa_QuerySQLServer) Send(result *pb.RowResponse) error {
	m.Results = append(m.Results, result)
	return nil
}

func (m *mockPilosa_QuerySQLServer) SendHeader(md metadata.MD) error {
	return m.MockServerTransportStream.SendHeader(md)
}

func (m *mockPilosa_QuerySQLServer) SetHeader(md metadata.MD) error {
	return m.MockServerTransportStream.SetHeader(md)
}

func (m *mockPilosa_QuerySQLServer) SetTrailer(md metadata.MD) {
	_ = m.MockServerTransportStream.SetTrailer(md)
}

func (m *mockPilosa_QuerySQLServer) Context() context.Context {
	return context.Background()
}

func (m *mockPilosa_QuerySQLServer) clearResults() {
	m.Results = m.Results[:0]
}
