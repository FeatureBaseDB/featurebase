// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package server_test

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/pg"
	"github.com/molecula/featurebase/v3/pg/pgtest"
	"github.com/molecula/featurebase/v3/server"
	"github.com/molecula/featurebase/v3/test"
)

func TestPostgresHandler(t *testing.T) {
	m := test.RunCommand(t)
	defer m.Close()

	pgh := server.NewPostgresHandler(m.API, logger.NewLogfLogger(t), server.SqlV1)

	m.MustCreateIndex(t, "i", pilosa.IndexOptions{TrackExistence: true})
	m.MustCreateField(t, "i", "set")
	m.MustCreateField(t, "i", "keyset", pilosa.OptFieldKeys())
	m.MustCreateField(t, "i", "mutex", pilosa.OptFieldTypeMutex(pilosa.CacheTypeNone, 0))
	m.MustCreateField(t, "i", "keymutex", pilosa.OptFieldKeys(), pilosa.OptFieldTypeMutex(pilosa.CacheTypeNone, 0))
	m.MustCreateField(t, "i", "int", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
	m.MustCreateField(t, "i", "decimal", pilosa.OptFieldTypeDecimal(2))
	m.MustCreateField(t, "i", "time", pilosa.OptFieldTypeTime("YMDH", "0"))
	m.MustCreateField(t, "i", "bool", pilosa.OptFieldTypeBool())

	m.MustCreateIndex(t, "j", pilosa.IndexOptions{TrackExistence: true, Keys: true})
	m.MustCreateField(t, "j", "set")

	storeOK := pgtest.ResultSet{
		Columns: []pg.ColumnInfo{
			{
				Name: "result",
				Type: pg.TypeCharoid,
			},
		},
		Data: [][]string{
			{
				"true",
			},
		},
	}

	cases := []struct {
		Name    string
		Queries []string
		Results []pgtest.ResultSet
	}{
		{
			Name: "Extract-Nothing",
			Queries: []string{
				`[i]Extract(All(), Rows(set))`,
			},
			Results: []pgtest.ResultSet{
				{
					Columns: []pg.ColumnInfo{
						{
							Name: "_id",
							Type: pg.TypeCharoid,
						},
						{
							Name: "set",
							Type: pg.TypeCharoid,
						},
					},
				},
			},
		},
		{
			Name: "Store",
			Queries: []string{
				`[i]Store(ConstRow(columns=[1, 2, 3]), set=4)`,
				`[i]Store(ConstRow(columns=[0, 2, 4]), set=5)`,
				`[j]Store(ConstRow(columns=[1, 2, 3]), set=4)`,
				`[j]Store(ConstRow(columns=[0, 2, 4]), set=5)`,
			},
			Results: []pgtest.ResultSet{
				storeOK,
				storeOK,
				storeOK,
				storeOK,
			},
		},
		{
			Name: "Set",
			Queries: []string{
				`[i]Set(1, keyset="a")`,
				`[i]Set(2, keyset="b")`,
				`[i]Set(3, mutex=3)`,
				`[i]Set(4, keymutex="d")`,
				`[i]Set(1, int=5)`,
				`[i]Set(2, decimal=6.01)`,
				`[i]Set(3, time=7, 2016-01-01T00:00)`,
				`[i]Set(4, bool=false)`,
			},
			Results: []pgtest.ResultSet{
				storeOK,
				storeOK,
				storeOK,
				storeOK,
				storeOK,
				storeOK,
				storeOK,
				storeOK,
			},
		},
		{
			Name: "Extract",
			Queries: []string{
				`[i]Extract(
					All(),
					Rows(set), Rows(keyset),
					Rows(mutex), Rows(keymutex),
					Rows(int), Rows(decimal),
					Rows(time),
					Rows(bool)
				)`,
			},
			Results: []pgtest.ResultSet{
				{
					Columns: []pg.ColumnInfo{
						{Name: "_id", Type: pg.TypeCharoid},
						{Name: "set", Type: pg.TypeCharoid},
						{Name: "keyset", Type: pg.TypeCharoid},
						{Name: "mutex", Type: pg.TypeCharoid},
						{Name: "keymutex", Type: pg.TypeCharoid},
						{Name: "int", Type: pg.TypeCharoid},
						{Name: "decimal", Type: pg.TypeCharoid},
						{Name: "time", Type: pg.TypeCharoid},
						{Name: "bool", Type: pg.TypeCharoid},
					},
					Data: [][]string{
						{`1`, `[4]`, `["a"]`, `null`, `null`, `5`, `null`, `[]`, `null`},
						{`2`, `[4,5]`, `["b"]`, `null`, `null`, `null`, `6.01`, `[]`, `null`},
						{`3`, `[4]`, `[]`, `3`, `null`, `null`, `null`, `[7]`, `null`},
						{`4`, `[5]`, `[]`, `null`, `d`, `null`, `null`, `[]`, `false`},
					},
				},
			},
		},
		{
			Name: "GroupBy",
			Queries: []string{
				`[i]GroupBy(Rows(set))`,
			},
			Results: []pgtest.ResultSet{
				{
					Columns: []pg.ColumnInfo{
						{Name: "set", Type: pg.TypeCharoid},
						{Name: "count", Type: pg.TypeCharoid},
					},
					Data: [][]string{
						{"4", "3"},
						{"5", "3"},
					},
				},
			},
		},
		{
			Name: "Count",
			Queries: []string{
				`[i]Count(Row(set=4))`,
				`[i]Count(Row(int > 0))`,
			},
			Results: []pgtest.ResultSet{
				{
					Columns: []pg.ColumnInfo{
						{Name: "count", Type: pg.TypeCharoid},
					},
					Data: [][]string{{"3"}},
				},
				{
					Columns: []pg.ColumnInfo{
						{Name: "count", Type: pg.TypeCharoid},
					},
					Data: [][]string{{"1"}},
				},
			},
		},
		{
			Name: "FieldValue",
			Queries: []string{
				`[i]FieldValue(field=int, column=1)`,
				`[i]FieldValue(field=decimal, column=2)`,
			},
			Results: []pgtest.ResultSet{
				{
					Columns: []pg.ColumnInfo{
						{Name: "value", Type: pg.TypeCharoid},
						{Name: "count", Type: pg.TypeCharoid},
					},
					Data: [][]string{
						{"5", "1"},
					},
				},
				{
					Columns: []pg.ColumnInfo{
						{Name: "value", Type: pg.TypeCharoid},
						{Name: "count", Type: pg.TypeCharoid},
					},
					Data: [][]string{
						{"6.01", "1"},
					},
				},
			},
		},
		{
			Name: "Rows",
			Queries: []string{
				`[i]Rows(set)`,
				`[i]Rows(keyset)`,
			},
			Results: []pgtest.ResultSet{
				{
					Columns: []pg.ColumnInfo{
						{Name: "set", Type: pg.TypeCharoid},
					},
					Data: [][]string{
						{"4"},
						{"5"},
					},
				},
				{
					Columns: []pg.ColumnInfo{
						{Name: "keyset", Type: pg.TypeCharoid},
					},
					Data: [][]string{
						{"a"},
						{"b"},
					},
				},
			},
		},
		{
			Name: "TopN",
			Queries: []string{
				`[i]TopN(set)`,
				`[i]TopN(keyset)`,
			},
			Results: []pgtest.ResultSet{
				{
					Columns: []pg.ColumnInfo{
						{Name: "set", Type: pg.TypeCharoid},
						{Name: "count", Type: pg.TypeCharoid},
					},
					Data: [][]string{
						{"5", "3"},
						{"4", "3"},
					},
				},
				{
					Columns: []pg.ColumnInfo{
						{Name: "keyset", Type: pg.TypeCharoid},
						{Name: "count", Type: pg.TypeCharoid},
					},
					Data: [][]string{
						{"b", "1"},
						{"a", "1"},
					},
				},
			},
		},
		{
			Name: "SQL",
			Queries: []string{
				`select _id from i;`,
			},
			Results: []pgtest.ResultSet{
				{
					Columns: []pg.ColumnInfo{
						{Name: "_id", Type: pg.TypeCharoid},
					},
					Data: [][]string{
						{`1`},
						{`2`},
						{`3`},
						{`4`},
					},
				},
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			for i, q := range c.Queries {
				var res pgtest.ResultSet
				err := pgh.HandleQuery(ctx, &res, pg.SimpleQuery(q))
				if err != nil {
					t.Errorf("query %q failed: %v", q, err)
					continue
				}

				expected := c.Results[i]
				if !reflect.DeepEqual(res, expected) {
					t.Errorf("query %q returned incorrect results: expected %v but got %v", q, expected, res)
				}
			}
		})
	}
}
