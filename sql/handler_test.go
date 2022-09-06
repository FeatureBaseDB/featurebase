// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package sql_test

import (
	"context"
	"math"
	"testing"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/sql"
	"github.com/featurebasedb/featurebase/v3/test"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestHandler(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	api := cluster.GetNode(0).API
	queryStr := "select * from nowhere"
	mapper := sql.NewMapper()
	query, err := mapper.MapSQL(queryStr)
	if err != nil {
		t.Fatal("failed to map SQL")
	}
	handler := sql.NewSelectHandler(api)
	_, err = handler.Handle(context.Background(), query)
	if err.Error() != "mapping select: handling: nowhere: index not found" {
		//expecting it to fail with index not found
		//can be more elaborate later
		t.Fatal(err)
	}

}

func TestSelectHandler_MapSelect(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	api := cluster.GetNode(0).API

	if _, err := api.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err = api.CreateField(context.Background(), "i", "bytes", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
		t.Fatal(err)
	} else if _, err = api.CreateField(context.Background(), "i", "duration_time", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
		t.Fatal(err)
	} else if _, err = api.CreateField(context.Background(), "i", "timestamp", pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds)); err != nil {
		t.Fatal(err)
	}

	if _, err := api.CreateIndex(context.Background(), "j", pilosa.IndexOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err = api.CreateField(context.Background(), "j", "bytes", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
		t.Fatal(err)
	} else if _, err = api.CreateField(context.Background(), "j", "bsi", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64), pilosa.OptFieldForeignIndex("i")); err != nil {
		t.Fatal(err)
	} else if _, err = api.CreateField(context.Background(), "j", "timestamp", pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds)); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name          string
		input         string
		output        string
		expectedError string
	}{
		{
			name:   "WhereTimestamp",
			input:  `SELECT * FROM i WHERE timestamp>"2000-01-01T00:00:00Z"`,
			output: `Extract(Row(timestamp>"2000-01-01T00:00:00Z"),Rows(bytes),Rows(duration_time),Rows(timestamp))`,
		},

		{
			name:   "WhereTimestampWithSpaces",
			input:  `SELECT * FROM i WHERE timestamp > "2000-01-01T00:00:00Z"`,
			output: `Extract(Row(timestamp>"2000-01-01T00:00:00Z"),Rows(bytes),Rows(duration_time),Rows(timestamp))`,
		},
		{
			name:   "InnerJoin",
			input:  `select count(*) from i INNER JOIN j ON i._id = j.bsi where i.bytes = 1 and j.bytes = 2`,
			output: `Count(Intersect(Row(bytes=1),Distinct(Row(bytes=2),index='j',field='bsi')))`,
		},
		{
			name:          "InnerJoinInvalidPrimary",
			input:         `select count(*) from z INNER JOIN j ON z._id = j.bsi where z.bytes = 1 and j.bytes = 2`,
			output:        "",
			expectedError: `handling: nonexistent index "z"`,
		},
		{
			name:          "InnerJoinInvalidSecondary",
			input:         `select count(*) from i INNER JOIN z ON i._id = z.bsi where i.bytes = 1 and z.bytes = 2`,
			output:        "",
			expectedError: `handling: nonexistent index "z"`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			query, err := sql.NewMapper().MapSQL(tt.input)
			if err != nil {
				t.Fatal(err)
			}

			h := sql.NewSelectHandler(api)
			mr, err := h.MapSelect(context.Background(), query.Statement.(*sqlparser.Select), query.Mask)
			if err != nil {
				if err.Error() != tt.expectedError {
					if tt.expectedError == "" {
						t.Fatalf("unexpected error %q", err.Error())
					} else {
						t.Fatalf("expected error %q, got %q", tt.expectedError, err.Error())
					}
				}
			} else if got, want := mr.Query, tt.output; got != want {
				if tt.expectedError != "" {
					t.Fatalf("expected error %q, got no error", tt.expectedError)
				}
				t.Fatalf("unexpected pql\npql:  %s\nwant: %s", got, want)
			}
		})
	}
}
