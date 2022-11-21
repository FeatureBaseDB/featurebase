package pilosa_test

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/server"
	"github.com/molecula/featurebase/v3/test"
)

func TestExecutor_Apply(t *testing.T) {
	c := test.MustRunCluster(t, 1, []server.CommandOption{
		server.OptCommandServerOptions(
			pilosa.OptServerIsDataframeEnabled(true),
		),
	})
	defer c.Close()
	indexName := "ti"
	fieldName := "f"
	ctx := context.Background()
	api := c.GetNode(0).API
	idx, err := api.CreateIndex(ctx, indexName, pilosa.IndexOptions{Keys: false, TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}
	opts := []pilosa.FieldOption{pilosa.OptFieldTypeInt(0, math.MaxInt64)}
	_, err = idx.CreateField(fieldName, "", opts...)
	if err != nil {
		t.Fatal(err)
	}
	req := &pilosa.ImportValueRequest{}
	req.Index = indexName
	req.Field = fieldName
	req.ColumnIDs = []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	req.Values = []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	qcx := api.Txf().NewQcx()
	defer qcx.Abort()

	if err := api.ImportValue(ctx, qcx, req); err != nil {
		t.Fatal(err)
	}

	if err := qcx.Finish(); err != nil {
		t.Fatal(err)
	}

	t.Run("dataframe ingest", func(t *testing.T) {
		//		func (c *Client) ApplyDataframeChangeset(indexName string, cr *pilosa.ChangesetRequest, shard uint64) (map[string]interface{}, error) {
		cr := &pilosa.ChangesetRequest{}
		// for each row a list of columns
		cr.Columns = []interface{}{
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			[]int64{2, 4, 6, 8, 10, 12, 14, 16, 18, 20},
			[]float64{1, 1.414, 1.732, 2, 2.236, 2.449, 2.646, 2.828, 3, 3.162},
		}
		cr.ShardIds = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		cr.SimpleSchema = []pilosa.NameType{
			{Name: "_ID", DataType: arrow.PrimitiveTypes.Int64},
			{Name: "ival", DataType: arrow.PrimitiveTypes.Int64},
			{Name: "fval", DataType: arrow.PrimitiveTypes.Float64},
		}
		shard := uint64(0)
		err := api.ApplyDataframeChangeset(ctx, indexName, cr, shard)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("dataframe schema", func(t *testing.T) {
		expectedJSON := `[{"Name":"_ID","Type":"int64"},{"Name":"ival","Type":"int64"},{"Name":"fval","Type":"float64"}]`
		parts, err := api.GetDataframeSchema(ctx, indexName)
		if err != nil {
			t.Fatal(err)
		}
		w := new(bytes.Buffer)
		if err := json.NewEncoder(w).Encode(parts); err != nil {
			t.Fatal(err)
		}
		got := strings.Trim(w.String(), "\t \n")
		if strings.Compare(got, expectedJSON) != 0 {
			t.Fatalf("expected: %v got: %v", expectedJSON, got)
		}
	})
	t.Run("dataframe apply all", func(t *testing.T) {
		pql := `Apply("_ID","_")`
		if res, err := api.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql}); err != nil {
			t.Fatal(err)
		} else {
			// qr := res.Results[0].(*dataframe.DataFrame)
			expectedJSON := `{"Results":[{"_ID":{"int":[10,9,8,7,6,5,4,3,2,1,0]}}],"Err":null,"Profile":null}`
			w := new(bytes.Buffer)
			if err := json.NewEncoder(w).Encode(res); err != nil {
				t.Fatal(err)
			}
			got := strings.Trim(w.String(), "\t \n")
			if strings.Compare(got, expectedJSON) != 0 {
				t.Fatalf("expected: %v got: %v", expectedJSON, got)
			}
		}
	})
	t.Run("dataframe apply filter", func(t *testing.T) {
		// TODO(twg) 2022/11/03 refactor this when changing marshalling

		pql := `Apply(ConstRow(columns=[2,4,6]),"_ID+0","_")`
		if res, err := api.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql}); err != nil {
			t.Fatal(err)
		} else {
			expectedJSON := `{"Results":[{"I":{"int":[6,4,2]}}],"Err":null,"Profile":null}`
			w := new(bytes.Buffer)
			if err := json.NewEncoder(w).Encode(res); err != nil {
				t.Fatal(err)
			}
			got := strings.Trim(w.String(), "\t \n")
			if strings.Compare(got, expectedJSON) != 0 {
				t.Fatalf("expected: %v got: %v", expectedJSON, got)
			}
		}
	})
	t.Run("dataframe apply ivy err map", func(t *testing.T) {
		pql := `Apply("barf","_")`
		if _, err := api.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql}); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("dataframe apply ivy err reduce", func(t *testing.T) {
		pql := `Apply("_ID","barfo")`
		if _, err := api.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql}); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("dataframe arrow filter", func(t *testing.T) {
		pql := `Arrow(ConstRow(columns=[2,4,6]))`
		if res, err := api.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql}); err != nil {
			t.Fatal(err)
		} else {
			expectedJSON := `{"Results":[{"_ID":[2,4,6],"fval":[1.414,2,2.449],"ival":[4,8,12]}],"Err":null,"Profile":null}`
			w := new(bytes.Buffer)
			if err := json.NewEncoder(w).Encode(res); err != nil {
				t.Fatal(err)
			}
			got := strings.Trim(w.String(), "\t \n")
			if strings.Compare(got, expectedJSON) != 0 {
				t.Fatalf("expected: %v got: %v", expectedJSON, got)
			}
		}
	})
	t.Run("dataframe arrow filter with header", func(t *testing.T) {
		pql := `Arrow(ConstRow(columns=[2,4,6]),header=["fval"])`
		if res, err := api.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql}); err != nil {
			t.Fatal(err)
		} else {
			expectedJSON := `{"Results":[{"_ID":[2,4,6],"fval":[1.414,2,2.449]}],"Err":null,"Profile":null}`
			w := new(bytes.Buffer)
			if err := json.NewEncoder(w).Encode(res); err != nil {
				t.Fatal(err)
			}
			got := strings.Trim(w.String(), "\t \n")
			if strings.Compare(got, expectedJSON) != 0 {
				t.Fatalf("expected: %v got: %v", expectedJSON, got)
			}
		}
	})
	t.Run("dataframe delete", func(t *testing.T) {
		err := api.DeleteDataframe(ctx, indexName)
		if err != nil {
			t.Fatal(err)
		}
		expectedJSON := `[]`
		parts, err := api.GetDataframeSchema(ctx, indexName)
		if err != nil {
			t.Fatal(err)
		}
		w := new(bytes.Buffer)
		if err := json.NewEncoder(w).Encode(parts); err != nil {
			t.Fatal(err)
		}
		got := strings.Trim(w.String(), "\t \n")
		if strings.Compare(got, expectedJSON) != 0 {
			t.Fatalf("expected: %v got: %v", expectedJSON, got)
		}
	})
}
