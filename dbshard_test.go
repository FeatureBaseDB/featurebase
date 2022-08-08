// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"context"
	"fmt"
	"testing"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/test"
	. "github.com/molecula/featurebase/v3/vprint" // nolint:staticcheck
)

func TestAPI_SimplerOneNode_ImportColumnKey(t *testing.T) {

	c := test.MustRunCluster(t, 1)
	defer c.Close()

	m0 := c.GetNode(0)

	t.Run("RowIDColumnKey", func(t *testing.T) {
		ctx := context.Background()
		indexName := c.Idx()
		fieldName := "f"

		index, err := m0.API.CreateIndex(ctx, indexName, pilosa.IndexOptions{Keys: true, TrackExistence: true})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		if index.CreatedAt() == 0 {
			t.Fatal("index createdAt is empty")
		}

		field, err := m0.API.CreateField(ctx, indexName, fieldName, pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}
		if field.CreatedAt() == 0 {
			t.Fatal("field createdAt is empty")
		}

		rowID := uint64(1)
		timestamp := int64(0)

		// Generate some keyed records.
		rowIDs := []uint64{}
		timestamps := []int64{}
		for i := 1; i <= 10; i++ {
			rowIDs = append(rowIDs, rowID)
			timestamps = append(timestamps, timestamp)
		}

		// Keys are sharded so ordering is not guaranteed.
		colKeys := []string{"col10", "col8", "col9", "col6", "col7", "col4", "col5", "col2", "col3", "col1"}

		// Import data with keys to the primary and verify that it gets
		// translated and forwarded to the owner of shard 0
		req := &pilosa.ImportRequest{
			Index:          indexName,
			IndexCreatedAt: index.CreatedAt(),
			Field:          fieldName,
			FieldCreatedAt: field.CreatedAt(),
			Shard:          0, // import is all on shard 0, why are we making bocu other shards? b/c this is ignored.
			RowIDs:         rowIDs,
			ColumnKeys:     colKeys,
			Timestamps:     timestamps,
		}

		qcx := m0.API.Txf().NewQcx()
		if err := m0.API.Import(ctx, qcx, req); err != nil {
			t.Fatal(err)
		}
		PanicOn(qcx.Finish())

		//select {}

		pql := fmt.Sprintf("Row(%s=%d)", fieldName, rowID)

		// Query node0.
		res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql})
		if err != nil {
			t.Fatal(err)
		}
		keys := res.Results[0].(*pilosa.Row).Keys
		if !sameStringSlice(keys, colKeys) {
			t.Fatalf("unexpected column keys: %#v", keys)
		}
	})

}
