// Copyright 2020 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/test"
)

func TestAPI_SimplerOneNode_ImportColumnKey(t *testing.T) {

	c := test.MustRunCluster(t, 1,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
	)
	defer c.Close()

	m0 := c.GetNode(0)

	t.Run("RowIDColumnKey", func(t *testing.T) {
		ctx := context.Background()
		indexName := "rick"
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

		// Import data with keys to the coordinator (node0) and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
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
		panicOn(qcx.Finish())

		//select {}

		pql := fmt.Sprintf("Row(%s=%d)", fieldName, rowID)

		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql}); err != nil {
			t.Fatal(err)
		} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
			t.Fatalf("unexpected column keys: %#v", keys)
		}
	})

}