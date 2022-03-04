// Copyright 2021 Molecula Corp. All rights reserved.
package main

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/boltdb"
	"github.com/molecula/featurebase/v3/server"
	"github.com/molecula/featurebase/v3/test"
	. "github.com/molecula/featurebase/v3/vprint" // nolint:staticcheck
)

func Test_RandomQuery(t *testing.T) {

	cfg := NewRandomQueryConfig()

	nNodes := 1
	nReplicas := 1

	name := t.Name()
	var nodeid []string
	for i := 0; i < nNodes; i++ {
		// work around a bug in the test.MustRunCluster that corrupts
		// the .topology file if we only join name with one "_" underscore.
		nodeid = append(nodeid, name+"__"+strconv.Itoa(i))
	}

	c := test.MustRunCluster(t, nNodes,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID(nodeid[0]),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
				pilosa.OptServerReplicaN(nReplicas),
			)},
	)
	defer c.Close()

	var nodes []*test.Command
	var dirs []string
	for i := 0; i < nNodes; i++ {
		nd := c.GetNode(i)
		nodes = append(nodes, nd)
		dirs = append(dirs, nd.Server.Holder().Path())
	}
	_ = dirs

	ctx := context.Background()

	indexes := []string{"rick"}
	fieldName := []string{"f"}
	idx := make([]*pilosa.Index, len(indexes))
	field := make([]*pilosa.Field, len(indexes))

	var err error

	for i := range indexes {

		idx[i], err = nodes[0].API.CreateIndex(ctx, indexes[i], pilosa.IndexOptions{Keys: true, TrackExistence: true})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		if idx[i].CreatedAt() == 0 {
			t.Fatal("index createdAt is empty")
		}

		field[i], err = nodes[0].API.CreateField(ctx, indexes[i], fieldName[i], pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}
		if field[i].CreatedAt() == 0 {
			t.Fatal("field createdAt is empty")
		}
	}

	timestamp := int64(0)

	for i := range indexes {

		// Generate some keyed records.
		rowIDs := []uint64{}
		timestamps := []int64{}
		N := 10
		for j := 1; j <= N; j++ {
			rowIDs = append(rowIDs, uint64(j))
			timestamps = append(timestamps, timestamp)
		}

		var colKeys []string
		switch i {
		case 0:
			// Keys are sharded so ordering is not guaranteed.
			colKeys = []string{"col10", "col8", "col9", "col6", "col7", "col4", "col5", "col2", "col3", "col1"}
			colKeys = colKeys[:N]
		case 1:
			colKeys = []string{"col11", "col12"}
			N = len(colKeys)
			rowIDs = rowIDs[:N]
			timestamps = timestamps[:N]
		}

		// Import data with keys to the primary and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
		req := &pilosa.ImportRequest{
			Index:          indexes[i],
			IndexCreatedAt: idx[i].CreatedAt(),
			Field:          fieldName[i],
			FieldCreatedAt: field[i].CreatedAt(),

			// even though this says Shard: 0, that won't matter. The column keys
			// get hashed and that decides the actual shard.
			Shard:      0,
			RowIDs:     rowIDs,
			ColumnKeys: colKeys,
			Timestamps: timestamps,
		}
		//vv("rowIDs = '%#v'", rowIDs)
		//vv("colKeys = '%#v'", colKeys)

		qcx := nodes[0].API.Txf().NewQcx()

		if err := nodes[0].API.Import(ctx, qcx, req); err != nil {
			t.Fatal(err)
		}
		PanicOn(qcx.Finish())
		//qcx.Reset()
	}
	// end of setup.
	cfg.Index = indexes[0]
	PanicOn(cfg.Setup(wrapApiToInternalClient(nodes[0].API)))

	for j := 0; j < 4; j++ {
		index := indexes[rand.Intn(len(indexes))]

		pql, err := cfg.GenQuery(index)
		PanicOn(err)

		//vv("pql = '%v'", pql)

		// Query node0.
		res, err := nodes[0].API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql})
		if err != nil {
			t.Fatal(err)
		}
		_ = res
		//vv("success on pql = '%v'; res='%v'", pql, res.Results[0])
	}
}
