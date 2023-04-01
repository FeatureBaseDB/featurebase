package tstore

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/featurebasedb/featurebase/v3/bufferpool"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/vprint"
	"github.com/stretchr/testify/assert"
)

func TestBTree_Dot(t *testing.T) {
	diskManager := bufferpool.NewTupleStoreDiskManager()

	objectId := int32(1)
	shard := int32(0)
	dataFile := fmt.Sprintf("ts-shard.%04d", shard)

	defer os.Remove(dataFile)

	diskManager.CreateOrOpenShard(objectId, shard, dataFile)

	bufferPool := bufferpool.NewBufferPool(100, diskManager)

	tableSchema := types.Schema{
		&types.PlannerColumn{
			ColumnName: "vtest",
			Type:       parser.NewDataTypeVarchar(50),
		},
	}

	b, err := NewBTree(8, objectId, shard, tableSchema, bufferPool)
	if err != nil {
		t.Fatal(err)
	}

	rowSchema := types.Schema{
		&types.PlannerColumn{
			ColumnName: "_id",
			Type:       parser.NewDataTypeID(),
		},
		&types.PlannerColumn{
			ColumnName: "vtest",
			Type:       parser.NewDataTypeVarchar(50),
		},
	}

	inserts := make([]int, 0)
	for i := 1; i <= 300; i++ {
		inserts = append(inserts, i)
	}
	rand.Shuffle(len(inserts), func(i, j int) { inserts[i], inserts[j] = inserts[j], inserts[i] })

	rr := make(types.Row, 2)
	for _, i := range inserts {
		rr[0] = int64(i)
		rr[1] = fmt.Sprintf("This is a test of things %d", i)

		tup := &BTreeTuple{
			TupleSchema: rowSchema,
			Tuple:       rr,
		}
		err = b.Insert(tup)
		if err != nil {
			t.Fatal(err)
		}
	}

	b.Dot(os.Stdout, "first", false)
	root, err := b.fetchNode(b.rootNode)
	assert.NotNil(t, err)
	itr := NewBTreeIterator(b, root, Int(1), Int(100), b.schema)
	assert.NotNil(t, err)
	// k, tupe, err := itr.Next()
	for itr.Next() {
		item := itr.Item()
		vprint.VV("item:%v", item)
	}
}

func TestDotOverflow(t *testing.T) {
	objectId := int32(1)
	shard := int32(0)
	dot, err := OpenBtree(fmt.Sprintf("ts-shard.%04d", shard))
	assert.NotNil(t, err)
	dot.Dot(os.Stdout, "next", true)

	diskManager := bufferpool.NewTupleStoreDiskManager()

	dataFile := fmt.Sprintf("ts-shard.%04d", shard)
	//	os.Remove(dataFile)

	diskManager.CreateOrOpenShard(objectId, shard, dataFile)
	bufferPool := bufferpool.NewBufferPool(100, diskManager)

	tableSchema := make(types.Schema, 0)

	numCols := 100
	numRecs := 10

	// build schema
	for i := 0; i < numCols; i++ {
		tableSchema = append(tableSchema, &types.PlannerColumn{
			ColumnName: fmt.Sprintf("vtest%d", i+1),
			Type:       parser.NewDataTypeVarchar(4),
		})
	}

	b, err := NewBTree(8, objectId, shard, tableSchema, bufferPool)
	_ = b
	_ = numRecs
	if err != nil {
		t.Fatal(err)
	}
	rowSchema := make(types.Schema, 0)

	rowSchema = append(rowSchema, &types.PlannerColumn{
		ColumnName: "_id",
		Type:       parser.NewDataTypeID(),
	})

	for i := 0; i < numCols; i++ {
		rowSchema = append(rowSchema, &types.PlannerColumn{
			ColumnName: fmt.Sprintf("vtest%d", i+1),
			Type:       parser.NewDataTypeVarchar(4),
		})
	}

	inserts := make([]int, 0)

	for i := 1; i <= numRecs; i++ {
		inserts = append(inserts, i)
	}

	rr := make(types.Row, numCols+1)

	for _, i := range inserts {
		rr[0] = int64(i)

		for j := 0; j < numCols; j++ {
			rr[j+1] = fmt.Sprintf("%04d", j)
		}

		tup := &BTreeTuple{
			TupleSchema: rowSchema,
			Tuple:       rr,
		}

		// fmt.Printf("[%d]row key %v\n\n", j, i)

		err = b.Insert(tup)
		if err != nil {
			t.Fatal(err)
		}

	}

	// b.Dump(0)
	b.Dot(os.Stdout, "first", true)
}
