package tstore

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/featurebasedb/featurebase/v3/bufferpool"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/stretchr/testify/assert"
)

func buildSample(objectID int32, shard int32) (*BTree, error, func()) {
	diskManager := bufferpool.NewTupleStoreDiskManager()

	dataFile := fmt.Sprintf("ts-shard.%04d", shard)

	diskManager.CreateOrOpenShard(objectID, shard, dataFile)

	bufferPool := bufferpool.NewBufferPool(100, diskManager)

	tableSchema := types.Schema{
		&types.PlannerColumn{
			ColumnName: "vtest",
			Type:       parser.NewDataTypeVarchar(50),
		},
	}
	b, e := NewBTree(8, objectID, shard, tableSchema, bufferPool)
	return b, e, func() {
		os.Remove(dataFile)
	}
}

func TestBTree_RangeIterator(t *testing.T) {
	b, err, c := buildSample(1, 0)
	defer c()
	assert.Nil(t, err)

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
	for i := 1; i <= 90; i++ {
		inserts = append(inserts, i)
	}
	rand.Shuffle(len(inserts), func(i, j int) { inserts[i], inserts[j] = inserts[j], inserts[i] })

	rr := make(types.Row, 2)
	want1 := make([]string, 0)
	want2 := make([]string, 0)
	want3 := make([]string, 0)
	for _, i := range inserts {
		rr[0] = int64(i)
		rr[1] = fmt.Sprintf("This is a test of things %d", i)

		if i >= 1 && i < 10 { // items in first page
			want1 = append(want1, rr[1].(string))
		}

		if i >= 75 && i < 80 { // items in last page
			want2 = append(want2, rr[1].(string))
		}
		if i >= 35 && i < 75 { // items in both pages
			want3 = append(want3, rr[1].(string))
		}

		tup := &BTreeTuple{
			TupleSchema: rowSchema,
			Tuple:       rr,
		}
		err = b.Insert(tup)
		if err != nil {
			t.Fatal(err)
		}
	}

	// b.Dot(os.Stdout, "first", false)
	root, err := b.fetchNode(b.rootNode)
	assert.Nil(t, err)
	t.Run("first page", func(t *testing.T) {
		itr := NewRangeIterator(b, root, Int(1), Int(10), b.schema)
		defer itr.Dispose()
		got := make([]string, 0)
		for itr.Next() {
			item, _ := itr.Item()
			got = append(got, item.Tuple[0].(string))
		}
		sort.Strings(got)
		sort.Strings(want1)
		assert.Equal(t, want1, got)
	})
	t.Run("last page", func(t *testing.T) {
		itr := NewRangeIterator(b, root, Int(75), Int(80), b.schema)
		defer itr.Dispose()
		got := make([]string, 0)
		for itr.Next() {
			item, _ := itr.Item()
			got = append(got, item.Tuple[0].(string))
		}
		sort.Strings(got)
		sort.Strings(want2)
		assert.Equal(t, want2, got)
	})
	t.Run("cover pages", func(t *testing.T) {
		itr := NewRangeIterator(b, root, Int(35), Int(75), b.schema)
		defer itr.Dispose()
		got := make([]string, 0)
		for itr.Next() {
			item, _ := itr.Item()
			got = append(got, item.Tuple[0].(string))
		}
		sort.Strings(got)
		sort.Strings(want3)
		assert.Equal(t, want3, got)
	})
}

func TestDotOverflow(t *testing.T) {
	t.Skip("need to figure out fail")
	objectId := int32(1)
	shard := int32(0)
	dot, err := OpenBtree(fmt.Sprintf("ts-shard.%04d", shard))
	assert.NotNil(t, err)
	dot.Dot(os.Stdout, "next", true)

	diskManager := bufferpool.NewTupleStoreDiskManager()

	dataFile := fmt.Sprintf("ts-shard.%04d", shard)
	defer os.Remove(dataFile)

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
