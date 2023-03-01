package tstore

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/featurebasedb/featurebase/v3/bufferpool"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

func TestAddItemsToBTreeAndValidate(t *testing.T) {
	diskManager := bufferpool.NewOnDiskDiskManager()

	objectId := int32(1)
	shard := int32(0)
	dataFile := fmt.Sprintf("ts-shard.%04d", shard)

	os.Remove(dataFile)

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

		// fmt.Printf("%v", tup)

		err = b.Insert(tup)
		if err != nil {
			t.Fatal(err)
		}
	}

	key, tuple := b.Search(nil, Int(33))

	fmt.Printf("%v, %v\n\n", key, tuple)

	b.Dump(0)
}

func TestAddItemsToBTreeAndValidate_VeryWide(t *testing.T) {
	diskManager := bufferpool.NewOnDiskDiskManager()

	objectId := int32(1)
	shard := int32(0)
	dataFile := fmt.Sprintf("ts-shard.%04d", shard)

	os.Remove(dataFile)

	diskManager.CreateOrOpenShard(objectId, shard, dataFile)

	bufferPool := bufferpool.NewBufferPool(100, diskManager)

	tableSchema := make(types.Schema, 0)

	for i := 0; i < 3000; i++ {
		tableSchema = append(tableSchema, &types.PlannerColumn{
			ColumnName: fmt.Sprintf("vtest%d", i+1),
			Type:       parser.NewDataTypeVarchar(4),
		})
	}

	b, err := NewBTree(8, objectId, shard, tableSchema, bufferPool)
	if err != nil {
		t.Fatal(err)
	}

	rowSchema := make(types.Schema, 0)
	rowSchema = append(rowSchema, &types.PlannerColumn{
		ColumnName: "_id",
		Type:       parser.NewDataTypeID(),
	})
	for i := 0; i < 3000; i++ {
		rowSchema = append(rowSchema, &types.PlannerColumn{
			ColumnName: fmt.Sprintf("vtest%d", i+1),
			Type:       parser.NewDataTypeVarchar(4),
		})
	}

	inserts := make([]int, 0)
	for i := 1; i <= 300; i++ {
		inserts = append(inserts, i)
	}
	rand.Seed(10)
	rand.Shuffle(len(inserts), func(i, j int) { inserts[i], inserts[j] = inserts[j], inserts[i] })

	rr := make(types.Row, 3001)
	for j, i := range inserts {
		rr[0] = int64(i)

		for j := 0; j < 3000; j++ {
			rr[j+1] = fmt.Sprintf("%04d", j)
		}

		tup := &BTreeTuple{
			TupleSchema: rowSchema,
			Tuple:       rr,
		}

		fmt.Printf("[%d]row key %v\n\n", j, i)

		if j == 17 {
			fmt.Printf("here\n")
		}

		err = b.Insert(tup)
		if err != nil {
			t.Fatal(err)
		}

		b.Dump(0)
		fmt.Printf("\n\n--------------\n\n")
	}

	key, tuple := b.Search(nil, Int(33))

	fmt.Printf("%v, %v\n\n", key, tuple)

	b.Dump(0)
}
