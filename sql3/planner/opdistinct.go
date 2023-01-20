// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"bytes"
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/bufferpool"
	"github.com/featurebasedb/featurebase/v3/extendiblehash"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpDistinct plan operator handles DISTINCT
// DISTINCT returns unique rows from its iterator and does this by
// creating a hash table and probing new rows against that hash table,
// if the row has already been seen, it is skipped, it it has not been
// seen, a 'key' is created from all the values in the row and this is
// inserted into the hash table.
// The hash table is implemented using Extendible Hashing and is backed
// by a buffer pool. The buffer pool is allocated to 128 pages (or 1Mb)
// and the disk manager used by the buffer pool will use an in-memory
// implementation up to 128 pages and thereafter spill to disk
type PlanOpDistinct struct {
	planner  *ExecutionPlanner
	ChildOp  types.PlanOperator
	warnings []string
}

func NewPlanOpDistinct(p *ExecutionPlanner, child types.PlanOperator) *PlanOpDistinct {
	return &PlanOpDistinct{
		planner:  p,
		ChildOp:  child,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpDistinct) Schema() types.Schema {
	return p.ChildOp.Schema()
}

func (p *PlanOpDistinct) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	i, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}
	return newDistinctIterator(p.Schema(), i), nil
}

func (p *PlanOpDistinct) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpDistinct(p.planner, children[0]), nil
}

func (p *PlanOpDistinct) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpDistinct) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["child"] = p.ChildOp.Plan()
	return result
}

func (p *PlanOpDistinct) String() string {
	return ""
}

func (p *PlanOpDistinct) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpDistinct) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	w = append(w, p.ChildOp.Warnings()...)
	return w
}

type distinctIterator struct {
	child      types.RowIterator
	schema     types.Schema
	hasStarted *struct{}
	hashTable  *extendiblehash.ExtendibleHashTable
}

func newDistinctIterator(schema types.Schema, child types.RowIterator) *distinctIterator {
	return &distinctIterator{
		schema: schema,
		child:  child,
	}
}

func (i *distinctIterator) rowSeen(ctx context.Context, row types.Row) (bool, error) {
	keyBytes := generateRowKey(row)
	_, found, err := i.hashTable.Get(keyBytes)
	if err != nil {
		return false, nil
	}
	// put the row in the hash table to recored that we've seen it
	if !found {
		i.hashTable.Put(keyBytes, []byte{1})
	}
	return found, nil
}

func (i *distinctIterator) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {
		//create the hashtable

		// ask the diskmanager to spill after 1Mb (128 8K pages)
		diskManager := bufferpool.NewInMemDiskSpillingDiskManager(128)
		// use 1Mb (128 8K pages)
		bufferPool := bufferpool.NewBufferPool(128, diskManager)

		// we're going to use something pretty conservative here - we could have sets, strings, ... all sorts of stuff
		// and we don't yet support long key overflowing
		keyLength := 128 // bytes

		valueLength := 1 // we're going to store a 1 (byte) for every key in the table

		ht, err := extendiblehash.NewExtendibleHashTable(keyLength, valueLength, bufferPool)
		if err != nil {
			return nil, err
		}
		i.hashTable = ht
		i.hasStarted = &struct{}{}
	}

	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			// clean up
			// TODO(pok) - we need to move clean up to higher level, and
			// implement at the operator level
			if err == types.ErrNoMoreRows {
				i.hashTable.Close()
			}
			return nil, err
		}
		// does row exist in hash table
		seen, err := i.rowSeen(ctx, row)
		if err != nil {
			return nil, err
		}
		// if we've seen it before, go to the next row
		if seen {
			continue
		}
		return row, nil
	}
}

func generateRowKey(row types.Row) []byte {
	var buf bytes.Buffer
	for _, v := range row {
		buf.WriteString(fmt.Sprintf("%#v", v))
	}
	return buf.Bytes()
}
