// Copyright 2021 Molecula Corp. All rights reserved.
package test

import (
	"context"
	"testing"

	featurebase "github.com/featurebasedb/featurebase/v3"
	fbcontext "github.com/featurebasedb/featurebase/v3/context"
	"github.com/featurebasedb/featurebase/v3/dax"
	plannertypes "github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	uuid "github.com/satori/go.uuid"
)

// MustQueryRows returns the row results as a slice of []interface{}, along with the columns.
func MustQueryRows(tb testing.TB, svr *featurebase.Server, q string) ([][]interface{}, []*featurebase.WireQueryField, error) {
	tb.Helper()
	requestId, err := uuid.NewV4()
	if err != nil {
		return nil, nil, err
	}

	ctx := fbcontext.WithRequestID(context.Background(), requestId.String())

	stmt, err := svr.CompileExecutionPlan(ctx, q)
	if err != nil {
		return nil, nil, err
	}

	// get the plan so that code runs during testing
	_ = stmt.Plan()

	ocolumns := stmt.Schema()

	rowIter, err := stmt.Iterator(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	results := make([][]interface{}, 0)

	next, err := rowIter.Next(ctx)
	if err != nil && err != plannertypes.ErrNoMoreRows {
		return nil, nil, err
	}
	for err != plannertypes.ErrNoMoreRows {
		result := make([]interface{}, len(ocolumns))
		for i := range result {
			result[i] = next[i]
		}
		results = append(results, result)
		next, err = rowIter.Next(ctx)
		if err != nil && err != plannertypes.ErrNoMoreRows {
			return nil, nil, err
		}
	}
	// temporarily transform to Columns()
	cols := make([]*featurebase.WireQueryField, 0)
	for _, oc := range ocolumns {
		cols = append(cols, &featurebase.WireQueryField{
			Name:     dax.FieldName(oc.ColumnName),
			Type:     oc.Type.TypeDescription(),
			BaseType: dax.BaseType(oc.Type.BaseTypeName()),
			TypeInfo: oc.Type.TypeInfo(),
		})
	}
	return results, cols, nil
}
