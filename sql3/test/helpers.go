// Copyright 2021 Molecula Corp. All rights reserved.
package test

import (
	"context"
	"testing"

	pilosa "github.com/featurebasedb/featurebase/v3"
	planner_types "github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// MustQueryRows returns the row results as a slice of []interface{}, along with the columns.
func MustQueryRows(tb testing.TB, svr *pilosa.Server, q string) ([][]interface{}, []*planner_types.PlannerColumn, error) {
	tb.Helper()

	ctx := context.Background()

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
	if err != nil && err != planner_types.ErrNoMoreRows {
		return nil, nil, err
	}
	for err != planner_types.ErrNoMoreRows {
		result := make([]interface{}, len(ocolumns))
		for i := range result {
			result[i] = next[i]
		}
		results = append(results, result)
		next, err = rowIter.Next(ctx)
		if err != nil && err != planner_types.ErrNoMoreRows {
			return nil, nil, err
		}
	}
	//temporarily transform to Columns()
	cols := make([]*planner_types.PlannerColumn, 0)
	for _, oc := range ocolumns {
		cols = append(cols, &planner_types.PlannerColumn{
			ColumnName: oc.ColumnName,
			Type:       oc.Type,
		})
	}
	return results, cols, nil
}
