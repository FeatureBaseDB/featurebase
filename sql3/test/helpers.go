// Copyright 2021 Molecula Corp. All rights reserved.
package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	fbcontext "github.com/featurebasedb/featurebase/v3/context"
	"github.com/featurebasedb/featurebase/v3/dax"
	plannertypes "github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	uuid "github.com/satori/go.uuid"
)

// MustQueryRows returns the row results as a slice of []interface{}, along with the columns, the query plan as a []byte or an error.
func MustQueryRows(tb testing.TB, svr *featurebase.Server, c context.Context, q string) ([][]interface{}, []*featurebase.WireQueryField, []byte, error) {
	tb.Helper()
	requestId, err := uuid.NewV4()
	if err != nil {
		return nil, nil, nil, err
	}

	// Originally MustQueryRows just created a context for itself do test with.
	// However, for some tests, we may want access to the test's context so that,
	// for example, we can cancel the context mid-test and make sure that gets
	// handled correctly. Since we need to be able to cancel the context before
	// the query finishes, if a context is set we also introduce a delay.
	var ctx context.Context
	delay := 0 * time.Millisecond
	if c == nil {
		ctx = fbcontext.WithRequestID(context.Background(), requestId.String())
	} else {
		ctx = fbcontext.WithRequestID(c, requestId.String())
		delay = 100 * time.Millisecond
	}

	stmt, err := svr.CompileExecutionPlan(ctx, q)
	if err != nil {
		return nil, nil, nil, err
	}

	// get the plan so that code runs during testing
	plan := stmt.Plan()
	bplan, err := json.MarshalIndent(plan, "", "    ")
	if err != nil {
		return nil, nil, nil, err
	}

	ocolumns := stmt.Schema()

	rowIter, err := stmt.Iterator(ctx, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	results := make([][]interface{}, 0)

	// figuring out where to put the sleep took some trial and error.
	// Too early and the context gets cancelled before the query can
	// start running, too late and you end up with the query getting
	// cancelled instead of the context.
	time.Sleep(delay)

	next, err := rowIter.Next(ctx)
	if err != nil && err != plannertypes.ErrNoMoreRows {
		return nil, nil, nil, err
	}

	for err != plannertypes.ErrNoMoreRows {
		result := make([]interface{}, len(ocolumns))
		for i := range result {
			result[i] = next[i]
		}
		results = append(results, result)
		next, err = rowIter.Next(ctx)
		if err != nil && err != plannertypes.ErrNoMoreRows {
			return nil, nil, nil, err
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
	return results, cols, bplan, nil
}
