package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	fbcontext "github.com/featurebasedb/featurebase/v3/context"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpQuery is a query - this is the root node of an execution plan
type PlanOpQuery struct {
	planner *ExecutionPlanner

	ChildOp types.PlanOperator

	// all the identifiers that are referenced
	referenceList []*qualifiedRefPlanExpression

	sql      string
	warnings []string
}

var _ types.PlanOperator = (*PlanOpQuery)(nil)

func NewPlanOpQuery(p *ExecutionPlanner, child types.PlanOperator, sql string) *PlanOpQuery {
	return &PlanOpQuery{
		planner:  p,
		ChildOp:  child,
		warnings: make([]string, 0),
		sql:      sql,
	}
}

func (p *PlanOpQuery) Schema() types.Schema {
	return p.ChildOp.Schema()
}

func (p *PlanOpQuery) Child() types.PlanOperator {
	return p.ChildOp
}

func (p *PlanOpQuery) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	iter, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}

	return newQueryIterator(p.planner.systemLayerAPI.ExecutionRequests(), p, iter), nil
}

func (p *PlanOpQuery) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpQuery) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	op := NewPlanOpQuery(p.planner, children[0], p.sql)
	op.warnings = append(op.warnings, p.warnings...)
	return op, nil

}

func (p *PlanOpQuery) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = sc

	result["sql"] = p.sql
	result["warnings"] = p.warnings
	result["child"] = p.ChildOp.Plan()
	return result
}

func (p *PlanOpQuery) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpQuery) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	if p.ChildOp != nil {
		w = append(w, p.ChildOp.Warnings()...)
	}
	return w
}

func (p *PlanOpQuery) String() string {
	return ""
}

type queryIterator struct {
	requests pilosa.ExecutionRequestsAPI
	query    *PlanOpQuery

	child types.RowIterator

	hasStarted *struct{}
}

func newQueryIterator(requests pilosa.ExecutionRequestsAPI, query *PlanOpQuery, child types.RowIterator) *queryIterator {
	return &queryIterator{
		requests: requests,
		query:    query,
		child:    child,
	}
}

func (i *queryIterator) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {

		requestId, ok := fbcontext.RequestID(ctx)
		if !ok {
			return nil, sql3.NewErrInternalf("unable to get request id from context")
		}

		userId := ""
		userId, _ = fbcontext.UserID(ctx)

		i.requests.AddRequest(requestId, userId, time.Now(), i.query.sql)
		i.hasStarted = &struct{}{}
	}

	row, err := i.child.Next(ctx)
	if err != nil {
		// either error or no more rows, either way update the request
		requestId, ok := fbcontext.RequestID(ctx)
		if !ok {
			return nil, sql3.NewErrInternalf("unable to get request id from context")
		}

		plan, _ := json.MarshalIndent(i.query.Plan(), "", "    ")
		i.requests.UpdateRequest(requestId, time.Now(), "complete", "", 0, "", 0, 0, 0, 0, 0, string(plan))
	}
	return row, err
}
