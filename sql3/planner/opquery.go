package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpQuery is a query - this is the root node of an execution plan
type PlanOpQuery struct {
	ChildOp types.PlanOperator

	// the list of aggregate terms
	aggregates []types.PlanExpression

	// all the identifiers that are referenced
	referenceList []*qualifiedRefPlanExpression

	sql      string
	warnings []string
}

var _ types.PlanOperator = (*PlanOpQuery)(nil)

func NewPlanOpQuery(child types.PlanOperator, sql string) *PlanOpQuery {
	return &PlanOpQuery{
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
	return p.Child().Iterator(ctx, row)
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
	op := NewPlanOpQuery(children[0], p.sql)
	op.warnings = append(op.warnings, p.warnings...)
	return op, nil

}

func (p *PlanOpQuery) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeName()))
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
