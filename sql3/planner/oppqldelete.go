// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpPQLConstRowDelete plan operator to delete rows from a table based on a single key.
type PlanOpPQLConstRowDelete struct {
	planner   *ExecutionPlanner
	ChildOp   types.PlanOperator
	tableName string
	warnings  []string
}

func NewPlanOpPQLConstRowDelete(p *ExecutionPlanner, tableName string, child types.PlanOperator) *PlanOpPQLConstRowDelete {
	return &PlanOpPQLConstRowDelete{
		planner:   p,
		ChildOp:   child,
		tableName: tableName,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpPQLConstRowDelete) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["child"] = p.ChildOp.Plan()
	result["tableName"] = p.tableName
	return result
}

func (p *PlanOpPQLConstRowDelete) String() string {
	return ""
}

func (p *PlanOpPQLConstRowDelete) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPQLConstRowDelete) Warnings() []string {
	return p.warnings
}

func (p *PlanOpPQLConstRowDelete) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpPQLConstRowDelete) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpPQLConstRowDelete) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	childIter, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}

	return &constRowDeleteRowIter{
		planner:   p.planner,
		childIter: childIter,
		tableName: p.tableName,
	}, nil
}

func (p *PlanOpPQLConstRowDelete) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpPQLConstRowDelete(p.planner, p.tableName, children[0]), nil
}

func (p *PlanOpPQLConstRowDelete) Expressions() []types.PlanExpression {
	// since we have to do const row lookups, we should always reference the _id column in the
	// table we are deleting from

	tbl, err := p.planner.schemaAPI.TableByName(context.Background(), dax.TableName(p.tableName))
	if err != nil {
		return []types.PlanExpression{}
	}

	var colType parser.ExprDataType
	if tbl.StringKeys() {
		colType = parser.NewDataTypeString()
	} else {
		colType = parser.NewDataTypeID()
	}

	return []types.PlanExpression{
		&qualifiedRefPlanExpression{
			tableName:   p.tableName,
			columnIndex: 0,
			dataType:    colType,
			columnName:  "_id",
		},
	}
}

func (p *PlanOpPQLConstRowDelete) WithUpdatedExpressions(exprs ...types.PlanExpression) (types.PlanOperator, error) {
	// just return ourselves
	return p, nil
}

type constRowDeleteRowIter struct {
	planner   *ExecutionPlanner
	childIter types.RowIterator
	tableName string
}

var _ types.RowIterator = (*constRowDeleteRowIter)(nil)

func (i *constRowDeleteRowIter) Next(ctx context.Context) (types.Row, error) {
	var err error

	err = i.planner.checkAccess(ctx, i.tableName, accessTypeWriteData)
	if err != nil {
		return nil, err
	}

	var row []interface{}
	row, err = i.childIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	keys := make([]interface{}, 0)
	for {
		keys = append(keys, row[0])

		row, err = i.childIter.Next(ctx)
		if err == types.ErrNoMoreRows {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	if len(keys) > 0 {
		// this row should contain the key to delete
		cond := &pql.Call{
			Name: "ConstRow",
			Args: map[string]interface{}{
				"columns": keys,
			},
			Type: pql.PrecallGlobal,
		}
		call := &pql.Call{Name: "Delete", Children: []*pql.Call{cond}}

		tbl, err := i.planner.schemaAPI.TableByName(ctx, dax.TableName(i.tableName))
		if err != nil {
			return nil, sql3.NewErrTableNotFound(0, 0, i.tableName)
		}

		_, err = i.planner.executor.Execute(ctx, tbl, &pql.Query{Calls: []*pql.Call{call}}, nil, nil)
		if err != nil {
			return nil, err
		}
	}
	return nil, types.ErrNoMoreRows
}
