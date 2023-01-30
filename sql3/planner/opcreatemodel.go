// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	uuid "github.com/satori/go.uuid"
)

// PlanOpCreateModel implements the CREATE MODEL operator
type PlanOpCreateModel struct {
	ChildOp  types.PlanOperator
	planner  *ExecutionPlanner
	model    *modelSystemObject
	warnings []string
}

func NewPlanOpCreateModel(planner *ExecutionPlanner, model *modelSystemObject, child types.PlanOperator) *PlanOpCreateModel {
	return &PlanOpCreateModel{
		ChildOp:  child,
		planner:  planner,
		model:    model,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpCreateModel) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpCreateModel) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	// get the query iterator
	iter, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(p.model.modelType) {
	case "linear_regresssion":
		return newCreateModelIter(p.planner, p.model, newLinearRegressionModelIter(p.planner, p.model, p.ChildOp.Schema(), iter)), nil

	default:
		return nil, sql3.NewErrInternalf("unexpected model tyoe '%s'", p.model.modelType)
	}
}

func (p *PlanOpCreateModel) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpCreateModel) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpCreateModel(p.planner, p.model, children[0]), nil
}

func (p *PlanOpCreateModel) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = sc
	result["model"] = p.model.name // TODO(pok) - add a Plan() method here (or some such)
	result["child"] = p.ChildOp.Plan()
	return result
}

func (p *PlanOpCreateModel) String() string {
	return ""
}

func (p *PlanOpCreateModel) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpCreateModel) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	w = append(w, p.ChildOp.Warnings()...)
	return w
}

type createModelIter struct {
	child      types.RowIterator
	planner    *ExecutionPlanner
	model      *modelSystemObject
	hasStarted *struct{}
}

func newCreateModelIter(planner *ExecutionPlanner, model *modelSystemObject, child types.RowIterator) *createModelIter {
	return &createModelIter{
		planner: planner,
		model:   model,
		child:   child,
	}
}

func (i *createModelIter) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {
		// store the model into fb_models and set the model status to 'training'
		i.model.status = "TRAINING"
		err := i.planner.insertModel(i.model)
		if err != nil {
			return nil, err
		}

		// do the actual training
		_, err = i.child.Next(ctx)
		if err != nil && err != types.ErrNoMoreRows {
			return nil, err
		}

		// update the model to ready
		i.model.status = "READY"
		err = i.planner.updateModel(i.model)
		if err != nil {
			return nil, err
		}
		i.hasStarted = &struct{}{}
	}
	return nil, types.ErrNoMoreRows
}

type linearRegressionModelIter struct {
	child       types.RowIterator
	planner     *ExecutionPlanner
	model       *modelSystemObject
	childSchema types.Schema
	hasStarted  *struct{}
}

func newLinearRegressionModelIter(planner *ExecutionPlanner, model *modelSystemObject, childSchema types.Schema, child types.RowIterator) *linearRegressionModelIter {
	return &linearRegressionModelIter{
		planner:     planner,
		model:       model,
		childSchema: childSchema,
		child:       child,
	}
}

func (i *linearRegressionModelIter) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {
		// this is linear regression now, so we actually 'train' when we predict (later)
		// for now we just store the values from the query in fb_model_data

		// delete anything from fb_model_data for this model
		err := i.planner.ensureModelDataSystemTableExists()
		if err != nil {
			return nil, err
		}
		diter := &filteredDeleteRowIter{
			planner:   i.planner,
			tableName: "fb_model_data",
			filter: newBinOpPlanExpression(
				newQualifiedRefPlanExpression("fb_model_data", "model_id", 0, parser.NewDataTypeString()),
				parser.EQ,
				newStringLiteralPlanExpression(i.model.name),
				parser.NewDataTypeBool(),
			),
		}
		_, err = diter.Next(context.Background())
		if err != nil && err != types.ErrNoMoreRows {
			return nil, err
		}

		iter := &insertRowIter{
			planner:   i.planner,
			tableName: "fb_model_data",
			targetColumns: []*qualifiedRefPlanExpression{
				newQualifiedRefPlanExpression("fb_model_data", "_id", 0, parser.NewDataTypeString()),
				newQualifiedRefPlanExpression("fb_model_data", "model_id", 0, parser.NewDataTypeString()),
				newQualifiedRefPlanExpression("fb_model_data", "data", 0, parser.NewDataTypeString()),
			},
			insertValues: [][]types.PlanExpression{},
		}

		trainingRefs := make([]*qualifiedRefPlanExpression, 0)

		// make sure label column exists and is type compatible with float
		labelColumn := i.model.labels[0]
		found := false
		for i, s := range i.childSchema {
			if strings.EqualFold(labelColumn, s.ColumnName) {
				if !typesAreAssignmentCompatible(parser.NewDataTypeDecimal(4), s.Type) {
					return nil, sql3.NewErrInternalf("types not assignment compatible")
				}
				trainingRefs = append(trainingRefs, newQualifiedRefPlanExpression("", labelColumn, i, s.Type))
				found = true
				break
			}
		}
		if !found {
			return nil, sql3.NewErrInternalf("label column found found")
		}

		// make sure input columns exists and are type compatible with float
		for _, ic := range i.model.inputColumns {
			found := false
			for i, s := range i.childSchema {
				if strings.EqualFold(ic, s.ColumnName) {
					if !typesAreAssignmentCompatible(parser.NewDataTypeDecimal(4), s.Type) {
						return nil, sql3.NewErrInternalf("types not assignment compatible")
					}
					trainingRefs = append(trainingRefs, newQualifiedRefPlanExpression("", ic, i, s.Type))
					found = true
					break
				}
			}
			if !found {
				return nil, sql3.NewErrInternalf("input column found found")
			}
		}

		// go run the query and iterate
		for {
			row, err := i.child.Next(ctx)
			if err != nil {
				if err == types.ErrNoMoreRows {
					break
				}
				return nil, err
			}

			fdata := make([]float64, 0)

			for _, ref := range trainingRefs {
				val, err := ref.Evaluate(row)
				if err != nil {
					return nil, err
				}
				cval, err := coerceValue(ref.dataType, parser.NewDataTypeDecimal(4), val, parser.Pos{Line: 0, Column: 0})
				if err != nil {
					return nil, err
				}
				dval := cval.(pql.Decimal)
				fdata = append(fdata, dval.Float64())
			}

			data, err := json.Marshal(fdata)
			if err != nil {
				return nil, err
			}

			rowID, err := uuid.NewV4()
			if err != nil {
				return nil, err
			}
			tuple := []types.PlanExpression{
				newStringLiteralPlanExpression(rowID.String()),
				newStringLiteralPlanExpression(i.model.name),
				newStringLiteralPlanExpression(string(data)),
			}
			iter.insertValues = append(iter.insertValues, tuple)

			fmt.Printf("%v", row)

		}
		_, err = iter.Next(context.Background())
		if err != nil && err != types.ErrNoMoreRows {
			return nil, err
		}

		i.hasStarted = &struct{}{}
	}
	return nil, types.ErrNoMoreRows
}
