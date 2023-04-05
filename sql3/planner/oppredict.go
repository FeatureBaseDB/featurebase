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
	"github.com/sajari/regression"
)

// PlanOpPredict is an operator for a PREDICT
type PlanOpPredict struct {
	ChildOp  types.PlanOperator
	planner  *ExecutionPlanner
	model    *modelSystemObject
	warnings []string
}

func NewPlanOpPredict(planner *ExecutionPlanner, model *modelSystemObject, child types.PlanOperator) *PlanOpPredict {
	return &PlanOpPredict{
		ChildOp:  child,
		planner:  planner,
		model:    model,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpPredict) Schema() types.Schema {
	result := make(types.Schema, 0)

	switch strings.ToLower(p.model.modelType) {
	case "linear_regresssion":
		labelName := p.model.labels[0]

		result = append(result, &types.PlannerColumn{
			ColumnName:   fmt.Sprintf("predicted_%s", labelName),
			RelationName: "",
			AliasName:    "",
			// we need to get this type from somewhere...probably needs to be stored in the model def
			Type: &parser.DataTypeDecimal{
				Scale: 4,
			},
		})

	default:
		// don't add anything
	}
	// add the columns from the select
	result = append(result, p.ChildOp.Schema()...)

	return result
}

func (p *PlanOpPredict) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	// get the query iterator
	iter, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(p.model.modelType) {
	case "linear_regresssion":
		return newLinearRegressionPredictIter(p.planner, p.model, p.ChildOp.Schema(), iter), nil

	default:
		return nil, sql3.NewErrInternalf("unexpected model tyoe '%s'", p.model.modelType)
	}
}

func (p *PlanOpPredict) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpPredict) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpPredict(p.planner, p.model, children[0]), nil
}

func (p *PlanOpPredict) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = sc
	result["model"] = p.model.name
	result["child"] = p.ChildOp.Plan()
	return result
}

func (p *PlanOpPredict) String() string {
	return ""
}

func (p *PlanOpPredict) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPredict) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	if p.ChildOp != nil {
		w = append(w, p.ChildOp.Warnings()...)
	}
	return w
}

type linearRegressionPredictIter struct {
	child         types.RowIterator
	planner       *ExecutionPlanner
	model         *modelSystemObject
	regres        *regression.Regression
	childSchema   types.Schema
	inferenceRefs []*qualifiedRefPlanExpression
	hasStarted    *struct{}
}

func newLinearRegressionPredictIter(planner *ExecutionPlanner, model *modelSystemObject, childSchema types.Schema, child types.RowIterator) *linearRegressionPredictIter {
	return &linearRegressionPredictIter{
		planner:       planner,
		model:         model,
		child:         child,
		childSchema:   childSchema,
		regres:        new(regression.Regression),
		inferenceRefs: make([]*qualifiedRefPlanExpression, 0),
	}
}

func (i *linearRegressionPredictIter) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {
		// label column
		i.regres.SetObserved("Murders per annum per 1,000,000 inhabitants")
		// input columns
		i.regres.SetVar(0, "Inhabitants")
		i.regres.SetVar(1, "Percent with incomes below $5000")
		i.regres.SetVar(2, "Percent unemployed")

		// go get the 'training set' from fb_model_data
		iter := &tableScanRowIter{
			planner:   i.planner,
			tableName: "fb_model_data",
			columns: []string{
				"_id",
				"model_id",
				"data",
			},
			predicate: newBinOpPlanExpression(
				newQualifiedRefPlanExpression("fb_model_data", "model_id", 0, parser.NewDataTypeString()),
				parser.EQ,
				newStringLiteralPlanExpression(i.model.name),
				parser.NewDataTypeBool(),
			),
		}

		for {
			row, err := iter.Next(context.Background())
			if err != nil {
				if err == types.ErrNoMoreRows {
					break
				}
				return nil, err
			}
			fdata := make([]float64, 0)
			err = json.Unmarshal([]byte(row[2].(string)), &fdata)
			if err != nil {
				return nil, err
			}
			label := fdata[0]
			vars := fdata[1:]
			i.regres.Train(regression.DataPoint(label, vars))
		}

		// run the regression
		err := i.regres.Run()
		if err != nil {
			return nil, err
		}

		fmt.Printf("Regression formula:\n%v\n", i.regres.Formula)
		fmt.Printf("Regression:\n%s\n", i.regres)

		for _, ic := range i.model.inputColumns {
			found := false
			for j, s := range i.childSchema {
				if strings.EqualFold(ic, s.ColumnName) {
					if !typesAreAssignmentCompatible(parser.NewDataTypeDecimal(4), s.Type) {
						return nil, sql3.NewErrInternalf("types not assignment compatible")
					}
					i.inferenceRefs = append(i.inferenceRefs, newQualifiedRefPlanExpression("", ic, j, s.Type))
					found = true
					break
				}
			}
			if !found {
				return nil, sql3.NewErrInternalf("input column found found")
			}
		}

		i.hasStarted = &struct{}{}
	}

	childrow, err := i.child.Next(ctx)
	if err != nil {
		return nil, err
	}

	// construct the inference data
	inferenceData := make([]float64, len(i.inferenceRefs))
	for j, ref := range i.inferenceRefs {

		val, err := ref.Evaluate(childrow)
		if err != nil {
			return nil, err
		}
		cval, err := coerceValue(ref.dataType, parser.NewDataTypeDecimal(4), val, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}
		dval := cval.(pql.Decimal)
		inferenceData[j] = dval.Float64()
	}

	// do the prediction
	prediction, err := i.regres.Predict(inferenceData)
	if err != nil {
		return nil, err
	}

	// turn the predition into a decimal
	dprediction, err := pql.FromFloat64WithScale(prediction, 4)
	if err != nil {
		return nil, err
	}

	// make an output row
	row := make(types.Row, len(childrow)+1)
	row[0] = dprediction
	copy(row[1:], childrow)

	return row, nil
}
