package planner

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/stretchr/testify/assert"
)

func TestExpressions(t *testing.T) {
	t.Run("StringerTest", func(t *testing.T) {
		uop := newUnaryOpPlanExpression(parser.PLUS, newIntLiteralPlanExpression(10), parser.NewDataTypeInt())
		assert.Equal(t, uop.String(), "+10")

		uop1 := newUnaryOpPlanExpression(parser.PLUS, newIntLiteralPlanExpression(10), parser.NewDataTypeID())
		assert.Equal(t, uop1.String(), "+10")

		uop2 := newUnaryOpPlanExpression(parser.MINUS, newIntLiteralPlanExpression(10), parser.NewDataTypeID())
		assert.Equal(t, uop2.String(), "-10")

		bop := newBinOpPlanExpression(newIntLiteralPlanExpression(10), parser.PLUS, newIntLiteralPlanExpression(20), parser.NewDataTypeInt())
		assert.Equal(t, bop.String(), "10+20")

		rop := newRangeOpPlanExpression(newIntLiteralPlanExpression(10), newIntLiteralPlanExpression(20), parser.NewDataTypeInt())
		assert.Equal(t, rop.String(), "between 10 and 20")

		cop := newCasePlanExpression(newStringLiteralPlanExpression("foo"),
			[]types.PlanExpression{
				newCaseBlockPlanExpression(newStringLiteralPlanExpression("20"), newIntLiteralPlanExpression(20)),
				newCaseBlockPlanExpression(newStringLiteralPlanExpression("30"), newIntLiteralPlanExpression(20)),
			},
			newIntLiteralPlanExpression(20), parser.NewDataTypeInt())
		assert.Equal(t, cop.String(), "case 'foo' when '20' then 20 end when '30' then 20 end else 20 end")

		bwop := newBetweenOpPlanExpression(newIntLiteralPlanExpression(10), parser.BETWEEN, newIntLiteralPlanExpression(20))
		assert.Equal(t, bwop.String(), "between 10 and 20")

		iop := newInOpPlanExpression(newIntLiteralPlanExpression(10), parser.IN, newIntLiteralPlanExpression(20))
		assert.Equal(t, iop.String(), "10 in (20)")

		callop := newCallPlanExpression("foo", []types.PlanExpression{newIntLiteralPlanExpression(10)}, parser.NewDataTypeInt(), nil)
		assert.Equal(t, callop.String(), "foo(10)")

		alop := newAliasPlanExpression("frobny", newIntLiteralPlanExpression(10))
		assert.Equal(t, alop.String(), "10 as frobny")

		qrop := newQualifiedRefPlanExpression("foo", "bar", 1, parser.NewDataTypeInt())
		assert.Equal(t, qrop.String(), "foo.bar")

		vop := newVariableRefPlanExpression("foo", 1, parser.NewDataTypeInt())
		assert.Equal(t, vop.String(), "@foo")

		nulop := newNullLiteralPlanExpression()
		assert.Equal(t, nulop.String(), "null")

		ilop := newIntLiteralPlanExpression(10)
		assert.Equal(t, ilop.String(), "10")

		flop := newFloatLiteralPlanExpression("12.3456")
		assert.Equal(t, flop.String(), "12.3456")

		blop := newBoolLiteralPlanExpression(false)
		assert.Equal(t, blop.String(), "false")

		tm, _ := parser.ConvertStringToTimestamp("2012-11-01T22:08:41+00:00")
		dlop := newTimestampLiteralPlanExpression(tm)
		assert.Equal(t, dlop.String(), "2012-11-01T22:08:41Z")

		slop := newStringLiteralPlanExpression("foo")
		assert.Equal(t, slop.String(), "'foo'")

		ctop := newCastPlanExpression(newIntLiteralPlanExpression(10), parser.NewDataTypeString())
		assert.Equal(t, ctop.String(), "cast(10 as string)")

		elop := newExprListExpression([]types.PlanExpression{newStringLiteralPlanExpression("foo"), newStringLiteralPlanExpression("bar")})
		assert.Equal(t, elop.String(), "('foo', 'bar')")

		stlop := newExprSetLiteralPlanExpression([]types.PlanExpression{newStringLiteralPlanExpression("foo"), newStringLiteralPlanExpression("bar")}, parser.NewDataTypeString())
		assert.Equal(t, stlop.String(), "['foo', 'bar']")

		tplop := newExprTupleLiteralPlanExpression([]types.PlanExpression{newStringLiteralPlanExpression("foo"), newStringLiteralPlanExpression("bar")}, parser.NewDataTypeString())
		assert.Equal(t, tplop.String(), "{'foo', 'bar'}")
	})
}
