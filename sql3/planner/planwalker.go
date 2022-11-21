package planner

import (
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanVisitor visits nodes in the plan.
type PlanVisitor interface {
	// VisitOperator method is invoked for each node during PlanWalk. If the
	// resulting PlanVisitor is not nil, PlanWalk visits each of the children of
	// the node with that visitor, followed by a call of VisitOperator(nil) to
	// the returned visitor.
	VisitOperator(op types.PlanOperator) PlanVisitor
}

// PlanWalk traverses the plan depth-first. It starts by calling
// v.VisitOperator; node must not be nil. If the result returned by
// v.VisitOperator is not nil, PlanWalk is invoked recursively with the returned
// result for each of the children of the plan operator, followed by a call of
// v.VisitOperator(nil) to the returned result. If v.VisitOperator(op) returns
// non-nil, then all children are walked, even if one of them returns nil.
func PlanWalk(v PlanVisitor, op types.PlanOperator) {
	if v = v.VisitOperator(op); v == nil {
		return
	}

	for _, child := range op.Children() {
		PlanWalk(v, child)
	}

	v.VisitOperator(nil)
}

type planInspectionFunction func(types.PlanOperator) bool

func (f planInspectionFunction) VisitOperator(op types.PlanOperator) PlanVisitor {
	if f(op) {
		return f
	}
	return nil
}

// InspectPlan traverses the plan op graph depth-first order
// if f(op) returns true, InspectPlan invokes f recursively for each of the children of op,
// followed by a call of f(nil).
func InspectPlan(op types.PlanOperator, f planInspectionFunction) {
	PlanWalk(f, op)
}

//-----------------------------------------------------------------------------

// ExprVisitor visits expressions in an expression tree.
type ExprVisitor interface {
	// VisitExpr method is invoked for each expr encountered by ExprWalk.
	// If the result is not nil, ExprWalk visits each of the children
	// of the expr, followed by a call of VisitExpr(nil) to the returned result.
	VisitExpr(expr types.PlanExpression) ExprVisitor
}

func ExprWalk(v ExprVisitor, expr types.PlanExpression) {
	if v = v.VisitExpr(expr); v == nil {
		return
	}

	for _, child := range expr.Children() {
		ExprWalk(v, child)
	}

	v.VisitExpr(nil)
}

type exprInspector func(types.PlanExpression) bool

func (f exprInspector) VisitExpr(e types.PlanExpression) ExprVisitor {
	if f(e) {
		return f
	}
	return nil
}

// WalkExpressions traverses the plan and calls ExprWalk on any expression it finds
func WalkExpressions(v ExprVisitor, op types.PlanOperator) {
	InspectPlan(op, func(operator types.PlanOperator) bool {
		if n, ok := operator.(types.ContainsExpressions); ok {
			for _, e := range n.Expressions() {
				ExprWalk(v, e)
			}
		}
		return true
	})
}

// InspectExpressions traverses the plan and calls WalkExpressions on any
// expression it finds.
func InspectExpressions(op types.PlanOperator, f exprInspector) {
	WalkExpressions(f, op)
}

// InspectExpression traverses expressoins in depth-first order
func InspectExpression(expr types.PlanExpression, f func(expr types.PlanExpression) bool) {
	ExprWalk(exprInspector(f), expr)
}

//-----------------------------------------------------------------------------

// PlanOpExprVisitor visits expressions in an expression tree. Like ExprVisitor, but with the added context of the plan op in which
// an expression is embedded.
type PlanOpExprVisitor interface {
	// VisitPlanOpExpr method is invoked for each expr encountered by Walk. If the result Visitor is not nil, Walk visits each of
	// the children of the expr with that visitor, followed by a call of VisitPlanOpExpr(nil, nil) to the returned visitor.
	VisitPlanOpExpr(op types.PlanOperator, expression types.PlanExpression) PlanOpExprVisitor
}

// ExprWithPlanOpWalk traverses the expression tree in depth-first order. It starts by calling v.VisitPlanOpExpr(op, expr); expr must
// not be nil. If the visitor returned by v.VisitPlanOpExpr(op, expr) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the expr, followed by a call of v.VisitPlanOpExpr(nil, nil) to the returned visitor.
func ExprWithPlanOpWalk(v PlanOpExprVisitor, op types.PlanOperator, expr types.PlanExpression) {
	if v = v.VisitPlanOpExpr(op, expr); v == nil {
		return
	}

	for _, child := range expr.Children() {
		ExprWithPlanOpWalk(v, op, child)
	}

	v.VisitPlanOpExpr(nil, nil)
}

type exprWithNodeInspector func(types.PlanOperator, types.PlanExpression) bool

func (f exprWithNodeInspector) VisitPlanOpExpr(op types.PlanOperator, expr types.PlanExpression) PlanOpExprVisitor {
	if f(op, expr) {
		return f
	}
	return nil
}

// WalkExpressionsWithPlanOp traverses the plan and calls ExprWithPlanOpWalk on any expression it finds.
func WalkExpressionsWithPlanOp(v PlanOpExprVisitor, op types.PlanOperator) {
	InspectPlan(op, func(operator types.PlanOperator) bool {
		if expressioner, ok := operator.(types.ContainsExpressions); ok {
			for _, e := range expressioner.Expressions() {
				ExprWithPlanOpWalk(v, operator, e)
			}
		}
		return true
	})
}

// InspectExpressionsWithPlanOp traverses the plan and calls f on any expression it finds.
func InspectExpressionsWithPlanOp(op types.PlanOperator, f exprWithNodeInspector) {
	WalkExpressionsWithPlanOp(f, op)
}

// PlanOpTransformFunc is a function that given a plan op will return either a transformed plan op or the original plan op.
// If there was a transformation, the bool will be true, and an error if there was an error
type PlanOpTransformFunc func(op types.PlanOperator) (types.PlanOperator, bool, error)

// TransformPlanOp applies a depth first transformation function to the given plan op
// It returns a tuple that is the result of the transformation; the new PlanOperator, a bool that
// is true if the resultant PlanOperator has not been transformed or an error.
// If the TransformPlanOp has children it will iterate the children and call the transformation
// function on each of them in turn. If those operators are transformed, it will create a new operator
// with those children. The last step is to call the transformation on the passed PlanOperator
func TransformPlanOp(op types.PlanOperator, f PlanOpTransformFunc) (types.PlanOperator, bool, error) {
	thisOperator := op

	children := thisOperator.Children()
	if len(children) == 0 {
		return f(thisOperator)
	}

	var newChildren []types.PlanOperator

	for i := range children {
		child := children[i]
		child, same, err := TransformPlanOp(child, f)
		if err != nil {
			return nil, true, err
		}
		if !same {
			if newChildren == nil {
				newChildren = make([]types.PlanOperator, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = child
		}
	}

	var err error
	sameChildren := true
	if len(newChildren) > 0 {
		sameChildren = false
		thisOperator, err = thisOperator.WithChildren(newChildren...)
		if err != nil {
			return nil, true, err
		}
	}

	resultOperator, sameOperator, err := f(thisOperator)
	if err != nil {
		return nil, true, err
	}
	return resultOperator, sameChildren && sameOperator, nil
}

// ParentContext is a struct that enables transformation functions to include a parent operator
type ParentContext struct {
	Operator   types.PlanOperator
	Parent     types.PlanOperator
	ChildCount int
}

type ParentContextFunc func(c ParentContext) (types.PlanOperator, bool, error)

type ParentSelectorFunc func(c ParentContext) bool

// TransformPlanOpWithParent applies a transformation function to a plan operator in the context that plan operators parent
func TransformPlanOpWithParent(op types.PlanOperator, s ParentSelectorFunc, f ParentContextFunc) (types.PlanOperator, bool, error) {
	return planOpWithParentHelper(ParentContext{op, nil, -1}, s, f)
}

func planOpWithParentHelper(c ParentContext, s ParentSelectorFunc, f ParentContextFunc) (types.PlanOperator, bool, error) {
	operator := c.Operator

	children := operator.Children()
	if len(children) == 0 {
		return f(c)
	}

	var (
		newChildren []types.PlanOperator
		err         error
	)
	for i := range children {
		child := children[i]
		cc := ParentContext{child, operator, i}
		if s == nil || s(cc) {
			child, same, err := planOpWithParentHelper(cc, s, f)
			if err != nil {
				return nil, true, err
			}
			if !same {
				if newChildren == nil {
					newChildren = make([]types.PlanOperator, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = child
			}
		}
	}

	sameChildren := true
	if len(newChildren) > 0 {
		sameChildren = false
		operator, err = operator.WithChildren(newChildren...)
		if err != nil {
			return nil, true, err
		}
	}

	resultOperator, sameOperator, err := f(ParentContext{operator, c.Parent, c.ChildCount})
	if err != nil {
		return nil, true, err
	}
	return resultOperator, sameChildren && sameOperator, nil
}

// ExprWithPlanOpFunc is a function that given an expression and the node
// that contains it, will return that expression as is or transformed
// along with an error, if any.
type ExprWithPlanOpFunc func(op types.PlanOperator, expr types.PlanExpression) (types.PlanExpression, bool, error)

// ExprFunc is a function that given an expression will return that
// expression as is or transformed, or bool to indicate
// whether the expression was modified, and an error or nil.
type ExprFunc func(expr types.PlanExpression) (types.PlanExpression, bool, error)

// TransformPlanOpExprsWithPlanOp applies a transformation function to all expressions on the given plan operator from the bottom up in the context of the plan operator
func TransformPlanOpExprsWithPlanOp(op types.PlanOperator, f ExprWithPlanOpFunc) (types.PlanOperator, bool, error) {
	return TransformPlanOp(op, func(n types.PlanOperator) (types.PlanOperator, bool, error) {
		return TransformSinglePlanOpExprsInPlanOpContext(n, f)
	})
}

// TransformPlanOpExprs applies a transformation function to all expressions on the given plan operator from the bottom up
func TransformPlanOpExprs(op types.PlanOperator, f ExprFunc) (types.PlanOperator, bool, error) {
	return TransformPlanOpExprsWithPlanOp(op, func(operator types.PlanOperator, expr types.PlanExpression) (types.PlanExpression, bool, error) {
		return f(expr)
	})
}

// TransformSinglePlanOpExprsInPlanOpContext applies a transformation function to all expressions on a given plan operator in the context of that plan operator
func TransformSinglePlanOpExprsInPlanOpContext(op types.PlanOperator, f ExprWithPlanOpFunc) (types.PlanOperator, bool, error) {
	ne, ok := op.(types.ContainsExpressions)
	if !ok {
		return op, true, nil
	}

	exprs := ne.Expressions()
	if len(exprs) == 0 {
		return op, true, nil
	}

	var (
		newExprs []types.PlanExpression
		err      error
	)

	for i := range exprs {
		e := exprs[i]
		e, same, err := TransformExprWithPlanOp(op, e, f)
		if err != nil {
			return nil, true, err
		}
		if !same {
			if newExprs == nil {
				newExprs = make([]types.PlanExpression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[i] = e
		}
	}

	if len(newExprs) > 0 {
		op, err = ne.WithUpdatedExpressions(newExprs...)
		if err != nil {
			return nil, true, err
		}
		return op, false, nil
	}
	return op, true, nil
}

// TransformSinglePlanOpExpressions applies a transformation function to all expressions on the given plan operator
func TransformSinglePlanOpExpressions(op types.PlanOperator, f ExprFunc) (types.PlanOperator, bool, error) {
	e, ok := op.(types.ContainsExpressions)
	if !ok {
		return op, true, nil
	}

	exprs := e.Expressions()
	if len(exprs) == 0 {
		return op, true, nil
	}

	var newExprs []types.PlanExpression
	for i := range exprs {
		expr := exprs[i]
		expr, same, err := TransformExpr(expr, f)
		if err != nil {
			return nil, true, err
		}
		if !same {
			if newExprs == nil {
				newExprs = make([]types.PlanExpression, len(exprs))
				copy(newExprs, exprs)
			}
			newExprs[i] = expr
		}
	}
	if len(newExprs) > 0 {
		n, err := e.WithUpdatedExpressions(newExprs...)
		if err != nil {
			return nil, true, err
		}
		return n, false, nil
	}
	return op, true, nil
}

// TransformExpr applies a  depth first transformation function to an expression
func TransformExpr(expr types.PlanExpression, f ExprFunc) (types.PlanExpression, bool, error) {
	thisExpr := expr

	children := expr.Children()
	if len(children) == 0 {
		return f(thisExpr)
	}

	var (
		newChildren []types.PlanExpression
		err         error
	)

	for i := 0; i < len(children); i++ {
		c := children[i]
		c, same, err := TransformExpr(c, f)
		if err != nil {
			return nil, true, err
		}
		if !same {
			if newChildren == nil {
				newChildren = make([]types.PlanExpression, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = c
		}
	}

	sameChildren := true
	if len(newChildren) > 0 {
		sameChildren = false
		thisExpr, err = thisExpr.WithChildren(newChildren...)
		if err != nil {
			return nil, true, err
		}
	}

	resultExpr, sameExpr, err := f(thisExpr)
	if err != nil {
		return nil, true, err
	}
	return resultExpr, sameChildren && sameExpr, nil
}

// TransformExprWithPlanOp applies a depth first transformation function to an expression in the context of a plan operator
func TransformExprWithPlanOp(n types.PlanOperator, e types.PlanExpression, f ExprWithPlanOpFunc) (types.PlanExpression, bool, error) {
	thisExpr := e

	children := thisExpr.Children()
	if len(children) == 0 {
		return f(n, e)
	}

	var (
		newChildren []types.PlanExpression
		err         error
	)

	for i := 0; i < len(children); i++ {
		c := children[i]
		c, same, err := TransformExprWithPlanOp(n, c, f)
		if err != nil {
			return nil, true, err
		}
		if !same {
			if newChildren == nil {
				newChildren = make([]types.PlanExpression, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = c
		}
	}

	sameChilren := true
	if len(newChildren) > 0 {
		sameChilren = false
		thisExpr, err = thisExpr.WithChildren(newChildren...)
		if err != nil {
			return nil, true, err
		}
	}

	resultExpr, sameExpr, err := f(n, thisExpr)
	if err != nil {
		return nil, true, err
	}
	return resultExpr, sameChilren && sameExpr, nil
}
