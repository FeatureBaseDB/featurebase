package planner

import (
	"github.com/molecula/featurebase/v3/sql3/planner/types"
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

type planInspector func(types.PlanOperator) bool

func (f planInspector) VisitOperator(op types.PlanOperator) PlanVisitor {
	if f(op) {
		return f
	}
	return nil
}

// InspectPlan traverses the plan op graph depth-first order
// if f(op) returns true, InspectPlan invokes f recursively for each of the children of op,
// followed by a call of f(nil).
func InspectPlan(op types.PlanOperator, f planInspector) {
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

// WalkExpressions traverses the plan and calls sql.Walk on any expression it finds.
func WalkExpressions(v ExprVisitor, node types.PlanOperator) {
	InspectPlan(node, func(node types.PlanOperator) bool {
		if n, ok := node.(types.ContainsExpressions); ok {
			for _, e := range n.Expressions() {
				ExprWalk(v, e)
			}
		}
		return true
	})
}

// InspectExpressions traverses the plan and calls WalkExpressions on any
// expression it finds.
func InspectExpressions(node types.PlanOperator, f exprInspector) {
	WalkExpressions(f, node)
}

//-----------------------------------------------------------------------------

// PlanOpExprVisitor visits expressions in an expression tree. Like ExprVisitor, but with the added context of the plan op in which
// an expression is embedded.
type PlanOpExprVisitor interface {
	// VisitPlanOpExpr method is invoked for each expr encountered by Walk. If the result Visitor is not nil, Walk visits each of
	// the children of the expr with that visitor, followed by a call of VisitPlanOpExpr(nil, nil) to the returned visitor.
	VisitPlanOpExpr(node types.PlanOperator, expression types.PlanExpression) PlanOpExprVisitor
}

// ExprWithPlanOpWalk traverses the expression tree in depth-first order. It starts by calling v.VisitPlanOpExpr(op, expr); expr must
// not be nil. If the visitor returned by v.VisitPlanOpExpr(op, expr) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the expr, followed by a call of v.VisitPlanOpExpr(nil, nil) to the returned visitor.
func ExprWithPlanOpWalk(v PlanOpExprVisitor, n types.PlanOperator, expr types.PlanExpression) {
	if v = v.VisitPlanOpExpr(n, expr); v == nil {
		return
	}

	for _, child := range expr.Children() {
		ExprWithPlanOpWalk(v, n, child)
	}

	v.VisitPlanOpExpr(nil, nil)
}

type exprWithNodeInspector func(types.PlanOperator, types.PlanExpression) bool

func (f exprWithNodeInspector) VisitPlanOpExpr(n types.PlanOperator, e types.PlanExpression) PlanOpExprVisitor {
	if f(n, e) {
		return f
	}
	return nil
}

// WalkExpressionsWithPlanOp traverses the plan and calls ExprWithPlanOpWalk on any expression it finds.
func WalkExpressionsWithPlanOp(v PlanOpExprVisitor, n types.PlanOperator) {
	InspectPlan(n, func(n types.PlanOperator) bool {
		if expressioner, ok := n.(types.ContainsExpressions); ok {
			for _, e := range expressioner.Expressions() {
				ExprWithPlanOpWalk(v, n, e)
			}
		}
		return true
	})
}

// InspectExpressionsWithPlanOp traverses the plan and calls f on any expression it finds.
func InspectExpressionsWithPlanOp(node types.PlanOperator, f exprWithNodeInspector) {
	WalkExpressionsWithPlanOp(f, node)
}

// PlanOpFunc is a function that given a plan op will return either a transformed plan op or the original plan op.
// If there was a transformation, the bool will be true, and an error if there was an error
type PlanOpFunc func(n types.PlanOperator) (types.PlanOperator, bool, error)

// TransformPlanOp applies a transformation function to the given plan op graph
func TransformPlanOp(op types.PlanOperator, f PlanOpFunc) (types.PlanOperator, bool, error) {

	children := op.Children()
	if len(children) == 0 {
		return f(op)
	}

	var (
		newChildren []types.PlanOperator
	)

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
	sameC := true
	if len(newChildren) > 0 {
		sameC = false
		op, err = op.WithChildren(newChildren...)
		if err != nil {
			return nil, true, err
		}
	}

	op, sameN, err := f(op)
	if err != nil {
		return nil, true, err
	}
	return op, sameC && sameN, nil
}

// ExprWithPlanOpFunc is a function that given an expression and the node
// that contains it, will return that expression as is or transformed
// along with an error, if any.
type ExprWithPlanOpFunc func(types.PlanOperator, types.PlanExpression) (types.PlanExpression, bool, error)

// ExprFunc is a function that given an expression will return that
// expression as is or transformed, or bool to indicate
// whether the expression was modified, and an error or nil.
type ExprFunc func(e types.PlanExpression) (types.PlanExpression, bool, error)

// TransformPlanOpExprsWithPlanOp applies a transformation function to all expressions on the given plan operator from the bottom up in the context of the plan operator
func TransformPlanOpExprsWithPlanOp(op types.PlanOperator, f ExprWithPlanOpFunc) (types.PlanOperator, bool, error) {
	return TransformPlanOp(op, func(n types.PlanOperator) (types.PlanOperator, bool, error) {
		return SinglePlanOpExprsWithPlanOp(n, f)
	})
}

// TransformPlanOpExprs applies a transformation function to all expressions on the given plan operator from the bottom up
func TransformPlanOpExprs(op types.PlanOperator, f ExprFunc) (types.PlanOperator, bool, error) {
	return TransformPlanOpExprsWithPlanOp(op, func(n types.PlanOperator, e types.PlanExpression) (types.PlanExpression, bool, error) {
		return f(e)
	})
}

// SinglePlanOpExprsWithPlanOp applies a transformation function to all expressions on a given plan operator in the context of that plan operator
func SinglePlanOpExprsWithPlanOp(n types.PlanOperator, f ExprWithPlanOpFunc) (types.PlanOperator, bool, error) {
	ne, ok := n.(types.ContainsExpressions)
	if !ok {
		return n, true, nil
	}

	exprs := ne.Expressions()
	if len(exprs) == 0 {
		return n, true, nil
	}

	var (
		newExprs []types.PlanExpression
		err      error
	)

	for i := range exprs {
		e := exprs[i]
		e, same, err := TransformExprWithPlanOp(n, e, f)
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
		n, err = ne.WithExpressions(newExprs...)
		if err != nil {
			return nil, true, err
		}
		return n, false, nil
	}
	return n, true, nil
}

// TransformSinglePlanOpExpressions applies a transformation function to all expressions on the given plan operator
func TransformSinglePlanOpExpressions(o types.PlanOperator, f ExprFunc) (types.PlanOperator, bool, error) {
	e, ok := o.(types.ContainsExpressions)
	if !ok {
		return o, true, nil
	}

	exprs := e.Expressions()
	if len(exprs) == 0 {
		return o, true, nil
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
		n, err := e.WithExpressions(newExprs...)
		if err != nil {
			return nil, true, err
		}
		return n, false, nil
	}
	return o, true, nil
}

// TransformExpr applies a transformation function to an expression
func TransformExpr(e types.PlanExpression, f ExprFunc) (types.PlanExpression, bool, error) {
	children := e.Children()
	if len(children) == 0 {
		return f(e)
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

	sameC := true
	if len(newChildren) > 0 {
		sameC = false
		e, err = e.WithChildren(newChildren...)
		if err != nil {
			return nil, true, err
		}
	}

	e, sameN, err := f(e)
	if err != nil {
		return nil, true, err
	}
	return e, sameC && sameN, nil
}

// TransformExprWithPlanOp applies a transformation function to an expression in the context of a plan operator
func TransformExprWithPlanOp(n types.PlanOperator, e types.PlanExpression, f ExprWithPlanOpFunc) (types.PlanExpression, bool, error) {
	children := e.Children()
	if len(children) == 0 {
		return f(n, e)
	}

	var (
		newChildren []types.PlanExpression
		err         error
	)

	for i := 0; i < len(children); i++ {
		c := children[i]
		c, sameC, err := TransformExprWithPlanOp(n, c, f)
		if err != nil {
			return nil, true, err
		}
		if !sameC {
			if newChildren == nil {
				newChildren = make([]types.PlanExpression, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = c
		}
	}

	sameC := true
	if len(newChildren) > 0 {
		sameC = false
		e, err = e.WithChildren(newChildren...)
		if err != nil {
			return nil, true, err
		}
	}

	e, sameN, err := f(n, e)
	if err != nil {
		return nil, true, err
	}
	return e, sameC && sameN, nil
}
