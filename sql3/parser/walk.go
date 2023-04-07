// Copyright 2021 Molecula Corp. All rights reserved.
package parser

// Visitor is an interface implemented by anything needed to act on nodes walked
// during a node traversal. A Visitor's Visit method is invoked for each node
// encountered by Walk. If the result visitor w is not nil, Walk visits each of
// the children of node with the visitor w, followed by a call of w.Visit(nil).
type Visitor interface {
	Visit(node Node) (w Visitor, n Node, err error)
	VisitEnd(node Node) (Node, error)
}

// Walk traverses an AST in depth-first order: It starts by calling
// v.Visit(node); node must not be nil. If the visitor w returned by
// v.Visit(node) is not nil, Walk is invoked recursively with visitor
// w for each of the non-nil children of node, followed by a call of
// w.Visit(nil).
func Walk(v Visitor, node Node) (Node, error) {
	return walk(v, node)
}

func walk(v Visitor, node Node) (_ Node, err error) {
	// Visit the node itself
	if v, node, err = v.Visit(node); err != nil {
		return node, err
	} else if v == nil {
		return node, nil
	}

	// Visit node's children.
	switch n := node.(type) {
	case *Assignment:
		if err := walkIdentList(v, n.Columns); err != nil {
			return node, err
		}
		if err := walkExpr(v, &n.Expr); err != nil {
			return node, err
		}

	case *ExplainStatement:
		if n.Stmt != nil {
			if stmt, err := walk(v, n.Stmt); err != nil {
				return node, err
			} else if stmt == nil {
				n.Stmt = nil
			} else {
				n.Stmt = stmt.(Statement)
			}
		}

	case *RollbackStatement:
		if err := walkIdent(v, &n.SavepointName); err != nil {
			return node, err
		}

	case *SavepointStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}

	case *ReleaseStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}

	case *CreateTableStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if err := walkColumnDefinitionList(v, n.Columns); err != nil {
			return node, err
		}
		if err := walkConstraintList(v, n.Constraints); err != nil {
			return node, err
		}
		if n.Select != nil {
			if sel, err := walk(v, n.Select); err != nil {
				return node, err
			} else if sel != nil {
				n.Select = sel.(*SelectStatement)
			} else {
				n.Select = nil
			}
		}

	case *AlterTableStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if err := walkIdent(v, &n.OldColumnName); err != nil {
			return node, err
		}
		if err := walkIdent(v, &n.NewColumnName); err != nil {
			return node, err
		}
		if n.ColumnDef != nil {
			if def, err := walk(v, n.ColumnDef); err != nil {
				return node, err
			} else if def != nil {
				n.ColumnDef = def.(*ColumnDefinition)
			} else {
				n.ColumnDef = nil
			}
		}
		if err := walkIdent(v, &n.DropColumnName); err != nil {
			return node, err
		}

	case *AnalyzeStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}

	case *CreateViewStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		// if err := walkIdentList(v, n.Columns); err != nil {
		// 	return node, err
		// }
		if n.Select != nil {
			if sel, err := walk(v, n.Select); err != nil {
				return node, err
			} else if sel != nil {
				n.Select = sel.(*SelectStatement)
			} else {
				n.Select = nil
			}
		}

	case *DropTableStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}

	case *DropViewStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}

	case *DropIndexStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}

	case *DropFunctionStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}

	case *CreateIndexStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if err := walkIdent(v, &n.Table); err != nil {
			return node, err
		}
		if err := walkIndexedColumnList(v, n.Columns); err != nil {
			return node, err
		}
		if err := walkExpr(v, &n.WhereExpr); err != nil {
			return node, err
		}

	case *CreateFunctionStatement:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		for i := range n.Body {
			if body, err := walk(v, n.Body[i]); err != nil {
				return node, err
			} else if body != nil {
				n.Body[i] = body.(Statement)
			} else {
				n.Body[i] = nil
			}
		}

	case *SelectStatement:
		if n.WithClause != nil {
			if clause, err := walk(v, n.WithClause); err != nil {
				return node, err
			} else if clause != nil {
				n.WithClause = clause.(*WithClause)
			} else {
				n.WithClause = nil
			}
		}
		for i := range n.Columns {
			if col, err := walk(v, n.Columns[i]); err != nil {
				return node, err
			} else if col != nil {
				n.Columns[i] = col.(*ResultColumn)
			} else {
				n.Columns[i] = nil
			}
		}
		if n.Source != nil {
			if src, err := walk(v, n.Source); err != nil {
				return node, err
			} else if src == nil {
				n.Source = nil
			}
		}
		if err := walkExpr(v, &n.WhereExpr); err != nil {
			return node, err
		}
		if err := walkExprList(v, n.GroupByExprs); err != nil {
			return node, err
		}
		if err := walkExpr(v, &n.HavingExpr); err != nil {
			return node, err
		}
		for i := range n.Windows {
			if w, err := walk(v, n.Windows[i]); err != nil {
				return node, err
			} else if w != nil {
				n.Windows[i] = w.(*Window)
			} else {
				n.Windows[i] = nil
			}
		}
		if n.Compound != nil {
			if stmt, err := walk(v, n.Compound); err != nil {
				return node, err
			} else if stmt != nil {
				n.Compound = stmt.(*SelectStatement)
			} else {
				n.Compound = nil
			}
		}
		for i := range n.OrderingTerms {
			if term, err := walk(v, n.OrderingTerms[i]); err != nil {
				return node, err
			} else if term != nil {
				n.OrderingTerms[i] = term.(*OrderingTerm)
			} else {
				n.OrderingTerms[i] = nil
			}
		}

	case *InsertStatement:
		/*if n.WithClause != nil {
			if clause, err := walk(v, n.WithClause); err != nil {
				return node, err
			} else if clause != nil {
				n.WithClause = clause.(*WithClause)
			} else {
				n.WithClause = nil
			}
		}*/
		if err := walkIdent(v, &n.Table); err != nil {
			return node, err
		}
		if err := walkIdent(v, &n.Alias); err != nil {
			return node, err
		}
		if err := walkIdentList(v, n.Columns); err != nil {
			return node, err
		}

		for i, tuple := range n.TupleList {
			if list, err := walk(v, tuple); err != nil {
				return node, err
			} else if list != nil {
				n.TupleList[i] = list.(*ExprList)
			} else {
				n.TupleList[i] = nil
			}
		}

		/*if n.Select != nil {
			if sel, err := walk(v, n.Select); err != nil {
				return node, err
			} else if sel != nil {
				n.Select = sel.(*SelectStatement)
			} else {
				n.Select = nil
			}
		}*/

	case *UpdateStatement:
		if n.WithClause != nil {
			if clause, err := walk(v, n.WithClause); err != nil {
				return node, err
			} else if clause != nil {
				n.WithClause = clause.(*WithClause)
			} else {
				n.WithClause = nil
			}
		}
		if n.Table != nil {
			if tbl, err := walk(v, n.Table); err != nil {
				return node, err
			} else if tbl != nil {
				n.Table = tbl.(*QualifiedTableName)
			} else {
				n.Table = nil
			}
		}
		for i := range n.Assignments {
			if assign, err := walk(v, n.Assignments[i]); err != nil {
				return node, err
			} else if assign != nil {
				n.Assignments[i] = assign.(*Assignment)
			} else {
				n.Assignments[i] = nil
			}
		}
		if err := walkExpr(v, &n.WhereExpr); err != nil {
			return node, err
		}

	case *UpsertClause:
		if err := walkIndexedColumnList(v, n.Columns); err != nil {
			return node, err
		}
		if err := walkExpr(v, &n.WhereExpr); err != nil {
			return node, err
		}
		for i := range n.Assignments {
			if assign, err := walk(v, n.Assignments[i]); err != nil {
				return node, err
			} else if assign != nil {
				n.Assignments[i] = assign.(*Assignment)
			} else {
				n.Assignments[i] = nil
			}
		}
		if err := walkExpr(v, &n.UpdateWhereExpr); err != nil {
			return node, err
		}

	case *DeleteStatement:
		// if n.WithClause != nil {
		// 	if clause, err := walk(v, n.WithClause); err != nil {
		// 		return node, err
		// 	} else if clause != nil {
		// 		n.WithClause = clause.(*WithClause)
		// 	} else {
		// 		n.WithClause = nil
		// 	}
		// }
		if n.Source != nil {
			if tbl, err := walk(v, n.Source); err != nil {
				return node, err
			} else if tbl != nil {
				n.Source = tbl.(*QualifiedTableName)
			} else {
				n.Source = nil
			}
		}
		if err := walkExpr(v, &n.WhereExpr); err != nil {
			return node, err
		}

	case *PrimaryKeyConstraint:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if err := walkIdentList(v, n.Columns); err != nil {
			return node, err
		}

	case *NotNullConstraint:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}

	case *UniqueConstraint:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if err := walkIdentList(v, n.Columns); err != nil {
			return node, err
		}

	case *CheckConstraint:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if err := walkExpr(v, &n.Expr); err != nil {
			return node, err
		}

	case *DefaultConstraint:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if err := walkExpr(v, &n.Expr); err != nil {
			return node, err
		}

	case *ForeignKeyConstraint:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if err := walkIdentList(v, n.Columns); err != nil {
			return node, err
		}
		if err := walkIdent(v, &n.ForeignTable); err != nil {
			return node, err
		}
		if err := walkIdentList(v, n.ForeignColumns); err != nil {
			return node, err
		}
		for i := range n.Args {
			if arg, err := walk(v, n.Args[i]); err != nil {
				return node, err
			} else if arg != nil {
				n.Args[i] = arg.(*ForeignKeyArg)
			} else {
				n.Args[i] = nil
			}
		}

	case *ParenExpr:
		if err := walkExpr(v, &n.X); err != nil {
			return node, err
		}

	case *UnaryExpr:
		if err := walkExpr(v, &n.X); err != nil {
			return node, err
		}

	case *BinaryExpr:
		if err := walkExpr(v, &n.X); err != nil {
			return node, err
		}
		if err := walkExpr(v, &n.Y); err != nil {
			return node, err
		}

	case *CastExpr:
		if err := walkExpr(v, &n.X); err != nil {
			return node, err
		}
		if n.Type != nil {
			if typ, err := walk(v, n.Type); err != nil {
				return node, err
			} else if typ != nil {
				n.Type = typ.(*Type)
			} else {
				n.Type = nil
			}
		}

	case *CaseBlock:
		if err := walkExpr(v, &n.Condition); err != nil {
			return node, err
		}
		if err := walkExpr(v, &n.Body); err != nil {
			return node, err
		}

	case *CaseExpr:
		if err := walkExpr(v, &n.Operand); err != nil {
			return node, err
		}
		for i := range n.Blocks {
			if blk, err := walk(v, n.Blocks[i]); err != nil {
				return node, err
			} else if blk != nil {
				n.Blocks[i] = blk.(*CaseBlock)
			} else {
				n.Blocks[i] = nil
			}
		}
		if err := walkExpr(v, &n.ElseExpr); err != nil {
			return node, err
		}

	case *ExprList:
		if err := walkExprList(v, n.Exprs); err != nil {
			return node, err
		}

	case *QualifiedRef:
		if err := walkIdent(v, &n.Table); err != nil {
			return node, err
		}
		if err := walkIdent(v, &n.Column); err != nil {
			return node, err
		}

	case *Call:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if err := walkExprList(v, n.Args); err != nil {
			return node, err
		}
		if n.Filter != nil {
			if filter, err := walk(v, n.Filter); err != nil {
				return node, err
			} else if filter != nil {
				n.Filter = filter.(*FilterClause)
			} else {
				n.Filter = nil
			}
		}
		if n.Over != nil {
			if over, err := walk(v, n.Over); err != nil {
				return node, err
			} else if over != nil {
				n.Over = over.(*OverClause)
			} else {
				n.Over = nil
			}
		}

	case *FilterClause:
		if err := walkExpr(v, &n.X); err != nil {
			return node, err
		}

	case *OverClause:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if n.Definition != nil {
			if def, err := walk(v, n.Definition); err != nil {
				return node, err
			} else if def != nil {
				n.Definition = def.(*WindowDefinition)
			} else {
				n.Definition = nil
			}
		}

	case *OrderingTerm:
		if err := walkExpr(v, &n.X); err != nil {
			return node, err
		}

	case *FrameSpec:
		if err := walkExpr(v, &n.X); err != nil {
			return node, err
		}
		if err := walkExpr(v, &n.Y); err != nil {
			return node, err
		}

	case *Range:
		if err := walkExpr(v, &n.X); err != nil {
			return node, err
		}
		if err := walkExpr(v, &n.Y); err != nil {
			return node, err
		}

	case *Exists:
		if n.Select != nil {
			if sel, err := walk(v, n.Select); err != nil {
				return node, err
			} else if sel != nil {
				n.Select = sel.(*SelectStatement)
			} else {
				n.Select = nil
			}
		}

	case *ParenSource:
		if n.X != nil {
			if x, err := walk(v, n.X); err != nil {
				return node, err
			} else if x != nil {
				n.X = x.(Source)
			} else {
				n.X = nil
			}
		}
		if err := walkIdent(v, &n.Alias); err != nil {
			return node, err
		}

	case *QualifiedTableName:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if err := walkIdent(v, &n.Alias); err != nil {
			return node, err
		}
		if err := walkTableQueryOptionList(v, n.QueryOptions); err != nil {
			return node, err
		}

	case *TableQueryOption:
		if err := walkIdent(v, &n.OptionName); err != nil {
			return node, err
		}
		if err := walkIdentList(v, n.OptionParams); err != nil {
			return node, err
		}

	case *JoinClause:
		if n.X != nil {
			if x, err := walk(v, n.X); err != nil {
				return node, err
			} else if x != nil {
				n.X = x.(Source)
			} else {
				n.X = nil
			}
		}
		if n.Operator != nil {
			if op, err := walk(v, n.Operator); err != nil {
				return node, err
			} else if op != nil {
				n.Operator = op.(*JoinOperator)
			} else {
				n.Operator = nil
			}
		}
		if n.Y != nil {
			if y, err := walk(v, n.Y); err != nil {
				return node, err
			} else if y != nil {
				n.Y = y.(Source)
			} else {
				n.Y = nil
			}
		}
		if n.Constraint != nil {
			if cons, err := walk(v, n.Constraint); err != nil {
				return node, err
			} else if cons != nil {
				n.Constraint = cons.(JoinConstraint)
			} else {
				n.Constraint = nil
			}
		}

	case *OnConstraint:
		if err := walkExpr(v, &n.X); err != nil {
			return node, err
		}

	case *UsingConstraint:
		if err := walkIdentList(v, n.Columns); err != nil {
			return node, err
		}

	case *ColumnDefinition:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if n.Type != nil {
			if typ, err := walk(v, n.Type); err != nil {
				return node, err
			} else if typ != nil {
				n.Type = typ.(*Type)
			} else {
				n.Type = nil
			}
		}
		if err := walkConstraintList(v, n.Constraints); err != nil {
			return node, err
		}

	case *ResultColumn:
		if err := walkExpr(v, &n.Expr); err != nil {
			return node, err
		}
		if err := walkIdent(v, &n.Alias); err != nil {
			return node, err
		}

	case *IndexedColumn:
		if err := walkExpr(v, &n.X); err != nil {
			return node, err
		}

	case *Window:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if n.Definition != nil {
			if def, err := walk(v, n.Definition); err != nil {
				return node, err
			} else if def != nil {
				n.Definition = def.(*WindowDefinition)
			} else {
				n.Definition = nil
			}
		}

	case *WindowDefinition:
		if err := walkIdent(v, &n.Base); err != nil {
			return node, err
		}
		if err := walkExprList(v, n.Partitions); err != nil {
			return node, err
		}
		for i := range n.OrderingTerms {
			if term, err := walk(v, n.OrderingTerms[i]); err != nil {
				return node, err
			} else if term != nil {
				n.OrderingTerms[i] = term.(*OrderingTerm)
			} else {
				n.OrderingTerms[i] = nil
			}
		}
		if n.Frame != nil {
			if frame, err := walk(v, n.Frame); err != nil {
				return node, err
			} else if frame != nil {
				n.Frame = frame.(*FrameSpec)
			} else {
				n.Frame = nil
			}
		}

	case *Type:
		if err := walkIdent(v, &n.Name); err != nil {
			return node, err
		}
		if n.Precision != nil {
			if p, err := walk(v, n.Precision); err != nil {
				return node, err
			} else if p != nil {
				n.Precision = p.(*IntegerLit)
			} else {
				n.Precision = nil
			}
		}
		if n.Scale != nil {
			if scale, err := walk(v, n.Scale); err != nil {
				return node, err
			} else if scale != nil {
				n.Scale = scale.(*IntegerLit)
			} else {
				n.Scale = nil
			}
		}
	}

	// Revisit original node after its children have been processed.
	return v.VisitEnd(node)
}

// VisitFunc represents a function type that implements Visitor.
// Only executes on node entry.
type VisitFunc func(Node) (Node, error)

// Visit executes fn. Walk visits node children if fn returns true.
func (fn VisitFunc) Visit(node Node) (Visitor, Node, error) {
	node, err := fn(node)
	if err != nil {
		return nil, nil, err
	}
	return fn, node, nil
}

// VisitEnd is a no-op.
func (fn VisitFunc) VisitEnd(node Node) (Node, error) { return node, nil }

// VisitEndFunc represents a function type that implements Visitor.
// Only executes on node exit.
type VisitEndFunc func(Node) (Node, error)

// Visit is a no-op.
func (fn VisitEndFunc) Visit(node Node) (Visitor, Node, error) { return fn, node, nil }

// VisitEnd executes fn.
func (fn VisitEndFunc) VisitEnd(node Node) (Node, error) { return fn(node) }

func walkIdent(v Visitor, x **Ident) error {
	if *x == nil {
		return nil
	}

	ident, err := walk(v, *x)
	if err != nil {
		return err
	} else if ident != nil {
		*x = ident.(*Ident)
	} else {
		*x = nil
	}
	return nil
}

func walkIdentList(v Visitor, a []*Ident) error {
	for i := range a {
		if err := walkIdent(v, &a[i]); err != nil {
			return err
		}
	}
	return nil
}

func walkExpr(v Visitor, x *Expr) error {
	if *x == nil {
		return nil
	}
	if other, err := walk(v, *x); err != nil {
		return err
	} else if other != nil {
		*x = other.(Expr)
	} else {
		*x = nil
	}
	return nil
}

func walkExprList(v Visitor, a []Expr) error {
	for i := range a {
		if err := walkExpr(v, &a[i]); err != nil {
			return err
		}
	}
	return nil
}

func walkConstraintList(v Visitor, a []Constraint) error {
	for i := range a {
		if cons, err := walk(v, a[i]); err != nil {
			return err
		} else if cons != nil {
			a[i] = cons.(Constraint)
		} else {
			a[i] = nil
		}
	}
	return nil
}

func walkIndexedColumnList(v Visitor, a []*IndexedColumn) error {
	for i := range a {
		if col, err := walk(v, a[i]); err != nil {
			return err
		} else if col != nil {
			a[i] = col.(*IndexedColumn)
		} else {
			a[i] = nil
		}
	}
	return nil
}

func walkColumnDefinitionList(v Visitor, a []*ColumnDefinition) error {
	for i := range a {
		if def, err := walk(v, a[i]); err != nil {
			return err
		} else if def != nil {
			a[i] = def.(*ColumnDefinition)
		} else {
			a[i] = nil
		}
	}
	return nil
}

func walkTableQueryOptionList(v Visitor, a []*TableQueryOption) error {
	for i := range a {
		if def, err := walk(v, a[i]); err != nil {
			return err
		} else if def != nil {
			a[i] = def.(*TableQueryOption)
		} else {
			a[i] = nil
		}
	}
	return nil
}
