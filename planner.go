// Copyright 2021 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/sql2"
)

type Planner struct {
	executor *executor
}

func NewPlanner(executor *executor) *Planner {
	return &Planner{executor: executor}
}

func (p *Planner) PlanStatement(ctx context.Context, stmt sql2.Statement) (*Stmt, error) {
	node, err := p.planStatement(ctx, stmt)
	if err != nil {
		return nil, err
	}
	return &Stmt{node: node}, nil
}

func (p *Planner) planStatement(ctx context.Context, stmt sql2.Statement) (StmtNode, error) {
	if err := p.checkStatement(stmt); err != nil {
		return nil, err
	}

	switch stmt := stmt.(type) {
	case *sql2.SelectStatement:
		return p.planSelectStatement(ctx, stmt)
	default:
		return nil, fmt.Errorf("cannot plan statement: %T", stmt)
	}
}

func (p *Planner) planSelectStatement(ctx context.Context, stmt *sql2.SelectStatement) (_ StmtNode, err error) {
	if stmt.IsAggregate() {
		return p.planAggregateSelectStatement(ctx, stmt)
	}
	return p.planNonAggregateSelectStatement(ctx, stmt)
}

func (p *Planner) planAggregateSelectStatement(ctx context.Context, stmt *sql2.SelectStatement) (_ StmtNode, err error) {
	indexName, err := statementTableName(stmt)
	if err != nil {
		return nil, err
	}

	// Convert WHERE clause.
	cond, err := p.planExprPQL(ctx, stmt, stmt.WhereExpr)
	if err != nil {
		return nil, err
	}

	// Extract calls and grouped expressions from column list.
	// TODO: Recursively traverse all expression trees.
	var calls []*sql2.Call
	var columns []*StmtColumn
	var resultCols []string
	for _, c := range stmt.Columns {
		columns = append(columns, &StmtColumn{
			Name: c.Name(),
			Type: sql2.ExprDataType(c.Expr),
		})

		switch expr := c.Expr.(type) {
		case *sql2.Call:
			calls = append(calls, expr)
			resultCols = append(resultCols, "_aggregate")
		case *sql2.QualifiedRef:
			resultCols = append(resultCols, expr.Column.Name)
		default:
			return nil, fmt.Errorf("unsupported expression type in aggregate query: %T", expr)
		}
	}

	// TODO: Support multiple calls per query.
	if len(calls) > 1 {
		return nil, fmt.Errorf("only one aggregate call allowed")
	}

	// Extract column names in GROUP BY clause.
	var groupByCols []string
	for _, expr := range stmt.GroupByExprs {
		switch expr := expr.(type) {
		case *sql2.QualifiedRef:
			groupByCols = append(groupByCols, expr.Column.Name)
		default:
			return nil, fmt.Errorf("unsupported expression type in GROUP BY clause: %T", expr)
		}
	}

	// Extract aggregate call and build execution node.
	callName := strings.ToUpper(sql2.IdentName(calls[0].Name))
	switch callName {
	case "COUNT":
		if len(groupByCols) == 0 {
			return NewCountNode(p.executor, indexName, columns[0], cond), nil
		}

		var aggregate *pql.Call
		if calls[0].Distinct.IsValid() {
			if len(calls[0].Args) != 1 {
				return nil, fmt.Errorf("distinct count must have exactly one field specified")
			}
			ref, ok := calls[0].Args[0].(*sql2.QualifiedRef)
			if !ok {
				return nil, fmt.Errorf("distinct count argument must be a field name")
			}

			aggregate = &pql.Call{
				Name: "Count",
				Children: []*pql.Call{{
					Name: "Distinct",
					Args: map[string]interface{}{"field": ref.Column.Name},
				}},
			}
		}

		return NewGroupByNode(p.executor, indexName, resultCols, groupByCols, columns, aggregate, cond), nil

	case "SUM":
		if len(calls[0].Args) != 1 {
			return nil, fmt.Errorf("sum must have exactly one field specified")
		}
		ref, ok := calls[0].Args[0].(*sql2.QualifiedRef)
		if !ok {
			return nil, fmt.Errorf("sum argument must be a field name")
		}

		aggregate := &pql.Call{
			Name: "Sum",
			Args: map[string]interface{}{"field": ref.Column.Name},
		}

		return NewGroupByNode(p.executor, indexName, resultCols, groupByCols, columns, aggregate, cond), nil

	default:
		return nil, fmt.Errorf("unsupported call in aggregate query: %s", callName)
	}

	// TODO: Support HAVING
}

func (p *Planner) planNonAggregateSelectStatement(ctx context.Context, stmt *sql2.SelectStatement) (_ StmtNode, err error) {
	indexName, err := statementTableName(stmt)
	if err != nil {
		return nil, err
	}

	// Lookup index.
	idx := p.executor.Holder.Index(indexName)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, indexName)
	}

	// Convert WHERE clause.
	cond, err := p.planExprPQL(ctx, stmt, stmt.WhereExpr)
	if err != nil {
		return nil, err
	}

	// Build column list.
	var srcs []string
	var columns []*StmtColumn
	for _, col := range stmt.Columns {
		// Handle expressions and qualified references.
		switch expr := col.Expr.(type) {
		case *sql2.QualifiedRef:
			srcs = append(srcs, sql2.IdentName(expr.Column))
			columns = append(columns, &StmtColumn{
				Name: sql2.IdentName(expr.Column),
				Type: sql2.ExprDataType(col.Expr),
			})

		default:
			return nil, fmt.Errorf("unsupported column expression: %T", expr)
		}
	}

	return NewExtractNode(p.executor, indexName, srcs, columns, cond), nil
}

// planExprPQL returns a PQL call tree for a given expression.
func (p *Planner) planExprPQL(ctx context.Context, stmt *sql2.SelectStatement, expr sql2.Expr) (_ *pql.Call, err error) {
	if expr == nil {
		return nil, nil
	}

	switch expr := expr.(type) {
	case *sql2.BinaryExpr:
		return p.planBinaryExprPQL(ctx, stmt, expr)
	case *sql2.BindExpr:
		return nil, fmt.Errorf("bind expressions are not supported")
	case *sql2.BlobLit:
		return nil, fmt.Errorf("blob literals are not supported")
	case *sql2.BoolLit:
		return nil, fmt.Errorf("boolean literals are not supported")
	case *sql2.Call:
		return nil, fmt.Errorf("call expressions are not supported")
	case *sql2.CaseExpr:
		return nil, fmt.Errorf("case expressions are not supported")
	case *sql2.CastExpr:
		return nil, fmt.Errorf("cast expressions are not supported")
	case *sql2.Exists:
		return nil, fmt.Errorf("exists expressions are not supported")
	case *sql2.ExprList:
		return nil, fmt.Errorf("expression lists are not supported")
	case *sql2.Ident:
		return nil, fmt.Errorf("identifiers are not supported")
	case *sql2.NullLit:
		return nil, fmt.Errorf("NULL expressions are not supported")
	case *sql2.NumberLit:
		return nil, fmt.Errorf("number expressions are not supported")
	case *sql2.ParenExpr:
		return p.planExprPQL(ctx, stmt, expr.X)
	case *sql2.QualifiedRef:
		return nil, fmt.Errorf("qualified references are not supported")
	case *sql2.Raise:
		return nil, fmt.Errorf("raise expressions are not supported")
	case *sql2.Range:
		return nil, fmt.Errorf("range expressions are not supported")
	case *sql2.StringLit:
		return nil, fmt.Errorf("string literals are not supported")
	case *sql2.UnaryExpr:
		return nil, fmt.Errorf("unary expressions are not supported")
	default:
		return nil, fmt.Errorf("unexpected SQL expression type: %T", expr)
	}
}

func (p *Planner) planBinaryExprPQL(ctx context.Context, stmt *sql2.SelectStatement, expr *sql2.BinaryExpr) (_ *pql.Call, err error) {
	switch op := expr.Op; op {
	case sql2.AND, sql2.OR:
		name := "Intersect"
		if op == sql2.OR {
			name = "Union"
		}

		x, err := p.planExprPQL(ctx, stmt, expr.X)
		if err != nil {
			return nil, err
		}
		y, err := p.planExprPQL(ctx, stmt, expr.Y)
		if err != nil {
			return nil, err
		}

		return &pql.Call{
			Name:     name,
			Children: []*pql.Call{x, y},
		}, nil

	case sql2.EQ, sql2.NE, sql2.LT, sql2.LE, sql2.GT, sql2.GE:
		// Ensure field reference exists in binary expression.
		x, y := expr.X, expr.Y
		xRef, xOk := x.(*sql2.QualifiedRef)
		yRef, yOk := y.(*sql2.QualifiedRef)
		if xOk && yOk {
			return nil, fmt.Errorf("cannot compare fields in a WHERE clause")
		} else if !xOk && !yOk {
			return nil, fmt.Errorf("expression must reference one field")
		}

		// Rewrite expression so field ref is LHS.
		if !xOk && yOk {
			xRef, y = yRef, x
			switch op {
			case sql2.LT:
				op = sql2.GT
			case sql2.LE:
				op = sql2.GE
			case sql2.GT:
				op = sql2.LT
			case sql2.GE:
				op = sql2.LE
			}
		}

		pqlValue, err := sqlToPQLValue(y)
		if err != nil {
			return nil, err
		}

		isBSI := true // TODO: Check field if it is a BSI field.
		if !isBSI {
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					sql2.IdentName(xRef.Column): pqlValue,
				},
			}, nil
		}

		pqlOp, err := sqlToPQLOp(op)
		if err != nil {
			return nil, err
		}
		return &pql.Call{
			Name: "Row",
			Args: map[string]interface{}{
				sql2.IdentName(xRef.Column): &pql.Condition{
					Op:    pqlOp,
					Value: pqlValue,
				},
			},
		}, nil

	case sql2.BITAND, sql2.BITOR, sql2.BITNOT, sql2.LSHIFT, sql2.RSHIFT:
		return nil, fmt.Errorf("bitwise operators are not supported in WHERE clause")
	case sql2.PLUS, sql2.MINUS, sql2.STAR, sql2.SLASH, sql2.REM: // +
		return nil, fmt.Errorf("arithmetic operators are not supported in WHERE clause")
	case sql2.CONCAT:
		return nil, fmt.Errorf("concatenation operator is not supported in WHERE clause")
	case sql2.IN, sql2.NOTIN:
		return nil, fmt.Errorf("IN operator is not supported")
	case sql2.BETWEEN, sql2.NOTBETWEEN:
		return nil, fmt.Errorf("BETWEEN operator is not supported")
	default:
		return nil, fmt.Errorf("unexpected binary expression operator: %s", expr.Op)
	}
}

// sqlToPQLOp converts a SQL2 operation token to PQL.
func sqlToPQLOp(op sql2.Token) (pql.Token, error) {
	switch op {
	case sql2.EQ:
		return pql.EQ, nil
	case sql2.NE:
		return pql.NEQ, nil
	case sql2.LT:
		return pql.LT, nil
	case sql2.LE:
		return pql.LTE, nil
	case sql2.GT:
		return pql.GT, nil
	case sql2.GE:
		return pql.GTE, nil
	default:
		return pql.ILLEGAL, fmt.Errorf("cannot convert SQL op %q to PQL", op)
	}
}

// sqlToPQLValue converts a literal SQL2 expression node to a PQL Go value.
func sqlToPQLValue(expr sql2.Expr) (interface{}, error) {
	switch expr := expr.(type) {
	case *sql2.StringLit:
		return expr.Value, nil
	case *sql2.NumberLit:
		if expr.IsFloat() {
			return strconv.ParseFloat(expr.Value, 64)
		}
		return strconv.ParseInt(expr.Value, 10, 64)
	case *sql2.BoolLit:
		return expr.Value, nil
	default:
		return nil, fmt.Errorf("cannot convert SQL expression %T to a literal value", expr)
	}
}

func (p *Planner) checkStatement(stmt sql2.Statement) error {
	switch stmt := stmt.(type) {
	case *sql2.SelectStatement:
		return p.checkSelectStatement(stmt)
	default:
		return nil
	}
}

func (p *Planner) checkSelectStatement(stmt *sql2.SelectStatement) error {
	indexName, err := statementTableName(stmt)
	if err != nil {
		return err
	}

	// Look up index.
	idx := p.executor.Holder.Index(indexName)
	if idx == nil {
		return newNotFoundError(ErrIndexNotFound, indexName)
	}

	// Replace wildcards with column references.
	columns := make([]*sql2.ResultColumn, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {
		// Unqualified wildcard.
		isWildcard := col.Star.IsValid()
		if ref, ok := col.Expr.(*sql2.QualifiedRef); ok && ref.Star.IsValid() {
			if ref.Table.Name != indexName {
				return fmt.Errorf("no such table: %q", ref.Table.Name)
			}
			isWildcard = true
		}

		// Simply add column as-is if it is not a wildcard.
		if !isWildcard {
			columns = append(columns, col)
			continue
		}

		// Add identifier field first.
		columns = append(columns, &sql2.ResultColumn{
			Expr: &sql2.QualifiedRef{
				Table:  &sql2.Ident{Name: idx.Name()},
				Column: &sql2.Ident{Name: "_id"},
			},
		})

		// Then add all fields besides the existence bit.
		for _, field := range idx.Fields() {
			if field.Name() == "_exists" {
				continue
			}
			columns = append(columns, &sql2.ResultColumn{
				Expr: &sql2.QualifiedRef{
					Table:  &sql2.Ident{Name: idx.Name()},
					Column: &sql2.Ident{Name: field.Name()},
				},
			})
		}
	}
	stmt.Columns = columns

	// Type check expressions in statement.
	for _, col := range stmt.Columns {
		if err := p.checkExpr(&col.Expr, stmt); err != nil {
			return err
		}
	}

	if err := p.checkExpr(&stmt.WhereExpr, stmt); err != nil {
		return err
	}

	for i := range stmt.GroupByExprs {
		if err := p.checkExpr(&stmt.GroupByExprs[i], stmt); err != nil {
			return err
		}
	}

	if err := p.checkExpr(&stmt.HavingExpr, stmt); err != nil {
		return err
	}

	for _, term := range stmt.OrderingTerms {
		if err := p.checkExpr(&term.X, stmt); err != nil {
			return err
		}
	}

	if err := p.checkExpr(&stmt.LimitExpr, stmt); err != nil {
		return err
	}

	if err := p.checkExpr(&stmt.OffsetExpr, stmt); err != nil {
		return err
	}

	return nil
}

func (p *Planner) checkExpr(expr *sql2.Expr, stmt sql2.Statement) error {
	if e, err := sql2.Walk(&sqlExprTypeChecker{
		holder: p.executor.Holder,
		stmt:   stmt,
	}, *expr); err != nil {
		return err
	} else if e != nil {
		*expr = e.(sql2.Expr)
	} else {
		*expr = nil
	}
	return nil
}

// sqlExprTypeChecker recursively performs type checking within an expression.
// Called by sqlTypeChecker. Implements sql2.Visitor.
type sqlExprTypeChecker struct {
	holder *Holder
	stmt   sql2.Statement // scope
}

var _ sql2.Visitor = (*sqlExprTypeChecker)(nil)

func (v *sqlExprTypeChecker) Visit(node sql2.Node) (_ sql2.Visitor, _ sql2.Node, err error) {
	switch n := node.(type) {
	case *sql2.Call:
		for i := range n.Args {
			if err := v.checkExpr(&n.Args[i]); err != nil {
				return nil, nil, err
			}
		}
		return nil, node, nil // skip
	case *sql2.Ident:
		if node, err = v.visitIdent(n); err != nil {
			return nil, nil, err
		}
		return nil, node, nil
	case *sql2.QualifiedRef:
		if node, err = v.visitQualifiedRef(n); err != nil {
			return nil, nil, err
		}
		return nil, node, nil
	default:
		return v, node, nil
	}
}

func (v *sqlExprTypeChecker) visitIdent(ident *sql2.Ident) (sql2.Node, error) {
	indexName, err := statementTableName(v.stmt)
	if err != nil {
		return nil, err
	}

	// Convert to a table qualified reference and validate through ref visit function.
	return v.visitQualifiedRef(&sql2.QualifiedRef{
		Table:  &sql2.Ident{Name: indexName},
		Column: &sql2.Ident{Name: ident.Name},
	})
}

func (v *sqlExprTypeChecker) visitQualifiedRef(ref *sql2.QualifiedRef) (sql2.Node, error) {
	idx := v.holder.Index(ref.Table.Name)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, ref.Table.Name)
	}

	switch name := ref.Column.Name; name {
	case "_id":
		ref.DataType = sql2.DataTypeInt
	default:
		field := idx.Field(ref.Column.Name)
		if field == nil {
			return nil, newNotFoundError(ErrFieldNotFound, ref.Column.Name)
		}
		ref.DataType = fieldSQLDataType(field)
	}

	return ref, nil
}

func (v *sqlExprTypeChecker) checkExpr(node *sql2.Expr) error {
	if expr, err := sql2.Walk(&sqlExprTypeChecker{
		holder: v.holder,
		stmt:   v.stmt,
	}, *node); err != nil {
		return err
	} else if expr != nil {
		*node = expr.(sql2.Expr)
	} else {
		*node = nil
	}
	return nil
}

func (v *sqlExprTypeChecker) VisitEnd(node sql2.Node) (sql2.Node, error) { return node, nil }

func fieldSQLDataType(f *Field) string {
	if f.Keys() {
		return sql2.DataTypeText
	}

	switch f.Type() {
	case FieldTypeInt, FieldTypeMutex, FieldTypeSet:
		return sql2.DataTypeInt
	case FieldTypeBool:
		return sql2.DataTypeBool
	case FieldTypeDecimal:
		return sql2.DataTypeDecimal
	case FieldTypeTime, FieldTypeTimestamp:
		return sql2.DataTypeTimestamp
	default:
		return ""
	}
}

type Stmt struct {
	node StmtNode
}

func (stmt *Stmt) Close() error { return nil }

func (stmt *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *StmtRow {
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return &StmtRow{err: err}
	}
	return &StmtRow{rows: rows}
}

func (stmt *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*StmtRows, error) {
	// TODO: Handle bind arguments.

	rows := &StmtRows{
		ctx:  ctx,
		node: stmt.node,
	}

	// Initialize the node.
	if err := rows.node.First(ctx); err != nil {
		return nil, fmt.Errorf("Query: initialize statement: %w", err)
	}

	return rows, nil
}

type StmtRows struct {
	ctx  context.Context
	node StmtNode
	err  error
}

func (rs *StmtRows) Close() error {
	return nil
}

func (rs *StmtRows) Err() error {
	if rs.err != nil && rs.err != sql.ErrNoRows {
		return rs.err
	}
	return nil
}

func (rs *StmtRows) Columns() []*StmtColumn {
	return rs.node.Columns()
}

	/*
func (rs *StmtRows) Row() int64 {
	return rs.node.Row()[0].(int64)
}
	*/
func (rs *StmtRows) Next() bool {
	if rs.err != nil {
		return false
	}

	if rs.err = rs.node.Next(rs.ctx); rs.err != nil {
		return false
	}
	return true
}

func (rs *StmtRows) Scan(dst ...interface{}) error {
	if rs.err != nil {
		return rs.err
	}

	//  Check len(dest) against node row length.
	row := rs.node.Row()
	if len(dst) != len(row) {
		return fmt.Errorf("Scan(): expected %d values, received %d values", len(dst), len(row))
	}

	// Copy values from row to destination pointers.
	for i := range dst {
		// Handle null values.
		// TODO: Handle double pointers.
		if row[i] == nil {
			switch p := dst[i].(type) {
			case *int:
				*p = 0
			case *int64:
				*p = 0
			case *uint:
				*p = 0
			case *uint64:
				*p = 0
			case *interface{}:
				*p = nil
			default:
				return fmt.Errorf("cannot scan NULL value into %T destination at index %d", p, i)
			}
			continue
		}

		// Copy row value to scan destination.
		switch v := row[i].(type) {
		case int64:
			switch p := dst[i].(type) {
			case *int:
				*p = int(v)
			case *int64:
				*p = v
			case *uint:
				*p = uint(v)
			case *uint64:
				*p = uint64(v)
			case *interface{}:
				*p = v
			default:
				return fmt.Errorf("cannot scan %T value into %T destination at index %d", v, p, i)
			}
		default:
			return fmt.Errorf("unexpected %T value at index %d", v, i)
		}
	}

	return nil
}

type StmtRow struct {
	err  error
	rows *StmtRows
}

func (r *StmtRow) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	return r.rows.Close()
}

func (r *StmtRow) Err() error {
	return r.err
}

type StmtColumn struct {
	Name string
	Type string
}

type StmtNode interface {
	// Initializes the node to its start.
	First(ctx context.Context) error

	// Moves the node to the next available row. Returns sql.ErrNoRows if done.
	Next(ctx context.Context) error

	// Returns the current row in the node.
	Row() []interface{}

	// Returns column definitions for the node.
	Columns() []*StmtColumn

	// Returns a reference to the value register for a named column.
	// Lookup(table, column string) (interface{}, error)
}

var _ StmtNode = (*ExtractNode)(nil)

// ExtractNode executes an Extract() query against a FeatureBase index.
type ExtractNode struct {
	executor  *executor
	indexName string
	srcs      []string
	columns   []*StmtColumn
	mapping   []int // map of output column indices to source column indices
	cond      *pql.Call

	result []ExtractedTableColumn
	row    []interface{}
}

func NewExtractNode(executor *executor, indexName string, srcs []string, columns []*StmtColumn, cond *pql.Call) *ExtractNode {
	if cond == nil {
		cond = &pql.Call{Name: "All"}
	}

	// Determine mapping between result elements & columns.
	// We'll exclude "_id" from the source columns here as well.
	mapping := make([]int, len(columns))
	srcs2 := make([]string, 0, len(srcs))
	for i := range mapping {
		if srcs[i] == "_id" {
			mapping[i] = -1
			continue
		}

		mapping[i] = len(srcs2)
		srcs2 = append(srcs2, srcs[i])
	}

	return &ExtractNode{
		executor:  executor,
		indexName: indexName,
		srcs:      srcs2,   // source column names (excluding "id")
		columns:   columns, // external column alias
		mapping:   mapping,
		cond:      cond,
		row:       make([]interface{}, len(srcs)),
	}
}

func (n *ExtractNode) Columns() []*StmtColumn {
	return n.columns
}

func (n *ExtractNode) First(ctx context.Context) error {
	n.result = nil
	return nil
}

func (n *ExtractNode) Next(ctx context.Context) error {
	// Fetch results if we haven't yet.
	if err := n.init(ctx); err != nil {
		return err
	}

	// Exit if no result rows remain.
	if len(n.result) == 0 {
		for i := range n.row {
			n.row[i] = nil
		}
		return sql.ErrNoRows
	}

	// Map result elements to row elements.
	for i, index := range n.mapping {
		result := n.result[0]

		// Map row array element to position in result row.
		if index >= 0 {
			n.row[i] = result.Rows[index]
			continue
		}

		// Otherwise use ID for value.
		if result.Column.Keyed {
			n.row[i] = result.Column.Key
		} else {
			n.row[i] = int64(result.Column.ID)
		}
	}

	// Move to next result element.
	n.result = n.result[1:]

	return nil
}

func (n *ExtractNode) init(ctx context.Context) error {
	if n.result != nil {
		return nil
	}

	// Generate PQL query with all specified rows.
	// Skip first column as it is the ID column.
	call := &pql.Call{Name: "Extract", Children: []*pql.Call{n.cond}}
	for _, src := range n.srcs {
		call.Children = append(call.Children,
			&pql.Call{
				Name: "Rows",
				Args: map[string]interface{}{"field": src},
			},
		)
	}

	// Execute Extract() against cluster.
	result, err := n.executor.Execute(ctx, n.indexName, &pql.Query{Calls: []*pql.Call{call}}, nil, nil)
	if err != nil {
		return err
	} else if result.Err != nil {
		return result.Err
	} else if len(result.Results) != 1 {
		return fmt.Errorf("expected single result table from Extract(), got %d results", len(result.Results))
	}

	// Extract out the column/row data from resultset.
	tbl, ok := result.Results[0].(ExtractedTable)
	if !ok {
		return fmt.Errorf("unexpected Extract() result type: %T", result.Results[0])
	}
	n.result = tbl.Columns

	return nil
}

func (n *ExtractNode) Row() []interface{} { return n.row }

var _ StmtNode = (*CountNode)(nil)

// CountNode executes a COUNT(*) against a FeatureBase index and returns a single row.
type CountNode struct {
	executor  *executor
	indexName string
	column    *StmtColumn
	cond      *pql.Call // conditional

	row []interface{}
}

func NewCountNode(executor *executor, indexName string, column *StmtColumn, cond *pql.Call) *CountNode {
	if cond == nil {
		cond = &pql.Call{Name: "All"}
	}
	return &CountNode{
		executor:  executor,
		indexName: indexName,
		column:    column,
		cond:      cond,
	}
}

func (n *CountNode) Columns() []*StmtColumn {
	return []*StmtColumn{n.column}
}

func (n *CountNode) First(ctx context.Context) error {
	n.row = nil
	return nil
}

func (n *CountNode) Next(ctx context.Context) error {
	if n.row != nil {
		return sql.ErrNoRows
	}

	q := &pql.Query{
		Calls: []*pql.Call{
			{Name: "Count", Children: []*pql.Call{n.cond}},
		},
	}

	result, err := n.executor.Execute(ctx, n.indexName, q, nil, nil)
	if err != nil {
		return err
	}

	n.row = []interface{}{int64(result.Results[0].(uint64))}
	return nil
}

func (n *CountNode) Row() []interface{} { return n.row }

// GroupByNode executes an aggregate with a GROUP BY against a FeatureBase index.
type GroupByNode struct {
	executor    *executor
	indexName   string
	groupByCols []string
	columns     []*StmtColumn
	mapping     []int
	aggregate   *pql.Call
	cond        *pql.Call

	result *GroupCounts
	index  int

	row []interface{}
}

func NewGroupByNode(executor *executor, indexName string, resultCols, groupByCols []string, columns []*StmtColumn, aggregate, cond *pql.Call) *GroupByNode {
	// Map result columns to output columns.
	mapping := make([]int, len(columns))
	for i := range mapping {
		if resultCols[i] == "_aggregate" {
			mapping[i] = -1
			continue
		}

		mapping[i] = stringSliceIndex(groupByCols, resultCols[i])
	}

	return &GroupByNode{
		executor:    executor,
		indexName:   indexName,
		groupByCols: groupByCols,
		columns:     columns,
		mapping:     mapping,
		aggregate:   aggregate,
		cond:        cond,
		row:         make([]interface{}, len(columns)),
	}
}

func (n *GroupByNode) Columns() []*StmtColumn {
	return n.columns
}

func (n *GroupByNode) First(ctx context.Context) error {
	n.result = nil
	return nil
}

func (n *GroupByNode) Next(ctx context.Context) (err error) {
	// Fetch resultset if it doesn't exist yet.
	if n.result == nil {
		if n.result, err = n.fetch(ctx); err != nil {
			return err
		}
	}

	// Exit if no more rows exist.
	if n.index >= len(n.result.groups) {
		return sql.ErrNoRows
	}

	// Copy results into current row.
	group := n.result.groups[n.index]
	n.index++

	for i, index := range n.mapping {
		// Assign aggregate to unmapped column.
		if index == -1 {
			if n.aggregate != nil {
				n.row[i] = int64(group.Agg)
			} else {
				n.row[i] = int64(group.Count)
			}
			continue
		}

		// Otherwise map from group value to result column index.
		g := group.Group[index]
		if g.Value != nil {
			n.row[i] = *g.Value
		} else if g.RowKey != "" {
			n.row[i] = g.RowKey
		} else {
			n.row[i] = int64(g.RowID)
		}
	}

	return nil
}

// fetch executes a call to compute the PQL results.
func (n *GroupByNode) fetch(ctx context.Context) (*GroupCounts, error) {
	call := &pql.Call{
		Name: "GroupBy",
		Args: map[string]interface{}{},
	}

	// Choose fields to group by.
	for _, name := range n.groupByCols {
		call.Children = append(call.Children, &pql.Call{
			Name: "Rows", Args: map[string]interface{}{"_field": name},
		})
	}

	// Apply filter & aggregate, if set.
	if n.aggregate != nil {
		call.Args["aggregate"] = n.aggregate
	}
	if n.cond != nil {
		call.Args["filter"] = n.cond
	}

	result, err := n.executor.Execute(ctx, n.indexName, &pql.Query{Calls: []*pql.Call{call}}, nil, nil)
	if err != nil {
		return nil, err
	}
	return result.Results[0].(*GroupCounts), nil
}

func (n *GroupByNode) Row() []interface{} { return n.row }

// statementTableName returns the table name for a single table SELECT statement.
//
// NOTE: This function is only temporary until we support more source types.
func statementTableName(stmt sql2.Statement) (string, error) {
	switch stmt := stmt.(type) {
	case *sql2.SelectStatement:
		return sourceTableName(stmt.Source)
	default:
		return "", fmt.Errorf("statement not currently supported")
	}
}

func sourceTableName(source sql2.Source) (string, error) {
	switch source := source.(type) {
	case *sql2.JoinClause:
		return "", fmt.Errorf("joins are not currently supported")
	case *sql2.ParenSource:
		return "", fmt.Errorf("parenthesized source is not currently supported")
	case *sql2.QualifiedTableName:
		return sql2.IdentName(source.Name), nil
	case *sql2.SelectStatement:
		return "", fmt.Errorf("sub-selects are not currently supported")
	default:
		return "", fmt.Errorf("unexpected source type: %T", source)
	}
}

// stringSliceIndex returns position of v in a. Returns -1 if not found.
func stringSliceIndex(a []string, v string) int {
	for i := range a {
		if a[i] == v {
			return i
		}
	}
	return -1
}
