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
	"io"
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
	// Extract table name from source.
	var source *sql2.QualifiedTableName
	switch src := stmt.Source.(type) {
	case *sql2.JoinClause:
		return nil, fmt.Errorf("cannot use JOIN in aggregate query")
	case *sql2.ParenSource:
		return nil, fmt.Errorf("cannot use parenthesized source in aggregate query")
	case *sql2.QualifiedTableName:
		source = src
	case *sql2.SelectStatement:
		return nil, fmt.Errorf("cannot use sub-select in aggregate query")
	default:
		return nil, fmt.Errorf("unexpected source type in aggregate query: %T", source)
	}

	// TODO: Support multiple aggregate calls.
	if len(stmt.Columns) > 1 {
		return nil, fmt.Errorf("only one call allowed in aggregate query")
	}

	// Extract aggregate call.
	col := stmt.Columns[0]
	var call *sql2.Call
	switch expr := col.Expr.(type) {
	case *sql2.Call:
		call = expr
	default:
		return nil, fmt.Errorf("unsupported expression in aggregate query: %T", expr)
	}

	callName := strings.ToUpper(sql2.IdentName(call.Name))
	switch callName {
	case "COUNT":
		return NewCountNode(p.executor, sql2.IdentName(source.Name)), nil
	default:
		return nil, fmt.Errorf("unsupported call in aggregate query: %T", callName)
	}

	// TODO: Support HAVING
}

func (p *Planner) planNonAggregateSelectStatement(ctx context.Context, stmt *sql2.SelectStatement) (_ StmtNode, err error) {
	panic("TODO: Implement non-aggregate SELECT")
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

type StmtNode interface {
	// Initializes the node to its start.
	First(ctx context.Context) error

	// Moves the node to the next available row. Returns sql.ErrNoRows if done.
	Next(ctx context.Context) error

	// Returns the current row in the node.
	Row() []interface{}

	// Returns column definitions for the node.
	// Columns() []*Column

	// Returns a reference to the value register for a named column.
	// Lookup(table, column string) (interface{}, error)
}

var _ StmtNode = (*CountNode)(nil)

// CountNode executes a COUNT(*) against a FeatureBase index and returns a single row.
type CountNode struct {
	executor  *executor
	indexName string

	row []interface{}
}

func NewCountNode(executor *executor, indexName string) *CountNode {
	return &CountNode{
		executor:  executor,
		indexName: indexName,
	}
}

func (n *CountNode) First(ctx context.Context) error {
	n.row = nil
	return nil
}

func (n *CountNode) Next(ctx context.Context) error {
	if n.row != nil {
		return io.EOF
	}
	result, err := n.executor.Execute(ctx, n.indexName, &pql.Query{
		Calls: []*pql.Call{
			{Name: "Count", Children: []*pql.Call{{Name: "All"}}},
		},
	}, nil, nil)
	if err != nil {
		return err
	}

	n.row = []interface{}{int64(result.Results[0].(uint64))}
	return nil
}

func (n *CountNode) Row() []interface{} { return n.row }
