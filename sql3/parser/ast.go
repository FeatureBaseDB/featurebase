// Copyright 2021 Molecula Corp. All rights reserved.
package parser

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

type Node interface {
	node()
	fmt.Stringer
}

func (*AlterDatabaseStatement) node()   {}
func (*AlterTableStatement) node()      {}
func (*AlterViewStatement) node()       {}
func (*AnalyzeStatement) node()         {}
func (*Assignment) node()               {}
func (*ShowDatabasesStatement) node()   {}
func (*ShowTablesStatement) node()      {}
func (*ShowColumnsStatement) node()     {}
func (*ShowCreateTableStatement) node() {}
func (*BeginStatement) node()           {}
func (*BinaryExpr) node()               {}
func (*BoolLit) node()                  {}
func (*BulkInsertMapDefinition) node()  {}
func (*BulkInsertStatement) node()      {}
func (*CacheTypeConstraint) node()      {}
func (*Call) node()                     {}
func (*CaseBlock) node()                {}
func (*CaseExpr) node()                 {}
func (*CastExpr) node()                 {}
func (*CheckConstraint) node()          {}
func (*ColumnDefinition) node()         {}
func (*CommitStatement) node()          {}
func (*CreateDatabaseStatement) node()  {}
func (*CreateIndexStatement) node()     {}
func (*CreateTableStatement) node()     {}
func (*CreateFunctionStatement) node()  {}
func (*CreateViewStatement) node()      {}
func (*DateLit) node()                  {}
func (*DefaultConstraint) node()        {}
func (*DeleteStatement) node()          {}
func (*DropDatabaseStatement) node()    {}
func (*DropIndexStatement) node()       {}
func (*DropTableStatement) node()       {}
func (*DropFunctionStatement) node()    {}
func (*DropViewStatement) node()        {}
func (*Exists) node()                   {}
func (*ExplainStatement) node()         {}
func (*ExprList) node()                 {}
func (*FilterClause) node()             {}
func (*FloatLit) node()                 {}
func (*ForeignKeyArg) node()            {}
func (*ForeignKeyConstraint) node()     {}
func (*FrameSpec) node()                {}
func (*Ident) node()                    {}
func (*Variable) node()                 {}
func (*IndexedColumn) node()            {}
func (*InsertStatement) node()          {}
func (*JoinClause) node()               {}
func (*JoinOperator) node()             {}
func (*KeyPartitionsOption) node()      {}
func (*MinConstraint) node()            {}
func (*MaxConstraint) node()            {}
func (*NotNullConstraint) node()        {}
func (*NullLit) node()                  {}
func (*IntegerLit) node()               {}
func (*OnConstraint) node()             {}
func (*OrderingTerm) node()             {}
func (*OverClause) node()               {}
func (*ParenExpr) node()                {}
func (*SetLiteralExpr) node()           {}
func (*ParenSource) node()              {}
func (*PrimaryKeyConstraint) node()     {}
func (*QualifiedRef) node()             {}
func (*QualifiedTableName) node()       {}
func (*Range) node()                    {}
func (*ReleaseStatement) node()         {}
func (*ResultColumn) node()             {}
func (*RollbackStatement) node()        {}
func (*SavepointStatement) node()       {}
func (*SelectStatement) node()          {}
func (*StringLit) node()                {}
func (*TableValuedFunction) node()      {}
func (*TimeUnitConstraint) node()       {}
func (*TimeQuantumConstraint) node()    {}
func (*TupleLiteralExpr) node()         {}
func (*Type) node()                     {}
func (*UnaryExpr) node()                {}
func (*UniqueConstraint) node()         {}
func (*UnitsOption) node()              {}
func (*UpdateStatement) node()          {}
func (*UpsertClause) node()             {}
func (*UsingConstraint) node()          {}
func (*Window) node()                   {}
func (*WindowDefinition) node()         {}
func (*WithClause) node()               {}
func (*CommentOption) node()            {}

type Statement interface {
	Node
	stmt()
}

func (*AlterDatabaseStatement) stmt()   {}
func (*AlterTableStatement) stmt()      {}
func (*AlterViewStatement) stmt()       {}
func (*AnalyzeStatement) stmt()         {}
func (*BeginStatement) stmt()           {}
func (*BulkInsertStatement) stmt()      {}
func (*ShowDatabasesStatement) stmt()   {}
func (*ShowTablesStatement) stmt()      {}
func (*ShowColumnsStatement) stmt()     {}
func (*ShowCreateTableStatement) stmt() {}
func (*CommitStatement) stmt()          {}
func (*CreateDatabaseStatement) stmt()  {}
func (*CreateIndexStatement) stmt()     {}
func (*CreateTableStatement) stmt()     {}
func (*CreateFunctionStatement) stmt()  {}
func (*CreateViewStatement) stmt()      {}
func (*DeleteStatement) stmt()          {}
func (*DropDatabaseStatement) stmt()    {}
func (*DropIndexStatement) stmt()       {}
func (*DropTableStatement) stmt()       {}
func (*DropFunctionStatement) stmt()    {}
func (*DropViewStatement) stmt()        {}
func (*ExplainStatement) stmt()         {}
func (*InsertStatement) stmt()          {}
func (*ReleaseStatement) stmt()         {}
func (*RollbackStatement) stmt()        {}
func (*SavepointStatement) stmt()       {}
func (*SelectStatement) stmt()          {}
func (*UpdateStatement) stmt()          {}

// CloneStatement returns a deep copy stmt.
func CloneStatement(stmt Statement) Statement {
	if stmt == nil {
		return nil
	}

	switch stmt := stmt.(type) {
	case *AlterDatabaseStatement:
		return stmt.Clone()
	case *AlterTableStatement:
		return stmt.Clone()
	case *AnalyzeStatement:
		return stmt.Clone()
	case *BeginStatement:
		return stmt.Clone()
	case *CommitStatement:
		return stmt.Clone()
	case *CreateDatabaseStatement:
		return stmt.Clone()
	case *CreateIndexStatement:
		return stmt.Clone()
	case *CreateTableStatement:
		return stmt.Clone()
	case *CreateFunctionStatement:
		return stmt.Clone()
	case *CreateViewStatement:
		return stmt.Clone()
	case *DeleteStatement:
		return stmt.Clone()
	case *DropDatabaseStatement:
		return stmt.Clone()
	case *DropIndexStatement:
		return stmt.Clone()
	case *DropTableStatement:
		return stmt.Clone()
	case *DropFunctionStatement:
		return stmt.Clone()
	case *DropViewStatement:
		return stmt.Clone()
	case *ExplainStatement:
		return stmt.Clone()
	case *InsertStatement:
		return stmt.Clone()
	case *ReleaseStatement:
		return stmt.Clone()
	case *RollbackStatement:
		return stmt.Clone()
	case *SavepointStatement:
		return stmt.Clone()
	case *SelectStatement:
		return stmt.Clone()
	case *UpdateStatement:
		return stmt.Clone()
	default:
		panic(fmt.Sprintf("invalid statement type: %T", stmt))
	}
}

func cloneStatements(a []Statement) []Statement {
	if a == nil {
		return nil
	}
	other := make([]Statement, len(a))
	for i := range a {
		other[i] = CloneStatement(a[i])
	}
	return other
}

// StatementSource returns the root statement for a statement.
func StatementSource(stmt Statement) Source {
	switch stmt := stmt.(type) {
	case *SelectStatement:
		return stmt.Source
	case *UpdateStatement:
		return stmt.Table
	case *DeleteStatement:
		return stmt.Source
	default:
		return nil
	}
}

type Expr interface {
	Node
	expr()

	IsLiteral() bool
	DataType() ExprDataType
	Pos() Pos
}

func (*BinaryExpr) expr()       {}
func (*BoolLit) expr()          {}
func (*Call) expr()             {}
func (*CaseExpr) expr()         {}
func (*CaseBlock) expr()        {}
func (*CastExpr) expr()         {}
func (*DateLit) expr()          {}
func (*Exists) expr()           {}
func (*ExprList) expr()         {}
func (*Ident) expr()            {}
func (*Variable) expr()         {}
func (*NullLit) expr()          {}
func (*IntegerLit) expr()       {}
func (*FloatLit) expr()         {}
func (*ParenExpr) expr()        {}
func (*SetLiteralExpr) expr()   {}
func (*TupleLiteralExpr) expr() {}
func (*QualifiedRef) expr()     {}
func (*Range) expr()            {}
func (*StringLit) expr()        {}
func (*UnaryExpr) expr()        {}
func (*SelectStatement) expr()  {}

// CloneExpr returns a deep copy expr.
func CloneExpr(expr Expr) Expr {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *BinaryExpr:
		return expr.Clone()
	case *BoolLit:
		return expr.Clone()
	case *Call:
		return expr.Clone()
	case *CaseExpr:
		return expr.Clone()
	case *CastExpr:
		return expr.Clone()
	case *Exists:
		return expr.Clone()
	case *ExprList:
		return expr.Clone()
	case *Ident:
		return expr.Clone()
	case *NullLit:
		return expr.Clone()
	case *IntegerLit:
		return expr.Clone()
	case *ParenExpr:
		return expr.Clone()
	case *QualifiedRef:
		return expr.Clone()
	case *Range:
		return expr.Clone()
	case *StringLit:
		return expr.Clone()
	case *UnaryExpr:
		return expr.Clone()
	case *Variable:
		return expr.Clone()

	default:
		panic(fmt.Sprintf("invalid expr type: %T", expr))
	}
}

func cloneExprs(a []Expr) []Expr {
	if a == nil {
		return nil
	}
	other := make([]Expr, len(a))
	for i := range a {
		other[i] = CloneExpr(a[i])
	}
	return other
}

// ExprString returns the string representation of expr.
// Returns a blank string if expr is nil.
func ExprString(expr Expr) string {
	if expr == nil {
		return ""
	}
	return expr.String()
}

// SplitExprTree splits apart expr so it is a list of all AND joined expressions.
// For example, the expression "A AND B AND (C OR (D AND E))" would be split into
// a list of "A", "B", "C OR (D AND E)".
func SplitExprTree(expr Expr) []Expr {
	if expr == nil {
		return nil
	}

	var a []Expr
	splitExprTree(expr, &a)
	return a
}

func splitExprTree(expr Expr, a *[]Expr) {
	switch expr := expr.(type) {
	case *BinaryExpr:
		if expr.Op != AND {
			*a = append(*a, expr)
			return
		}
		splitExprTree(expr.X, a)
		splitExprTree(expr.Y, a)
	case *ParenExpr:
		splitExprTree(expr.X, a)
	default:
		*a = append(*a, expr)
	}
}

// SourceOutputColumn is an identifier that is either a possible output column
// for a Source or a referenced output column for a Source. These are computed during
// the analysis phase
type SourceOutputColumn struct {
	TableName   string
	ColumnName  string
	ColumnIndex int
	Datatype    ExprDataType
}

// Source represents a data source for a select statement.
// A select statement has one source, but they can be one of a table ref, a join,
// another select statement or any of the above parenthesiszed. For join operators, the Source
// can form a graph, with the join terms being themselves a Source.
type Source interface {
	Node
	source()
	SourceFromAlias(alias string) Source

	// get the possible output columns from the source
	PossibleOutputColumns() []*SourceOutputColumn

	// find output columns by name
	OutputColumnNamed(name string) (*SourceOutputColumn, error)
	OutputColumnQualifierNamed(qualifier string, name string) (*SourceOutputColumn, error)
}

func (*JoinClause) source()          {}
func (*ParenSource) source()         {}
func (*QualifiedTableName) source()  {}
func (*TableValuedFunction) source() {}
func (*SelectStatement) source()     {}

// CloneSource returns a deep copy src.
func CloneSource(src Source) Source {
	if src == nil {
		return nil
	}
	switch src := src.(type) {
	case *JoinClause:
		return src.Clone()
	case *ParenSource:
		return src.Clone()
	case *QualifiedTableName:
		return src.Clone()
	case *SelectStatement:
		return src.Clone()
	default:
		panic(fmt.Sprintf("invalid source type: %T", src))
	}
}

// SourceList returns a list of sources starting from a source.
func SourceList(src Source) []Source {
	var a []Source
	ForEachSource(src, func(s Source) bool {
		a = append(a, s)
		return true
	})
	return a
}

// ForEachSource calls fn for every source within the current scope.
// Stops iteration if fn returns false.
func ForEachSource(src Source, fn func(Source) bool) {
	forEachSource(src, fn)
}

func forEachSource(src Source, fn func(Source) bool) bool {
	if !fn(src) {
		return false
	}

	switch src := src.(type) {
	case *JoinClause:
		if !forEachSource(src.X, fn) {
			return false
		} else if !forEachSource(src.Y, fn) {
			return false
		}
	case *SelectStatement:
		if !forEachSource(src.Source, fn) {
			return false
		}
	}
	return true
}

// JoinConstraint represents either an ON or USING join constraint.
type JoinConstraint interface {
	Node
	joinConstraint()
}

func (*OnConstraint) joinConstraint()    {}
func (*UsingConstraint) joinConstraint() {}

// CloneJoinConstraint returns a deep copy cons.
func CloneJoinConstraint(cons JoinConstraint) JoinConstraint {
	if cons == nil {
		return nil
	}

	switch cons := cons.(type) {
	case *OnConstraint:
		return cons.Clone()
	case *UsingConstraint:
		return cons.Clone()
	default:
		panic(fmt.Sprintf("invalid join constraint type: %T", cons))
	}
}

type ExplainStatement struct {
	Explain   Pos       // position of EXPLAIN
	Query     Pos       // position of QUERY (optional)
	QueryPlan Pos       // position of PLAN after QUERY (optional)
	Stmt      Statement // target statement
}

// Clone returns a deep copy of s.
func (s *ExplainStatement) Clone() *ExplainStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Stmt = CloneStatement(s.Stmt)
	return &other
}

// String returns the string representation of the statement.
func (s *ExplainStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("EXPLAIN")
	if s.QueryPlan.IsValid() {
		buf.WriteString(" QUERY PLAN")
	}
	fmt.Fprintf(&buf, " %s", s.Stmt.String())
	return buf.String()
}

type ShowDatabasesStatement struct {
	Show      Pos // position of SHOW
	Databases Pos // position of DATABASES
}

// String returns the string representation of the statement.
func (s *ShowDatabasesStatement) String() string {
	return "SHOW DATABASES"
}

type ShowTablesStatement struct {
	Show   Pos // position of SHOW
	Tables Pos // position of TABLES
}

// String returns the string representation of the statement.
func (s *ShowTablesStatement) String() string {
	return "SHOW TABLES"
}

type ShowColumnsStatement struct {
	Show      Pos    // position of SHOW
	Columns   Pos    // position of COLUMNS
	From      Pos    // position of FROM
	TableName *Ident // name of table
}

// String returns the string representation of the statement.
func (s *ShowColumnsStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("SHOW COLUMNS ")
	if s.TableName != nil {
		buf.WriteString(" FROM")
		fmt.Fprintf(&buf, " %s", s.TableName.String())
	}
	return buf.String()
}

type ShowCreateTableStatement struct {
	Show      Pos    // position of SHOW
	Create    Pos    // position of CREATE
	Table     Pos    // position of CREATE
	TableName *Ident // name of table
}

// String returns the string representation of the statement.
func (s *ShowCreateTableStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("SHOW CREATE TABLE ")
	if s.TableName != nil {
		fmt.Fprintf(&buf, " %s", s.TableName.String())
	}
	return buf.String()
}

type BeginStatement struct {
	Begin       Pos // position of BEGIN
	Deferred    Pos // position of DEFERRED keyword
	Immediate   Pos // position of IMMEDIATE keyword
	Exclusive   Pos // position of EXCLUSIVE keyword
	Transaction Pos // position of TRANSACTION keyword  (optional)
}

// Clone returns a deep copy of s.
func (s *BeginStatement) Clone() *BeginStatement {
	if s == nil {
		return nil
	}
	other := *s
	return &other
}

// String returns the string representation of the statement.
func (s *BeginStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("BEGIN")
	if s.Deferred.IsValid() {
		buf.WriteString(" DEFERRED")
	} else if s.Immediate.IsValid() {
		buf.WriteString(" IMMEDIATE")
	} else if s.Exclusive.IsValid() {
		buf.WriteString(" EXCLUSIVE")
	}
	if s.Transaction.IsValid() {
		buf.WriteString(" TRANSACTION")
	}
	return buf.String()
}

type CommitStatement struct {
	Commit      Pos // position of COMMIT keyword
	End         Pos // position of END keyword
	Transaction Pos // position of TRANSACTION keyword  (optional)
}

// Clone returns a deep copy of s.
func (s *CommitStatement) Clone() *CommitStatement {
	if s == nil {
		return nil
	}
	other := *s
	return &other
}

// String returns the string representation of the statement.
func (s *CommitStatement) String() string {
	var buf bytes.Buffer
	if s.End.IsValid() {
		buf.WriteString("END")
	} else {
		buf.WriteString("COMMIT")
	}

	if s.Transaction.IsValid() {
		buf.WriteString(" TRANSACTION")
	}
	return buf.String()
}

type RollbackStatement struct {
	Rollback      Pos    // position of ROLLBACK keyword
	Transaction   Pos    // position of TRANSACTION keyword  (optional)
	To            Pos    // position of TO keyword  (optional)
	Savepoint     Pos    // position of SAVEPOINT keyword  (optional)
	SavepointName *Ident // name of savepoint
}

// Clone returns a deep copy of s.
func (s *RollbackStatement) Clone() *RollbackStatement {
	if s == nil {
		return s
	}
	other := *s
	other.SavepointName = s.SavepointName.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *RollbackStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("ROLLBACK")
	if s.Transaction.IsValid() {
		buf.WriteString(" TRANSACTION")
	}

	if s.SavepointName != nil {
		buf.WriteString(" TO")
		if s.Savepoint.IsValid() {
			buf.WriteString(" SAVEPOINT")
		}
		fmt.Fprintf(&buf, " %s", s.SavepointName.String())
	}
	return buf.String()
}

type SavepointStatement struct {
	Savepoint Pos    // position of SAVEPOINT keyword
	Name      *Ident // name of savepoint
}

// Clone returns a deep copy of s.
func (s *SavepointStatement) Clone() *SavepointStatement {
	if s == nil {
		return s
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *SavepointStatement) String() string {
	return fmt.Sprintf("SAVEPOINT %s", s.Name.String())
}

type ReleaseStatement struct {
	Release   Pos    // position of RELEASE keyword
	Savepoint Pos    // position of SAVEPOINT keyword (optional)
	Name      *Ident // name of savepoint
}

// Clone returns a deep copy of s.
func (s *ReleaseStatement) Clone() *ReleaseStatement {
	if s == nil {
		return s
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *ReleaseStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("RELEASE")
	if s.Savepoint.IsValid() {
		buf.WriteString(" SAVEPOINT")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type CreateDatabaseStatement struct {
	Create      Pos    // position of CREATE keyword
	Database    Pos    // position of DATABASE keyword
	If          Pos    // position of IF keyword (optional)
	IfNot       Pos    // position of NOT keyword (optional)
	IfNotExists Pos    // position of EXISTS keyword (optional)
	Name        *Ident // database name

	Options []DatabaseOption // database options
}

// Clone returns a deep copy of s.
func (s *CreateDatabaseStatement) Clone() *CreateDatabaseStatement {
	if s == nil {
		return s
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *CreateDatabaseStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE DATABASE")
	if s.IfNotExists.IsValid() {
		buf.WriteString(" IF NOT EXISTS")
	}
	buf.WriteString(" ")
	buf.WriteString(s.Name.String())

	for _, opt := range s.Options {
		buf.WriteString(" ")
		buf.WriteString(opt.String())
	}

	return buf.String()
}

type CreateTableStatement struct {
	Create      Pos    // position of CREATE keyword
	Table       Pos    // position of TABLE keyword
	If          Pos    // position of IF keyword (optional)
	IfNot       Pos    // position of NOT keyword (optional)
	IfNotExists Pos    // position of EXISTS keyword (optional)
	Name        *Ident // table name

	Lparen      Pos                 // position of left paren of column list
	Columns     []*ColumnDefinition // column definitions
	Constraints []Constraint        // table constraints
	Rparen      Pos                 // position of right paren of column list

	As      Pos              // position of AS keyword (optional)
	Select  *SelectStatement // select stmt to build from
	Options []TableOption    // table options
}

// Clone returns a deep copy of s.
func (s *CreateTableStatement) Clone() *CreateTableStatement {
	if s == nil {
		return s
	}
	other := *s
	other.Name = s.Name.Clone()
	other.Columns = cloneColumnDefinitions(s.Columns)
	other.Constraints = cloneConstraints(s.Constraints)
	other.Select = s.Select.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *CreateTableStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE")
	if s.IfNotExists.IsValid() {
		buf.WriteString(" IF NOT EXISTS")
	}
	buf.WriteString(" ")
	buf.WriteString(s.Name.String())

	if s.Select != nil {
		buf.WriteString(" AS ")
		buf.WriteString(s.Select.String())
	} else {
		buf.WriteString(" (")
		for i := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(s.Columns[i].String())
		}
		for i := range s.Constraints {
			buf.WriteString(", ")
			buf.WriteString(s.Constraints[i].String())
		}
		buf.WriteString(")")
	}

	return buf.String()
}

type ColumnDefinition struct {
	Name        *Ident       // column name
	Type        *Type        // data type
	Constraints []Constraint // column constraints
}

// Clone returns a deep copy of d.
func (d *ColumnDefinition) Clone() *ColumnDefinition {
	if d == nil {
		return d
	}
	other := *d
	other.Name = d.Name.Clone()
	other.Type = d.Type.Clone()
	other.Constraints = cloneConstraints(d.Constraints)
	return &other
}

func cloneColumnDefinitions(a []*ColumnDefinition) []*ColumnDefinition {
	if a == nil {
		return nil
	}
	other := make([]*ColumnDefinition, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the statement.
func (c *ColumnDefinition) String() string {
	var buf bytes.Buffer
	buf.WriteString(c.Name.String())
	buf.WriteString(" ")
	buf.WriteString(c.Type.String())
	for i := range c.Constraints {
		buf.WriteString(" ")
		buf.WriteString(c.Constraints[i].String())
	}
	return buf.String()
}

type DatabaseOption interface {
	Node
	dbOption()
}

func (*UnitsOption) dbOption()   {}
func (*CommentOption) dbOption() {}

type UnitsOption struct {
	Units Pos  // position of UNITS keyword
	Expr  Expr // expression
}

func (o *UnitsOption) String() string {
	var buf bytes.Buffer
	buf.WriteString("UNITS ")
	buf.WriteString(o.Expr.String())
	return buf.String()
}

type TableOption interface {
	Node
	option()
}

func (*KeyPartitionsOption) option() {}
func (*CommentOption) option()       {}

type KeyPartitionsOption struct {
	KeyPartitions Pos  // position of KEYPARTITIONS keyword
	Expr          Expr // expression
}

func (o *KeyPartitionsOption) String() string {
	var buf bytes.Buffer
	buf.WriteString("KEYPARTITIONS (")
	buf.WriteString(o.Expr.String())
	buf.WriteString(")")
	return buf.String()
}

type CommentOption struct {
	Comment Pos  // position of COMMENT keyword
	Expr    Expr // expression
}

func (o *CommentOption) String() string {
	var buf bytes.Buffer
	buf.WriteString("COMMENT (")
	buf.WriteString(o.Expr.String())
	buf.WriteString(")")
	return buf.String()
}

type Constraint interface {
	Node
	constraint()
}

func (*PrimaryKeyConstraint) constraint()  {}
func (*NotNullConstraint) constraint()     {}
func (*UniqueConstraint) constraint()      {}
func (*CheckConstraint) constraint()       {}
func (*DefaultConstraint) constraint()     {}
func (*ForeignKeyConstraint) constraint()  {}
func (*MinConstraint) constraint()         {}
func (*MaxConstraint) constraint()         {}
func (*CacheTypeConstraint) constraint()   {}
func (*TimeUnitConstraint) constraint()    {}
func (*TimeQuantumConstraint) constraint() {}

// CloneConstraint returns a deep copy cons.
func CloneConstraint(cons Constraint) Constraint {
	if cons == nil {
		return nil
	}

	switch cons := cons.(type) {
	case *PrimaryKeyConstraint:
		return cons.Clone()
	case *NotNullConstraint:
		return cons.Clone()
	case *UniqueConstraint:
		return cons.Clone()
	case *CheckConstraint:
		return cons.Clone()
	case *DefaultConstraint:
		return cons.Clone()
	case *ForeignKeyConstraint:
		return cons.Clone()
	default:
		panic(fmt.Sprintf("invalid constraint type: %T", cons))
	}
}

func cloneConstraints(a []Constraint) []Constraint {
	if a == nil {
		return nil
	}
	other := make([]Constraint, len(a))
	for i := range a {
		other[i] = CloneConstraint(a[i])
	}
	return other
}

type PrimaryKeyConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Primary    Pos    // position of PRIMARY keyword
	Key        Pos    // position of KEY keyword

	Lparen  Pos      // position of left paren (table only)
	Columns []*Ident // indexed columns (table only)
	Rparen  Pos      // position of right paren (table only)

	Autoincrement Pos // position of AUTOINCREMENT keyword (column only)
}

// Clone returns a deep copy of c.
func (c *PrimaryKeyConstraint) Clone() *PrimaryKeyConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Columns = cloneIdents(c.Columns)
	return &other
}

// String returns the string representation of the constraint.
func (c *PrimaryKeyConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("PRIMARY KEY")

	if len(c.Columns) > 0 {
		buf.WriteString(" (")
		for i := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Columns[i].String())
		}
		buf.WriteString(")")
	}

	if c.Autoincrement.IsValid() {
		buf.WriteString(" AUTOINCREMENT")
	}
	return buf.String()
}

type NotNullConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Not        Pos    // position of NOT keyword
	Null       Pos    // position of NULL keyword
}

// Clone returns a deep copy of c.
func (c *NotNullConstraint) Clone() *NotNullConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	return &other
}

// String returns the string representation of the constraint.
func (c *NotNullConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("NOT NULL")

	return buf.String()
}

type UniqueConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Unique     Pos    // position of UNIQUE keyword

	Lparen  Pos      // position of left paren (table only)
	Columns []*Ident // indexed columns (table only)
	Rparen  Pos      // position of right paren (table only)
}

// Clone returns a deep copy of c.
func (c *UniqueConstraint) Clone() *UniqueConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Columns = cloneIdents(c.Columns)
	return &other
}

// String returns the string representation of the constraint.
func (c *UniqueConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("UNIQUE")

	if len(c.Columns) > 0 {
		buf.WriteString(" (")
		for i := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Columns[i].String())
		}
		buf.WriteString(")")
	}

	return buf.String()
}

type MinConstraint struct {
	Min  Pos  // position of MIN keyword
	Expr Expr // min expression
}

// Clone returns a deep copy of c.
func (c *MinConstraint) Clone() *MinConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Expr = CloneExpr(c.Expr)
	return &other
}

// String returns the string representation of the constraint.
func (c *MinConstraint) String() string {
	var buf bytes.Buffer
	buf.WriteString("MIN ")
	buf.WriteString(c.Expr.String())
	return buf.String()
}

type MaxConstraint struct {
	Max  Pos  // position of MAX keyword
	Expr Expr // check expression
}

// Clone returns a deep copy of c.
func (c *MaxConstraint) Clone() *MaxConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Expr = CloneExpr(c.Expr)
	return &other
}

// String returns the string representation of the constraint.
func (c *MaxConstraint) String() string {
	var buf bytes.Buffer
	buf.WriteString("MAX ")
	buf.WriteString(c.Expr.String())
	return buf.String()
}

type CacheTypeConstraint struct {
	CacheType      Pos // position of CACHETYPE keyword
	CacheTypeValue string
	Size           Pos  // position of SIZE keyword
	SizeExpr       Expr // check expression
}

// Clone returns a deep copy of c.
func (c *CacheTypeConstraint) Clone() *CacheTypeConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.SizeExpr = CloneExpr(c.SizeExpr)
	return &other
}

// String returns the string representation of the constraint.
func (c *CacheTypeConstraint) String() string {
	var buf bytes.Buffer
	buf.WriteString("CACHETYPE ")
	buf.WriteString(c.CacheTypeValue)
	if c.Size.IsValid() {
		buf.WriteString(" SIZE ")
		buf.WriteString(c.SizeExpr.String())
	}
	return buf.String()
}

type TimeUnitConstraint struct {
	TimeUnit  Pos  // position of TIMEUNIT keyword
	Expr      Expr // expression
	Epoch     Pos  // position of TIMEUNIT keyword
	EpochExpr Expr // expression
}

// Clone returns a deep copy of c.
func (c *TimeUnitConstraint) Clone() *TimeUnitConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Expr = CloneExpr(c.Expr)
	return &other
}

// String returns the string representation of the constraint.
func (c *TimeUnitConstraint) String() string {
	var buf bytes.Buffer
	buf.WriteString("TIMEUNIT ")
	buf.WriteString(c.Expr.String())
	if c.Epoch.IsValid() {
		buf.WriteString(" EPOCH ")
		buf.WriteString(c.EpochExpr.String())
	}
	return buf.String()
}

type TimeQuantumConstraint struct {
	TimeQuantum Pos  // position of TIMEQUANTUM keyword
	Expr        Expr // expression
	Ttl         Pos
	TtlExpr     Expr
}

// Clone returns a deep copy of c.
func (c *TimeQuantumConstraint) Clone() *TimeQuantumConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Expr = CloneExpr(c.Expr)
	return &other
}

// String returns the string representation of the constraint.
func (c *TimeQuantumConstraint) String() string {
	var buf bytes.Buffer
	buf.WriteString("TIMEQUANTUM (")
	buf.WriteString(c.Expr.String())
	buf.WriteString(")")
	return buf.String()
}

type CheckConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Check      Pos    // position of UNIQUE keyword
	Lparen     Pos    // position of left paren
	Expr       Expr   // check expression
	Rparen     Pos    // position of right paren
}

// Clone returns a deep copy of c.
func (c *CheckConstraint) Clone() *CheckConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Expr = CloneExpr(c.Expr)
	return &other
}

// String returns the string representation of the constraint.
func (c *CheckConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("CHECK (")
	buf.WriteString(c.Expr.String())
	buf.WriteString(")")
	return buf.String()
}

type DefaultConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Default    Pos    // position of DEFAULT keyword
	Lparen     Pos    // position of left paren
	Expr       Expr   // default expression
	Rparen     Pos    // position of right paren
}

// Clone returns a deep copy of c.
func (c *DefaultConstraint) Clone() *DefaultConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Expr = CloneExpr(c.Expr)
	return &other
}

// String returns the string representation of the constraint.
func (c *DefaultConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("DEFAULT ")

	if c.Lparen.IsValid() {
		buf.WriteString("(")
		buf.WriteString(c.Expr.String())
		buf.WriteString(")")
	} else {
		buf.WriteString(c.Expr.String())
	}
	return buf.String()
}

type ForeignKeyConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name

	Foreign    Pos      // position of FOREIGN keyword (table only)
	ForeignKey Pos      // position of KEY keyword after FOREIGN (table only)
	Lparen     Pos      // position of left paren (table only)
	Columns    []*Ident // indexed columns (table only)
	Rparen     Pos      // position of right paren (table only)

	References         Pos              // position of REFERENCES keyword
	ForeignTable       *Ident           // foreign table name
	ForeignLparen      Pos              // position of left paren
	ForeignColumns     []*Ident         // column list
	ForeignRparen      Pos              // position of right paren
	Args               []*ForeignKeyArg // arguments
	Deferrable         Pos              // position of DEFERRABLE keyword
	Not                Pos              // position of NOT keyword
	NotDeferrable      Pos              // position of DEFERRABLE keyword after NOT
	Initially          Pos              // position of INITIALLY keyword
	InitiallyDeferred  Pos              // position of DEFERRED keyword after INITIALLY
	InitiallyImmediate Pos              // position of IMMEDIATE keyword after INITIALLY
}

// Clone returns a deep copy of c.
func (c *ForeignKeyConstraint) Clone() *ForeignKeyConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Columns = cloneIdents(c.Columns)
	other.ForeignTable = c.ForeignTable.Clone()
	other.ForeignColumns = cloneIdents(c.ForeignColumns)
	other.Args = cloneForeignKeyArgs(c.Args)
	return &other
}

// String returns the string representation of the constraint.
func (c *ForeignKeyConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	if len(c.Columns) > 0 {
		buf.WriteString("FOREIGN KEY (")
		for i := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Columns[i].String())
		}
		buf.WriteString(") ")
	}

	buf.WriteString("REFERENCES ")
	buf.WriteString(c.ForeignTable.String())
	if len(c.ForeignColumns) > 0 {
		buf.WriteString(" (")
		for i := range c.ForeignColumns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.ForeignColumns[i].String())
		}
		buf.WriteString(")")
	}

	for i := range c.Args {
		buf.WriteString(" ")
		buf.WriteString(c.Args[i].String())
	}

	if c.Deferrable.IsValid() || c.NotDeferrable.IsValid() {
		if c.Deferrable.IsValid() {
			buf.WriteString(" DEFERRABLE")
		} else {
			buf.WriteString(" NOT DEFERRABLE")
		}

		if c.InitiallyDeferred.IsValid() {
			buf.WriteString(" INITIALLY DEFERRED")
		} else if c.InitiallyImmediate.IsValid() {
			buf.WriteString(" INITIALLY IMMEDIATE")
		}
	}

	return buf.String()
}

type ForeignKeyArg struct {
	On         Pos // position of ON keyword
	OnUpdate   Pos // position of the UPDATE keyword
	OnDelete   Pos // position of the DELETE keyword
	Set        Pos // position of the SET keyword
	SetNull    Pos // position of the NULL keyword after SET
	SetDefault Pos // position of the DEFAULT keyword after SET
	Cascade    Pos // position of the CASCADE keyword
	Restrict   Pos // position of the RESTRICT keyword
	No         Pos // position of the NO keyword
	NoAction   Pos // position of the ACTION keyword after NO
}

// Clone returns a deep copy of arg.
func (arg *ForeignKeyArg) Clone() *ForeignKeyArg {
	if arg == nil {
		return nil
	}
	other := *arg
	return &other
}

func cloneForeignKeyArgs(a []*ForeignKeyArg) []*ForeignKeyArg {
	if a == nil {
		return nil
	}
	other := make([]*ForeignKeyArg, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the argument.
func (c *ForeignKeyArg) String() string {
	var buf bytes.Buffer
	buf.WriteString("ON")
	if c.OnUpdate.IsValid() {
		buf.WriteString(" UPDATE")
	} else {
		buf.WriteString(" DELETE")
	}

	if c.SetNull.IsValid() {
		buf.WriteString(" SET NULL")
	} else if c.SetDefault.IsValid() {
		buf.WriteString(" SET DEFAULT")
	} else if c.Cascade.IsValid() {
		buf.WriteString(" CASCADE")
	} else if c.Restrict.IsValid() {
		buf.WriteString(" RESTRICT")
	} else if c.NoAction.IsValid() {
		buf.WriteString(" NO ACTION")
	}
	return buf.String()
}

type AnalyzeStatement struct {
	Analyze Pos    // position of ANALYZE keyword
	Name    *Ident // table name
}

// Clone returns a deep copy of s.
func (s *AnalyzeStatement) Clone() *AnalyzeStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *AnalyzeStatement) String() string {
	return fmt.Sprintf("ANALYZE %s", s.Name.String())
}

type AlterDatabaseStatement struct {
	Alter    Pos    // position of ALTER keyword
	Database Pos    // position of DATABASE keyword
	Name     *Ident // database name

	Set Pos // position of SET keyword

	Option DatabaseOption
}

// Clone returns a deep copy of s.
func (s *AlterDatabaseStatement) Clone() *AlterDatabaseStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = other.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *AlterDatabaseStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("ALTER DATABASE ")
	buf.WriteString(s.Name.String())

	if s.Option != nil {
		buf.WriteString(" SET ")
		buf.WriteString(s.Option.String())
	}
	return buf.String()
}

type AlterTableStatement struct {
	Alter Pos    // position of ALTER keyword
	Table Pos    // position of TABLE keyword
	Name  *Ident // table name

	Rename Pos // position of RENAME keyword
	//RenameTo Pos    // position of TO keyword after RENAME
	//NewName  *Ident // new table name

	RenameColumn  Pos    // position of COLUMN keyword after RENAME
	OldColumnName *Ident // old column name
	To            Pos    // position of TO keyword
	NewColumnName *Ident // new column name

	Add       Pos               // position of ADD keyword
	AddColumn Pos               // position of COLUMN keyword after ADD
	ColumnDef *ColumnDefinition // new column definition

	Drop           Pos    // position of ADD keyword
	DropColumn     Pos    // position of COLUMN keyword after ADD
	DropColumnName *Ident // drop column name
}

// Clone returns a deep copy of s.
func (s *AlterTableStatement) Clone() *AlterTableStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = other.Name.Clone()
	//other.NewName = s.NewName.Clone()
	other.OldColumnName = s.OldColumnName.Clone()
	other.NewColumnName = s.NewColumnName.Clone()
	other.ColumnDef = s.ColumnDef.Clone()
	other.DropColumnName = s.DropColumnName.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *AlterTableStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE ")
	buf.WriteString(s.Name.String())

	if s.OldColumnName != nil {
		buf.WriteString(" RENAME COLUMN ")
		buf.WriteString(s.OldColumnName.String())
		buf.WriteString(" TO ")
		buf.WriteString(s.NewColumnName.String())
	} else if s.DropColumnName != nil {
		buf.WriteString(" DROP COLUMN ")
		buf.WriteString(s.DropColumnName.String())
	} else if s.ColumnDef != nil {
		buf.WriteString(" ADD COLUMN ")
		if s.AddColumn.IsValid() {
			buf.WriteString(" COLUMN ")
		}
		buf.WriteString(s.ColumnDef.String())
	}
	return buf.String()
}

type Ident struct {
	NamePos Pos    // identifier position
	Name    string // identifier name
	Quoted  bool   // true if double quoted
}

func (expr *Ident) IsLiteral() bool { return false }

func (expr *Ident) DataType() ExprDataType {
	return NewDataTypeVoid()
}

func (expr *Ident) Pos() Pos {
	return expr.NamePos
}

// Clone returns a deep copy of i.
func (i *Ident) Clone() *Ident {
	if i == nil {
		return nil
	}
	other := *i
	return &other
}

func cloneIdents(a []*Ident) []*Ident {
	if a == nil {
		return nil
	}
	other := make([]*Ident, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the expression.
func (i *Ident) String() string {
	if i.Quoted {
		return `"` + strings.Replace(i.Name, `"`, `""`, -1) + `"`
	}
	return i.Name
}

// IdentName returns the name of ident. Returns a blank string if ident is nil.
func IdentName(ident *Ident) string {
	if ident == nil {
		return ""
	}
	return ident.Name
}

type Variable struct {
	NamePos       Pos    // variable position
	Name          string // variable name
	VariableIndex int
	VarDataType   ExprDataType
}

func (expr *Variable) IsLiteral() bool { return false }

func (expr *Variable) DataType() ExprDataType {
	return expr.VarDataType
}

func (expr *Variable) Pos() Pos {
	return expr.NamePos
}

// Clone returns a deep copy of i.
func (i *Variable) Clone() *Variable {
	if i == nil {
		return nil
	}
	other := *i
	return &other
}

// String returns the string representation of the expression.
func (i *Variable) String() string {
	return i.Name
}

func (i *Variable) VarName() string {
	return i.Name[1:]
}

type Type struct {
	Name      *Ident      // type name
	Lparen    Pos         // position of left paren (optional)
	Precision *IntegerLit // precision (optional)
	Scale     *IntegerLit // scale (optional)
	Rparen    Pos         // position of right paren (optional)
}

// Clone returns a deep copy of t.
func (t *Type) Clone() *Type {
	if t == nil {
		return nil
	}
	other := *t
	other.Name = t.Name.Clone()
	other.Precision = t.Precision.Clone()
	other.Scale = t.Scale.Clone()
	return &other
}

// String returns the string representation of the type.
func (t *Type) String() string {
	if t.Precision != nil && t.Scale != nil {
		return fmt.Sprintf("%s(%s,%s)", t.Name.Name, t.Precision.String(), t.Scale.String())
	} else if t.Precision != nil {
		return fmt.Sprintf("%s(%s)", t.Name.Name, t.Precision.String())
	}
	return t.Name.Name
}

type StringLit struct {
	ValuePos Pos    // literal position
	Value    string // literal value (without quotes)
}

func (expr *StringLit) IsLiteral() bool { return true }

func (expr *StringLit) DataType() ExprDataType {
	return NewDataTypeString()
}

func (expr *StringLit) Pos() Pos {
	return expr.ValuePos
}

func (expr *StringLit) ConvertToTimestamp() *DateLit {
	//try to coerce to a date
	if tm, err := time.ParseInLocation(time.RFC3339Nano, expr.Value, time.UTC); err == nil {
		return &DateLit{ValuePos: expr.ValuePos, Value: tm}
	} else if tm, err := time.ParseInLocation(time.RFC3339, expr.Value, time.UTC); err == nil {
		return &DateLit{ValuePos: expr.ValuePos, Value: tm}
	} else if tm, err := time.ParseInLocation("2006-01-02", expr.Value, time.UTC); err == nil {
		return &DateLit{ValuePos: expr.ValuePos, Value: tm}
	} else {
		return nil
	}
}

// Clone returns a deep copy of lit.
func (lit *StringLit) Clone() *StringLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *StringLit) String() string {
	return `'` + strings.Replace(lit.Value, `'`, `''`, -1) + `'`
}

type IntegerLit struct {
	ValuePos Pos    // literal position
	Value    string // literal value
}

func (expr *IntegerLit) IsLiteral() bool { return true }

func (expr *IntegerLit) DataType() ExprDataType {
	return NewDataTypeInt()
}

func (expr *IntegerLit) Pos() Pos {
	return expr.ValuePos
}

// Clone returns a deep copy of lit.
func (lit *IntegerLit) Clone() *IntegerLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *IntegerLit) String() string {
	return lit.Value
}

type FloatLit struct {
	ValuePos Pos    // literal position
	Value    string // literal value
}

func (expr *FloatLit) IsLiteral() bool { return true }

func (expr *FloatLit) DataType() ExprDataType {
	//how many decimal places do we have on the right of the point?
	scale := NumDecimalPlaces(expr.Value)
	return NewDataTypeDecimal(int64(scale))
}

func (expr *FloatLit) Pos() Pos {
	return expr.ValuePos
}

// Clone returns a deep copy of lit.
func (lit *FloatLit) Clone() *FloatLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *FloatLit) String() string {
	return lit.Value
}

type NullLit struct {
	ValuePos Pos
}

func (expr *NullLit) IsLiteral() bool { return true }

func (expr *NullLit) DataType() ExprDataType {
	return NewDataTypeVoid()
}

func (expr *NullLit) Pos() Pos {
	return expr.ValuePos
}

// Clone returns a deep copy of lit.
func (lit *NullLit) Clone() *NullLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *NullLit) String() string {
	return "NULL"
}

type BoolLit struct {
	ValuePos Pos  // literal position
	Value    bool // literal value
}

func (expr *BoolLit) IsLiteral() bool { return true }

func (expr *BoolLit) DataType() ExprDataType {
	return NewDataTypeBool()
}

func (expr *BoolLit) Pos() Pos {
	return expr.ValuePos
}

// Clone returns a deep copy of lit.
func (lit *BoolLit) Clone() *BoolLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *BoolLit) String() string {
	if lit.Value {
		return "TRUE"
	}
	return "FALSE"
}

type DateLit struct {
	ValuePos Pos       // literal position
	Value    time.Time // literal value
}

func (expr *DateLit) IsLiteral() bool { return true }

func (expr *DateLit) DataType() ExprDataType {
	return NewDataTypeTimestamp()
}

func (expr *DateLit) Pos() Pos {
	return expr.ValuePos
}

// Clone returns a deep copy of lit.
func (lit *DateLit) Clone() *DateLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *DateLit) String() string {
	return lit.Value.Format(time.RFC3339)
}

type UnaryExpr struct {
	OpPos Pos   // operation position
	Op    Token // operation
	X     Expr  // target expression

	ResultDataType ExprDataType
}

func (expr *UnaryExpr) IsLiteral() bool {
	return expr.X.IsLiteral()
}

func (expr *UnaryExpr) DataType() ExprDataType {
	return expr.ResultDataType
}

func (expr *UnaryExpr) Pos() Pos {
	return expr.OpPos
}

// Clone returns a deep copy of expr.
func (expr *UnaryExpr) Clone() *UnaryExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	return &other
}

// String returns the string representation of the expression.
func (expr *UnaryExpr) String() string {
	switch expr.Op {
	case PLUS:
		return "+" + expr.X.String()
	case MINUS:
		return "-" + expr.X.String()
	case BITNOT:
		return "!" + expr.X.String()
	default:
		panic(fmt.Sprintf("sql.UnaryExpr.String(): invalid op %s", expr.Op))
	}
}

type BinaryExpr struct {
	X     Expr  // lhs
	OpPos Pos   // position of Op
	Op    Token // operator
	Y     Expr  // rhs

	ResultDataType ExprDataType
}

func (expr *BinaryExpr) IsLiteral() bool {
	return expr.X.IsLiteral() && expr.Y.IsLiteral()
}

func (expr *BinaryExpr) DataType() ExprDataType {
	return expr.ResultDataType
}

func (expr *BinaryExpr) Pos() Pos {
	return expr.X.Pos()
}

// Clone returns a deep copy of expr.
func (expr *BinaryExpr) Clone() *BinaryExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	other.Y = CloneExpr(expr.Y)
	return &other
}

// String returns the string representation of the expression.
func (expr *BinaryExpr) String() string {
	switch expr.Op {
	case PLUS:
		return expr.X.String() + " + " + expr.Y.String()
	case MINUS:
		return expr.X.String() + " - " + expr.Y.String()
	case STAR:
		return expr.X.String() + " * " + expr.Y.String()
	case SLASH:
		return expr.X.String() + " / " + expr.Y.String()
	case REM:
		return expr.X.String() + " % " + expr.Y.String()
	case CONCAT:
		return expr.X.String() + " || " + expr.Y.String()
	case BETWEEN:
		return expr.X.String() + " BETWEEN " + expr.Y.String()
	case NOTBETWEEN:
		return expr.X.String() + " NOT BETWEEN " + expr.Y.String()
	case LSHIFT:
		return expr.X.String() + " << " + expr.Y.String()
	case RSHIFT:
		return expr.X.String() + " >> " + expr.Y.String()
	case BITAND:
		return expr.X.String() + " & " + expr.Y.String()
	case BITOR:
		return expr.X.String() + " | " + expr.Y.String()
	case LT:
		return expr.X.String() + " < " + expr.Y.String()
	case LE:
		return expr.X.String() + " <= " + expr.Y.String()
	case GT:
		return expr.X.String() + " > " + expr.Y.String()
	case GE:
		return expr.X.String() + " >= " + expr.Y.String()
	case EQ:
		return expr.X.String() + " = " + expr.Y.String()
	case NE:
		return expr.X.String() + " != " + expr.Y.String()
	case IS:
		return expr.X.String() + " IS " + expr.Y.String()
	case ISNOT:
		return expr.X.String() + " IS NOT " + expr.Y.String()
	case IN:
		return expr.X.String() + " IN " + expr.Y.String()
	case NOTIN:
		return expr.X.String() + " NOT IN " + expr.Y.String()
	case LIKE:
		return expr.X.String() + " LIKE " + expr.Y.String()
	case NOTLIKE:
		return expr.X.String() + " NOT LIKE " + expr.Y.String()
	case GLOB:
		return expr.X.String() + " GLOB " + expr.Y.String()
	case NOTGLOB:
		return expr.X.String() + " NOT GLOB " + expr.Y.String()
	case MATCH:
		return expr.X.String() + " MATCH " + expr.Y.String()
	case NOTMATCH:
		return expr.X.String() + " NOT MATCH " + expr.Y.String()
	case REGEXP:
		return expr.X.String() + " REGEXP " + expr.Y.String()
	case NOTREGEXP:
		return expr.X.String() + " NOT REGEXP " + expr.Y.String()
	case AND:
		return expr.X.String() + " AND " + expr.Y.String()
	case OR:
		return expr.X.String() + " OR " + expr.Y.String()
	default:
		panic(fmt.Sprintf("sql.BinaryExpr.String(): invalid op %s", expr.Op))
	}
}

type CastExpr struct {
	Cast   Pos   // position of CAST keyword
	Lparen Pos   // position of left paren
	X      Expr  // target expression
	As     Pos   // position of AS keyword
	Type   *Type // cast type
	Rparen Pos   // position of right paren

	ResultDataType ExprDataType
}

func (expr *CastExpr) IsLiteral() bool {
	return expr.X.IsLiteral()
}

func (expr *CastExpr) DataType() ExprDataType {
	return expr.ResultDataType
}

func (expr *CastExpr) Pos() Pos {
	return expr.Cast
}

// Clone returns a deep copy of expr.
func (expr *CastExpr) Clone() *CastExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	other.Type = expr.Type.Clone()
	return &other
}

// String returns the string representation of the expression.
func (expr *CastExpr) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", expr.X.String(), expr.Type.String())
}

type CaseExpr struct {
	Case     Pos          // position of CASE keyword
	Operand  Expr         // optional condition after the CASE keyword
	Blocks   []*CaseBlock // list of WHEN/THEN pairs
	Else     Pos          // position of ELSE keyword
	ElseExpr Expr         // expression used by default case
	End      Pos          // position of END keyword

	ResultDataType ExprDataType
}

func (expr *CaseExpr) IsLiteral() bool { return false }

func (expr *CaseExpr) DataType() ExprDataType {
	return expr.ResultDataType
}

func (expr *CaseExpr) Pos() Pos {
	return expr.Case
}

// Clone returns a deep copy of expr.
func (expr *CaseExpr) Clone() *CaseExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.Operand = CloneExpr(expr.Operand)
	other.Blocks = cloneCaseBlocks(expr.Blocks)
	other.ElseExpr = CloneExpr(expr.ElseExpr)
	return &other
}

// String returns the string representation of the expression.
func (expr *CaseExpr) String() string {
	var buf bytes.Buffer
	buf.WriteString("CASE")
	if expr.Operand != nil {
		buf.WriteString(" ")
		buf.WriteString(expr.Operand.String())
	}
	for _, blk := range expr.Blocks {
		buf.WriteString(" ")
		buf.WriteString(blk.String())
	}
	if expr.ElseExpr != nil {
		buf.WriteString(" ELSE ")
		buf.WriteString(expr.ElseExpr.String())
	}
	buf.WriteString(" END")
	return buf.String()
}

type CaseBlock struct {
	When      Pos  // position of WHEN keyword
	Condition Expr // block condition
	Then      Pos  // position of THEN keyword
	Body      Expr // result expression
}

func (expr *CaseBlock) IsLiteral() bool { return false }

func (expr *CaseBlock) DataType() ExprDataType {
	return expr.Body.DataType()
}

func (expr *CaseBlock) Pos() Pos {
	return expr.When
}

// Clone returns a deep copy of blk.
func (blk *CaseBlock) Clone() *CaseBlock {
	if blk == nil {
		return nil
	}
	other := *blk
	other.Condition = CloneExpr(blk.Condition)
	other.Body = CloneExpr(blk.Body)
	return &other
}

func cloneCaseBlocks(a []*CaseBlock) []*CaseBlock {
	if a == nil {
		return nil
	}
	other := make([]*CaseBlock, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the block.
func (b *CaseBlock) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", b.Condition.String(), b.Body.String())
}

type Exists struct {
	Not    Pos              // position of optional NOT keyword
	Exists Pos              // position of EXISTS keyword
	Lparen Pos              // position of left paren
	Select *SelectStatement // select statement
	Rparen Pos              // position of right paren
}

func (expr *Exists) IsLiteral() bool { return false }

func (expr *Exists) DataType() ExprDataType {
	return NewDataTypeBool()
}

func (expr *Exists) Pos() Pos {
	if expr.Not.IsValid() {
		return expr.Not
	}
	return expr.Exists
}

// Clone returns a deep copy of expr.
func (expr *Exists) Clone() *Exists {
	if expr == nil {
		return nil
	}
	other := *expr
	other.Select = expr.Select.Clone()
	return &other
}

// String returns the string representation of the expression.
func (expr *Exists) String() string {
	if expr.Not.IsValid() {
		return fmt.Sprintf("NOT EXISTS (%s)", expr.Select.String())
	}
	return fmt.Sprintf("EXISTS (%s)", expr.Select.String())
}

type ExprList struct {
	Lparen Pos    // position of left paren
	Exprs  []Expr // list of expressions
	Rparen Pos    // position of right paren
}

func (expr *ExprList) IsLiteral() bool {
	for _, e := range expr.Exprs {
		if !e.IsLiteral() {
			return false
		}
	}
	return true
}

func (expr *ExprList) DataType() ExprDataType {
	return NewDataTypeVoid()
}

func (expr *ExprList) Pos() Pos {
	return expr.Lparen
}

// Clone returns a deep copy of l.
func (l *ExprList) Clone() *ExprList {
	if l == nil {
		return nil
	}
	other := *l
	other.Exprs = cloneExprs(l.Exprs)
	return &other
}

func cloneExprLists(a []*ExprList) []*ExprList {
	if a == nil {
		return nil
	}
	other := make([]*ExprList, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the expression.
func (l *ExprList) String() string {
	var buf bytes.Buffer
	buf.WriteString("(")
	for i, expr := range l.Exprs {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(expr.String())
	}
	buf.WriteString(")")
	return buf.String()
}

type Range struct {
	X   Expr // lhs expression
	And Pos  // position of AND keyword
	Y   Expr // rhs expression

	ResultDataType ExprDataType
}

func (expr *Range) IsLiteral() bool { return false }

func (expr *Range) DataType() ExprDataType {
	return expr.ResultDataType
}

func (expr *Range) Pos() Pos {
	return expr.X.Pos()
}

// Clone returns a deep copy of r.
func (r *Range) Clone() *Range {
	if r == nil {
		return nil
	}
	other := *r
	other.X = CloneExpr(r.X)
	other.Y = CloneExpr(r.Y)
	return &other
}

// String returns the string representation of the expression.
func (r *Range) String() string {
	return fmt.Sprintf("%s AND %s", r.X.String(), r.Y.String())
}

type QualifiedRef struct {
	Table       *Ident // table name
	Dot         Pos    // position of dot
	Star        Pos    // position of * (result column only)
	Column      *Ident // column name
	ColumnIndex int

	// Set by the planner; not at parse-time
	RefDataType ExprDataType
}

func (expr *QualifiedRef) IsLiteral() bool { return false }

func (expr *QualifiedRef) DataType() ExprDataType {
	return expr.RefDataType
}

func (expr *QualifiedRef) Pos() Pos {
	return expr.Table.Pos()
}

// Clone returns a deep copy of r.
func (r *QualifiedRef) Clone() *QualifiedRef {
	if r == nil {
		return nil
	}
	other := *r
	other.Table = r.Table.Clone()
	other.Column = r.Column.Clone()
	return &other
}

// String returns the string representation of the expression.
func (r *QualifiedRef) String() string {
	if r.Star.IsValid() {
		return fmt.Sprintf("%s.*", r.Table.String())
	}
	return fmt.Sprintf("%s.%s", r.Table.String(), r.Column.String())
}

type Call struct {
	Name     *Ident        // function name
	Lparen   Pos           // position of left paren
	Star     Pos           // position of *
	Distinct Pos           // position of DISTINCT keyword
	Args     []Expr        // argument list
	Rparen   Pos           // position of right paren
	Filter   *FilterClause // filter clause
	Over     *OverClause   // over clause

	ResultDataType ExprDataType
}

func (expr *Call) IsLiteral() bool { return false }

func (expr *Call) DataType() ExprDataType {
	return expr.ResultDataType
}

func (expr *Call) Pos() Pos {
	return expr.Name.Pos()
}

// Clone returns a deep copy of c.
func (c *Call) Clone() *Call {
	if c == nil {
		return nil
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Args = cloneExprs(c.Args)
	other.Filter = c.Filter.Clone()
	other.Over = c.Over.Clone()
	return &other
}

// String returns the string representation of the expression.
func (c *Call) String() string {
	var buf bytes.Buffer
	buf.WriteString(c.Name.Name)
	buf.WriteString("(")
	if c.Star.IsValid() {
		buf.WriteString("*")
	} else {
		if c.Distinct.IsValid() {
			buf.WriteString("DISTINCT")
			if len(c.Args) != 0 {
				buf.WriteString(" ")
			}
		}
		for i, arg := range c.Args {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(arg.String())
		}
	}
	buf.WriteString(")")

	if c.Filter != nil {
		buf.WriteString(" ")
		buf.WriteString(c.Filter.String())
	}

	if c.Over != nil {
		buf.WriteString(" ")
		buf.WriteString(c.Over.String())
	}

	return buf.String()
}

type FilterClause struct {
	Filter Pos  // position of FILTER keyword
	Lparen Pos  // position of left paren
	Where  Pos  // position of WHERE keyword
	X      Expr // filter expression
	Rparen Pos  // position of right paren
}

// Clone returns a deep copy of c.
func (c *FilterClause) Clone() *FilterClause {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneExpr(c.X)
	return &other
}

// String returns the string representation of the clause.
func (c *FilterClause) String() string {
	return fmt.Sprintf("FILTER (WHERE %s)", c.X.String())
}

type OverClause struct {
	Over       Pos               // position of OVER keyword
	Name       *Ident            // window name
	Definition *WindowDefinition // window definition
}

// Clone returns a deep copy of c.
func (c *OverClause) Clone() *OverClause {
	if c == nil {
		return nil
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Definition = c.Definition.Clone()
	return &other
}

// String returns the string representation of the clause.
func (c *OverClause) String() string {
	if c.Name != nil {
		return fmt.Sprintf("OVER %s", c.Name.String())
	}
	return fmt.Sprintf("OVER %s", c.Definition.String())
}

type OrderingTerm struct {
	X Expr // ordering expression

	Asc  Pos // position of ASC keyword
	Desc Pos // position of DESC keyword

	Nulls      Pos // position of NULLS keyword
	NullsFirst Pos // position of FIRST keyword
	NullsLast  Pos // position of LAST keyword
}

// Clone returns a deep copy of t.
func (t *OrderingTerm) Clone() *OrderingTerm {
	if t == nil {
		return nil
	}
	other := *t
	other.X = CloneExpr(t.X)
	return &other
}

func cloneOrderingTerms(a []*OrderingTerm) []*OrderingTerm {
	if a == nil {
		return nil
	}
	other := make([]*OrderingTerm, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the term.
func (t *OrderingTerm) String() string {
	var buf bytes.Buffer
	buf.WriteString(t.X.String())

	if t.Asc.IsValid() {
		buf.WriteString(" ASC")
	} else if t.Desc.IsValid() {
		buf.WriteString(" DESC")
	}

	if t.NullsFirst.IsValid() {
		buf.WriteString(" NULLS FIRST")
	} else if t.NullsLast.IsValid() {
		buf.WriteString(" NULLS LAST")
	}

	return buf.String()
}

type FrameSpec struct {
	Range  Pos // position of RANGE keyword
	Rows   Pos // position of ROWS keyword
	Groups Pos // position of GROUPS keyword

	Between Pos // position of BETWEEN keyword

	X           Expr // lhs expression
	UnboundedX  Pos  // position of lhs UNBOUNDED keyword
	PrecedingX  Pos  // position of lhs PRECEDING keyword
	CurrentX    Pos  // position of lhs CURRENT keyword
	CurrentRowX Pos  // position of lhs ROW keyword
	FollowingX  Pos  // position of lhs FOLLOWING keyword

	And Pos // position of AND keyword

	Y           Expr // lhs expression
	UnboundedY  Pos  // position of rhs UNBOUNDED keyword
	FollowingY  Pos  // position of rhs FOLLOWING keyword
	CurrentY    Pos  // position of rhs CURRENT keyword
	CurrentRowY Pos  // position of rhs ROW keyword
	PrecedingY  Pos  // position of rhs PRECEDING keyword

	Exclude           Pos // position of EXCLUDE keyword
	ExcludeNo         Pos // position of NO keyword after EXCLUDE
	ExcludeNoOthers   Pos // position of OTHERS keyword after EXCLUDE NO
	ExcludeCurrent    Pos // position of CURRENT keyword after EXCLUDE
	ExcludeCurrentRow Pos // position of ROW keyword after EXCLUDE CURRENT
	ExcludeGroup      Pos // position of GROUP keyword after EXCLUDE
	ExcludeTies       Pos // position of TIES keyword after EXCLUDE
}

// Clone returns a deep copy of s.
func (s *FrameSpec) Clone() *FrameSpec {
	if s == nil {
		return nil
	}
	other := *s
	other.X = CloneExpr(s.X)
	other.X = CloneExpr(s.Y)
	return &other
}

// String returns the string representation of the frame spec.
func (s *FrameSpec) String() string {
	var buf bytes.Buffer
	if s.Range.IsValid() {
		buf.WriteString("RANGE")
	} else if s.Rows.IsValid() {
		buf.WriteString("ROWS")
	} else if s.Groups.IsValid() {
		buf.WriteString("GROUPS")
	}

	if s.Between.IsValid() {
		buf.WriteString(" BETWEEN")
		if s.UnboundedX.IsValid() && s.PrecedingX.IsValid() {
			buf.WriteString(" UNBOUNDED PRECEDING")
		} else if s.X != nil && s.PrecedingX.IsValid() {
			fmt.Fprintf(&buf, " %s PRECEDING", s.X.String())
		} else if s.CurrentRowX.IsValid() {
			buf.WriteString(" CURRENT ROW")
		} else if s.X != nil && s.FollowingX.IsValid() {
			fmt.Fprintf(&buf, " %s FOLLOWING", s.X.String())
		}

		buf.WriteString(" AND")

		if s.Y != nil && s.PrecedingY.IsValid() {
			fmt.Fprintf(&buf, " %s PRECEDING", s.Y.String())
		} else if s.CurrentRowY.IsValid() {
			buf.WriteString(" CURRENT ROW")
		} else if s.Y != nil && s.FollowingY.IsValid() {
			fmt.Fprintf(&buf, " %s FOLLOWING", s.Y.String())
		} else if s.UnboundedY.IsValid() && s.FollowingY.IsValid() {
			buf.WriteString(" UNBOUNDED FOLLOWING")
		}
	} else {
		if s.UnboundedX.IsValid() && s.PrecedingX.IsValid() {
			buf.WriteString(" UNBOUNDED PRECEDING")
		} else if s.X != nil && s.PrecedingX.IsValid() {
			fmt.Fprintf(&buf, " %s PRECEDING", s.X.String())
		} else if s.CurrentRowX.IsValid() {
			buf.WriteString(" CURRENT ROW")
		}
	}

	if s.ExcludeNoOthers.IsValid() {
		buf.WriteString(" EXCLUDE NO OTHERS")
	} else if s.ExcludeCurrentRow.IsValid() {
		buf.WriteString(" EXCLUDE CURRENT ROW")
	} else if s.ExcludeGroup.IsValid() {
		buf.WriteString(" EXCLUDE GROUP")
	} else if s.ExcludeTies.IsValid() {
		buf.WriteString(" EXCLUDE TIES")
	}

	return buf.String()
}

type ColumnArg interface {
	Node
	columnArg()
}

type DropDatabaseStatement struct {
	Drop     Pos    // position of DROP keyword
	Database Pos    // position of DATABASE keyword
	If       Pos    // position of IF keyword
	IfExists Pos    // position of EXISTS keyword after IF
	Name     *Ident // database name
}

// Clone returns a deep copy of s.
func (s *DropDatabaseStatement) Clone() *DropDatabaseStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *DropDatabaseStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP DATABASE")
	if s.IfExists.IsValid() {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type DropTableStatement struct {
	Drop     Pos    // position of DROP keyword
	Table    Pos    // position of TABLE keyword
	If       Pos    // position of IF keyword
	IfExists Pos    // position of EXISTS keyword after IF
	Name     *Ident // table name
}

// Clone returns a deep copy of s.
func (s *DropTableStatement) Clone() *DropTableStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *DropTableStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP TABLE")
	if s.IfExists.IsValid() {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type CreateViewStatement struct {
	Create      Pos    // position of CREATE keyword
	View        Pos    // position of VIEW keyword
	If          Pos    // position of IF keyword
	IfNot       Pos    // position of NOT keyword after IF
	IfNotExists Pos    // position of EXISTS keyword after IF NOT
	Name        *Ident // view name
	// TODO(pok) - we'll do this later - see note in parseCompileView()
	// Lparen      Pos              // position of column list left paren
	// Columns     []*Ident         // column list
	// Rparen      Pos              // position of column list right paren
	As     Pos              // position of AS keyword
	Select *SelectStatement // source statement
}

// Clone returns a deep copy of s.
func (s *CreateViewStatement) Clone() *CreateViewStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	// other.Columns = cloneIdents(s.Columns)
	other.Select = s.Select.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *CreateViewStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE VIEW")
	if s.IfNotExists.IsValid() {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())

	// if len(s.Columns) > 0 {
	// 	buf.WriteString(" (")
	// 	for i, col := range s.Columns {
	// 		if i != 0 {
	// 			buf.WriteString(", ")
	// 		}
	// 		buf.WriteString(col.String())
	// 	}
	// 	buf.WriteString(")")
	// }

	fmt.Fprintf(&buf, " AS %s", s.Select.String())

	return buf.String()
}

type AlterViewStatement struct {
	Alter Pos    // position of CREATE keyword
	View  Pos    // position of VIEW keyword
	Name  *Ident // view name

	// TODO(pok) - we'll do this later - see note in parseCompileView()
	// Lparen  Pos              // position of column list left paren
	// Columns []*Ident         // column list
	// Rparen  Pos              // position of column list right paren
	As     Pos              // position of AS keyword
	Select *SelectStatement // source statement
}

// Clone returns a deep copy of s.
func (s *AlterViewStatement) Clone() *AlterViewStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	// other.Columns = cloneIdents(s.Columns)
	other.Select = s.Select.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *AlterViewStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("ALTER VIEW")
	fmt.Fprintf(&buf, " %s", s.Name.String())

	// if len(s.Columns) > 0 {
	// 	buf.WriteString(" (")
	// 	for i, col := range s.Columns {
	// 		if i != 0 {
	// 			buf.WriteString(", ")
	// 		}
	// 		buf.WriteString(col.String())
	// 	}
	// 	buf.WriteString(")")
	// }
	fmt.Fprintf(&buf, " AS %s", s.Select.String())
	return buf.String()
}

type DropViewStatement struct {
	Drop     Pos    // position of DROP keyword
	View     Pos    // position of VIEW keyword
	If       Pos    // position of IF keyword
	IfExists Pos    // position of EXISTS keyword after IF
	Name     *Ident // view name
}

// Clone returns a deep copy of s.
func (s *DropViewStatement) Clone() *DropViewStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *DropViewStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP VIEW")
	if s.IfExists.IsValid() {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type CreateIndexStatement struct {
	Create      Pos              // position of CREATE keyword
	Unique      Pos              // position of optional UNIQUE keyword
	Index       Pos              // position of INDEX keyword
	If          Pos              // position of IF keyword
	IfNot       Pos              // position of NOT keyword after IF
	IfNotExists Pos              // position of EXISTS keyword after IF NOT
	Name        *Ident           // index name
	On          Pos              // position of ON keyword
	Table       *Ident           // index name
	Lparen      Pos              // position of column list left paren
	Columns     []*IndexedColumn // column list
	Rparen      Pos              // position of column list right paren
	Where       Pos              // position of WHERE keyword
	WhereExpr   Expr             // conditional expression
}

// Clone returns a deep copy of s.
func (s *CreateIndexStatement) Clone() *CreateIndexStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	other.Table = s.Table.Clone()
	other.Columns = cloneIndexedColumns(s.Columns)
	other.WhereExpr = CloneExpr(s.WhereExpr)
	return &other
}

// String returns the string representation of the statement.
func (s *CreateIndexStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE")
	if s.Unique.IsValid() {
		buf.WriteString(" UNIQUE")
	}
	buf.WriteString(" INDEX")
	if s.IfNotExists.IsValid() {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s ON %s ", s.Name.String(), s.Table.String())

	buf.WriteString("(")
	for i, col := range s.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	buf.WriteString(")")

	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
	}

	return buf.String()
}

type DropIndexStatement struct {
	Drop     Pos    // position of DROP keyword
	Index    Pos    // position of INDEX keyword
	If       Pos    // position of IF keyword
	IfExists Pos    // position of EXISTS keyword after IF
	Name     *Ident // index name
}

// Clone returns a deep copy of s.
func (s *DropIndexStatement) Clone() *DropIndexStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *DropIndexStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP INDEX")
	if s.IfExists.IsValid() {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type ParameterDefinition struct {
	Name *Variable // parameter name
	Type *Type     // data type
}

type CreateFunctionStatement struct {
	Create      Pos    // position of CREATE keyword
	Function    Pos    // position of FUNCTION keyword
	If          Pos    // position of IF keyword
	IfNot       Pos    // position of NOT keyword after IF
	IfNotExists Pos    // position of EXISTS keyword after IF NOT
	Name        *Ident // index name

	Lparen     Pos                    // position of parameter LParen
	Parameters []*ParameterDefinition // parameters
	Rparen     Pos                    // position of parameter RParen

	Returns   Pos                  // position of RETURNS keyword
	ReturnDef *ParameterDefinition // return def

	As Pos // position of AS keyword

	Begin Pos         // position of BEGIN keyword
	Body  []Statement // function body
	End   Pos         // position of END keyword
}

// Clone returns a deep copy of s.
func (s *CreateFunctionStatement) Clone() *CreateFunctionStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	other.Body = cloneStatements(s.Body)
	return &other
}

// String returns the string representation of the statement.
func (s *CreateFunctionStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE FUNCTION")
	if s.IfNotExists.IsValid() {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())

	if len(s.Parameters) > 0 {
		buf.WriteString(" (")
		for idx, p := range s.Parameters {
			if idx > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "%s %s", p.Name.Name, p.Type.Name)
		}
		buf.WriteString(")")
	}

	buf.WriteString(" RETURNS ")
	fmt.Fprintf(&buf, "%s %s", s.ReturnDef.Name, s.ReturnDef.Type.Name)

	buf.WriteString(" AS BEGIN")
	for i := range s.Body {
		fmt.Fprintf(&buf, " %s;", s.Body[i].String())
	}
	buf.WriteString(" END")

	return buf.String()
}

type DropFunctionStatement struct {
	Drop     Pos    // position of DROP keyword
	Trigger  Pos    // position of TRIGGER keyword
	If       Pos    // position of IF keyword
	IfExists Pos    // position of EXISTS keyword after IF
	Name     *Ident // trigger name
}

// Clone returns a deep copy of s.
func (s *DropFunctionStatement) Clone() *DropFunctionStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

func (s *DropFunctionStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP TRIGGER")
	if s.IfExists.IsValid() {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type BulkInsertMapDefinition struct {
	Name    *Ident // map name
	Type    *Type  // data type
	MapExpr Expr
}

// Clone returns a deep copy of d.
func (d *BulkInsertMapDefinition) Clone() *BulkInsertMapDefinition {
	if d == nil {
		return d
	}
	other := *d
	other.Name = d.Name.Clone()
	other.Type = d.Type.Clone()
	//other.MapExpr = d.MapExpr.Clone()
	return &other
}

// String returns the string representation of the statement.
func (c *BulkInsertMapDefinition) String() string {
	var buf bytes.Buffer
	buf.WriteString(c.MapExpr.String())
	buf.WriteString(" ")
	buf.WriteString(c.Name.String())
	buf.WriteString(" ")
	buf.WriteString(c.Type.String())
	return buf.String()
}

type BulkInsertStatement struct {
	Bulk    Pos // position of BULK keyword
	Insert  Pos // position of INSERT keyword
	Replace Pos // position of REPLACE keyword
	Into    Pos // position of INTO keyword

	Table *Ident // table name

	ColumnsLparen Pos      // position of column list left paren
	Columns       []*Ident // optional column list
	ColumnsRparen Pos      // position of column list right paren

	Map       Pos                        // position of MAP keyword
	MapLparen Pos                        // position of column list left paren
	MapList   []*BulkInsertMapDefinition // source to column map
	MapRparen Pos                        // position of column list right paren

	Transform       Pos    // position of MAP keyword
	TransformLparen Pos    // position of column list left paren
	TransformList   []Expr // source to column map
	TransformRparen Pos    // position of column list right paren

	From               Pos  // position of FROM keyword
	DataSource         Expr // data source
	With               Pos  // position of WITH keyword
	BatchSize          Expr
	RowsLimit          Expr
	Format             Expr
	Input              Expr
	HeaderRow          Expr // has header row (that needs to be skipped)
	AllowMissingValues Expr // allows missing values
}

func (s *BulkInsertStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("BULK ")
	if s.Replace.IsValid() {
		buf.WriteString("REPLACE")
	} else {
		buf.WriteString("INSERT")
	}
	buf.WriteString(" INTO ")
	fmt.Fprintf(&buf, " %s", s.Table.String())

	if s.Columns != nil {
		buf.WriteString("(")

		for i, col := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}

		buf.WriteString(")")
	}
	buf.WriteString(" MAP ")
	buf.WriteString("(")
	for i, m := range s.MapList {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(m.String())
	}
	buf.WriteString(")")
	if s.TransformList != nil {
		buf.WriteString(" TRANSFORM ")
		buf.WriteString("(")

		for i, t := range s.TransformList {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(t.String())
		}

		buf.WriteString(")")
	}

	buf.WriteString(" FROM ")
	fmt.Fprintf(&buf, " %s", s.DataSource.String())
	buf.WriteString(" WITH ")

	if s.Format != nil {
		buf.WriteString("FORMAT ")
		buf.WriteString(s.Format.String())
	}

	if s.Input != nil {
		buf.WriteString("INPUT ")
		buf.WriteString(s.Input.String())
	}

	if s.HeaderRow != nil {
		buf.WriteString("HEADER_ROW ")
	}

	if s.BatchSize != nil {
		buf.WriteString("BATCHSIZE ")
		buf.WriteString(s.BatchSize.String())
	}

	if s.RowsLimit != nil {
		buf.WriteString("ROWSLIMIT ")
		buf.WriteString(s.RowsLimit.String())
	}

	return buf.String()
}

type InsertStatement struct {
	//WithClause *WithClause // clause containing CTEs

	Insert          Pos // position of INSERT keyword
	Replace         Pos // position of REPLACE keyword
	InsertOr        Pos // position of OR keyword after INSERT
	InsertOrReplace Pos // position of REPLACE keyword after INSERT OR
	//	InsertOrRollback Pos // position of ROLLBACK keyword after INSERT OR
	//	InsertOrAbort    Pos // position of ABORT keyword after INSERT OR
	//	InsertOrFail     Pos // position of FAIL keyword after INSERT OR
	//	InsertOrIgnore   Pos // position of IGNORE keyword after INSERT OR
	Into Pos // position of INTO keyword

	Table *Ident // table name
	As    Pos    // position of AS keyword
	Alias *Ident // optional alias

	ColumnsLparen Pos      // position of column list left paren
	Columns       []*Ident // optional column list
	ColumnsRparen Pos      // position of column list right paren

	Values    Pos         // position of VALUES keyword
	TupleList []*ExprList // multiple tuples

	//	Select *SelectStatement // SELECT statement

	//	Default       Pos // position of DEFAULT keyword
	//	DefaultValues Pos // position of VALUES keyword after DEFAULT

	//	UpsertClause *UpsertClause // optional upsert clause
}

// Clone returns a deep copy of s.
func (s *InsertStatement) Clone() *InsertStatement {
	if s == nil {
		return nil
	}
	other := *s
	//other.WithClause = s.WithClause.Clone()
	other.Table = s.Table.Clone()
	other.Alias = s.Alias.Clone()
	other.Columns = cloneIdents(s.Columns)
	other.TupleList = cloneExprLists(s.TupleList)
	//other.Select = s.Select.Clone()
	//other.UpsertClause = s.UpsertClause.Clone()
	return &other
}

func (s *InsertStatement) String() string {
	var buf bytes.Buffer
	//if s.WithClause != nil {
	//	buf.WriteString(s.WithClause.String())
	//	buf.WriteString(" ")
	//}

	if s.Replace.IsValid() {
		buf.WriteString("REPLACE")
	} else {
		buf.WriteString("INSERT")
	}
	/*if s.InsertOrReplace.IsValid() {
		buf.WriteString(" OR REPLACE")
		//} else if s.InsertOrRollback.IsValid() {
		//	buf.WriteString(" OR ROLLBACK")
		//} else if s.InsertOrAbort.IsValid() {
		//	buf.WriteString(" OR ABORT")
		//} else if s.InsertOrFail.IsValid() {
		//	buf.WriteString(" OR FAIL")
		//} else if s.InsertOrIgnore.IsValid() {
		//	buf.WriteString(" OR IGNORE")
		//}
	}*/

	fmt.Fprintf(&buf, " INTO %s ", s.Table.String())
	if s.Alias != nil {
		fmt.Fprintf(&buf, "AS %s ", s.Alias.String())
	}

	if len(s.Columns) != 0 {
		buf.WriteString("(")
		for i, col := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	//if s.DefaultValues.IsValid() {
	//	buf.WriteString(" DEFAULT VALUES")
	//} else if s.Select != nil {
	//	fmt.Fprintf(&buf, " %s", s.Select.String())
	//} else {
	buf.WriteString(" VALUES")
	for i, tuple := range s.TupleList {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(" (")
		for j, expr := range tuple.Exprs {
			if j != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(expr.String())
		}
		buf.WriteString(")")
	}
	//}

	//if s.UpsertClause != nil {
	//	fmt.Fprintf(&buf, " %s", s.UpsertClause.String())
	//}

	return buf.String()
}

type UpsertClause struct {
	On         Pos // position of ON keyword
	OnConflict Pos // position of CONFLICT keyword after ON

	Lparen    Pos              // position of column list left paren
	Columns   []*IndexedColumn // optional indexed column list
	Rparen    Pos              // position of column list right paren
	Where     Pos              // position of WHERE keyword
	WhereExpr Expr             // optional conditional expression

	Do              Pos           // position of DO keyword
	DoNothing       Pos           // position of NOTHING keyword after DO
	DoUpdate        Pos           // position of UPDATE keyword after DO
	DoUpdateSet     Pos           // position of SET keyword after DO UPDATE
	Assignments     []*Assignment // list of column assignments
	UpdateWhere     Pos           // position of WHERE keyword for DO UPDATE SET
	UpdateWhereExpr Expr          // optional conditional expression for DO UPDATE SET
}

// Clone returns a deep copy of c.
func (c *UpsertClause) Clone() *UpsertClause {
	if c == nil {
		return nil
	}
	other := *c
	other.Columns = cloneIndexedColumns(c.Columns)
	other.WhereExpr = CloneExpr(c.WhereExpr)
	other.Assignments = cloneAssignments(c.Assignments)
	other.UpdateWhereExpr = CloneExpr(c.UpdateWhereExpr)
	return &other
}

// String returns the string representation of the clause.
func (c *UpsertClause) String() string {
	var buf bytes.Buffer
	buf.WriteString("ON CONFLICT")

	if len(c.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")

		if c.WhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", c.WhereExpr.String())
		}
	}

	buf.WriteString(" DO")
	if c.DoNothing.IsValid() {
		buf.WriteString(" NOTHING")
	} else {
		buf.WriteString(" UPDATE SET ")
		for i := range c.Assignments {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Assignments[i].String())
		}

		if c.UpdateWhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", c.UpdateWhereExpr.String())
		}
	}

	return buf.String()
}

type UpdateStatement struct {
	WithClause *WithClause // clause containing CTEs

	Update           Pos // position of UPDATE keyword
	UpdateOr         Pos // position of OR keyword after UPDATE
	UpdateOrReplace  Pos // position of REPLACE keyword after UPDATE OR
	UpdateOrRollback Pos // position of ROLLBACK keyword after UPDATE OR
	UpdateOrAbort    Pos // position of ABORT keyword after UPDATE OR
	UpdateOrFail     Pos // position of FAIL keyword after UPDATE OR
	UpdateOrIgnore   Pos // position of IGNORE keyword after UPDATE OR

	Table *QualifiedTableName // table name

	Set         Pos           // position of SET keyword
	Assignments []*Assignment // list of column assignments
	Where       Pos           // position of WHERE keyword
	WhereExpr   Expr          // conditional expression
}

// Clone returns a deep copy of s.
func (s *UpdateStatement) Clone() *UpdateStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.WithClause = s.WithClause.Clone()
	other.Table = s.Table.Clone()
	other.Assignments = cloneAssignments(s.Assignments)
	other.WhereExpr = CloneExpr(s.WhereExpr)
	return &other
}

// String returns the string representation of the clause.
func (s *UpdateStatement) String() string {
	var buf bytes.Buffer
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	buf.WriteString("UPDATE")
	if s.UpdateOrRollback.IsValid() {
		buf.WriteString(" OR ROLLBACK")
	} else if s.UpdateOrAbort.IsValid() {
		buf.WriteString(" OR ABORT")
	} else if s.UpdateOrReplace.IsValid() {
		buf.WriteString(" OR REPLACE")
	} else if s.UpdateOrFail.IsValid() {
		buf.WriteString(" OR FAIL")
	} else if s.UpdateOrIgnore.IsValid() {
		buf.WriteString(" OR IGNORE")
	}

	fmt.Fprintf(&buf, " %s ", s.Table.String())

	buf.WriteString("SET ")
	for i := range s.Assignments {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(s.Assignments[i].String())
	}

	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
	}

	return buf.String()
}

type DeleteStatement struct {
	//	WithClause *WithClause    // clause containing CTEs
	Delete    Pos                 // position of UPDATE keyword
	From      Pos                 // position of FROM keyword
	TableName *QualifiedTableName // the name of the table we are deleting from
	Source    Source              // source for the delete

	Where     Pos  // position of WHERE keyword
	WhereExpr Expr // conditional expression
}

// Clone returns a deep copy of s.
func (s *DeleteStatement) Clone() *DeleteStatement {
	if s == nil {
		return nil
	}
	other := *s
	//other.WithClause = s.WithClause.Clone()
	other.Source = CloneSource(s.Source)
	other.WhereExpr = CloneExpr(s.WhereExpr)
	return &other
}

// String returns the string representation of the clause.
func (s *DeleteStatement) String() string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "DELETE FROM %s", s.TableName.String())
	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
	}
	return buf.String()
}

// Assignment is used within the UPDATE statement & upsert clause.
// It is similiar to an expression except that it must be an equality.
type Assignment struct {
	Lparen  Pos      // position of column list left paren
	Columns []*Ident // column list
	Rparen  Pos      // position of column list right paren
	Eq      Pos      // position of =
	Expr    Expr     // assigned expression
}

// Clone returns a deep copy of a.
func (a *Assignment) Clone() *Assignment {
	if a == nil {
		return nil
	}
	other := *a
	other.Columns = cloneIdents(a.Columns)
	other.Expr = CloneExpr(a.Expr)
	return &other
}

func cloneAssignments(a []*Assignment) []*Assignment {
	if a == nil {
		return nil
	}
	other := make([]*Assignment, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the clause.
func (a *Assignment) String() string {
	var buf bytes.Buffer
	if len(a.Columns) == 1 {
		buf.WriteString(a.Columns[0].String())
	} else if len(a.Columns) > 1 {
		buf.WriteString("(")
		for i, col := range a.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " = %s", a.Expr.String())
	return buf.String()
}

type IndexedColumn struct {
	X    Expr // column expression
	Asc  Pos  // position of optional ASC keyword
	Desc Pos  // position of optional DESC keyword
}

// Clone returns a deep copy of c.
func (c *IndexedColumn) Clone() *IndexedColumn {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneExpr(c.X)
	return &other
}

func cloneIndexedColumns(a []*IndexedColumn) []*IndexedColumn {
	if a == nil {
		return nil
	}
	other := make([]*IndexedColumn, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the column.
func (c *IndexedColumn) String() string {
	if c.Asc.IsValid() {
		return fmt.Sprintf("%s ASC", c.X.String())
	} else if c.Desc.IsValid() {
		return fmt.Sprintf("%s DESC", c.X.String())
	}
	return c.X.String()
}

type SelectStatement struct {
	WithClause *WithClause // clause containing CTEs

	//	Values     Pos         // position of VALUES keyword
	//	ValueLists []*ExprList // lists of lists of values

	Select   Pos // position of SELECT keyword
	Distinct Pos // position of DISTINCT keyword
	//	All      Pos             // position of ALL keyword
	Columns []*ResultColumn // list of result columns in the SELECT clause

	Top     Pos  // position of TOP keyword
	TopN    Pos  // position of TOPN keyword
	TopExpr Expr // TOP expr

	From   Pos    // position of FROM keyword
	Source Source // chain of tables & subqueries in FROM clause

	Where     Pos  // position of WHERE keyword
	WhereExpr Expr // condition for WHERE clause

	Group        Pos    // position of GROUP keyword
	GroupBy      Pos    // position of BY keyword after GROUP
	GroupByExprs []Expr // group by expression list
	Having       Pos    // position of HAVING keyword
	HavingExpr   Expr   // HAVING expression

	Window  Pos       // position of WINDOW keyword
	Windows []*Window // window list

	Union     Pos              // position of UNION keyword
	UnionAll  Pos              // position of ALL keyword after UNION
	Intersect Pos              // position of INTERSECT keyword
	Except    Pos              // position of EXCEPT keyword
	Compound  *SelectStatement // compounded SELECT statement

	Order         Pos             // position of ORDER keyword
	OrderBy       Pos             // position of BY keyword after ORDER
	OrderingTerms []*OrderingTerm // terms of ORDER BY clause

}

// Clone returns a deep copy of s.
func (s *SelectStatement) Clone() *SelectStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.WithClause = s.WithClause.Clone()
	//other.ValueLists = cloneExprLists(s.ValueLists)
	other.TopExpr = CloneExpr(s.TopExpr)
	other.Columns = cloneResultColumns(s.Columns)
	other.Source = CloneSource(s.Source)
	other.WhereExpr = CloneExpr(s.WhereExpr)
	other.GroupByExprs = cloneExprs(s.GroupByExprs)
	other.HavingExpr = CloneExpr(s.HavingExpr)
	other.Windows = cloneWindows(s.Windows)
	other.Compound = s.Compound.Clone()
	other.OrderingTerms = cloneOrderingTerms(s.OrderingTerms)
	return &other
}

func (expr *SelectStatement) IsLiteral() bool { return false }

// HasWildcard returns true any result column contains a wildcard (STAR).
func (s *SelectStatement) HasWildcard() bool {
	for _, col := range s.Columns {
		// Unqualified wildcard.
		if col.Star.IsValid() {
			return true
		}

		// Table-qualified wildcard.
		if ref, ok := col.Expr.(*QualifiedRef); ok && ref.Star.IsValid() {
			return true
		}
	}

	return false
}

func (s *SelectStatement) DataType() ExprDataType {
	return nil
}

func (s *SelectStatement) Pos() Pos {
	return s.Select
}

// String returns the string representation of the statement.
func (s *SelectStatement) String() string {
	var buf bytes.Buffer
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	/*if len(s.ValueLists) > 0 {
		buf.WriteString("VALUES ")
		for i, exprs := range s.ValueLists {
			if i != 0 {
				buf.WriteString(", ")
			}

			buf.WriteString("(")
			for j, expr := range exprs.Exprs {
				if j != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(expr.String())
			}
			buf.WriteString(")")
		}
	} else {*/
	buf.WriteString("SELECT ")
	if s.Distinct.IsValid() {
		buf.WriteString("DISTINCT ")
	} //else if s.All.IsValid() {
	//	buf.WriteString("ALL ")
	//}
	if s.Top.IsValid() {
		fmt.Fprintf(&buf, "TOP(%s) ", s.TopExpr.String())
	}
	if s.TopN.IsValid() {
		fmt.Fprintf(&buf, "TOPN(%s) ", s.TopExpr.String())
	}

	for i, col := range s.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}

	if s.Source != nil {
		fmt.Fprintf(&buf, " FROM %s", s.Source.String())
	}

	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
	}

	if len(s.GroupByExprs) != 0 {
		buf.WriteString(" GROUP BY ")
		for i, expr := range s.GroupByExprs {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(expr.String())
		}

		if s.HavingExpr != nil {
			fmt.Fprintf(&buf, " HAVING %s", s.HavingExpr.String())
		}
	}

	if len(s.Windows) != 0 {
		buf.WriteString(" WINDOW ")
		for i, window := range s.Windows {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(window.String())
		}
	}
	//	}

	// Write compound operator.
	if s.Compound != nil {
		switch {
		case s.Union.IsValid():
			buf.WriteString(" UNION")
			if s.UnionAll.IsValid() {
				buf.WriteString(" ALL")
			}
		case s.Intersect.IsValid():
			buf.WriteString(" INTERSECT")
		case s.Except.IsValid():
			buf.WriteString(" EXCEPT")
		}

		fmt.Fprintf(&buf, " %s", s.Compound.String())
	}

	// Write ORDER BY.
	if len(s.OrderingTerms) != 0 {
		buf.WriteString(" ORDER BY ")
		for i, term := range s.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	return buf.String()
}

func (c *SelectStatement) SourceFromAlias(alias string) Source {
	return nil
}

func (c *SelectStatement) PossibleOutputColumns() []*SourceOutputColumn {
	result := make([]*SourceOutputColumn, 0)
	// populate the output columns from the columns in the select list
	for idx, col := range c.Columns {
		soc := &SourceOutputColumn{
			TableName:   "",
			ColumnName:  col.Name(),
			ColumnIndex: idx,
			Datatype:    col.Expr.DataType(),
		}
		result = append(result, soc)
	}
	return result
}

func (c *SelectStatement) OutputColumnNamed(name string) (*SourceOutputColumn, error) {
	ocs := c.PossibleOutputColumns()

	for _, oc := range ocs {
		if strings.EqualFold(oc.ColumnName, name) {
			return oc, nil
		}
	}
	return nil, nil
}

func (c *SelectStatement) OutputColumnQualifierNamed(qualifier string, name string) (*SourceOutputColumn, error) {
	return nil, nil
}

type ResultColumn struct {
	Star  Pos    // position of *
	Expr  Expr   // column expression (may be "tbl.*")
	As    Pos    // position of AS keyword
	Alias *Ident // alias name
}

// Name returns the column name. Uses the alias, if specified.
// Otherwise returns a generated name.
func (c *ResultColumn) Name() string {
	if c.Alias != nil {
		return IdentName(c.Alias)
	}

	switch expr := c.Expr.(type) {
	case *Ident:
		return IdentName(expr)
	case *QualifiedRef:
		return IdentName(expr.Column)
	default:
		return ""
	}
}

func (expr *ResultColumn) IsLiteral() bool { return false }

// Clone returns a deep copy of c.
func (c *ResultColumn) Clone() *ResultColumn {
	if c == nil {
		return nil
	}
	other := *c
	other.Expr = CloneExpr(c.Expr)
	other.Alias = c.Alias.Clone()
	return &other
}

func cloneResultColumns(a []*ResultColumn) []*ResultColumn {
	if a == nil {
		return nil
	}
	other := make([]*ResultColumn, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the column.
func (c *ResultColumn) String() string {
	if c.Star.IsValid() {
		return "*"
	} else if c.Alias != nil {
		return fmt.Sprintf("%s AS %s", c.Expr.String(), c.Alias.String())
	}
	return c.Expr.String()
}

type QualifiedTableName struct {
	Name          *Ident                // table name
	As            Pos                   // position of AS keyword
	Alias         *Ident                // optional table alias
	Indexed       Pos                   // position of INDEXED keyword
	IndexedBy     Pos                   // position of BY keyword after INDEXED
	Not           Pos                   // position of NOT keyword before INDEXED
	NotIndexed    Pos                   // position of NOT keyword before INDEXED
	Index         *Ident                // name of index
	OutputColumns []*SourceOutputColumn // output columns - populated during analysis
}

// TableName returns the name used to identify n.
// Returns the alias, if one is specified. Otherwise returns the name.
func (n *QualifiedTableName) TableName() string {
	if s := IdentName(n.Alias); s != "" {
		return s
	}
	return IdentName(n.Name)
}

func (n *QualifiedTableName) MatchesTablenameOrAlias(match string) bool {
	return strings.EqualFold(IdentName(n.Alias), match) || strings.EqualFold(IdentName(n.Name), match)
}

// Clone returns a deep copy of n.
func (n *QualifiedTableName) Clone() *QualifiedTableName {
	if n == nil {
		return nil
	}
	other := *n
	other.Name = n.Name.Clone()
	other.Alias = n.Alias.Clone()
	other.Index = n.Index.Clone()
	return &other
}

// String returns the string representation of the table name.
func (n *QualifiedTableName) String() string {
	var buf bytes.Buffer
	buf.WriteString(n.Name.String())
	if n.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", n.Alias.String())
	}

	if n.Index != nil {
		fmt.Fprintf(&buf, " INDEXED BY %s", n.Index.String())
	} else if n.NotIndexed.IsValid() {
		buf.WriteString(" NOT INDEXED")
	}
	return buf.String()
}

func (c *QualifiedTableName) SourceFromAlias(alias string) Source {
	if strings.EqualFold(IdentName(c.Alias), alias) {
		return c
	}
	if strings.EqualFold(IdentName(c.Name), alias) {
		return c
	}
	return nil
}

func (c *QualifiedTableName) PossibleOutputColumns() []*SourceOutputColumn {
	return c.OutputColumns
}

func (c *QualifiedTableName) OutputColumnNamed(name string) (*SourceOutputColumn, error) {
	for _, oc := range c.OutputColumns {
		if strings.EqualFold(oc.ColumnName, name) {
			return oc, nil
		}
	}
	return nil, nil
}

func (c *QualifiedTableName) OutputColumnQualifierNamed(qualifier string, name string) (*SourceOutputColumn, error) {
	if strings.EqualFold(IdentName(c.Alias), qualifier) || strings.EqualFold(IdentName(c.Name), qualifier) {
		return c.OutputColumnNamed(name)
	}
	return nil, nil
}

type TableValuedFunction struct {
	Name          *Ident                // table name
	As            Pos                   // position of AS keyword
	Alias         *Ident                // optional table alias
	Call          *Call                 // call
	OutputColumns []*SourceOutputColumn // output columns - populated during analysis
}

// TableName returns the name used to identify n.
// Returns the alias, if one is specified. Otherwise returns the name.
func (n *TableValuedFunction) TableName() string {
	if s := IdentName(n.Alias); s != "" {
		return s
	}
	return IdentName(n.Name)
}

func (n *TableValuedFunction) MatchesTablenameOrAlias(match string) bool {
	return strings.EqualFold(IdentName(n.Alias), match) || strings.EqualFold(IdentName(n.Name), match)
}

// Clone returns a deep copy of n.
func (n *TableValuedFunction) Clone() *TableValuedFunction {
	if n == nil {
		return nil
	}
	other := *n
	other.Name = n.Name.Clone()
	other.Alias = n.Alias.Clone()
	return &other
}

// String returns the string representation of the table name.
func (n *TableValuedFunction) String() string {
	var buf bytes.Buffer
	buf.WriteString(n.Name.String())
	if n.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", n.Alias.String())
	}

	return buf.String()
}

func (c *TableValuedFunction) SourceFromAlias(alias string) Source {
	if strings.EqualFold(IdentName(c.Alias), alias) {
		return c
	}
	if strings.EqualFold(IdentName(c.Name), alias) {
		return c
	}
	return nil
}

func (c *TableValuedFunction) PossibleOutputColumns() []*SourceOutputColumn {
	return c.OutputColumns
}

func (c *TableValuedFunction) OutputColumnNamed(name string) (*SourceOutputColumn, error) {
	for _, oc := range c.OutputColumns {
		if strings.EqualFold(oc.ColumnName, name) {
			return oc, nil
		}
	}
	return nil, nil
}

func (c *TableValuedFunction) OutputColumnQualifierNamed(qualifier string, name string) (*SourceOutputColumn, error) {
	if strings.EqualFold(IdentName(c.Alias), qualifier) || strings.EqualFold(IdentName(c.Name), qualifier) {
		return c.OutputColumnNamed(name)
	}
	return nil, nil
}

type ParenSource struct {
	Lparen Pos    // position of left paren
	X      Source // nested source
	Rparen Pos    // position of right paren
	As     Pos    // position of AS keyword (select source only)
	Alias  *Ident // optional table alias (select source only)
}

// Clone returns a deep copy of s.
func (s *ParenSource) Clone() *ParenSource {
	if s == nil {
		return nil
	}
	other := *s
	other.X = CloneSource(s.X)
	other.Alias = s.Alias.Clone()
	return &other
}

// String returns the string representation of the source.
func (s *ParenSource) String() string {
	if s.Alias != nil {
		return fmt.Sprintf("(%s) AS %s", s.X.String(), s.Alias.String())
	}
	return fmt.Sprintf("(%s)", s.X.String())
}

func (c *ParenSource) SourceFromAlias(alias string) Source {
	if strings.EqualFold(IdentName(c.Alias), alias) {
		return c
	}
	return c.X.SourceFromAlias(alias)
}

func (c *ParenSource) PossibleOutputColumns() []*SourceOutputColumn {
	aliasName := IdentName(c.Alias)
	poc := c.X.PossibleOutputColumns()
	for _, pc := range poc {
		pc.TableName = aliasName
	}
	return poc
}

func (c *ParenSource) OutputColumnNamed(name string) (*SourceOutputColumn, error) {
	return c.X.OutputColumnNamed(name)
}

func (c *ParenSource) OutputColumnQualifierNamed(qualifier string, name string) (*SourceOutputColumn, error) {
	if strings.EqualFold(IdentName(c.Alias), qualifier) {
		return c.OutputColumnNamed(name)
	}
	return nil, nil
}

type JoinClause struct {
	X          Source         // lhs source
	Operator   *JoinOperator  // join operator
	Y          Source         // rhs source
	Constraint JoinConstraint // join constraint
}

// Clone returns a deep copy of c.
func (c *JoinClause) Clone() *JoinClause {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneSource(c.X)
	other.Y = CloneSource(c.Y)
	other.Constraint = CloneJoinConstraint(c.Constraint)
	return &other
}

// String returns the string representation of the clause.
func (c *JoinClause) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s%s%s", c.X.String(), c.Operator.String(), c.Y.String())
	if c.Constraint != nil {
		fmt.Fprintf(&buf, " %s", c.Constraint.String())
	}
	return buf.String()
}

func (c *JoinClause) PossibleOutputColumns() []*SourceOutputColumn {
	poc := make([]*SourceOutputColumn, 0)
	poc = append(poc, c.X.PossibleOutputColumns()...)
	poc = append(poc, c.Y.PossibleOutputColumns()...)
	return poc

}

func (c *JoinClause) OutputColumnNamed(name string) (*SourceOutputColumn, error) {
	if col, err := c.X.OutputColumnNamed(name); err != nil {
		return nil, err
	} else if col != nil {
		return col, nil
	}

	if col, err := c.Y.OutputColumnNamed(name); err != nil {
		return nil, err
	} else if col != nil {
		return col, nil
	}

	return nil, nil
}

func (c *JoinClause) OutputColumnQualifierNamed(qualifier string, name string) (*SourceOutputColumn, error) {
	if col, err := c.X.OutputColumnQualifierNamed(qualifier, name); err != nil {
		return nil, err
	} else if col != nil {
		return col, nil
	}

	if col, err := c.Y.OutputColumnQualifierNamed(qualifier, name); err != nil {
		return nil, err
	} else if col != nil {
		return col, nil
	}

	return nil, nil
}

func (c *JoinClause) SourceFromAlias(alias string) Source {
	if src := c.X.SourceFromAlias(alias); src != nil {
		return src
	}
	if src := c.Y.SourceFromAlias(alias); src != nil {
		return src
	}
	return nil
}

type JoinOperator struct {
	Comma Pos // position of comma
	Left  Pos // position of LEFT keyword
	Right Pos // position of RIGHT keyword
	Full  Pos // position of FULL keyword
	Outer Pos // position of OUTER keyword
	Inner Pos // position of INNER keyword
	//	Cross   Pos // position of CROSS keyword // TODO(pok) - add cross back when we do it
	Join Pos // position of JOIN keyword
}

// Clone returns a deep copy of op.
func (op *JoinOperator) Clone() *JoinOperator {
	if op == nil {
		return nil
	}
	other := *op
	return &other
}

// String returns the string representation of the operator.
func (op *JoinOperator) String() string {
	if op.Comma.IsValid() {
		return ", "
	}

	var buf bytes.Buffer
	if op.Left.IsValid() {
		buf.WriteString(" LEFT")
		if op.Outer.IsValid() {
			buf.WriteString(" OUTER")
		}
	} else if op.Inner.IsValid() {
		buf.WriteString(" INNER")
		// } else if op.Cross.IsValid() {
		// 	buf.WriteString(" CROSS")
	}
	buf.WriteString(" JOIN ")

	return buf.String()
}

type OnConstraint struct {
	On Pos  // position of ON keyword
	X  Expr // constraint expression
}

// Clone returns a deep copy of c.
func (c *OnConstraint) Clone() *OnConstraint {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneExpr(c.X)
	return &other
}

// String returns the string representation of the constraint.
func (c *OnConstraint) String() string {
	return "ON " + c.X.String()
}

type UsingConstraint struct {
	Using   Pos      // position of USING keyword
	Lparen  Pos      // position of left paren
	Columns []*Ident // column list
	Rparen  Pos      // position of right paren
}

// Clone returns a deep copy of c.
func (c *UsingConstraint) Clone() *UsingConstraint {
	if c == nil {
		return nil
	}
	other := *c
	other.Columns = cloneIdents(c.Columns)
	return &other
}

// String returns the string representation of the constraint.
func (c *UsingConstraint) String() string {
	var buf bytes.Buffer
	buf.WriteString("USING (")
	for i, col := range c.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	buf.WriteString(")")
	return buf.String()
}

type WithClause struct {
	With      Pos    // position of WITH keyword
	Recursive Pos    // position of RECURSIVE keyword
	CTEs      []*CTE // common table expressions
}

// Clone returns a deep copy of c.
func (c *WithClause) Clone() *WithClause {
	if c == nil {
		return nil
	}
	other := *c
	other.CTEs = cloneCTEs(c.CTEs)
	return &other
}

// String returns the string representation of the clause.
func (c *WithClause) String() string {
	var buf bytes.Buffer
	buf.WriteString("WITH ")
	if c.Recursive.IsValid() {
		buf.WriteString("RECURSIVE ")
	}

	for i, cte := range c.CTEs {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(cte.String())
	}

	return buf.String()
}

// CTE represents an AST node for a common table expression.
type CTE struct {
	TableName     *Ident           // table name
	ColumnsLparen Pos              // position of column list left paren
	Columns       []*Ident         // optional column list
	ColumnsRparen Pos              // position of column list right paren
	As            Pos              // position of AS keyword
	SelectLparen  Pos              // position of select left paren
	Select        *SelectStatement // select statement
	SelectRparen  Pos              // position of select right paren
}

// Clone returns a deep copy of cte.
func (cte *CTE) Clone() *CTE {
	if cte == nil {
		return nil
	}
	other := *cte
	other.TableName = cte.TableName.Clone()
	other.Columns = cloneIdents(cte.Columns)
	other.Select = cte.Select.Clone()
	return &other
}

func cloneCTEs(a []*CTE) []*CTE {
	if a == nil {
		return nil
	}
	other := make([]*CTE, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the CTE.
func (cte *CTE) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s", cte.TableName.String())

	if len(cte.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range cte.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " AS (%s)", cte.Select.String())

	return buf.String()
}

type ParenExpr struct {
	Lparen Pos  // position of left paren
	X      Expr // parenthesized expression
	Rparen Pos  // position of right paren
}

func (expr *ParenExpr) IsLiteral() bool {
	return expr.X.IsLiteral()
}

func (expr *ParenExpr) DataType() ExprDataType {
	return expr.X.DataType()
}

func (expr *ParenExpr) Pos() Pos {
	return expr.Lparen
}

// Clone returns a deep copy of expr.
func (expr *ParenExpr) Clone() *ParenExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	return &other
}

// String returns the string representation of the expression.
func (expr *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", expr.X.String())
}

type SetLiteralExpr struct {
	Lbracket Pos    // position of left bracket
	Members  []Expr // bracketed expression
	Rbracket Pos    // position of right bracket

	ResultDataType ExprDataType
}

func (expr *SetLiteralExpr) IsLiteral() bool {
	return true
}

func (expr *SetLiteralExpr) DataType() ExprDataType {
	return expr.ResultDataType
}

func (expr *SetLiteralExpr) Pos() Pos {
	return expr.Lbracket
}

// Clone returns a deep copy of expr.
func (expr *SetLiteralExpr) Clone() *SetLiteralExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.Members = cloneExprs(expr.Members)
	return &other
}

// String returns the string representation of the expression.
func (expr *SetLiteralExpr) String() string {
	var buf bytes.Buffer

	if len(expr.Members) != 0 {
		buf.WriteString("[")
		for i, col := range expr.Members {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString("]")
	}

	return buf.String()
}

type TupleLiteralExpr struct {
	Lbrace  Pos    // position of left brace
	Members []Expr // bracketed expression
	Rbrace  Pos    // position of right brace

	ResultDataType ExprDataType
}

func (expr *TupleLiteralExpr) IsLiteral() bool {
	return true
}

func (expr *TupleLiteralExpr) DataType() ExprDataType {
	return expr.ResultDataType
}

func (expr *TupleLiteralExpr) Pos() Pos {
	return expr.Lbrace
}

// Clone returns a deep copy of expr.
func (expr *TupleLiteralExpr) Clone() *TupleLiteralExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.Members = cloneExprs(expr.Members)
	return &other
}

// String returns the string representation of the expression.
func (expr *TupleLiteralExpr) String() string {
	var buf bytes.Buffer

	if len(expr.Members) != 0 {
		buf.WriteString("{")
		for i, col := range expr.Members {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString("}")
	}

	return buf.String()
}

type Window struct {
	Name       *Ident            // name of window
	As         Pos               // position of AS keyword
	Definition *WindowDefinition // window definition
}

// Clone returns a deep copy of w.
func (w *Window) Clone() *Window {
	if w == nil {
		return nil
	}
	other := *w
	other.Name = w.Name.Clone()
	other.Definition = w.Definition.Clone()
	return &other
}

func cloneWindows(a []*Window) []*Window {
	if a == nil {
		return nil
	}
	other := make([]*Window, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the window.
func (w *Window) String() string {
	return fmt.Sprintf("%s AS %s", w.Name.String(), w.Definition.String())
}

type WindowDefinition struct {
	Lparen        Pos             // position of left paren
	Base          *Ident          // base window name
	Partition     Pos             // position of PARTITION keyword
	PartitionBy   Pos             // position of BY keyword (after PARTITION)
	Partitions    []Expr          // partition expressions
	Order         Pos             // position of ORDER keyword
	OrderBy       Pos             // position of BY keyword (after ORDER)
	OrderingTerms []*OrderingTerm // ordering terms
	Frame         *FrameSpec      // frame
	Rparen        Pos             // position of right paren
}

// Clone returns a deep copy of d.
func (d *WindowDefinition) Clone() *WindowDefinition {
	if d == nil {
		return nil
	}
	other := *d
	other.Base = d.Base.Clone()
	other.Partitions = cloneExprs(d.Partitions)
	other.OrderingTerms = cloneOrderingTerms(d.OrderingTerms)
	other.Frame = d.Frame.Clone()
	return &other
}

// String returns the string representation of the window definition.
func (d *WindowDefinition) String() string {
	var buf bytes.Buffer
	buf.WriteString("(")
	if d.Base != nil {
		buf.WriteString(d.Base.String())
	}

	if len(d.Partitions) != 0 {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString("PARTITION BY ")

		for i, p := range d.Partitions {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(p.String())
		}
	}

	if len(d.OrderingTerms) != 0 {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString("ORDER BY ")

		for i, term := range d.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	if d.Frame != nil {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString(d.Frame.String())
	}

	buf.WriteString(")")

	return buf.String()
}
