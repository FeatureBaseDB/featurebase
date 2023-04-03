// Copyright 2021 Molecula Corp. All rights reserved.
package parser

import (
	"fmt"
	"io"
	"strings"
)

// Parser represents a SQL parser.
type Parser struct {
	s *Scanner

	pos  Pos    // current position
	tok  Token  // current token
	lit  string // current literal value
	full bool   // buffer full
}

// NewParser returns a new instance of Parser that reads from r.
func NewParser(r io.Reader) *Parser {
	return &Parser{
		s: NewScanner(r),
	}
}

// ParseExprString parses s into an expression. Returns nil if s is blank.
func ParseExprString(s string) (Expr, error) {
	if s == "" {
		return nil, nil
	}
	return NewParser(strings.NewReader(s)).ParseExpr()
}

// MustParseExprString parses s into an expression. Panic on error.
func MustParseExprString(s string) Expr {
	expr, err := ParseExprString(s)
	if err != nil {
		panic(err)
	}
	return expr
}

func (p *Parser) ParseStatement() (stmt Statement, err error) {
	switch tok := p.peek(); tok {
	case EOF:
		return nil, io.EOF
	//case EXPLAIN:
	//	if stmt, err = p.parseExplainStatement(); err != nil {
	//		return stmt, err
	//	}
	default:
		if stmt, err = p.parseNonExplainStatement(); err != nil {
			return stmt, err
		}
	}

	// Read trailing semicolon or end of file.
	if tok := p.peek(); tok != EOF && tok != SEMI {
		return stmt, p.errorExpected(p.pos, p.tok, "semicolon or EOF")
	}
	p.scan()

	return stmt, nil
}

/*
// parseExplain parses EXPLAIN [QUERY PLAN] STMT.
func (p *Parser) parseExplainStatement() (_ *ExplainStatement, err error) {
	var tok Token

	// Parse initial "EXPLAIN" token.
	var stmt ExplainStatement
	stmt.Explain, tok, _ = p.scan()
	assert(tok == EXPLAIN)

	// Parse optional "QUERY PLAN" tokens.
	if p.peek() == QUERY {
		stmt.Query, _, _ = p.scan()

		if p.peek() != PLAN {
			return &stmt, p.errorExpected(p.pos, p.tok, "PLAN")
		}
		stmt.QueryPlan, _, _ = p.scan()
	}

	// Parse statement to be explained.
	if stmt.Stmt, err = p.parseNonExplainStatement(); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}*/

// parseStmt parses all statement types.
func (p *Parser) parseNonExplainStatement() (Statement, error) {
	switch p.peek() {
	//case ANALYZE:
	//	return p.parseAnalyzeStatement()
	case ALTER:
		return p.parseAlterStatement()
	case BULK:
		return p.parseBulkInsertStatement()
	case CREATE:
		return p.parseCreateStatement()
	case DROP:
		return p.parseDropStatement()
	case SELECT:
		return p.parseSelectStatement(false, nil)
	case INSERT, REPLACE:
		return p.parseInsertStatement(nil)
	case UPDATE:
		return p.parseUpdateStatement(nil)
	case DELETE:
		return p.parseDeleteStatement()
		//	case WITH:
		//		return p.parseWithStatement()
	case SHOW:
		return p.parseShowStatement()
	default:
		return nil, p.errorExpected(p.pos, p.tok, "statement")
	}
}

// parseWithStatement is called only from parseNonExplainStatement as we don't
// know what kind of statement we'll have after the CTEs (e.g. SELECT, INSERT, etc).
/*func (p *Parser) parseWithStatement() (Statement, error) {
	withClause, err := p.parseWithClause()
	if err != nil {
		return nil, err
	}

	switch p.peek() {
	case SELECT, VALUES:
		return p.parseSelectStatement(false, withClause)
	case INSERT, REPLACE:
		return p.parseInsertStatement(withClause)
	case UPDATE:
		return p.parseUpdateStatement(withClause)
	case DELETE:
		return p.parseDeleteStatement(withClause)
	default:
		return nil, p.errorExpected(p.pos, p.tok, "SELECT, VALUES, INSERT, REPLACE, UPDATE, or DELETE")
	}
}*/

func (p *Parser) parseShowStatement() (Statement, error) {
	assert(p.peek() == SHOW)
	show, _, _ := p.scan()

	switch p.peek() {
	case DATABASES:
		return p.parseShowDatabasesStatement(show)
	case TABLES:
		return p.parseShowTablesStatement(show)
	case COLUMNS:
		return p.parseShowColumnsStatement(show)
	case CREATE:
		return p.parseShowCreateStatement(show)
	default:
		return nil, p.errorExpected(p.pos, p.tok, "DATABASES, TABLES, COLUMNS or CREATE")
	}
}

func (p *Parser) parseShowDatabasesStatement(showPos Pos) (*ShowDatabasesStatement, error) {
	switch p.peek() {
	case DATABASES:
		var stmt ShowDatabasesStatement
		stmt.Show = showPos
		stmt.Databases, _, _ = p.scan()
		return &stmt, nil
	default:
		return nil, p.errorExpected(p.pos, p.tok, "DATABASES")
	}
}

func (p *Parser) parseShowTablesStatement(showPos Pos) (*ShowTablesStatement, error) {
	switch p.peek() {
	case TABLES:
		var stmt ShowTablesStatement
		stmt.Show = showPos
		stmt.Tables, _, _ = p.scan()
		return &stmt, nil
	default:
		return nil, p.errorExpected(p.pos, p.tok, "TABLES")
	}
}

func (p *Parser) parseShowColumnsStatement(showPos Pos) (_ *ShowColumnsStatement, err error) {
	assert(p.peek() == COLUMNS)
	columns, _, _ := p.scan()

	var stmt ShowColumnsStatement
	stmt.Show = showPos
	stmt.Columns = columns

	switch p.peek() {
	case FROM:
		stmt.From, _, _ = p.scan()
		if stmt.TableName, err = p.parseIdent("table name"); err != nil {
			return &stmt, err
		}
		return &stmt, nil
	default:
		return nil, p.errorExpected(p.pos, p.tok, "FROM")
	}
}

func (p *Parser) parseShowCreateStatement(showPos Pos) (Statement, error) {
	assert(p.peek() == CREATE)
	create, _, _ := p.scan()

	switch p.peek() {
	case TABLE:
		return p.parseShowCreateTableStatement(showPos, create)
	default:
		return nil, p.errorExpected(p.pos, p.tok, "TABLES")
	}
}

func (p *Parser) parseShowCreateTableStatement(showPos Pos, createPos Pos) (_ *ShowCreateTableStatement, err error) {
	assert(p.peek() == TABLE)
	table, _, _ := p.scan()

	var stmt ShowCreateTableStatement
	stmt.Show = showPos
	stmt.Create = createPos
	stmt.Table = table

	if stmt.TableName, err = p.parseIdent("table name"); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}

/*func (p *Parser) parseBeginStatement() (*BeginStatement, error) {
	assert(p.peek() == BEGIN)

	var stmt BeginStatement
	stmt.Begin, _, _ = p.scan()

	// Parse transaction type.
	switch p.peek() {
	case DEFERRED:
		stmt.Deferred, _, _ = p.scan()
	case IMMEDIATE:
		stmt.Immediate, _, _ = p.scan()
	case EXCLUSIVE:
		stmt.Exclusive, _, _ = p.scan()
	}

	// Parse optional TRANSCTION keyword.
	if p.peek() == TRANSACTION {
		stmt.Transaction, _, _ = p.scan()
	}
	return &stmt, nil
}*/

/*func (p *Parser) parseCommitStatement() (*CommitStatement, error) {
	assert(p.peek() == COMMIT || p.peek() == END)

	var stmt CommitStatement
	if p.peek() == COMMIT {
		stmt.Commit, _, _ = p.scan()
	} else {
		stmt.End, _, _ = p.scan()
	}

	if p.peek() == TRANSACTION {
		stmt.Transaction, _, _ = p.scan()
	}
	return &stmt, nil
}*/

/*func (p *Parser) parseRollbackStatement() (_ *RollbackStatement, err error) {
	assert(p.peek() == ROLLBACK)

	var stmt RollbackStatement
	stmt.Rollback, _, _ = p.scan()

	// Parse optional "TRANSACTION".
	if p.peek() == TRANSACTION {
		stmt.Transaction, _, _ = p.scan()
	}

	// Parse optional "TO SAVEPOINT savepoint-name"
	if p.peek() == TO {
		stmt.To, _, _ = p.scan()
		if p.peek() == SAVEPOINT {
			stmt.Savepoint, _, _ = p.scan()
		}
		if stmt.SavepointName, err = p.parseIdent("savepoint name"); err != nil {
			return &stmt, err
		}
	}
	return &stmt, nil
}*/

/*func (p *Parser) parseSavepointStatement() (_ *SavepointStatement, err error) {
	assert(p.peek() == SAVEPOINT)

	var stmt SavepointStatement
	stmt.Savepoint, _, _ = p.scan()
	if stmt.Name, err = p.parseIdent("savepoint name"); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}*/

/*func (p *Parser) parseReleaseStatement() (_ *ReleaseStatement, err error) {
	assert(p.peek() == RELEASE)

	var stmt ReleaseStatement
	stmt.Release, _, _ = p.scan()

	if p.peek() == SAVEPOINT {
		stmt.Savepoint, _, _ = p.scan()
	}

	if stmt.Name, err = p.parseIdent("savepoint name"); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}*/

func (p *Parser) parseCreateStatement() (Statement, error) {
	assert(p.peek() == CREATE)
	pos, tok, _ := p.scan()

	switch p.peek() {
	case DATABASE:
		return p.parseCreateDatabaseStatement(pos)
	case TABLE:
		return p.parseCreateTableStatement(pos)
	case VIEW:
		return p.parseCreateViewStatement(pos)
		/*case INDEX, UNIQUE:
		return p.parseCreateIndexStatement(pos)*/
	case FUNCTION:
		return p.parseCreateFunctionStatement(pos)
	default:
		return nil, p.errorExpected(pos, tok, "DATABASE, TABLE, VIEW or FUNCTION")
	}
}

func (p *Parser) parseAlterStatement() (Statement, error) {
	assert(p.peek() == ALTER)
	pos, tok, _ := p.scan()

	switch p.peek() {
	case DATABASE:
		return p.parseAlterDatabaseStatement(pos)
	case TABLE:
		return p.parseAlterTableStatement(pos)
	case VIEW:
		return p.parseAlterViewStatement(pos)
	default:
		return nil, p.errorExpected(pos, tok, "DATABASE, TABLE or VIEW")
	}
}

func (p *Parser) parseDropStatement() (Statement, error) {
	assert(p.peek() == DROP)
	pos, tok, _ := p.scan()

	switch p.peek() {
	case DATABASE:
		return p.parseDropDatabaseStatement(pos)
	case TABLE:
		return p.parseDropTableStatement(pos)
	case VIEW:
		return p.parseDropViewStatement(pos)
		/* case INDEX:
		return p.parseDropIndexStatement(pos)*/
	case FUNCTION:
		return p.parseDropFunctionStatement(pos)
	default:
		return nil, p.errorExpected(pos, tok, "DATABASE, TABLE, VIEW or FUNCTION")
	}
}

func (p *Parser) parseCreateDatabaseStatement(createPos Pos) (_ *CreateDatabaseStatement, err error) {
	assert(p.peek() == DATABASE)

	var stmt CreateDatabaseStatement
	stmt.Create = createPos
	stmt.Database, _, _ = p.scan()

	// Parse optional "IF NOT EXISTS".
	if p.peek() == IF {
		stmt.If, _, _ = p.scan()

		pos, tok, _ := p.scan()
		if tok != NOT {
			return &stmt, p.errorExpected(pos, tok, "NOT")
		}
		stmt.IfNot = pos

		pos, tok, _ = p.scan()
		if tok != EXISTS {
			return &stmt, p.errorExpected(pos, tok, "EXISTS")
		}
		stmt.IfNotExists = pos
	}

	if stmt.Name, err = p.parseIdent("database name"); err != nil {
		return &stmt, err
	}

	switch p.peek() {
	case WITH:
		stmt.With, _, _ = p.scan()

		// look for database options
		if stmt.Options, err = p.parseDatabaseOptions(); err != nil {
			return &stmt, err
		}
		if len(stmt.Options) == 0 {
			return &stmt, p.errorExpected(stmt.With, p.peek(), "at least one option after WITH")
		}
	}

	return &stmt, nil
}

func (p *Parser) parseDatabaseOptions() (_ []DatabaseOption, err error) {
	if !isDatabaseOptionStartToken(p.peek()) {
		return nil, nil
	}

	var a []DatabaseOption

	for {
		if !isDatabaseOptionStartToken(p.peek()) {
			return a, nil
		}
		cons, err := p.parseDatabaseOption()
		if cons != nil {
			a = append(a, cons)
		}
		if err != nil {
			return a, err
		}
	}
}

func (p *Parser) parseDatabaseOption() (_ DatabaseOption, err error) {
	assert(isDatabaseOptionStartToken(p.peek()))

	// Parse database options.
	switch p.peek() {
	case UNITS:
		return p.parseUnitsOption()
	default:
		assert(p.peek() == COMMENT)
		return p.parseCommentOption()
	}
}

func (p *Parser) parseUnitsOption() (_ *UnitsOption, err error) {
	assert(p.peek() == UNITS)

	var opt UnitsOption
	opt.Units, _, _ = p.scan()

	if isLiteralToken(p.peek()) {
		opt.Expr = p.mustParseLiteral()
	} else {
		return &opt, p.errorExpected(p.pos, p.tok, "literal")
	}

	return &opt, nil
}

func (p *Parser) parseCreateTableStatement(createPos Pos) (_ *CreateTableStatement, err error) {
	assert(p.peek() == TABLE)

	var stmt CreateTableStatement
	stmt.Create = createPos
	stmt.Table, _, _ = p.scan()

	// Parse optional "IF NOT EXISTS".
	if p.peek() == IF {
		stmt.If, _, _ = p.scan()

		pos, tok, _ := p.scan()
		if tok != NOT {
			return &stmt, p.errorExpected(pos, tok, "NOT")
		}
		stmt.IfNot = pos

		pos, tok, _ = p.scan()
		if tok != EXISTS {
			return &stmt, p.errorExpected(pos, tok, "EXISTS")
		}
		stmt.IfNotExists = pos
	}

	if stmt.Name, err = p.parseIdent("table name"); err != nil {
		return &stmt, err
	}

	// Parse either a column/constraint list or build table from "AS <select>".
	switch p.peek() {
	case LP:
		stmt.Lparen, _, _ = p.scan()

		if stmt.Columns, err = p.parseColumnDefinitions(); err != nil {
			return &stmt, err
		} /*else if stmt.Constraints, err = p.parseTableConstraints(); err != nil {
			return &stmt, err
		}*/

		if p.peek() != RP {
			return &stmt, p.errorExpected(p.pos, p.tok, "right paren")
		}
		stmt.Rparen, _, _ = p.scan()

		//look for table options
		if stmt.Options, err = p.parseTableOptions(); err != nil {
			return &stmt, err
		}
		return &stmt, nil
	/*case AS:
	stmt.As, _, _ = p.scan()
	if stmt.Select, err = p.parseSelectStatement(false, nil); err != nil {
		return &stmt, err
	}
	return &stmt, nil*/
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
	}
}

func (p *Parser) parseTableOptions() (_ []TableOption, err error) {
	if !isTableOptionStartToken(p.peek()) {
		return nil, nil
	}

	var a []TableOption

	for {
		if !isTableOptionStartToken(p.peek()) {
			return a, nil
		}
		cons, err := p.parseTableOption()
		if cons != nil {
			a = append(a, cons)
		}
		if err != nil {
			return a, err
		}
	}
}

func (p *Parser) parseTableOption() (_ TableOption, err error) {
	assert(isTableOptionStartToken(p.peek()))

	var optionPos Pos

	// Parse table options.
	switch p.peek() {
	case KEYPARTITIONS:
		return p.parseKeyPartitionsOption(optionPos)
	default:
		assert(p.peek() == COMMENT)
		return p.parseCommentOption()
	}
}

func (p *Parser) parseCommentOption() (_ *CommentOption, err error) {
	assert(p.peek() == COMMENT)

	var opt CommentOption

	opt.Comment, _, _ = p.scan()

	if isLiteralToken(p.peek()) {
		opt.Expr = p.mustParseLiteral()
	} else {
		return &opt, p.errorExpected(p.pos, p.tok, "literal")
	}

	return &opt, nil
}

func (p *Parser) parseKeyPartitionsOption(optionPos Pos) (_ *KeyPartitionsOption, err error) {
	assert(p.peek() == KEYPARTITIONS)

	var opt KeyPartitionsOption
	opt.KeyPartitions, _, _ = p.scan()

	if isLiteralToken(p.peek()) {
		opt.Expr = p.mustParseLiteral()
	} else {
		return &opt, p.errorExpected(p.pos, p.tok, "literal")
	}

	return &opt, nil
}

func (p *Parser) parseColumnDefinitions() (_ []*ColumnDefinition, err error) {
	var columns []*ColumnDefinition
	for {
		switch {
		case isIdentToken(p.peek()):
			col, err := p.parseColumnDefinition()
			columns = append(columns, col)
			if err != nil {
				return columns, err
			}
			if p.peek() == COMMA {
				p.scan()
			}
		case p.peek() == RP || isConstraintStartToken(p.peek(), true):
			return columns, nil
		default:
			return columns, p.errorExpected(p.pos, p.tok, "column name, or right paren")
		}
	}
}

func (p *Parser) parseColumnDefinition() (_ *ColumnDefinition, err error) {
	var col ColumnDefinition
	if col.Name, err = p.parseIdent("column name"); err != nil {
		return &col, err
	} else if col.Type, err = p.parseType(); err != nil {
		return &col, err
	}

	if col.Constraints, err = p.parseColumnConstraints(); err != nil {
		return &col, err
	}
	return &col, nil
}

/*func (p *Parser) parseTableConstraints() (_ []Constraint, err error) {
	if !isConstraintStartToken(p.peek(), true) {
		return nil, nil
	}

	var a []Constraint
	for {
		cons, err := p.parseConstraint(true)
		if cons != nil {
			a = append(a, cons)
		}
		if err != nil {
			return a, err
		}

		// Scan delimiting comma.
		if p.peek() != COMMA {
			return a, nil
		}
		p.scan()
	}
}*/

func (p *Parser) parseColumnConstraints() (_ []Constraint, err error) {
	var a []Constraint
	for isConstraintStartToken(p.peek(), false) {
		cons, err := p.parseConstraint(false)
		a = append(a, cons)
		if err != nil {
			return a, err
		}
	}
	return a, nil
}

func (p *Parser) parseConstraint(isTable bool) (_ Constraint, err error) {
	assert(isConstraintStartToken(p.peek(), isTable))

	var constraintPos Pos
	var name *Ident

	// Parse constraint name, if specified.
	/*if p.peek() == CONSTRAINT {
		constraintPos, _, _ = p.scan()

		if name, err = p.parseIdent("constraint name"); err != nil {
			return nil, err
		}
	}*/

	// Table constraints only use a subset of column constraints.
	/*if isTable {
		switch p.peek() {
		case PRIMARY:
			return p.parsePrimaryKeyConstraint(constraintPos, name, isTable)
		case UNIQUE:
			return p.parseUniqueConstraint(constraintPos, name, isTable)
		case CHECK:
			return p.parseCheckConstraint(constraintPos, name)
		default:
			assert(p.peek() == FOREIGN)
			return p.parseForeignKeyConstraint(constraintPos, name, isTable)
		}
	}*/

	// Parse column constraints.
	switch p.peek() {
	//case PRIMARY:
	//	return p.parsePrimaryKeyConstraint(constraintPos, name, isTable)
	//case NOT:
	//	return p.parseNotNullConstraint(constraintPos, name)
	case MIN:
		return p.parseMinConstraint(constraintPos, name)
	case MAX:
		return p.parseMaxConstraint(constraintPos, name)
	case TIMEUNIT:
		return p.parseTimeUnitConstraint(constraintPos, name)
	case TIMEQUANTUM:
		return p.parseTimeQuantumConstraint(constraintPos, name)
		//case UNIQUE:
		//	return p.parseUniqueConstraint(constraintPos, name, isTable)
		//case CHECK:
		//	return p.parseCheckConstraint(constraintPos, name)
		//case DEFAULT:
		//	return p.parseDefaultConstraint(constraintPos, name)
	default:
		assert(p.peek() == CACHETYPE)
		return p.parseCacheTypeConstraint(constraintPos, name)
	}
}

/*func (p *Parser) parsePrimaryKeyConstraint(constraintPos Pos, name *Ident, isTable bool) (_ *PrimaryKeyConstraint, err error) {
	assert(p.peek() == PRIMARY)

	var cons PrimaryKeyConstraint
	cons.Constraint = constraintPos
	cons.Name = name
	cons.Primary, _, _ = p.scan()

	if p.peek() != KEY {
		return &cons, p.errorExpected(p.pos, p.tok, "KEY")
	}
	cons.Key, _, _ = p.scan()

	// Table constraints specify columns; column constraints specify sort direction.
	if isTable {
		if p.peek() != LP {
			return &cons, p.errorExpected(p.pos, p.tok, "left paren")
		}
		cons.Lparen, _, _ = p.scan()

		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &cons, err
			}
			cons.Columns = append(cons.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &cons, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		cons.Rparen, _, _ = p.scan()

	}

	if !isTable {
		if p.peek() == AUTOINCREMENT {
			cons.Autoincrement, _, _ = p.scan()
		}
	}
	return &cons, nil
}*/

/*func (p *Parser) parseNotNullConstraint(constraintPos Pos, name *Ident) (_ *NotNullConstraint, err error) {
	assert(p.peek() == NOT)

	var cons NotNullConstraint
	cons.Constraint = constraintPos
	cons.Name = name
	cons.Not, _, _ = p.scan()

	if p.peek() != NULL {
		return &cons, p.errorExpected(p.pos, p.tok, "NULL")
	}
	cons.Null, _, _ = p.scan()

	return &cons, nil
}*/

func (p *Parser) parseMinConstraint(constraintPos Pos, name *Ident) (_ *MinConstraint, err error) {
	assert(p.peek() == MIN)

	var cons MinConstraint
	cons.Min, _, _ = p.scan()

	// This parses an expression, as opposed to just a literal, because a
	// negative value is a unary expression with Op = MINUS, so we need to allow
	// for that.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	cons.Expr = expr

	return &cons, nil
}

func (p *Parser) parseMaxConstraint(constraintPos Pos, name *Ident) (_ *MaxConstraint, err error) {
	assert(p.peek() == MAX)

	var cons MaxConstraint
	cons.Max, _, _ = p.scan()

	// This parses an expression, as opposed to just a literal, because a
	// negative value is a unary expression with Op = MINUS, so we need to allow
	// for that.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	cons.Expr = expr

	return &cons, nil
}

func (p *Parser) parseCacheTypeConstraint(constraintPos Pos, name *Ident) (_ *CacheTypeConstraint, err error) {
	assert(p.peek() == CACHETYPE)

	var cons CacheTypeConstraint
	cons.CacheType, _, _ = p.scan()

	switch p.peek() {
	case RANKED, LRU:
		_, _, cacheTypeValue := p.scan()
		// FeatureBase expects a lowercase cache type value.
		cons.CacheTypeValue = strings.ToLower(cacheTypeValue)
	default:
		return &cons, p.errorExpected(p.pos, p.tok, "RANKED or LRU")
	}

	if p.peek() == SIZE {
		cons.Size, _, _ = p.scan()
		if isLiteralToken(p.peek()) {
			cons.SizeExpr = p.mustParseLiteral()
		} else {
			return &cons, p.errorExpected(p.pos, p.tok, "literal")
		}
	}

	return &cons, nil
}

func (p *Parser) parseTimeUnitConstraint(constraintPos Pos, name *Ident) (_ *TimeUnitConstraint, err error) {
	assert(p.peek() == TIMEUNIT)

	var cons TimeUnitConstraint
	cons.TimeUnit, _, _ = p.scan()

	if isLiteralToken(p.peek()) {
		cons.Expr = p.mustParseLiteral()
	} else {
		return &cons, p.errorExpected(p.pos, p.tok, "literal")
	}
	return &cons, nil
}

func (p *Parser) parseTimeQuantumConstraint(constraintPos Pos, name *Ident) (_ *TimeQuantumConstraint, err error) {
	assert(p.peek() == TIMEQUANTUM)

	var cons TimeQuantumConstraint
	cons.TimeQuantum, _, _ = p.scan()

	if isLiteralToken(p.peek()) {
		cons.Expr = p.mustParseLiteral()
	} else {
		return &cons, p.errorExpected(p.pos, p.tok, "literal")
	}
	if p.peek() == TTL {
		cons.Ttl, _, _ = p.scan()

		if isLiteralToken(p.peek()) {
			cons.TtlExpr = p.mustParseLiteral()
		} else {
			return &cons, p.errorExpected(p.pos, p.tok, "literal")
		}
	}
	return &cons, nil
}

/*func (p *Parser) parseUniqueConstraint(constraintPos Pos, name *Ident, isTable bool) (_ *UniqueConstraint, err error) {
	assert(p.peek() == UNIQUE)

	var cons UniqueConstraint
	cons.Constraint = constraintPos
	cons.Name = name
	cons.Unique, _, _ = p.scan()

	if isTable {
		if p.peek() != LP {
			return &cons, p.errorExpected(p.pos, p.tok, "left paren")
		}
		cons.Lparen, _, _ = p.scan()

		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &cons, err
			}
			cons.Columns = append(cons.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &cons, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		cons.Rparen, _, _ = p.scan()
	}

	return &cons, nil
}*/

/*func (p *Parser) parseCheckConstraint(constraintPos Pos, name *Ident) (_ *CheckConstraint, err error) {
	assert(p.peek() == CHECK)

	var cons CheckConstraint
	cons.Constraint = constraintPos
	cons.Name = name
	cons.Check, _, _ = p.scan()

	if p.peek() != LP {
		return &cons, p.errorExpected(p.pos, p.tok, "left paren")
	}
	cons.Lparen, _, _ = p.scan()

	if cons.Expr, err = p.ParseExpr(); err != nil {
		return &cons, err
	}

	if p.peek() != RP {
		return &cons, p.errorExpected(p.pos, p.tok, "right paren")
	}
	cons.Rparen, _, _ = p.scan()

	return &cons, nil
}*/

/*func (p *Parser) parseDefaultConstraint(constraintPos Pos, name *Ident) (_ *DefaultConstraint, err error) {
	assert(p.peek() == DEFAULT)

	var cons DefaultConstraint
	cons.Constraint = constraintPos
	cons.Name = name
	cons.Default, _, _ = p.scan()
	if isLiteralToken(p.peek()) {
		cons.Expr = p.mustParseLiteral()
	} else if p.peek() == PLUS || p.peek() == MINUS {
		if cons.Expr, err = p.parseSignedNumber("signed number"); err != nil {
			return &cons, err
		}
	} else {
		if p.peek() != LP {
			return &cons, p.errorExpected(p.pos, p.tok, "literal value or left paren")
		}
		cons.Lparen, _, _ = p.scan()

		if cons.Expr, err = p.ParseExpr(); err != nil {
			return &cons, err
		}

		if p.peek() != RP {
			return &cons, p.errorExpected(p.pos, p.tok, "right paren")
		}
		cons.Rparen, _, _ = p.scan()
	}
	return &cons, nil
}*/

/*func (p *Parser) parseForeignKeyConstraint(constraintPos Pos, name *Ident, isTable bool) (_ *ForeignKeyConstraint, err error) {
	var cons ForeignKeyConstraint
	cons.Constraint = constraintPos
	cons.Name = name

	// Table constraints start with "FOREIGN KEY (col1, col2, etc)".
	if isTable {
		assert(p.peek() == FOREIGN)
		cons.Foreign, _, _ = p.scan()

		if p.peek() != KEY {
			return &cons, p.errorExpected(p.pos, p.tok, "KEY")
		}
		cons.ForeignKey, _, _ = p.scan()

		if p.peek() != LP {
			return &cons, p.errorExpected(p.pos, p.tok, "left paren")
		}
		cons.Lparen, _, _ = p.scan()

		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &cons, err
			}
			cons.Columns = append(cons.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &cons, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		cons.Rparen, _, _ = p.scan()
	}

	if p.peek() != REFERENCES {
		return &cons, p.errorExpected(p.pos, p.tok, "REFERENCES")
	}
	cons.References, _, _ = p.scan()

	if cons.ForeignTable, err = p.parseIdent("foreign table name"); err != nil {
		return &cons, err
	}

	// Parse column list.
	if p.peek() != LP {
		return &cons, p.errorExpected(p.pos, p.tok, "left paren")
	}
	cons.ForeignLparen, _, _ = p.scan()

	for {
		col, err := p.parseIdent("foreign column name")
		if err != nil {
			return &cons, err
		}
		cons.ForeignColumns = append(cons.ForeignColumns, col)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &cons, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.scan()
	}

	cons.ForeignRparen, _, _ = p.scan()

	// Parse foreign key args.
	for p.peek() == ON {
		var arg ForeignKeyArg
		arg.On, _, _ = p.scan()

		// Parse foreign key type.
		if p.peek() == UPDATE {
			arg.OnUpdate, _, _ = p.scan()
		} else if p.peek() == DELETE {
			arg.OnDelete, _, _ = p.scan()
		} else {
			return &cons, p.errorExpected(p.pos, p.tok, "UPDATE or DELETE")
		}

		// Parse foreign key action.
		if p.peek() == SET {
			arg.Set, _, _ = p.scan()
			if p.peek() == NULL {
				arg.SetNull, _, _ = p.scan()
			} else if p.peek() == DEFAULT {
				arg.SetDefault, _, _ = p.scan()
			} else {
				return &cons, p.errorExpected(p.pos, p.tok, "NULL or DEFAULT")
			}
		} else if p.peek() == CASCADE {
			arg.Cascade, _, _ = p.scan()
		} else if p.peek() == RESTRICT {
			arg.Restrict, _, _ = p.scan()
		} else if p.peek() == NO {
			arg.No, _, _ = p.scan()
			if p.peek() == ACTION {
				arg.NoAction, _, _ = p.scan()
			} else {
				return &cons, p.errorExpected(p.pos, p.tok, "ACTION")
			}
		} else {
			return &cons, p.errorExpected(p.pos, p.tok, "SET NULL, SET DEFAULT, CASCADE, RESTRICT, or NO ACTION")
		}

		cons.Args = append(cons.Args, &arg)
	}

	// Parse deferrable subclause.
	if p.peek() == NOT || p.peek() == DEFERRABLE {
		if p.peek() == NOT {
			cons.Not, _, _ = p.scan()
			if p.peek() != DEFERRABLE {
				return &cons, p.errorExpected(p.pos, p.tok, "DEFERRABLE")
			}
			cons.NotDeferrable, _, _ = p.scan()
		} else {
			cons.Deferrable, _, _ = p.scan()
		}

		if p.peek() == INITIALLY {
			cons.Initially, _, _ = p.scan()
			if p.peek() == DEFERRED {
				cons.InitiallyDeferred, _, _ = p.scan()
			} else if p.peek() == IMMEDIATE {
				cons.InitiallyImmediate, _, _ = p.scan()
			}
		}
	}

	return &cons, nil
}*/

func (p *Parser) parseDropDatabaseStatement(dropPos Pos) (_ *DropDatabaseStatement, err error) {
	assert(p.peek() == DATABASE)

	var stmt DropDatabaseStatement
	stmt.Drop = dropPos
	stmt.Database, _, _ = p.scan()

	// Parse optional "IF EXISTS".
	if p.peek() == IF {
		stmt.If, _, _ = p.scan()
		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfExists, _, _ = p.scan()
	}

	if stmt.Name, err = p.parseIdent("database name"); err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

func (p *Parser) parseDropTableStatement(dropPos Pos) (_ *DropTableStatement, err error) {
	assert(p.peek() == TABLE)

	var stmt DropTableStatement
	stmt.Drop = dropPos
	stmt.Table, _, _ = p.scan()

	// Parse optional "IF EXISTS".
	if p.peek() == IF {
		stmt.If, _, _ = p.scan()
		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfExists, _, _ = p.scan()
	}

	if stmt.Name, err = p.parseIdent("table name"); err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

func (p *Parser) parseCreateViewStatement(createPos Pos) (_ *CreateViewStatement, err error) {
	assert(p.peek() == VIEW)

	var stmt CreateViewStatement
	stmt.Create = createPos
	stmt.View, _, _ = p.scan()

	// Parse optional "IF NOT EXISTS".
	if p.peek() == IF {
		stmt.If, _, _ = p.scan()

		if p.peek() != NOT {
			return &stmt, p.errorExpected(p.pos, p.tok, "NOT")
		}
		stmt.IfNot, _, _ = p.scan()

		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfNotExists, _, _ = p.scan()
	}

	if stmt.Name, err = p.parseIdent("view name"); err != nil {
		return &stmt, err
	}

	// TODO(pok) - we'll do this later - right now views are implemented as
	// jit compiled from text, which makes implementing these columns a pain
	// when we can pre-compile a plan op subgraph and stored it, we can put this
	// back in
	// Parse optional column list.
	// if p.peek() == LP {
	// 	stmt.Lparen, _, _ = p.scan()
	// 	for {
	// 		col, err := p.parseIdent("column name")
	// 		if err != nil {
	// 			return &stmt, err
	// 		}
	// 		stmt.Columns = append(stmt.Columns, col)

	// 		if p.peek() == RP {
	// 			break
	// 		} else if p.peek() != COMMA {
	// 			return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
	// 		}
	// 		p.scan()
	// 	}
	// 	stmt.Rparen, _, _ = p.scan()
	// }

	// Parse "AS select-stmt"
	if p.peek() != AS {
		return &stmt, p.errorExpected(p.pos, p.tok, "AS")
	}
	stmt.As, _, _ = p.scan()
	if stmt.Select, err = p.parseSelectStatement(false, nil); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}

func (p *Parser) parseAlterViewStatement(alterPos Pos) (_ *AlterViewStatement, err error) {
	var stmt AlterViewStatement
	stmt.Alter = alterPos
	if p.peek() != VIEW {
		return &stmt, p.errorExpected(p.pos, p.tok, "VIEW")
	}
	stmt.View, _, _ = p.scan()

	if stmt.Name, err = p.parseIdent("view name"); err != nil {
		return &stmt, err
	}

	// TODO(pok) - we'll do this later - see note in parseCompileView()
	// Parse optional column list.
	// if p.peek() == LP {
	// 	stmt.Lparen, _, _ = p.scan()
	// 	for {
	// 		col, err := p.parseIdent("column name")
	// 		if err != nil {
	// 			return &stmt, err
	// 		}
	// 		stmt.Columns = append(stmt.Columns, col)

	// 		if p.peek() == RP {
	// 			break
	// 		} else if p.peek() != COMMA {
	// 			return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
	// 		}
	// 		p.scan()
	// 	}
	// 	stmt.Rparen, _, _ = p.scan()
	// }

	// Parse "AS select-stmt"
	if p.peek() != AS {
		return &stmt, p.errorExpected(p.pos, p.tok, "AS")
	}
	stmt.As, _, _ = p.scan()
	if stmt.Select, err = p.parseSelectStatement(false, nil); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}

func (p *Parser) parseDropViewStatement(dropPos Pos) (_ *DropViewStatement, err error) {
	assert(p.peek() == VIEW)

	var stmt DropViewStatement
	stmt.Drop = dropPos
	stmt.View, _, _ = p.scan()

	// Parse optional "IF EXISTS".
	if p.peek() == IF {
		stmt.If, _, _ = p.scan()
		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfExists, _, _ = p.scan()
	}

	if stmt.Name, err = p.parseIdent("view name"); err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

/*func (p *Parser) parseCreateIndexStatement(createPos Pos) (_ *CreateIndexStatement, err error) {
	assert(p.peek() == INDEX || p.peek() == UNIQUE)

	var stmt CreateIndexStatement
	stmt.Create = createPos
	if p.peek() == UNIQUE {
		stmt.Unique, _, _ = p.scan()
	}
	if p.peek() != INDEX {
		return &stmt, p.errorExpected(p.pos, p.tok, "INDEX")
	}
	stmt.Index, _, _ = p.scan()

	// Parse optional "IF NOT EXISTS".
	if p.peek() == IF {
		stmt.If, _, _ = p.scan()

		if p.peek() != NOT {
			return &stmt, p.errorExpected(p.pos, p.tok, "NOT")
		}
		stmt.IfNot, _, _ = p.scan()

		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfNotExists, _, _ = p.scan()
	}

	if stmt.Name, err = p.parseIdent("index name"); err != nil {
		return &stmt, err
	}

	if p.peek() != ON {
		return &stmt, p.errorExpected(p.pos, p.tok, "ON")
	}
	stmt.On, _, _ = p.scan()

	if stmt.Table, err = p.parseIdent("table name"); err != nil {
		return &stmt, err
	}

	// Parse optional column list.
	if p.peek() != LP {
		return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
	}
	stmt.Lparen, _, _ = p.scan()
	for {
		col, err := p.parseIndexedColumn()
		if err != nil {
			return &stmt, err
		}
		stmt.Columns = append(stmt.Columns, col)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.scan()
	}
	stmt.Rparen, _, _ = p.scan()

	// Parse optional "WHERE expr"
	if p.peek() == WHERE {
		stmt.Where, _, _ = p.scan()
		if stmt.WhereExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}
	}
	return &stmt, nil
}*/

/*func (p *Parser) parseDropIndexStatement(dropPos Pos) (_ *DropIndexStatement, err error) {
	assert(p.peek() == INDEX)

	var stmt DropIndexStatement
	stmt.Drop = dropPos
	stmt.Index, _, _ = p.scan()

	// Parse optional "IF EXISTS".
	if p.peek() == IF {
		stmt.If, _, _ = p.scan()
		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfExists, _, _ = p.scan()
	}

	if stmt.Name, err = p.parseIdent("index name"); err != nil {
		return &stmt, err
	}

	return &stmt, nil
}*/

func (p *Parser) parseParameterDefinitions() (_ []*ParameterDefinition, err error) {
	var params []*ParameterDefinition
	for {
		switch {
		case p.peek() == VARIABLE:
			col, err := p.parseParameterDefinition()
			params = append(params, col)
			if err != nil {
				return params, err
			}
			if p.peek() == COMMA {
				p.scan()
			}
		case p.peek() == RP:
			return params, nil
		default:
			return params, p.errorExpected(p.pos, p.tok, "parameter name, or right paren")
		}
	}
}

func (p *Parser) parseParameterDefinition() (_ *ParameterDefinition, err error) {
	var param ParameterDefinition
	if param.Name, err = p.parseVariable("parameter name"); err != nil {
		return &param, err
	} else if param.Type, err = p.parseType(); err != nil {
		return &param, err
	}
	return &param, nil
}

func (p *Parser) parseCreateFunctionStatement(createPos Pos) (_ *CreateFunctionStatement, err error) {
	assert(p.peek() == FUNCTION)

	var stmt CreateFunctionStatement
	stmt.Create = createPos
	stmt.Function, _, _ = p.scan()

	// Parse optional "IF NOT EXISTS".
	if p.peek() == IF {
		stmt.If, _, _ = p.scan()

		if p.peek() != NOT {
			return &stmt, p.errorExpected(p.pos, p.tok, "NOT")
		}
		stmt.IfNot, _, _ = p.scan()

		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfNotExists, _, _ = p.scan()
	}

	if stmt.Name, err = p.parseIdent("function name"); err != nil {
		return &stmt, err
	}

	// parameters
	if p.peek() == LP {
		stmt.Lparen, _, _ = p.scan()

		stmt.Parameters, err = p.parseParameterDefinitions()
		if err != nil {
			return &stmt, err
		}

		if p.peek() != RP {
			return &stmt, p.errorExpected(p.pos, p.tok, ")")
		}
		stmt.Rparen, _, _ = p.scan()
	}

	if p.peek() != RETURNS {
		return &stmt, p.errorExpected(p.pos, p.tok, "RETURNS")
	}
	stmt.Returns, _, _ = p.scan()
	stmt.ReturnDef, err = p.parseParameterDefinition()
	if err != nil {
		return &stmt, err
	}

	if p.peek() != AS {
		return &stmt, p.errorExpected(p.pos, p.tok, "AS")
	}
	stmt.As, _, _ = p.scan()

	if p.peek() != BEGIN {
		return &stmt, p.errorExpected(p.pos, p.tok, "BEGIN")
	}
	stmt.Begin, _, _ = p.scan()

	for {
		s, err := p.parseFunctionBodyStatement()
		if err != nil {
			return &stmt, err
		}
		if s != nil {
			stmt.Body = append(stmt.Body, s)
		}

		if p.peek() == END {
			break
		}
	}

	if p.peek() != END {
		return &stmt, p.errorExpected(p.pos, p.tok, "END")
	}
	stmt.End, _, _ = p.scan()

	return &stmt, nil
}

func (p *Parser) parseFunctionBodyStatement() (stmt Statement, err error) {
	switch p.peek() {
	case END:
		break
	default:
		return nil, p.errorExpected(p.pos, p.tok, "statement")
	}
	if err != nil {
		return stmt, err
	}
	return stmt, nil
}

func (p *Parser) parseDropFunctionStatement(dropPos Pos) (_ *DropFunctionStatement, err error) {
	assert(p.peek() == FUNCTION)

	var stmt DropFunctionStatement
	stmt.Drop = dropPos
	stmt.Function, _, _ = p.scan()

	// Parse optional "IF EXISTS".
	if p.peek() == IF {
		stmt.If, _, _ = p.scan()
		if p.peek() != EXISTS {
			return &stmt, p.errorExpected(p.pos, p.tok, "EXISTS")
		}
		stmt.IfExists, _, _ = p.scan()
	}

	if stmt.Name, err = p.parseIdent("function name"); err != nil {
		return &stmt, err
	}

	return &stmt, nil
}

func (p *Parser) parseIdent(desc string) (*Ident, error) {
	pos, tok, lit := p.scan()
	switch tok {
	case IDENT, QIDENT:
		return &Ident{Name: lit, NamePos: pos, Quoted: tok == QIDENT}, nil
	default:
		return nil, p.errorExpected(pos, tok, desc)
	}
}

func (p *Parser) parseVariable(desc string) (*Variable, error) {
	pos, tok, lit := p.scan()
	switch tok {
	case VARIABLE:
		return &Variable{
			Name:    lit,
			NamePos: pos,
		}, nil
	default:
		return nil, p.errorExpected(pos, tok, desc)
	}
}

func (p *Parser) parseType() (_ *Type, err error) {
	var typ Type
	if typ.Name, err = p.parseIdent("type name"); err != nil {
		return &typ, err
	}

	// Optionally parse scale.
	if p.peek() == LP {
		typ.Lparen, _, _ = p.scan()
		if typ.Scale, err = p.parseIntegerLiteral("scale"); err != nil {
			return &typ, err
		}

		if p.peek() != RP {
			return nil, p.errorExpected(p.pos, p.tok, "right paren")
		}
		typ.Rparen, _, _ = p.scan()
	}

	return &typ, nil
}

func (p *Parser) parseBulkInsertStatement() (_ *BulkInsertStatement, err error) {
	if p.peek() != BULK {
		return nil, p.errorExpected(p.pos, p.tok, "BULK")
	}

	var stmt BulkInsertStatement

	stmt.Bulk, _, _ = p.scan()

	if pk := p.peek(); pk != INSERT && pk != REPLACE {
		return nil, p.errorExpected(p.pos, p.tok, "INSERT or REPLACE")
	}
	if p.peek() == INSERT {
		stmt.Insert, _, _ = p.scan()
	} else {
		stmt.Replace, _, _ = p.scan()
	}

	if p.peek() != INTO {
		return nil, p.errorExpected(p.pos, p.tok, "INTO")
	}
	stmt.Into, _, _ = p.scan()

	// Parse table name
	if stmt.Table, err = p.parseIdent("table name"); err != nil {
		return nil, err
	}

	if p.peek() == LP {
		stmt.ColumnsLparen, _, _ = p.scan()
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &stmt, err
			}
			stmt.Columns = append(stmt.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		stmt.ColumnsRparen, _, _ = p.scan()
	}

	if p.peek() != MAP {
		return &stmt, p.errorExpected(p.pos, p.tok, "MAP")
	}
	stmt.Map, _, _ = p.scan()
	if p.peek() != LP {
		return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
	}
	stmt.MapLparen, _, _ = p.scan()

	mapIdx := 0
	for {
		expr, err := p.ParseExpr()
		if err != nil {
			return &stmt, err
		}

		mapType, err := p.parseType()
		if err != nil {
			return &stmt, err
		}

		stmt.MapList = append(stmt.MapList, &BulkInsertMapDefinition{
			Name: &Ident{
				Name: fmt.Sprintf("%d", mapIdx),
			},
			MapExpr: expr,
			Type:    mapType,
		})
		mapIdx++

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.scan()
	}
	stmt.MapRparen, _, _ = p.scan()

	if p.peek() == TRANSFORM {
		stmt.Transform, _, _ = p.scan()
		if p.peek() != LP {
			return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
		}
		stmt.TransformLparen, _, _ = p.scan()

		for {
			expr, err := p.ParseExpr()
			if err != nil {
				return &stmt, err
			}
			stmt.TransformList = append(stmt.TransformList, expr)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		stmt.TransformRparen, _, _ = p.scan()
	}

	if p.peek() != FROM {
		return nil, p.errorExpected(p.pos, p.tok, "FROM")
	}
	stmt.From, _, _ = p.scan()

	if isLiteralToken(p.peek()) {
		stmt.DataSource = p.mustParseLiteral()
	} else {
		return nil, p.errorExpected(p.pos, p.tok, "literal")
	}

	if p.peek() != WITH {
		return nil, p.errorExpected(p.pos, p.tok, "WITH")
	}
	stmt.With, _, _ = p.scan()
	if !isBulkInsertOptionStartToken(p.peek(), p) {
		return nil, p.errorExpected(p.pos, p.tok, "BATCHSIZE, ROWSLIMIT, FORMAT, INPUT, ALLOW_MISSING_VALUES or HEADER_ROW")
	}
	for {
		err := p.parseBulkInsertOption(&stmt)
		if err != nil {
			return nil, err
		}
		if !isBulkInsertOptionStartToken(p.peek(), p) {
			break
		}
	}

	return &stmt, nil
}

func (p *Parser) parseBulkInsertOption(stmt *BulkInsertStatement) error {
	switch p.peek() {
	case IDENT:
		ident, err := p.parseIdent("bulk insert option")
		if err != nil {
			return err
		}
		switch strings.ToUpper(ident.Name) {
		case "BATCHSIZE":
			if isLiteralToken(p.peek()) {
				stmt.BatchSize = p.mustParseLiteral()
				return nil
			} else {
				return p.errorExpected(p.pos, p.tok, "literal")
			}

		case "ROWSLIMIT":
			if isLiteralToken(p.peek()) {
				stmt.RowsLimit = p.mustParseLiteral()
				return nil
			} else {
				return p.errorExpected(p.pos, p.tok, "literal")
			}

		case "FORMAT":
			if isLiteralToken(p.peek()) {
				stmt.Format = p.mustParseLiteral()
				return nil
			} else {
				return p.errorExpected(p.pos, p.tok, "literal")
			}

		case "INPUT":
			if isLiteralToken(p.peek()) {
				stmt.Input = p.mustParseLiteral()
				return nil
			} else {
				return p.errorExpected(p.pos, p.tok, "literal")
			}
		case "ALLOW_MISSING_VALUES":
			stmt.AllowMissingValues = ident
			return nil

		case "HEADER_ROW":
			stmt.HeaderRow = ident
			return nil
		}
	}
	return p.errorExpected(p.pos, p.tok, "BATCHSIZE, ROWSLIMIT, FORMAT, INPUT or HEADER_ROW")
}

func (p *Parser) parseInsertStatement(withClause *WithClause) (_ *InsertStatement, err error) {
	if pk := p.peek(); pk != INSERT && pk != REPLACE {
		return nil, p.errorExpected(p.pos, p.tok, "INSERT or REPLACE")
	}

	var stmt InsertStatement
	//stmt.WithClause = withClause

	if p.peek() == INSERT {
		stmt.Insert, _, _ = p.scan()

		/*if p.peek() == OR {
			stmt.InsertOr, _, _ = p.scan()

			switch p.peek() {
			//case ROLLBACK:
			//	stmt.InsertOrRollback, _, _ = p.scan()
			case REPLACE:
				stmt.InsertOrReplace, _, _ = p.scan()
			//case ABORT:
			//	stmt.InsertOrAbort, _, _ = p.scan()
			//case FAIL:
			//	stmt.InsertOrFail, _, _ = p.scan()
			//case IGNORE:
			//	stmt.InsertOrIgnore, _, _ = p.scan()
			default:
				return &stmt, p.errorExpected(p.pos, p.tok, "REPLACE")
			}
		} */
	} else {
		stmt.Replace, _, _ = p.scan()
	}

	if p.peek() != INTO {
		return &stmt, p.errorExpected(p.pos, p.tok, "INTO")
	}
	stmt.Into, _, _ = p.scan()

	// Parse table name & optional alias.
	if stmt.Table, err = p.parseIdent("table name"); err != nil {
		return &stmt, err
	}
	if p.peek() == AS {
		stmt.As, _, _ = p.scan()
		if stmt.Alias, err = p.parseIdent("alias"); err != nil {
			return &stmt, err
		}
	}

	// Parse optional column list.
	if p.peek() == LP {
		stmt.ColumnsLparen, _, _ = p.scan()
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &stmt, err
			}
			stmt.Columns = append(stmt.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		stmt.ColumnsRparen, _, _ = p.scan()
	}

	switch p.peek() {
	case VALUES:
		stmt.Values, _, _ = p.scan()

		// Parse out the value tuples.
		stmt.TupleList = make([]*ExprList, 0)

		for {
			var tuple ExprList
			if p.peek() != LP {
				return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
			}
			tuple.Lparen, _, _ = p.scan()

			for {
				expr, err := p.ParseExpr()
				if err != nil {
					return &stmt, err
				}
				tuple.Exprs = append(tuple.Exprs, expr)

				if p.peek() == RP {
					break
				} else if p.peek() != COMMA {
					return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
				}
				p.scan()
			}
			tuple.Rparen, _, _ = p.scan()

			stmt.TupleList = append(stmt.TupleList, &tuple)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}

	//case SELECT:
	//	if stmt.Select, err = p.parseSelectStatement(false, nil); err != nil {
	//		return &stmt, err
	//	}
	//case DEFAULT:
	//	stmt.Default, _, _ = p.scan()
	//	if p.peek() != VALUES {
	//		return &stmt, p.errorExpected(p.pos, p.tok, "VALUES")
	//	}
	//	stmt.DefaultValues, _, _ = p.scan()
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "VALUES")
	}

	// Parse optional upsert clause.
	//if p.peek() == ON {
	//	if stmt.UpsertClause, err = p.parseUpsertClause(); err != nil {
	//		return &stmt, err
	//	}
	//}

	return &stmt, nil
}

/*func (p *Parser) parseUpsertClause() (_ *UpsertClause, err error) {
	assert(p.peek() == ON)

	var clause UpsertClause

	// Parse "ON CONFLICT"
	clause.On, _, _ = p.scan()
	if p.peek() != CONFLICT {
		return &clause, p.errorExpected(p.pos, p.tok, "CONFLICT")
	}
	clause.OnConflict, _, _ = p.scan()

	// Parse optional indexed column list & WHERE conditional.
	if p.peek() == LP {
		clause.Lparen, _, _ = p.scan()
		for {
			col, err := p.parseIndexedColumn()
			if err != nil {
				return &clause, err
			}
			clause.Columns = append(clause.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &clause, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		clause.Rparen, _, _ = p.scan()

		if p.peek() == WHERE {
			clause.Where, _, _ = p.scan()
			if clause.WhereExpr, err = p.ParseExpr(); err != nil {
				return &clause, err
			}
		}
	}

	// Parse "DO NOTHING" or "DO UPDATE SET".
	if p.peek() != DO {
		return &clause, p.errorExpected(p.pos, p.tok, "DO")
	}
	clause.Do, _, _ = p.scan()

	// If next token is NOTHING, then read it and exit immediately.
	if p.peek() == NOTHING {
		clause.DoNothing, _, _ = p.scan()
		return &clause, nil
	} else if p.peek() != UPDATE {
		return &clause, p.errorExpected(p.pos, p.tok, "NOTHING or UPDATE SET")
	}

	// Otherwise parse "UPDATE SET"
	clause.DoUpdate, _, _ = p.scan()
	if p.peek() != SET {
		return &clause, p.errorExpected(p.pos, p.tok, "SET")
	}
	clause.DoUpdateSet, _, _ = p.scan()

	// Parse list of assignments.
	for {
		assignment, err := p.parseAssignment()
		if err != nil {
			return &clause, err
		}
		clause.Assignments = append(clause.Assignments, assignment)

		if p.peek() != COMMA {
			break
		}
		p.scan()
	}

	// Parse WHERE after DO UPDATE SET.
	if p.peek() == WHERE {
		clause.UpdateWhere, _, _ = p.scan()
		if clause.UpdateWhereExpr, err = p.ParseExpr(); err != nil {
			return &clause, err
		}
	}

	return &clause, nil
}*/

/*func (p *Parser) parseIndexedColumn() (_ *IndexedColumn, err error) {
	var col IndexedColumn
	if col.X, err = p.ParseExpr(); err != nil {
		return &col, err
	}
	if p.peek() == ASC {
		col.Asc, _, _ = p.scan()
	} else if p.peek() == DESC {
		col.Desc, _, _ = p.scan()
	}
	return &col, nil
}*/

func (p *Parser) parseUpdateStatement(withClause *WithClause) (_ *UpdateStatement, err error) {
	assert(p.peek() == UPDATE)

	var stmt UpdateStatement
	stmt.WithClause = withClause

	stmt.Update, _, _ = p.scan()
	if p.peek() == OR {
		stmt.UpdateOr, _, _ = p.scan()

		switch p.peek() {
		case ROLLBACK:
			stmt.UpdateOrRollback, _, _ = p.scan()
		case REPLACE:
			stmt.UpdateOrReplace, _, _ = p.scan()
		case ABORT:
			stmt.UpdateOrAbort, _, _ = p.scan()
		case FAIL:
			stmt.UpdateOrFail, _, _ = p.scan()
		case IGNORE:
			stmt.UpdateOrIgnore, _, _ = p.scan()
		default:
			return &stmt, p.errorExpected(p.pos, p.tok, "ROLLBACK, REPLACE, ABORT, FAIL, or IGNORE")
		}
	}

	ident, err := p.parseIdent("table name")
	if err != nil {
		return &stmt, err
	}
	stmt.Table, err = p.parseQualifiedTableName(ident)
	if err != nil {
		return &stmt, err
	}

	// Parse SET + list of assignments.
	if p.peek() != SET {
		return &stmt, p.errorExpected(p.pos, p.tok, "SET")
	}
	stmt.Set, _, _ = p.scan()

	for {
		assignment, err := p.parseAssignment()
		if err != nil {
			return &stmt, err
		}
		stmt.Assignments = append(stmt.Assignments, assignment)

		if p.peek() != COMMA {
			break
		}
		p.scan()
	}

	// Parse WHERE clause.
	if p.peek() == WHERE {
		stmt.Where, _, _ = p.scan()
		if stmt.WhereExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}
	}

	return &stmt, nil
}

func (p *Parser) parseDeleteStatement( /*withClause *WithClause*/ ) (_ *DeleteStatement, err error) {
	assert(p.peek() == DELETE)

	var stmt DeleteStatement
	//stmt.WithClause = withClause

	// Parse "DELETE FROM tbl"
	stmt.Delete, _, _ = p.scan()
	if p.peek() != FROM {
		return &stmt, p.errorExpected(p.pos, p.tok, "FROM")
	}
	stmt.From, _, _ = p.scan()

	ident, err := p.parseIdent("table name")
	if err != nil {
		return &stmt, err
	}
	tableName, err := p.parseQualifiedTableName(ident)
	if err != nil {
		return &stmt, err
	}
	stmt.Source = tableName
	// keep the table name too
	stmt.TableName = tableName.Clone()

	// parse WHERE clause.
	if p.peek() == WHERE {
		stmt.Where, _, _ = p.scan()
		if stmt.WhereExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}
	}

	return &stmt, nil
}

func (p *Parser) parseAssignment() (_ *Assignment, err error) {
	var assignment Assignment

	// Parse either a single column (IDENT) or a column list (LP IDENT COMMA IDENT RP)
	if isIdentToken(p.peek()) {
		col, _ := p.parseIdent("column name")
		assignment.Columns = []*Ident{col}
	} else if p.peek() == LP {
		assignment.Lparen, _, _ = p.scan()
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &assignment, err
			}
			assignment.Columns = append(assignment.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &assignment, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		assignment.Rparen, _, _ = p.scan()
	} else {
		return &assignment, p.errorExpected(p.pos, p.tok, "column name or column list")
	}

	if p.peek() != EQ {
		return &assignment, p.errorExpected(p.pos, p.tok, "=")
	}
	assignment.Eq, _, _ = p.scan()

	if assignment.Expr, err = p.ParseExpr(); err != nil {
		return &assignment, err
	}

	return &assignment, nil
}

// parseSelectStatement parses a SELECT statement.
// If compounded is true, some parts of the SELECT syntax are skipped.
func (p *Parser) parseSelectStatement(compounded bool, withClause *WithClause) (_ *SelectStatement, err error) {
	var stmt SelectStatement
	//stmt.WithClause = withClause

	// Parse optional "WITH [RECURSIVE} cte, cte..."
	// This is only called here if this method is called directly. Generic
	// statement parsing will parse the WITH clause and pass it in instead.
	//if !compounded && stmt.WithClause == nil && p.peek() == WITH {
	//	if stmt.WithClause, err = p.parseWithClause(); err != nil {
	//		return &stmt, err
	//	}
	//}

	switch p.peek() {
	/*case VALUES:
	stmt.Values, _, _ = p.scan()

	for {
		var list ExprList
		if p.peek() != LP {
			return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
		}
		list.Lparen, _, _ = p.scan()

		for {
			expr, err := p.ParseExpr()
			if err != nil {
				return &stmt, err
			}
			list.Exprs = append(list.Exprs, expr)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		list.Rparen, _, _ = p.scan()
		stmt.ValueLists = append(stmt.ValueLists, &list)

		if p.peek() != COMMA {
			break
		}
		p.scan()

	}*/

	case SELECT:
		stmt.Select, _, _ = p.scan()

		// Parse optional "DISTINCT".
		if tok := p.peek(); tok == DISTINCT {
			stmt.Distinct, _, _ = p.scan()
		}

		if p.peek() == TOP {
			stmt.Top, _, _ = p.scan()
			if p.peek() == LP {
				_, _, _ = p.scan()
			}
			if stmt.TopExpr, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
			if p.peek() == RP {
				_, _, _ = p.scan()
			}
		}

		if p.peek() == TOPN {
			stmt.TopN, _, _ = p.scan()
			if p.peek() == LP {
				_, _, _ = p.scan()
			}
			if stmt.TopExpr, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
			if p.peek() == RP {
				_, _, _ = p.scan()
			}
		}

		// Parse result columns.
		for {
			col, err := p.parseResultColumn()
			if err != nil {
				return &stmt, err
			}
			stmt.Columns = append(stmt.Columns, col)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}

		// Parse FROM clause.
		if p.peek() == FROM {
			stmt.From, _, _ = p.scan()
			if stmt.Source, err = p.parseSource(); err != nil {
				return &stmt, err
			}
		}

		// Parse WHERE clause.
		if p.peek() == WHERE {
			stmt.Where, _, _ = p.scan()
			if stmt.WhereExpr, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}

		// Parse GROUP BY/HAVING clause.
		if p.peek() == GROUP {
			stmt.Group, _, _ = p.scan()
			if p.peek() != BY {
				return &stmt, p.errorExpected(p.pos, p.tok, "BY")
			}
			stmt.GroupBy, _, _ = p.scan()

			for {
				expr, err := p.ParseExpr()
				if err != nil {
					return &stmt, err
				}
				stmt.GroupByExprs = append(stmt.GroupByExprs, expr)

				if p.peek() != COMMA {
					break
				}
				p.scan()
			}

			// Parse optional HAVING clause.
			if p.peek() == HAVING {
				stmt.Having, _, _ = p.scan()
				if stmt.HavingExpr, err = p.ParseExpr(); err != nil {
					return &stmt, err
				}
			}
		}

		// Parse WINDOW clause.
		if p.peek() == WINDOW {
			stmt.Window, _, _ = p.scan()

			for {
				var window Window
				if window.Name, err = p.parseIdent("window name"); err != nil {
					return &stmt, err
				}

				if p.peek() != AS {
					return &stmt, p.errorExpected(p.pos, p.tok, "AS")
				}
				window.As, _, _ = p.scan()

				if window.Definition, err = p.parseWindowDefinition(); err != nil {
					return &stmt, err
				}

				stmt.Windows = append(stmt.Windows, &window)

				if p.peek() != COMMA {
					break
				}
				p.scan()
			}
		}
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "SELECT")
	}

	// Optionally compound additional SELECT/VALUES.
	switch tok := p.peek(); tok {
	case UNION, INTERSECT, EXCEPT:
		if tok == UNION {
			stmt.Union, _, _ = p.scan()
			if p.peek() == ALL {
				stmt.UnionAll, _, _ = p.scan()
			}
		} else if tok == INTERSECT {
			stmt.Intersect, _, _ = p.scan()
		} else {
			stmt.Except, _, _ = p.scan()
		}

		if stmt.Compound, err = p.parseSelectStatement(true, nil); err != nil {
			return &stmt, err
		}
	}

	// Parse ORDER BY clause.
	if !compounded && p.peek() == ORDER {
		stmt.Order, _, _ = p.scan()
		if p.peek() != BY {
			return &stmt, p.errorExpected(p.pos, p.tok, "BY")
		}
		stmt.OrderBy, _, _ = p.scan()

		for {
			term, err := p.parseOrderingTerm()
			if err != nil {
				return &stmt, err
			}
			stmt.OrderingTerms = append(stmt.OrderingTerms, term)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	}

	return &stmt, nil
}

func (p *Parser) parseResultColumn() (_ *ResultColumn, err error) {
	var col ResultColumn

	// An initial "*" returns all columns.
	if p.peek() == STAR {
		col.Star, _, _ = p.scan()
		return &col, nil
	}

	// Next can be either "EXPR [[AS] column-alias]" or "IDENT DOT STAR".
	// We need read the next element as an expression and then determine what next.
	if col.Expr, err = p.ParseExpr(); err != nil {
		return &col, err
	}

	// If we have a qualified ref w/ a star, don't allow an alias.
	if ref, ok := col.Expr.(*QualifiedRef); ok && ref.Star.IsValid() {
		return &col, nil
	}

	// If "AS" is next, the alias must follow.
	// Otherwise it can optionally be an IDENT alias.
	if p.peek() == AS {
		col.As, _, _ = p.scan()
		if !isIdentToken(p.peek()) {
			return &col, p.errorExpected(p.pos, p.tok, "column alias")
		}
		col.Alias, _ = p.parseIdent("column alias")
	} else if isIdentToken(p.peek()) {
		col.Alias, _ = p.parseIdent("column alias")
	}

	return &col, nil
}

func (p *Parser) parseSource() (source Source, err error) {
	source, err = p.parseUnarySource()
	if err != nil {
		return source, err
	}

	for {
		// Exit immediately if not part of a join operator.
		switch p.peek() {
		case COMMA, LEFT, RIGHT, FULL, INNER /*CROSS, */, JOIN:
		default:
			return source, nil
		}

		// Parse join operator.
		operator, err := p.parseJoinOperator()
		if err != nil {
			return source, err
		}
		y, err := p.parseUnarySource()
		if err != nil {
			return source, err
		}
		constraint, err := p.parseJoinConstraint()
		if err != nil {
			return source, err
		}

		source = &JoinClause{X: source, Operator: operator, Y: y, Constraint: constraint}
	}
}

// parseUnarySource parses a quailfied table name or subquery but not a JOIN.
func (p *Parser) parseUnarySource() (source Source, err error) {
	switch p.peek() {
	case LP:
		return p.parseParenSource()
	case IDENT, QIDENT:
		ident, err := p.parseIdent("table or function")
		if err != nil {
			return nil, err
		}
		if p.peek() == LP {
			return p.parseTableValuedFunction(ident)
		}
		return p.parseQualifiedTableName(ident)
	default:
		return nil, p.errorExpected(p.pos, p.tok, "table name or left paren")
	}
}

func (p *Parser) parseJoinOperator() (*JoinOperator, error) {
	var op JoinOperator

	// Handle single comma join.
	if p.peek() == COMMA {
		op.Comma, _, _ = p.scan()
		return &op, nil
	}

	// Parse  "INNER", "LEFT [OUTER]", "RIGHT [OUTER]", "FULL [OUTER]", or "CROSS"
	switch p.peek() {
	case LEFT:
		op.Left, _, _ = p.scan()
		if p.peek() == OUTER {
			op.Outer, _, _ = p.scan()
		}
	case RIGHT:
		op.Right, _, _ = p.scan()
		if p.peek() == OUTER {
			op.Outer, _, _ = p.scan()
		}
	case FULL:
		op.Full, _, _ = p.scan()
		if p.peek() == OUTER {
			op.Outer, _, _ = p.scan()
		}
	case INNER:
		op.Inner, _, _ = p.scan()

		// case CROSS:
		// 	op.Cross, _, _ = p.scan()
	}

	// Parse final JOIN.
	if p.peek() != JOIN {
		return &op, p.errorExpected(p.pos, p.tok, "JOIN")
	}
	op.Join, _, _ = p.scan()
	return &op, nil
}

func (p *Parser) parseJoinConstraint() (JoinConstraint, error) {
	switch p.peek() {
	case ON:
		return p.parseOnConstraint()
	case USING:
		return p.parseUsingConstraint()
	default:
		return nil, nil
	}
}

func (p *Parser) parseOnConstraint() (_ *OnConstraint, err error) {
	assert(p.peek() == ON)

	var con OnConstraint
	con.On, _, _ = p.scan()
	if con.X, err = p.ParseExpr(); err != nil {
		return &con, err
	}
	return &con, nil
}

func (p *Parser) parseUsingConstraint() (*UsingConstraint, error) {
	assert(p.peek() == USING)

	var con UsingConstraint
	con.Using, _, _ = p.scan()

	if p.peek() != LP {
		return &con, p.errorExpected(p.pos, p.tok, "left paren")
	}
	con.Lparen, _, _ = p.scan()

	for {
		col, err := p.parseIdent("column name")
		if err != nil {
			return &con, err
		}
		con.Columns = append(con.Columns, col)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &con, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.scan()
	}
	con.Rparen, _, _ = p.scan()

	return &con, nil
}

func (p *Parser) parseParenSource() (_ *ParenSource, err error) {
	assert(p.peek() == LP)

	var source ParenSource
	source.Lparen, _, _ = p.scan()

	if p.peek() == SELECT {
		if source.X, err = p.parseSelectStatement(false, nil); err != nil {
			return &source, err
		}
	} else {
		if source.X, err = p.parseSource(); err != nil {
			return &source, err
		}
	}

	if p.peek() != RP {
		return nil, p.errorExpected(p.pos, p.tok, "right paren")
	}
	source.Rparen, _, _ = p.scan()

	// Only parse aliases for nested select statements.
	if _, ok := source.X.(*SelectStatement); ok && (p.peek() == AS || isIdentToken(p.peek())) {
		if p.peek() == AS {
			source.As, _, _ = p.scan()
		}
		if source.Alias, err = p.parseIdent("table alias"); err != nil {
			return &source, err
		}
	}

	return &source, nil
}

func (p *Parser) parseQualifiedTableName(ident *Ident) (_ *QualifiedTableName, err error) {
	var tbl QualifiedTableName

	tbl.Name = ident

	// Parse optional table alias ("AS alias" or just "alias").
	if tok := p.peek(); tok == AS || isIdentToken(tok) {
		if p.peek() == AS {
			tbl.As, _, _ = p.scan()
		}
		if tbl.Alias, err = p.parseIdent("table alias"); err != nil {
			return &tbl, err
		}
	}

	// Parse optional "INDEXED BY index-name" or "NOT INDEXED".
	/*switch p.peek() {
	case INDEXED:
		tbl.Indexed, _, _ = p.scan()
		if p.peek() != BY {
			return &tbl, p.errorExpected(p.pos, p.tok, "BY")
		}
		tbl.IndexedBy, _, _ = p.scan()

		if tbl.Index, err = p.parseIdent("index name"); err != nil {
			return &tbl, err
		}
	case NOT:
		tbl.Not, _, _ = p.scan()
		if p.peek() != INDEXED {
			return &tbl, p.errorExpected(p.pos, p.tok, "INDEXED")
		}
		tbl.NotIndexed, _, _ = p.scan()
	}*/

	return &tbl, nil
}

func (p *Parser) parseTableValuedFunction(ident *Ident) (_ *TableValuedFunction, err error) {
	var tbl TableValuedFunction

	tbl.Name = ident

	tbl.Call, err = p.parseCall(ident)
	if err != nil {
		return &tbl, err
	}

	// Parse optional table alias ("AS alias" or just "alias").
	if tok := p.peek(); tok == AS || isIdentToken(tok) {
		if p.peek() == AS {
			tbl.As, _, _ = p.scan()
		}
		if tbl.Alias, err = p.parseIdent("table alias"); err != nil {
			return &tbl, err
		}
	}
	return &tbl, nil
}

/*func (p *Parser) parseWithClause() (*WithClause, error) {
	assert(p.peek() == WITH)

	var clause WithClause
	clause.With, _, _ = p.scan()
	if p.peek() == RECURSIVE {
		clause.Recursive, _, _ = p.scan()
	}

	// Parse comma-delimited list of common table expressions (CTE).
	for {
		cte, err := p.parseCTE()
		if err != nil {
			return &clause, err
		}
		clause.CTEs = append(clause.CTEs, cte)

		if p.peek() != COMMA {
			break
		}
		p.scan()
	}
	return &clause, nil
}*/

/*func (p *Parser) parseCTE() (_ *CTE, err error) {
	var cte CTE
	if cte.TableName, err = p.parseIdent("table name"); err != nil {
		return &cte, err
	}

	// Parse optional column list.
	if p.peek() == LP {
		cte.ColumnsLparen, _, _ = p.scan()

		for {
			column, err := p.parseIdent("column name")
			if err != nil {
				return &cte, err
			}
			cte.Columns = append(cte.Columns, column)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return nil, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		cte.ColumnsRparen, _, _ = p.scan()
	}

	if p.peek() != AS {
		return nil, p.errorExpected(p.pos, p.tok, "AS")
	}
	cte.As, _, _ = p.scan()

	// Parse select statement.
	if p.peek() != LP {
		return nil, p.errorExpected(p.pos, p.tok, "left paren")
	}
	cte.SelectLparen, _, _ = p.scan()

	if cte.Select, err = p.parseSelectStatement(false, nil); err != nil {
		return &cte, err
	}

	if p.peek() != RP {
		return nil, p.errorExpected(p.pos, p.tok, "right paren")
	}
	cte.SelectRparen, _, _ = p.scan()

	return &cte, nil
}*/

func (p *Parser) mustParseLiteral() Expr {
	assert(isLiteralToken(p.tok))
	pos, tok, lit := p.scan()
	switch tok {
	case STRING:
		return &StringLit{ValuePos: pos, Value: lit}
	case INTEGER:
		return &IntegerLit{ValuePos: pos, Value: lit}
	case FLOAT:
		return &FloatLit{ValuePos: pos, Value: lit}
	case TRUE, FALSE:
		return &BoolLit{ValuePos: pos, Value: tok == TRUE}
	case BLOB:
		return &StringLit{ValuePos: pos, IsBlob: true, Value: lit}
	default:
		assert(tok == NULL)
		return &NullLit{ValuePos: pos}
	}
}

func (p *Parser) ParseExpr() (expr Expr, err error) {
	return p.parseBinaryExpr(LowestPrec + 1)
}

func (p *Parser) parseOperand() (expr Expr, err error) {
	pos, tok, lit := p.scan()
	switch tok {
	case IDENT, QIDENT:
		ident := &Ident{Name: lit, NamePos: pos, Quoted: tok == QIDENT}
		if p.peek() == DOT {
			return p.parseQualifiedRef(ident)
		} else if p.peek() == LP {
			return p.parseCall(ident)
		}
		return ident, nil
	case VARIABLE:
		return &Variable{Name: lit, NamePos: pos}, nil
	case MIN, MAX:
		pk := p.peek()
		if pk == LP {
			ident := &Ident{Name: lit, NamePos: pos, Quoted: false}
			return p.parseCall(ident)
		}
		return nil, p.errorExpected(p.pos, pk, "call expression")
	case STRING:
		return &StringLit{ValuePos: pos, Value: lit}, nil
	case FLOAT:
		return &FloatLit{ValuePos: pos, Value: lit}, nil
	case INTEGER:
		return &IntegerLit{ValuePos: pos, Value: lit}, nil
	case NULL:
		return &NullLit{ValuePos: pos}, nil
	case CURRENT_DATE:
		return &SysVariable{NamePos: pos, Token: tok}, nil
	case CURRENT_TIMESTAMP:
		return &SysVariable{NamePos: pos, Token: tok}, nil
	case TRUE, FALSE:
		return &BoolLit{ValuePos: pos, Value: tok == TRUE}, nil
	case PLUS, MINUS, BITNOT:
		expr, err = p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{OpPos: pos, Op: tok, X: expr}, nil
	case LP:
		p.unscan()
		return p.parseParenExpr()
	case LB:
		p.unscan()
		return p.parseSetLiteralExpr()
	case LBR:
		p.unscan()
		return p.parseTupleLiteralExpr()
	case CAST:
		p.unscan()
		return p.parseCastExpr()
	case CASE:
		p.unscan()
		return p.parseCaseExpr()
	case NOT, EXISTS:
		p.unscan()
		return p.parseExists()
	case SELECT:
		p.unscan()
		return p.parseSelectStatement(false, nil)

	default:
		return nil, p.errorExpected(p.pos, p.tok, "expression")
	}
}

func (p *Parser) parseBinaryExpr(prec1 int) (expr Expr, err error) {
	x, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	for {
		if p.peek().Precedence() < prec1 {
			return x, nil
		}

		pos, op, err := p.scanBinaryOp()
		if err != nil {
			return nil, err
		}

		switch op {
		case IN, NOTIN:
			y, err := p.parseExprList()
			if err != nil {
				return x, err
			}
			x = &BinaryExpr{X: x, OpPos: pos, Op: op, Y: y}

		case BETWEEN, NOTBETWEEN:
			// Parsing the expression should yield a binary expression with AND op.
			// However, we don't want to conflate the boolean AND and the ranged AND
			// so we convert the expression to a Range.
			if rng, err := p.parseBinaryExpr(LowestPrec + 1); err != nil {
				return x, err
			} else if rng, ok := rng.(*BinaryExpr); !ok || rng.Op != AND {
				return x, p.errorExpected(p.pos, p.tok, "range expression")
			} else {
				x = &BinaryExpr{
					X:     x,
					OpPos: pos,
					Op:    op,
					Y:     &Range{X: rng.X, And: rng.OpPos, Y: rng.Y},
				}
			}

		default:
			y, err := p.parseBinaryExpr(op.Precedence() + 1)
			if err != nil {
				return nil, err
			}
			x = &BinaryExpr{X: x, OpPos: pos, Op: op, Y: y}
		}
	}

}

func (p *Parser) parseExprList() (_ *ExprList, err error) {
	var list ExprList
	if p.peek() != LP {
		return &list, p.errorExpected(p.pos, p.tok, "left paren")
	}
	list.Lparen, _, _ = p.scan()

	for p.peek() != RP {
		x, err := p.ParseExpr()
		if err != nil {
			return &list, err
		}
		list.Exprs = append(list.Exprs, x)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &list, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.scan()
	}

	list.Rparen, _, _ = p.scan()

	return &list, nil
}

func (p *Parser) parseQualifiedRef(table *Ident) (_ *QualifiedRef, err error) {
	assert(p.peek() == DOT)

	var expr QualifiedRef
	expr.Table = table
	expr.Dot, _, _ = p.scan()

	if p.peek() == STAR {
		expr.Star, _, _ = p.scan()
	} else if isIdentToken(p.peek()) {
		pos, tok, lit := p.scan()
		expr.Column = &Ident{Name: lit, NamePos: pos, Quoted: tok == QIDENT}
	} else {
		return &expr, p.errorExpected(p.pos, p.tok, "column name")
	}

	return &expr, nil
}

func (p *Parser) parseCall(name *Ident) (_ *Call, err error) {
	assert(p.peek() == LP)

	var expr Call
	expr.Name = name
	expr.Lparen, _, _ = p.scan()

	// Parse argument list: either "*" or "[DISTINCT] expr, expr..."
	if p.peek() == STAR {
		expr.Star, _, _ = p.scan()
	} else {
		if p.peek() == DISTINCT {
			expr.Distinct, _, _ = p.scan()
		}
		for p.peek() != RP {
			arg, err := p.ParseExpr()
			if err != nil {
				return &expr, err
			}
			expr.Args = append(expr.Args, arg)

			if tok := p.peek(); tok == COMMA {
				p.scan()
			} else if tok != RP {
				return &expr, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}

		}
	}

	if p.peek() != RP {
		return &expr, p.errorExpected(p.pos, p.tok, "right paren")
	}
	expr.Rparen, _, _ = p.scan()

	// Parse optional filter clause.
	if p.peek() == FILTER {
		if expr.Filter, err = p.parseFilterClause(); err != nil {
			return &expr, err
		}
	}

	// Parse optional over clause.
	if p.peek() == OVER {
		if expr.Over, err = p.parseOverClause(); err != nil {
			return &expr, err
		}
	}

	return &expr, nil
}

func (p *Parser) parseFilterClause() (_ *FilterClause, err error) {
	assert(p.peek() == FILTER)

	var clause FilterClause
	clause.Filter, _, _ = p.scan()

	if p.peek() != LP {
		return &clause, p.errorExpected(p.pos, p.tok, "left paren")
	}
	clause.Lparen, _, _ = p.scan()

	if p.peek() != WHERE {
		return &clause, p.errorExpected(p.pos, p.tok, "WHERE")
	}
	clause.Where, _, _ = p.scan()

	if clause.X, err = p.ParseExpr(); err != nil {
		return &clause, err
	}

	if p.peek() != RP {
		return &clause, p.errorExpected(p.pos, p.tok, "right paren")
	}
	clause.Rparen, _, _ = p.scan()

	return &clause, nil
}

func (p *Parser) parseOverClause() (_ *OverClause, err error) {
	assert(p.peek() == OVER)

	var clause OverClause
	clause.Over, _, _ = p.scan()

	// If specifying a window name, read it and exit.
	if isIdentToken(p.peek()) {
		pos, tok, lit := p.scan()
		clause.Name = &Ident{Name: lit, NamePos: pos, Quoted: tok == QIDENT}
		return &clause, nil
	}

	if clause.Definition, err = p.parseWindowDefinition(); err != nil {
		return &clause, err
	}
	return &clause, nil
}

func (p *Parser) parseWindowDefinition() (_ *WindowDefinition, err error) {
	var def WindowDefinition

	// Otherwise parse the window definition.
	if p.peek() != LP {
		return &def, p.errorExpected(p.pos, p.tok, "left paren")
	}
	def.Lparen, _, _ = p.scan()

	// Read base window name.
	if isIdentToken(p.peek()) {
		pos, tok, lit := p.scan()
		def.Base = &Ident{Name: lit, NamePos: pos, Quoted: tok == QIDENT}
	}

	// Parse "PARTITION BY expr, expr..."
	if p.peek() == PARTITION {
		def.Partition, _, _ = p.scan()
		if p.peek() != BY {
			return &def, p.errorExpected(p.pos, p.tok, "BY")
		}
		def.PartitionBy, _, _ = p.scan()

		for {
			partition, err := p.ParseExpr()
			if err != nil {
				return &def, err
			}
			def.Partitions = append(def.Partitions, partition)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	}

	// Parse "ORDER BY ordering-term, ordering-term..."
	if p.peek() == ORDER {
		def.Order, _, _ = p.scan()
		if p.peek() != BY {
			return &def, p.errorExpected(p.pos, p.tok, "BY")
		}
		def.OrderBy, _, _ = p.scan()

		for {
			term, err := p.parseOrderingTerm()
			if err != nil {
				return &def, err
			}
			def.OrderingTerms = append(def.OrderingTerms, term)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	}

	// Parse frame spec.
	if tok := p.peek(); tok == RANGE || tok == ROWS || tok == GROUPS {
		if def.Frame, err = p.parseFrameSpec(); err != nil {
			return &def, err
		}
	}

	// Parse final rparen.
	if p.peek() != RP {
		return &def, p.errorExpected(p.pos, p.tok, "right paren")
	}
	def.Rparen, _, _ = p.scan()

	return &def, nil
}

func (p *Parser) parseOrderingTerm() (_ *OrderingTerm, err error) {
	var term OrderingTerm
	if term.X, err = p.ParseExpr(); err != nil {
		return &term, err
	}

	// Parse optional sort direction ("ASC" or "DESC")
	switch p.peek() {
	case ASC:
		term.Asc, _, _ = p.scan()
	case DESC:
		term.Desc, _, _ = p.scan()
	}

	// Parse optional "NULLS FIRST" or "NULLS LAST"
	if p.peek() == NULLS {
		term.Nulls, _, _ = p.scan()
		switch p.peek() {
		case FIRST:
			term.NullsFirst, _, _ = p.scan()
		case LAST:
			term.NullsLast, _, _ = p.scan()
		default:
			return &term, p.errorExpected(p.pos, p.tok, "FIRST or LAST")
		}
	}

	return &term, nil
}

func (p *Parser) parseFrameSpec() (_ *FrameSpec, err error) {
	assert(p.peek() == RANGE || p.peek() == ROWS || p.peek() == GROUPS)

	var spec FrameSpec

	switch p.peek() {
	case RANGE:
		spec.Range, _, _ = p.scan()
	case ROWS:
		spec.Rows, _, _ = p.scan()
	case GROUPS:
		spec.Groups, _, _ = p.scan()
	}

	// Parsing BETWEEN indicates that two expressions are required.
	if p.peek() == BETWEEN {
		spec.Between, _, _ = p.scan()
	}

	// Parse X expression: "UNBOUNDED PRECEDING", "CURRENT ROW", "expr PRECEDING|FOLLOWING"
	if p.peek() == UNBOUNDED {
		spec.UnboundedX, _, _ = p.scan()
		if p.peek() != PRECEDING {
			return &spec, p.errorExpected(p.pos, p.tok, "PRECEDING")
		}
		spec.PrecedingX, _, _ = p.scan()
	} else if p.peek() == CURRENT {
		spec.CurrentX, _, _ = p.scan()
		if p.peek() != ROW {
			return &spec, p.errorExpected(p.pos, p.tok, "ROW")
		}
		spec.CurrentRowX, _, _ = p.scan()
	} else {
		if spec.X, err = p.ParseExpr(); err != nil {
			return &spec, err
		}
		if p.peek() == PRECEDING {
			spec.PrecedingX, _, _ = p.scan()
		} else if p.peek() == FOLLOWING && spec.Between.IsValid() { // FOLLOWING only allowed with BETWEEN
			spec.FollowingX, _, _ = p.scan()
		} else {
			if spec.Between.IsValid() {
				return &spec, p.errorExpected(p.pos, p.tok, "PRECEDING or FOLLOWING")
			}
			return &spec, p.errorExpected(p.pos, p.tok, "PRECEDING")
		}
	}

	// Read "AND y" if range is BETWEEN.
	if spec.Between.IsValid() {
		if p.peek() != AND {
			return &spec, p.errorExpected(p.pos, p.tok, "AND")
		}
		spec.And, _, _ = p.scan()

		// Parse Y expression: "UNBOUNDED FOLLOWING", "CURRENT ROW", "expr PRECEDING|FOLLOWING"
		if p.peek() == UNBOUNDED {
			spec.UnboundedY, _, _ = p.scan()
			if p.peek() != FOLLOWING {
				return &spec, p.errorExpected(p.pos, p.tok, "FOLLOWING")
			}
			spec.FollowingY, _, _ = p.scan()
		} else if p.peek() == CURRENT {
			spec.CurrentY, _, _ = p.scan()
			if p.peek() != ROW {
				return &spec, p.errorExpected(p.pos, p.tok, "ROW")
			}
			spec.CurrentRowY, _, _ = p.scan()
		} else {
			if spec.Y, err = p.ParseExpr(); err != nil {
				return &spec, err
			}
			if p.peek() == PRECEDING {
				spec.PrecedingY, _, _ = p.scan()
			} else if p.peek() == FOLLOWING {
				spec.FollowingY, _, _ = p.scan()
			} else {
				return &spec, p.errorExpected(p.pos, p.tok, "PRECEDING or FOLLOWING")
			}
		}
	}

	// Parse optional EXCLUDE.
	if p.peek() == EXCLUDE {
		spec.Exclude, _, _ = p.scan()

		switch p.peek() {
		case NO:
			spec.ExcludeNo, _, _ = p.scan()
			if p.peek() != OTHERS {
				return &spec, p.errorExpected(p.pos, p.tok, "OTHERS")
			}
			spec.ExcludeNoOthers, _, _ = p.scan()
		case CURRENT:
			spec.ExcludeCurrent, _, _ = p.scan()
			if p.peek() != ROW {
				return &spec, p.errorExpected(p.pos, p.tok, "ROW")
			}
			spec.ExcludeCurrentRow, _, _ = p.scan()
		case GROUP:
			spec.ExcludeGroup, _, _ = p.scan()
		case TIES:
			spec.ExcludeTies, _, _ = p.scan()
		default:
			return &spec, p.errorExpected(p.pos, p.tok, "NO OTHERS, CURRENT ROW, GROUP, or TIES")
		}
	}

	return &spec, nil
}

func (p *Parser) parseParenExpr() (_ *ParenExpr, err error) {
	var expr ParenExpr
	expr.Lparen, _, _ = p.scan()
	if expr.X, err = p.ParseExpr(); err != nil {
		return &expr, err
	}
	expr.Rparen, _, _ = p.scan()
	return &expr, nil
}

func (p *Parser) parseSetLiteralExpr() (_ *SetLiteralExpr, err error) {
	var expr SetLiteralExpr
	expr.Lbracket, _, _ = p.scan()

	for p.peek() != RB {
		x, err := p.ParseExpr()
		if err != nil {
			return &expr, err
		}
		expr.Members = append(expr.Members, x)

		if p.peek() == RB {
			break
		} else if p.peek() != COMMA {
			return &expr, p.errorExpected(p.pos, p.tok, "comma or right bracket")
		}
		p.scan()
	}

	expr.Rbracket, _, _ = p.scan()
	return &expr, nil
}

func (p *Parser) parseTupleLiteralExpr() (_ *TupleLiteralExpr, err error) {
	var expr TupleLiteralExpr
	expr.Lbrace, _, _ = p.scan()

	for p.peek() != RBR {
		x, err := p.ParseExpr()
		if err != nil {
			return &expr, err
		}
		expr.Members = append(expr.Members, x)

		if p.peek() == RBR {
			break
		} else if p.peek() != COMMA {
			return &expr, p.errorExpected(p.pos, p.tok, "comma or right brace")
		}
		p.scan()
	}

	expr.Rbrace, _, _ = p.scan()
	return &expr, nil
}

func (p *Parser) parseCastExpr() (_ *CastExpr, err error) {
	assert(p.peek() == CAST)

	var expr CastExpr
	expr.Cast, _, _ = p.scan()

	if p.peek() != LP {
		return &expr, p.errorExpected(p.pos, p.tok, "left paren")
	}
	expr.Lparen, _, _ = p.scan()

	if expr.X, err = p.ParseExpr(); err != nil {
		return &expr, err
	}

	if p.peek() != AS {
		return &expr, p.errorExpected(p.pos, p.tok, "AS")
	}
	expr.As, _, _ = p.scan()

	if expr.Type, err = p.parseType(); err != nil {
		return &expr, err
	}

	if p.peek() != RP {
		return &expr, p.errorExpected(p.pos, p.tok, "right paren")
	}
	expr.Rparen, _, _ = p.scan()
	return &expr, nil
}

func (p *Parser) parseCaseExpr() (_ *CaseExpr, err error) {
	assert(p.peek() == CASE)

	var expr CaseExpr
	expr.Case, _, _ = p.scan()

	// Parse optional expression if WHEN is not next.
	if p.peek() != WHEN {
		if expr.Operand, err = p.ParseExpr(); err != nil {
			return &expr, err
		}
	}

	// Parse one or more WHEN/THEN pairs.
	for {
		var blk CaseBlock
		if p.peek() != WHEN {
			return &expr, p.errorExpected(p.pos, p.tok, "WHEN")
		}
		blk.When, _, _ = p.scan()

		if blk.Condition, err = p.ParseExpr(); err != nil {
			return &expr, err
		}

		if p.peek() != THEN {
			return &expr, p.errorExpected(p.pos, p.tok, "THEN")
		}
		blk.Then, _, _ = p.scan()

		if blk.Body, err = p.ParseExpr(); err != nil {
			return &expr, err
		}

		expr.Blocks = append(expr.Blocks, &blk)

		if tok := p.peek(); tok == ELSE || tok == END {
			break
		} else if tok != WHEN {
			return &expr, p.errorExpected(p.pos, p.tok, "WHEN, ELSE or END")
		}
	}

	// Parse optional ELSE block.
	if p.peek() == ELSE {
		expr.Else, _, _ = p.scan()
		if expr.ElseExpr, err = p.ParseExpr(); err != nil {
			return &expr, err
		}
	}

	if p.peek() != END {
		return &expr, p.errorExpected(p.pos, p.tok, "END")
	}
	expr.End, _, _ = p.scan()

	return &expr, nil
}

func (p *Parser) parseExists() (_ *Exists, err error) {
	assert(p.peek() == NOT || p.peek() == EXISTS)

	var expr Exists

	if p.peek() == NOT {
		expr.Not, _, _ = p.scan()
	}

	if p.peek() != EXISTS {
		return &expr, p.errorExpected(p.pos, p.tok, "EXISTS")
	}
	expr.Exists, _, _ = p.scan()

	if p.peek() != LP {
		return &expr, p.errorExpected(p.pos, p.tok, "left paren")
	}
	expr.Lparen, _, _ = p.scan()

	if expr.Select, err = p.parseSelectStatement(false, nil); err != nil {
		return &expr, err
	}

	if p.peek() != RP {
		return &expr, p.errorExpected(p.pos, p.tok, "right paren")
	}
	expr.Rparen, _, _ = p.scan()

	return &expr, nil
}

func (p *Parser) parseIntegerLiteral(desc string) (*IntegerLit, error) {
	pos, tok, lit := p.scan()

	switch tok {
	case INTEGER:
		return &IntegerLit{ValuePos: pos, Value: lit}, nil
	default:
		return nil, p.errorExpected(p.pos, p.tok, desc)
	}
}

func (p *Parser) parseAlterDatabaseStatement(alterPos Pos) (_ *AlterDatabaseStatement, err error) {
	var stmt AlterDatabaseStatement
	stmt.Alter = alterPos
	if p.peek() != DATABASE {
		return &stmt, p.errorExpected(p.pos, p.tok, "DATABASE")
	}
	stmt.Database, _, _ = p.scan()

	if stmt.Name, err = p.parseIdent("database name"); err != nil {
		return &stmt, err
	}

	switch p.peek() {
	case WITH:
		stmt.With, _, _ = p.scan()

		// look for database option
		if !isDatabaseOptionStartToken(p.peek()) {
			return &stmt, p.errorExpected(p.pos, p.tok, "UNITS")
		}
		if stmt.Option, err = p.parseDatabaseOption(); err != nil {
			return &stmt, err
		}
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "WITH")
	}

	return &stmt, nil
}

func (p *Parser) parseAlterTableStatement(alterPos Pos) (_ *AlterTableStatement, err error) {
	var stmt AlterTableStatement
	stmt.Alter = alterPos
	if p.peek() != TABLE {
		return &stmt, p.errorExpected(p.pos, p.tok, "TABLE")
	}
	stmt.Table, _, _ = p.scan()

	if stmt.Name, err = p.parseIdent("table name"); err != nil {
		return &stmt, err
	}

	switch p.peek() {
	case RENAME:
		stmt.Rename, _, _ = p.scan()

		/*// Parse "RENAME TO new-table-name".
		if p.peek() == TO {
			stmt.RenameTo, _, _ = p.scan()
			if stmt.NewName, err = p.parseIdent("new table name"); err != nil {
				return &stmt, err
			}
			return &stmt, nil
		}*/

		// Otherwise parse "RENAME [COLUMN] column-name TO new-column-name".
		if p.peek() == COLUMN {
			stmt.RenameColumn, _, _ = p.scan()
		} else if !isIdentToken(p.peek()) {
			return &stmt, p.errorExpected(p.pos, p.tok, "COLUMN keyword or column name")
		}
		if stmt.OldColumnName, err = p.parseIdent("column name"); err != nil {
			return &stmt, err
		}
		if p.peek() != TO {
			return &stmt, p.errorExpected(p.pos, p.tok, "TO")
		}
		stmt.To, _, _ = p.scan()
		if stmt.NewColumnName, err = p.parseIdent("new column name"); err != nil {
			return &stmt, err
		}

		return &stmt, nil
	case ADD:
		stmt.Add, _, _ = p.scan()
		if p.peek() == COLUMN {
			stmt.AddColumn, _, _ = p.scan()
		} else if !isIdentToken(p.peek()) {
			return &stmt, p.errorExpected(p.pos, p.tok, "COLUMN keyword or column name")
		}
		if stmt.ColumnDef, err = p.parseColumnDefinition(); err != nil {
			return &stmt, err
		}
		return &stmt, nil

	case DROP:
		stmt.Drop, _, _ = p.scan()
		if p.peek() == COLUMN {
			stmt.DropColumn, _, _ = p.scan()
		} else if !isIdentToken(p.peek()) {
			return &stmt, p.errorExpected(p.pos, p.tok, "COLUMN keyword or column name")
		}
		if stmt.DropColumnName, err = p.parseIdent("column name"); err != nil {
			return &stmt, err
		}
		return &stmt, nil

	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "ADD, DROP or RENAME")
	}
}

/*func (p *Parser) parseAnalyzeStatement() (_ *AnalyzeStatement, err error) {
	assert(p.peek() == ANALYZE)

	var stmt AnalyzeStatement
	stmt.Analyze, _, _ = p.scan()
	if stmt.Name, err = p.parseIdent("table or index name"); err != nil {
		return &stmt, err
	}
	return &stmt, nil
}*/

func (p *Parser) scan() (Pos, Token, string) {
	if p.full {
		p.full = false
		return p.pos, p.tok, p.lit
	}

	p.pos, p.tok, p.lit = p.s.Scan()
	return p.pos, p.tok, p.lit
}

// scanBinaryOp performs a scan but combines multi-word operations into a single token.
func (p *Parser) scanBinaryOp() (Pos, Token, error) {
	pos, tok, _ := p.scan()
	switch tok {
	case IS:
		switch p.peek() {
		case NOT:
			p.scan()
			return pos, ISNOT, nil
		}
		return pos, IS, nil
	case NOT:
		switch p.peek() {
		case IN:
			p.scan()
			return pos, NOTIN, nil
		case LIKE:
			p.scan()
			return pos, NOTLIKE, nil
		case GLOB:
			p.scan()
			return pos, NOTGLOB, nil
		case REGEXP:
			p.scan()
			return pos, NOTREGEXP, nil
		case MATCH:
			p.scan()
			return pos, NOTMATCH, nil
		case BETWEEN:
			p.scan()
			return pos, NOTBETWEEN, nil
		default:
			return pos, tok, p.errorExpected(p.pos, p.tok, "IN, LIKE, GLOB, REGEXP, MATCH, or BETWEEN")
		}
	default:
		return pos, tok, nil
	}
}

func (p *Parser) peek() Token {
	if !p.full {
		p.scan()
		p.unscan()
	}
	return p.tok
}

func (p *Parser) unscan() {
	assert(!p.full)
	p.full = true
}

func (p *Parser) errorExpected(pos Pos, tok Token, msg string) error {
	msg = "expected " + msg
	if pos == p.pos {
		if p.tok.IsLiteral() {
			msg += ", found " + p.lit
		} else {
			msg += ", found '" + p.tok.String() + "'"
		}
	}
	return &Error{Pos: pos, Msg: msg}
}

// Error represents a parse error.
type Error struct {
	Pos Pos
	Msg string
}

// Error implements the error interface.
func (e Error) Error() string {
	if e.Pos.IsValid() {
		return e.Pos.String() + ": " + e.Msg
	}
	return e.Msg
}

// isDatabaseOptionStartToken returns true if tok is the initial token of a table option.
func isDatabaseOptionStartToken(tok Token) bool {
	switch tok {
	case UNITS, COMMENT:
		return true
	default:
		return false
	}
}

// isTableOptionStartToken returns true if tok is the initial token of a table option.
func isTableOptionStartToken(tok Token) bool {
	switch tok {
	case KEYPARTITIONS, COMMENT:
		return true
	default:
		return false
	}
}

// isBulkInsertOptionStartToken returns true if tok is the initial token of a bulk insert option.
func isBulkInsertOptionStartToken(tok Token, p *Parser) bool {
	switch tok {
	case IDENT:
		ident, err := p.parseIdent("bulk insert option")
		defer p.unscan()
		if err != nil {
			return false
		}
		switch strings.ToUpper(ident.Name) {
		case "BATCHSIZE", "ROWSLIMIT", "FORMAT", "INPUT", "HEADER_ROW", "ALLOW_MISSING_VALUES":
			return true
		}
	}
	return false
}

// isConstraintStartToken returns true if tok is the initial token of a constraint.
func isConstraintStartToken(tok Token, isTable bool) bool {
	switch tok {
	//case CONSTRAINT, PRIMARY, UNIQUE, CHECK:
	//	return true // table & column
	//case FOREIGN:
	//	return isTable // table only
	case MIN, MAX, TIMEUNIT, TIMEQUANTUM, CACHETYPE:
		return !isTable // column only
	default:
		return false
	}
}

// isLiteralToken returns true if token represents a literal value.
func isLiteralToken(tok Token) bool {
	switch tok {
	case FLOAT, INTEGER, STRING, BLOB, TRUE, FALSE, NULL,
		CURRENT_DATE, CURRENT_TIMESTAMP:
		return true
	default:
		return false
	}
}
