// Copyright 2021 Molecula Corp. All rights reserved.
package parser_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/go-test/deep"
)

func TestExprString(t *testing.T) {
	if got, want := parser.ExprString(&parser.NullLit{}), "NULL"; got != want {
		t.Fatalf("ExprString()=%q, want %q", got, want)
	} else if got, want := parser.ExprString(nil), ""; got != want {
		t.Fatalf("ExprString()=%q, want %q", got, want)
	}
}

func TestSplitExprTree(t *testing.T) {
	t.Run("AND-only", func(t *testing.T) {
		AssertSplitExprTree(t, `x = 1 AND y = 2 AND z = 3`, []parser.Expr{
			&parser.BinaryExpr{X: &parser.Ident{Name: "x"}, Op: parser.EQ, Y: &parser.IntegerLit{Value: "1"}},
			&parser.BinaryExpr{X: &parser.Ident{Name: "y"}, Op: parser.EQ, Y: &parser.IntegerLit{Value: "2"}},
			&parser.BinaryExpr{X: &parser.Ident{Name: "z"}, Op: parser.EQ, Y: &parser.IntegerLit{Value: "3"}},
		})
	})

	t.Run("OR", func(t *testing.T) {
		AssertSplitExprTree(t, `x = 1 AND (y = 2 OR y = 3) AND z = 4`, []parser.Expr{
			&parser.BinaryExpr{X: &parser.Ident{Name: "x"}, Op: parser.EQ, Y: &parser.IntegerLit{Value: "1"}},
			&parser.BinaryExpr{
				X:  &parser.BinaryExpr{X: &parser.Ident{Name: "y"}, Op: parser.EQ, Y: &parser.IntegerLit{Value: "2"}},
				Op: parser.OR,
				Y:  &parser.BinaryExpr{X: &parser.Ident{Name: "y"}, Op: parser.EQ, Y: &parser.IntegerLit{Value: "3"}},
			},
			&parser.BinaryExpr{X: &parser.Ident{Name: "z"}, Op: parser.EQ, Y: &parser.IntegerLit{Value: "4"}},
		})
	})

	t.Run("ParenExpr", func(t *testing.T) {
		AssertSplitExprTree(t, `x = 1 AND (y = 2 AND z = 3)`, []parser.Expr{
			&parser.BinaryExpr{X: &parser.Ident{Name: "x"}, Op: parser.EQ, Y: &parser.IntegerLit{Value: "1"}},
			&parser.BinaryExpr{X: &parser.Ident{Name: "y"}, Op: parser.EQ, Y: &parser.IntegerLit{Value: "2"}},
			&parser.BinaryExpr{X: &parser.Ident{Name: "z"}, Op: parser.EQ, Y: &parser.IntegerLit{Value: "3"}},
		})
	})
}

func AssertSplitExprTree(tb testing.TB, s string, want []parser.Expr) {
	tb.Helper()
	if diff := deep.Equal(parser.SplitExprTree(StripExprPos(parser.MustParseExprString(s))), want); diff != nil {
		tb.Fatal("mismatch: \n" + strings.Join(diff, "\n"))
	}
}

func TestAlterTableStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.AlterTableStatement{
		Name:          &parser.Ident{Name: "foo"},
		RenameColumn:  pos(0),
		OldColumnName: &parser.Ident{Name: "col1"},
		NewColumnName: &parser.Ident{Name: "col2"},
	}, `ALTER TABLE foo RENAME COLUMN col1 TO col2`)

	AssertStatementStringer(t, &parser.AlterTableStatement{
		Name:      &parser.Ident{Name: "foo"},
		AddColumn: pos(0),
		ColumnDef: &parser.ColumnDefinition{
			Name: &parser.Ident{Name: "bar"},
			Type: &parser.Type{Name: &parser.Ident{Name: "INTEGER"}},
		},
	}, `ALTER TABLE foo ADD COLUMN bar INTEGER`)
	AssertStatementStringer(t, &parser.AlterTableStatement{
		Name:           &parser.Ident{Name: "foo"},
		DropColumn:     pos(0),
		DropColumnName: &parser.Ident{Name: "bar"},
	}, `ALTER TABLE foo DROP COLUMN bar`)
}

/*func TestAnalyzeStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.AnalyzeStatement{Name: &parser.Ident{Name: "foo"}}, `ANALYZE "foo"`)
}*/

func TestBeginStatement_String(t *testing.T) {
	t.Skip("BEGIN is currently disabled in the parser")
	AssertStatementStringer(t, &parser.BeginStatement{}, `BEGIN`)
	AssertStatementStringer(t, &parser.BeginStatement{Deferred: pos(0)}, `BEGIN DEFERRED`)
	AssertStatementStringer(t, &parser.BeginStatement{Immediate: pos(0)}, `BEGIN IMMEDIATE`)
	AssertStatementStringer(t, &parser.BeginStatement{Exclusive: pos(0)}, `BEGIN EXCLUSIVE`)
	AssertStatementStringer(t, &parser.BeginStatement{Immediate: pos(0), Transaction: pos(0)}, `BEGIN IMMEDIATE TRANSACTION`)
}

func TestCommitStatement_String(t *testing.T) {
	t.Skip("COMMIT is currently disabled in the parser")
	AssertStatementStringer(t, &parser.CommitStatement{}, `COMMIT`)
	AssertStatementStringer(t, &parser.CommitStatement{End: pos(0)}, `END`)
	AssertStatementStringer(t, &parser.CommitStatement{End: pos(0), Transaction: pos(0)}, `END TRANSACTION`)
}

func TestCreateIndexStatement_String(t *testing.T) {
	t.Skip("CREATE INDEX is currently disabled in the parser")
	AssertStatementStringer(t, &parser.CreateIndexStatement{
		Name:    &parser.Ident{Name: "foo"},
		Table:   &parser.Ident{Name: "bar"},
		Columns: []*parser.IndexedColumn{{X: &parser.Ident{Name: "baz"}}},
	}, `CREATE INDEX "foo" ON "bar" ("baz")`)

	AssertStatementStringer(t, &parser.CreateIndexStatement{
		Unique:      pos(0),
		IfNotExists: pos(0),
		Name:        &parser.Ident{Name: "foo"},
		Table:       &parser.Ident{Name: "bar"},
		Columns: []*parser.IndexedColumn{
			{X: &parser.Ident{Name: "baz"}},
			{X: &parser.Ident{Name: "bat"}},
		},
		WhereExpr: &parser.BoolLit{Value: true},
	}, `CREATE UNIQUE INDEX IF NOT EXISTS "foo" ON "bar" ("baz", "bat") WHERE TRUE`)
}

func TestCreateDatabaseStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.CreateDatabaseStatement{
		Name: &parser.Ident{Name: "db1"},
	}, `CREATE DATABASE db1`)

	AssertStatementStringer(t, &parser.CreateDatabaseStatement{
		Name:        &parser.Ident{Name: "db1"},
		IfNotExists: pos(0),
	}, `CREATE DATABASE IF NOT EXISTS db1`)

	AssertStatementStringer(t, &parser.CreateDatabaseStatement{
		Name: &parser.Ident{Name: "db1"},
		With: pos(10),
		Options: []parser.DatabaseOption{
			&parser.UnitsOption{
				Units: pos(0),
				Expr:  &parser.IntegerLit{Value: "4"},
			},
		},
	}, `CREATE DATABASE db1 WITH UNITS 4`)
}

func TestAlterDatabaseStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.AlterDatabaseStatement{
		Name: &parser.Ident{Name: "db1"},
		Option: &parser.UnitsOption{
			Units: pos(0),
			Expr:  &parser.IntegerLit{Value: "4"},
		},
	}, `ALTER DATABASE db1 WITH UNITS 4`)
}

func TestCreateTableStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.CreateTableStatement{
		Name:        &parser.Ident{Name: "foo"},
		IfNotExists: pos(0),
		Columns: []*parser.ColumnDefinition{
			{
				Name: &parser.Ident{Name: "bar"},
				Type: &parser.Type{Name: &parser.Ident{Name: "INTEGER"}},
			},
			{
				Name: &parser.Ident{Name: "baz"},
				Type: &parser.Type{Name: &parser.Ident{Name: "STRING"}},
			},
		},
	}, `CREATE TABLE IF NOT EXISTS foo (bar INTEGER, baz STRING)`)

	AssertStatementStringer(t, &parser.CreateTableStatement{
		Name:        &parser.Ident{Name: "foo"},
		IfNotExists: pos(0),
		Columns: []*parser.ColumnDefinition{
			{
				Name: &parser.Ident{Name: "boolcol"},
				Type: &parser.Type{Name: &parser.Ident{Name: "BOOL"}},
			},
			{
				Name: &parser.Ident{Name: "decimalcol"},
				Type: &parser.Type{Name: &parser.Ident{Name: "DECIMAL"}},
				Constraints: []parser.Constraint{
					&parser.MinConstraint{Expr: &parser.IntegerLit{Value: "100.25"}},
					&parser.MaxConstraint{Expr: &parser.IntegerLit{Value: "1000.75"}},
				},
			},
			{
				Name: &parser.Ident{Name: "idcol"},
				Type: &parser.Type{Name: &parser.Ident{Name: "ID"}},
				Constraints: []parser.Constraint{
					&parser.CacheTypeConstraint{
						CacheTypeValue: "RANKED",
						Size:           pos(0),
						SizeExpr:       &parser.IntegerLit{Value: "10000"},
					},
				},
			},
			{
				Name: &parser.Ident{Name: "idsetcol"},
				Type: &parser.Type{Name: &parser.Ident{Name: "IDSET"}},
				Constraints: []parser.Constraint{
					&parser.CacheTypeConstraint{
						CacheTypeValue: "RANKED",
						Size:           pos(0),
						SizeExpr:       &parser.IntegerLit{Value: "10000"},
					},
				},
			},
			{
				Name: &parser.Ident{Name: "intcol"},
				Type: &parser.Type{Name: &parser.Ident{Name: "INTEGER"}},
				Constraints: []parser.Constraint{
					&parser.MinConstraint{Expr: &parser.IntegerLit{Value: "100"}},
					&parser.MaxConstraint{Expr: &parser.IntegerLit{Value: "1000"}},
				},
			},
			{
				Name: &parser.Ident{Name: "stringcol"},
				Type: &parser.Type{Name: &parser.Ident{Name: "STRING"}},
				Constraints: []parser.Constraint{
					&parser.CacheTypeConstraint{
						CacheTypeValue: "RANKED",
						Size:           pos(0),
						SizeExpr:       &parser.IntegerLit{Value: "10000"},
					},
				},
			},
			{
				Name: &parser.Ident{Name: "stringsetcol"},
				Type: &parser.Type{Name: &parser.Ident{Name: "STRINGSET"}},
				Constraints: []parser.Constraint{
					&parser.CacheTypeConstraint{
						CacheTypeValue: "RANKED",
						Size:           pos(0),
						SizeExpr:       &parser.IntegerLit{Value: "10000"},
					},
				},
			},
			{
				Name: &parser.Ident{Name: "timestampcol"},
				Type: &parser.Type{Name: &parser.Ident{Name: "TIMESTAMP"}},
				Constraints: []parser.Constraint{
					&parser.TimeUnitConstraint{
						Expr: &parser.StringLit{Value: "s"},
					},
				},
			},
		},
	}, `CREATE TABLE IF NOT EXISTS foo (`+
		`boolcol BOOL, `+
		`decimalcol DECIMAL MIN 100.25 MAX 1000.75, `+
		`idcol ID CACHETYPE RANKED SIZE 10000, `+
		`idsetcol IDSET CACHETYPE RANKED SIZE 10000, `+
		`intcol INTEGER MIN 100 MAX 1000, `+
		`stringcol STRING CACHETYPE RANKED SIZE 10000, `+
		`stringsetcol STRINGSET CACHETYPE RANKED SIZE 10000, `+
		`timestampcol TIMESTAMP TIMEUNIT 's'`+
		`)`)
}

func OLDTestCreateTableStatement_String(t *testing.T) {
	t.Skip("These tests refer to an older version of CREATE TABLE")
	AssertStatementStringer(t, &parser.CreateTableStatement{
		Name:        &parser.Ident{Name: "foo"},
		IfNotExists: pos(0),
		Columns: []*parser.ColumnDefinition{
			{
				Name: &parser.Ident{Name: "bar"},
				Type: &parser.Type{Name: &parser.Ident{Name: "INTEGER"}},
			},
			{
				Name: &parser.Ident{Name: "baz"},
				Type: &parser.Type{Name: &parser.Ident{Name: "TEXT"}},
			},
		},
	}, `CREATE TABLE IF NOT EXISTS "foo" ("bar" INTEGER, "baz" TEXT)`)

	AssertStatementStringer(t, &parser.CreateTableStatement{
		Name: &parser.Ident{Name: "foo"},
		Columns: []*parser.ColumnDefinition{{
			Name: &parser.Ident{Name: "bar"},
			Type: &parser.Type{Name: &parser.Ident{Name: "INTEGER"}},
			Constraints: []parser.Constraint{
				&parser.PrimaryKeyConstraint{Autoincrement: pos(0)},
				&parser.NotNullConstraint{Name: &parser.Ident{Name: "nn"}},
				&parser.DefaultConstraint{Name: &parser.Ident{Name: "def"}, Expr: &parser.IntegerLit{Value: "123"}},
				&parser.DefaultConstraint{Expr: &parser.IntegerLit{Value: "456"}, Lparen: pos(0)},
				&parser.UniqueConstraint{},
			},
		}},
	}, `CREATE TABLE "foo" ("bar" INTEGER PRIMARY KEY AUTOINCREMENT CONSTRAINT "nn" NOT NULL CONSTRAINT "def" DEFAULT 123 DEFAULT (456) UNIQUE)`)

	AssertStatementStringer(t, &parser.CreateTableStatement{
		Name: &parser.Ident{Name: "foo"},
		Columns: []*parser.ColumnDefinition{{
			Name: &parser.Ident{Name: "bar"},
			Type: &parser.Type{Name: &parser.Ident{Name: "INTEGER"}},
			Constraints: []parser.Constraint{
				&parser.ForeignKeyConstraint{
					ForeignTable:   &parser.Ident{Name: "x"},
					ForeignColumns: []*parser.Ident{{Name: "y"}},
					Args: []*parser.ForeignKeyArg{
						{OnDelete: pos(0), SetNull: pos(0)},
						{OnUpdate: pos(0), SetDefault: pos(0)},
						{OnUpdate: pos(0), Cascade: pos(0)},
						{OnUpdate: pos(0), Restrict: pos(0)},
						{OnUpdate: pos(0), NoAction: pos(0)},
					},
				},
			},
		}},
	}, `CREATE TABLE "foo" ("bar" INTEGER REFERENCES "x" ("y") ON DELETE SET NULL ON UPDATE SET DEFAULT ON UPDATE CASCADE ON UPDATE RESTRICT ON UPDATE NO ACTION)`)

	AssertStatementStringer(t, &parser.CreateTableStatement{
		Name: &parser.Ident{Name: "foo"},
		Columns: []*parser.ColumnDefinition{{
			Name: &parser.Ident{Name: "bar"},
			Type: &parser.Type{Name: &parser.Ident{Name: "INTEGER"}},
			Constraints: []parser.Constraint{
				&parser.ForeignKeyConstraint{
					ForeignTable:      &parser.Ident{Name: "x"},
					ForeignColumns:    []*parser.Ident{{Name: "y"}},
					Deferrable:        pos(0),
					InitiallyDeferred: pos(0),
				},
			},
		}},
	}, `CREATE TABLE "foo" ("bar" INTEGER REFERENCES "x" ("y") DEFERRABLE INITIALLY DEFERRED)`)

	AssertStatementStringer(t, &parser.CreateTableStatement{
		Name: &parser.Ident{Name: "foo"},
		Columns: []*parser.ColumnDefinition{{
			Name: &parser.Ident{Name: "bar"},
			Type: &parser.Type{Name: &parser.Ident{Name: "INTEGER"}},
			Constraints: []parser.Constraint{
				&parser.ForeignKeyConstraint{
					ForeignTable:       &parser.Ident{Name: "x"},
					ForeignColumns:     []*parser.Ident{{Name: "y"}},
					NotDeferrable:      pos(0),
					InitiallyImmediate: pos(0),
				},
			},
		}},
	}, `CREATE TABLE "foo" ("bar" INTEGER REFERENCES "x" ("y") NOT DEFERRABLE INITIALLY IMMEDIATE)`)

	AssertStatementStringer(t, &parser.CreateTableStatement{
		Name: &parser.Ident{Name: "foo"},
		Columns: []*parser.ColumnDefinition{{
			Name: &parser.Ident{Name: "bar"},
			Type: &parser.Type{Name: &parser.Ident{Name: "DECIMAL"}, Modifier: &parser.IntegerLit{Value: "100"}},
		}},
		Constraints: []parser.Constraint{
			&parser.PrimaryKeyConstraint{
				Name: &parser.Ident{Name: "pk"},
				Columns: []*parser.Ident{
					{Name: "x"},
					{Name: "y"},
				},
			},
			&parser.UniqueConstraint{
				Name: &parser.Ident{Name: "uniq"},
				Columns: []*parser.Ident{
					{Name: "x"},
					{Name: "y"},
				},
			},
			&parser.CheckConstraint{
				Name: &parser.Ident{Name: "chk"},
				Expr: &parser.BoolLit{Value: true},
			},
		},
	}, `CREATE TABLE "foo" ("bar" DECIMAL(100), CONSTRAINT "pk" PRIMARY KEY ("x", "y"), CONSTRAINT "uniq" UNIQUE ("x", "y"), CONSTRAINT "chk" CHECK (TRUE))`)

	AssertStatementStringer(t, &parser.CreateTableStatement{
		Name: &parser.Ident{Name: "foo"},
		Columns: []*parser.ColumnDefinition{{
			Name: &parser.Ident{Name: "bar"},
			Type: &parser.Type{Name: &parser.Ident{Name: "DECIMAL"}, Modifier: &parser.IntegerLit{Value: "200"}},
		}},
		Constraints: []parser.Constraint{
			&parser.ForeignKeyConstraint{
				Name:           &parser.Ident{Name: "fk"},
				Columns:        []*parser.Ident{{Name: "a"}, {Name: "b"}},
				ForeignTable:   &parser.Ident{Name: "x"},
				ForeignColumns: []*parser.Ident{{Name: "y"}, {Name: "z"}},
			},
		},
	}, `CREATE TABLE "foo" ("bar" DECIMAL(200), CONSTRAINT "fk" FOREIGN KEY ("a", "b") REFERENCES "x" ("y", "z"))`)

	AssertStatementStringer(t, &parser.CreateTableStatement{
		Name: &parser.Ident{Name: "foo"},
		Select: &parser.SelectStatement{
			Columns: []*parser.ResultColumn{{Star: pos(0)}},
		},
	}, `CREATE TABLE "foo" AS SELECT *`)
}

func TestCreateFunctionStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.CreateFunctionStatement{
		Name: &parser.Ident{Name: "func"},
		Parameters: []*parser.ParameterDefinition{
			{
				Name: &parser.Variable{Name: "@param1"},
				Type: &parser.Type{Name: &parser.Ident{Name: "int"}},
			},
		},
		ReturnType: &parser.Type{Name: &parser.Ident{Name: "int"}},
	}, `CREATE FUNCTION func (@param1 int) RETURNS int AS BEGIN END`)

	AssertStatementStringer(t, &parser.CreateFunctionStatement{
		IfNotExists: pos(0),
		Name:        &parser.Ident{Name: "func"},
		Parameters: []*parser.ParameterDefinition{
			{
				Name: &parser.Variable{Name: "@param1"},
				Type: &parser.Type{Name: &parser.Ident{Name: "int"}},
			},
		},
		ReturnType: &parser.Type{Name: &parser.Ident{Name: "int"}},
	}, `CREATE FUNCTION IF NOT EXISTS func (@param1 int) RETURNS int AS BEGIN END`)
}

func TestCreateViewStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.CreateViewStatement{
		Name: &parser.Ident{Name: "vw"},
		// Columns: []*parser.Ident{
		// 	{Name: "x"},
		// 	{Name: "y"},
		// },
		Select: &parser.SelectStatement{
			Columns: []*parser.ResultColumn{{Star: pos(0)}},
		},
		//}, `CREATE VIEW vw (x, y) AS SELECT *`)
	}, `CREATE VIEW vw AS SELECT *`)

	AssertStatementStringer(t, &parser.CreateViewStatement{
		IfNotExists: pos(0),
		Name:        &parser.Ident{Name: "vw"},
		Select: &parser.SelectStatement{
			Columns: []*parser.ResultColumn{{Star: pos(0)}},
		},
	}, `CREATE VIEW IF NOT EXISTS vw AS SELECT *`)
}

func TestAlterViewStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.AlterViewStatement{
		Name: &parser.Ident{Name: "vw"},
		// Columns: []*parser.Ident{
		// 	{Name: "x"},
		// 	{Name: "y"},
		// },
		Select: &parser.SelectStatement{
			Columns: []*parser.ResultColumn{{Star: pos(0)}},
		},
		//}, `CREATE VIEW vw (x, y) AS SELECT *`)
	}, `ALTER VIEW vw AS SELECT *`)
}

func TestDeleteStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.DeleteStatement{
		TableName: &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}, Alias: &parser.Ident{Name: "tbl2"}, As: pos(0)},
	}, `DELETE FROM tbl AS tbl2`)

	// AssertStatementStringer(t, &sql.DeleteStatement{
	// 	Table: &sql.QualifiedTableName{Name: &sql.Ident{Name: "tbl"}, Index: &sql.Ident{Name: "idx"}},
	// }, `DELETE FROM "tbl" INDEXED BY "idx"`)

	// AssertStatementStringer(t, &sql.DeleteStatement{
	// 	Table: &sql.QualifiedTableName{Name: &sql.Ident{Name: "tbl"}, NotIndexed: pos(0)},
	// }, `DELETE FROM "tbl" NOT INDEXED`)

	/*AssertStatementStringer(t, &parser.DeleteStatement{
		Table:     &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		WhereExpr: &parser.BoolLit{Value: true},
		OrderingTerms: []*parser.OrderingTerm{
			{X: &parser.Ident{Name: "x"}},
			{X: &parser.Ident{Name: "y"}},
		},
		LimitExpr:  &parser.IntegerLit{Value: "10"},
		OffsetExpr: &parser.IntegerLit{Value: "5"},
	}, `DELETE FROM "tbl" WHERE TRUE ORDER BY "x", "y" LIMIT 10 OFFSET 5`)*/

	// AssertStatementStringer(t, &sql.DeleteStatement{
	// 	WithClause: &sql.WithClause{
	// 		Recursive: pos(0),
	// 		CTEs: []*sql.CTE{{
	// 			TableName: &sql.Ident{Name: "cte"},
	// 			Select: &sql.SelectStatement{
	// 				Columns: []*sql.ResultColumn{{Star: pos(0)}},
	// 			},
	// 		}},
	// 	},
	// 	Table: &sql.QualifiedTableName{Name: &sql.Ident{Name: "tbl"}},
	// }, `WITH RECURSIVE "cte" AS (SELECT *) DELETE FROM "tbl"`)
}

func TestDropIndexStatement_String(t *testing.T) {
	t.Skip("DROP INDEX is currently disabled in the parser")
	AssertStatementStringer(t, &parser.DropIndexStatement{
		Name: &parser.Ident{Name: "idx"},
	}, `DROP INDEX "idx"`)

	AssertStatementStringer(t, &parser.DropIndexStatement{
		IfExists: pos(0),
		Name:     &parser.Ident{Name: "idx"},
	}, `DROP INDEX IF EXISTS "idx"`)
}

func TestDropDatabaseStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.DropDatabaseStatement{
		Name: &parser.Ident{Name: "db"},
	}, `DROP DATABASE db`)

	AssertStatementStringer(t, &parser.DropDatabaseStatement{
		IfExists: pos(0),
		Name:     &parser.Ident{Name: "db"},
	}, `DROP DATABASE IF EXISTS db`)
}

func TestDropTableStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.DropTableStatement{
		Name: &parser.Ident{Name: "tbl"},
	}, `DROP TABLE tbl`)

	AssertStatementStringer(t, &parser.DropTableStatement{
		IfExists: pos(0),
		Name:     &parser.Ident{Name: "tbl"},
	}, `DROP TABLE IF EXISTS tbl`)
}

func TestDropTriggerStatement_String(t *testing.T) {
	t.Skip("DROP TRIGGER is currently disabled in the parser")
	AssertStatementStringer(t, &parser.DropFunctionStatement{
		Name: &parser.Ident{Name: "trig"},
	}, `DROP TRIGGER "trig"`)

	AssertStatementStringer(t, &parser.DropFunctionStatement{
		IfExists: pos(0),
		Name:     &parser.Ident{Name: "trig"},
	}, `DROP TRIGGER IF EXISTS "trig"`)
}

func TestDropViewStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.DropViewStatement{
		Name: &parser.Ident{Name: "vw"},
	}, `DROP VIEW vw`)

	AssertStatementStringer(t, &parser.DropViewStatement{
		IfExists: pos(0),
		Name:     &parser.Ident{Name: "vw"},
	}, `DROP VIEW IF EXISTS vw`)
}

func TestExplainStatement_String(t *testing.T) {
	t.Skip("These are currently disabled in the parser")
	AssertStatementStringer(t, &parser.ExplainStatement{
		Stmt: &parser.DropViewStatement{
			Name: &parser.Ident{Name: "vw"},
		},
	}, `EXPLAIN DROP VIEW "vw"`)

	AssertStatementStringer(t, &parser.ExplainStatement{
		QueryPlan: pos(0),
		Stmt: &parser.DropViewStatement{
			Name: &parser.Ident{Name: "vw"},
		},
	}, `EXPLAIN QUERY PLAN DROP VIEW "vw"`)
}

func TestInsertStatement_String(t *testing.T) {
	/*AssertStatementStringer(t, &parser.InsertStatement{
		Table:         &parser.Ident{Name: "tbl"},
		DefaultValues: pos(0),
	}, `INSERT INTO "tbl" DEFAULT VALUES`)*/

	/*AssertStatementStringer(t, &parser.InsertStatement{
		Table:         &parser.Ident{Name: "tbl"},
		Alias:         &parser.Ident{Name: "x"},
		DefaultValues: pos(0),
	}, `INSERT INTO "tbl" AS "x" DEFAULT VALUES`)*/

	/*AssertStatementStringer(t, &parser.InsertStatement{
		InsertOrReplace: pos(0),
		Table:           &parser.Ident{Name: "tbl"},
		DefaultValues:   pos(0),
	}, `INSERT OR REPLACE INTO "tbl" DEFAULT VALUES`)*/

	/*AssertStatementStringer(t, &parser.InsertStatement{
		InsertOrRollback: pos(0),
		Table:            &parser.Ident{Name: "tbl"},
		DefaultValues:    pos(0),
	}, `INSERT OR ROLLBACK INTO "tbl" DEFAULT VALUES`)*/

	/*AssertStatementStringer(t, &parser.InsertStatement{
		InsertOrAbort: pos(0),
		Table:         &parser.Ident{Name: "tbl"},
		DefaultValues: pos(0),
	}, `INSERT OR ABORT INTO "tbl" DEFAULT VALUES`)*/

	/*AssertStatementStringer(t, &parser.InsertStatement{
		InsertOrFail:  pos(0),
		Table:         &parser.Ident{Name: "tbl"},
		DefaultValues: pos(0),
	}, `INSERT OR FAIL INTO "tbl" DEFAULT VALUES`)*/

	/*AssertStatementStringer(t, &parser.InsertStatement{
		InsertOrIgnore: pos(0),
		Table:          &parser.Ident{Name: "tbl"},
		DefaultValues:  pos(0),
	}, `INSERT OR IGNORE INTO "tbl" DEFAULT VALUES`)*/

	/*AssertStatementStringer(t, &parser.InsertStatement{
		Replace:       pos(0),
		Table:         &parser.Ident{Name: "tbl"},
		DefaultValues: pos(0),
	}, `REPLACE INTO "tbl" DEFAULT VALUES`)*/

	/*AssertStatementStringer(t, &parser.InsertStatement{
		Table: &parser.Ident{Name: "tbl"},
		Select: &parser.SelectStatement{
			Columns: []*parser.ResultColumn{{Star: pos(0)}},
		},
	}, `INSERT INTO "tbl" SELECT *`)*/

	AssertStatementStringer(t, &parser.InsertStatement{
		Table: &parser.Ident{Name: "tbl"},
		Columns: []*parser.Ident{
			{Name: "x"},
			{Name: "y"},
		},
		TupleList: []*parser.ExprList{
			{
				Exprs: []parser.Expr{&parser.NullLit{}, &parser.NullLit{}},
			},
		},
	}, `INSERT INTO tbl (x, y) VALUES (NULL, NULL)`)

	AssertStatementStringer(t, &parser.InsertStatement{
		Table: &parser.Ident{Name: "tbl"},
		Columns: []*parser.Ident{
			{Name: "x"},
			{Name: "y"},
		},
		TupleList: []*parser.ExprList{
			{
				Exprs: []parser.Expr{&parser.IntegerLit{Value: "1"}, &parser.IntegerLit{Value: "2"}},
			},
			{
				Exprs: []parser.Expr{&parser.IntegerLit{Value: "3"}, &parser.IntegerLit{Value: "4"}},
			},
		},
	}, `INSERT INTO tbl (x, y) VALUES (1, 2), (3, 4)`)

	// AssertStatementStringer(t, &sql.InsertStatement{
	// 	WithClause: &sql.WithClause{
	// 		CTEs: []*sql.CTE{
	// 			{
	// 				TableName: &sql.Ident{Name: "cte"},
	// 				Select: &sql.SelectStatement{
	// 					Columns: []*sql.ResultColumn{{Star: pos(0)}},
	// 				},
	// 			},
	// 			{
	// 				TableName: &sql.Ident{Name: "cte2"},
	// 				Select: &sql.SelectStatement{
	// 					Columns: []*sql.ResultColumn{{Star: pos(0)}},
	// 				},
	// 			},
	// 		},
	// 	},
	// 	Table:         &sql.Ident{Name: "tbl"},
	// 	DefaultValues: pos(0),
	// }, `WITH "cte" AS (SELECT *), "cte2" AS (SELECT *) INSERT INTO "tbl" DEFAULT VALUES`)

	/*AssertStatementStringer(t, &parser.InsertStatement{
		Table:         &parser.Ident{Name: "tbl"},
		DefaultValues: pos(0),
		UpsertClause: &parser.UpsertClause{
			DoNothing: pos(0),
		},
	}, `INSERT INTO "tbl" DEFAULT VALUES ON CONFLICT DO NOTHING`)*/

	/*AssertStatementStringer(t, &parser.InsertStatement{
		Table:         &parser.Ident{Name: "tbl"},
		DefaultValues: pos(0),
		UpsertClause: &parser.UpsertClause{
			Columns: []*parser.IndexedColumn{
				{X: &parser.Ident{Name: "x"}, Asc: pos(0)},
				{X: &parser.Ident{Name: "y"}, Desc: pos(0)},
			},
			WhereExpr: &parser.BoolLit{Value: true},
			Assignments: []*parser.Assignment{
				{Columns: []*parser.Ident{{Name: "x"}}, Expr: &parser.IntegerLit{Value: "100"}},
				{Columns: []*parser.Ident{{Name: "y"}, {Name: "z"}}, Expr: &parser.IntegerLit{Value: "200"}},
			},
			UpdateWhereExpr: &parser.BoolLit{Value: false},
		},
	}, `INSERT INTO "tbl" DEFAULT VALUES ON CONFLICT ("x" ASC, "y" DESC) WHERE TRUE DO UPDATE SET "x" = 100, ("y", "z") = 200 WHERE FALSE`)*/

	// Testing upsert clause separately until it is enabled in Insert.
	{
		upsertast := parser.UpsertClause{
			DoNothing: pos(0),
		}
		upsertsql := `ON CONFLICT DO NOTHING`

		if upsertast.String() != upsertsql {
			t.Fatalf("parser.UpsertClause.String()=%q, want %q", upsertast.String(), upsertsql)
		}
		upsertast = parser.UpsertClause{
			Columns: []*parser.IndexedColumn{
				{X: &parser.Ident{Name: "x"}, Asc: pos(0)},
				{X: &parser.Ident{Name: "y"}, Desc: pos(0)},
			},
			WhereExpr: &parser.BoolLit{Value: true},
			Assignments: []*parser.Assignment{
				{Columns: []*parser.Ident{{Name: "x"}}, Expr: &parser.IntegerLit{Value: "100"}},
				{Columns: []*parser.Ident{{Name: "y"}, {Name: "z"}}, Expr: &parser.IntegerLit{Value: "200"}},
			},
			UpdateWhereExpr: &parser.BoolLit{Value: false},
		}
		upsertsql = "ON CONFLICT (x ASC, y DESC) WHERE TRUE DO UPDATE SET x = 100, (y, z) = 200 WHERE FALSE"

		if upsertast.String() != upsertsql {
			t.Fatalf("parser.UpsertClause.String()=%q, want %q", upsertast.String(), upsertsql)
		}

		if upsertast.Clone().String() != upsertsql {
			t.Fatalf("parser.UpsertClause.Clone().String()=%q, want %q", upsertast.Clone().String(), upsertsql)
		}
	}
}

// Test Bulk Insert for CSV format
func TestBulkInsertStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.BulkInsertStatement{
		Table: &parser.Ident{Name: "tbl"},
		Columns: []*parser.Ident{
			{Name: "string"},
			{Name: "int"},
			{Name: "decimal"},
			{Name: "timestamp"},
		},
		MapList: []*parser.BulkInsertMapDefinition{
			{Name: &parser.Ident{Name: "string"},
				Type:    &parser.Type{Name: &parser.Ident{Name: "STRING"}},
				MapExpr: &parser.Ident{Name: "1"}},
			{Name: &parser.Ident{Name: "int"},
				Type:    &parser.Type{Name: &parser.Ident{Name: "INT"}},
				MapExpr: &parser.Ident{Name: "2"}},
			{Name: &parser.Ident{Name: "decimal"},
				Type: &parser.Type{Name: &parser.Ident{Name: "DECIMAL"},
					Modifier: &parser.IntegerLit{Value: "2"}},
				MapExpr: &parser.Ident{Name: "3"}},
			{Name: &parser.Ident{Name: "timestamp"},
				Type:    &parser.Type{Name: &parser.Ident{Name: "TIMESTAMP"}},
				MapExpr: &parser.Ident{Name: "4"}},
		},
		TransformList: []parser.Expr{
			&parser.CaseExpr{
				//Operand: &parser.Ident{Name: "foo"},
				Blocks: []*parser.CaseBlock{
					{Condition: &parser.BinaryExpr{Op: parser.EQ, X: &parser.Variable{Name: "@0", VariableIndex: 0}, Y: &parser.StringLit{Value: "Texas"}}, Body: &parser.StringLit{Value: "TX"}},
					{Condition: &parser.BinaryExpr{Op: parser.EQ, X: &parser.Variable{Name: "@0", VariableIndex: 0}, Y: &parser.StringLit{Value: "Mass"}}, Body: &parser.StringLit{Value: "MA"}},
				},
				ElseExpr: &parser.NullLit{},
			},
			&parser.BinaryExpr{Op: parser.STAR, X: &parser.Variable{Name: "@1", VariableIndex: 1}, Y: &parser.IntegerLit{Value: "10"}},
			&parser.Variable{Name: "@2", VariableIndex: 2},
			&parser.SysVariable{Token: parser.CURRENT_TIMESTAMP},
		},
		DataSource:         &parser.StringLit{Value: "csvdata.csv"},
		BatchSize:          &parser.IntegerLit{Value: "100000"},
		Format:             &parser.StringLit{Value: "CSV"},
		Input:              &parser.StringLit{Value: "FILE"},
		RowsLimit:          &parser.IntegerLit{Value: "1000000"},
		HeaderRow:          &parser.BoolLit{Value: false},
		AllowMissingValues: &parser.BoolLit{Value: true},
	}, `BULK INSERT INTO  tbl(string, int, decimal, timestamp) MAP (1 STRING, 2 INT, 3 DECIMAL(2), 4 TIMESTAMP) TRANSFORM (CASE WHEN @0 = 'Texas' THEN 'TX' WHEN @0 = 'Mass' THEN 'MA' ELSE NULL END, @1 * 10, @2, CURRENT_TIMESTAMP) FROM  'csvdata.csv' WITH FORMAT 'CSV' INPUT 'FILE' HEADER_ROW  BATCHSIZE 100000 ROWSLIMIT 1000000 ALLOW_MISSING_VALUES `)

}

func TestReleaseStatement_String(t *testing.T) {
	t.Skip("RELEASE is currently disabled in the parser")
	AssertStatementStringer(t, &parser.ReleaseStatement{Name: &parser.Ident{Name: "x"}}, `RELEASE "x"`)
	AssertStatementStringer(t, &parser.ReleaseStatement{Savepoint: pos(0), Name: &parser.Ident{Name: "x"}}, `RELEASE SAVEPOINT "x"`)
}

func TestRollbackStatement_String(t *testing.T) {
	t.Skip("ROLLBACK is currently disabled in the parser")
	AssertStatementStringer(t, &parser.RollbackStatement{}, `ROLLBACK`)
	AssertStatementStringer(t, &parser.RollbackStatement{Transaction: pos(0)}, `ROLLBACK TRANSACTION`)
	AssertStatementStringer(t, &parser.RollbackStatement{SavepointName: &parser.Ident{Name: "x"}}, `ROLLBACK TO "x"`)
	AssertStatementStringer(t, &parser.RollbackStatement{Savepoint: pos(0), SavepointName: &parser.Ident{Name: "x"}}, `ROLLBACK TO SAVEPOINT "x"`)
}

func TestSavepointStatement_String(t *testing.T) {
	t.Skip("SAVEPOINT is currently disabled in the parser")
	AssertStatementStringer(t, &parser.SavepointStatement{Name: &parser.Ident{Name: "x"}}, `SAVEPOINT "x"`)
}

func TestSelectStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.SelectStatement{
		Columns: []*parser.ResultColumn{
			{Expr: &parser.Ident{Name: "x"}, Alias: &parser.Ident{Name: "y"}, As: pos(0)},
			{Expr: &parser.Ident{Name: "z"}},
		},
	}, `SELECT x AS y, z`)

	AssertStatementStringer(t, &parser.SelectStatement{
		Distinct: pos(0),
		Columns: []*parser.ResultColumn{
			{Expr: &parser.Ident{Name: "x"}},
		},
	}, `SELECT DISTINCT x`)

	AssertStatementStringer(t, &parser.SelectStatement{
		Top:     pos(0),
		TopExpr: &parser.IntegerLit{Value: "10"},
		Columns: []*parser.ResultColumn{
			{Expr: &parser.Ident{Name: "x"}},
		},
	}, `SELECT TOP(10) x`)

	AssertStatementStringer(t, &parser.SelectStatement{
		TopN:    pos(0),
		TopExpr: &parser.IntegerLit{Value: "10"},
		Columns: []*parser.ResultColumn{
			{Expr: &parser.Ident{Name: "x"}},
		},
	}, `SELECT TOPN(10) x`)

	// AssertStatementStringer(t, &sql.SelectStatement{
	// 	All: pos(0),
	// 	Columns: []*sql.ResultColumn{
	// 		{Expr: &sql.Ident{Name: "x"}},
	// 	},
	// }, `SELECT ALL "x"`)

	AssertStatementStringer(t, &parser.SelectStatement{
		Columns:      []*parser.ResultColumn{{Star: pos(0)}},
		Source:       &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		WhereExpr:    &parser.BoolLit{Value: true},
		GroupByExprs: []parser.Expr{&parser.Ident{Name: "x"}, &parser.Ident{Name: "y"}},
		HavingExpr:   &parser.Ident{Name: "z"},
	}, `SELECT * FROM tbl WHERE TRUE GROUP BY x, y HAVING z`)

	AssertStatementStringer(t, &parser.SelectStatement{
		Columns: []*parser.ResultColumn{{Star: pos(0)}},
		Source: &parser.ParenSource{
			X:     &parser.SelectStatement{Columns: []*parser.ResultColumn{{Star: pos(0)}}},
			Alias: &parser.Ident{Name: "tbl"},
		},
	}, `SELECT * FROM (SELECT *) AS tbl`)

	AssertStatementStringer(t, &parser.SelectStatement{
		Columns: []*parser.ResultColumn{{Star: pos(0)}},
		Source: &parser.ParenSource{
			X: &parser.SelectStatement{Columns: []*parser.ResultColumn{{Star: pos(0)}}},
		},
	}, `SELECT * FROM (SELECT *)`)

	// AssertStatementStringer(t, &parser.SelectStatement{
	// 	Columns: []*parser.ResultColumn{{Star: pos(0)}},
	// 	Source:  &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
	// 	Windows: []*parser.Window{
	// 		{
	// 			Name: &parser.Ident{Name: "win1"},
	// 			Definition: &parser.WindowDefinition{
	// 				Base:       &parser.Ident{Name: "base"},
	// 				Partitions: []parser.Expr{&parser.Ident{Name: "x"}, &parser.Ident{Name: "y"}},
	// 				OrderingTerms: []*parser.OrderingTerm{
	// 					{X: &parser.Ident{Name: "x"}, Asc: pos(0), NullsFirst: pos(0)},
	// 					{X: &parser.Ident{Name: "y"}, Desc: pos(0), NullsLast: pos(0)},
	// 				},
	// 				Frame: &parser.FrameSpec{
	// 					Range:      pos(0),
	// 					UnboundedX: pos(0),
	// 					PrecedingX: pos(0),
	// 				},
	// 			},
	// 		},
	// 		{
	// 			Name: &parser.Ident{Name: "win2"},
	// 			Definition: &parser.WindowDefinition{
	// 				Base: &parser.Ident{Name: "base2"},
	// 			},
	// 		},
	// 	},
	// }, `SELECT * FROM tbl WINDOW win1 AS (base PARTITION BY x, y ORDER BY x ASC NULLS FIRST, y DESC NULLS LAST RANGE UNBOUNDED PRECEDING), win2 AS (base2)`)

	// AssertStatementStringer(t, &sql.SelectStatement{
	// 	WithClause: &sql.WithClause{
	// 		CTEs: []*sql.CTE{{
	// 			TableName: &sql.Ident{Name: "cte"},
	// 			Columns: []*sql.Ident{
	// 				{Name: "x"},
	// 				{Name: "y"},
	// 			},
	// 			Select: &sql.SelectStatement{
	// 				Columns: []*sql.ResultColumn{{Star: pos(0)}},
	// 			},
	// 		}},
	// 	},
	// 	ValueLists: []*sql.ExprList{
	// 		{Exprs: []sql.Expr{&sql.NumberLit{Value: "1"}, &sql.NumberLit{Value: "2"}}},
	// 		{Exprs: []sql.Expr{&sql.NumberLit{Value: "3"}, &sql.NumberLit{Value: "4"}}},
	// 	},
	// }, `WITH "cte" ("x", "y") AS (SELECT *) VALUES (1, 2), (3, 4)`)

	// AssertStatementStringer(t, &parser.SelectStatement{
	// 	Columns: []*parser.ResultColumn{{Star: pos(0)}},
	// 	Union:   pos(0),
	// 	Compound: &parser.SelectStatement{
	// 		Columns: []*parser.ResultColumn{{Star: pos(0)}},
	// 	},
	// }, `SELECT * UNION SELECT *`)

	// AssertStatementStringer(t, &parser.SelectStatement{
	// 	Columns:  []*parser.ResultColumn{{Star: pos(0)}},
	// 	Union:    pos(0),
	// 	UnionAll: pos(0),
	// 	Compound: &parser.SelectStatement{
	// 		Columns: []*parser.ResultColumn{{Star: pos(0)}},
	// 	},
	// }, `SELECT * UNION ALL SELECT *`)

	// AssertStatementStringer(t, &parser.SelectStatement{
	// 	Columns:   []*parser.ResultColumn{{Star: pos(0)}},
	// 	Intersect: pos(0),
	// 	Compound: &parser.SelectStatement{
	// 		Columns: []*parser.ResultColumn{{Star: pos(0)}},
	// 	},
	// }, `SELECT * INTERSECT SELECT *`)

	// AssertStatementStringer(t, &parser.SelectStatement{
	// 	Columns: []*parser.ResultColumn{{Star: pos(0)}},
	// 	Except:  pos(0),
	// 	Compound: &parser.SelectStatement{
	// 		Columns: []*parser.ResultColumn{{Star: pos(0)}},
	// 	},
	// }, `SELECT * EXCEPT SELECT *`)

	AssertStatementStringer(t, &parser.SelectStatement{
		Columns: []*parser.ResultColumn{{Star: pos(0)}},
		OrderingTerms: []*parser.OrderingTerm{
			{X: &parser.Ident{Name: "x"}},
			{X: &parser.Ident{Name: "y"}},
		},
	}, `SELECT * ORDER BY x, y`)

	AssertStatementStringer(t, &parser.SelectStatement{
		Columns: []*parser.ResultColumn{{Star: pos(0)}},
		Source: &parser.JoinClause{
			X:        &parser.QualifiedTableName{Name: &parser.Ident{Name: "x"}},
			Operator: &parser.JoinOperator{Comma: pos(0)},
			Y:        &parser.QualifiedTableName{Name: &parser.Ident{Name: "y"}},
		},
	}, `SELECT * FROM x, y`)

	AssertStatementStringer(t, &parser.SelectStatement{
		Columns: []*parser.ResultColumn{{Star: pos(0)}},
		Source: &parser.JoinClause{
			X:          &parser.QualifiedTableName{Name: &parser.Ident{Name: "x"}},
			Operator:   &parser.JoinOperator{},
			Y:          &parser.QualifiedTableName{Name: &parser.Ident{Name: "y"}},
			Constraint: &parser.OnConstraint{X: &parser.BoolLit{Value: true}},
		},
	}, `SELECT * FROM x JOIN y ON TRUE`)

	AssertStatementStringer(t, &parser.SelectStatement{
		Columns: []*parser.ResultColumn{{Star: pos(0)}},
		Source: &parser.JoinClause{
			X:        &parser.QualifiedTableName{Name: &parser.Ident{Name: "x"}},
			Operator: &parser.JoinOperator{Left: pos(0)},
			Y:        &parser.QualifiedTableName{Name: &parser.Ident{Name: "y"}},
		},
	}, `SELECT * FROM x LEFT JOIN y`)

	AssertStatementStringer(t, &parser.SelectStatement{
		Columns: []*parser.ResultColumn{{Star: pos(0)}},
		Source: &parser.JoinClause{
			X:        &parser.QualifiedTableName{Name: &parser.Ident{Name: "x"}},
			Operator: &parser.JoinOperator{Left: pos(0), Outer: pos(0)},
			Y:        &parser.QualifiedTableName{Name: &parser.Ident{Name: "y"}},
		},
	}, `SELECT * FROM x LEFT OUTER JOIN y`)

	// AssertStatementStringer(t, &parser.SelectStatement{
	// 	Columns: []*parser.ResultColumn{{Star: pos(0)}},
	// 	Source: &parser.JoinClause{
	// 		X:        &parser.QualifiedTableName{Name: &parser.Ident{Name: "x"}},
	// 		Operator: &parser.JoinOperator{Cross: pos(0)},
	// 		Y:        &parser.QualifiedTableName{Name: &parser.Ident{Name: "y"}},
	// 	},
	// }, `SELECT * FROM x CROSS JOIN y`)

	// Test SELECT with WITH clause only upto SQL comparison, skip AssertStatementSanity() until parser can handle WITH clauses.
	{
		selectast := parser.SelectStatement{
			WithClause: &parser.WithClause{
				CTEs: []*parser.CTE{
					{
						TableName: &parser.Ident{Name: "cte"},
						Columns: []*parser.Ident{
							{Name: "col1"},
							{Name: "col2"},
						},
						Select: &parser.SelectStatement{
							Columns: []*parser.ResultColumn{
								{Expr: &parser.Ident{Name: "col1"}},
								{Expr: &parser.Ident{Name: "col2"}},
							},
							Source: &parser.QualifiedTableName{Name: &parser.Ident{Name: "table"}},
						},
						As: parser.Pos{Column: 1},
					}},
			},
			Columns: []*parser.ResultColumn{{Star: pos(0)}},
			Source:  &parser.QualifiedTableName{Name: &parser.Ident{Name: "cte"}},
		}
		selectsql := `WITH cte (col1, col2) AS (SELECT col1, col2 FROM table) SELECT * FROM cte`

		if s := selectast.String(); s != selectsql {
			t.Fatalf("parser.SelectStatement.String()=%q, want %q", s, selectsql)
		}
		if s := selectast.Clone().String(); s != selectsql {
			t.Fatalf("parser.SelectStatement.Clone().String()=%q, want %q", s, selectsql)
		}
	}
	// Test SelectStatement.HasWildcard()
	{
		selectast := &parser.SelectStatement{
			Columns: []*parser.ResultColumn{{Star: pos(0)}},
			Source:  &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		}
		if !selectast.HasWildcard() {
			t.Fatalf("parser.SelectStatement.HasWildcard()=%v, want %v", false, true)
		}

		selectast = &parser.SelectStatement{
			Columns: []*parser.ResultColumn{{Expr: &parser.QualifiedRef{Star: pos(0)}}},
			Source:  &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		}
		if !selectast.HasWildcard() {
			t.Fatalf("parser.SelectStatement.HasWildcard()=%v, want %v", false, true)
		}

		selectast = &parser.SelectStatement{
			Columns: []*parser.ResultColumn{{Expr: &parser.Ident{Name: "col"}}},
			Source:  &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		}
		if selectast.HasWildcard() {
			t.Fatalf("parser.SelectStatement.HasWildcard()=%v, want %v", true, false)
		}
	}
}

func TestSources_String(t *testing.T) {
	// Test helper functions for QualifiedTableName
	{
		qtast := parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}}
		if s := qtast.TableName(); s != "tbl" {
			t.Fatalf("parser.QualifiedTableName.TableName()=%v, want %v", s, "tbl")
		}
		if !qtast.MatchesTablenameOrAlias("tbl") {
			t.Fatalf("parser.QualifiedTableName.MatchesTablenameOrAlias()=%v, want %v", false, true)
		}
		qtast = parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}, Alias: &parser.Ident{Name: "t1"}}
		if qtast.SourceFromAlias("t1") != qtast.SourceFromAlias("tbl") {
			t.Fatalf("parser.QualifiedTableName.SourceFromAlias()=%v, want %v", qtast.SourceFromAlias("t1"), qtast.SourceFromAlias("tbl"))
		}
		qtast = parser.QualifiedTableName{
			Name:  &parser.Ident{Name: "tbl"},
			Alias: &parser.Ident{Name: "t1"},
			OutputColumns: []*parser.SourceOutputColumn{
				{TableName: "tbl", ColumnName: "col1", ColumnIndex: 1},
				{TableName: "tbl", ColumnName: "col2", ColumnIndex: 2},
			},
		}
		if n := len(qtast.PossibleOutputColumns()); n != 2 {
			t.Fatalf("len(parser.QualifiedTableName.PossibleOutputColumns())=%v, want %v", n, 2)
		}
		if c, _ := qtast.OutputColumnNamed("col1"); c.ColumnName != "col1" {
			t.Fatalf("parser.QualifiedTableName.OutputColumnNamed()=%v, want %v", c.ColumnName, "col1")
		}
		if c, _ := qtast.OutputColumnNamed("col99"); c != nil {
			t.Fatalf("parser.QualifiedTableName.OutputColumnNamed()=%v, want %v", c, nil)
		}
		if c, _ := qtast.OutputColumnQualifierNamed("tbl", "col1"); c.ColumnName != "col1" {
			t.Fatalf("parser.QualifiedTableName.OutputColumnQualifierNamed()=%v, want %v", c.ColumnName, "col1")
		}
		if c, _ := qtast.OutputColumnQualifierNamed("t1", "col1"); c.ColumnName != "col1" {
			t.Fatalf("parser.QualifiedTableName.OutputColumnQualifierNamed()=%v, want %v", c.ColumnName, "col1")
		}
		if c, _ := qtast.OutputColumnQualifierNamed("t9", "col99"); c != nil {
			t.Fatalf("parser.QualifiedTableName.OutputColumnQualifierNamed()=%v, want %v", c, nil)
		}
	}

	// Test helper functions for JoinClause
	{
		jcast := parser.JoinClause{
			X: &parser.QualifiedTableName{
				Name:  &parser.Ident{Name: "tbl1"},
				Alias: &parser.Ident{Name: "t1"},
				OutputColumns: []*parser.SourceOutputColumn{
					{TableName: "tbl1", ColumnName: "col1", ColumnIndex: 1},
					{TableName: "tbl1", ColumnName: "col2", ColumnIndex: 2},
				},
			},
			Y: &parser.QualifiedTableName{
				Name:  &parser.Ident{Name: "tbl2"},
				Alias: &parser.Ident{Name: "t2"},
				OutputColumns: []*parser.SourceOutputColumn{
					{TableName: "tbl2", ColumnName: "col3", ColumnIndex: 1},
					{TableName: "tbl2", ColumnName: "col4", ColumnIndex: 2},
				},
			},
		}

		if n := len(jcast.PossibleOutputColumns()); n != 4 {
			t.Fatalf("len(parser.JoinClause.PossibleOutputColumns())=%v, want %v", n, 4)
		}
		if c, _ := jcast.OutputColumnNamed("col1"); c.ColumnName != "col1" {
			t.Fatalf("parser.JoinClause.OutputColumnNamed()=%v, want %v", c.ColumnName, "col1")
		}
		if c, _ := jcast.OutputColumnNamed("col3"); c.ColumnName != "col3" {
			t.Fatalf("parser.JoinClause.OutputColumnNamed()=%v, want %v", c.ColumnName, "col3")
		}
		if c, _ := jcast.OutputColumnNamed("col99"); c != nil {
			t.Fatalf("parser.JoinClause.OutputColumnNamed()=%v, want %v", c, nil)
		}
		if c, _ := jcast.OutputColumnQualifierNamed("tbl1", "col1"); c.ColumnName != "col1" {
			t.Fatalf("parser.JoinClause.OutputColumnQualifierNamed()=%v, want %v", c.ColumnName, "col1")
		}
		if c, _ := jcast.OutputColumnQualifierNamed("t2", "col3"); c.ColumnName != "col3" {
			t.Fatalf("parser.JoinClause.OutputColumnQualifierNamed()=%v, want %v", c.ColumnName, "col3")
		}
		if c, _ := jcast.OutputColumnQualifierNamed("t1", "col3"); c != nil {
			t.Fatalf("parser.JoinClause.OutputColumnQualifierNamed()=%v, want %v", c, nil)
		}
		if c, _ := jcast.OutputColumnQualifierNamed("t2", "col1"); c != nil {
			t.Fatalf("parser.JoinClause.OutputColumnQualifierNamed()=%v, want %v", c, nil)
		}
		if s := jcast.SourceFromAlias("t1"); s != jcast.X {
			t.Fatalf("parser.JoinClause.SourceFromAlias()=%v, want %v", s, jcast.X)
		}
		if s := jcast.SourceFromAlias("t2"); s != jcast.Y {
			t.Fatalf("parser.JoinClause.SourceFromAlias()=%v, want %v", s, jcast.Y)
		}
		if s := jcast.SourceFromAlias("t3"); s != nil {
			t.Fatalf("parser.JoinClause.SourceFromAlias()=%v, want %v", s, nil)
		}
	}

	// test ParenSource helper functions
	{
		psast := parser.ParenSource{
			X: &parser.QualifiedTableName{
				Name: &parser.Ident{Name: "tbl1"},
				OutputColumns: []*parser.SourceOutputColumn{
					{TableName: "tbl1", ColumnName: "col1", ColumnIndex: 1},
					{TableName: "tbl1", ColumnName: "col2", ColumnIndex: 2},
				},
			},
			Alias: &parser.Ident{Name: "t1"},
		}
		if s := psast.SourceFromAlias("t1"); s.String() != psast.String() {
			t.Fatalf("parser.ParenSource.SourceFromAlias()=%v, want %v", s, psast)
		}
		if s := psast.SourceFromAlias("t3"); s != nil {
			t.Fatalf("parser.ParenSource.SourceFromAlias()=%v, want %v", s, nil)
		}
		if n := len(psast.PossibleOutputColumns()); n != 2 {
			t.Fatalf("len(parser.JoinClause.PossibleOutputColumns())=%v, want %v", n, 2)
		}
		if c, _ := psast.OutputColumnNamed("col1"); c.ColumnName != "col1" {
			t.Fatalf("parser.JoinClause.OutputColumnNamed()=%v, want %v", c.ColumnName, "col1")
		}
		if c, _ := psast.OutputColumnNamed("col99"); c != nil {
			t.Fatalf("parser.JoinClause.OutputColumnNamed()=%v, want %v", c, nil)
		}
		if c, _ := psast.OutputColumnQualifierNamed("t1", "col1"); c.ColumnName != "col1" {
			t.Fatalf("parser.JoinClause.OutputColumnQualifierNamed()=%v, want %v", c.ColumnName, "col1")
		}
		if c, _ := psast.OutputColumnQualifierNamed("t1", "col3"); c != nil {
			t.Fatalf("parser.JoinClause.OutputColumnQualifierNamed()=%v, want %v", c, nil)
		}
	}

	// Test select statement source helper functions
	{
		selectast := &parser.SelectStatement{
			Columns: []*parser.ResultColumn{
				{Expr: &parser.Ident{Name: "col1"}},
				{Expr: &parser.Ident{Name: "col2"}},
			},
			Source: &parser.QualifiedTableName{Name: &parser.Ident{Name: "table"}},
		}

		if s := selectast.SourceFromAlias("table"); s != nil {
			t.Fatalf("parser.SelectStatement.SourceFromAlias()=%v, want %v", s, nil)
		}
		if n := len(selectast.PossibleOutputColumns()); n != 2 {
			t.Fatalf("len(parser.SelectStatement.PossibleOutputColumns())=%v, want %v", n, 2)
		}
		if c, _ := selectast.OutputColumnNamed("col1"); c.ColumnName != "col1" {
			t.Fatalf("parser.SelectStatement.OutputColumnNamed()=%v, want %v", c.ColumnName, "col1")
		}
		if c, _ := selectast.OutputColumnNamed("col99"); c != nil {
			t.Fatalf("parser.SelectStatement.OutputColumnNamed()=%v, want %v", c, nil)
		}
		if c, _ := selectast.OutputColumnQualifierNamed("table", "col1"); c != nil {
			t.Fatalf("parser.SelectStatement.OutputColumnQualifierNamed()=%v, want %v", c, nil)
		}

	}
}

func TestUpdateStatement_String(t *testing.T) {
	AssertStatementStringer(t, &parser.UpdateStatement{
		Table: &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		Assignments: []*parser.Assignment{
			{Columns: []*parser.Ident{{Name: "x"}}, Expr: &parser.IntegerLit{Value: "100"}},
			{Columns: []*parser.Ident{{Name: "y"}}, Expr: &parser.IntegerLit{Value: "200"}},
		},
		WhereExpr: &parser.BoolLit{Value: true},
	}, `UPDATE tbl SET x = 100, y = 200 WHERE TRUE`)

	AssertStatementStringer(t, &parser.UpdateStatement{
		UpdateOrRollback: pos(0),
		Table:            &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		Assignments: []*parser.Assignment{
			{Columns: []*parser.Ident{{Name: "x"}}, Expr: &parser.IntegerLit{Value: "100"}},
		},
	}, `UPDATE OR ROLLBACK tbl SET x = 100`)

	AssertStatementStringer(t, &parser.UpdateStatement{
		UpdateOrAbort: pos(0),
		Table:         &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		Assignments: []*parser.Assignment{
			{Columns: []*parser.Ident{{Name: "x"}}, Expr: &parser.IntegerLit{Value: "100"}},
		},
	}, `UPDATE OR ABORT tbl SET x = 100`)

	AssertStatementStringer(t, &parser.UpdateStatement{
		UpdateOrReplace: pos(0),
		Table:           &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		Assignments: []*parser.Assignment{
			{Columns: []*parser.Ident{{Name: "x"}}, Expr: &parser.IntegerLit{Value: "100"}},
		},
	}, `UPDATE OR REPLACE tbl SET x = 100`)

	AssertStatementStringer(t, &parser.UpdateStatement{
		UpdateOrFail: pos(0),
		Table:        &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		Assignments: []*parser.Assignment{
			{Columns: []*parser.Ident{{Name: "x"}}, Expr: &parser.IntegerLit{Value: "100"}},
		},
	}, `UPDATE OR FAIL tbl SET x = 100`)

	AssertStatementStringer(t, &parser.UpdateStatement{
		UpdateOrIgnore: pos(0),
		Table:          &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
		Assignments: []*parser.Assignment{
			{Columns: []*parser.Ident{{Name: "x"}}, Expr: &parser.IntegerLit{Value: "100"}},
		},
	}, `UPDATE OR IGNORE tbl SET x = 100`)

	// AssertStatementStringer(t, &parser.UpdateStatement{
	// 	WithClause: &parser.WithClause{
	// 		CTEs: []*parser.CTE{{
	// 			TableName: &parser.Ident{Name: "cte"},
	// 			Select: &parser.SelectStatement{
	// 				Columns: []*parser.ResultColumn{{Star: pos(0)}},
	// 			},
	// 			As: parser.Pos{Column: 1},
	// 		}},
	// 	},
	// 	Table: &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
	// 	Assignments: []*parser.Assignment{
	// 		{Columns: []*parser.Ident{{Name: "x"}}, Expr: &parser.IntegerLit{Value: "100"}},
	// 	},
	// }, `WITH cte AS (SELECT *) UPDATE tbl SET x = 100`)

	// Testing UPDATE with WITH clause only upto SQL comparison until parser can handle WITH clauses.
	{
		updateast := parser.UpdateStatement{
			WithClause: &parser.WithClause{
				Recursive: parser.Pos{Column: 1},
				CTEs: []*parser.CTE{{
					TableName: &parser.Ident{Name: "cte1"},
					Select: &parser.SelectStatement{
						Columns: []*parser.ResultColumn{{Star: pos(0)}},
						Source:  &parser.QualifiedTableName{Name: &parser.Ident{Name: "table"}},
					},
					As: parser.Pos{Column: 1},
				},
					{
						TableName: &parser.Ident{Name: "cte2"},
						Select: &parser.SelectStatement{
							Columns: []*parser.ResultColumn{{Star: pos(0)}},
							Source:  &parser.QualifiedTableName{Name: &parser.Ident{Name: "cte1"}},
						},
						As: parser.Pos{Column: 1},
					}},
			},
			Table: &parser.QualifiedTableName{Name: &parser.Ident{Name: "tbl"}},
			Assignments: []*parser.Assignment{
				{Columns: []*parser.Ident{{Name: "x"}}, Expr: &parser.IntegerLit{Value: "100"}},
			},
		}
		updatesql := `WITH RECURSIVE cte1 AS (SELECT * FROM table), cte2 AS (SELECT * FROM cte1) UPDATE tbl SET x = 100`

		if updateast.String() != updatesql {
			t.Fatalf("parser.UpdateStatement.String()=%q, want %q", updateast.String(), updatesql)
		}
		if updateast.Clone().String() != updatesql {
			t.Fatalf("parser.UpdateStatement.Clone().String()=%q, want %q", updateast.Clone().String(), updatesql)
		}
	}
}

func TestIdent_String(t *testing.T) {
	AssertExprStringer(t, &parser.Ident{Name: "foo", Quoted: true}, `"foo"`)
	AssertExprStringer(t, &parser.Ident{Name: "foo \" bar", Quoted: true}, `"foo "" bar"`)
}

func TestStringLit_String(t *testing.T) {
	AssertExprStringer(t, &parser.StringLit{Value: "foo"}, `'foo'`)
	AssertExprStringer(t, &parser.StringLit{Value: "foo ' bar"}, `'foo '' bar'`)
}

func TestNumberLit_String(t *testing.T) {
	AssertExprStringer(t, &parser.IntegerLit{Value: "123.45"}, `123.45`)
}

func TestBoolLit_String(t *testing.T) {
	AssertExprStringer(t, &parser.BoolLit{Value: true}, `TRUE`)
	AssertExprStringer(t, &parser.BoolLit{Value: false}, `FALSE`)
}

func TestNullLit_String(t *testing.T) {
	AssertExprStringer(t, &parser.NullLit{}, `NULL`)
}

// test Date literal type. DateLit.String() will return a quoted string.
func TestDateLit_String(t *testing.T) {
	dl := &parser.DateLit{Value: time.Unix(0, 0).UTC()}
	AssertExprStringer(t, dl, `'1970-01-01T00:00:00Z'`)
}

// test SetLiteralExpr.
func TestSetLiteralExpr_String(t *testing.T) {
	sl := &parser.ArrayLiteralExpr{
		Lbracket: pos(0),
		Rbracket: pos(0),
		Members: []parser.Expr{
			&parser.StringLit{Value: "val1"},
			&parser.StringLit{Value: "val2"},
		},
	}
	AssertExprStringer(t, sl, `['val1', 'val2']`)
}

// test TupleLiteralExpr.
func TestTupleLiteralExpr_String(t *testing.T) {
	sl := &parser.TupleLiteralExpr{
		Lbrace: pos(0),
		Rbrace: pos(0),
		Members: []parser.Expr{
			&parser.StringLit{Value: "val1"},
			&parser.StringLit{Value: "val2"},
		},
	}
	AssertExprStringer(t, sl, `{'val1', 'val2'}`)
}

// Test string literal to timestamp conversion
func TestStringLit_ConvertToTimestamp(t *testing.T) {
	// string value in RFC3339 format
	sl := &parser.StringLit{Value: "2023-03-24T10:06:01Z"}
	AssertExprStringer(t, sl.ConvertToTimestamp(), `'2023-03-24T10:06:01Z'`)

	// string value in RFC3339Nano format
	// DateLit.String() uses time.RFC3339 format, because of that the nano part
	// will be truncated in the string.
	sl = &parser.StringLit{Value: "2023-03-24T10:06:01.100000Z"}
	AssertExprStringer(t, sl.ConvertToTimestamp(), `'2023-03-24T10:06:01Z'`)

	// string value in common date format
	sl = &parser.StringLit{Value: "2023-03-24"}
	AssertExprStringer(t, sl.ConvertToTimestamp(), `'2023-03-24T00:00:00Z'`)

	// string value contains a bad date
	sl = &parser.StringLit{Value: "2023-13-32"}
	dl := sl.ConvertToTimestamp()
	if dl != nil {
		t.Fatalf("StringLit('2023-13-32').ConvertToTimestamp()=%q, want %q", dl.String(), "nil")
	}
}

// test System Variable type.
func TestSysVariable_String(t *testing.T) {
	// test CURRENT_DATE
	sv := &parser.SysVariable{Token: parser.CURRENT_DATE}
	AssertExprStringer(t, sv, parser.CURRENT_DATE.String())
	// test CURRENTTIMESTAMP
	sv = &parser.SysVariable{Token: parser.CURRENT_TIMESTAMP}
	AssertExprStringer(t, sv, parser.CURRENT_TIMESTAMP.String())
	// test CURRENT_TIMESTAMP's data type and expect it to be timestamp type
	if sv.DataType() != parser.NewDataTypeTimestamp() {
		t.Fatalf("SysVariable(CURRENT_TIMESTAMP).DataType()=%q, want %q", sv.DataType().TypeDescription(), parser.NewDataTypeTimestamp().TypeDescription())
	}
	// test CURRENT_TIMESTAMP's name and expect it to be CURRENT_TIMESTAMP
	if sv.Name() != sv.String() {
		t.Fatalf("SysVariable(CURRENT_TIMESTAMP).Name()=%q, want %q", sv.String(), sv.Name())
	}
}

func TestParenExpr_String(t *testing.T) {
	AssertExprStringer(t, &parser.ParenExpr{X: &parser.NullLit{}}, `(NULL)`)
}

func TestUnaryExpr_String(t *testing.T) {
	AssertExprStringer(t, &parser.UnaryExpr{Op: parser.PLUS, X: &parser.IntegerLit{Value: "100"}}, `+100`)
	AssertExprStringer(t, &parser.UnaryExpr{Op: parser.MINUS, X: &parser.IntegerLit{Value: "100"}}, `-100`)
	AssertExprStringer(t, &parser.UnaryExpr{Op: parser.BITNOT, X: &parser.BoolLit{Value: true}}, `!TRUE`)
	AssertNodeStringerPanic(t, &parser.UnaryExpr{X: &parser.IntegerLit{Value: "100"}}, `sql.UnaryExpr.String(): invalid op ILLEGAL`)
}

func TestBinaryExpr_String(t *testing.T) {
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.PLUS, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 + 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.MINUS, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 - 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.STAR, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 * 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.SLASH, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 / 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.REM, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 % 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.CONCAT, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 || 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.BETWEEN, X: &parser.IntegerLit{Value: "1"}, Y: &parser.Range{X: &parser.IntegerLit{Value: "2"}, Y: &parser.IntegerLit{Value: "3"}}}, `1 BETWEEN 2 AND 3`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.NOTBETWEEN, X: &parser.IntegerLit{Value: "1"}, Y: &parser.BinaryExpr{Op: parser.AND, X: &parser.IntegerLit{Value: "2"}, Y: &parser.IntegerLit{Value: "3"}}}, `1 NOT BETWEEN 2 AND 3`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.LSHIFT, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 << 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.RSHIFT, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 >> 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.BITAND, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 & 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.BITOR, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 | 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.LT, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 < 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.LE, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 <= 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.GT, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 > 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.GE, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 >= 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.EQ, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 = 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.NE, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 != 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.IS, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 IS 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.ISNOT, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 IS NOT 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.IN, X: &parser.IntegerLit{Value: "1"}, Y: &parser.ExprList{Exprs: []parser.Expr{&parser.IntegerLit{Value: "2"}}}}, `1 IN (2)`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.NOTIN, X: &parser.IntegerLit{Value: "1"}, Y: &parser.ExprList{Exprs: []parser.Expr{&parser.IntegerLit{Value: "2"}}}}, `1 NOT IN (2)`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.LIKE, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 LIKE 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.NOTLIKE, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 NOT LIKE 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.GLOB, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 GLOB 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.NOTGLOB, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 NOT GLOB 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.MATCH, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 MATCH 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.NOTMATCH, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 NOT MATCH 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.REGEXP, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 REGEXP 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.NOTREGEXP, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 NOT REGEXP 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.AND, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 AND 2`)
	AssertExprStringer(t, &parser.BinaryExpr{Op: parser.OR, X: &parser.IntegerLit{Value: "1"}, Y: &parser.IntegerLit{Value: "2"}}, `1 OR 2`)
	AssertNodeStringerPanic(t, &parser.BinaryExpr{}, `sql.BinaryExpr.String(): invalid op ILLEGAL`)
}

func TestCastExpr_String(t *testing.T) {
	AssertExprStringer(t, &parser.CastExpr{X: &parser.IntegerLit{Value: "1"}, Type: &parser.Type{Name: &parser.Ident{Name: "INTEGER"}}}, `CAST (1 AS INTEGER)`)
}

func TestCaseExpr_String(t *testing.T) {
	AssertExprStringer(t, &parser.CaseExpr{
		Operand: &parser.Ident{Name: "foo"},
		Blocks: []*parser.CaseBlock{
			{Condition: &parser.IntegerLit{Value: "1"}, Body: &parser.BoolLit{Value: true}},
			{Condition: &parser.IntegerLit{Value: "2"}, Body: &parser.BoolLit{Value: false}},
		},
		ElseExpr: &parser.NullLit{},
	}, `CASE foo WHEN 1 THEN TRUE WHEN 2 THEN FALSE ELSE NULL END`)

	AssertExprStringer(t, &parser.CaseExpr{
		Blocks: []*parser.CaseBlock{
			{Condition: &parser.IntegerLit{Value: "1"}, Body: &parser.BoolLit{Value: true}},
		},
	}, `CASE WHEN 1 THEN TRUE END`)
}

func TestExprList_String(t *testing.T) {
	AssertExprStringer(t, &parser.ExprList{Exprs: []parser.Expr{&parser.NullLit{}}}, `(NULL)`)
	AssertExprStringer(t, &parser.ExprList{Exprs: []parser.Expr{&parser.NullLit{}, &parser.NullLit{}}}, `(NULL, NULL)`)
}

func TestQualifiedRef_String(t *testing.T) {
	AssertExprStringer(t, &parser.QualifiedRef{Table: &parser.Ident{Name: "tbl"}, Column: &parser.Ident{Name: "col"}}, `tbl.col`)
	AssertExprStringer(t, &parser.QualifiedRef{Table: &parser.Ident{Name: "tbl"}, Star: pos(0)}, `tbl.*`)
}

func TestCall_String(t *testing.T) {
	AssertExprStringer(t, &parser.Call{Name: &parser.Ident{Name: "foo"}}, `foo()`)
	AssertExprStringer(t, &parser.Call{Name: &parser.Ident{Name: "foo"}, Star: pos(0)}, `foo(*)`)

	AssertExprStringer(t, &parser.Call{
		Name:     &parser.Ident{Name: "foo"},
		Distinct: pos(0),
		Args: []parser.Expr{
			&parser.NullLit{},
			&parser.NullLit{},
		},
	}, `foo(DISTINCT NULL, NULL)`)

	AssertExprStringer(t, &parser.Call{
		Name: &parser.Ident{Name: "foo"},
		Filter: &parser.FilterClause{
			X: &parser.BoolLit{Value: true},
		},
	}, `foo() FILTER (WHERE TRUE)`)

	AssertExprStringer(t, &parser.Call{
		Name: &parser.Ident{Name: "foo"},
		Over: &parser.OverClause{
			Name: &parser.Ident{Name: "win"},
		},
	}, `foo() OVER win`)

	t.Run("FrameSpec", func(t *testing.T) {
		AssertExprStringer(t, &parser.Call{
			Name: &parser.Ident{Name: "foo"},
			Over: &parser.OverClause{
				Definition: &parser.WindowDefinition{
					Frame: &parser.FrameSpec{
						Rows:            pos(0),
						X:               &parser.NullLit{},
						PrecedingX:      pos(0),
						ExcludeNoOthers: pos(0),
					},
				},
			},
		}, `foo() OVER (ROWS NULL PRECEDING EXCLUDE NO OTHERS)`)

		AssertExprStringer(t, &parser.Call{
			Name: &parser.Ident{Name: "foo"},
			Over: &parser.OverClause{
				Definition: &parser.WindowDefinition{
					Frame: &parser.FrameSpec{
						Groups:            pos(0),
						CurrentRowX:       pos(0),
						ExcludeCurrentRow: pos(0),
					},
				},
			},
		}, `foo() OVER (GROUPS CURRENT ROW EXCLUDE CURRENT ROW)`)

		AssertExprStringer(t, &parser.Call{
			Name: &parser.Ident{Name: "foo"},
			Over: &parser.OverClause{
				Definition: &parser.WindowDefinition{
					Frame: &parser.FrameSpec{
						Rows:        pos(0),
						UnboundedX:  pos(0),
						PrecedingX:  pos(0),
						Between:     pos(0),
						CurrentRowY: pos(0),
					},
				},
			},
		}, `foo() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`)

		AssertExprStringer(t, &parser.Call{
			Name: &parser.Ident{Name: "foo"},
			Over: &parser.OverClause{
				Definition: &parser.WindowDefinition{
					Frame: &parser.FrameSpec{
						Rows:        pos(0),
						X:           &parser.NullLit{},
						PrecedingX:  pos(0),
						Between:     pos(0),
						CurrentRowY: pos(0),
					},
				},
			},
		}, `foo() OVER (ROWS BETWEEN NULL PRECEDING AND CURRENT ROW)`)

		AssertExprStringer(t, &parser.Call{
			Name: &parser.Ident{Name: "foo"},
			Over: &parser.OverClause{
				Definition: &parser.WindowDefinition{
					Frame: &parser.FrameSpec{
						Range:        pos(0),
						X:            &parser.NullLit{},
						FollowingX:   pos(0),
						Between:      pos(0),
						Y:            &parser.BoolLit{Value: true},
						PrecedingY:   pos(0),
						ExcludeGroup: pos(0),
					},
				},
			},
		}, `foo() OVER (RANGE BETWEEN NULL FOLLOWING AND TRUE PRECEDING EXCLUDE GROUP)`)

		AssertExprStringer(t, &parser.Call{
			Name: &parser.Ident{Name: "foo"},
			Over: &parser.OverClause{
				Definition: &parser.WindowDefinition{
					Frame: &parser.FrameSpec{
						Range:       pos(0),
						CurrentRowX: pos(0),
						Between:     pos(0),
						Y:           &parser.BoolLit{Value: true},
						FollowingY:  pos(0),
						ExcludeTies: pos(0),
					},
				},
			},
		}, `foo() OVER (RANGE BETWEEN CURRENT ROW AND TRUE FOLLOWING EXCLUDE TIES)`)

		AssertExprStringer(t, &parser.Call{
			Name: &parser.Ident{Name: "foo"},
			Over: &parser.OverClause{
				Definition: &parser.WindowDefinition{
					Frame: &parser.FrameSpec{
						Range:       pos(0),
						CurrentRowX: pos(0),
						Between:     pos(0),
						CurrentRowY: pos(0),
					},
				},
			},
		}, `foo() OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW)`)

		AssertExprStringer(t, &parser.Call{
			Name: &parser.Ident{Name: "foo"},
			Over: &parser.OverClause{
				Definition: &parser.WindowDefinition{
					Frame: &parser.FrameSpec{
						Range:       pos(0),
						CurrentRowX: pos(0),
						Between:     pos(0),
						UnboundedY:  pos(0),
						FollowingY:  pos(0),
					},
				},
			},
		}, `foo() OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)`)
	})
}

func TestExists_String(t *testing.T) {
	AssertExprStringer(t, &parser.Exists{
		Select: &parser.SelectStatement{
			Columns: []*parser.ResultColumn{
				{Star: pos(0)},
			},
		},
	}, `EXISTS (SELECT *)`)

	AssertExprStringer(t, &parser.Exists{
		Not:    pos(0),
		Exists: pos(0),
		Select: &parser.SelectStatement{
			Columns: []*parser.ResultColumn{
				{Star: pos(0)},
			},
		},
	}, `NOT EXISTS (SELECT *)`)
}

func AssertExprStringer(tb testing.TB, expr parser.Expr, s string) {
	tb.Helper()

	if str := expr.String(); str != s {
		tb.Fatalf("String()=%s, expected %s", str, s)
	} else if _, err := parser.NewParser(strings.NewReader(str)).ParseExpr(); err != nil {
		tb.Fatalf("cannot parse string: %s; err=%s", str, err)
	} else {
		AssertExprSanity(tb, expr, s)
	}
}

// AssertExprSanity checks that an expression can be cloned, and that
// the clone's string matches the string we're given. This is a
// kind of round-trip test both for AssertExprStringer and
// AssertParseExpr, although they approach it from different directions.
func AssertExprSanity(tb testing.TB, expr parser.Expr, s string) {
	_, err := parser.Walk(parser.VisitFunc(func(node parser.Node) (parser.Node, error) {
		return node, nil
	}), expr)
	if err != nil {
		tb.Fatalf("walking expression %q: %v", s, err)
	}
	clone := parser.CloneExpr(expr)
	cloneStr := clone.String()
	// We case-smash keywords a lot, and trying to match them all exhaustively sucks.
	// This means this test alone won't catch some hypothetical case smashing
	// errors on provided data, but those should be getting tested directly anyway.
	if !strings.EqualFold(s, cloneStr) {
		tb.Fatalf("expression %q cloned to %q", s, cloneStr)
	}
}

// AssertStatementSanity checks that a statement can be cloned, and that
// the clone's string matches the string we're given. This is a
// kind of round-trip test both for AssertStatementStringer and
// AssertParseStatement, although they approach it from different
// directions. Similarly, this checks that, if a statement has a source,
// SourceList for that source produces at least one source, which
// logically it should. This lets us get coverage of the Source logic
// across a broad variety of statements.
func AssertStatementSanity(tb testing.TB, stmt parser.Statement, s string) {
	_, err := parser.Walk(parser.VisitFunc(func(node parser.Node) (parser.Node, error) {
		return node, nil
	}), stmt)
	if err != nil {
		tb.Fatalf("walking expression %q: %v", s, err)
	}
	clone := parser.CloneStatement(stmt)
	cloneStr := clone.String()
	// We case-smash keywords a lot, and trying to match them all exhaustively sucks.
	// This means this test alone won't catch some hypothetical case smashing
	// errors on provided data, but those should be getting tested directly anyway.
	if !strings.EqualFold(s, cloneStr) {
		tb.Fatalf("statement %q cloned to %q", s, cloneStr)
	}
	src := parser.StatementSource(stmt)
	if src != nil {
		list := parser.SourceList(src)
		if len(list) == 0 {
			tb.Fatalf("statement %q has a source, but that source has a total of 0 sources", s)
		}
	}
}

func AssertStatementStringer(tb testing.TB, stmt parser.Statement, s string) {
	tb.Helper()
	if str := stmt.String(); str != s {
		tb.Fatalf("String()=%s, expected %s", str, s)
	} else if _, err := parser.NewParser(strings.NewReader(str)).ParseStatement(); err != nil {
		tb.Fatalf("cannot parse string: %s; err=%s", str, err)
	} else {
		AssertStatementSanity(tb, stmt, s)
	}
}

func AssertNodeStringerPanic(tb testing.TB, node parser.Node, msg string) {
	tb.Helper()
	var r interface{}
	func() {
		defer func() { r = recover() }()
		_ = node.String()
	}()
	if r == nil {
		tb.Fatal("expected node stringer to panic")
	} else if r != msg {
		tb.Fatalf("recover()=%s, want %s", r, msg)
	}
}

// StripPos removes the position data from a node and its children.
// This function returns the root argument passed in.
func StripPos(root parser.Node) parser.Node {
	zero := reflect.ValueOf(parser.Pos{})

	_, _ = parser.Walk(parser.VisitFunc(func(node parser.Node) (parser.Node, error) {
		value := reflect.Indirect(reflect.ValueOf(node))
		for i := 0; i < value.NumField(); i++ {
			if field := value.Field(i); field.Type() == zero.Type() {
				field.Set(zero)
			}
		}
		return node, nil
	}), root)
	return root
}

func StripExprPos(root parser.Expr) parser.Expr {
	StripPos(root)
	return root
}
