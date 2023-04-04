package defs

var createTable = TableTest{
	name: "createTable",
	SQLTests: []SQLTest{
		{
			name: "keyPartitionsSetTo0",
			SQLs: sqls(
				"create table foo (_id id, i1 int) keypartitions 0",
			),
			ExpErr: "invalid value '0' for key partitions (should be a number between 1-10000)",
		},
		{
			name: "keyPartitionsSetTo10001",
			SQLs: sqls(
				"create table foo (_id id, i1 int) keypartitions 10001",
			),
			ExpErr: "invalid value '10001' for key partitions (should be a number between 1-10000)",
		},
		{
			name: "commentInt",
			SQLs: sqls(
				"create table foo (_id id, i1 int) comment 34",
			),
			ExpErr: "string literal expected",
		},
		{
			name: "commentStringNoQuote",
			SQLs: sqls(
				"create table foo (_id id, i1 int) comment bad",
			),
			ExpErr: "expected literal, found bad",
		},
		{
			name: "minAboveMax",
			SQLs: sqls(
				"create table bar (_id id, i1 int min 20 max 19)",
			),
			ExpErr: "int field min cannot be greater than max",
		},
		{
			name: "commentString",
			SQLs: sqls(
				"create table bar (_id id, i1 int) comment 'this should work'",
			),
		},
	},
}

var alterTable = TableTest{
	name: "alterTable",
	Table: tbl(
		"alter_table_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a_int", fldTypeInt),
		),
	),
	SQLTests: []SQLTest{
		{
			name: "alterTableBadTable",
			SQLs: sqls(
				"alter table alter_table_test_foo add column a_int int",
			),
			ExpErr: "table 'alter_table_test_foo' not found",
		},
		{
			name: "alterTableAddExistingCol",
			SQLs: sqls(
				"alter table alter_table_test add column a_int int",
			),
			ExpErr: "duplicate column 'a_int'",
		},
		{
			name: "alterTableDropNonExistentCol",
			SQLs: sqls(
				"alter table alter_table_test drop column b_int",
			),
			ExpErr: "column 'b_int' not found",
		},
	},
}
