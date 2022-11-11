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
			name: "shardWidthSetTo0",
			SQLs: sqls(
				"create table foo (_id id, i1 int) shardwidth 0",
			),
			ExpErr: "invalid value '0' for shardwidth (should be a number that is a power of 2 and greater or equal to 2^16)",
		},
		{
			name: "shardWidthSetTo11",
			SQLs: sqls(
				"create table foo (_id id, i1 int) shardwidth 11",
			),
			ExpErr: "invalid value '11' for shardwidth (should be a number that is a power of 2 and greater or equal to 2^16)",
		},
		{
			name: "shardWidthSetTo11",
			SQLs: sqls(
				"create table foo (_id id, i1 int) shardwidth 32",
			),
			ExpErr: "invalid value '32' for shardwidth (should be a number that is a power of 2 and greater or equal to 2^16)",
		},
		{
			name: "shardWidthSetTo131072",
			SQLs: sqls(
				"create table foo (_id id, i1 int) shardwidth 131072",
			),
		},
	},
}
