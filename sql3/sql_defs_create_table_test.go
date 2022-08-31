package sql3_test

var createTable = tableTest{
	name: "createTable",
	sqlTests: []sqlTest{
		{
			name: "keyPartitionsSetTo0",
			sqls: sqls(
				"create table foo (_id id, i1 int) keypartitions 0",
			),
			expErr: "invalid value '0' for key partitions (should be a number between 1-10000)",
		},
		{
			name: "keyPartitionsSetTo10001",
			sqls: sqls(
				"create table foo (_id id, i1 int) keypartitions 10001",
			),
			expErr: "invalid value '10001' for key partitions (should be a number between 1-10000)",
		},
		{
			name: "shardWidthSetTo0",
			sqls: sqls(
				"create table foo (_id id, i1 int) shardwidth 0",
			),
			expErr: "invalid value '0' for shardwidth (should be a number that is a power of 2 and greater or equal to 2^16)",
		},
		{
			name: "shardWidthSetTo11",
			sqls: sqls(
				"create table foo (_id id, i1 int) shardwidth 11",
			),
			expErr: "invalid value '11' for shardwidth (should be a number that is a power of 2 and greater or equal to 2^16)",
		},
		{
			name: "shardWidthSetTo11",
			sqls: sqls(
				"create table foo (_id id, i1 int) shardwidth 32",
			),
			expErr: "invalid value '32' for shardwidth (should be a number that is a power of 2 and greater or equal to 2^16)",
		},
		{
			name: "shardWidthSetTo131072",
			sqls: sqls(
				"create table foo (_id id, i1 int) shardwidth 131072",
			),
		},
	},
}
