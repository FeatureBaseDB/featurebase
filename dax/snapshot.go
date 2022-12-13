package dax

type SnapshotShardDataRequest struct {
	Address Address `json:"address"`

	TableKey TableKey `json:"table-key"`
	ShardNum ShardNum `json:"shard"`
}

type SnapshotTableKeysRequest struct {
	Address Address `json:"address"`

	TableKey     TableKey     `json:"table-key"`
	PartitionNum PartitionNum `json:"partition"`
	FromVersion  int          `json:"from-version"`
	ToVersion    int          `json:"to-version"`

	Directive Directive `json:"directive"`
}

type SnapshotFieldKeysRequest struct {
	Address Address `json:"address"`

	TableKey    TableKey  `json:"table-key"`
	Field       FieldName `json:"field"`
	FromVersion int       `json:"from-version"`
	ToVersion   int       `json:"to-version"`

	Directive Directive `json:"directive"`
}
