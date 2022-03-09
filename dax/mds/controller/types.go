package controller

import "github.com/molecula/featurebase/v3/dax"

// ComputeNode represents a compute node and the table/shards for which it is
// responsible.
type ComputeNode struct {
	Address dax.Address   `json:"address"`
	Table   dax.TableKey  `json:"table"`
	Shards  dax.ShardNums `json:"shards"`
}

// TranslateNode represents a translate node and the table/partitions for which
// it is responsible.
type TranslateNode struct {
	Address    dax.Address       `json:"address"`
	Table      dax.TableKey      `json:"table"`
	Partitions dax.PartitionNums `json:"partitions"`
}
