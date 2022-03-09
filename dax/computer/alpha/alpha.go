package alpha

import (
	"fmt"
	"path"

	"github.com/molecula/featurebase/v3/dax"
)

const (
	keysFileName = "keys"
)

func partitionBucket(table dax.TableKey, partition dax.PartitionNum) string {
	return path.Join(string(table), "partition", fmt.Sprintf("%d", partition))
}

func shardKey(shard dax.ShardNum) string {
	return path.Join("shard", fmt.Sprintf("%d", shard))
}

func fieldBucket(table dax.TableKey, field dax.FieldName) string {
	return path.Join(string(table), "field", string(field))
}
