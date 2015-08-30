package storage

import (
	_ "github.com/umbel/pilosa/index/storage/cassandra"
	_ "github.com/umbel/pilosa/index/storage/leveldb"
	_ "github.com/umbel/pilosa/index/storage/mem"
)
