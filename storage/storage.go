package storage

import (
	_ "github.com/umbel/pilosa/storage/cassandra"
	_ "github.com/umbel/pilosa/storage/leveldb"
	_ "github.com/umbel/pilosa/storage/mem"
)
