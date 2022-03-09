package dax

import "fmt"

// ShardNum is the numerical (uint64) shard value.
type ShardNum uint64

// ShardNums is a slice of ShardNum.
type ShardNums []ShardNum

func (s ShardNum) String() string {
	return fmt.Sprintf("%d", s)
}

// Shard is a versioned shard.
type Shard struct {
	Num     ShardNum `json:"num"`
	Version int      `json:"version"`
}

// NewShard returns a Shard with the provided num and version.
func NewShard(num ShardNum, version int) Shard {
	return Shard{
		Num:     num,
		Version: version,
	}
}

// String returns the Shard (i.e. its Num and Version) as a string.
func (s Shard) String() string {
	return fmt.Sprintf("%d.%d", s.Num, s.Version)
}

// Shards is a sortable slice of Shard.
type Shards []Shard

func (s Shards) Len() int           { return len(s) }
func (s Shards) Less(i, j int) bool { return s[i].Num < s[j].Num }
func (s Shards) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// NewShards returns the provided list of shard nums as a list of Shard with an
// invalid version (-1). This is to use for cases where the request should not
// be aware of shard versioning.
func NewShards(shardNums ...ShardNum) Shards {
	svs := make(Shards, len(shardNums))

	for i := range shardNums {
		svs[i] = Shard{
			Num:     shardNums[i],
			Version: -1,
		}
	}

	return svs
}

// Nums returns a slice of all the shard numbers in Shards.
func (s Shards) Nums() []ShardNum {
	ss := make([]ShardNum, len(s))
	for i := range s {
		ss[i] = s[i].Num
	}
	return ss
}
