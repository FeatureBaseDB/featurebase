package dax

import "fmt"

// ShardNum is the numerical (uint64) shard value.
type ShardNum uint64

// ShardNums is a slice of ShardNum.
type ShardNums []ShardNum

func (s ShardNum) String() string {
	return fmt.Sprintf("%d", s)
}

// VersionedShard is a shard number along with the snapshot version which it is
// currently writing at.
type VersionedShard struct {
	Num     ShardNum `json:"num"`
	Version int      `json:"version"`
}

// NewVersionedShard returns a VersionedShard with the provided shard number and version.
func NewVersionedShard(num ShardNum, version int) VersionedShard {
	return VersionedShard{
		Num:     num,
		Version: version,
	}
}

// String returns the VersionedShard (i.e. its Num and Version) as a string.
func (s VersionedShard) String() string {
	return fmt.Sprintf("%d.%d", s.Num, s.Version)
}

// VersionedShards is a sortable slice of VersionedShard.
type VersionedShards []VersionedShard

func (s VersionedShards) Len() int           { return len(s) }
func (s VersionedShards) Less(i, j int) bool { return s[i].Num < s[j].Num }
func (s VersionedShards) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// NewVersionedShards returns the provided list of shard nums as a list of
// VersionedShard with an invalid version (-1). This is to use for cases where
// the request should not be aware of shard versioning.
func NewVersionedShards(shardNums ...ShardNum) VersionedShards {
	svs := make(VersionedShards, len(shardNums))

	for i := range shardNums {
		svs[i] = VersionedShard{
			Num:     shardNums[i],
			Version: -1,
		}
	}

	return svs
}

// Nums returns a slice of all the shard numbers in VersionedShards.
func (s VersionedShards) Nums() []ShardNum {
	ss := make([]ShardNum, len(s))
	for i := range s {
		ss[i] = s[i].Num
	}
	return ss
}
