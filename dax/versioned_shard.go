package dax

import "fmt"

// ShardNum is the numerical (uint64) shard value.
type ShardNum uint64

// ShardNums is a slice of ShardNum.
type ShardNums []ShardNum

func (s ShardNum) String() string {
	return fmt.Sprintf("%d", s)
}

func (s ShardNums) Len() int           { return len(s) }
func (s ShardNums) Less(i, j int) bool { return s[i] < s[j] }
func (s ShardNums) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func NewShardNums(nums ...uint64) ShardNums {
	shards := make(ShardNums, len(nums))
	for i, n := range nums {
		shards[i] = ShardNum(n)
	}
	return shards
}
