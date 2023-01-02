package dax

import "fmt"

// PartitionNum is the numerical (int) partition value.
type PartitionNum int

// PartitionNums is a slice of PartitionNum.
type PartitionNums []PartitionNum

func (p PartitionNums) Len() int           { return len(p) }
func (p PartitionNums) Less(i, j int) bool { return p[i] < p[j] }
func (p PartitionNums) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// String returns the PartitionNum as a string.
func (p PartitionNum) String() string {
	return fmt.Sprintf("%d", p)
}

func NewPartitionNums(nums ...uint64) PartitionNums {
	partitions := make(PartitionNums, len(nums))
	for i, n := range nums {
		partitions[i] = PartitionNum(n)
	}
	return partitions
}
