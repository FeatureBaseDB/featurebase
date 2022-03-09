package dax

import "fmt"

// PartitionNum is the numerical (int) partition value.
type PartitionNum int

// PartitionNums is a slice of PartitionNum.
type PartitionNums []PartitionNum

// String returns the PartitionNum as a string.
func (p PartitionNum) String() string {
	return fmt.Sprintf("%d", p)
}

// Partition is a versioned partition.
type Partition struct {
	Num     PartitionNum `json:"num"`
	Version int          `json:"version"`
}

// NewPartition returns a Partition with the provided num and version.
func NewPartition(num PartitionNum, version int) Partition {
	return Partition{
		Num:     num,
		Version: version,
	}
}

// String returns the Partition (i.e. its Num and Version) as a string.
func (p Partition) String() string {
	return fmt.Sprintf("%d.%d", p.Num, p.Version)
}

// Partitions is a sortable slice of Partition.
type Partitions []Partition

func (p Partitions) Len() int           { return len(p) }
func (p Partitions) Less(i, j int) bool { return p[i].Num < p[j].Num }
func (p Partitions) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// NewPartitions returns the provided list of partition nums as a list of
// Partition with an invalid version (-1). This is to use for cases where the
// request should not be aware of a partition versioning.
func NewPartitions(partitionNums ...PartitionNum) Partitions {
	pvs := make(Partitions, len(partitionNums))

	for i := range partitionNums {
		pvs[i] = Partition{
			Num:     partitionNums[i],
			Version: -1,
		}
	}

	return pvs
}

// Nums returns a slice of all the partition numbers in Partitions.
func (p Partitions) Nums() []PartitionNum {
	pp := make([]PartitionNum, len(p))
	for i := range p {
		pp[i] = p[i].Num
	}
	return pp
}
