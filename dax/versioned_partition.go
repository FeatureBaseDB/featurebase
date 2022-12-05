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

// VersionedPartition is a partition number along with the snapshot version
// which it is currently writing at.
type VersionedPartition struct {
	Num     PartitionNum `json:"num"`
	Version int          `json:"version"`
}

// NewVersionedPartition returns a VersionedPartition with the provided
// partition number and version.
func NewVersionedPartition(num PartitionNum, version int) VersionedPartition {
	return VersionedPartition{
		Num:     num,
		Version: version,
	}
}

// String returns the VersionedPartition (i.e. its Num and Version) as a string.
func (p VersionedPartition) String() string {
	return fmt.Sprintf("%d.%d", p.Num, p.Version)
}

// VersionedPartitions is a sortable slice of VersionedPartition.
type VersionedPartitions []VersionedPartition

func (p VersionedPartitions) Len() int           { return len(p) }
func (p VersionedPartitions) Less(i, j int) bool { return p[i].Num < p[j].Num }
func (p VersionedPartitions) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// NewVersionedPartitions returns the provided list of partition nums as a list
// of VersionedPartition with an invalid version (-1). This is to use for cases
// where the request should not be aware of a partition versioning.
func NewVersionedPartitions(partitionNums ...PartitionNum) VersionedPartitions {
	pvs := make(VersionedPartitions, len(partitionNums))

	for i := range partitionNums {
		pvs[i] = VersionedPartition{
			Num:     partitionNums[i],
			Version: -1,
		}
	}

	return pvs
}

// Nums returns a slice of all the partition numbers in VersionedPartitions.
func (p VersionedPartitions) Nums() []PartitionNum {
	pp := make([]PartitionNum, len(p))
	for i := range p {
		pp[i] = p[i].Num
	}
	return pp
}
