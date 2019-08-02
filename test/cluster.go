// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

// ModDistributor represents a simple, mod-based shard distributor.
type ModDistributor struct {
	partitionN int
}

// NewModDistributor returns a new instance of ModDistributor.
func NewModDistributor(partitionN int) *ModDistributor {
	return &ModDistributor{partitionN: partitionN}
}

// NodeOwners satisfies the ShardDistributor interface.
func (d *ModDistributor) NodeOwners(nodeIDs []string, replicaN int, index string, shard uint64) []string {
	idx := int(shard % uint64(d.partitionN))
	owners := make([]string, 0, replicaN)
	for i := 0; i < replicaN; i++ {
		owners = append(owners, nodeIDs[(idx+i)%len(nodeIDs)])
	}
	return owners
}

// AddNode is a nop and only exists to satisfy the `ShardDistributor` interface.
func (d *ModDistributor) AddNode(nodeID string) error { return nil }

// RemoveNode is a nop and only exists to satisfy the `ShardDistributor` interface.
func (d *ModDistributor) RemoveNode(nodeID string) error { return nil }
