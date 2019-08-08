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

package pilosa

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

const MOD = "mod"

// NewTestCluster returns a cluster with n nodes and uses a mod-based hasher.
func NewTestCluster(n int) *cluster {
	path, err := ioutil.TempDir("", "pilosa-cluster-")
	if err != nil {
		panic(err)
	}

	c := newCluster()
	c.ReplicaN = 1
	c.shardDistributors = map[string]ShardDistributor{MOD: NewTestModDistributor(defaultPartitionN)}
	c.defaultShardDistributor = MOD
	c.Path = path
	c.Topology = newTopology()

	for i := 0; i < n; i++ {
		c.nodes = append(c.nodes, &Node{
			ID:  fmt.Sprintf("node%d", i),
			URI: NewTestURI("http", fmt.Sprintf("host%d", i), uint16(0)),
		})
	}

	c.Node = c.nodes[0]
	c.Coordinator = c.nodes[0].ID
	c.SetState(ClusterStateNormal)

	return c
}

// NewTestURI is a test URI creator that intentionally swallows errors.
func NewTestURI(scheme, host string, port uint16) URI {
	uri := defaultURI()
	_ = uri.setScheme(scheme)
	_ = uri.setHost(host)
	uri.SetPort(port)
	return *uri
}

func NewTestURIFromHostPort(host string, port uint16) URI {
	uri := defaultURI()
	_ = uri.setHost(host)
	uri.SetPort(port)
	return *uri
}

// ModDistributor represents a simple, mod-based shard distributor.
type TestModDistributor struct {
	partitionN int
}

// NewTestModDistributor returns a new instance of ModDistributor.
func NewTestModDistributor(partitionN int) *TestModDistributor {
	return &TestModDistributor{partitionN: partitionN}
}

// NodeOwners satisfies the ShardDistributor interface.
func (d *TestModDistributor) NodeOwners(nodeIDs []string, replicaN int, index string, shard uint64) []string {
	idx := int(shard % uint64(d.partitionN))
	owners := make([]string, 0, replicaN)
	for i := 0; i < replicaN; i++ {
		owners = append(owners, nodeIDs[(idx+i)%len(nodeIDs)])
	}
	return owners
}

// AddNode is a nop and only exists to satisfy the `ShardDistributor` interface.
func (d *TestModDistributor) AddNode(nodeID string) error { return nil }

// RemoveNode is a nop and only exists to satisfy the `ShardDistributor` interface.
func (d *TestModDistributor) RemoveNode(nodeID string) error { return nil }

// ClusterCluster represents a cluster of test nodes, each of which
// has a Cluster.
// ClusterCluster implements Broadcaster interface.
type ClusterCluster struct {
	Clusters []*cluster

	common *commonClusterSettings

	mu         sync.RWMutex
	resizing   bool
	resizeDone chan struct{}
}

type commonClusterSettings struct {
	Nodes []*Node
}

func (t *ClusterCluster) CreateIndex(name string) error {
	for _, c := range t.Clusters {
		if _, err := c.holder.CreateIndexIfNotExists(name, IndexOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (t *ClusterCluster) CreateField(index, field string, opts FieldOption) error {
	for _, c := range t.Clusters {
		idx, err := c.holder.CreateIndexIfNotExists(index, IndexOptions{})
		if err != nil {
			return err
		}
		if _, err := idx.CreateField(field, opts); err != nil {
			return err
		}
	}
	return nil
}

func (t *ClusterCluster) SetBit(index, field string, rowID, colID uint64, x *time.Time) error {
	// Determine which node should receive the SetBit.
	c0 := t.Clusters[0] // use the first node's cluster to determine shard location.
	shard := colID / ShardWidth
	nodes := c0.shardNodes(index, shard)

	for _, node := range nodes {
		c := t.clusterByID(node.ID)
		if c == nil {
			continue
		}
		f := c.holder.Field(index, field)
		if f == nil {
			return fmt.Errorf("index/field does not exist: %s/%s", index, field)
		}
		_, err := f.SetBit(rowID, colID, x)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *ClusterCluster) clusterByID(id string) *cluster {
	for _, c := range t.Clusters {
		if c.Node.ID == id {
			return c
		}
	}
	return nil
}

// addNode adds a node to the cluster and (potentially) starts a resize job.
func (t *ClusterCluster) addNode() error {
	id := len(t.Clusters)

	c, err := t.addCluster(id, false)
	if err != nil {
		return err
	}

	// Send NodeJoin event to coordinator.
	if id > 0 {
		coord := t.Clusters[0]
		ev := &NodeEvent{
			Event: NodeJoin,
			Node:  c.Node,
		}

		if err := coord.ReceiveEvent(ev); err != nil {
			return err
		}

		// Wait for the AddNode job to finish.
		if c.State() != ClusterStateNormal {
			t.resizeDone = make(chan struct{})
			t.mu.Lock()
			t.resizing = true
			t.mu.Unlock()
			<-t.resizeDone
		}
	}

	return nil
}

// WriteTopology writes the given topology to disk.
func (t *ClusterCluster) WriteTopology(path string, top *Topology) error {
	if buf, err := proto.Marshal(top.encode()); err != nil {
		return err
	} else if err := ioutil.WriteFile(filepath.Join(path, ".topology"), buf, 0666); err != nil {
		return err
	}
	return nil
}

func (t *ClusterCluster) addCluster(i int, saveTopology bool) (*cluster, error) {

	id := fmt.Sprintf("node%d", i)
	uri := NewTestURI("http", fmt.Sprintf("host%d", i), uint16(0))

	node := &Node{
		ID:  id,
		URI: uri,
	}

	// add URI to common
	//t.common.NodeIDs = append(t.common.NodeIDs, id)
	//sort.Sort(t.common.NodeIDs)

	// add node to common
	t.common.Nodes = append(t.common.Nodes, node)

	// create node-specific temp directory
	path, err := ioutil.TempDir(*TempDir, fmt.Sprintf("pilosa-cluster-node-%d-", i))
	if err != nil {
		return nil, err
	}

	// holder
	h := NewHolder()
	h.Path = path
	h.defaultShardDistributor = MOD

	// cluster
	c := newCluster()
	c.ReplicaN = 1
	c.shardDistributors = map[string]ShardDistributor{MOD: NewTestModDistributor(defaultPartitionN)}
	c.defaultShardDistributor = MOD
	c.Path = path
	c.Topology = newTopology()
	c.holder = h
	c.Node = node
	c.Coordinator = t.common.Nodes[0].ID // the first node is the coordinator
	c.broadcaster = t.broadcaster(c)

	// add nodes
	if saveTopology {
		for _, n := range t.common.Nodes {
			if err := c.addNode(n); err != nil {
				return nil, err
			}
		}
	}

	// Add this node to the ClusterCluster.
	t.Clusters = append(t.Clusters, c)

	// Update modulus of modhasher on cluster addition.
	for _, c := range t.Clusters {
		c.shardDistributors[MOD] = NewTestModDistributor(len(t.Clusters))
	}

	return c, nil
}

// NewClusterCluster returns a new instance of test.Cluster.
func NewClusterCluster(n int) *ClusterCluster {

	tc := &ClusterCluster{
		common: &commonClusterSettings{},
	}

	// add clusters
	for i := 0; i < n; i++ {
		_, err := tc.addCluster(i, true)
		if err != nil {
			panic(err)
		}
	}
	return tc
}

// SetState sets the state of the cluster on each node.
func (t *ClusterCluster) SetState(state string) {
	for _, c := range t.Clusters {
		c.SetState(state)
	}
}

// Open opens all clusters in the test cluster.
func (t *ClusterCluster) Open() error {
	for _, c := range t.Clusters {
		if err := c.open(); err != nil {
			return err
		}
		if err := c.holder.Open(); err != nil {
			return err
		}
		if err := c.setNodeState(nodeStateReady); err != nil {
			return err
		}
	}

	// Start the listener on the coordinator.
	if len(t.Clusters) == 0 {
		return nil
	}
	t.Clusters[0].listenForJoins()

	return nil
}

// Close closes all clusters in the test cluster.
func (t *ClusterCluster) Close() error {
	for _, c := range t.Clusters {
		err := c.close()
		if err != nil {
			return err
		}
	}
	return nil
}

type bcast struct {
	t *ClusterCluster
	c *cluster
}

func (b bcast) SendSync(m Message) error {
	switch obj := m.(type) {
	case *ClusterStatus:
		// Apply the send message to all nodes (except the coordinator).
		for _, c := range b.t.Clusters {
			if c != b.c {
				err := c.mergeClusterStatus(obj)
				if err != nil {
					return err
				}
			}
		}
		b.t.mu.RLock()
		if obj.State == ClusterStateNormal && b.t.resizing {
			close(b.t.resizeDone)
		}
		b.t.mu.RUnlock()
	}
	return nil
}

func (t *ClusterCluster) broadcaster(c *cluster) broadcaster {
	return bcast{
		t: t,
		c: c,
	}
}

// SendAsync is a test implemenetation of Broadcaster SendAsync method.
func (bcast) SendAsync(Message) error {
	return nil
}

// SendTo is a test implementation of Broadcaster SendTo method.
func (b bcast) SendTo(to *Node, m Message) error {
	switch obj := m.(type) {
	case *ResizeInstruction:
		err := b.t.FollowResizeInstruction(obj)
		if err != nil {
			return err
		}
	case *ResizeInstructionComplete:
		coord := b.t.clusterByID(to.ID)
		// this used to be async, but that prevented us from checking
		// its error status...
		return coord.markResizeInstructionComplete(obj)
	case *ClusterStatus:
		// Apply the send message to the node.
		for _, c := range b.t.Clusters {
			if c.Node.ID == to.ID {
				err := c.mergeClusterStatus(obj)
				if err != nil {
					return err
				}
			}
		}
		b.t.mu.RLock()
		if obj.State == ClusterStateNormal && b.t.resizing {
			close(b.t.resizeDone)
		}
		b.t.mu.RUnlock()
	default:
		panic(fmt.Sprintf("message not handled:\n%#v\n", obj))
	}
	return nil
}

// FollowResizeInstruction is a version of cluster.FollowResizeInstruction used for testing.
func (t *ClusterCluster) FollowResizeInstruction(instr *ResizeInstruction) error {

	// Prepare the return message.
	complete := &ResizeInstructionComplete{
		JobID: instr.JobID,
		Node:  instr.Node,
		Error: "",
	}

	// Stop processing on any error.
	if err := func() error {

		// figure out which node it was meant for, then call the operation on that cluster
		// basically need to mimic this: client.RetrieveShardFromURI(context.Background(), src.Index, src.Field, src.View, src.Shard, srcURI)
		instrNode := instr.Node
		destCluster := t.clusterByID(instrNode.ID)

		// Sync the schema received in the resize instruction.
		if err := destCluster.holder.applySchema(instr.NodeStatus.Schema); err != nil {
			return err
		}

		// Sync available shards.
		for _, is := range instr.NodeStatus.Indexes {
			for _, fs := range is.Fields {
				f := destCluster.holder.Field(is.Name, fs.Name)

				// if we don't know about a field locally, log an error because
				// fields should be created and synced prior to shard creation
				if f == nil {
					continue
				}
				if err := f.AddRemoteAvailableShards(fs.AvailableShards); err != nil {
					return errors.Wrap(err, "adding remote available shards")
				}
			}
		}

		for _, src := range instr.Sources {
			srcCluster := t.clusterByID(src.Node.ID)

			srcFragment := srcCluster.holder.fragment(src.Index, src.Field, src.View, src.Shard)
			destFragment := destCluster.holder.fragment(src.Index, src.Field, src.View, src.Shard)
			if destFragment == nil {
				// Create fragment on destination if it doesn't exist.
				f := destCluster.holder.Field(src.Index, src.Field)
				v := f.view(src.View)
				var err error
				destFragment, err = v.CreateFragmentIfNotExists(src.Shard)
				if err != nil {
					return err
				}
			}

			buf := bytes.NewBuffer(nil)

			bw := bufio.NewWriter(buf)
			br := bufio.NewReader(buf)

			// Get the fragment from source.
			if _, err := srcFragment.WriteTo(bw); err != nil {
				return err
			}

			// Flush the bufio.buf to the io.Writer (buf).
			bw.Flush()

			// Write data to destination.
			if _, err := destFragment.ReadFrom(br); err != nil {
				return err
			}
		}

		return nil
	}(); err != nil {
		complete.Error = err.Error()
	}

	node := instr.Coordinator
	return bcast{t: t}.SendTo(node, complete)
}
