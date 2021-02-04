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
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	pnet "github.com/pilosa/pilosa/v2/net"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/testhook"
	"github.com/pilosa/pilosa/v2/topology"
	"github.com/pkg/errors"
)

// utilities used by tests

// mustAddR is a helper for calling roaring.Container.Add() in tests to
// keep the linter happy that we are checking the error.
func mustAddR(changed bool, err error) {
	panicOn(err)
}

// mustRemove is a helper for calling Tx.Remove() in tests to
// keep the linter happy that we are checking the error.
func mustRemove(changeCount int, err error) {
	panicOn(err)
}

func getTestBitmapAsRawRoaring(bitsToSet ...uint64) []byte {
	b := roaring.NewBitmap()
	changed := b.DirectAddN(bitsToSet...)
	n := len(bitsToSet)
	if changed != n {
		panic(fmt.Sprintf("changed=%v but bitsToSet len = %v", changed, n))
	}
	buf := bytes.NewBuffer(make([]byte, 0, 100000))
	_, err := b.WriteTo(buf)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// NewTestCluster returns a cluster with n nodes and uses a mod-based hasher.
func NewTestCluster(tb testing.TB, n int) *cluster {
	path, err := testhook.TempDir(tb, "pilosa-cluster-")
	if err != nil {
		panic(err)
	}

	availableShardFileFlushDuration.Set(100 * time.Millisecond)
	c := newCluster()
	c.ReplicaN = 1
	c.Hasher = NewTestModHasher()
	c.Path = path
	c.Topology = NewTopology(c.Hasher, c.partitionN, c.ReplicaN, c)

	for i := 0; i < n; i++ {
		c.noder.AppendNode(&topology.Node{
			ID:  fmt.Sprintf("node%d", i),
			URI: NewTestURI("http", fmt.Sprintf("host%d", i), uint16(0)),
		})
	}

	cNodes := c.noder.Nodes()

	c.Node = cNodes[0]
	return c
}

// NewTestURI is a test URI creator that intentionally swallows errors.
func NewTestURI(scheme, host string, port uint16) pnet.URI {
	uri := pnet.DefaultURI()
	_ = uri.SetScheme(scheme)
	_ = uri.SetHost(host)
	uri.SetPort(port)
	return *uri
}

func NewTestURIFromHostPort(host string, port uint16) pnet.URI {
	uri := pnet.DefaultURI()
	_ = uri.SetHost(host)
	uri.SetPort(port)
	return *uri
}

// ModHasher represents a simple, mod-based hashing.
type TestModHasher struct{}

// NewTestModHasher returns a new instance of ModHasher with n buckets.
func NewTestModHasher() *TestModHasher { return &TestModHasher{} }

func (*TestModHasher) Hash(key uint64, n int) int { return int(key) % n }

func (*TestModHasher) Name() string { return "mod" }

// ClusterCluster represents a cluster of test nodes, each of which
// has a Cluster.
// ClusterCluster implements Broadcaster interface.
type ClusterCluster struct {
	Clusters []*cluster

	common *commonClusterSettings

	mu         sync.RWMutex
	resizing   bool
	resizeDone chan struct{}
	tb         testing.TB
}

type commonClusterSettings struct {
	Nodes []*topology.Node
}

func (t *ClusterCluster) CreateIndex(name string) error {
	for _, c := range t.Clusters {
		if _, err := c.holder.CreateIndexIfNotExists(name, IndexOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (t *ClusterCluster) CreateIndexWithOpt(name string, opt IndexOptions) error {
	for _, c := range t.Clusters {
		if _, err := c.holder.CreateIndexIfNotExists(name, opt); err != nil {
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

		if err := func() error {
			idx := c.holder.Index(f.index)
			shard := colID / ShardWidth
			tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard})
			if tx != nil {
				defer tx.Rollback()
			}

			if _, err := f.SetBit(tx, rowID, colID, x); err != nil {
				return err
			} else if err := tx.Commit(); err != nil {
				return err
			}
			return nil
		}(); err != nil {
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

		state, err := coord.State()
		if err != nil {
			return err
		}

		// Wait for the AddNode job to finish.
		if state != string(ClusterStateNormal) {
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

	node := &topology.Node{
		ID:  id,
		URI: uri,
	}

	// add URI to common
	//t.common.NodeIDs = append(t.common.NodeIDs, id)
	//sort.Sort(t.common.NodeIDs)

	// add node to common
	t.common.Nodes = append(t.common.Nodes, node)

	// create node-specific temp directory
	path, err := testhook.TempDirInDir(t.tb, *TempDir, fmt.Sprintf("pilosa-cluster-node-%d-", i))
	if err != nil {
		return nil, err
	}

	// holder
	h := NewHolder(path, nil)

	// cluster
	c := newCluster()
	c.ReplicaN = 1
	c.Hasher = NewTestModHasher()
	c.Path = path
	c.partitionN = topology.DefaultPartitionN
	c.Topology = NewTopology(c.Hasher, c.partitionN, c.ReplicaN, c)
	c.holder = h
	c.Node = node
	// c.Coordinator = t.common.Nodes[0].ID // the first node is the coordinator
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

	return c, nil
}

// NewClusterCluster returns a new instance of test.Cluster.
func NewClusterCluster(tb testing.TB, n int) *ClusterCluster {

	tc := &ClusterCluster{
		common: &commonClusterSettings{},
		tb:     tb,
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

// Open opens all clusters in the test cluster.
func (t *ClusterCluster) Open() error {
	for _, c := range t.Clusters {
		if err := c.open(); err != nil {
			return err
		}
		if err := c.holder.Open(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all clusters in the test cluster.
func (t *ClusterCluster) Close() error {
	for _, c := range t.Clusters {
		err := c.close()
		if err != nil {
			return err
		}
		// Make sure open indexes get shut down too. we wouldn't do
		// this normally for a cluster, but we want to for test cases.
		c.holder.Close()
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
		if obj.State == string(ClusterStateNormal) && b.t.resizing {
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
func (b bcast) SendTo(to *topology.Node, m Message) error {
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
		if obj.State == string(ClusterStateNormal) && b.t.resizing {
			close(b.t.resizeDone)
		}
		b.t.mu.RUnlock()
	default:
		panic(fmt.Sprintf("message not handled:\n%#v\n", obj))
	}
	return nil
}

// FollowResizeInstruction is a version of cluster.followResizeInstruction used for testing.
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
		for k, is := range instr.NodeStatus.Indexes {
			_ = k
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

			// this is the *test* version of a network call, transferring fragments between
			// nodes in a cluster. So it is allowed to be kind of a hack.

			// there will be two -rbfdb directories/databases, we need to copy
			// from src to dest the fragment. This simulates sending the fragment over the network.
			srcIdx := srcCluster.holder.Index(src.Index)
			srctx := srcIdx.holder.txf.NewTx(Txo{Write: !writable, Index: srcIdx, Fragment: srcFragment, Shard: srcFragment.shard})

			destIdx := destCluster.holder.Index(src.Index)

			desttx := destIdx.holder.txf.NewTx(Txo{Write: writable, Index: destIdx, Fragment: destFragment, Shard: destFragment.shard})

			citer, _, err := srctx.ContainerIterator(src.Index, src.Field, src.View, src.Shard, 0)
			panicOn(err)
			d := destFragment
			for citer.Next() {
				ckey, c := citer.Value()
				err := desttx.PutContainer(d.index(), d.field(), d.view(), d.shard, ckey, c)
				panicOn(err)
			}
			citer.Close()
			panicOn(desttx.Commit())
			srctx.Rollback()
		}

		return nil
	}(); err != nil {
		complete.Error = err.Error()
	}

	node := instr.Primary
	return bcast{t: t}.SendTo(node, complete)
}

var _ = NewTestClusterWithReplication // happy linter

func NewTestClusterWithReplication(tb testing.TB, nNodes, nReplicas, partitionN int) (c *cluster, cleaner func()) {
	path, err := testhook.TempDir(tb, "pilosa-cluster-")
	if err != nil {
		panic(err)
	}

	// holder
	h := NewHolder(path, nil)

	// cluster
	availableShardFileFlushDuration.Set(100 * time.Millisecond)
	c = newCluster()
	c.holder = h
	c.ReplicaN = nReplicas
	c.Hasher = &topology.Jmphasher{}
	c.Path = path
	c.partitionN = partitionN
	c.Topology = NewTopology(c.Hasher, c.partitionN, c.ReplicaN, c)

	for i := 0; i < nNodes; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		c.noder.AppendNode(&topology.Node{
			ID:  nodeID,
			URI: NewTestURI("http", fmt.Sprintf("host%d", i), uint16(0)),
		})
		c.Topology.addID(nodeID)
	}

	cNodes := c.noder.Nodes()

	c.Node = cNodes[0]

	if err := c.holder.Open(); err != nil {
		panic(err)
	}

	return c, func() {
		c.holder.Close()
		c.close()
	}
}
