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
	"github.com/pilosa/pilosa/internal"
)

// NewTestCluster returns a cluster with n nodes and uses a mod-based hasher.
func NewTestCluster(n int) *Cluster {
	path, err := ioutil.TempDir("", "pilosa-cluster-")
	if err != nil {
		panic(err)
	}

	c := NewCluster()
	c.ReplicaN = 1
	c.Hasher = NewTestModHasher()
	c.Path = path
	c.Topology = NewTopology()

	for i := 0; i < n; i++ {
		c.Nodes = append(c.Nodes, &Node{
			ID:  fmt.Sprintf("node%d", i),
			URI: NewTestURI("http", fmt.Sprintf("host%d", i), uint16(0)),
		})
	}

	c.Node = c.Nodes[0]
	c.Coordinator = c.Nodes[0].ID
	c.SetState(ClusterStateNormal)

	return c
}

// NewTestURI is a test URI creator that intentionally swallows errors.
func NewTestURI(scheme, host string, port uint16) URI {
	uri := DefaultURI()
	uri.SetScheme(scheme)
	uri.SetHost(host)
	uri.SetPort(port)
	return *uri
}

func NewTestURIFromHostPort(host string, port uint16) URI {
	uri := DefaultURI()
	uri.SetHost(host)
	uri.SetPort(port)
	return *uri
}

// ModHasher represents a simple, mod-based hashing.
type TestModHasher struct{}

// NewTestModHasher returns a new instance of ModHasher with n buckets.
func NewTestModHasher() *TestModHasher { return &TestModHasher{} }

func (*TestModHasher) Hash(key uint64, n int) int { return int(key) % n }

// ClusterCluster represents a cluster of test nodes, each of which
// has a Cluster.
// ClusterCluster implements Broadcaster interface.
type ClusterCluster struct {
	Clusters []*Cluster

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
		if _, err := c.Holder.CreateIndexIfNotExists(name, IndexOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (t *ClusterCluster) CreateFrame(index, frame string, opt FrameOptions) error {
	for _, c := range t.Clusters {
		idx, err := c.Holder.CreateIndexIfNotExists(index, IndexOptions{})
		if err != nil {
			return err
		}
		if _, err := idx.CreateFrame(frame, opt); err != nil {
			return err
		}
	}
	return nil
}

func (t *ClusterCluster) SetBit(index, frame, view string, rowID, colID uint64, x *time.Time) error {
	// Determine which node should receive the SetBit.
	c0 := t.Clusters[0] // use the first node's cluster to determine slice location.
	slice := colID / SliceWidth
	nodes := c0.SliceNodes(index, slice)

	for _, node := range nodes {
		c := t.clusterByID(node.ID)
		if c == nil {
			continue
		}
		f := c.Holder.Frame(index, frame)
		if f == nil {
			return fmt.Errorf("index/frame does not exist: %s/%s", index, frame)
		}
		_, err := f.SetBit(view, rowID, colID, x)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *ClusterCluster) SetFieldValue(index, frame string, columnID uint64, name string, value int64) error {
	// Determine which node should receive the SetFieldValue.
	c0 := t.Clusters[0] // use the first node's cluster to determine slice location.
	slice := columnID / SliceWidth
	nodes := c0.SliceNodes(index, slice)

	for _, node := range nodes {
		c := t.clusterByID(node.ID)
		if c == nil {
			continue
		}
		f := c.Holder.Frame(index, frame)
		if f == nil {
			return fmt.Errorf("index/frame does not exist: %s/%s", index, frame)
		}
		_, err := f.SetFieldValue(columnID, name, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *ClusterCluster) clusterByID(id string) *Cluster {
	for _, c := range t.Clusters {
		if c.Node.ID == id {
			return c
		}
	}
	return nil
}

// AddNode adds a node to the cluster and (potentially) starts a resize job.
func (t *ClusterCluster) AddNode(saveTopology bool) error {
	id := len(t.Clusters)

	c, err := t.addCluster(id, saveTopology)
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
	if buf, err := proto.Marshal(top.Encode()); err != nil {
		return err
	} else if err := ioutil.WriteFile(filepath.Join(path, ".topology"), buf, 0666); err != nil {
		return err
	}
	return nil
}

func (t *ClusterCluster) addCluster(i int, saveTopology bool) (*Cluster, error) {

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
	path, err := ioutil.TempDir("", fmt.Sprintf("pilosa-cluster-node-%d-", i))
	if err != nil {
		return nil, err
	}

	// holder
	h := NewHolder()
	h.Path = path

	// cluster
	c := NewCluster()
	c.ReplicaN = 1
	c.Hasher = NewTestModHasher()
	c.Path = path
	c.Topology = NewTopology()
	c.Holder = h
	c.MemberSet = NewStaticMemberSet(c.Nodes)
	c.Node = node
	c.Coordinator = t.common.Nodes[0].ID // the first node is the coordinator
	c.Broadcaster = t

	// add nodes
	if saveTopology {
		for _, n := range t.common.Nodes {
			c.AddNode(n)
		}
	}

	// Add this node to the ClusterCluster.
	t.Clusters = append(t.Clusters, c)

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
		if err := c.Open(); err != nil {
			return err
		}
		if err := c.Holder.Open(); err != nil {
			return err
		}
		if err := c.SetNodeState(NodeStateReady); err != nil {
			return err
		}
	}

	// Start the listener on the coordinator.
	if len(t.Clusters) == 0 {
		return nil
	}
	t.Clusters[0].ListenForJoins()

	return nil
}

// Close closes all clusters in the test cluster.
func (t *ClusterCluster) Close() error {
	for _, c := range t.Clusters {
		err := c.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// SendSync is a test implemenetation of Broadcaster SendSync method.
func (t *ClusterCluster) SendSync(pb proto.Message) error {
	switch obj := pb.(type) {
	case *internal.ClusterStatus:
		// Apply the send message to all nodes (except the coordinator).
		for _, c := range t.Clusters {
			c.MergeClusterStatus(obj)
		}
		t.mu.RLock()
		if obj.State == ClusterStateNormal && t.resizing {
			close(t.resizeDone)
		}
		t.mu.RUnlock()
	}

	return nil
}

// SendAsync is a test implemenetation of Broadcaster SendAsync method.
func (t *ClusterCluster) SendAsync(pb proto.Message) error {
	return nil
}

// SendTo is a test implemenetation of Broadcaster SendTo method.
func (t *ClusterCluster) SendTo(to *Node, pb proto.Message) error {
	switch obj := pb.(type) {
	case *internal.ResizeInstruction:
		err := t.FollowResizeInstruction(obj)
		if err != nil {
			return err
		}
	case *internal.ResizeInstructionComplete:
		coord := t.clusterByID(to.ID)
		go coord.MarkResizeInstructionComplete(obj)
	}
	return nil
}

// FollowResizeInstruction is a version of cluster.FollowResizeInstruction used for testing.
func (t *ClusterCluster) FollowResizeInstruction(instr *internal.ResizeInstruction) error {

	// Prepare the return message.
	complete := &internal.ResizeInstructionComplete{
		JobID: instr.JobID,
		Node:  instr.Node,
		Error: "",
	}

	// Stop processing on any error.
	if err := func() error {

		// figure out which node it was meant for, then call the operation on that cluster
		// basically need to mimic this: client.RetrieveSliceFromURI(context.Background(), src.Index, src.Frame, src.View, src.Slice, srcURI)
		instrNode := DecodeNode(instr.Node)
		destCluster := t.clusterByID(instrNode.ID)

		// Sync the schema received in the resize instruction.
		if err := destCluster.Holder.ApplySchema(instr.Schema); err != nil {
			return err
		}

		for _, src := range instr.Sources {
			srcNode := DecodeNode(src.Node)
			srcCluster := t.clusterByID(srcNode.ID)

			srcFragment := srcCluster.Holder.Fragment(src.Index, src.Frame, src.View, src.Slice)
			destFragment := destCluster.Holder.Fragment(src.Index, src.Frame, src.View, src.Slice)
			if destFragment == nil {
				// Create fragment on destination if it doesn't exist.
				f := destCluster.Holder.Frame(src.Index, src.Frame)
				v := f.View(src.View)
				var err error
				destFragment, err = v.CreateFragmentIfNotExists(src.Slice)
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

	node := DecodeNode(instr.Coordinator)
	if err := t.SendTo(node, complete); err != nil {
		return err
	}

	return nil
}
