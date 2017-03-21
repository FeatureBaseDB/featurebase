package pilosa

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"net/url"

	"golang.org/x/sync/errgroup"

	"github.com/gogo/protobuf/proto"
)

const (
	// DefaultPartitionN is the default number of partitions in a cluster.
	DefaultPartitionN = 16

	// DefaultReplicaN is the default number of replicas per partition.
	DefaultReplicaN = 1

	// HealthStatus is the return value of the /health endpoint for a node in the cluster.
	HealthStatusUp   = "UP"
	HealthStatusDown = "DOWN"
)

// Node represents a node in the cluster.
type Node struct {
	Host string `json:"host"`
}

// Nodes represents a list of nodes.
type Nodes []*Node

// Contains returns true if a node exists in the list.
func (a Nodes) Contains(n *Node) bool {
	for i := range a {
		if a[i] == n {
			return true
		}
	}
	return false
}

// ContainsHost returns true if host matches one of the node's host.
func (a Nodes) ContainsHost(host string) bool {
	for _, n := range a {
		if n.Host == host {
			return true
		}
	}
	return false
}

// Filter returns a new list of nodes with node removed.
func (a Nodes) Filter(n *Node) []*Node {
	other := make([]*Node, 0, len(a))
	for i := range a {
		if a[i] != n {
			other = append(other, a[i])
		}
	}
	return other
}

// FilterHost returns a new list of nodes with host removed.
func (a Nodes) FilterHost(host string) []*Node {
	other := make([]*Node, 0, len(a))
	for _, node := range a {
		if node.Host != host {
			other = append(other, node)
		}
	}
	return other
}

// Hosts returns a list of all hostnames.
func (a Nodes) Hosts() []string {
	hosts := make([]string, len(a))
	for i, n := range a {
		hosts[i] = n.Host
	}
	return hosts
}

// Clone returns a shallow copy of nodes.
func (a Nodes) Clone() []*Node {
	other := make([]*Node, len(a))
	copy(other, a)
	return other
}

// Cluster represents a collection of nodes.
type Cluster struct {
	Nodes   []*Node
	NodeSet NodeSet

	// Hashing algorithm used to assign partitions to nodes.
	Hasher Hasher

	// The number of partitions in the cluster.
	PartitionN int

	// The number of replicas a partition has.
	ReplicaN int
}

// NewCluster returns a new instance of Cluster with defaults.
func NewCluster() *Cluster {
	return &Cluster{
		Hasher:     &jmphasher{},
		PartitionN: DefaultPartitionN,
		ReplicaN:   DefaultReplicaN,
	}
}

// NodeSetHosts returns the list of host strings for NodeSet members
func (c *Cluster) NodeSetHosts() []string {
	if c.NodeSet == nil {
		return []string{}
	}
	a := make([]string, 0, len(c.NodeSet.Nodes()))
	for _, m := range c.NodeSet.Nodes() {
		a = append(a, m.Host)
	}
	return a
}

// Health returns a list of nodes in the cluster along with each node's state (UP/DOWN).
func (c *Cluster) Health() map[string]string {
	h := make(map[string]string)
	for _, n := range c.Nodes {
		h[n.Host] = HealthStatusDown
	}
	// we are assuming that NodeSetHosts is a subset of c.Nodes
	for _, m := range c.NodeSetHosts() {
		if _, ok := h[m]; ok {
			h[m] = HealthStatusUp
		}
	}
	return h
}

// NodeByHost returns a node reference by host.
func (c *Cluster) NodeByHost(host string) *Node {
	for _, n := range c.Nodes {
		if n.Host == host {
			return n
		}
	}
	return nil
}

// Partition returns the partition that a slice belongs to.
func (c *Cluster) Partition(db string, slice uint64) int {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], slice)

	// Hash the bytes and mod by partition count.
	h := fnv.New64a()
	h.Write([]byte(db))
	h.Write(buf[:])
	return int(h.Sum64() % uint64(c.PartitionN))
}

// FragmentNodes returns a list of nodes that own a fragment.
func (c *Cluster) FragmentNodes(db string, slice uint64) []*Node {
	return c.PartitionNodes(c.Partition(db, slice))
}

// OwnsFragment returns true if a host owns a fragment.
func (c *Cluster) OwnsFragment(host string, db string, slice uint64) bool {
	return Nodes(c.FragmentNodes(db, slice)).ContainsHost(host)
}

// PartitionNodes returns a list of nodes that own a partition.
func (c *Cluster) PartitionNodes(partitionID int) []*Node {
	// Default replica count to between one and the number of nodes.
	// The replica count can be zero if there are no nodes.
	replicaN := c.ReplicaN
	if replicaN > len(c.Nodes) {
		replicaN = len(c.Nodes)
	} else if replicaN == 0 {
		replicaN = 1
	}

	// Determine primary owner node.
	index := c.Hasher.Hash(uint64(partitionID), len(c.Nodes))

	// Collect nodes around the ring.
	nodes := make([]*Node, replicaN)
	for i := 0; i < replicaN; i++ {
		nodes[i] = c.Nodes[(index+i)%len(c.Nodes)]
	}

	return nodes
}

// NodeSet represents an interface to maintaining Node state.
type NodeSet interface {
	// Returns a list of all Nodes in the cluster
	Nodes() []*Node

	// Attempts to join a cluster having `nodes` as its existing members
	Join(nodes []*Node) (int, error)

	// Open starts any network activity implemented by the NodeSet
	Open() error

	// SetMessageHandler provides the NodeSet with a function to call on ReceiveMessage
	SetMessageHandler(f func(proto.Message) error)

	// SetRemoteStateHandler provides the function to call on MergeRemoteState
	SetRemoteStateHandler(f func(proto.Message) error)

	// SetLocalStateSource provides the function to get the current node's local state.
	SetLocalStateSource(f func() (proto.Message, error))
}

// Hasher represents an interface to hash integers into buckets.
type Hasher interface {
	// Hashes the key into a number between [0,N).
	Hash(key uint64, n int) int
}

// NewHasher returns a new instance of the default hasher.
func NewHasher() Hasher { return &jmphasher{} }

// jmphasher represents an implementation of jmphash. Implements Hasher.
type jmphasher struct{}

// Hash returns the integer hash for the given key.
func (h *jmphasher) Hash(key uint64, n int) int {
	b, j := int64(-1), int64(0)
	for j < int64(n) {
		b = j
		key = key*uint64(2862933555777941757) + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}

// HTTPNodeSet represents a NodeSet that broadcasts messages over HTTP.
type HTTPNodeSet struct {
	nodes          []*Node
	messageHandler func(m proto.Message) error
	// remoteStateHandler func(m proto.Message) error
	// localStateSource   func() (proto.Message, error)
}

// NewHTTPNodeSet returns a new instance of HTTPNodeSet.
func NewHTTPNodeSet() *HTTPNodeSet {
	return &HTTPNodeSet{}
}

func (h *HTTPNodeSet) Nodes() []*Node {
	return h.nodes
}

func (h *HTTPNodeSet) Join(nodes []*Node) (int, error) {
	h.nodes = nodes
	return 0, nil
}

func (h *HTTPNodeSet) Open() error {
	return nil
}

// SendMessage asyncronously broadcasts a protobuf message to all nodes.
func (h *HTTPNodeSet) SendMessage(pb proto.Message, method string) error {

	// Marshal the pb to []byte
	buf, err := MarshalMessage(pb)
	if err != nil {
		return err
	}

	var g errgroup.Group
	for _, n := range h.nodes {
		node := n
		g.Go(func() error {
			return h.sendNodeMessage(node, buf)
		})
	}
	return g.Wait()
}

// ReceiveMessage is called when a node receives a message.
func (h *HTTPNodeSet) ReceiveMessage(pb proto.Message) error {
	if h.messageHandler != nil {
		return h.messageHandler(pb)
	}
	// The messageHandler has not been set.
	return nil
}

func (h *HTTPNodeSet) sendNodeMessage(node *Node, msg []byte) error {
	var client *http.Client
	client = http.DefaultClient

	// Create HTTP request.
	req, err := http.NewRequest("POST", (&url.URL{
		Scheme: "http",
		Host:   node.Host,
		Path:   "/message",
	}).String(), bytes.NewReader(msg))
	if err != nil {
		return err
	}

	// Require protobuf encoding.
	req.Header.Set("Content-Type", "application/x-protobuf")

	// Send request to remote node.
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read response into buffer.
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	// Check status code.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status: code=%d, err=%s", resp.StatusCode, body)
	}

	return nil
}

// SetMessageHandler provides the Messenger with a function to handle incoming messages.
func (h *HTTPNodeSet) SetMessageHandler(f func(proto.Message) error) {
	h.messageHandler = f
}

// SetRemoteStateHandler provides the Messenger with a function to merge remote state.
func (h *HTTPNodeSet) SetRemoteStateHandler(f func(proto.Message) error) {
	// not implemented
	// h.remoteStateHandler = f
}

// SetLocalStateSource currently no-ops.
func (h *HTTPNodeSet) SetLocalStateSource(f func() (proto.Message, error)) {
	// not implemented
	// h.localStateSource = f
}

// StaticNodeSet represents a basic NodeSet for testing
type StaticNodeSet struct {
	Messenger
	nodes []*Node
}

func NewStaticNodeSet() *StaticNodeSet {
	return &StaticNodeSet{}
}

func (s *StaticNodeSet) Nodes() []*Node {
	return s.nodes
}

func (s *StaticNodeSet) Join(nodes []*Node) (int, error) {
	s.nodes = nodes
	return 0, nil
}

func (s *StaticNodeSet) Open() error {
	return nil
}

func (s *StaticNodeSet) SetMessageHandler(f func(proto.Message) error) {
	return
}

func (s *StaticNodeSet) SetRemoteStateHandler(f func(proto.Message) error) {
	return
}

func (s *StaticNodeSet) SetLocalStateSource(f func() (proto.Message, error)) {
	return
}

func (s *StaticNodeSet) SendMessage(pb proto.Message, method string) error {
	return nil
}
func (s *StaticNodeSet) ReceiveMessage(pb proto.Message) error {
	return nil
}
