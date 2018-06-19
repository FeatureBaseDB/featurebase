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
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const (
	// DefaultPartitionN is the default number of partitions in a cluster.
	DefaultPartitionN = 256

	// ClusterState represents the state returned in the /status endpoint.
	ClusterStateStarting = "STARTING"
	ClusterStateNormal   = "NORMAL"
	ClusterStateResizing = "RESIZING"

	// NodeState represents the state of a node during startup.
	NodeStateLoading = "LOADING"
	NodeStateReady   = "READY"

	// resizeJob states.
	resizeJobStateRunning = "RUNNING"
	// Final states.
	resizeJobStateDone    = "DONE"
	resizeJobStateAborted = "ABORTED"

	resizeJobActionAdd    = "ADD"
	resizeJobActionRemove = "REMOVE"
)

// Node represents a node in the cluster.
type Node struct {
	ID            string `json:"id"`
	URI           URI    `json:"uri"`
	IsCoordinator bool   `json:"isCoordinator"`
}

func (n Node) String() string {
	return fmt.Sprintf("Node: %s", n.ID)
}

// EncodeNodes converts a slice of Nodes into its internal representation.
func EncodeNodes(a []*Node) []*internal.Node {
	other := make([]*internal.Node, len(a))
	for i := range a {
		other[i] = EncodeNode(a[i])
	}
	return other
}

// EncodeNode converts a Node into its internal representation.
func EncodeNode(n *Node) *internal.Node {
	return &internal.Node{
		ID:            n.ID,
		URI:           n.URI.Encode(),
		IsCoordinator: n.IsCoordinator,
	}
}

// DecodeNodes converts a proto message into a slice of Nodes.
func DecodeNodes(a []*internal.Node) []*Node {
	if len(a) == 0 {
		return nil
	}
	other := make([]*Node, len(a))
	for i := range a {
		other[i] = DecodeNode(a[i])
	}
	return other
}

// DecodeNode converts a proto message into a Node.
func DecodeNode(node *internal.Node) *Node {
	return &Node{
		ID:            node.ID,
		URI:           decodeURI(node.URI),
		IsCoordinator: node.IsCoordinator,
	}
}

func DecodeNodeEvent(ne *internal.NodeEventMessage) *NodeEvent {
	return &NodeEvent{
		Event: NodeEventType(ne.Event),
		Node:  DecodeNode(ne.Node),
	}
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

// ContainsID returns true if host matches one of the node's id.
func (a Nodes) ContainsID(id string) bool {
	for _, n := range a {
		if n.ID == id {
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

// FilterID returns a new list of nodes with ID removed.
func (a Nodes) FilterID(id string) []*Node {
	other := make([]*Node, 0, len(a))
	for _, node := range a {
		if node.ID != id {
			other = append(other, node)
		}
	}
	return other
}

// FilterURI returns a new list of nodes with URI removed.
func (a Nodes) FilterURI(uri URI) []*Node {
	other := make([]*Node, 0, len(a))
	for _, node := range a {
		if node.URI != uri {
			other = append(other, node)
		}
	}
	return other
}

// IDs returns a list of all node IDs.
func (a Nodes) IDs() []string {
	ids := make([]string, len(a))
	for i, n := range a {
		ids[i] = n.ID
	}
	return ids
}

// URIs returns a list of all uris.
func (a Nodes) URIs() []URI {
	uris := make([]URI, len(a))
	for i, n := range a {
		uris[i] = n.URI
	}
	return uris
}

// Clone returns a shallow copy of nodes.
func (a Nodes) Clone() []*Node {
	other := make([]*Node, len(a))
	copy(other, a)
	return other
}

// byID implements sort.Interface for []Node based on
// the ID field.
type byID []*Node

func (h byID) Len() int           { return len(h) }
func (h byID) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h byID) Less(i, j int) bool { return h[i].ID < h[j].ID }

// nodeAction represents a node that is joining or leaving the cluster.
type nodeAction struct {
	node   *Node
	action string
}

// Cluster represents a collection of nodes.
type Cluster struct {
	ID        string
	Node      *Node
	Nodes     []*Node // TODO phase this out?
	MemberSet MemberSet

	// Hashing algorithm used to assign partitions to nodes.
	Hasher Hasher

	// The number of partitions in the cluster.
	PartitionN int

	// The number of replicas a partition has.
	ReplicaN int

	// Threshold for logging long-running queries
	LongQueryTime time.Duration

	// Maximum number of SetBit() or ClearBit() commands per request.
	MaxWritesPerRequest int

	// EventReceiver receives NodeEvents pertaining to node membership.
	EventReceiver EventReceiver

	// Data directory path.
	Path     string
	Topology *Topology

	// Required for cluster Resize.
	Static      bool // Static is primarily used for testing in a non-gossip environment.
	state       string
	Coordinator string
	Holder      *Holder
	Broadcaster Broadcaster

	joiningLeavingNodes chan nodeAction

	// joining is held open until this node
	// receives ClusterStatus from the coordinator.
	joining chan struct{}
	joined  bool

	mu         sync.RWMutex
	jobs       map[int64]*resizeJob
	currentJob *resizeJob

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	Logger Logger

	InternalClient InternalClient
}

// NewCluster returns a new instance of Cluster with defaults.
func NewCluster() *Cluster {
	return &Cluster{
		Hasher:        &jmphasher{},
		PartitionN:    DefaultPartitionN,
		ReplicaN:      1,
		EventReceiver: NopEventReceiver,

		joiningLeavingNodes: make(chan nodeAction, 10), // buffered channel
		jobs:                make(map[int64]*resizeJob),
		closing:             make(chan struct{}),
		joining:             make(chan struct{}),

		InternalClient: NewNopInternalClient(),

		Logger: NopLogger,
	}
}

// coordinatorNode returns the coordinator node.
func (c *Cluster) coordinatorNode() *Node {
	return c.unprotectedNodeByID(c.Coordinator)
}

// isCoordinator is true if this node is the coordinator.
func (c *Cluster) isCoordinator() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.unprotectedIsCoordinator()
}

func (c *Cluster) unprotectedIsCoordinator() bool {
	return c.Coordinator == c.Node.ID
}

// setCoordinator tells the current node to become the
// Coordinator. In response to this, the current node
// will consider itself coordinator and update the other
// nodes with its version of Cluster.Status.
func (c *Cluster) setCoordinator(n *Node) error {
	c.mu.Lock()
	// Verify that the new Coordinator value matches
	// this node.
	if c.Node.ID != n.ID {
		c.mu.Unlock()
		return fmt.Errorf("coordinator node does not match this node")
	}

	// Update IsCoordinator on all nodes (locally).
	_ = c.unprotectedUpdateCoordinator(n)
	c.mu.Unlock()
	// Send the update coordinator message to all nodes.
	err := c.Broadcaster.SendSync(
		&internal.UpdateCoordinatorMessage{
			New: EncodeNode(n),
		})
	if err != nil {
		return fmt.Errorf("problem sending UpdateCoordinator message: %v", err)
	}

	// Broadcast cluster status.
	return c.Broadcaster.SendSync(c.Status())
}

// updateCoordinator updates this nodes Coordinator value as well as
// changing the corresponding node's IsCoordinator value
// to true, and sets all other nodes to false. Returns true if the value
// changed.
func (c *Cluster) updateCoordinator(n *Node) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unprotectedUpdateCoordinator(n)
}

func (c *Cluster) unprotectedUpdateCoordinator(n *Node) bool {
	var changed bool
	if c.Coordinator != n.ID {
		c.Coordinator = n.ID
		changed = true
	}
	for _, node := range c.Nodes {
		if node.ID == n.ID {
			node.IsCoordinator = true
		} else {
			node.IsCoordinator = false
		}
	}
	return changed
}

// addNode adds a node to the Cluster and updates and saves the
// new topology.
func (c *Cluster) addNode(node *Node) error {
	c.Logger.Printf("add node %s to cluster on %s", node, c.Node)

	// If the node being added is the coordinator, set it for this node.
	if node.IsCoordinator {
		c.Coordinator = node.ID
	}

	// add to cluster
	if !c.addNodeBasicSorted(node) {
		return nil
	}

	// add to topology
	if c.Topology == nil {
		return fmt.Errorf("Cluster.Topology is nil")
	}
	if !c.Topology.AddID(node.ID) {
		return nil
	}

	// save topology
	return c.saveTopology()
}

// removeNode removes a node from the Cluster and updates and saves the
// new topology.
func (c *Cluster) removeNode(node *Node) error {
	// remove from cluster
	if !c.removeNodeBasicSorted(node) {
		return nil
	}

	// remove from topology
	if c.Topology == nil {
		return fmt.Errorf("Cluster.Topology is nil")
	}
	if !c.Topology.RemoveID(node.ID) {
		return nil
	}

	// save topology
	return c.saveTopology()
}

// nodeIDs returns the list of IDs in the cluster.
func (c *Cluster) nodeIDs() []string {
	return Nodes(c.Nodes).IDs()
}

func (c *Cluster) setID(id string) {
	// Don't overwrite ClusterID.
	if c.ID != "" {
		return
	}
	c.ID = id

	// Make sure the Topology is updated.
	c.Topology.ClusterID = c.ID
}

func (c *Cluster) State() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *Cluster) SetState(state string) {
	c.mu.Lock()
	c.setState(state)
	c.mu.Unlock()
}

func (c *Cluster) setState(state string) {
	// Ignore cases where the state hasn't changed.
	if state == c.state {
		return
	}

	c.Logger.Printf("change cluster state from %s to %s on %s", c.state, state, c.Node.ID)

	var doCleanup bool

	switch state {
	case ClusterStateNormal:
		// If state is RESIZING -> NORMAL then run cleanup.
		if c.state == ClusterStateResizing {
			doCleanup = true
		}
	}

	c.state = state

	// TODO: consider NOT running cleanup on an active node that has
	// been removed.
	// It's safe to do a cleanup after state changes back to normal.
	if doCleanup {
		var cleaner HolderCleaner
		cleaner.Node = c.Node
		cleaner.Holder = c.Holder
		cleaner.Cluster = c
		cleaner.Closing = c.closing

		// Clean holder.
		if err := cleaner.CleanHolder(); err != nil {
			c.Logger.Printf("holder clean error: err=%s", err)
		}
	}
}

func (c *Cluster) setNodeState(state string) error {
	if c.isCoordinator() {
		return c.receiveNodeState(c.Node.ID, state)
	}

	// Send node state to coordinator.
	ns := &internal.NodeStateMessage{
		NodeID: c.Node.ID,
		State:  state,
	}

	c.Logger.Printf("Sending State %s (%s)", state, c.Coordinator)
	if err := c.sendTo(c.coordinatorNode(), ns); err != nil {
		return fmt.Errorf("sending node state error: err=%s", err)
	}

	return nil
}

// receiveNodeState sets node state in Topology in order for the
// Coordinator to keep track of, during startup, which nodes have
// finished opening their Holder.
func (c *Cluster) receiveNodeState(nodeID string, state string) error {
	if !c.isCoordinator() {
		return nil
	}

	// This method is really only useful during initial startup.
	if c.State() != ClusterStateStarting {
		return nil
	}

	c.Topology.nodeStates[nodeID] = state
	c.Logger.Printf("received state %s (%s)", state, nodeID)

	// Set cluster state to NORMAL.
	if c.haveTopologyAgreement() && c.allNodesReady() {
		return c.setStateAndBroadcast(ClusterStateNormal)
	}

	return nil
}

// Status returns the internal ClusterStatus representation.
func (c *Cluster) Status() *internal.ClusterStatus {
	return &internal.ClusterStatus{
		ClusterID: c.ID,
		State:     c.state,
		Nodes:     EncodeNodes(c.Nodes),
	}
}

func (c *Cluster) nodeByID(id string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.unprotectedNodeByID(id)
}

// unprotectedNodeByID returns a node reference by ID.
func (c *Cluster) unprotectedNodeByID(id string) *Node {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// nodePositionByID returns the position of the node in slice c.Nodes.
func (c *Cluster) nodePositionByID(nodeID string) int {
	for i, n := range c.Nodes {
		if n.ID == nodeID {
			return i
		}
	}
	return -1
}

// addNodeBasicSorted adds a node to the cluster, sorted by id.
// Returns a pointer to the node and true if the node was added.
func (c *Cluster) addNodeBasicSorted(node *Node) bool {
	n := c.unprotectedNodeByID(node.ID)
	if n != nil {
		return false
	}

	c.Nodes = append(c.Nodes, node)

	// All hosts must be merged in the same order on all nodes in the cluster.
	sort.Sort(byID(c.Nodes))

	return true
}

// removeNodeBasicSorted removes a node from the cluster, maintaining
// the sort order. Returns true if the node was removed.
func (c *Cluster) removeNodeBasicSorted(node *Node) bool {
	i := c.nodePositionByID(node.ID)
	if i < 0 {
		return false
	}

	copy(c.Nodes[i:], c.Nodes[i+1:])
	c.Nodes[len(c.Nodes)-1] = nil
	c.Nodes = c.Nodes[:len(c.Nodes)-1]

	return true
}

// frag is a struct of basic fragment information.
type frag struct {
	field string
	view  string
	slice uint64
}

func fragsDiff(a, b []frag) []frag {
	m := make(map[frag]uint64)

	for _, y := range b {
		m[y]++
	}

	var ret []frag
	for _, x := range a {
		if m[x] > 0 {
			m[x]--
			continue
		}
		ret = append(ret, x)
	}

	return ret
}

type fragsByHost map[string][]frag

func (a fragsByHost) add(b fragsByHost) fragsByHost {
	for k, v := range b {
		a[k] = append(a[k], v...)
	}
	return a
}

type viewsByField map[string][]string

func (a viewsByField) addView(field, view string) {
	a[field] = append(a[field], view)
}

func (c *Cluster) fragsByHost(idx *Index) fragsByHost {
	// fieldViews is a map of field to slice of views.
	fieldViews := make(viewsByField)

	for _, field := range idx.Fields() {
		for _, view := range field.Views() {
			fieldViews.addView(field.Name(), view.name)

		}
	}
	return c.fragCombos(idx.Name(), idx.MaxSlice(), fieldViews)
}

// fragCombos returns a map (by uri) of lists of fragments for a given index
// by creating every combination of field/view specified in `fieldViews` up to maxSlice.
func (c *Cluster) fragCombos(idx string, maxSlice uint64, fieldViews viewsByField) fragsByHost {
	t := make(fragsByHost)
	for i := uint64(0); i <= maxSlice; i++ {
		nodes := c.sliceNodes(idx, i)
		for _, n := range nodes {
			// for each field/view combination:
			for field, views := range fieldViews {
				for _, view := range views {
					t[n.ID] = append(t[n.ID], frag{field, view, i})
				}
			}
		}
	}
	return t
}

// diff compares c with another cluster and determines if a node is being
// added or removed. An error is returned for any case other than where
// exactly one node is added or removed.
func (c *Cluster) diff(other *Cluster) (action string, nodeID string, err error) {
	lenFrom := len(c.Nodes)
	lenTo := len(other.Nodes)
	// Determine if a node is being added or removed.
	if lenFrom == lenTo {
		return "", "", errors.New("clusters are the same size")
	}
	if lenFrom < lenTo {
		// Adding a node.
		if lenTo-lenFrom > 1 {
			return "", "", errors.New("adding more than one node at a time is not supported")
		}
		action = resizeJobActionAdd
		// Determine the node ID that is being added.
		for _, n := range other.Nodes {
			if c.unprotectedNodeByID(n.ID) == nil {
				nodeID = n.ID
				break
			}
		}
	} else if lenFrom > lenTo {
		// Removing a node.
		if lenFrom-lenTo > 1 {
			return "", "", errors.New("removing more than one node at a time is not supported")
		}
		action = resizeJobActionRemove
		// Determine the node ID that is being removed.
		for _, n := range c.Nodes {
			if other.unprotectedNodeByID(n.ID) == nil {
				nodeID = n.ID
				break
			}
		}
	}
	return action, nodeID, nil
}

// fragSources returns a list of ResizeSources - for each node in the `to` cluster -
// required to move from cluster `c` to cluster `to`.
func (c *Cluster) fragSources(to *Cluster, idx *Index) (map[string][]*internal.ResizeSource, error) {
	m := make(map[string][]*internal.ResizeSource)

	// Determine if a node is being added or removed.
	action, diffNodeID, err := c.diff(to)
	if err != nil {
		return nil, errors.Wrap(err, "diffing")
	}

	// Initialize the map with all the nodes in `to`.
	for _, n := range to.Nodes {
		m[n.ID] = nil
	}

	// If a node is being added, the source can be confined to the
	// primary fragments (i.e. no need to use replicas as source data).
	// In this case, source fragments can be based on a cluster with
	// replica = 1.
	// If a node is being removed, however, then it will most likely
	// require that a replica fragment be the source data.
	srcCluster := c
	if action == resizeJobActionAdd && c.ReplicaN > 1 {
		srcCluster = NewCluster()
		srcCluster.Nodes = Nodes(c.Nodes).Clone()
		srcCluster.Hasher = c.Hasher
		srcCluster.PartitionN = c.PartitionN
		srcCluster.ReplicaN = 1
	}

	// Represents the fragment location for the from/to clusters.
	fFrags := c.fragsByHost(idx)
	tFrags := to.fragsByHost(idx)

	// srcFrags is the frag map based on a source cluster of replica = 1.
	srcFrags := srcCluster.fragsByHost(idx)

	// srcNodesByFrag is the inverse representation of srcFrags.
	srcNodesByFrag := make(map[frag]string)
	for nodeID, frags := range srcFrags {
		// If a node is being removed, don't consider it as a source.
		if action == resizeJobActionRemove && nodeID == diffNodeID {
			continue
		}
		for _, frag := range frags {
			srcNodesByFrag[frag] = nodeID
		}
	}

	// Get the frag diff for each nodeID.
	diffs := make(fragsByHost)
	for nodeID, frags := range tFrags {
		if _, ok := fFrags[nodeID]; ok {
			diffs[nodeID] = fragsDiff(frags, fFrags[nodeID])
		} else {
			diffs[nodeID] = frags
		}
	}

	// Get the ResizeSource for each diff.
	for nodeID, diff := range diffs {
		m[nodeID] = []*internal.ResizeSource{}
		for _, frag := range diff {
			// If there is no valid source node ID for a fragment,
			// it likely means that the replica factor was not
			// high enough for the remaining nodes to contain
			// the fragment.
			srcNodeID, ok := srcNodesByFrag[frag]
			if !ok {
				return nil, errors.New("not enough data to perform resize (replica factor may need to be increased)")
			}

			src := &internal.ResizeSource{
				Node:  EncodeNode(c.unprotectedNodeByID(srcNodeID)),
				Index: idx.Name(),
				Field: frag.field,
				View:  frag.view,
				Slice: frag.slice,
			}

			m[nodeID] = append(m[nodeID], src)
		}
	}

	return m, nil
}

// partition returns the partition that a slice belongs to.
func (c *Cluster) partition(index string, slice uint64) int {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], slice)

	// Hash the bytes and mod by partition count.
	h := fnv.New64a()
	h.Write([]byte(index))
	h.Write(buf[:])
	return int(h.Sum64() % uint64(c.PartitionN))
}

// sliceNodes returns a list of nodes that own a fragment.
func (c *Cluster) sliceNodes(index string, slice uint64) []*Node {
	return c.partitionNodes(c.partition(index, slice))
}

// ownsSlice returns true if a host owns a fragment.
func (c *Cluster) ownsSlice(nodeID string, index string, slice uint64) bool {
	return Nodes(c.sliceNodes(index, slice)).ContainsID(nodeID)
}

// partitionNodes returns a list of nodes that own a partition.
func (c *Cluster) partitionNodes(partitionID int) []*Node {
	// Default replica count to between one and the number of nodes.
	// The replica count can be zero if there are no nodes.
	replicaN := c.ReplicaN
	if replicaN > len(c.Nodes) {
		replicaN = len(c.Nodes)
	} else if replicaN == 0 {
		replicaN = 1
	}

	// Determine primary owner node.
	nodeIndex := c.Hasher.Hash(uint64(partitionID), len(c.Nodes))

	// Collect nodes around the ring.
	nodes := make([]*Node, replicaN)
	for i := 0; i < replicaN; i++ {
		nodes[i] = c.Nodes[(nodeIndex+i)%len(c.Nodes)]
	}

	return nodes
}

// containsSlices is like OwnsSlices, but it includes replicas.
func (c *Cluster) containsSlices(index string, maxSlice uint64, node *Node) []uint64 {
	var slices []uint64
	for i := uint64(0); i <= maxSlice; i++ {
		p := c.partition(index, i)
		// Determine the nodes for partition.
		nodes := c.partitionNodes(p)
		for _, n := range nodes {
			if n.ID == node.ID {
				slices = append(slices, i)
			}
		}
	}
	return slices
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

func (c *Cluster) open() error {
	// Cluster always comes up in state STARTING until cluster membership is determined.
	c.state = ClusterStateStarting

	// Load topology file if it exists.
	if err := c.loadTopology(); err != nil {
		return errors.Wrap(err, "loading topology")
	}

	c.ID = c.Topology.ClusterID

	// Only the coordinator needs to consider the .topology file.
	if c.isCoordinator() {
		err := c.considerTopology()
		if err != nil {
			return fmt.Errorf("considerTopology: %v", err)
		}
	}

	// Add the local node to the cluster.
	err := c.addNode(c.Node)
	if err != nil {
		return errors.Wrap(err, "adding local node")
	}

	// Start the EventReceiver.
	if err := c.EventReceiver.Start(c); err != nil {
		return fmt.Errorf("starting EventReceiver: %v", err)
	}

	// Open MemberSet communication.
	if err := c.MemberSet.Open(c.Node); err != nil {
		return fmt.Errorf("opening MemberSet: %v", err)
	}

	// If not coordinator then wait for ClusterStatus from coordinator.
	if !c.isCoordinator() {
		// In the case where a node has been restarted and memberlist has
		// not had enough time to determine the node went down/up, then
		// the coorninator needs to be alerted that this node is back up
		// (and now in a state of STARTING) so that it can be put to the correct
		// cluster state.
		// TODO: Because the normal code path already sends a NodeJoin event (via
		// memberlist), this it a bit redundant in most cases. Perhaps determine
		// that the node has been restarted and don't do this step.
		msg := &internal.NodeEventMessage{
			Event: uint32(NodeJoin),
			Node:  EncodeNode(c.Node),
		}
		if err := c.Broadcaster.SendSync(msg); err != nil {
			return fmt.Errorf("sending restart NodeJoin: %v", err)
		}

		c.Logger.Printf("wait for joining to complete")
		<-c.joining
		c.Logger.Printf("joining has completed")
	}

	return nil
}

func (c *Cluster) close() error {
	// Notify goroutines of closing and wait for completion.
	close(c.closing)
	c.wg.Wait()

	return nil
}

func (c *Cluster) markAsJoined() {
	c.Logger.Printf("mark node as joined (received coordinator update)")
	if !c.joined {
		c.joined = true
		close(c.joining)
	}
}

func (c *Cluster) needTopologyAgreement() bool {
	return c.State() == ClusterStateStarting && !stringSlicesAreEqual(c.Topology.NodeIDs, c.nodeIDs())
}

func (c *Cluster) haveTopologyAgreement() bool {
	if c.Static {
		return true
	}
	return stringSlicesAreEqual(c.Topology.NodeIDs, c.nodeIDs())
}

func (c *Cluster) allNodesReady() bool {
	if c.Static {
		return true
	}
	for _, uri := range c.Topology.NodeIDs {
		if c.Topology.nodeStates[uri] != NodeStateReady {
			return false
		}
	}
	return true
}

func (c *Cluster) handleNodeAction(nodeAction nodeAction) error {
	j, err := c.generateResizeJob(nodeAction)
	if err != nil {
		c.Logger.Printf("generateResizeJob error: err=%s", err)
		if err := c.setStateAndBroadcast(ClusterStateNormal); err != nil {
			c.Logger.Printf("setStateAndBroadcast error: err=%s", err)
		}
		return errors.Wrap(err, "setting state")
	}

	// j.Run() runs in a goroutine because in the case where the
	// job requires no action, it immediately writes to the j.result
	// channel, which is not consumed until the code below.
	var eg errgroup.Group
	eg.Go(func() error {
		return j.run()
	})

	// Wait for the resizeJob to finish or be aborted.
	c.Logger.Printf("wait for jobResult")
	jobResult := <-j.result

	// Make sure j.Run() didn't return an error.
	if eg.Wait() != nil {
		return errors.Wrap(err, "running job")
	}

	c.Logger.Printf("received jobResult: %s", jobResult)
	switch jobResult {
	case resizeJobStateDone:
		if err := c.completeCurrentJob(resizeJobStateDone); err != nil {
			return errors.Wrap(err, "completing finished job")
		}
		// Add/remove uri to/from the cluster.
		if j.action == resizeJobActionRemove {
			return c.removeNode(nodeAction.node)
		} else if j.action == resizeJobActionAdd {
			return c.addNode(nodeAction.node)
		}
	case resizeJobStateAborted:
		if err := c.completeCurrentJob(resizeJobStateAborted); err != nil {
			return errors.Wrap(err, "completing aborted job")
		}
	}
	return nil
}

func (c *Cluster) setStateAndBroadcast(state string) error {
	c.SetState(state)
	// Broadcast cluster status changes to the cluster.
	c.Logger.Printf("broadcasting ClusterStatus: %s", state)
	return c.Broadcaster.SendSync(c.Status())
}

func (c *Cluster) sendTo(node *Node, msg proto.Message) error {
	if err := c.Broadcaster.SendTo(node, msg); err != nil {
		return errors.Wrap(err, "sending")
	}
	return nil
}

// listenForJoins handles cluster-resize events.
func (c *Cluster) listenForJoins() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		// When a cluster starts, the state is STARTING.
		// We first want to wait for at least one node to join.
		// Then we want to clear out the joiningLeavingNodes queue (buffered channel).
		// Then we want to set the cluster state to NORMAL and resume processing of joiningLeavingNodes events.
		// We use a bool `setNormal` to indicate when at least one node has joined.
		var setNormal bool

		for {

			// Handle all pending joins before changing state back to NORMAL.
			select {
			case nodeAction := <-c.joiningLeavingNodes:
				err := c.handleNodeAction(nodeAction)
				if err != nil {
					c.Logger.Printf("handleNodeAction error: err=%s", err)
					continue
				}
				setNormal = true
				continue
			default:
			}

			// Only change state to NORMAL if we have successfully added at least one host.
			if setNormal {
				// Put the cluster back to state NORMAL and broadcast.
				if err := c.setStateAndBroadcast(ClusterStateNormal); err != nil {
					c.Logger.Printf("setStateAndBroadcast error: err=%s", err)
				}
			}

			// Wait for a joining host or a close.
			select {
			case <-c.closing:
				return
			case nodeAction := <-c.joiningLeavingNodes:
				err := c.handleNodeAction(nodeAction)
				if err != nil {
					c.Logger.Printf("handleNodeAction error: err=%s", err)
					continue
				}
				setNormal = true
				continue
			}
		}
	}()
}

// generateResizeJob creates a new resizeJob based on the new node being
// added/removed. It also saves a reference to the resizeJob in the `jobs` map
// for future lookup by JobID.
func (c *Cluster) generateResizeJob(nodeAction nodeAction) (*resizeJob, error) {
	c.Logger.Printf("generateResizeJob: %v", nodeAction)
	c.mu.Lock()
	defer c.mu.Unlock()

	j, err := c.generateResizeJobByAction(nodeAction)
	if err != nil {
		return nil, errors.Wrap(err, "generating job")
	}
	c.Logger.Printf("generated resizeJob: %d", j.ID)

	// Save job in jobs map for future reference.
	c.jobs[j.ID] = j

	// Set job as currentJob.
	if c.currentJob != nil {
		return nil, fmt.Errorf("there is currently a resize job running")
	}
	c.currentJob = j

	return j, nil
}

// generateResizeJobByAction returns a resizeJob with instructions based on
// the difference between Cluster and a new Cluster with/without uri.
// Broadcaster is associated to the resizeJob here for use in broadcasting
// the resize instructions to other nodes in the cluster.
func (c *Cluster) generateResizeJobByAction(nodeAction nodeAction) (*resizeJob, error) {
	j := newResizeJob(c.Nodes, nodeAction.node, nodeAction.action)
	j.Broadcaster = c.Broadcaster

	// toCluster is a clone of Cluster with the new node added/removed for comparison.
	toCluster := NewCluster()
	toCluster.Nodes = Nodes(c.Nodes).Clone()
	toCluster.Hasher = c.Hasher
	toCluster.PartitionN = c.PartitionN
	toCluster.ReplicaN = c.ReplicaN
	if nodeAction.action == resizeJobActionRemove {
		toCluster.removeNodeBasicSorted(nodeAction.node)
	} else if nodeAction.action == resizeJobActionAdd {
		toCluster.addNodeBasicSorted(nodeAction.node)
	}

	// multiIndex is a map of sources initialized with all the nodes in toCluster.
	multiIndex := make(map[string][]*internal.ResizeSource)

	for _, n := range toCluster.Nodes {
		multiIndex[n.ID] = nil
	}

	// Add to multiIndex the instructions for each index.
	for _, idx := range c.Holder.Indexes() {
		fragSources, err := c.fragSources(toCluster, idx)
		if err != nil {
			return nil, errors.Wrap(err, "getting sources")
		}

		for id, sources := range fragSources {
			multiIndex[id] = append(multiIndex[id], sources...)
		}
	}

	for id, sources := range multiIndex {
		// If a host doesn't need to request data, mark it as complete.
		if len(sources) == 0 {
			j.IDs[id] = true
			continue
		}
		instr := &internal.ResizeInstruction{
			JobID:         j.ID,
			Node:          EncodeNode(toCluster.unprotectedNodeByID(id)),
			Coordinator:   EncodeNode(c.coordinatorNode()),
			Sources:       sources,
			Schema:        c.Holder.EncodeSchema(), // Include the schema to ensure it's in sync on the receiving node.
			ClusterStatus: c.Status(),
		}
		j.Instructions = append(j.Instructions, instr)
	}

	return j, nil
}

// completeCurrentJob sets the state of the current resizeJob
// then removes the pointer to currentJob.
func (c *Cluster) completeCurrentJob(state string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.unprotectedIsCoordinator() {
		return ErrNodeNotCoordinator
	}
	if c.currentJob == nil {
		return ErrResizeNotRunning
	}
	c.currentJob.setState(state)
	c.currentJob = nil
	return nil
}

// followResizeInstruction is run by any node that receives a ResizeInstruction.
func (c *Cluster) followResizeInstruction(instr *internal.ResizeInstruction) error {
	c.Logger.Printf("follow resize instruction on %s", c.Node.ID)
	// Make sure the cluster status on this node agrees with the Coordinator
	// before attempting a resize.
	if err := c.mergeClusterStatus(instr.ClusterStatus); err != nil {
		return errors.Wrap(err, "merging cluster status")
	}

	c.Logger.Printf("MergeClusterStatus done, start goroutine")

	// The actual resizing runs in a goroutine because we don't want to block
	// the distribution of other ResizeInstructions to the rest of the cluster.
	go func() {

		// Make sure the holder has opened.
		<-c.Holder.opened

		// Prepare the return message.
		complete := &internal.ResizeInstructionComplete{
			JobID: instr.JobID,
			Node:  instr.Node,
			Error: "",
		}

		// Stop processing on any error.
		if err := func() error {

			// Sync the schema received in the resize instruction.
			c.Logger.Printf("Holder ApplySchema")
			if err := c.Holder.ApplySchema(instr.Schema); err != nil {
				return errors.Wrap(err, "applying schema")
			}

			// Request each source file in ResizeSources.
			for _, src := range instr.Sources {
				c.Logger.Printf("get slice %d for index %s from host %s", src.Slice, src.Index, src.Node.URI)

				srcURI := decodeURI(src.Node.URI)

				// Retrieve field.
				f := c.Holder.Field(src.Index, src.Field)
				if f == nil {
					return ErrFieldNotFound
				}

				// Create view.
				v, err := f.CreateViewIfNotExists(src.View)
				if err != nil {
					return errors.Wrap(err, "creating view")
				}

				// Create the local fragment.
				frag, err := v.CreateFragmentIfNotExists(src.Slice)
				if err != nil {
					return errors.Wrap(err, "creating fragment")
				}

				// Stream slice from remote node.
				c.Logger.Printf("retrieve slice %d for index %s from host %s", src.Slice, src.Index, src.Node.URI)
				rd, err := c.InternalClient.RetrieveSliceFromURI(context.Background(), src.Index, src.Field, src.Slice, srcURI)
				if err != nil {
					// For now it is an acceptable error if the fragment is not found
					// on the remote node. This occurs when a slice has been skipped and
					// therefore doesn't contain data. The coordinator correctly determined
					// the resize instruction to retrieve the slice, but it doesn't have data.
					// TODO: figure out a way to distinguish from "fragment not found" errors
					// which are true errors and which simply mean the fragment doesn't have data.
					if err == ErrFragmentNotFound {
						return nil
					}
					return errors.Wrap(err, "retrieving slice")
				} else if rd == nil {
					return fmt.Errorf("slice %v doesn't exist on host: %s", src.Slice, src.Node.URI)
				}

				// Write to local field and always close reader.
				if err := func() error {
					defer rd.Close()
					_, err := frag.ReadFrom(rd)
					return err
				}(); err != nil {
					return errors.Wrap(err, "copying remote slice")
				}
			}
			return nil
		}(); err != nil {
			complete.Error = err.Error()
		}

		if err := c.sendTo(DecodeNode(instr.Coordinator), complete); err != nil {
			c.Logger.Printf("sending resizeInstructionComplete error: err=%s", err)
		}
	}()
	return nil
}

func (c *Cluster) markResizeInstructionComplete(complete *internal.ResizeInstructionComplete) error {

	j := c.job(complete.JobID)

	// Abort the job if an error exists in the complete object.
	if complete.Error != "" {
		j.result <- resizeJobStateAborted
		return errors.New(complete.Error)
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	if j.isComplete() {
		return fmt.Errorf("resize job %d is no longer running", j.ID)
	}

	// Mark host complete.
	j.IDs[complete.Node.ID] = true

	if !j.nodesArePending() {
		j.result <- resizeJobStateDone
	}

	return nil
}

// job returns a resizeJob by id.
func (c *Cluster) job(id int64) *resizeJob {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.jobs[id]
}

type resizeJob struct {
	ID           int64
	IDs          map[string]bool
	Instructions []*internal.ResizeInstruction
	Broadcaster  Broadcaster

	action string
	result chan string

	mu    sync.RWMutex
	state string

	Logger Logger
}

// newResizeJob returns a new instance of resizeJob.
func newResizeJob(existingNodes []*Node, node *Node, action string) *resizeJob {

	// Build a map of uris to track their resize status.
	// The value for a node will be set to true after that node
	// has indicated that it has completed all resize instructions.
	ids := make(map[string]bool)

	if action == resizeJobActionRemove {
		for _, n := range existingNodes {
			// Exclude the removed node from the map.
			if n.ID == node.ID {
				continue
			}
			ids[n.ID] = false
		}
	} else if action == resizeJobActionAdd {
		for _, n := range existingNodes {
			ids[n.ID] = false
		}
		// Include the added node in the map for tracking.
		ids[node.ID] = false
	}

	return &resizeJob{
		ID:     rand.Int63(),
		IDs:    ids,
		action: action,
		result: make(chan string),
		Logger: NopLogger,
	}
}

func (j *resizeJob) setState(state string) {
	j.mu.Lock()
	if j.state == "" || j.state == resizeJobStateRunning {
		j.state = state
	}
	j.mu.Unlock()
}

// run distributes ResizeInstructions.
func (j *resizeJob) run() error {
	j.Logger.Printf("run resizeJob")
	// Set job state to RUNNING.
	j.setState(resizeJobStateRunning)

	// Job can be considered done in the case where it doesn't require any action.
	if !j.nodesArePending() {
		j.Logger.Printf("resizeJob contains no pending tasks; mark as done")
		j.result <- resizeJobStateDone
		return nil
	}

	j.Logger.Printf("distribute tasks for resizeJob")
	err := j.distributeResizeInstructions()
	if err != nil {
		j.result <- resizeJobStateAborted
		return errors.Wrap(err, "distributing instructions")
	}
	return nil
}

// isComplete return true if the job is any one of several completion states.
func (j *resizeJob) isComplete() bool {
	switch j.state {
	case resizeJobStateDone, resizeJobStateAborted:
		return true
	default:
		return false
	}
}

// nodesArePending returns true if any node is still working on the resize.
func (j *resizeJob) nodesArePending() bool {
	for _, complete := range j.IDs {
		if !complete {
			return true
		}
	}
	return false
}

func (j *resizeJob) distributeResizeInstructions() error {
	j.Logger.Printf("distributeResizeInstructions for job %d", j.ID)
	// Loop through the ResizeInstructions in resizeJob and send to each host.
	for _, instr := range j.Instructions {
		// Because the node may not be in the cluster yet, create
		// a dummy node object to use in the SendTo() method.
		node := &Node{
			ID:  instr.Node.ID,
			URI: decodeURI(instr.Node.URI),
		}
		j.Logger.Printf("send resize instructions: %v", instr)
		if err := j.Broadcaster.SendTo(node, instr); err != nil {
			return errors.Wrap(err, "sending instruction")
		}
	}
	return nil
}

type NodeIDs []string

func (n NodeIDs) Len() int           { return len(n) }
func (n NodeIDs) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n NodeIDs) Less(i, j int) bool { return n[i] < n[j] }

// ContainsID returns true if idi matches one of the nodesets's IDs.
func (n NodeIDs) ContainsID(id string) bool {
	for _, nid := range n {
		if nid == id {
			return true
		}
	}
	return false
}

// Topology represents the list of hosts in the cluster.
type Topology struct {
	mu      sync.RWMutex
	NodeIDs []string

	ClusterID string

	// nodeStates holds the state of each node according to
	// the coordinator. Used during startup and data load.
	nodeStates map[string]string
}

func NewTopology() *Topology {
	return &Topology{
		nodeStates: make(map[string]string),
	}
}

// ContainsID returns true if id matches one of the topology's IDs.
func (t *Topology) ContainsID(id string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.containsID(id)
}

func (t *Topology) containsID(id string) bool {
	return NodeIDs(t.NodeIDs).ContainsID(id)
}

func (t *Topology) positionByID(nodeID string) int {
	for i, tid := range t.NodeIDs {
		if tid == nodeID {
			return i
		}
	}
	return -1
}

// AddID adds the node ID to the topology and returns true if added.
func (t *Topology) AddID(nodeID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.containsID(nodeID) {
		return false
	}
	t.NodeIDs = append(t.NodeIDs, nodeID)

	sort.Slice(t.NodeIDs,
		func(i, j int) bool {
			return t.NodeIDs[i] < t.NodeIDs[j]
		})

	return true
}

// RemoveID removes the node ID from the topology and returns true if removed.
func (t *Topology) RemoveID(nodeID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	i := t.positionByID(nodeID)
	if i < 0 {
		return false
	}

	copy(t.NodeIDs[i:], t.NodeIDs[i+1:])
	t.NodeIDs[len(t.NodeIDs)-1] = ""
	t.NodeIDs = t.NodeIDs[:len(t.NodeIDs)-1]

	return true
}

// Encode converts t into its internal representation.
func (t *Topology) Encode() *internal.Topology {
	return encodeTopology(t)
}

// loadTopology reads the topology for the node.
func (c *Cluster) loadTopology() error {
	buf, err := ioutil.ReadFile(filepath.Join(c.Path, ".topology"))
	if os.IsNotExist(err) {
		c.Topology = NewTopology()
		return nil
	} else if err != nil {
		return errors.Wrap(err, "reading file")
	}

	var pb internal.Topology
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return errors.Wrap(err, "unmarshalling")
	}
	top, err := decodeTopology(&pb)
	if err != nil {
		return errors.Wrap(err, "decoding")
	}
	c.Topology = top

	return nil
}

// saveTopology writes the current topology to disk.
func (c *Cluster) saveTopology() error {

	if err := os.MkdirAll(c.Path, 0777); err != nil {
		return errors.Wrap(err, "creating directory")
	}

	if buf, err := proto.Marshal(encodeTopology(c.Topology)); err != nil {
		return errors.Wrap(err, "marshalling")
	} else if err := ioutil.WriteFile(filepath.Join(c.Path, ".topology"), buf, 0666); err != nil {
		return errors.Wrap(err, "writing file")
	}
	return nil
}

func encodeTopology(topology *Topology) *internal.Topology {
	if topology == nil {
		return nil
	}
	return &internal.Topology{
		ClusterID: topology.ClusterID,
		NodeIDs:   topology.NodeIDs,
	}
}

func decodeTopology(topology *internal.Topology) (*Topology, error) {
	if topology == nil {
		return nil, nil
	}

	t := NewTopology()
	t.ClusterID = topology.ClusterID
	t.NodeIDs = topology.NodeIDs
	sort.Slice(t.NodeIDs,
		func(i, j int) bool {
			return t.NodeIDs[i] < t.NodeIDs[j]
		})

	return t, nil
}

func (c *Cluster) considerTopology() error {
	// Create ClusterID if one does not already exist.
	if c.ID == "" {
		u := uuid.NewV4()
		c.ID = u.String()
		c.Topology.ClusterID = c.ID
	}

	if c.Static {
		return nil
	}

	// If there is no .topology file, it's safe to proceed.
	if len(c.Topology.NodeIDs) == 0 {
		return nil
	}

	// The local node (coordinator) must be in the .topology.
	if !c.Topology.ContainsID(c.Node.ID) {
		return fmt.Errorf("coordinator %s is not in topology: %v", c.Node.ID, c.Topology.NodeIDs)
	}

	// If local node is the only thing in .topology, continue.
	//if len(c.Topology.NodeIDs) == 1 {
	//	return nil
	//}

	// Keep the cluster in state "STARTING" until hearing from all nodes.
	// Topology contains 2+ hosts.
	return nil
}

// ReceiveEvent represents an implementation of EventHandler.
func (c *Cluster) ReceiveEvent(e *NodeEvent) error {
	// Ignore events sent from this node.
	if e.Node.ID == c.Node.ID {
		return nil
	}

	switch e.Event {
	case NodeJoin:
		c.Logger.Printf("received NodeJoin event: %v", e)
		// Ignore the event if this is not the coordinator.
		if !c.isCoordinator() {
			return nil
		}
		return c.nodeJoin(e.Node)
	case NodeLeave:
		// Automatic nodeLeave is intentionally not implemented.
	case NodeUpdate:
		// NodeUpdate is intentionally not implemented.
	}

	return nil
}

func (c *Cluster) nodeJoin(node *Node) error {
	if c.needTopologyAgreement() {
		// A host that is not part of the topology can't be added to the STARTING cluster.
		if !c.Topology.ContainsID(node.ID) {
			err := fmt.Sprintf("host is not in topology: %s", node.ID)
			c.Logger.Printf("%v", err)
			return errors.New(err)
		}

		if err := c.addNode(node); err != nil {
			return errors.Wrap(err, "adding node for agreement")
		}

		// Only change to normal if there is no existing data. Otherwise,
		// the coordinator needs to wait to receive READY messages (nodeStates)
		// from remote nodes before setting the cluster to state NORMAL.
		if ok, err := c.Holder.HasData(); !ok && err == nil {
			// If the result of the previous AddNode completed the joining of nodes
			// in the topology, then change the state to NORMAL.
			if c.haveTopologyAgreement() {
				return c.setStateAndBroadcast(ClusterStateNormal)
			}
			return nil
		} else if err != nil {
			return errors.Wrap(err, "checking if holder has data")
		}

		if c.haveTopologyAgreement() && c.allNodesReady() {
			return c.setStateAndBroadcast(ClusterStateNormal)
		} else {
			// Send the status to the remote node. This lets the remote node
			// know that it can proceed with opening its Holder.
			return c.sendTo(node, c.Status())
		}
	}

	// If the cluster already contains the node, just send it the cluster status.
	// This is useful in the case where a node is restarted or temporarily leaves
	// the cluster.
	if node := c.unprotectedNodeByID(node.ID); node != nil {
		return c.sendTo(node, c.Status())
	}

	// If the holder does not yet contain data, go ahead and add the node.
	if ok, err := c.Holder.HasData(); !ok && err == nil {
		if err := c.addNode(node); err != nil {
			return errors.Wrap(err, "adding node")
		}
		return c.setStateAndBroadcast(ClusterStateNormal)
	} else if err != nil {
		return errors.Wrap(err, "checking if holder has data2")
	}

	// If the cluster has data, we need to change to RESIZING and
	// kick off the resizing process.
	if err := c.setStateAndBroadcast(ClusterStateResizing); err != nil {
		return errors.Wrap(err, "broadcasting state")
	}
	c.joiningLeavingNodes <- nodeAction{node, resizeJobActionAdd}

	return nil
}

// nodeLeave initiates the removal of a node from the cluster.
func (c *Cluster) nodeLeave(node *Node) error {
	// Refuse the request if this is not the coordinator.
	if !c.isCoordinator() {
		return fmt.Errorf("node removal requests are only valid on the coordinator node: %s", c.coordinatorNode().ID)
	}

	if c.State() != ClusterStateNormal {
		return fmt.Errorf("Cluster must be in state %s to remove a node. Current state: %s", ClusterStateNormal, c.State())
	}

	// Ensure that node is in the cluster.
	if c.unprotectedNodeByID(node.ID) == nil {
		return fmt.Errorf("Node is not a member of the cluster: %s", node.ID)
	}

	// Prevent removing the coordinator node (this node).
	if node.ID == c.Node.ID {
		return fmt.Errorf("coordinator cannot be removed; first, make a different node the new coordinator.")
	}

	// See if resize job can be generated
	if _, err := c.generateResizeJobByAction(nodeAction{c.unprotectedNodeByID(node.ID), resizeJobActionRemove}); err != nil {
		return errors.Wrap(err, "generating job")
	}

	// Get the actual node in the local cluster.
	n := c.unprotectedNodeByID(node.ID)

	// Don't do anything else if the cluster doesn't contain the node.
	if n == nil {
		return nil
	}

	// If the holder does not yet contain data, go ahead and remove the node.
	if ok, err := c.Holder.HasData(); !ok && err == nil {
		if err := c.removeNode(n); err != nil {
			return errors.Wrap(err, "removing node")
		}
		return c.setStateAndBroadcast(ClusterStateNormal)
	} else if err != nil {
		return errors.Wrap(err, "checking if holder has data")
	}

	// If the cluster has data then change state to RESIZING and
	// kick off the resizing process.
	if err := c.setStateAndBroadcast(ClusterStateResizing); err != nil {
		return errors.Wrap(err, "broadcasting state")
	}
	c.joiningLeavingNodes <- nodeAction{n, resizeJobActionRemove}

	return nil
}

func (c *Cluster) mergeClusterStatus(cs *internal.ClusterStatus) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Logger.Printf("merge cluster status: %v", cs)
	// Ignore status updates from self (coordinator).
	if c.unprotectedIsCoordinator() {
		return nil
	}

	// Set ClusterID.
	c.setID(cs.ClusterID)

	officialNodes := DecodeNodes(cs.Nodes)

	// Add all nodes from the coordinator.
	for _, node := range officialNodes {
		if err := c.addNode(node); err != nil {
			return errors.Wrap(err, "adding node")
		}
	}

	// Remove any nodes not specified by the coordinator
	// except for self. Generate a list to remove first
	// so that nodes aren't removed mid-loop.
	nodeIDsToRemove := []string{}
	for _, node := range c.Nodes {
		// Don't remove this node.
		if node.ID == c.Node.ID {
			continue
		}
		if Nodes(officialNodes).ContainsID(node.ID) {
			continue
		}
		nodeIDsToRemove = append(nodeIDsToRemove, node.ID)
	}

	for _, nodeID := range nodeIDsToRemove {
		if err := c.removeNode(c.unprotectedNodeByID(nodeID)); err != nil {
			return errors.Wrap(err, "removing node")
		}
	}

	c.setState(cs.State)

	c.markAsJoined()

	return nil
}
