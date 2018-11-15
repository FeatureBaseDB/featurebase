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
	"github.com/pilosa/pilosa/logger"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const (
	// defaultPartitionN is the default number of partitions in a cluster.
	defaultPartitionN = 256

	// ClusterState represents the state returned in the /status endpoint.
	ClusterStateStarting = "STARTING"
	ClusterStateDegraded = "DEGRADED" // cluster is running but we've lost some # of hosts >0 but < replicaN
	ClusterStateNormal   = "NORMAL"
	ClusterStateResizing = "RESIZING"

	// NodeState represents the state of a node during startup.
	nodeStateReady = "READY"
	nodeStateDown  = "DOWN"

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
	State         string `json:"state"`
}

func (n Node) String() string {
	return fmt.Sprintf("Node:%s:%s:%s", n.URI, n.State, n.ID[:6])
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

// cluster represents a collection of nodes.
type cluster struct { // nolint: maligned
	id    string
	Node  *Node
	nodes []*Node

	// Hashing algorithm used to assign partitions to nodes.
	Hasher Hasher

	// The number of partitions in the cluster.
	partitionN int

	// The number of replicas a partition has.
	ReplicaN int

	// Threshold for logging long-running queries
	longQueryTime time.Duration

	// Maximum number of Set() or Clear() commands per request.
	maxWritesPerRequest int

	// Data directory path.
	Path     string
	Topology *Topology

	// Required for cluster Resize.
	Static      bool // Static is primarily used for testing in a non-gossip environment.
	state       string
	Coordinator string
	holder      *Holder
	broadcaster broadcaster

	joiningLeavingNodes chan nodeAction

	// joining is held open until this node
	// receives ClusterStatus from the coordinator.
	joining chan struct{}
	joined  bool

	abortAntiEntropyCh chan struct{}

	mu         sync.RWMutex
	jobs       map[int64]*resizeJob
	currentJob *resizeJob

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	logger logger.Logger

	InternalClient InternalClient
}

// newCluster returns a new instance of Cluster with defaults.
func newCluster() *cluster {
	return &cluster{
		Hasher:     &jmphasher{},
		partitionN: defaultPartitionN,
		ReplicaN:   1,

		joiningLeavingNodes: make(chan nodeAction, 10), // buffered channel
		jobs:                make(map[int64]*resizeJob),
		closing:             make(chan struct{}),
		joining:             make(chan struct{}),

		InternalClient: newNopInternalClient(),

		logger: logger.NopLogger,
	}
}

// initializeAntiEntropy is called by the anti entropy routine when it starts.
// If the AE channel is created without a routine reading from it, cluster will
// block indefinitely when calling abortAntiEntropy().
func (c *cluster) initializeAntiEntropy() {
	c.mu.Lock()
	c.abortAntiEntropyCh = make(chan struct{})
	c.mu.Unlock()
}

// abortAntiEntropyQ checks whether the cluster wants to abort the anti entropy
// process (so that it can resize). It does not block.
func (c *cluster) abortAntiEntropyQ() bool {
	select {
	case <-c.abortAntiEntropyCh:
		return true
	default:
		return false
	}
}

// abortAntiEntropy blocks until the anti-entropy routine calls abortAntiEntropyQ
func (c *cluster) abortAntiEntropy() {
	if c.abortAntiEntropyCh != nil {
		c.abortAntiEntropyCh <- struct{}{}
	}
}

func (c *cluster) coordinatorNode() *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.unprotectedCoordinatorNode()
}

// unprotectedCoordinatorNode returns the coordinator node.
func (c *cluster) unprotectedCoordinatorNode() *Node {
	return c.unprotectedNodeByID(c.Coordinator)
}

// isCoordinator is true if this node is the coordinator.
func (c *cluster) isCoordinator() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.unprotectedIsCoordinator()
}

func (c *cluster) unprotectedIsCoordinator() bool {
	return c.Coordinator == c.Node.ID
}

// setCoordinator tells the current node to become the
// Coordinator. In response to this, the current node
// will consider itself coordinator and update the other
// nodes with its version of Cluster.Status.
func (c *cluster) setCoordinator(n *Node) error {
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
	err := c.broadcaster.SendSync(
		&UpdateCoordinatorMessage{
			New: n,
		})
	if err != nil {
		return fmt.Errorf("problem sending UpdateCoordinator message: %v", err)
	}

	// Broadcast cluster status.
	return c.broadcaster.SendSync(c.status())
}

// updateCoordinator updates this nodes Coordinator value as well as
// changing the corresponding node's IsCoordinator value
// to true, and sets all other nodes to false. Returns true if the value
// changed.
func (c *cluster) updateCoordinator(n *Node) bool { // nolint: unparam
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unprotectedUpdateCoordinator(n)
}

func (c *cluster) unprotectedUpdateCoordinator(n *Node) bool {
	var changed bool
	if c.Coordinator != n.ID {
		c.Coordinator = n.ID
		changed = true
	}
	for _, node := range c.nodes {
		if node.ID == n.ID {
			node.IsCoordinator = true
		} else {
			node.IsCoordinator = false
		}
	}
	return changed
}

// addNode adds a node to the Cluster and updates and saves the
// new topology. unprotected.
func (c *cluster) addNode(node *Node) error {
	c.logger.Printf("add node %s to cluster on %s", node, c.Node)

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
	if !c.Topology.addID(node.ID) {
		return nil
	}

	// save topology
	return c.saveTopology()
}

// removeNode removes a node from the Cluster and updates and saves the
// new topology. unprotected.
func (c *cluster) removeNode(nodeID string) error {
	// remove from cluster
	c.removeNodeBasicSorted(nodeID)

	// remove from topology
	if c.Topology == nil {
		return fmt.Errorf("Cluster.Topology is nil")
	}
	if !c.Topology.removeID(nodeID) {
		return nil
	}

	// save topology
	return c.saveTopology()
}

// nodeIDs returns the list of IDs in the cluster.
func (c *cluster) nodeIDs() []string {
	return Nodes(c.nodes).IDs()
}

func (c *cluster) unprotectedSetID(id string) {
	// Don't overwrite ClusterID.
	if c.id != "" {
		return
	}
	c.id = id

	// Make sure the Topology is updated.
	c.Topology.clusterID = c.id
}

func (c *cluster) State() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *cluster) SetState(state string) {
	c.mu.Lock()
	c.unprotectedSetState(state)
	c.mu.Unlock()
}

func (c *cluster) unprotectedSetState(state string) {
	// Ignore cases where the state hasn't changed.
	if state == c.state {
		return
	}

	c.logger.Printf("change cluster state from %s to %s on %s", c.state, state, c.Node.ID)

	var doCleanup bool

	switch state {
	case ClusterStateNormal, ClusterStateDegraded:
		// If state is RESIZING -> NORMAL then run cleanup.
		if c.state == ClusterStateResizing {
			doCleanup = true
		}
	}

	c.state = state

	if state == ClusterStateResizing {
		c.abortAntiEntropy()
	}

	// TODO: consider NOT running cleanup on an active node that has
	// been removed.
	// It's safe to do a cleanup after state changes back to normal.
	if doCleanup {
		var cleaner holderCleaner
		cleaner.Node = c.Node
		cleaner.Holder = c.holder
		cleaner.Cluster = c
		cleaner.Closing = c.closing

		// Clean holder.
		if err := cleaner.CleanHolder(); err != nil {
			c.logger.Printf("holder clean error: err=%s", err)
		}
	}
}

func (c *cluster) setMyNodeState(state string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Node.State = state
	for i, n := range c.nodes {
		if n.ID == c.Node.ID {
			c.nodes[i].State = state
		}
	}
}

func (c *cluster) setNodeState(state string) error { // nolint: unparam
	c.setMyNodeState(state)
	if c.isCoordinator() {
		return c.receiveNodeState(c.Node.ID, state)
	}

	// Send node state to coordinator.
	ns := &NodeStateMessage{
		NodeID: c.Node.ID,
		State:  state,
	}

	c.logger.Printf("Sending State %s (%s)", state, c.Coordinator)
	if err := c.sendTo(c.coordinatorNode(), ns); err != nil {
		return fmt.Errorf("sending node state error: err=%s", err)
	}

	return nil
}

// receiveNodeState sets node state in Topology in order for the
// Coordinator to keep track of, during startup, which nodes have
// finished opening their Holder.
func (c *cluster) receiveNodeState(nodeID string, state string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.unprotectedIsCoordinator() {
		return nil
	}

	c.Topology.mu.Lock()
	changed := false
	if c.Topology.nodeStates[nodeID] != state {
		changed = true
		c.Topology.nodeStates[nodeID] = state
		for i, n := range c.nodes {
			if n.ID == nodeID {
				c.nodes[i].State = state
			}
		}
	}
	c.Topology.mu.Unlock()
	c.logger.Printf("received state %s (%s)", state, nodeID)

	if changed {
		return c.unprotectedSetStateAndBroadcast(c.determineClusterState())
	}
	return nil
}

// determineClusterState is unprotected.
func (c *cluster) determineClusterState() (clusterState string) {
	if c.state == ClusterStateResizing {
		return ClusterStateResizing
	}
	if c.haveTopologyAgreement() && c.allNodesReady() {
		return ClusterStateNormal
	}
	if len(c.Topology.nodeIDs)-len(c.nodeIDs()) < c.ReplicaN && c.allNodesReady() {
		return ClusterStateDegraded
	}
	return ClusterStateStarting
}

func (c *cluster) status() *ClusterStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.unprotectedStatus()
}

// unprotectedStatus returns the the cluster's status including what nodes it contains, its ID, and current state.
func (c *cluster) unprotectedStatus() *ClusterStatus {
	return &ClusterStatus{
		ClusterID: c.id,
		State:     c.state,
		Nodes:     c.nodes,
	}
}

func (c *cluster) nodeByID(id string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.unprotectedNodeByID(id)
}

// unprotectedNodeByID returns a node reference by ID.
func (c *cluster) unprotectedNodeByID(id string) *Node {
	for _, n := range c.nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

func (c *cluster) topologyContainsNode(id string) bool {
	c.Topology.mu.RLock()
	defer c.Topology.mu.RUnlock()
	for _, nid := range c.Topology.nodeIDs {
		if id == nid {
			return true
		}
	}
	return false
}

// nodePositionByID returns the position of the node in slice c.Nodes.
func (c *cluster) nodePositionByID(nodeID string) int {
	for i, n := range c.nodes {
		if n.ID == nodeID {
			return i
		}
	}
	return -1
}

// addNodeBasicSorted adds a node to the cluster, sorted by id. Returns a
// pointer to the node and true if the node was added. unprotected.
func (c *cluster) addNodeBasicSorted(node *Node) bool {
	n := c.unprotectedNodeByID(node.ID)
	if n != nil {
		return false
	}

	c.nodes = append(c.nodes, node)

	// All hosts must be merged in the same order on all nodes in the cluster.
	sort.Sort(byID(c.nodes))

	return true
}

// Nodes returns a copy of the slice of nodes in the cluster. Safe for
// concurrent use, result may be modified.
func (c *cluster) Nodes() []*Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make([]*Node, len(c.nodes))
	copy(ret, c.nodes)
	return ret
}

// removeNodeBasicSorted removes a node from the cluster, maintaining the sort
// order. Returns true if the node was removed. unprotected.
func (c *cluster) removeNodeBasicSorted(nodeID string) bool {
	i := c.nodePositionByID(nodeID)
	if i < 0 {
		return false
	}

	copy(c.nodes[i:], c.nodes[i+1:])
	c.nodes[len(c.nodes)-1] = nil
	c.nodes = c.nodes[:len(c.nodes)-1]

	return true
}

// frag is a struct of basic fragment information.
type frag struct {
	field string
	view  string
	shard uint64
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

type viewsByField map[string][]string

func (a viewsByField) addView(field, view string) {
	a[field] = append(a[field], view)
}

func (c *cluster) fragsByHost(idx *Index) fragsByHost {
	// fieldViews is a map of field to slice of views.
	fieldViews := make(viewsByField)

	for _, field := range idx.Fields() {
		for _, view := range field.views() {
			fieldViews.addView(field.Name(), view.name)
		}
	}
	return c.fragCombos(idx.Name(), idx.AvailableShards(), fieldViews)
}

// fragCombos returns a map (by uri) of lists of fragments for a given index
// by creating every combination of field/view specified in `fieldViews` up
// for the given set of shards with data.
func (c *cluster) fragCombos(idx string, availableShards *roaring.Bitmap, fieldViews viewsByField) fragsByHost {
	t := make(fragsByHost)
	availableShards.ForEach(func(i uint64) {
		nodes := c.shardNodes(idx, i)
		for _, n := range nodes {
			// for each field/view combination:
			for field, views := range fieldViews {
				for _, view := range views {
					t[n.ID] = append(t[n.ID], frag{field, view, i})
				}
			}
		}
	})
	return t
}

// diff compares c with another cluster and determines if a node is being
// added or removed. An error is returned for any case other than where
// exactly one node is added or removed. unprotected.
func (c *cluster) diff(other *cluster) (action string, nodeID string, err error) {
	lenFrom := len(c.nodes)
	lenTo := len(other.nodes)
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
		for _, n := range other.nodes {
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
		for _, n := range c.nodes {
			if other.unprotectedNodeByID(n.ID) == nil {
				nodeID = n.ID
				break
			}
		}
	}
	return action, nodeID, nil
}

// fragSources returns a list of ResizeSources - for each node in the `to` cluster -
// required to move from cluster `c` to cluster `to`. unprotected.
func (c *cluster) fragSources(to *cluster, idx *Index) (map[string][]*ResizeSource, error) {
	m := make(map[string][]*ResizeSource)

	// Determine if a node is being added or removed.
	action, diffNodeID, err := c.diff(to)
	if err != nil {
		return nil, errors.Wrap(err, "diffing")
	}

	// Initialize the map with all the nodes in `to`.
	for _, n := range to.nodes {
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
		srcCluster = newCluster()
		srcCluster.nodes = Nodes(c.nodes).Clone()
		srcCluster.Hasher = c.Hasher
		srcCluster.partitionN = c.partitionN
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
		m[nodeID] = []*ResizeSource{}
		for _, frag := range diff {
			// If there is no valid source node ID for a fragment,
			// it likely means that the replica factor was not
			// high enough for the remaining nodes to contain
			// the fragment.
			srcNodeID, ok := srcNodesByFrag[frag]
			if !ok {
				return nil, errors.New("not enough data to perform resize (replica factor may need to be increased)")
			}

			src := &ResizeSource{
				Node:  c.unprotectedNodeByID(srcNodeID),
				Index: idx.Name(),
				Field: frag.field,
				View:  frag.view,
				Shard: frag.shard,
			}

			m[nodeID] = append(m[nodeID], src)
		}
	}

	return m, nil
}

// partition returns the partition that a shard belongs to.
func (c *cluster) partition(index string, shard uint64) int {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], shard)

	// Hash the bytes and mod by partition count.
	h := fnv.New64a()
	h.Write([]byte(index))
	h.Write(buf[:])
	return int(h.Sum64() % uint64(c.partitionN))
}

// shardNodes returns a list of nodes that own a fragment. unprotected
func (c *cluster) shardNodes(index string, shard uint64) []*Node {
	return c.partitionNodes(c.partition(index, shard))
}

// ownsShard returns true if a host owns a fragment.
func (c *cluster) ownsShard(nodeID string, index string, shard uint64) bool {
	return Nodes(c.shardNodes(index, shard)).ContainsID(nodeID)
}

// partitionNodes returns a list of nodes that own a partition. unprotected.
func (c *cluster) partitionNodes(partitionID int) []*Node {
	// Default replica count to between one and the number of nodes.
	// The replica count can be zero if there are no nodes.
	replicaN := c.ReplicaN
	if replicaN > len(c.nodes) {
		replicaN = len(c.nodes)
	} else if replicaN == 0 {
		replicaN = 1
	}

	// Determine primary owner node.
	nodeIndex := c.Hasher.Hash(uint64(partitionID), len(c.nodes))

	// Collect nodes around the ring.
	nodes := make([]*Node, replicaN)
	for i := 0; i < replicaN; i++ {
		nodes[i] = c.nodes[(nodeIndex+i)%len(c.nodes)]
	}

	return nodes
}

// containsShards is like OwnsShards, but it includes replicas.
func (c *cluster) containsShards(index string, availableShards *roaring.Bitmap, node *Node) []uint64 {
	var shards []uint64
	availableShards.ForEach(func(i uint64) {
		p := c.partition(index, i)
		// Determine the nodes for partition.
		nodes := c.partitionNodes(p)
		for _, n := range nodes {
			if n.ID == node.ID {
				shards = append(shards, i)
			}
		}
	})
	return shards
}

// Hasher represents an interface to hash integers into buckets.
type Hasher interface {
	// Hashes the key into a number between [0,N).
	Hash(key uint64, n int) int
}

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

func (c *cluster) setup() error {
	// Cluster always comes up in state STARTING until cluster membership is determined.
	c.state = ClusterStateStarting

	// Load topology file if it exists.
	if err := c.loadTopology(); err != nil {
		return errors.Wrap(err, "loading topology")
	}

	c.id = c.Topology.clusterID

	// Only the coordinator needs to consider the .topology file.
	if c.isCoordinator() {
		err := c.considerTopology()
		if err != nil {
			return errors.Wrap(err, "considerTopology")
		}
	}

	// Add the local node to the cluster.
	err := c.addNode(c.Node)
	if err != nil {
		return errors.Wrap(err, "adding local node")
	}
	return nil
}

func (c *cluster) open() error {
	err := c.setup()
	if err != nil {
		return errors.Wrap(err, "setting up cluster")
	}
	return c.waitForStarted()
}

func (c *cluster) waitForStarted() error {
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
		msg := &NodeEvent{
			Event: NodeJoin,
			Node:  c.Node,
		}
		if err := c.broadcaster.SendSync(msg); err != nil {
			return fmt.Errorf("sending restart NodeJoin: %v", err)
		}

		c.logger.Printf("%v wait for joining to complete", c.Node.ID)
		<-c.joining
		c.logger.Printf("joining has completed")
	}
	return nil
}

func (c *cluster) close() error {
	// Notify goroutines of closing and wait for completion.
	close(c.closing)
	c.wg.Wait()

	return nil
}

func (c *cluster) markAsJoined() {
	c.logger.Printf("mark node as joined (received coordinator update)")
	if !c.joined {
		c.joined = true
		close(c.joining)
	}
}

// needTopologyAgreement is unprotected.
func (c *cluster) needTopologyAgreement() bool {
	return c.state == ClusterStateStarting && !stringSlicesAreEqual(c.Topology.nodeIDs, c.nodeIDs())
}

// haveTopologyAgreement is unprotected.
func (c *cluster) haveTopologyAgreement() bool {
	if c.Static {
		return true
	}
	return stringSlicesAreEqual(c.Topology.nodeIDs, c.nodeIDs())
}

// allNodesReady is unprotected.
func (c *cluster) allNodesReady() (ret bool) {
	if c.Static {
		return true
	}
	for _, id := range c.nodeIDs() {
		if c.Topology.nodeStates[id] != nodeStateReady {
			return false
		}
	}
	return true
}

func (c *cluster) handleNodeAction(nodeAction nodeAction) error {
	c.mu.Lock()
	j, err := c.unprotectedGenerateResizeJob(nodeAction)
	c.mu.Unlock()
	if err != nil {
		c.logger.Printf("generateResizeJob error: err=%s", err)
		if err := c.setStateAndBroadcast(ClusterStateNormal); err != nil {
			c.logger.Printf("setStateAndBroadcast error: err=%s", err)
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
	c.logger.Printf("wait for jobResult")
	jobResult := <-j.result

	// Make sure j.run() didn't return an error.
	if eg.Wait() != nil {
		return errors.Wrap(err, "running job")
	}

	c.logger.Printf("received jobResult: %s", jobResult)
	switch jobResult {
	case resizeJobStateDone:
		if err := c.completeCurrentJob(resizeJobStateDone); err != nil {
			return errors.Wrap(err, "completing finished job")
		}
		// Add/remove uri to/from the cluster.
		if j.action == resizeJobActionRemove {
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.removeNode(nodeAction.node.ID)
		} else if j.action == resizeJobActionAdd {
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.addNode(nodeAction.node)
		}
	case resizeJobStateAborted:
		if err := c.completeCurrentJob(resizeJobStateAborted); err != nil {
			return errors.Wrap(err, "completing aborted job")
		}
	}
	return nil
}

func (c *cluster) setStateAndBroadcast(state string) error { // nolint: unparam
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unprotectedSetStateAndBroadcast(state)
}

func (c *cluster) unprotectedSetStateAndBroadcast(state string) error {
	c.unprotectedSetState(state)
	if c.Static {
		return nil
	}
	// Broadcast cluster status changes to the cluster.
	status := c.unprotectedStatus()
	c.logger.Printf("broadcasting ClusterStatus: %s", status)
	return c.broadcaster.SendSync(status) // TODO fix c.Status

}

func (c *cluster) sendTo(node *Node, m Message) error {
	if err := c.broadcaster.SendTo(node, m); err != nil {
		return errors.Wrap(err, "sending")
	}
	return nil
}

// listenForJoins handles cluster-resize events.
func (c *cluster) listenForJoins() {
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
					c.logger.Printf("handleNodeAction error: err=%s", err)
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
					c.logger.Printf("setStateAndBroadcast error: err=%s", err)
				}
			}

			// Wait for a joining host or a close.
			select {
			case <-c.closing:
				return
			case nodeAction := <-c.joiningLeavingNodes:
				err := c.handleNodeAction(nodeAction)
				if err != nil {
					c.logger.Printf("handleNodeAction error: err=%s", err)
					continue
				}
				setNormal = true
				continue
			}
		}
	}()
}

// unprotectedGenerateResizeJob creates a new resizeJob based on the new node being
// added/removed. It also saves a reference to the resizeJob in the `jobs` map
// for future lookup by JobID.
func (c *cluster) unprotectedGenerateResizeJob(nodeAction nodeAction) (*resizeJob, error) {
	c.logger.Printf("generateResizeJob: %v", nodeAction)

	j, err := c.unprotectedGenerateResizeJobByAction(nodeAction)
	if err != nil {
		return nil, errors.Wrap(err, "generating job")
	}
	c.logger.Printf("generated resizeJob: %d", j.ID)

	// Save job in jobs map for future reference.
	c.jobs[j.ID] = j

	// Set job as currentJob.
	if c.currentJob != nil {
		return nil, fmt.Errorf("there is currently a resize job running")
	}
	c.currentJob = j

	return j, nil
}

// unprotectedGenerateResizeJobByAction returns a resizeJob with instructions based on
// the difference between Cluster and a new Cluster with/without uri.
// Broadcaster is associated to the resizeJob here for use in broadcasting
// the resize instructions to other nodes in the cluster.
func (c *cluster) unprotectedGenerateResizeJobByAction(nodeAction nodeAction) (*resizeJob, error) {
	j := newResizeJob(c.nodes, nodeAction.node, nodeAction.action)
	j.Broadcaster = c.broadcaster

	// toCluster is a clone of Cluster with the new node added/removed for comparison.
	toCluster := newCluster()
	toCluster.nodes = Nodes(c.nodes).Clone()
	toCluster.Hasher = c.Hasher
	toCluster.partitionN = c.partitionN
	toCluster.ReplicaN = c.ReplicaN
	if nodeAction.action == resizeJobActionRemove {
		toCluster.removeNodeBasicSorted(nodeAction.node.ID)
	} else if nodeAction.action == resizeJobActionAdd {
		toCluster.addNodeBasicSorted(nodeAction.node)
	}

	// multiIndex is a map of sources initialized with all the nodes in toCluster.
	multiIndex := make(map[string][]*ResizeSource)

	for _, n := range toCluster.nodes {
		multiIndex[n.ID] = nil
	}

	// Add to multiIndex the instructions for each index.
	for _, idx := range c.holder.Indexes() {
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
		instr := &ResizeInstruction{
			JobID:         j.ID,
			Node:          toCluster.unprotectedNodeByID(id),
			Coordinator:   c.unprotectedCoordinatorNode(),
			Sources:       sources,
			Schema:        &Schema{Indexes: c.holder.Schema()}, // Include the schema to ensure it's in sync on the receiving node.
			ClusterStatus: c.unprotectedStatus(),
		}
		j.Instructions = append(j.Instructions, instr)
	}

	return j, nil
}

// completeCurrentJob sets the state of the current resizeJob
// then removes the pointer to currentJob.
func (c *cluster) completeCurrentJob(state string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unprotectedCompleteCurrentJob(state)
}

func (c *cluster) unprotectedCompleteCurrentJob(state string) error {
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
func (c *cluster) followResizeInstruction(instr *ResizeInstruction) error {
	c.logger.Printf("follow resize instruction on %s", c.Node.ID)
	// Make sure the cluster status on this node agrees with the Coordinator
	// before attempting a resize.
	if err := c.mergeClusterStatus(instr.ClusterStatus); err != nil {
		return errors.Wrap(err, "merging cluster status")
	}

	c.logger.Printf("MergeClusterStatus done, start goroutine")

	// The actual resizing runs in a goroutine because we don't want to block
	// the distribution of other ResizeInstructions to the rest of the cluster.
	go func() {

		// Make sure the holder has opened.
		<-c.holder.opened

		// Prepare the return message.
		complete := &ResizeInstructionComplete{
			JobID: instr.JobID,
			Node:  instr.Node,
			Error: "",
		}

		// Stop processing on any error.
		if err := func() error {

			// Sync the schema received in the resize instruction.
			c.logger.Printf("Holder ApplySchema")
			if err := c.holder.applySchema(instr.Schema); err != nil {
				return errors.Wrap(err, "applying schema")
			}

			// Request each source file in ResizeSources.
			for _, src := range instr.Sources {
				c.logger.Printf("get shard %d for index %s from host %s", src.Shard, src.Index, src.Node.URI)

				srcURI := src.Node.URI

				// Retrieve field.
				f := c.holder.Field(src.Index, src.Field)
				if f == nil {
					return ErrFieldNotFound
				}

				// Create view.
				v, err := f.createViewIfNotExists(src.View)
				if err != nil {
					return errors.Wrap(err, "creating view")
				}

				// Create the local fragment.
				frag, err := v.CreateFragmentIfNotExists(src.Shard)
				if err != nil {
					return errors.Wrap(err, "creating fragment")
				}

				// Stream shard from remote node.
				c.logger.Printf("retrieve shard %d for index %s from host %s", src.Shard, src.Index, src.Node.URI)
				rd, err := c.InternalClient.RetrieveShardFromURI(context.Background(), src.Index, src.Field, src.Shard, srcURI)
				if err != nil {
					// For now it is an acceptable error if the fragment is not found
					// on the remote node. This occurs when a shard has been skipped and
					// therefore doesn't contain data. The coordinator correctly determined
					// the resize instruction to retrieve the shard, but it doesn't have data.
					// TODO: figure out a way to distinguish from "fragment not found" errors
					// which are true errors and which simply mean the fragment doesn't have data.
					if err == ErrFragmentNotFound {
						return nil
					}
					return errors.Wrap(err, "retrieving shard")
				} else if rd == nil {
					return fmt.Errorf("shard %v doesn't exist on host: %s", src.Shard, src.Node.URI)
				}

				// Write to local field and always close reader.
				if err := func() error {
					defer rd.Close()
					_, err := frag.ReadFrom(rd)
					return err
				}(); err != nil {
					return errors.Wrap(err, "copying remote shard")
				}
			}
			return nil
		}(); err != nil {
			complete.Error = err.Error()
		}

		if err := c.sendTo(instr.Coordinator, complete); err != nil {
			c.logger.Printf("sending resizeInstructionComplete error: err=%s", err)
		}
	}()
	return nil
}

func (c *cluster) markResizeInstructionComplete(complete *ResizeInstructionComplete) error {

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
func (c *cluster) job(id int64) *resizeJob {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.jobs[id]
}

type resizeJob struct {
	ID           int64
	IDs          map[string]bool
	Instructions []*ResizeInstruction
	Broadcaster  broadcaster

	action string
	result chan string

	mu    sync.RWMutex
	state string

	Logger logger.Logger
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
		Logger: logger.NopLogger,
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
			URI: instr.Node.URI,
		}
		j.Logger.Printf("send resize instructions: %v", instr)
		if err := j.Broadcaster.SendTo(node, instr); err != nil {
			return errors.Wrap(err, "sending instruction")
		}
	}
	return nil
}

type nodeIDs []string

func (n nodeIDs) Len() int           { return len(n) }
func (n nodeIDs) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n nodeIDs) Less(i, j int) bool { return n[i] < n[j] }

// ContainsID returns true if idi matches one of the nodesets's IDs.
func (n nodeIDs) ContainsID(id string) bool {
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
	nodeIDs []string

	clusterID string

	// nodeStates holds the state of each node according to
	// the coordinator. Used during startup and data load.
	nodeStates map[string]string
}

func newTopology() *Topology {
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
	return nodeIDs(t.nodeIDs).ContainsID(id)
}

func (t *Topology) positionByID(nodeID string) int {
	for i, tid := range t.nodeIDs {
		if tid == nodeID {
			return i
		}
	}
	return -1
}

// addID adds the node ID to the topology and returns true if added.
func (t *Topology) addID(nodeID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.containsID(nodeID) {
		return false
	}
	t.nodeIDs = append(t.nodeIDs, nodeID)

	sort.Slice(t.nodeIDs,
		func(i, j int) bool {
			return t.nodeIDs[i] < t.nodeIDs[j]
		})

	return true
}

// removeID removes the node ID from the topology and returns true if removed.
func (t *Topology) removeID(nodeID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	i := t.positionByID(nodeID)
	if i < 0 {
		return false
	}

	copy(t.nodeIDs[i:], t.nodeIDs[i+1:])
	t.nodeIDs[len(t.nodeIDs)-1] = ""
	t.nodeIDs = t.nodeIDs[:len(t.nodeIDs)-1]

	return true
}

// encode converts t into its internal representation.
func (t *Topology) encode() *internal.Topology {
	return encodeTopology(t)
}

// loadTopology reads the topology for the node. unprotected.
func (c *cluster) loadTopology() error {
	buf, err := ioutil.ReadFile(filepath.Join(c.Path, ".topology"))
	if os.IsNotExist(err) {
		c.Topology = newTopology()
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

// saveTopology writes the current topology to disk. unprotected.
func (c *cluster) saveTopology() error {

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

func (c *cluster) considerTopology() error {
	// Create ClusterID if one does not already exist.
	if c.id == "" {
		u := uuid.NewV4()
		c.id = u.String()
		c.Topology.clusterID = c.id
	}

	if c.Static {
		return nil
	}

	// If there is no .topology file, it's safe to proceed.
	if len(c.Topology.nodeIDs) == 0 {
		return nil
	}

	// The local node (coordinator) must be in the .topology.
	if !c.Topology.ContainsID(c.Node.ID) {
		return fmt.Errorf("coordinator %s is not in topology: %v", c.Node.ID, c.Topology.nodeIDs)
	}

	// Keep the cluster in state "STARTING" until hearing from all nodes.
	// Topology contains 2+ hosts.
	return nil
}

// ReceiveEvent represents an implementation of EventHandler.
func (c *cluster) ReceiveEvent(e *NodeEvent) (err error) {
	// Ignore events sent from this node.
	if e.Node.ID == c.Node.ID {
		return nil
	}

	switch e.Event {
	case NodeJoin:
		c.logger.Printf("nodeJoin of %s on %s", e.Node.URI, c.Node.URI)
		// Ignore the event if this is not the coordinator.
		if !c.isCoordinator() {
			return nil
		}
		return c.nodeJoin(e.Node)
	case NodeLeave:
		c.logger.Printf("received node leave on %s: %s, uri: %v", c.Node, e.Node, e.Node.URI)
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.unprotectedIsCoordinator() {
			// if removeNodeBasicSorted succeeds, that means that the node was
			// not already removed by a removeNode request. We treat this as the
			// host being temporarily unavailable, and expect it to come back
			// up.
			if c.removeNodeBasicSorted(e.Node.ID) {
				c.Topology.nodeStates[e.Node.ID] = nodeStateDown
				// put the cluster into STARTING if we've lost a number of nodes
				// equal to or greater than ReplicaN
				err = c.unprotectedSetStateAndBroadcast(c.determineClusterState())
			}
		}
		c.logger.Printf("finished node leave on %s: %s, uri: %v", c.Node, e.Node, e.Node.URI)
	case NodeUpdate:
		c.logger.Printf("received node update event: id: %v, string: %v, uri: %v", e.Node.ID, e.Node.String(), e.Node.URI)
		// NodeUpdate is intentionally not implemented.
	}

	return err
}

// nodeJoin should only be called by the coordinator.
func (c *cluster) nodeJoin(node *Node) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger.Printf("NodeJoin event on coordinator, node: %s, id: %s", node.URI, node.ID)
	if c.needTopologyAgreement() {
		// A host that is not part of the topology can't be added to the STARTING cluster.
		if !c.Topology.ContainsID(node.ID) {
			err := fmt.Sprintf("host is not in topology: %s", node.ID)
			c.logger.Printf("%v", err)
			return errors.New(err)
		}

		if err := c.addNode(node); err != nil {
			return errors.Wrap(err, "adding node for agreement")
		}

		// Only change to normal if there is no existing data. Otherwise,
		// the coordinator needs to wait to receive READY messages (nodeStates)
		// from remote nodes before setting the cluster to state NORMAL.
		if ok, err := c.holder.HasData(); !ok && err == nil {
			// If the result of the previous AddNode completed the joining of nodes
			// in the topology, then change the state to NORMAL.
			if c.haveTopologyAgreement() {
				return c.unprotectedSetStateAndBroadcast(ClusterStateNormal)
			}
			return nil
		} else if err != nil {
			return errors.Wrap(err, "checking if holder has data")
		}

		if c.haveTopologyAgreement() && c.allNodesReady() {
			return c.unprotectedSetStateAndBroadcast(ClusterStateNormal)
		}
		// Send the status to the remote node. This lets the remote node
		// know that it can proceed with opening its Holder.
		return c.sendTo(node, c.unprotectedStatus())
	}

	// If the cluster already contains the node, just send it the cluster status.
	// This is useful in the case where a node is restarted or temporarily leaves
	// the cluster.
	if cnode := c.unprotectedNodeByID(node.ID); cnode != nil {
		if cnode.URI != node.URI {
			c.logger.Printf("Node: %v changed URI from %s to %s", cnode.ID, cnode.URI, node.URI)
			cnode.URI = node.URI
		}
		return c.unprotectedSetStateAndBroadcast(c.determineClusterState())
	}

	// If the holder does not yet contain data, go ahead and add the node.
	if ok, err := c.holder.HasData(); !ok && err == nil {
		if err := c.addNode(node); err != nil {
			return errors.Wrap(err, "adding node")
		}
		return c.unprotectedSetStateAndBroadcast(ClusterStateNormal)
	} else if err != nil {
		return errors.Wrap(err, "checking if holder has data2")
	}

	// If the cluster has data, we need to change to RESIZING and
	// kick off the resizing process.
	if err := c.unprotectedSetStateAndBroadcast(ClusterStateResizing); err != nil {
		return errors.Wrap(err, "broadcasting state")
	}
	c.joiningLeavingNodes <- nodeAction{node, resizeJobActionAdd}

	return nil
}

// nodeLeave initiates the removal of a node from the cluster.
func (c *cluster) nodeLeave(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Refuse the request if this is not the coordinator.
	if !c.unprotectedIsCoordinator() {
		return fmt.Errorf("node removal requests are only valid on the coordinator node: %s",
			c.unprotectedCoordinatorNode().ID)
	}

	if c.state != ClusterStateNormal && c.state != ClusterStateDegraded {
		return fmt.Errorf("Cluster must be in state %s to remove a node. Current state: %s",
			ClusterStateNormal, c.state)
	}

	// Ensure that node is in the cluster.
	if !c.topologyContainsNode(nodeID) {
		return fmt.Errorf("Node is not a member of the cluster: %s", nodeID)
	}

	// Prevent removing the coordinator node (this node).
	if nodeID == c.Node.ID {
		return fmt.Errorf("coordinator cannot be removed; first, make a different node the new coordinator.")
	}

	// See if resize job can be generated
	if _, err := c.unprotectedGenerateResizeJobByAction(
		nodeAction{
			node:   &Node{ID: nodeID},
			action: resizeJobActionRemove},
	); err != nil {
		return errors.Wrap(err, "generating job")
	}

	// If the holder does not yet contain data, go ahead and remove the node.
	if ok, err := c.holder.HasData(); !ok && err == nil {
		if err := c.removeNode(nodeID); err != nil {
			return errors.Wrap(err, "removing node")
		}
		return c.unprotectedSetStateAndBroadcast(c.determineClusterState())
	} else if err != nil {
		return errors.Wrap(err, "checking if holder has data")
	}

	// If the cluster has data then change state to RESIZING and
	// kick off the resizing process.
	if err := c.unprotectedSetStateAndBroadcast(ClusterStateResizing); err != nil {
		return errors.Wrap(err, "broadcasting state")
	}
	c.joiningLeavingNodes <- nodeAction{node: &Node{ID: nodeID}, action: resizeJobActionRemove}

	return nil
}

func (c *cluster) mergeClusterStatus(cs *ClusterStatus) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger.Printf("merge cluster status: %v", cs)
	// Ignore status updates from self (coordinator).
	if c.unprotectedIsCoordinator() {
		return nil
	}

	// Set ClusterID.
	c.unprotectedSetID(cs.ClusterID)

	officialNodes := cs.Nodes

	// Add all nodes from the coordinator.
	for _, node := range officialNodes {
		if node.ID == c.Node.ID && node.State != c.Node.State {
			c.logger.Printf("mismatched state in mergeClusterStatus got %v have %v", node.State, c.Node.State)
			go c.setNodeState(c.Node.State)
		}
		if err := c.addNode(node); err != nil {
			return errors.Wrap(err, "adding node")
		}
	}

	// Remove any nodes not specified by the coordinator
	// except for self. Generate a list to remove first
	// so that nodes aren't removed mid-loop.
	nodeIDsToRemove := []string{}
	for _, node := range c.nodes {
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
		if err := c.removeNode(nodeID); err != nil {
			return errors.Wrap(err, "removing node")
		}
	}

	// If the cluster membership has changed, reset the primary for
	// translate store replication.
	c.holder.setPrimaryTranslateStore(c.unprotectedPreviousNode())

	c.unprotectedSetState(cs.State)

	c.markAsJoined()

	return nil
}

// unprotectedPreviousNode returns the node listed before the current node in c.Nodes.
// If there is only one node in the cluster, returns nil.
// If the current node is the first node in the list, returns the last node.
func (c *cluster) unprotectedPreviousNode() *Node {
	if len(c.nodes) <= 1 {
		return nil
	}

	pos := c.nodePositionByID(c.Node.ID)
	if pos == -1 {
		return nil
	} else if pos == 0 {
		return c.nodes[len(c.nodes)-1]
	} else {
		return c.nodes[pos-1]
	}
}

// setStatic is unprotected, but only called before the cluster has been started
// (and therefore not concurrently).
func (c *cluster) setStatic(hosts []string) error {
	c.Static = true
	c.Coordinator = c.Node.ID
	for _, address := range hosts {
		uri, err := NewURIFromAddress(address)
		if err != nil {
			return errors.Wrap(err, "getting URI")
		}
		c.nodes = append(c.nodes, &Node{URI: *uri})
	}
	return nil
}

type ClusterStatus struct {
	ClusterID string
	State     string
	Nodes     []*Node
}

type ResizeInstruction struct {
	JobID         int64
	Node          *Node
	Coordinator   *Node
	Sources       []*ResizeSource
	Schema        *Schema
	ClusterStatus *ClusterStatus
}

type ResizeSource struct {
	Node  *Node  `protobuf:"bytes,1,opt,name=Node" json:"Node,omitempty"`
	Index string `protobuf:"bytes,2,opt,name=Index,proto3" json:"Index,omitempty"`
	Field string `protobuf:"bytes,3,opt,name=Field,proto3" json:"Field,omitempty"`
	View  string `protobuf:"bytes,4,opt,name=View,proto3" json:"View,omitempty"`
	Shard uint64 `protobuf:"varint,5,opt,name=Shard,proto3" json:"Shard,omitempty"`
}

// Schema contains information about indexes and their configuration.
type Schema struct {
	Indexes []*IndexInfo
}

func encodeTopology(topology *Topology) *internal.Topology {
	if topology == nil {
		return nil
	}
	return &internal.Topology{
		ClusterID: topology.clusterID,
		NodeIDs:   topology.nodeIDs,
	}
}

func decodeTopology(topology *internal.Topology) (*Topology, error) {
	if topology == nil {
		return nil, nil
	}

	t := newTopology()
	t.clusterID = topology.ClusterID
	t.nodeIDs = topology.NodeIDs
	sort.Slice(t.nodeIDs,
		func(i, j int) bool {
			return t.nodeIDs[i] < t.nodeIDs[j]
		})

	return t, nil
}

type CreateShardMessage struct {
	Index string
	Field string
	Shard uint64
}

type CreateIndexMessage struct {
	Index string
	Meta  *IndexOptions
}

type DeleteIndexMessage struct {
	Index string
}

type CreateFieldMessage struct {
	Index string
	Field string
	Meta  *FieldOptions
}

type DeleteFieldMessage struct {
	Index string
	Field string
}

type DeleteAvailableShardMessage struct {
	Index   string
	Field   string
	ShardID uint64
}

type CreateViewMessage struct {
	Index string
	Field string
	View  string
}
type DeleteViewMessage struct {
	Index string
	Field string
	View  string
}

type ResizeInstructionComplete struct {
	JobID int64
	Node  *Node
	Error string
}

type SetCoordinatorMessage struct {
	New *Node
}

type UpdateCoordinatorMessage struct {
	New *Node
}

type NodeStateMessage struct {
	NodeID string `protobuf:"bytes,1,opt,name=NodeID,proto3" json:"NodeID,omitempty"`
	State  string `protobuf:"bytes,2,opt,name=State,proto3" json:"State,omitempty"`
}

type NodeStatus struct {
	Node    *Node
	Indexes []*IndexStatus
	Schema  *Schema
}

type IndexStatus struct {
	Name   string
	Fields []*FieldStatus
}

type FieldStatus struct {
	Name            string
	AvailableShards *roaring.Bitmap
}

type RecalculateCaches struct{}
