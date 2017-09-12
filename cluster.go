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
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

const (
	// DefaultPartitionN is the default number of partitions in a cluster.
	DefaultPartitionN = 256

	// DefaultReplicaN is the default number of replicas per partition.
	DefaultReplicaN = 1

	// NodeState represents node state returned in /status endpoint for a node in the cluster.
	NodeStateStarting = "STARTING"
	NodeStateNormal   = "NORMAL"
	NodeStateResizing = "RESIZING"

	// ResizeJob states.
	ResizeJobStateRunning = "RUNNING"
	// Final states.
	ResizeJobStateDone    = "DONE"
	ResizeJobStateAborted = "ABORTED"
)

// Node represents a node in the cluster.
type Node struct {
	Scheme string `json:"scheme"`
	Host   string `json:"host"` // HostPort

	status *internal.NodeStatus `json:"status"`
}

// SetStatus sets the NodeStatus.
func (n *Node) SetStatus(s *internal.NodeStatus) {
	n.status = s
}

// SetState sets the Node.status.state.
func (n *Node) SetState(s string) {
	if n.status == nil {
		n.status = &internal.NodeStatus{}
	}
	n.status.State = s
}

// URI returns the pilosa.URI corresponding to this node
func (n *Node) URI() (*URI, error) {
	uri, err := NewURIFromAddress(n.Host)
	if err != nil {
		return nil, err
	}
	uri.SetScheme(n.Scheme)
	return uri, nil
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

// ByHost implements sort.Interface for []Node based on
// the Host field.
type ByHost []*Node

func (h ByHost) Len() int           { return len(h) }
func (h ByHost) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h ByHost) Less(i, j int) bool { return h[i].Host < h[j].Host }

// Cluster represents a collection of nodes.
type Cluster struct {
	URI     *URI
	Nodes   []*Node // TODO phase this out?
	NodeSet NodeSet

	// Hashing algorithm used to assign partitions to nodes.
	Hasher Hasher

	// The number of partitions in the cluster.
	PartitionN int

	// The number of replicas a partition has.
	ReplicaN int

	// Threshold for logging long-running queries
	LongQueryTime time.Duration

	// EventReceiver receives NodeEvents pertaining to node membership.
	EventReceiver EventReceiver

	// Data directory path.
	Path     string
	Topology *Topology

	// Required for cluster Resize.
	State         string
	Coordinator   string
	IndexReporter IndexReporter
	Broadcaster   Broadcaster

	joiningHosts chan string

	mu         sync.RWMutex
	jobs       map[int64]*ResizeJob
	currentJob *ResizeJob

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	// The writer for any logging.
	LogOutput io.Writer
}

// NewCluster returns a new instance of Cluster with defaults.
func NewCluster() *Cluster {
	return &Cluster{
		Hasher:        &jmphasher{},
		PartitionN:    DefaultPartitionN,
		ReplicaN:      DefaultReplicaN,
		EventReceiver: NopEventReceiver,

		joiningHosts: make(chan string, 10), // buffered channel
		jobs:         make(map[int64]*ResizeJob),
		closing:      make(chan struct{}),

		LogOutput: os.Stderr,
	}
}

// logger returns a logger for the cluster.
func (c *Cluster) logger() *log.Logger {
	return log.New(c.LogOutput, "", log.LstdFlags)
}

// IsCoordinator is true if this node is the coordinator.
func (c *Cluster) IsCoordinator() bool {
	return c.Coordinator == c.URI.HostPort()
}

// AddHost adds a node to the Cluster and updates and saves the
// new topology.
func (c *Cluster) AddHost(host string) error {

	// add to cluster
	_, added := c.AddNode(host)
	if !added {
		return nil
	}

	// add to topology
	if c.Topology == nil {
		return fmt.Errorf("Cluster.Topology is nil")
	}
	if !c.Topology.AddHost(host) {
		return nil
	}

	// save topology
	return c.saveTopology()
}

// HostList returns the list of hosts in the cluster.
func (c *Cluster) HostList() []string {
	return Nodes(c.Nodes).Hosts()
}

func (c *Cluster) setState(state string) {
	c.State = state
	localNode := c.localNode()
	localNode.SetState(state)
}

func (c *Cluster) localNode() *Node {
	return c.NodeByHost(c.URI.HostPort())
}

// Status returns the internal ClusterStatus representation.
func (c *Cluster) Status() *internal.ClusterStatus {
	return &internal.ClusterStatus{
		State:    c.State,
		HostList: c.HostList(),
		//NodeStatuses: encodeNodeStatuses(c.Nodes), // TODO travis: remove this?
	}
}

/*
// encodeNodeStatuses converts a into its internal representation.
func encodeNodeStatuses(a []*Node) []*internal.NodeStatus {
	other := make([]*internal.NodeStatus, len(a))
	for i := range a {
		other[i] = a[i].status
	}
	return other
}
*/

// NodeByHost returns a node reference by host.
func (c *Cluster) NodeByHost(host string) *Node {
	for _, n := range c.Nodes {
		if n.Host == host {
			return n
		}
	}
	return nil
}

// AddNode adds a node to the cluster, sorted by host.
// Returns a pointer to the node and true if the node was added.
func (c *Cluster) AddNode(host string) (*Node, bool) {
	n := c.NodeByHost(host)
	if n != nil {
		return n, false
	}

	n = &Node{Host: host}
	c.Nodes = append(c.Nodes, n)

	// All hosts must be merged in the same order on all nodes in the cluster.
	sort.Sort(ByHost(c.Nodes))

	return n, true
}

// frag is a struct of basic fragment information.
type frag struct {
	frame string
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
		for _, vv := range v {
			a[k] = append(a[k], vv)
		}
	}
	return a
}

type viewsByFrame map[string][]string

func (a viewsByFrame) addView(frame, view string) {
	a[frame] = append(a[frame], view)
}

func (c *Cluster) fragsByHost(idx *Index) fragsByHost {
	// frameViews is a map of frame to slice of views.
	frameViews := make(viewsByFrame)
	inverseFrameViews := make(viewsByFrame)

	for _, frame := range idx.Frames() {
		for _, view := range frame.Views() {
			if IsInverseView(view.Name()) {
				inverseFrameViews.addView(frame.Name(), view.Name())
			} else {
				frameViews.addView(frame.Name(), view.Name())
			}
		}
	}

	std := c.fragCombos(idx.Name(), idx.MaxSlice(), frameViews)
	inv := c.fragCombos(idx.Name(), idx.MaxInverseSlice(), inverseFrameViews)
	return std.add(inv)
}

// fragCombos returns a map (by host) of lists of fragments for a given index
// by creating every combination of frame/view specified in `frameViews` up to maxSlice.
func (c *Cluster) fragCombos(idx string, maxSlice uint64, frameViews viewsByFrame) fragsByHost {
	t := make(fragsByHost)
	for i := uint64(0); i <= maxSlice; i++ {
		f := c.FragmentNodes(idx, i)
		for _, n := range f {
			// for each frame/view combination:
			for frame, views := range frameViews {
				for _, view := range views {
					t[n.Host] = append(t[n.Host], frag{frame, view, i})
				}
			}
		}
	}
	return t
}

// DataDiff returns a list of ResizeSources - for each host in the `to` cluster -
// required to move from cluster `c` to cluster `to`.
func (c *Cluster) DataDiff(to *Cluster, idx *Index) map[string][]*internal.ResizeSource {
	m := make(map[string][]*internal.ResizeSource)

	// Initialize the map with all the nodes in `to`.
	for _, n := range to.Nodes {
		m[n.Host] = nil
	}

	// For now, we want our source to be confined to the primary fragment
	// (i.e. don't use replicas as source data). So if it's not already,
	// base our source fragments on a cluster with replica = 1.
	srcCluster := c
	if c.ReplicaN > 1 {
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

	// srcHostsByFrag is the inverse representation of srcFrags.
	srcHostsByFrag := make(map[frag]string)
	for host, frags := range srcFrags {
		for _, frag := range frags {
			srcHostsByFrag[frag] = host
		}
	}

	// Get the frag diff for each host.
	diffs := make(fragsByHost)
	for host, frags := range tFrags {
		if _, ok := fFrags[host]; ok {
			diffs[host] = fragsDiff(frags, fFrags[host])
		} else {
			diffs[host] = frags
		}
	}

	// Get the ResizeSource for each diff.
	for host, diff := range diffs {
		m[host] = []*internal.ResizeSource{}
		for _, frag := range diff {
			src := &internal.ResizeSource{
				Host:  srcHostsByFrag[frag],
				Index: idx.Name(),
				Frame: frag.frame,
				View:  frag.view,
				Slice: frag.slice,
			}
			m[host] = append(m[host], src)
		}
	}

	return m
}

// Partition returns the partition that a slice belongs to.
func (c *Cluster) Partition(index string, slice uint64) int {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], slice)

	// Hash the bytes and mod by partition count.
	h := fnv.New64a()
	h.Write([]byte(index))
	h.Write(buf[:])
	return int(h.Sum64() % uint64(c.PartitionN))
}

// FragmentNodes returns a list of nodes that own a fragment.
func (c *Cluster) FragmentNodes(index string, slice uint64) []*Node {
	return c.PartitionNodes(c.Partition(index, slice))
}

// OwnsFragment returns true if a host owns a fragment.
func (c *Cluster) OwnsFragment(host string, index string, slice uint64) bool {
	return Nodes(c.FragmentNodes(index, slice)).ContainsHost(host)
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
	nodeIndex := c.Hasher.Hash(uint64(partitionID), len(c.Nodes))

	// Collect nodes around the ring.
	nodes := make([]*Node, replicaN)
	for i := 0; i < replicaN; i++ {
		nodes[i] = c.Nodes[(nodeIndex+i)%len(c.Nodes)]
	}

	return nodes
}

// OwnsSlices find the set of slices owned by the node per Index
func (c *Cluster) OwnsSlices(index string, maxSlice uint64, host string) []uint64 {
	var slices []uint64
	for i := uint64(0); i <= maxSlice; i++ {
		p := c.Partition(index, i)
		// Determine primary owner node.
		nodeIndex := c.Hasher.Hash(uint64(p), len(c.Nodes))
		if c.Nodes[nodeIndex].Host == host {
			slices = append(slices, i)
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

func (c *Cluster) Open() error {
	// Cluster always comes up in state STARTING until cluster membership is determined.
	c.State = NodeStateStarting

	// Load topology file if it exists.
	if err := c.loadTopology(); err != nil {
		return fmt.Errorf("load topology: %v", err)
	}

	// Only the coordinator needs to consider the .topology file.
	if c.IsCoordinator() {
		state, err := c.considerTopology()
		if err != nil {
			return fmt.Errorf("considerTopology: %v", err)
		}
		// Add the local node to the cluster and update state.
		c.AddHost(c.URI.HostPort())
		c.setState(state)
	} else {
		// Add the local node to the cluster.
		c.AddHost(c.URI.HostPort())
	}

	// Start the EventReceiver.
	if err := c.EventReceiver.Start(c); err != nil {
		return fmt.Errorf("starting EventReceiver: %v", err)
	}

	// Open NodeSet communication.
	if err := c.NodeSet.Open(); err != nil {
		return fmt.Errorf("opening NodeSet: %v", err)
	}

	// Listen for cluster-resize events.
	c.wg.Add(1)
	go func() { defer c.wg.Done(); c.listenForJoins() }()

	return nil
}

func (c *Cluster) Close() error {
	// Notify goroutines of closing and wait for completion.
	close(c.closing)
	c.wg.Wait()

	return nil
}

func (c *Cluster) needTopologyAgreement() bool {
	return c.State == NodeStateStarting && !SlicesAreEqual(c.Topology.HostList, Nodes(c.Nodes).Hosts())
}

func (c *Cluster) haveTopologyAgreement() bool {
	return SlicesAreEqual(c.Topology.HostList, Nodes(c.Nodes).Hosts())
}

func (c *Cluster) handleJoiningHost(host string) error {
	j, err := c.GenerateResizeJob(host)
	if err != nil {
		return err
	}

	// Run the job.
	err = j.Run()
	if err != nil {
		return err
	}

	// Wait for the ResizeJob to finish or be aborted.
	jobResult := <-j.result
	switch jobResult {
	case ResizeJobStateDone:
		c.CompleteCurrentJob(ResizeJobStateDone)
		// Add host to the cluster.
		return c.AddHost(host)
	case ResizeJobStateAborted:
		c.CompleteCurrentJob(ResizeJobStateAborted)
	}
	return nil
}

func (c *Cluster) setStateAndBroadcast(state string) error {
	c.setState(state)
	// Broadcast status changes to the cluster.
	return c.Broadcaster.SendSync(c.Status())
}

func (c *Cluster) listenForJoins() {
	var hostJoined bool

	for {

		// Handle all pending joins before changing state back to NORMAL.
		select {
		case host := <-c.joiningHosts:
			err := c.handleJoiningHost(host)
			if err != nil {
				c.logger().Printf("handleJoiningHost error: err=%s", err)
				continue
			}
			hostJoined = true
			continue
		default:
		}

		// Only change state to NORMAL if we have successfully added at least one host.
		if hostJoined {
			// Put the cluster back to state NORMAL and broadcast.
			if err := c.setStateAndBroadcast(NodeStateNormal); err != nil {
				c.logger().Printf("setStateAndBroadcast error: err=%s", err)
			}
		}

		// Wait for a joining host or a close.
		select {
		case <-c.closing:
			return
		case host := <-c.joiningHosts:
			err := c.handleJoiningHost(host)
			if err != nil {
				c.logger().Printf("handleJoiningHost error: err=%s", err)
				continue
			}
			hostJoined = true
			continue
		}
	}
}

// GenerateResizeJob creates a new ResizeJob based on the new host being
// added. It also saves a reference to the ResizeJob in the `jobs` map
// for future lookup by JobID.
func (c *Cluster) GenerateResizeJob(addHost string) (*ResizeJob, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	j := c.generateResizeJob(addHost)

	// Save job in jobs map for future reference.
	c.jobs[j.ID] = j

	// Set job as currentJob.
	if c.currentJob != nil {
		return nil, fmt.Errorf("there is currently a resize job running")
	}
	c.currentJob = j

	return j, nil
}

// generateResizeJob returns a ResizeJob with instructions based on
// the difference between Cluster and a new Cluster containing addHost.
// Broadcaster is associated to the ResizeJob here for use in broadcasting
// the resize instructions to other nodes in the cluster.
func (c *Cluster) generateResizeJob(addHost string) *ResizeJob {

	j := NewResizeJob(addHost, Nodes(c.Nodes).Hosts())
	j.Broadcaster = c.Broadcaster

	// toCluster is a clone of Cluster with the new node added for comparison.
	toCluster := NewCluster()
	toCluster.Nodes = Nodes(c.Nodes).Clone()
	toCluster.Hasher = c.Hasher
	toCluster.PartitionN = c.PartitionN
	toCluster.ReplicaN = c.ReplicaN
	toCluster.AddNode(addHost)

	// Add to the ResizeJob the instructions for each index.
	for _, idx := range c.IndexReporter.Indexes() {
		// dataDiff is map[string][]*internal.ResizeSource, where string is
		// a host in toCluster.
		dataDiff := c.DataDiff(toCluster, idx)

		for host, sources := range dataDiff {
			// If a host doesn't need to request data, mark it as complete.
			if len(sources) == 0 {
				j.Hosts[host] = true
				continue
			}
			instr := &internal.ResizeInstruction{
				JobID:       j.ID,
				Host:        host,
				Coordinator: c.Coordinator,
				Sources:     sources,
			}
			j.Instructions = append(j.Instructions, instr)
		}
	}

	return j
}

// CompleteCurrentJob sets the state of the current ResizeJob
// then removes the pointer to currentJob.
func (c *Cluster) CompleteCurrentJob(state string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentJob == nil {
		return
	}
	c.currentJob.SetState(state)
	c.currentJob = nil
}

// followResizeInstruction is run by any node that receives a ResizeInstruction.
func (c *Cluster) followResizeInstruction(instr *internal.ResizeInstruction) {
	go func() {
		// Request each source file in ResizeSources.
		for _, src := range instr.Sources {
			/************************************************************/
			// TODO travis: get the data files from other nodes.
			fmt.Printf("\n**** Get slice %d for index %s from host %s ****\n\n", src.Slice, src.Index, src.Host)
			for i := 0; i <= 4; i++ {
				fmt.Printf(" %d", i)
				time.Sleep(1 * time.Second)
			}
			fmt.Println("")
			/************************************************************/
		}

		complete := &internal.ResizeInstructionComplete{
			JobID: instr.JobID,
			Host:  instr.Host,
		}

		node := &Node{
			Host: instr.Coordinator,
		}
		if err := c.Broadcaster.SendTo(node, complete); err != nil {
			c.logger().Printf("sending resizeInstructionComplete error: err=%s", err)
		}
	}()
}

func (c *Cluster) MarkResizeInstructionComplete(complete *internal.ResizeInstructionComplete) error {
	j := c.Job(complete.JobID)

	j.mu.Lock()
	defer j.mu.Unlock()

	if j.isComplete() {
		return fmt.Errorf("ResizeJob %d is no longer running", j.ID)
	}

	// Mark host complete.
	j.Hosts[complete.Host] = true

	if !j.hostsArePending() {
		j.result <- ResizeJobStateDone
	}

	return nil
}

// Job returns a ResizeJob by id.
func (c *Cluster) Job(id int64) *ResizeJob {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.job(id)
}

func (c *Cluster) job(id int64) *ResizeJob { return c.jobs[id] }

type ResizeJob struct {
	ID           int64
	Hosts        map[string]bool
	Instructions []*internal.ResizeInstruction
	Broadcaster  Broadcaster

	result chan string

	mu    sync.RWMutex
	state string
}

// NewResizeJob returns a new instance of ResizeJob.
func NewResizeJob(addHost string, existingHosts []string) *ResizeJob {

	// Build a map of hosts to track their resize status.
	hosts := make(map[string]bool)

	// The value for a node will be set to true after that node
	// has indicated that it has completed all resize instructions.
	for _, h := range existingHosts {
		hosts[h] = false
	}
	// Include the added node in the map for tracking.
	hosts[addHost] = false

	return &ResizeJob{
		ID:     rand.Int63(),
		Hosts:  hosts,
		result: make(chan string),
	}
}

func (j *ResizeJob) State() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.state
}

func (j *ResizeJob) SetState(state string) {
	j.mu.Lock()
	j.setState(state)
	j.mu.Unlock()
}

func (j *ResizeJob) setState(state string) {
	if j.state == "" || j.state == ResizeJobStateRunning {
		j.state = state
	}
}

// Run distributes ResizeInstructions.
func (j *ResizeJob) Run() error {
	j.mu.RLock()
	defer j.mu.RUnlock()

	// Set job state to RUNNING.
	j.setState(ResizeJobStateRunning)

	// Job can be considered done in the case where it doesn't require any action.
	if !j.hostsArePending() {
		j.result <- ResizeJobStateDone
		return nil
	}

	err := j.distributeResizeInstructions()
	if err != nil {
		j.result <- ResizeJobStateAborted
		return err
	}
	return nil
}

// isComplete return true if the job is any one of several completion states.
func (j *ResizeJob) isComplete() bool {
	switch j.state {
	case ResizeJobStateDone, ResizeJobStateAborted:
		return true
	default:
		return false
	}
}

// hostsArePending returns true if any host is still working on the resize.
func (j *ResizeJob) hostsArePending() bool {
	for _, complete := range j.Hosts {
		if !complete {
			return true
		}
	}
	return false
}

func (j *ResizeJob) distributeResizeInstructions() error {
	// Loop through the ResizeInstructions in ResizeJob and send to each host.
	for _, instr := range j.Instructions {
		// Because the node may not be in the cluster yet, create
		// a dummy node object to use in the SendTo() method.
		node := &Node{
			Host: instr.Host,
		}
		if err := j.Broadcaster.SendTo(node, instr); err != nil {
			return err
		}
	}
	return nil
}

// Topology represents the list of hosts in the cluster.
type Topology struct {
	mu       sync.RWMutex
	HostList []string
}

func NewTopology() *Topology {
	return &Topology{}
}

// ContainsHost returns true if host matches one of the topology's hosts.
func (t *Topology) ContainsHost(host string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.containsHost(host)
}

func (t *Topology) containsHost(host string) bool {
	for _, thost := range t.HostList {
		if thost == host {
			return true
		}
	}
	return false
}

// AddHost adds the host to the topology and returns true if added.
func (t *Topology) AddHost(host string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.containsHost(host) {
		return false
	}
	t.HostList = append(t.HostList, host)
	return true
}

// loadTopology reads the topology for the node.
func (c *Cluster) loadTopology() error {
	buf, err := ioutil.ReadFile(filepath.Join(c.Path, ".topology"))
	if os.IsNotExist(err) {
		c.Topology = NewTopology()
		return nil
	} else if err != nil {
		return err
	}

	var pb internal.Topology
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	c.Topology = decodeTopology(&pb)

	return nil
}

// saveTopology writes the current topology to disk.
func (c *Cluster) saveTopology() error {
	if buf, err := proto.Marshal(encodeTopology(c.Topology)); err != nil {
		return err
	} else if err := ioutil.WriteFile(filepath.Join(c.Path, ".topology"), buf, 0666); err != nil {
		return err
	}
	return nil
}

func encodeTopology(topology *Topology) *internal.Topology {
	if topology == nil {
		return nil
	}
	return &internal.Topology{
		HostList: topology.HostList,
	}
}

func decodeTopology(topology *internal.Topology) *Topology {
	if topology == nil {
		return nil
	}
	t := &Topology{
		HostList: topology.HostList,
	}
	return t
}

func (c *Cluster) considerTopology() (string, error) {
	// If there is no .topology file, it's safe to go to state NORMAL.
	if len(c.Topology.HostList) == 0 {
		return NodeStateNormal, nil
	}

	// The local node (coordinator) must be in the .topology.
	if !c.Topology.ContainsHost(c.Coordinator) {
		return "", fmt.Errorf("coordinator %s is not in topology: %v", c.Coordinator, c.Topology.HostList)
	}

	// If local node is the only thing in .topology, continue to state NORMAL.
	if len(c.Topology.HostList) == 1 {
		return NodeStateNormal, nil
	}

	// Keep the cluster in state "STARTING" until hearing from all nodes.
	// Topology contains 2+ hosts.
	return NodeStateStarting, nil
}

// ReceiveEvent represents an implementation of EventHandler.
func (c *Cluster) ReceiveEvent(e *NodeEvent) error {
	// Ignore events sent from this node.
	if e.Host == c.URI.HostPort() {
		return nil
	}

	switch e.Event {
	case NodeJoin:
		// Ignore the event if this is not the coordinator.
		if !c.IsCoordinator() {
			return nil
		}

		if c.needTopologyAgreement() {
			// A host that is not part of the topology can't be added to the STARTING cluster.
			if !c.Topology.ContainsHost(e.Host) {
				return fmt.Errorf("host is not in topology: %v", e.Host)
			}

			if err := c.AddHost(e.Host); err != nil {
				return err
			}

			// If the result of the previous AddHost completed the joining of nodes
			// in the topology, then change the state to NORMAL.
			if c.haveTopologyAgreement() {
				return c.setStateAndBroadcast(NodeStateNormal)
			}

			return nil
		}

		// Don't do anything else if the cluster already contains the node.
		if c.NodeByHost(e.Host) != nil {
			return nil
		}

		// If the index does not yet have data, go ahead and add the node.
		if !c.IndexReporter.HasData() {
			if err := c.AddHost(e.Host); err != nil {
				return err
			}
			return c.setStateAndBroadcast(NodeStateNormal)
		}

		// If the cluster has data, we need to change to RESIZING and
		// kick off the resizing process.
		if err := c.setStateAndBroadcast(NodeStateResizing); err != nil {
			return err
		}
		c.joiningHosts <- e.Host

	case NodeLeave:
		// TODO: implement this
	case NodeUpdate:
		// TODO: implement this
	}

	return nil
}

func (c *Cluster) mergeClusterStatus(cs *internal.ClusterStatus) error {
	// Ignore status updates from self (coordinator).
	if c.IsCoordinator() {
		return nil
	}

	for _, host := range cs.HostList {
		c.AddHost(host)
	}
	c.setState(cs.State)

	return nil
}
