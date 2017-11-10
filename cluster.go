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
	"errors"
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

	// ClusterState represents the state returned in the /status endpoint.
	ClusterStateStarting = "STARTING"
	ClusterStateNormal   = "NORMAL"
	ClusterStateResizing = "RESIZING"

	// ResizeJob states.
	ResizeJobStateRunning = "RUNNING"
	// Final states.
	ResizeJobStateDone    = "DONE"
	ResizeJobStateAborted = "ABORTED"
)

// Node represents a node in the cluster.
type Node struct {
	URI URI `json:"uri"`
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

// ContainsURI returns true if host matches one of the node's uri.
func (a Nodes) ContainsURI(uri URI) bool {
	for _, n := range a {
		if n.URI == uri {
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

// ByHost implements sort.Interface for []Node based on
// the Host field.
type ByHost []*Node

func (h ByHost) Len() int           { return len(h) }
func (h ByHost) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h ByHost) Less(i, j int) bool { return h[i].URI.String() < h[j].URI.String() }

// Cluster represents a collection of nodes.
type Cluster struct {
	URI       URI
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

	// EventReceiver receives NodeEvents pertaining to node membership.
	EventReceiver EventReceiver

	// Data directory path.
	Path     string
	Topology *Topology

	// Required for cluster Resize.
	State       string
	Coordinator URI
	Holder      *Holder
	Broadcaster Broadcaster

	joiningURIs chan URI

	mu         sync.RWMutex
	jobs       map[int64]*ResizeJob
	currentJob *ResizeJob

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}
	prefect SecurityManager

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

		joiningURIs: make(chan URI, 10), // buffered channel
		jobs:        make(map[int64]*ResizeJob),
		closing:     make(chan struct{}),

		LogOutput: os.Stderr,
		prefect:   &NopSecurityManager{},
	}
}

// logger returns a logger for the cluster.
func (c *Cluster) logger() *log.Logger {
	return log.New(c.LogOutput, "", log.LstdFlags)
}

// IsCoordinator is true if this node is the coordinator.
func (c *Cluster) IsCoordinator() bool {
	return c.Coordinator == c.URI
}

// AddNode adds a node to the Cluster and updates and saves the
// new topology.
func (c *Cluster) AddNode(uri URI) error {

	// add to cluster
	_, added := c.AddNodeBasicSorted(uri)
	if !added {
		return nil
	}

	// add to topology
	if c.Topology == nil {
		return fmt.Errorf("Cluster.Topology is nil")
	}
	if !c.Topology.AddURI(uri) {
		return nil
	}

	// save topology
	return c.saveTopology()
}

// NodeSet returns the list of uris in the cluster.
func (c *Cluster) NodeSet() []URI {
	return Nodes(c.Nodes).URIs()
}

func (c *Cluster) setState(state string) {
	// Ignore cases where the state hasn't changed.
	if state == c.State {
		return
	}

	switch state {
	case ClusterStateResizing:
		c.prefect.SetRestricted()
	case ClusterStateNormal:
		c.prefect.SetNormal()
		// Don't change routing for these states:
		// - ClusterStateStarting
	}

	c.State = state
}

// localNode is not being used.
//func (c *Cluster) localNode() *Node {
//	return c.NodeByURI(c.URI)
//}

// Status returns the internal ClusterStatus representation.
func (c *Cluster) Status() *internal.ClusterStatus {
	return &internal.ClusterStatus{
		State:   c.State,
		NodeSet: encodeURIs(c.NodeSet()),
	}
}

// NodeByURI returns a node reference by uri.
func (c *Cluster) NodeByURI(uri URI) *Node {
	for _, n := range c.Nodes {
		if n.URI == uri {
			return n
		}
	}
	return nil
}

// AddNodeBasicSorted adds a node to the cluster, sorted by uri.
// Returns a pointer to the node and true if the node was added.
func (c *Cluster) AddNodeBasicSorted(uri URI) (*Node, bool) {
	n := c.NodeByURI(uri)
	if n != nil {
		return n, false
	}

	n = &Node{URI: uri}
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

type fragsByHost map[URI][]frag

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

// fragCombos returns a map (by uri) of lists of fragments for a given index
// by creating every combination of frame/view specified in `frameViews` up to maxSlice.
func (c *Cluster) fragCombos(idx string, maxSlice uint64, frameViews viewsByFrame) fragsByHost {
	t := make(fragsByHost)
	for i := uint64(0); i <= maxSlice; i++ {
		f := c.FragmentNodes(idx, i)
		for _, n := range f {
			// for each frame/view combination:
			for frame, views := range frameViews {
				for _, view := range views {
					t[n.URI] = append(t[n.URI], frag{frame, view, i})
				}
			}
		}
	}
	return t
}

// DataDiff returns a list of ResizeSources - for each host in the `to` cluster -
// required to move from cluster `c` to cluster `to`.
func (c *Cluster) DataDiff(to *Cluster, idx *Index) map[URI][]*internal.ResizeSource {
	m := make(map[URI][]*internal.ResizeSource)

	// Initialize the map with all the nodes in `to`.
	for _, n := range to.Nodes {
		m[n.URI] = nil
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
	srcHostsByFrag := make(map[frag]URI)
	for uri, frags := range srcFrags {
		for _, frag := range frags {
			srcHostsByFrag[frag] = uri
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
				URI:   (srcHostsByFrag[frag]).Encode(),
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
func (c *Cluster) OwnsFragment(uri URI, index string, slice uint64) bool {
	return Nodes(c.FragmentNodes(index, slice)).ContainsURI(uri)
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
func (c *Cluster) OwnsSlices(index string, maxSlice uint64, uri URI) []uint64 {
	var slices []uint64
	for i := uint64(0); i <= maxSlice; i++ {
		p := c.Partition(index, i)
		// Determine primary owner node.
		nodeIndex := c.Hasher.Hash(uint64(p), len(c.Nodes))
		if c.Nodes[nodeIndex].URI == uri {
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
	c.State = ClusterStateStarting

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
		c.AddNode(c.URI)
		c.setState(state)
	} else {
		// Add the local node to the cluster.
		c.AddNode(c.URI)
	}

	// Start the EventReceiver.
	if err := c.EventReceiver.Start(c); err != nil {
		return fmt.Errorf("starting EventReceiver: %v", err)
	}

	// Open MemberSet communication.
	if err := c.MemberSet.Open(); err != nil {
		return fmt.Errorf("opening MemberSet: %v", err)
	}

	return nil
}

func (c *Cluster) Close() error {
	// Notify goroutines of closing and wait for completion.
	close(c.closing)
	c.wg.Wait()

	return nil
}

func (c *Cluster) needTopologyAgreement() bool {
	return c.State == ClusterStateStarting && !URISlicesAreEqual(c.Topology.NodeSet, c.NodeSet())
}

func (c *Cluster) haveTopologyAgreement() bool {
	return URISlicesAreEqual(c.Topology.NodeSet, c.NodeSet())
}

func (c *Cluster) handleJoiningHost(uri URI) error {
	j, err := c.GenerateResizeJob(uri)
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
		if err := c.CompleteCurrentJob(ResizeJobStateDone); err != nil {
			return err
		}
		// Add uri to the cluster.
		return c.AddNode(uri)
	case ResizeJobStateAborted:
		if err := c.CompleteCurrentJob(ResizeJobStateAborted); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) setStateAndBroadcast(state string) error {
	c.setState(state)
	// Broadcast cluster status changes to the cluster.
	return c.Broadcaster.SendSync(c.Status())
}

// ListenForJoins handles cluster-resize events.
func (c *Cluster) ListenForJoins() {
	c.wg.Add(1)
	go func() { defer c.wg.Done(); c.listenForJoins() }()
}

func (c *Cluster) listenForJoins() {
	var uriJoined bool

	for {

		// Handle all pending joins before changing state back to NORMAL.
		select {
		case uri := <-c.joiningURIs:
			err := c.handleJoiningHost(uri)
			if err != nil {
				c.logger().Printf("handleJoiningHost error: err=%s", err)
				continue
			}
			uriJoined = true
			continue
		default:
		}

		// Only change state to NORMAL if we have successfully added at least one host.
		if uriJoined {
			// Put the cluster back to state NORMAL and broadcast.
			if err := c.setStateAndBroadcast(ClusterStateNormal); err != nil {
				c.logger().Printf("setStateAndBroadcast error: err=%s", err)
			}
		}

		// Wait for a joining host or a close.
		select {
		case <-c.closing:
			return
		case uri := <-c.joiningURIs:
			err := c.handleJoiningHost(uri)
			if err != nil {
				c.logger().Printf("handleJoiningHost error: err=%s", err)
				continue
			}
			uriJoined = true
			continue
		}
	}
}

// GenerateResizeJob creates a new ResizeJob based on the new host being
// added. It also saves a reference to the ResizeJob in the `jobs` map
// for future lookup by JobID.
func (c *Cluster) GenerateResizeJob(addURI URI) (*ResizeJob, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	j := c.generateResizeJob(addURI)

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
func (c *Cluster) generateResizeJob(addURI URI) *ResizeJob {

	j := NewResizeJob(addURI, Nodes(c.Nodes).URIs())
	j.Broadcaster = c.Broadcaster

	// toCluster is a clone of Cluster with the new node added for comparison.
	toCluster := NewCluster()
	toCluster.Nodes = Nodes(c.Nodes).Clone()
	toCluster.Hasher = c.Hasher
	toCluster.PartitionN = c.PartitionN
	toCluster.ReplicaN = c.ReplicaN
	toCluster.AddNodeBasicSorted(addURI)

	pbSchema := c.Holder.EncodeSchema()

	// Add to the ResizeJob the instructions for each index.
	for _, idx := range c.Holder.Indexes() {
		// dataDiff is map[string][]*internal.ResizeSource, where string is
		// a host in toCluster.
		dataDiff := c.DataDiff(toCluster, idx)

		for uri, sources := range dataDiff {
			// If a host doesn't need to request data, mark it as complete.
			if len(sources) == 0 {
				j.URIs[uri] = true
				continue
			}
			// TODO: we can probably consilidate the instructions that go to the same
			// node but apply to different indexes. (i.e. don't nest this in the Indexes() loop)
			instr := &internal.ResizeInstruction{
				JobID:       j.ID,
				URI:         uri.Encode(),
				Coordinator: encodeURI(c.Coordinator),
				Sources:     sources,
				Schema:      pbSchema,
			}
			j.Instructions = append(j.Instructions, instr)
		}
	}

	return j
}

// CompleteCurrentJob sets the state of the current ResizeJob
// then removes the pointer to currentJob.
func (c *Cluster) CompleteCurrentJob(state string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentJob == nil {
		return fmt.Errorf("no resize job currently running")
	}
	c.currentJob.SetState(state)
	c.currentJob = nil
	return nil
}

// FollowResizeInstruction is run by any node that receives a ResizeInstruction.
func (c *Cluster) FollowResizeInstruction(instr *internal.ResizeInstruction) error {
	go func() {
		// Prepare the return message.
		complete := &internal.ResizeInstructionComplete{
			JobID: instr.JobID,
			URI:   instr.URI,
			Error: "",
		}

		// Stop processing on any error.
		if err := func() error {

			// Sync the schema received in the resize instruction.
			if err := c.Holder.ApplySchema(instr.Schema); err != nil {
				return err
			}

			// Create a client for calling remote nodes.
			client := NewInternalHTTPClientFromURI(&c.URI, nil) // TODO: ClientOptions

			// Request each source file in ResizeSources.
			for _, src := range instr.Sources {
				c.logger().Printf("\n**** Get slice %d for index %s from host %s ****\n\n", src.Slice, src.Index, src.URI)

				srcURI := decodeURI(src.URI)

				// Retrieve frame.
				f := c.Holder.Frame(src.Index, src.Frame)
				if f == nil {
					return ErrFrameNotFound
				}

				// Create view.
				v, err := f.CreateViewIfNotExists(src.View)
				if err != nil {
					return err
				}

				// Create the local fragment.
				frag, err := v.CreateFragmentIfNotExists(src.Slice)
				if err != nil {
					return err
				}

				// Stream slice from remote node.
				rd, err := client.RetrieveSliceFromURI(context.Background(), src.Index, src.Frame, src.View, src.Slice, srcURI)
				if err != nil {
					return err
				} else if rd == nil {
					return fmt.Errorf("slice %v doesn't exist on host: %s", src.Slice, src.URI)
				}

				// Write to local frame and always close reader.
				if err := func() error {
					defer rd.Close()
					if _, err := frag.ReadFrom(rd); err != nil {
						return err
					}
					return nil
				}(); err != nil {
					return err
				}
			}
			return nil
		}(); err != nil {
			complete.Error = err.Error()
		}

		node := &Node{
			URI: decodeURI(instr.Coordinator),
		}
		if err := c.Broadcaster.SendTo(node, complete); err != nil {
			c.logger().Printf("sending resizeInstructionComplete error: err=%s", err)
		}
	}()
	return nil
}

func (c *Cluster) MarkResizeInstructionComplete(complete *internal.ResizeInstructionComplete) error {

	j := c.Job(complete.JobID)

	// Abort the job if an error exists in the complete object.
	if complete.Error != "" {
		j.result <- ResizeJobStateAborted
		return errors.New(complete.Error)
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	if j.isComplete() {
		return fmt.Errorf("ResizeJob %d is no longer running", j.ID)
	}

	uri := decodeURI(complete.URI)

	// Mark host complete.
	j.URIs[uri] = true

	if !j.urisArePending() {
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
	URIs         map[URI]bool
	Instructions []*internal.ResizeInstruction
	Broadcaster  Broadcaster

	result chan string

	mu    sync.RWMutex
	state string
}

// NewResizeJob returns a new instance of ResizeJob.
func NewResizeJob(addURI URI, existingURIs []URI) *ResizeJob {

	// Build a map of uris to track their resize status.
	uris := make(map[URI]bool)

	// The value for a node will be set to true after that node
	// has indicated that it has completed all resize instructions.
	for _, u := range existingURIs {
		uris[u] = false
	}
	// Include the added node in the map for tracking.
	uris[addURI] = false

	return &ResizeJob{
		ID:     rand.Int63(),
		URIs:   uris,
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
	// Set job state to RUNNING.
	j.SetState(ResizeJobStateRunning)

	// Job can be considered done in the case where it doesn't require any action.
	if !j.urisArePending() {
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

// urisArePending returns true if any uri is still working on the resize.
func (j *ResizeJob) urisArePending() bool {
	for _, complete := range j.URIs {
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
			URI: decodeURI(instr.URI),
		}
		if err := j.Broadcaster.SendTo(node, instr); err != nil {
			return err
		}
	}
	return nil
}

type NodeSet []URI

func (n NodeSet) Len() int           { return len(n) }
func (n NodeSet) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n NodeSet) Less(i, j int) bool { return n[i].String() < n[j].String() }

func (u NodeSet) ToHostPortStrings() []string {
	other := make([]string, 0, len(u))
	for _, uri := range u {
		other = append(other, uri.HostPort())
	}
	return other
}

func (u NodeSet) ToStrings() []string {
	other := make([]string, 0, len(u))
	for _, uri := range u {
		other = append(other, uri.String())
	}
	return other
}

// Topology represents the list of hosts in the cluster.
type Topology struct {
	mu      sync.RWMutex
	NodeSet []URI
}

func NewTopology() *Topology {
	return &Topology{}
}

// ContainsURI returns true if uri matches one of the topology's uris.
func (t *Topology) ContainsURI(uri URI) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.containsURI(uri)
}

func (t *Topology) containsURI(uri URI) bool {
	for _, turi := range t.NodeSet {
		if turi == uri {
			return true
		}
	}
	return false
}

// AddURI adds the uri to the topology and returns true if added.
func (t *Topology) AddURI(uri URI) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.containsURI(uri) {
		return false
	}
	t.NodeSet = append(t.NodeSet, uri)
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
		return err
	}

	var pb internal.Topology
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	top, err := decodeTopology(&pb)
	if err != nil {
		return err
	}
	c.Topology = top

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
		NodeSet: encodeURIs(topology.NodeSet),
	}
}

func decodeTopology(topology *internal.Topology) (*Topology, error) {
	if topology == nil {
		return nil, nil
	}

	t := &Topology{
		NodeSet: decodeURIs(topology.NodeSet),
	}
	return t, nil
}

func (c *Cluster) considerTopology() (string, error) {
	// If there is no .topology file, it's safe to go to state NORMAL.
	if len(c.Topology.NodeSet) == 0 {
		return ClusterStateNormal, nil
	}

	// The local node (coordinator) must be in the .topology.
	if !c.Topology.ContainsURI(c.Coordinator) {
		return "", fmt.Errorf("coordinator %s is not in topology: %v", c.Coordinator, c.Topology.NodeSet)
	}

	// If local node is the only thing in .topology, continue to state NORMAL.
	if len(c.Topology.NodeSet) == 1 {
		return ClusterStateNormal, nil
	}

	// Keep the cluster in state "STARTING" until hearing from all nodes.
	// Topology contains 2+ hosts.
	return ClusterStateStarting, nil
}

// ReceiveEvent represents an implementation of EventHandler.
func (c *Cluster) ReceiveEvent(e *NodeEvent) error {
	// Ignore events sent from this node.
	if e.URI == c.URI {
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
			if !c.Topology.ContainsURI(e.URI) {
				return fmt.Errorf("host is not in topology: %v", e.URI)
			}

			if err := c.AddNode(e.URI); err != nil {
				return err
			}

			// If the result of the previous AddNode completed the joining of nodes
			// in the topology, then change the state to NORMAL.
			if c.haveTopologyAgreement() {
				return c.setStateAndBroadcast(ClusterStateNormal)
			}

			return nil
		}

		// Don't do anything else if the cluster already contains the node.
		if c.NodeByURI(e.URI) != nil {
			return nil
		}

		// If the index does not yet have data, go ahead and add the node.
		if !c.Holder.HasData() {
			if err := c.AddNode(e.URI); err != nil {
				return err
			}
			return c.setStateAndBroadcast(ClusterStateNormal)
		}

		// If the cluster has data, we need to change to RESIZING and
		// kick off the resizing process.
		if err := c.setStateAndBroadcast(ClusterStateResizing); err != nil {
			return err
		}
		c.joiningURIs <- e.URI

	case NodeLeave:
		// TODO: implement this
	case NodeUpdate:
		// TODO: implement this
	}

	return nil
}

func (c *Cluster) MergeClusterStatus(cs *internal.ClusterStatus) error {
	// Ignore status updates from self (coordinator).
	if c.IsCoordinator() {
		return nil
	}

	for _, uri := range decodeURIs(cs.NodeSet) {
		c.AddNode(uri)
	}
	c.setState(cs.State)

	return nil
}
