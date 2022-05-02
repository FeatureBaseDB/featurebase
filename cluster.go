// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/ingest"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/topology"
	"github.com/molecula/featurebase/v3/tracing"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	resizeJobActionAdd    = "ADD"
	resizeJobActionRemove = "REMOVE"

	defaultConfirmDownRetries = 10
	defaultConfirmDownSleep   = 1 * time.Second
)

type ResizeNodeMessage struct {
	NodeID string
	Action string
}

type ResizeNodeProgress struct {
	FromID string
	ToID   string
	Done   bool
	Error  string
}

func (p ResizeNodeProgress) applyJSON(fn func([]byte) error) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}

	return fn(data)
}

type ResizeAbortMessage struct{}

// cluster represents a collection of nodes.
type cluster struct { // nolint: maligned
	noder topology.Noder

	id   string
	Node *topology.Node

	// Hashing algorithm used to assign partitions to nodes.
	Hasher topology.Hasher

	// The number of partitions in the cluster.
	partitionN int

	// The number of replicas a partition has.
	ReplicaN int

	// Human-readable name of the cluster.
	Name string

	// Maximum number of Set() or Clear() commands per request.
	maxWritesPerRequest int

	// Data directory path.
	Path string

	// Distributed Consensus
	disCo   disco.DisCo
	stator  disco.Stator
	resizer disco.Resizer
	sharder disco.Sharder

	// Required for cluster Resize.
	Static      bool // Static is primarily used for testing.
	holder      *Holder
	broadcaster broadcaster

	abortAntiEntropyCh chan struct{}
	muAntiEntropy      sync.Mutex

	translationSyncer TranslationSyncer

	mu           sync.RWMutex
	jobs         map[int64]*resizeJob
	resizeCancel context.CancelFunc

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	logger logger.Logger

	InternalClient *InternalClient

	confirmDownRetries int
	confirmDownSleep   time.Duration

	partitionAssigner string
}

// newCluster returns a new instance of Cluster with defaults.
func newCluster() *cluster {
	return &cluster{
		Hasher:     &topology.Jmphasher{},
		partitionN: topology.DefaultPartitionN,
		ReplicaN:   1,

		jobs:    make(map[int64]*resizeJob),
		closing: make(chan struct{}),

		translationSyncer: NopTranslationSyncer,

		InternalClient: &InternalClient{}, // TODO might have to fill this out a bit

		logger: logger.NopLogger,

		confirmDownRetries: defaultConfirmDownRetries,
		confirmDownSleep:   defaultConfirmDownSleep,

		disCo:   disco.NopDisCo,
		noder:   topology.NewEmptyLocalNoder(),
		stator:  disco.NopStator,
		resizer: disco.NopResizer,
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

func (c *cluster) primaryNode() *topology.Node {
	return c.unprotectedPrimaryNode()
}

// unprotectedPrimaryNode returns the primary node.
func (c *cluster) unprotectedPrimaryNode() *topology.Node {
	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := c.NewSnapshot()
	return snap.PrimaryFieldTranslationNode()
}

func (c *cluster) applySchemaWithNewShards(schema *Schema) error {
	if schema == nil || len(schema.Indexes) == 0 {
		return nil
	}

	if err := c.holder.applySchema(schema); err != nil {
		return errors.Wrap(err, "applying schema")
	}

	// Get and set the shards for each field.
	for _, idx := range c.holder.indexes {
		for _, fld := range idx.fields {
			err := fld.loadAvailableShards()
			if err != nil {
				return errors.Wrapf(err, "getting shards for field: %s/%s", idx.name, fld.name)
			}
		}
	}

	return nil
}

// addNode adds a node to the Cluster and starts resizing process
func (c *cluster) addNode(id string) error {
	// If this method is being called on the node which was just added, then the
	// node will be completely empty. That means that it won't have the current
	// schema with which to calculate its resize intructions (in
	// c.resizeNodeOnAdd, which calls c.generateResizeInstructionOnAdd). Because
	// of this, we need to request and apply the current schema from etcd before
	// we can proceed with the resize process.
	if id == c.disCo.ID() {
		schema, err := c.remoteSchema()
		if err != nil {
			return err
		}

		if err := c.applySchemaWithNewShards(schema); err != nil {
			return err
		}
	}

	eg := &errgroup.Group{}
	for _, n := range c.noder.Nodes() {
		if err := c.sendTo(n, &ResizeNodeMessage{NodeID: id, Action: resizeJobActionAdd}); err != nil {
			return errors.Wrap(err, "broadcasting resize message")
		}

		nodeID := n.ID
		eg.Go(func() error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := c.resizer.Watch(ctx, nodeID, func(data []byte) error {
				var progress ResizeNodeProgress
				if err := json.Unmarshal(data, &progress); err != nil {
					return errors.Wrapf(err, "watching progress node %s", nodeID)
				}
				if progress.Error != "" {
					return errors.Errorf("watching progress node %s: %s", nodeID, progress.Error)
				}
				if progress.Done {
					return io.EOF
				}
				return nil
			})
			if err == io.EOF {
				err = nil
			}
			return err
		})
	}

	// Wait for all background resize threads to return. If there were any
	// errors, then we need to delete the node (which we were attempting to add)
	// from the etcd cluster.
	go func() {
		if err := eg.Wait(); err != nil {
			c.logger.Printf("Stop watching all peers: %+v", err)

			if err := c.disCo.DeleteNode(context.Background(), id); err != nil {
				// resizing failed, so we have to delete the new node.
				c.logger.Printf("Cannot delete the node %s: %+v", id, err)
			}
		}
	}()

	return nil
}

func (c *cluster) resizeNodeOnAdd(addNodeID string) error {
	ctx, cancel := context.WithCancel(context.Background())

	// set status to RESIZING
	progressFunc, err := c.resizer.Resize(context.Background())
	if err != nil {
		cancel()
		return errors.Wrapf(err, "setting RESIZING state on %s", c.disCo.ID())
	}

	c.resizeCancel = cancel
	// start async. data balancing
	go func() {
		progress := ResizeNodeProgress{ToID: addNodeID, FromID: c.disCo.ID()}
		defer func() {
			err := progress.applyJSON(progressFunc)
			if err != nil {
				c.logger.Printf("updating resize progress (%s -> %s): %+v", c.disCo.ID(), addNodeID, err)
			}
			if c.resizeCancel != nil {
				c.resizeCancel()
			}
			err = c.resizer.DoneResize()
			if err != nil {
				c.logger.Printf("done resize (%s -> %s): %+v", c.disCo.ID(), addNodeID, err)
			}
		}()

		instr, err := c.generateResizeInstructionOnAdd(addNodeID)
		if err != nil {
			progress.Error = errors.Wrapf(err, "generating resize instruction (%s -> %s)", c.disCo.ID(), addNodeID).Error()
			c.logger.Printf(progress.Error)
			return
		}

		if err = c.followResizeInstruction(ctx, instr); err != nil {
			progress.Error = errors.Wrapf(err, "following resize instruction (%s -> %s)", c.disCo.ID(), addNodeID).Error()
			c.logger.Printf(progress.Error)
			return
		}
		progress.Done = true
	}()

	return nil
}

func (c *cluster) generateResizeInstructionOnAdd(addNodeID string) (*ResizeInstruction, error) {
	fromCluster := newCluster()
	for _, n := range topology.Nodes(c.noder.Nodes()).Clone() {
		if n.ID == addNodeID {
			continue
		}
		fromCluster.noder.AppendNode(n)
	}
	fromCluster.Hasher = c.Hasher
	fromCluster.partitionN = c.partitionN
	fromCluster.ReplicaN = c.ReplicaN

	// fragmentSourcesByNode is a map of Node.ID to sources of fragment data.
	// It is initialized with all the nodes in toCluster.
	fragmentSourcesByNode := make(map[string][]*ResizeSource)
	for _, n := range c.noder.Nodes() {
		fragmentSourcesByNode[n.ID] = nil
	}

	indexes := c.holder.Indexes()
	// Add to fragmentSourcesByNode the instructions for each index.
	for _, idx := range indexes {
		fragSources, err := fromCluster.fragSources(c, idx)
		if err != nil {
			return nil, errors.Wrap(err, "getting sources")
		}

		for nodeid, sources := range fragSources {
			fragmentSourcesByNode[nodeid] = append(fragmentSourcesByNode[nodeid], sources...)
		}
	}

	// translationSourcesByNode is a map of Node.ID to sources of partitioned
	// key translation data for indexes.
	// It is initialized with all the nodes in toCluster.
	translationSourcesByNode := make(map[string][]*TranslationResizeSource)
	for _, n := range c.noder.Nodes() {
		translationSourcesByNode[n.ID] = nil
	}

	if len(indexes) > 0 {
		// Add to translationSourcesByNode the instructions for the cluster.
		translationNodes, err := fromCluster.translationNodes(c)
		if err != nil {
			return nil, errors.Wrap(err, "getting translation sources")
		}

		// Create a list of TranslationResizeSource for each index,
		// using translationNodes as a template.
		translationSources := make(map[string][]*TranslationResizeSource)
		for _, idx := range indexes {
			// Only include indexes with keys.
			if !idx.Keys() {
				continue
			}
			indexName := idx.Name()
			for node, resizeNodes := range translationNodes {
				for i := range resizeNodes {
					translationSources[node] = append(translationSources[node],
						&TranslationResizeSource{
							Node:        resizeNodes[i].node,
							Index:       indexName,
							PartitionID: resizeNodes[i].partitionID,
						})
				}
			}
		}

		for nodeid, sources := range translationSources {
			translationSourcesByNode[nodeid] = sources
		}
	}

	status, err := c.unprotectedStatus()
	if err != nil {
		return nil, errors.Wrap(err, "getting cluster status")
	}

	myid := c.disCo.ID()
	nodeStatus, err := c.nodeStatus()
	if err != nil {
		return nil, errors.Wrap(err, "getting node status")
	}
	return &ResizeInstruction{
		Node:               c.unprotectedNodeByID(myid),
		Sources:            fragmentSourcesByNode[myid],
		TranslationSources: translationSourcesByNode[myid],
		NodeStatus:         nodeStatus, // Include the NodeStatus in order to ensure that schema and availableShards are in sync on the receiving node.
		ClusterStatus:      status,
	}, nil
}

// removeNode removes a node from the Cluster and starts resizing process.
func (c *cluster) removeNode(id string) error {
	eg := &errgroup.Group{}
	for _, n := range c.noder.Nodes() {
		// Don't send the resize message to the node being removed.
		if n.ID == id {
			continue
		}

		if err := c.sendTo(n, &ResizeNodeMessage{NodeID: id, Action: resizeJobActionRemove}); err != nil {
			return errors.Wrap(err, "broadcasting resize message")
		}

		nodeID := n.ID
		eg.Go(func() error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := c.resizer.Watch(ctx, nodeID, func(data []byte) error {
				var progress ResizeNodeProgress
				if err := json.Unmarshal(data, &progress); err != nil {
					return errors.Wrapf(err, "watching progress node %s", nodeID)
				}
				if progress.Error != "" {
					return errors.Errorf("watching progress node %s: %s", nodeID, progress.Error)
				}
				if progress.Done {
					return io.EOF
				}
				return nil
			})
			if err == io.EOF {
				err = nil
			}
			return err
		})
	}

	// monitor all background resize threads
	go func() {
		if err := eg.Wait(); err != nil {
			c.logger.Printf("Stop watching all peers: %+v", err)
			return
		}

		if err := c.disCo.DeleteNode(context.Background(), id); err != nil {
			// it's ok, we can delete the node
			c.logger.Printf("Cannot delete the node %s: %+v", id, err)
		}
	}()

	return nil
}

func (c *cluster) resizeNodeOnRemove(removeNodeID string) error {
	ctx, cancel := context.WithCancel(context.Background())

	// set status to RESIZING
	progressFunc, err := c.resizer.Resize(context.Background())
	if err != nil {
		cancel()
		return errors.Wrapf(err, "setting RESIZING state on %s", c.disCo.ID())
	}

	c.resizeCancel = cancel
	// start async. data balancing
	go func() {
		progress := ResizeNodeProgress{FromID: removeNodeID, ToID: c.disCo.ID()}
		defer func() {
			err := progress.applyJSON(progressFunc)
			if err != nil {
				c.logger.Printf("updating resize progress (%s <- %s): %+v", c.disCo.ID(), removeNodeID, err)
			}
			if c.resizeCancel != nil {
				c.resizeCancel()
			}
			err = c.resizer.DoneResize()
			if err != nil {
				c.logger.Printf("done resize (%s <- %s): %+v", c.disCo.ID(), removeNodeID, err)
			}
		}()

		instr, err := c.generateResizeInstructionOnRemove(removeNodeID)
		if err != nil {
			progress.Error = errors.Wrapf(err, "generating resize instruction (%s <- %s)", c.disCo.ID(), removeNodeID).Error()
			c.logger.Printf(progress.Error)
			return
		}

		if err = c.followResizeInstruction(ctx, instr); err != nil {
			progress.Error = errors.Wrapf(err, "following resize instruction (%s <- %s)", c.disCo.ID(), removeNodeID).Error()
			c.logger.Printf(progress.Error)
			return
		}
		progress.Done = true
	}()

	return nil
}

func (c *cluster) generateResizeInstructionOnRemove(removeNodeID string) (*ResizeInstruction, error) {
	toCluster := newCluster()
	toCluster.noder.SetNodes(topology.Nodes(c.noder.Nodes()).Clone())
	toCluster.Hasher = c.Hasher
	toCluster.partitionN = c.partitionN
	toCluster.ReplicaN = c.ReplicaN
	toCluster.removeNodeBasicSorted(removeNodeID)

	// fragmentSourcesByNode is a map of Node.ID to sources of fragment data.
	// It is initialized with all the nodes in toCluster.
	fragmentSourcesByNode := make(map[string][]*ResizeSource)
	for _, n := range toCluster.noder.Nodes() {
		fragmentSourcesByNode[n.ID] = nil
	}

	indexes := c.holder.Indexes()
	// Add to fragmentSourcesByNode the instructions for each index.
	for _, idx := range indexes {
		fragSources, err := c.fragSources(toCluster, idx)
		if err != nil {
			return nil, errors.Wrap(err, "getting sources")
		}

		for nodeid, sources := range fragSources {
			fragmentSourcesByNode[nodeid] = append(fragmentSourcesByNode[nodeid], sources...)
		}
	}

	// translationSourcesByNode is a map of Node.ID to sources of partitioned
	// key translation data for indexes.
	// It is initialized with all the nodes in toCluster.
	translationSourcesByNode := make(map[string][]*TranslationResizeSource)
	for _, n := range toCluster.noder.Nodes() {
		translationSourcesByNode[n.ID] = nil
	}

	if len(indexes) > 0 {
		// Add to translationSourcesByNode the instructions for the cluster.
		translationNodes, err := c.translationNodes(toCluster)
		if err != nil {
			return nil, errors.Wrap(err, "getting translation sources")
		}

		// Create a list of TranslationResizeSource for each index,
		// using translationNodes as a template.
		translationSources := make(map[string][]*TranslationResizeSource)
		for _, idx := range indexes {
			// Only include indexes with keys.
			if !idx.Keys() {
				continue
			}
			indexName := idx.Name()
			for node, resizeNodes := range translationNodes {
				for i := range resizeNodes {
					translationSources[node] = append(translationSources[node],
						&TranslationResizeSource{
							Node:        resizeNodes[i].node,
							Index:       indexName,
							PartitionID: resizeNodes[i].partitionID,
						})
				}
			}
		}

		for nodeid, sources := range translationSources {
			translationSourcesByNode[nodeid] = sources
		}
	}

	status, err := c.unprotectedStatus()
	if err != nil {
		return nil, errors.Wrap(err, "getting cluster status")
	}

	myid := c.disCo.ID()
	nodeStatus, err := c.nodeStatus()
	if err != nil {
		return nil, errors.Wrap(err, "getting node status")
	}
	return &ResizeInstruction{
		Node:               toCluster.unprotectedNodeByID(myid),
		Sources:            fragmentSourcesByNode[myid],
		TranslationSources: translationSourcesByNode[myid],
		NodeStatus:         nodeStatus, // Include the NodeStatus in order to ensure that schema and availableShards are in sync on the receiving node.
		ClusterStatus:      status,
	}, nil
}

// unprotectedStatus returns the the cluster's status including what nodes it contains, its ID, and current state.
func (c *cluster) unprotectedStatus() (*ClusterStatus, error) {
	state, err := c.stator.ClusterState(context.Background())
	if err != nil {
		return nil, err
	}

	indexes, err := c.holder.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}

	return &ClusterStatus{
		State:  string(state),
		Nodes:  c.Nodes(),
		Schema: &Schema{Indexes: indexes},
	}, nil
}

func (c *cluster) remoteSchema() (*Schema, error) {
	for _, n := range c.noder.Nodes() {
		if c.disCo.ID() == n.ID {
			continue
		}

		ii, err := c.InternalClient.SchemaNode(context.Background(), &n.URI, true)
		if err != nil {
			return nil, errors.Wrapf(err, "getting schema from %s (%v)", n.ID, n.URI)
		}

		return &Schema{ii}, nil
	}
	return nil, nil
}

// nodeIDs returns the list of IDs in the cluster.
func (c *cluster) nodeIDs() []string {
	return topology.Nodes(c.Nodes()).IDs()
}

func (c *cluster) State() (disco.ClusterState, error) {
	return c.stator.ClusterState(context.Background())
}

func (c *cluster) nodeByID(id string) *topology.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.unprotectedNodeByID(id)
}

// unprotectedNodeByID returns a node reference by ID.
func (c *cluster) unprotectedNodeByID(id string) *topology.Node {
	for _, n := range c.noder.Nodes() {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// nodePositionByID returns the position of the node in slice c.Nodes.
func (c *cluster) nodePositionByID(nodeID string) int {
	for i, n := range c.noder.Nodes() {
		if n.ID == nodeID {
			return i
		}
	}
	return -1
}

// addNodeBasicSorted adds a node to the cluster, sorted by id. Returns a
// pointer to the node and true if the node was added or updated. unprotected.
func (c *cluster) addNodeBasicSorted(node *topology.Node) bool {
	n := c.unprotectedNodeByID(node.ID)

	if n != nil {
		nn := &topology.Node{
			ID:        node.ID,
			URI:       node.URI,
			GRPCURI:   node.GRPCURI,
			IsPrimary: node.IsPrimary,
			State:     node.State,
		}
		if n.State != node.State || n.IsPrimary != node.IsPrimary || n.URI != node.URI {
			*n = *nn
			return true
		}
		return false
	}

	c.noder.AppendNode(node)
	return true
}

// Nodes returns a copy of the slice of nodes in the cluster. Safe for
// concurrent use, result may be modified.
func (c *cluster) Nodes() []*topology.Node {
	nodes := c.noder.Nodes()
	// duplicate the nodes since we're going to be altering them
	copiedNodes := make([]topology.Node, len(nodes))
	result := make([]*topology.Node, len(nodes))

	primary := topology.PrimaryNode(nodes, c.Hasher)

	// Set node states and IsPrimary.
	for i, node := range nodes {
		copiedNodes[i] = *node
		result[i] = &copiedNodes[i]
		if node == primary {
			copiedNodes[i].IsPrimary = true
		}
	}
	return result
}

// removeNodeBasicSorted removes a node from the cluster, maintaining the sort
// order. Returns true if the node was removed. unprotected.
func (c *cluster) removeNodeBasicSorted(nodeID string) bool {
	return c.noder.RemoveNode(nodeID)
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
	return c.fragCombos(idx.Name(), idx.AvailableShards(includeRemote), fieldViews)
}

// fragCombos returns a map (by uri) of lists of fragments for a given index
// by creating every combination of field/view specified in `fieldViews` up
// for the given set of shards with data.
func (c *cluster) fragCombos(idx string, availableShards *roaring.Bitmap, fieldViews viewsByField) fragsByHost {
	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := c.NewSnapshot()

	t := make(fragsByHost)
	_ = availableShards.ForEach(func(i uint64) error {
		nodes := snap.ShardNodes(idx, i)
		for _, n := range nodes {
			// for each field/view combination:
			for field, views := range fieldViews {
				for _, view := range views {
					t[n.ID] = append(t[n.ID], frag{field, view, i})
				}
			}
		}
		return nil
	})
	return t
}

// diff compares c with another cluster and determines if a node is being
// added or removed. An error is returned for any case other than where
// exactly one node is added or removed. unprotected.
func (c *cluster) diff(other *cluster) (action string, nodeID string, err error) {
	cNodes := c.noder.Nodes()
	otherNodes := other.noder.Nodes()
	lenFrom := len(cNodes)
	lenTo := len(otherNodes)
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
		for _, n := range otherNodes {
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
		for _, n := range cNodes {
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
	for _, n := range to.noder.Nodes() {
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
		srcCluster.noder.SetNodes(topology.Nodes(c.noder.Nodes()).Clone())
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

// translationNodes returns a list of translationResizeNodes - for each node
// in the `to` cluster - required to move from cluster `c` to cluster `to`. unprotected.
// Because the parition scheme for every index is the same, this is used as a template
// to create index-specific `TranslationResizeSource`s.
func (c *cluster) translationNodes(to *cluster) (map[string][]*translationResizeNode, error) {
	m := make(map[string][]*translationResizeNode)

	// Determine if a node is being added or removed.
	action, diffNodeID, err := c.diff(to)
	if err != nil {
		return nil, errors.Wrap(err, "diffing")
	}

	// Initialize the map with all the nodes in `to`.
	for _, n := range to.noder.Nodes() {
		m[n.ID] = nil
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	fSnap := c.NewSnapshot()
	toSnap := topology.NewClusterSnapshot(to.noder, c.Hasher, c.partitionAssigner, to.ReplicaN)

	for pid := 0; pid < c.partitionN; pid++ {
		fNodes := fSnap.PartitionNodes(pid)
		tNodes := toSnap.PartitionNodes(pid)

		// For `to` cluster, we include all nodes containing a
		// replica for the partition. The source for each replica
		// will be the primary in the `from` cluster. For the `from`
		// cluster, we only need the first node, unless that node is
		// being removed, then we use the second node. If no second
		// node exists in that case, then we have to raise an error
		// indicating that not enough replicas exist to support
		// the resize.
		if len(tNodes) > 0 {
			var foundPrimary bool
			for i := range fNodes {
				if action == resizeJobActionRemove && fNodes[i].ID == diffNodeID {
					continue
				}
				// We only need to add the source if the nodes differ;
				// in other words if the primary partition is on the
				// same node, it doesn't need to retrieve it.
				for n := range tNodes {
					if tNodes[n].ID != fNodes[i].ID {
						m[tNodes[n].ID] = append(m[tNodes[n].ID],
							&translationResizeNode{
								node:        fNodes[i],
								partitionID: pid,
							})
					}
				}
				foundPrimary = true
				break
			}
			if !foundPrimary {
				return nil, ErrResizeNoReplicas
			}
		}
	}

	return m, nil
}

// shardDistributionByIndex returns a map of [nodeID][primaryOrReplica][]uint64,
// where the int slices are lists of shards.
func (c *cluster) shardDistributionByIndex(indexName string) map[string]map[string][]uint64 {
	dist := make(map[string]map[string][]uint64)

	for _, node := range c.noder.Nodes() {
		nodeDist := make(map[string][]uint64)
		nodeDist["primary-shards"] = make([]uint64, 0)
		nodeDist["replica-shards"] = make([]uint64, 0)
		dist[node.ID] = nodeDist
	}

	index := c.holder.Index(indexName)
	available := index.AvailableShards(includeRemote).Slice()

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := c.NewSnapshot()

	for _, shard := range available {
		p := snap.ShardToShardPartition(indexName, shard)
		nodes := snap.PartitionNodes(p)
		dist[nodes[0].ID]["primary-shards"] = append(dist[nodes[0].ID]["primary-shards"], shard)
		for k := 1; k < len(nodes); k++ {
			dist[nodes[k].ID]["replica-shards"] = append(dist[nodes[k].ID]["replica-shards"], shard)
		}
	}

	return dist
}

func (c *cluster) close() error {
	// Notify goroutines of closing and wait for completion.
	close(c.closing)
	c.wg.Wait()

	return nil
}

func (c *cluster) sendTo(node *topology.Node, m Message) error {
	if err := c.broadcaster.SendTo(node, m); err != nil {
		return errors.Wrap(err, "sending")
	}
	return nil
}

func (c *cluster) followResizeInstruction(ctx context.Context, instr *ResizeInstruction) error {
	// Make sure the holder has opened.
	c.holder.opened.Recv()

	span, _ := tracing.StartSpanFromContext(ctx, "Cluster.followResizeInstruction")
	defer span.Finish()

	// Sync the NodeStatus received in the resize instruction.
	// Sync schema.
	c.logger.Debugf("holder applySchema")
	if err := c.holder.applySchema(instr.NodeStatus.Schema); err != nil {
		return errors.Wrap(err, "applying schema")
	}

	// Sync available shards.
	for _, is := range instr.NodeStatus.Indexes {
		for _, fs := range is.Fields {
			f := c.holder.Field(is.Name, fs.Name)
			// if we don't know about a field locally, log an error because
			// fields should be created and synced prior to shard creation
			if f == nil {
				c.logger.Errorf("local field not found: %s/%s", is.Name, fs.Name)
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()

			default:
				err := f.loadAvailableShards()
				if err != nil {
					return errors.Wrapf(err, "getting shards for field: %s/%s", is.Name, f.name)
				}

			}
		}
	}

	// Request each source file in ResizeSources.
	for _, src := range instr.Sources {
		srcURI := src.Node.URI
		c.logger.Printf("get shard %d for index %s from host %s", src.Shard, src.Index, srcURI)
		// Retrieve field.
		f := c.holder.Field(src.Index, src.Field)
		if f == nil {
			return newNotFoundError(ErrFieldNotFound, src.Field)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			// Create view.
			var v *view
			if err := func() (err error) {
				v, err = f.createViewIfNotExists(src.View)
				return err
			}(); err != nil {
				return errors.Wrap(err, "creating view")
			}

			// Create the local fragment.
			frag, err := v.CreateFragmentIfNotExists(src.Shard)
			if err != nil {
				return errors.Wrap(err, "creating fragment")
			}

			// Stream shard from remote node.
			c.logger.Printf("retrieve shard %d for index %s from host %s", src.Shard, src.Index, srcURI)
			rd, err := c.InternalClient.RetrieveShardFromURI(ctx, src.Index, src.Field, src.View, src.Shard, srcURI)
			if err != nil {
				// For now it is an acceptable error if the fragment is not found
				// on the remote node. This occurs when a shard has been skipped and
				// therefore doesn't contain data. The primary correctly determined
				// the resize instruction to retrieve the shard, but it doesn't have data.
				// TODO: figure out a way to distinguish from "fragment not found" errors
				// which are true errors and which simply mean the fragment doesn't have data.
				if err == ErrFragmentNotFound {
					continue
				}
				return errors.Wrap(err, "retrieving shard")
			} else if rd == nil {
				return fmt.Errorf("shard %v doesn't exist on host: %s", src.Shard, srcURI)
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
	}

	// Request each translation source file in TranslationResizeSources.
	for _, src := range instr.TranslationSources {
		srcURI := src.Node.URI

		idx := c.holder.Index(src.Index)
		if idx == nil {
			return newNotFoundError(ErrIndexNotFound, src.Index)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			// Retrieve partition from remote node.
			c.logger.Printf("retrieve translate partition %d for index %s from host %s", src.PartitionID, src.Index, srcURI)
			rd, err := c.InternalClient.RetrieveTranslatePartitionFromURI(ctx, src.Index, src.PartitionID, srcURI)
			if err != nil {
				return errors.Wrap(err, "retrieving translate partition")
			} else if rd == nil {
				return fmt.Errorf("partition %d doesn't exist on host: %s", src.PartitionID, src.Node.URI)
			}

			// Write to local store and always close reader.
			if err := func() error {
				defer rd.Close()
				// Get the translate store for this index/partition.
				store := idx.TranslateStore(src.PartitionID)
				_, err = store.ReadFrom(rd)
				return errors.Wrap(err, "reading from reader")
			}(); err != nil {
				return errors.Wrap(err, "copying remote partition")
			}
		}
	}

	// fire off translation sync
	_ = c.translationSyncer.Reset()

	return nil
}

func (c *cluster) resizeAbortAndBroadcast() error {
	if err := c.resizeAbort(); err != nil {
		return err
	}
	return c.broadcaster.SendSync(&ResizeAbortMessage{})
}

func (c *cluster) resizeAbort() error {
	if c.resizeCancel != nil {
		c.resizeCancel()
	}
	// fire off translation sync
	_ = c.translationSyncer.Reset()
	return nil
}

type resizeJob struct {
	ID           int64
	IDs          map[string]bool
	Instructions []*ResizeInstruction
	Broadcaster  broadcaster

	action string
	result chan string

	Logger logger.Logger
}

// newResizeJob returns a new instance of resizeJob.
func newResizeJob(existingNodes []*topology.Node, node *topology.Node, action string) *resizeJob {

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

///////////////////////////////////////////
// Cluster implements the Noder interface.
// This is temporary and should be removed once etcd is fully implemented as
// noder.

// SetNodes implements the Noder interface.
func (c *cluster) SetNodes(nodes []*topology.Node) {}

// AppendNode implements the Noder interface.
func (c *cluster) AppendNode(node *topology.Node) {}

// RemoveNode implements the Noder interface.
func (c *cluster) RemoveNode(nodeID string) bool {
	return false
}

// SetNodeState implements the Noder interface.
func (c *cluster) SetNodeState(nodeID string, state string) {}

///////////////////////////////////////////

func (c *cluster) nodeStatus() (*NodeStatus, error) {
	indexes, err := c.holder.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}
	ns := &NodeStatus{
		Node:   c.Node,
		Schema: &Schema{Indexes: indexes},
	}
	var availableShards *roaring.Bitmap
	for _, idx := range ns.Schema.Indexes {
		is := &IndexStatus{Name: idx.Name, CreatedAt: idx.CreatedAt}
		for _, f := range idx.Fields {
			if field := c.holder.Field(idx.Name, f.Name); field != nil {
				availableShards = field.AvailableShards(includeRemote)
			} else {
				availableShards = roaring.NewBitmap()
			}
			is.Fields = append(is.Fields, &FieldStatus{
				Name:            f.Name,
				CreatedAt:       f.CreatedAt,
				AvailableShards: availableShards,
			})
		}
		ns.Indexes = append(ns.Indexes, is)
	}
	return ns, nil
}

// unprotectedPreviousNode returns the node listed before the current node in c.Nodes.
// If there is only one node in the cluster, returns nil.
// If the current node is the first node in the list, returns the last node.
func (c *cluster) unprotectedPreviousNode() *topology.Node {
	cNodes := c.noder.Nodes()
	if len(cNodes) <= 1 {
		return nil
	}

	pos := c.nodePositionByID(c.Node.ID)
	if pos == -1 {
		return nil
	} else if pos == 0 {
		return cNodes[len(cNodes)-1]
	} else {
		return cNodes[pos-1]
	}
}

// PrimaryReplicaNode returns the node listed before the current node in c.Nodes.
// This is different than "previous node" as the first node always returns nil.
func (c *cluster) PrimaryReplicaNode() *topology.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.unprotectedPrimaryReplicaNode()
}

func (c *cluster) unprotectedPrimaryReplicaNode() *topology.Node {
	pos := c.nodePositionByID(c.Node.ID)
	if pos <= 0 {
		return nil
	}
	cNodes := c.noder.Nodes()
	return cNodes[pos-1]
}

// TODO: remove this when it is no longer used
func (c *cluster) translateFieldKeys(ctx context.Context, field *Field, keys []string, writable bool) ([]uint64, error) {
	var trans map[string]uint64
	var err error
	if writable {
		trans, err = c.createFieldKeys(ctx, field, keys...)
	} else {
		trans, err = c.findFieldKeys(ctx, field, keys...)
	}
	if err != nil {
		return nil, err
	}

	ids := make([]uint64, len(keys))
	for i, key := range keys {
		id, ok := trans[key]
		if !ok {
			return nil, ErrTranslatingKeyNotFound
		}

		ids[i] = id
	}

	return ids, nil
}

func (c *cluster) findFieldKeys(ctx context.Context, field *Field, keys ...string) (map[string]uint64, error) {
	if idx := field.ForeignIndex(); idx != "" {
		// The field uses foreign index keys.
		// Therefore, the field keys are actually column keys on a different index.
		return c.findIndexKeys(ctx, idx, keys...)
	}
	if !field.Keys() {
		return nil, errors.Errorf("cannot find keys on unkeyed field %q", field.Name())
	}

	// Attempt to find the keys locally.
	localTranslations, err := field.TranslateStore().FindKeys(keys...)
	if err != nil {
		return nil, errors.Wrapf(err, "translating field(%s/%s) keys(%v) locally", field.Index(), field.Name(), keys)
	}

	// Check for missing keys.
	var missing []string
	if len(keys) > len(localTranslations) {
		// There are either duplicate keys or missing keys.
		// This should work either way.
		missing = make([]string, 0, len(keys)-len(localTranslations))
		for _, k := range keys {
			_, found := localTranslations[k]
			if !found {
				missing = append(missing, k)
			}
		}
	} else if len(localTranslations) > len(keys) {
		panic(fmt.Sprintf("more translations than keys! translation count=%v, key count=%v", len(localTranslations), len(keys)))
	}
	if len(missing) == 0 {
		// All keys were available locally.
		return localTranslations, nil
	}

	// It is possible that the missing keys exist, but have not been synced to the local replica.
	primary := c.primaryNode()
	if primary == nil {
		return nil, errors.Errorf("translating field(%s/%s) keys(%v) - cannot find primary node", field.Index(), field.Name(), keys)
	}
	if c.Node.ID == primary.ID {
		// The local copy is the authoritative copy.
		return localTranslations, nil
	}

	// Forward the missing keys to the primary.
	// The primary has the authoritative copy.
	remoteTranslations, err := c.InternalClient.FindFieldKeysNode(ctx, &primary.URI, field.Index(), field.Name(), missing...)
	if err != nil {
		return nil, errors.Wrapf(err, "translating field(%s/%s) keys(%v) remotely", field.Index(), field.Name(), keys)
	}

	// Merge the remote translations into the local translations.
	translations := localTranslations
	for key, id := range remoteTranslations {
		translations[key] = id
	}

	return translations, nil
}

func (c *cluster) createFieldKeys(ctx context.Context, field *Field, keys ...string) (map[string]uint64, error) {
	if idx := field.ForeignIndex(); idx != "" {
		// The field uses foreign index keys.
		// Therefore, the field keys are actually column keys on a different index.
		return c.createIndexKeys(ctx, idx, keys...)
	}

	if !field.Keys() {
		return nil, errors.Errorf("cannot create keys on unkeyed field %q", field.Name())
	}

	// The primary is the only node that can create field keys, since it owns the authoritative copy.
	primary := c.primaryNode()
	if primary == nil {
		return nil, errors.Errorf("translating field(%s/%s) keys(%v) - cannot find primary node", field.Index(), field.Name(), keys)
	}
	if c.Node.ID == primary.ID {
		// The local copy is the authoritative copy.
		return field.TranslateStore().CreateKeys(keys...)
	}

	// Attempt to find the keys locally.
	// They cannot be created locally, but skipping keys that exist can reduce network usage.
	localTranslations, err := field.TranslateStore().FindKeys(keys...)
	if err != nil {
		return nil, errors.Wrapf(err, "translating field(%s/%s) keys(%v) locally", field.Index(), field.Name(), keys)
	}

	// Check for missing keys.
	var missing []string
	if len(keys) > len(localTranslations) {
		// There are either duplicate keys or missing keys.
		// This should work either way.
		missing = make([]string, 0, len(keys)-len(localTranslations))
		for _, k := range keys {
			_, found := localTranslations[k]
			if !found {
				missing = append(missing, k)
			}
		}
	} else if len(localTranslations) > len(keys) {
		panic(fmt.Sprintf("more translations than keys! translation count=%v, key count=%v", len(localTranslations), len(keys)))
	}
	if len(missing) == 0 {
		// All keys exist locally.
		// There is no need to create anything.
		return localTranslations, nil
	}

	// Forward the missing keys to the primary to be created.
	remoteTranslations, err := c.InternalClient.CreateFieldKeysNode(ctx, &primary.URI, field.Index(), field.Name(), missing...)
	if err != nil {
		return nil, errors.Wrapf(err, "translating field(%s/%s) keys(%v) remotely", field.Index(), field.Name(), keys)
	}

	// Merge the remote translations into the local translations.
	translations := localTranslations
	for key, id := range remoteTranslations {
		translations[key] = id
	}

	return translations, nil
}

func (c *cluster) matchField(ctx context.Context, field *Field, like string) ([]uint64, error) {
	// The primary is the only node that can match field keys, since it is the only node with all of the keys.
	primary := c.primaryNode()
	if primary == nil {
		return nil, errors.Errorf("matching field(%s/%s) like %q - cannot find primary node", field.Index(), field.Name(), like)
	}
	if c.Node.ID == primary.ID {
		// The local copy is the authoritative copy.
		plan := planLike(like)
		store := field.TranslateStore()
		if store == nil {
			return nil, ErrTranslateStoreNotFound
		}
		return field.TranslateStore().Match(func(key []byte) bool {
			return matchLike(key, plan...)
		})
	}

	// Forward the request to the primary.
	return c.InternalClient.MatchFieldKeysNode(ctx, &primary.URI, field.Index(), field.Name(), like)
}

func (c *cluster) translateFieldIDs(ctx context.Context, field *Field, ids map[uint64]struct{}) (map[uint64]string, error) {
	idList := make([]uint64, len(ids))
	{
		i := 0
		for id := range ids {
			idList[i] = id
			i++
		}
	}

	keyList, err := c.translateFieldListIDs(ctx, field, idList)
	if err != nil {
		return nil, err
	}

	mapped := make(map[uint64]string, len(idList))
	for i, key := range keyList {
		mapped[idList[i]] = key
	}
	return mapped, nil
}

func (c *cluster) translateFieldListIDs(ctx context.Context, field *Field, ids []uint64) (keys []string, err error) {
	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := c.NewSnapshot()

	primary := snap.PrimaryFieldTranslationNode()
	if primary == nil {
		return nil, errors.Errorf("translating field(%s/%s) ids(%v) - cannot find primary node", field.Index(), field.Name(), ids)
	}

	if c.Node.ID == primary.ID {
		store := field.TranslateStore()
		if store == nil {
			return nil, ErrTranslateStoreNotFound
		}
		keys, err = field.TranslateStore().TranslateIDs(ids)
	} else {
		keys, err = c.InternalClient.TranslateIDsNode(ctx, &primary.URI, field.Index(), field.Name(), ids)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "translating field(%s/%s) ids(%v)", field.Index(), field.Name(), ids)
	}

	return keys, err
}

// TODO: remove this when it is no longer used
func (c *cluster) translateIndexKey(ctx context.Context, indexName string, key string, writable bool) (uint64, error) {
	keyMap, err := c.translateIndexKeySet(ctx, indexName, map[string]struct{}{key: {}}, writable)
	if err != nil {
		return 0, err
	}
	return keyMap[key], nil
}

// TODO: remove this when it is no longer used
func (c *cluster) translateIndexKeys(ctx context.Context, indexName string, keys []string, writable bool) ([]uint64, error) {
	var trans map[string]uint64
	var err error
	if writable {
		trans, err = c.createIndexKeys(ctx, indexName, keys...)
	} else {
		trans, err = c.findIndexKeys(ctx, indexName, keys...)
	}
	if err != nil {
		return nil, err
	}

	ids := make([]uint64, len(keys))
	for i, key := range keys {
		id, ok := trans[key]
		if !ok {
			return nil, ErrTranslatingKeyNotFound
		}

		ids[i] = id
	}

	return ids, nil
}

// This implements ingest's key translator interface on a cluster/index pair.
type clusterKeyTranslator struct {
	ctx       context.Context // we're created within a request context and need to pass that to cluster ops
	c         *cluster
	indexName string
}

var _ ingest.KeyTranslator = &clusterKeyTranslator{}

func (i clusterKeyTranslator) TranslateKeys(keys ...string) (map[string]uint64, error) {
	return i.c.createIndexKeys(i.ctx, i.indexName, keys...)
}

func (i clusterKeyTranslator) TranslateIDs(ids ...uint64) (map[uint64]string, error) {
	keys, err := i.c.translateIndexIDs(i.ctx, i.indexName, ids)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(ids) {
		return nil, fmt.Errorf("translating %d id(s), got %d key(s)", len(ids), len(keys))
	}
	out := make(map[uint64]string, len(keys))
	for i, id := range ids {
		out[id] = keys[i]
	}
	return out, nil
}

func newIngestKeyTranslatorFromCluster(ctx context.Context, c *cluster, indexName string) *clusterKeyTranslator {
	return &clusterKeyTranslator{ctx: ctx, c: c, indexName: indexName}
}

// TODO: remove this when it is no longer used
func (c *cluster) translateIndexKeySet(ctx context.Context, indexName string, keySet map[string]struct{}, writable bool) (map[string]uint64, error) {
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}

	if writable {
		return c.createIndexKeys(ctx, indexName, keys...)
	}

	trans, err := c.findIndexKeys(ctx, indexName, keys...)
	if err != nil {
		return nil, err
	}
	if len(trans) != len(keys) {
		return nil, ErrTranslatingKeyNotFound
	}

	return trans, nil
}

func (c *cluster) findIndexKeys(ctx context.Context, indexName string, keys ...string) (map[string]uint64, error) {
	done := ctx.Done()

	idx := c.holder.Index(indexName)
	if idx == nil {
		return nil, ErrIndexNotFound
	}
	if !idx.Keys() {
		return nil, errors.Errorf("cannot find keys on unkeyed index %q", indexName)
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := c.NewSnapshot()

	// Split keys by partition.
	keysByPartition := make(map[int][]string, c.partitionN)
	for _, key := range keys {
		partitionID := snap.KeyToKeyPartition(indexName, key)
		keysByPartition[partitionID] = append(keysByPartition[partitionID], key)
	}

	// TODO: use local replicas to short-circuit network traffic

	// Group keys by node.
	keysByNode := make(map[*topology.Node][]string)
	for partitionID, keys := range keysByPartition {
		// Find the primary node for this partition.
		primary := snap.PrimaryPartitionNode(partitionID)
		if primary == nil {
			return nil, errors.Errorf("translating index(%s) keys(%v) on partition(%d) - cannot find primary node", indexName, keys, partitionID)
		}

		if c.Node.ID == primary.ID {
			// The partition is local.
			continue
		}

		// Group the partition to be processed remotely.
		keysByNode[primary] = append(keysByNode[primary], keys...)

		// Delete remote keys from the by-partition map so that it can be used for local translation.
		delete(keysByPartition, partitionID)
	}

	// Start translating keys remotely.
	// On child calls, there are no remote results since we were only sent the keys that we own.
	remoteResults := make(chan map[string]uint64, len(keysByNode))
	var g errgroup.Group
	defer g.Wait() //nolint:errcheck
	for node, keys := range keysByNode {
		node, keys := node, keys

		g.Go(func() error {
			translations, err := c.InternalClient.FindIndexKeysNode(ctx, &node.URI, indexName, keys...)
			if err != nil {
				return errors.Wrapf(err, "translating index(%s) keys(%v) on node %s", indexName, keys, node.ID)
			}

			remoteResults <- translations
			return nil
		})
	}

	// Translate local keys.
	translations := make(map[string]uint64)
	for partitionID, keys := range keysByPartition {
		// Handle cancellation.
		select {
		case <-done:
			return nil, ctx.Err()
		default:
		}

		// Find the keys within the partition.
		t, err := idx.TranslateStore(partitionID).FindKeys(keys...)
		if err != nil {
			return nil, errors.Wrapf(err, "translating index(%s) keys(%v) on partition(%d)", idx.Name(), keys, partitionID)
		}

		// Merge the translations from this partition.
		for key, id := range t {
			translations[key] = id
		}
	}

	// Wait for remote key sets.
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Merge the translations.
	// All data should have been written to here while we waited.
	// Closing the channel prevents the range from blocking.
	close(remoteResults)
	for t := range remoteResults {
		for key, id := range t {
			translations[key] = id
		}
	}
	return translations, nil
}

func (c *cluster) createIndexKeys(ctx context.Context, indexName string, keys ...string) (map[string]uint64, error) {
	// Check for early cancellation.
	done := ctx.Done()
	select {
	case <-done:
		return nil, ctx.Err()
	default:
	}

	idx := c.holder.Index(indexName)
	if idx == nil {
		return nil, ErrIndexNotFound
	}

	if !idx.keys {
		return nil, errors.Errorf("cannot create keys on unkeyed index %q", indexName)
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := c.NewSnapshot()

	// Split keys by partition.
	keysByPartition := make(map[int][]string, c.partitionN)
	for _, key := range keys {
		partitionID := snap.KeyToKeyPartition(indexName, key)
		keysByPartition[partitionID] = append(keysByPartition[partitionID], key)
	}

	// TODO: use local replicas to short-circuit network traffic

	// Group keys by node.
	// Delete remote keys from the by-partition map so that it can be used for local translation.
	keysByNode := make(map[*topology.Node][]string)
	for partitionID, keys := range keysByPartition {
		// Find the primary node for this partition.
		primary := snap.PrimaryPartitionNode(partitionID)
		if primary == nil {
			return nil, errors.Errorf("translating index(%s) keys(%v) on partition(%d) - cannot find primary node", indexName, keys, partitionID)
		}

		if c.Node.ID == primary.ID {
			// The partition is local.
			continue
		}

		// Group the partition to be processed remotely.
		keysByNode[primary] = append(keysByNode[primary], keys...)
		delete(keysByPartition, partitionID)
	}

	translateResults := make(chan map[string]uint64, len(keysByNode)+len(keysByPartition))
	var g errgroup.Group
	defer g.Wait() //nolint:errcheck

	// Start translating keys remotely.
	// On child calls, there are no remote results since we were only sent the keys that we own.
	for node, keys := range keysByNode {
		node, keys := node, keys

		g.Go(func() error {
			translations, err := c.InternalClient.CreateIndexKeysNode(ctx, &node.URI, indexName, keys...)
			if err != nil {
				return errors.Wrapf(err, "translating index(%s) keys(%v) on node %s", indexName, keys, node.ID)
			}

			translateResults <- translations
			return nil
		})
	}

	// Translate local keys.
	// TODO: make this less horrible (why fsync why?????)
	// 		This is kinda terrible because each goroutine does an fsync, thus locking up an entire OS thread.
	// 		AHHHHHHHHHHHHHHHHHH
	for partitionID, keys := range keysByPartition {
		partitionID, keys := partitionID, keys

		g.Go(func() error {
			// Handle cancellation.
			select {
			case <-done:
				return ctx.Err()
			default:
			}

			translations, err := idx.TranslateStore(partitionID).CreateKeys(keys...)
			if err != nil {
				return errors.Wrapf(err, "translating index(%s) keys(%v) on partition(%d)", idx.Name(), keys, partitionID)
			}

			translateResults <- translations
			return nil
		})
	}

	// Wait for remote key sets.
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Merge the translations.
	// All data should have been written to here while we waited.
	// Closing the channel prevents the range from blocking.
	translations := make(map[string]uint64, len(keys))
	close(translateResults)
	for t := range translateResults {
		for key, id := range t {
			translations[key] = id
		}
	}
	return translations, nil
}

func (c *cluster) translateIndexIDs(ctx context.Context, indexName string, ids []uint64) ([]string, error) {
	idSet := make(map[uint64]struct{})
	for _, id := range ids {
		idSet[id] = struct{}{}
	}

	idMap, err := c.translateIndexIDSet(ctx, indexName, idSet)
	if err != nil {
		return nil, err
	}

	keys := make([]string, len(ids))
	for i := range ids {
		keys[i] = idMap[ids[i]]
	}
	return keys, nil
}

func (c *cluster) translateIndexIDSet(ctx context.Context, indexName string, idSet map[uint64]struct{}) (map[uint64]string, error) {
	idMap := make(map[uint64]string, len(idSet))

	index := c.holder.Index(indexName)
	if index == nil {
		return nil, newNotFoundError(ErrIndexNotFound, indexName)
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := c.NewSnapshot()

	// Split ids by partition.
	idsByPartition := make(map[int][]uint64, c.partitionN)
	for id := range idSet {
		partitionID := snap.IDToShardPartition(indexName, id)
		idsByPartition[partitionID] = append(idsByPartition[partitionID], id)
	}

	// Translate ids by partition.
	var g errgroup.Group
	var mu sync.Mutex
	for partitionID := range idsByPartition {
		partitionID := partitionID
		ids := idsByPartition[partitionID]

		g.Go(func() (err error) {
			var keys []string

			primary := snap.PrimaryPartitionNode(partitionID)
			if primary == nil {
				return errors.Errorf("translating index(%s) ids(%v) on partition(%d) - cannot find primary node", indexName, ids, partitionID)
			}

			if c.Node.ID == primary.ID {
				keys, err = index.TranslateStore(partitionID).TranslateIDs(ids)
			} else {
				keys, err = c.InternalClient.TranslateIDsNode(ctx, &primary.URI, indexName, "", ids)
			}

			if err != nil {
				return errors.Wrapf(err, "translating index(%s) ids(%v) on partition(%d)", indexName, ids, partitionID)
			}

			mu.Lock()
			for i, id := range ids {
				idMap[id] = keys[i]
			}
			mu.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return idMap, nil
}

func (c *cluster) NewSnapshot() *topology.ClusterSnapshot {
	return topology.NewClusterSnapshot(c.noder, c.Hasher, c.partitionAssigner, c.ReplicaN)
}

// ClusterStatus describes the status of the cluster including its
// state and node topology.
type ClusterStatus struct {
	ClusterID string
	State     string
	Nodes     []*topology.Node
	Schema    *Schema
}

// ResizeInstruction contains the instruction provided to a node
// during a cluster resize operation.
type ResizeInstruction struct {
	JobID              int64
	Node               *topology.Node
	Primary            *topology.Node
	Sources            []*ResizeSource
	TranslationSources []*TranslationResizeSource
	NodeStatus         *NodeStatus
	ClusterStatus      *ClusterStatus
}

// ResizeSource is the source of data for a node acting on a
// ResizeInstruction.
type ResizeSource struct {
	Node  *topology.Node `protobuf:"bytes,1,opt,name=Node" json:"Node,omitempty"`
	Index string         `protobuf:"bytes,2,opt,name=Index,proto3" json:"Index,omitempty"`
	Field string         `protobuf:"bytes,3,opt,name=Field,proto3" json:"Field,omitempty"`
	View  string         `protobuf:"bytes,4,opt,name=View,proto3" json:"View,omitempty"`
	Shard uint64         `protobuf:"varint,5,opt,name=Shard,proto3" json:"Shard,omitempty"`
}

// TranslationResizeSource is the source of translation data for
// a node acting on a ResizeInstruction.
type TranslationResizeSource struct {
	Node        *topology.Node
	Index       string
	PartitionID int
}

// translateResizeNode holds the node/partition pairs used
// to create a TranslationResizeSource for each index.
type translationResizeNode struct {
	node        *topology.Node
	partitionID int
}

// Schema contains information about indexes and their configuration.
type Schema struct {
	Indexes []*IndexInfo `json:"indexes"`
}

// CreateShardMessage is an internal message indicating shard creation.
type CreateShardMessage struct {
	Index string
	Field string
	Shard uint64
}

// CreateIndexMessage is an internal message indicating index creation.
type CreateIndexMessage struct {
	Index     string
	CreatedAt int64
	Meta      IndexOptions
}

// DeleteIndexMessage is an internal message indicating index deletion.
type DeleteIndexMessage struct {
	Index string
}

// CreateFieldMessage is an internal message indicating field creation.
type CreateFieldMessage struct {
	Index     string
	Field     string
	CreatedAt int64
	Meta      *FieldOptions
}

// UpdateFieldMessage represents a change to an existing field. The
// CreateFieldMessage holds the changed field, while the update shows
// the change that was made.
type UpdateFieldMessage struct {
	CreateFieldMessage CreateFieldMessage
	Update             FieldUpdate
}

// DeleteFieldMessage is an internal message indicating field deletion.
type DeleteFieldMessage struct {
	Index string
	Field string
}

// DeleteAvailableShardMessage is an internal message indicating available shard deletion.
type DeleteAvailableShardMessage struct {
	Index   string
	Field   string
	ShardID uint64
}

// CreateViewMessage is an internal message indicating view creation.
type CreateViewMessage struct {
	Index string
	Field string
	View  string
}

// DeleteViewMessage is an internal message indicating view deletion.
type DeleteViewMessage struct {
	Index string
	Field string
	View  string
}

// ResizeInstructionComplete is an internal message to the primary indicating
// that the resize instructions performed on a single node have completed.
type ResizeInstructionComplete struct {
	JobID int64
	Node  *topology.Node
	Error string
}

// NodeStateMessage is an internal message for broadcasting a node's state.
type NodeStateMessage struct {
	NodeID string `protobuf:"bytes,1,opt,name=NodeID,proto3" json:"NodeID,omitempty"`
	State  string `protobuf:"bytes,2,opt,name=State,proto3" json:"State,omitempty"`
}

// NodeStatus is an internal message representing the contents of a node.
type NodeStatus struct {
	Node    *topology.Node
	Indexes []*IndexStatus
	Schema  *Schema
}

// IndexStatus is an internal message representing the contents of an index.
type IndexStatus struct {
	Name      string
	CreatedAt int64
	Fields    []*FieldStatus
}

// FieldStatus is an internal message representing the contents of a field.
type FieldStatus struct {
	Name            string
	CreatedAt       int64
	AvailableShards *roaring.Bitmap
}

// RecalculateCaches is an internal message for recalculating all caches
// within a holder.
type RecalculateCaches struct{}

// Transaction Actions
const (
	TRANSACTION_START    = "start"
	TRANSACTION_FINISH   = "finish"
	TRANSACTION_VALIDATE = "validate"
)

type TransactionMessage struct {
	Transaction *Transaction
	Action      string
}
