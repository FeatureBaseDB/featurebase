// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/ingest"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/topology"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	defaultConfirmDownRetries = 10
	defaultConfirmDownSleep   = 1 * time.Second
)

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
	sharder disco.Sharder

	holder      *Holder
	broadcaster broadcaster

	abortAntiEntropyCh chan struct{}
	muAntiEntropy      sync.Mutex

	translationSyncer TranslationSyncer

	mu sync.RWMutex

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

		closing: make(chan struct{}),

		translationSyncer: NopTranslationSyncer,

		InternalClient: &InternalClient{}, // TODO might have to fill this out a bit

		logger: logger.NopLogger,

		confirmDownRetries: defaultConfirmDownRetries,
		confirmDownSleep:   defaultConfirmDownSleep,

		disCo:  disco.NopDisCo,
		noder:  topology.NewEmptyLocalNoder(),
		stator: disco.NopStator,
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
