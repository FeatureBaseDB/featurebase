// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/api/client"
	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/etcd"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/proto"
	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/featurebasedb/featurebase/v3/storage"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Cluster represents a per-test wrapper of a "real" cluster.
// Individual tests which request a cluster can get one of these
// back, wrapped with their test name.
type Cluster struct {
	*ShareableCluster
	tb         testing.TB
	indexName  string
	indexBytes []byte
}

// Idx produces an index name suitable for this test's
// cluster, using the name itself, or the concatenation
// of that name and any provided strings, joined by underscores.
func (c *Cluster) Idx(of ...string) string {
	if len(of) == 0 {
		return c.indexName
	}
	for i := range of {
		of[i] = indexName(of[i])
	}
	return c.indexName + strings.Join(of, "_")
}

// Format implements Formatter, allowing us to use clusters in
// format strings to handle the extremely common problem of "I
// want to embed the index name in this here string". Because we
// are horrible criminals, we append the rune, so you can use
// %i and %j to get index-name-plus-i and index-name-plus-j,
// respectively. We do assume that the rune works as a plain byte,
// though.
func (c *Cluster) Format(state fmt.State, r rune) {
	c.indexBytes = append(c.indexBytes[:0], c.indexName...)
	c.indexBytes = append(c.indexBytes, byte(r))
	_, _ = state.Write(c.indexBytes)
}

func (c *Cluster) Close() error {
	if c.shared {
		// We're not going to actually close the cluster, BUT, we do want to
		// delete and close all the indexes we might have just made.
		h := c.GetHolder(0)
		indexes := h.Indexes()
		var lastErr error
		api := c.Nodes[0].API
		for _, idx := range indexes {
			name := idx.Name()
			if strings.HasPrefix(name, c.indexName) {
				err := api.DeleteIndex(context.Background(), name)
				if err != nil {
					lastErr = err
				}
			}
		}
		return lastErr
	}
	if !c.started {
		return errors.New("cluster not started yet")
	}
	return c.ShareableCluster.Close()
}

func (c *Cluster) Start() error {
	if c.started {
		return errors.New("cluster already started")
	}
	c.started = true
	return c.ShareableCluster.Start()
}

// ShareableCluster represents a featurebase cluster (multiple Command instances)
// without test-specific overhead.
type ShareableCluster struct {
	Nodes   []*Command
	started bool
	shared  bool
}

// Query executes an API.Query through one of the cluster's node's API. It fails
// the test if there is an error.
func (c *ShareableCluster) Query(t testing.TB, index, query string) pilosa.QueryResponse {
	t.Helper()
	if len(c.Nodes) == 0 {
		t.Fatal("must have at least one node in cluster to query")
	}

	return c.GetPrimary().QueryAPI(t, &pilosa.QueryRequest{Index: index, Query: query})
}

// QueryHTTP executes a PQL query through the HTTP endpoint. It fails
// the test for explicit errors, but returns an error which has the
// response body if the HTTP call returns a non-OK status.
func (c *ShareableCluster) QueryHTTP(t testing.TB, index, query string) (string, error) {
	t.Helper()
	if len(c.Nodes) == 0 {
		t.Fatal("must have at least one node in cluster to query")
	}

	return c.GetPrimary().Query(t, index, "", query)
}

// QueryGRPC executes a PQL query through the GRPC endpoint. It fails the
// test if there is an error.
func (c *ShareableCluster) QueryGRPC(t testing.TB, index, query string) *proto.TableResponse {
	t.Helper()
	if len(c.Nodes) == 0 {
		t.Fatal("must have at least one node in cluster to query")
	}

	grpcClient, err := client.NewGRPCClient([]string{fmt.Sprintf("%s:%d", c.GetPrimary().Server.GRPCURI().Host, c.GetPrimary().Server.GRPCURI().Port)}, nil, logger.NopLogger)
	if err != nil {
		t.Fatalf("getting GRPC client: %v", err)
	}
	defer grpcClient.Close()

	tableResp, err := grpcClient.QueryUnary(context.Background(), index, query)
	if err != nil {
		t.Fatalf("querying unary: %v", err)
	}

	return tableResp
}

// GetIdleNode gets the node at the given index. This method is used (instead of
// `GetNode()`) when the cluster has yet to be started. In that case, etcd has
// not assigned each node an ID, and therefore the nodes are not in their final,
// sorted order. In other words, this method can only be used to retrieve a node
// when order doesn't matter. An example is if you need to do something like
// this:
//   c.GetNode(0).Config.Cluster.ReplicaN = 2
//   c.GetNode(1).Config.Cluster.ReplicaN = 2
// In this example, the test needs the replication factor to be set to 2 before
// starting; it's ok to reference each node by its index in the pre-sorted node
// list. It's also safe to use this method after `MustRunCluster()` if the
// cluster contains only one node.
func (c *ShareableCluster) GetIdleNode(n int) *Command {
	return c.Nodes[n]
}

// GetNode gets the node at the given index; this method assumes the cluster has
// already been started. Because the node IDs are assigned randomly, they can be
// in an order that does not align with the test's expectations. For example, a
// test might create a 3-node cluster and retrieve them using `GetNode(0)`,
// `GetNode(1)`, and `GetNode(2)` respectively. But if the node IDs are `456`,
// `123`, `789`, then we actually want `GetNode(0)` to return `c.Nodes[1]`, and
// `GetNode(1)` to return `c.Nodes[0]`. This method looks at all the node IDs,
// sorts them, and then returns the node that the test expects.
func (c *ShareableCluster) GetNode(n int) *Command {
	// Put all the node IDs into a list to be sorted.
	ids := make([]nodePlace, len(c.Nodes))
	for i := range c.Nodes {
		ids[i].id = c.Nodes[i].ID()
		ids[i].idx = i
	}

	// Sort the list.
	sort.SliceStable(ids, func(i, j int) bool {
		return ids[i].id < ids[j].id
	})

	// Return the node which is at the given position in the sorted list.
	return c.Nodes[ids[n].idx]
}

// GetPrimary gets the node which has been determined to be the primary.
// This used to be node0 in tests, but since implementing etcd, the primary
// can be any node in the cluster, so we have to use this method in tests which
// need to act on the primary.
func (c *ShareableCluster) GetPrimary() *Command {
	for _, n := range c.Nodes {
		if n.IsPrimary() {
			return n
		}
	}
	return nil
}

// GetNonPrimary gets first first non-primary node in the list of nodes.
func (c *ShareableCluster) GetNonPrimary() *Command {
	for _, n := range c.Nodes {
		if !n.IsPrimary() {
			return n
		}
	}
	return nil
}

// GetNonPrimaries gets all nodes except the primary.
func (c *ShareableCluster) GetNonPrimaries() []*Command {
	rtn := make([]*Command, 0)
	for _, n := range c.Nodes {
		if !n.IsPrimary() {
			rtn = append(rtn, n)
		}
	}
	return rtn
}

// nodePlace represents a node's ID and its index into the c.Nodes slice.
type nodePlace struct {
	id  string
	idx int
}

func (c *ShareableCluster) GetHolder(n int) *Holder {
	return &Holder{Holder: c.GetNode(n).Server.Holder()}
}

func (c *ShareableCluster) Len() int {
	return len(c.Nodes)
}

func (c *ShareableCluster) ImportBitsWithTimestamp(t testing.TB, index, field string, rowcols [][2]uint64, timestamps []int64) {
	t.Helper()
	byShard := make(map[uint64][][2]uint64)
	byShardTs := make(map[uint64][]int64)
	for i, rowcol := range rowcols {
		shard := rowcol[1] / pilosa.ShardWidth
		byShard[shard] = append(byShard[shard], rowcol)
		if len(timestamps) > 0 {
			byShardTs[shard] = append(byShardTs[shard], timestamps[i])
		}
	}

	for shard, bits := range byShard {
		rowIDs := make([]uint64, len(bits))
		colIDs := make([]uint64, len(bits))
		for i, bit := range bits {
			rowIDs[i] = bit[0]
			colIDs[i] = bit[1]
		}
		nodes, err := c.GetPrimary().API.ShardNodes(context.Background(), index, shard)
		if err != nil {
			t.Fatalf("getting shard nodes: %v", err)
		}
		// TODO won't be necessary to do all nodes once that works hits
		// (travis) this TODO is not clear to me, but I think it's
		// suggesting that elsewhere we would support importing to a
		// single node, regardless of where the data ends up.
		for _, node := range nodes {
			for _, com := range c.Nodes {
				if com.API.Node().ID != node.ID {
					continue
				}
				func() {
					qcx := com.API.Txf().NewQcx()
					defer qcx.Abort()
					if len(timestamps) == 0 {
						err := com.API.Import(context.Background(), qcx, &pilosa.ImportRequest{
							Index:     index,
							Field:     field,
							Shard:     shard,
							RowIDs:    rowIDs,
							ColumnIDs: colIDs,
						})
						if err != nil {
							t.Fatalf("importing data: %v", err)
						}
					} else {
						ts := byShardTs[shard]
						err := com.API.Import(context.Background(), qcx, &pilosa.ImportRequest{
							Index:      index,
							Field:      field,
							Shard:      shard,
							RowIDs:     rowIDs,
							ColumnIDs:  colIDs,
							Timestamps: ts,
						})
						if err != nil {
							t.Fatalf("importing data: %v", err)
						}

					}
				}()

			}
		}
	}
}

func (c *ShareableCluster) ImportBits(t testing.TB, index, field string, rowcols [][2]uint64) {
	var noTime []int64
	c.ImportBitsWithTimestamp(t, index, field, rowcols, noTime)
}

// ImportKeyKey imports data into an index where both the index and
// the field are using string keys.
func (c *ShareableCluster) ImportKeyKey(t testing.TB, index, field string, valAndRecKeys [][2]string) {
	t.Helper()
	importRequest := &pilosa.ImportRequest{
		Index:      index,
		Field:      field,
		RowKeys:    make([]string, len(valAndRecKeys)),
		ColumnKeys: make([]string, len(valAndRecKeys)),
	}
	for i, vk := range valAndRecKeys {
		importRequest.RowKeys[i] = vk[0]
		importRequest.ColumnKeys[i] = vk[1]
	}
	qcx := c.GetPrimary().API.Txf().NewQcx()
	defer qcx.Abort()
	err := c.GetPrimary().API.Import(context.Background(), qcx, importRequest)
	if err != nil {
		t.Fatalf("importing keykey data: %v", err)
	}
}

// TimeQuantumKey is a string key and a string+key value
type TimeQuantumKey struct {
	RowKey string
	ColKey string
	Ts     int64
}

// ImportTimeQuantumKey imports data into an index where the index is keyd
// and the field is a time-quantum
func (c *ShareableCluster) ImportTimeQuantumKey(t testing.TB, index, field string, entries []TimeQuantumKey) {
	t.Helper()
	importRequest := &pilosa.ImportRequest{
		Index:      index,
		Field:      field,
		RowKeys:    make([]string, len(entries)),
		ColumnKeys: make([]string, len(entries)),
		Timestamps: make([]int64, len(entries)),
	}
	for i, entry := range entries {
		importRequest.ColumnKeys[i] = entry.ColKey
		importRequest.RowKeys[i] = entry.RowKey
		importRequest.Timestamps[i] = entry.Ts

	}
	qcx := c.GetPrimary().API.Txf().NewQcx()
	defer qcx.Abort()
	err := c.GetPrimary().API.Import(context.Background(), qcx, importRequest)
	if err != nil {
		t.Fatalf("importing keykey data: %v", err)
	}
}

// IntKey is a string key and a signed integer value.
type IntKey struct {
	Val int64
	Key string
}

// ImportIntKey imports int data into an index which uses string keys.
func (c *ShareableCluster) ImportIntKey(t testing.TB, index, field string, pairs []IntKey) {
	t.Helper()
	importRequest := &pilosa.ImportValueRequest{
		Index:      index,
		Field:      field,
		Shard:      math.MaxUint64,
		ColumnKeys: make([]string, len(pairs)),
		Values:     make([]int64, len(pairs)),
	}
	for i, pair := range pairs {
		importRequest.Values[i] = pair.Val
		importRequest.ColumnKeys[i] = pair.Key
	}
	qcx := c.GetPrimary().API.Txf().NewQcx()
	defer qcx.Abort()
	if err := c.GetPrimary().API.ImportValue(context.Background(), qcx, importRequest); err != nil {
		t.Fatalf("importing IntKey data: %v", err)
	}
}

type IntID struct {
	Val int64
	ID  uint64
}

// ImportIntID imports data into an int field in an unkeyed index.
func (c *ShareableCluster) ImportIntID(t testing.TB, index, field string, pairs []IntID) {
	t.Helper()
	importRequest := &pilosa.ImportValueRequest{
		Index:     index,
		Field:     field,
		Shard:     math.MaxUint64,
		ColumnIDs: make([]uint64, len(pairs)),
		Values:    make([]int64, len(pairs)),
	}
	for i, pair := range pairs {
		importRequest.Values[i] = pair.Val
		importRequest.ColumnIDs[i] = pair.ID
	}
	qcx := c.GetPrimary().API.Txf().NewQcx()
	defer qcx.Abort()
	if err := c.GetPrimary().API.ImportValue(context.Background(), qcx, importRequest); err != nil {
		t.Fatalf("importing IntID data: %v", err)
	}
}

// KeyID represents a key and an ID for importing data into an index
// and field where one uses string keys and the other does not.
type KeyID struct {
	Key string
	ID  uint64
}

//ImportIDKey imports data into an unkeyed set field in a keyed index.
func (c *ShareableCluster) ImportIDKey(t testing.TB, index, field string, pairs []KeyID) {
	t.Helper()
	importRequest := &pilosa.ImportRequest{
		Index:      index,
		Field:      field,
		RowIDs:     make([]uint64, len(pairs)),
		ColumnKeys: make([]string, len(pairs)),
	}
	for i, pair := range pairs {
		importRequest.RowIDs[i] = pair.ID
		importRequest.ColumnKeys[i] = pair.Key
	}
	qcx := c.GetPrimary().API.Txf().NewQcx()
	defer qcx.Abort()
	err := c.GetPrimary().API.Import(context.Background(), qcx, importRequest)
	if err != nil {
		t.Fatalf("importing IDKey data: %v", err)
	}
}

// CreateField creates the index (if necessary) and field specified.
func (c *ShareableCluster) CreateField(t testing.TB, index string, iopts pilosa.IndexOptions, field string, fopts ...pilosa.FieldOption) *pilosa.Field {
	t.Helper()
	idx, err := c.GetPrimary().API.CreateIndex(context.Background(), index, iopts)
	if err != nil && !strings.Contains(err.Error(), "index already exists") {
		t.Fatalf("creating index: %v", err)
	} else if err != nil { // index exists
		idx, err = c.GetPrimary().API.Index(context.Background(), index)
		if err != nil {
			t.Fatalf("getting index: %v", err)
		}
	}
	if idx.Options() != iopts {
		t.Logf("existing index options:\n%v\ndon't match given opts:\n%v\n in pilosa/test.Cluster.CreateField", idx.Options(), iopts)
	}

	f, err := c.GetPrimary().API.CreateField(context.Background(), index, field, fopts...)
	// we'll assume the field doesn't exist because checking if the options
	// match seems painful.
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	return f
}

// Start runs a Cluster
func (c *ShareableCluster) Start() error {
	var eg errgroup.Group
	for _, cc := range c.Nodes {
		cc := cc
		eg.Go(func() error {
			return cc.Start()
		})
	}
	err := eg.Wait()
	if err != nil {
		return errors.Wrap(err, "starting cluster")
	}
	return c.AwaitState(disco.ClusterStateNormal, 30*time.Second)
}

// Close stops a Cluster
func (c *ShareableCluster) Close() error {
	if c.shared {
		for i := range c.Nodes {
			holder := c.GetHolder(i)
			indexes := holder.Indexes()
			var indexNames []string
			for _, idx := range indexes {
				indexNames = append(indexNames, idx.Name())
			}
		}
	}
	for i, cc := range c.Nodes {
		if err := cc.Close(); err != nil {
			return errors.Wrapf(err, "stopping server %d", i)
		}
	}
	return nil
}

func (c *ShareableCluster) CloseAndRemoveNonPrimary() error {
	if c.shared {
		return errors.New("can't close-and-remove in shared cluster")
	}
	for i, n := range c.Nodes {
		if !n.IsPrimary() {
			return c.CloseAndRemove(i)
		}
	}
	return errors.New("could not find non-primary node")
}

func (c *ShareableCluster) CloseAndRemove(n int) error {
	if c.shared {
		return errors.New("can't close-and-remove in shared cluster")
	}
	if n < 0 || n >= len(c.Nodes) {
		return fmt.Errorf("close/remove from cluster: index %d out of range (len %d)", n, len(c.Nodes))
	}
	err := c.Nodes[n].Close()
	copy(c.Nodes[n:], c.Nodes[n+1:])
	c.Nodes = c.Nodes[:len(c.Nodes)-1]
	return err
}

// AwaitPrimaryState waits for the cluster primary to reach a specified cluster state.
// When this happens, we know etcd reached a combination of node states that
// would imply this cluster state, but some nodes may not have caught up yet;
// we just test that the coordinator thought the cluster was in the given state.
func (c *ShareableCluster) AwaitPrimaryState(expectedState disco.ClusterState, timeout time.Duration) error {
	if len(c.Nodes) < 1 {
		return errors.New("can't await coordinator state on an empty cluster")
	}
	primary := c.GetPrimary()
	if primary == nil {
		startTime := time.Now()
		var elapsed time.Duration
		for elapsed = 0; elapsed <= timeout; elapsed = time.Since(startTime) {
			time.Sleep(50 * time.Millisecond)
			primary = c.GetPrimary()
			if primary != nil {
				break
			}
		}
		if primary == nil {
			return errors.New("timed out waiting for cluster to have valid topology")
		}
		// we used up some of our timeout waiting for this
		timeout -= elapsed
	}
	onlyCoordinator := &ShareableCluster{Nodes: []*Command{primary}}
	return onlyCoordinator.AwaitState(expectedState, timeout)
}

// ExceptionalState returns an error if any node in the cluster is not
// in the expected state.
func (c *ShareableCluster) ExceptionalState(expectedState disco.ClusterState) error {
	for _, node := range c.Nodes {
		state, err := node.API.State()
		if err != nil || state != expectedState {
			return fmt.Errorf("node %q: state %s: err %v", node.ID(), state, err)
		}
	}
	return nil
}

// AwaitState waits for the whole cluster to reach a specified state.
func (c *ShareableCluster) AwaitState(expectedState disco.ClusterState, timeout time.Duration) (err error) {
	if len(c.Nodes) < 1 {
		return errors.New("can't await state of an empty cluster")
	}
	startTime := time.Now()
	var elapsed time.Duration
	for elapsed = 0; elapsed <= timeout; elapsed = time.Since(startTime) {
		// Counterintuitive: We're returning if the err *is* nil,
		// meaning we've reached the expected state.
		if err = c.ExceptionalState(expectedState); err == nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("waited %v for cluster to reach state %q: %v",
		elapsed, expectedState, err)
}

// MustNewCluster creates a new cluster or returns an existing one. It never shares
// a cluster with non-empty opts. If opts contains only one
// slice of command options, those options are used with every node.
// If it is empty, default options are used. Otherwise, it must contain size
// slices of command options, which are used with corresponding nodes.
func MustNewCluster(tb testing.TB, size int, opts ...[]server.CommandOption) *Cluster {
	if size > 1 && !etcd.AllowCluster() {
		tb.Skip("Testing PLG which does not allow clustering")
	}
	tb.Helper()

	shareable := len(opts) == 0
	// We want tests to default to using the in-memory translate store, so we
	// prepend opts with that functional option. If a different translate store
	// has been specified, it will override this one.
	opts = prependOpts(opts, size)

	c, err := newCluster(tb, size, shareable, opts...)
	if err != nil {
		tb.Fatalf("new cluster: %v", err)
	}
	return c
}

// MustUnsharedCluster creates a new cluster. If opts contains only one
// slice of command options, those options are used with every node.
// If it is empty, default options are used. Otherwise, it must contain size
// slices of command options, which are used with corresponding nodes. The
// new cluster is always unshared.
func MustUnsharedCluster(tb testing.TB, size int, opts ...[]server.CommandOption) *Cluster {
	if size > 1 && !etcd.AllowCluster() {
		tb.Skip("Testing PLG which does not allow clustering")
	}
	tb.Helper()
	// We want tests to default to using the in-memory translate store, so we
	// prepend opts with that functional option. If a different translate store
	// has been specified, it will override this one.
	opts = prependOpts(opts, size)

	c, err := newCluster(tb, size, false, opts...)
	if err != nil {
		tb.Fatalf("new cluster: %v", err)
	}
	return c
}

// MustRunUnsharedCluster creates a new cluster. If opts contains only one
// slice of command options, those options are used with every node.
// If it is empty, default options are used. Otherwise, it must contain size
// slices of command options, which are used with corresponding nodes. The
// new cluster is always unshared. The new cluster is started automatically,
// or the test is failed.
func MustRunUnsharedCluster(tb testing.TB, size int, opts ...[]server.CommandOption) *Cluster {
	if size > 1 && !etcd.AllowCluster() {
		tb.Skip("Testing PLG which does not allow clustering")
	}
	tb.Helper()
	// We want tests to default to using the in-memory translate store, so we
	// prepend opts with that functional option. If a different translate store
	// has been specified, it will override this one.
	opts = prependOpts(opts, size)

	c, err := newCluster(tb, size, false, opts...)
	if err != nil {
		tb.Fatalf("new cluster: %v", err)
	}
	err = c.Start()
	if err != nil {
		tb.Fatalf("starting cluster: %v", err)
	}
	return c
}

type clusterCache struct {
	mu       sync.Mutex
	clusters map[int]*ShareableCluster
}

// CleanupClusters calls the close functions on any shared clusters that are
// still open.
func (c *clusterCache) CleanupClusters() {
	for k, v := range c.clusters {
		_ = v.Close()
		delete(c.clusters, k)
	}
}

func (c *clusterCache) newCluster(tb testing.TB, size int) (*ShareableCluster, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c := c.clusters[size]; c != nil {
		return c, nil
	}
	// Make a whole-test wrapper so that the cluster we create will use the provided tb
	// for nearly everything, but the call to TempDir inside NewCommand will pick up a
	// persistent directory which outlives the provided TB.
	newTB := NewWholeTestRun(tb)
	if c.clusters == nil {
		c.clusters = make(map[int]*ShareableCluster)
		// tb should always be a wholeTestRun for clusterCache, and we need to
		// register with that, so our cleanup happens *before* the deletion of
		// the directories, otherwise etcd can fail to flush WAL files on
		// exit, causing tests to fail.
		newTB.Cleanup(c.CleanupClusters)
	}
	cluster, err := underlyingNewCluster(newTB, size)
	if err != nil {
		return nil, err
	}
	cluster.shared = true
	c.clusters[size] = cluster
	return cluster, nil
}

func underlyingNewCluster(tb DirCleaner, size int, opts ...[]server.CommandOption) (*ShareableCluster, error) {
	cluster := &ShareableCluster{Nodes: make([]*Command, size)}
	for i := 0; i < size; i++ {
		var commandOpts []server.CommandOption
		if len(opts) > 0 {
			commandOpts = opts[i%len(opts)]
		}
		m := NewCommandNode(tb, commandOpts...)
		m.Config.ImportWorkerPoolSize = 2

		cluster.Nodes[i] = m
	}
	// The GetPorts... stuff calls things elsewhere that want a plain testing.TB,
	// and doesn't produce permanent directories, I think.
	err := GetPortsGenConfigs(tb, cluster.Nodes)
	if err != nil {
		return nil, errors.Wrap(err, "configuring cluster ports")
	}
	return cluster, nil
}

var globalClusterCache clusterCache

// newCluster creates a new cluster, using the shared cluster cache if no opts are
// specified.
func newCluster(tb testing.TB, size int, shareable bool, opts ...[]server.CommandOption) (*Cluster, error) {
	if size == 0 {
		return nil, errors.New("cluster must contain at least one node")
	}

	if len(opts) != size && len(opts) != 0 && len(opts) != 1 {
		return nil, errors.New("Slice of CommandOptions must be of length 0, 1, or equal to the number of cluster nodes")
	}

	var shared *ShareableCluster
	var err error
	if !shareable {
		shared, err = underlyingNewCluster(tb, size, opts...)
	} else {
		shared, err = globalClusterCache.newCluster(tb, size)
	}
	if err != nil {
		return nil, err
	}
	return &Cluster{ShareableCluster: shared, tb: tb, indexName: indexName(tb.Name())}, nil
}

// MustRunCluster creates and starts a new cluster. The opts parameter
// is slightly magical; see MustNewCluster.
func MustRunCluster(tb testing.TB, size int, opts ...[]server.CommandOption) *Cluster {
	cluster := MustNewCluster(tb, size, opts...)
	if !cluster.started {
		err := cluster.Start()
		if err != nil {
			tb.Fatalf("run cluster: %v", err)
		}
		cluster.started = true
	}
	return cluster
}

// prependOpts applies prependTestServerOpts to each of the ops (one per
// node, or one for the entire cluser).
func prependOpts(opts [][]server.CommandOption, size int) [][]server.CommandOption {
	if len(opts) == 0 {
		opts = make([][]server.CommandOption, size)
		for i := 0; i < size; i++ {
			opts[i] = prependTestServerOpts([]server.CommandOption{})
		}
	} else if len(opts) == 1 {
		opts2 := make([][]server.CommandOption, size)
		for i := 0; i < size; i++ {
			opts2[i] = prependTestServerOpts(opts[0])
		}
		return opts2
	} else {
		for i := range opts {
			opts[i] = prependTestServerOpts(opts[i])
		}
	}
	return opts
}

// prependTestServerOpts prepends opts with the OpenInMemTranslateStore,
// tweaks to initial startup delay, and storage config to disable fsync and
// specify a smaller RBF size.
func prependTestServerOpts(opts []server.CommandOption) []server.CommandOption {
	cfg := pilosa.DefaultHolderConfig()
	cfg.RBFConfig.FsyncEnabled = false
	cfg.RBFConfig.MaxSize = (1 << 28)
	cfg.RBFConfig.MaxWALSize = (1 << 28)
	defaultOpts := []server.CommandOption{
		server.OptCommandServerOptions(
			pilosa.OptServerOpenTranslateStore(pilosa.OpenInMemTranslateStore),
			pilosa.OptServerNodeDownRetries(5, 100*time.Millisecond),
			pilosa.OptServerStorageConfig(&storage.Config{
				Backend:      storage.DefaultBackend,
				FsyncEnabled: false,
			}),
			pilosa.OptServerRBFConfig(cfg.RBFConfig),
		),
	}
	return append(defaultOpts, opts...)
}

func indexName(in string) string {
	return strings.Map(func(r rune) rune {
		if r < 127 {
			switch {
			case unicode.IsLetter(r):
				return unicode.ToLower(r)
			case unicode.IsNumber(r):
				return r
			case r == '/':
				return '_'
			}
		}
		return -1
	}, in)
}
