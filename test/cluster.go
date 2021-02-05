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

package test

import (
	"context"
	"fmt"
	"math"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/api/client"
	"github.com/pilosa/pilosa/v2/proto"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/storage"
	"github.com/pilosa/pilosa/v2/test/port"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// modHasher represents a simple, mod-based hashing.
type ModHasher struct{}

func (*ModHasher) Hash(key uint64, n int) int { return int(key) % n }

func (*ModHasher) Name() string { return "mod" }

// Cluster represents a Pilosa cluster (multiple Command instances)
type Cluster struct {
	Nodes []*Command
}

// Query executes an API.Query through one of the cluster's node's API. It fails
// the test if there is an error.
func (c *Cluster) Query(t testing.TB, index, query string) pilosa.QueryResponse {
	t.Helper()
	if len(c.Nodes) == 0 {
		t.Fatal("must have at least one node in cluster to query")
	}

	return c.GetCoordinator().QueryAPI(t, &pilosa.QueryRequest{Index: index, Query: query})
}

// QueryHTTP executes a PQL query through the HTTP endpoint. It fails
// the test for explicit errors, but returns an error which has the
// response body if the HTTP call returns a non-OK status.
func (c *Cluster) QueryHTTP(t testing.TB, index, query string) (string, error) {
	t.Helper()
	if len(c.Nodes) == 0 {
		t.Fatal("must have at least one node in cluster to query")
	}

	return c.GetCoordinator().Query(t, index, "", query)
}

// QueryGRPC executes a PQL query through the GRPC endpoint. It fails the
// test if there is an error.
func (c *Cluster) QueryGRPC(t testing.TB, index, query string) *proto.TableResponse {
	t.Helper()
	if len(c.Nodes) == 0 {
		t.Fatal("must have at least one node in cluster to query")
	}

	grpcClient, err := client.NewGRPCClient([]string{fmt.Sprintf("%s:%d", c.GetCoordinator().Server.GRPCURI().Host, c.GetCoordinator().Server.GRPCURI().Port)}, nil)
	if err != nil {
		t.Fatalf("getting GRPC client: %v", err)
	}

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
func (c *Cluster) GetIdleNode(n int) *Command {
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
func (c *Cluster) GetNode(n int) *Command {
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

// GetCoordinator gets the node which has been determined to be the coordinator.
// This used to be node0 in tests, but since implementing etcd, the coordinator
// can be any node in the cluster, so we have to use this method in tests which
// need to act on the coordinator.
func (c *Cluster) GetCoordinator() *Command {
	for _, n := range c.Nodes {
		if n.IsPrimary() {
			return n
		}
	}
	return nil
}

// GetNonCoordinator gets first first non-coordinator node in the list of nodes.
func (c *Cluster) GetNonCoordinator() *Command {
	for _, n := range c.Nodes {
		if !n.IsPrimary() {
			return n
		}
	}
	return nil
}

// GetNonCoordinators gets all nodes except the coordinator.
func (c *Cluster) GetNonCoordinators() []*Command {
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

func (c *Cluster) GetHolder(n int) *Holder {
	return &Holder{Holder: c.GetNode(n).Server.Holder()}
}

// GetCoordinatorHolder returns the Holder for the coordinator node.
func (c *Cluster) GetCoordinatorHolder() *Holder {
	return &Holder{Holder: c.GetCoordinator().Server.Holder()}
}

// GetNonCoordinatorHolder returns the Holder for the the first non-coordinator
// node in the list of nodes.
func (c *Cluster) GetNonCoordinatorHolder() *Holder {
	return &Holder{Holder: c.GetNonCoordinator().Server.Holder()}
}

func (c *Cluster) Len() int {
	return len(c.Nodes)
}

func (c *Cluster) ImportBits(t testing.TB, index, field string, rowcols [][2]uint64) {
	t.Helper()
	byShard := make(map[uint64][][2]uint64)
	for _, rowcol := range rowcols {
		shard := rowcol[1] / pilosa.ShardWidth
		byShard[shard] = append(byShard[shard], rowcol)
	}

	for shard, bits := range byShard {
		rowIDs := make([]uint64, len(bits))
		colIDs := make([]uint64, len(bits))
		for i, bit := range bits {
			rowIDs[i] = bit[0]
			colIDs[i] = bit[1]
		}
		nodes, err := c.GetCoordinator().API.ShardNodes(context.Background(), index, shard)
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

				err := com.API.Import(context.Background(), nil, &pilosa.ImportRequest{
					Index:     index,
					Field:     field,
					Shard:     shard,
					RowIDs:    rowIDs,
					ColumnIDs: colIDs,
				})
				if err != nil {
					t.Fatalf("importing data: %v", err)
				}
			}
		}
	}
}

// ImportKeyKey imports data into an index where both the index and
// the field are using string keys.
func (c *Cluster) ImportKeyKey(t testing.TB, index, field string, valAndRecKeys [][2]string) {
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
	err := c.GetCoordinator().API.Import(context.Background(), nil, importRequest)
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
func (c *Cluster) ImportTimeQuantumKey(t testing.TB, index, field string, entries []TimeQuantumKey) {
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
	err := c.GetCoordinator().API.Import(context.Background(), nil, importRequest)
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
func (c *Cluster) ImportIntKey(t testing.TB, index, field string, pairs []IntKey) {
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
	if err := c.GetCoordinator().API.ImportValue(context.Background(), nil, importRequest); err != nil {
		t.Fatalf("importing IntKey data: %v", err)
	}
}

type IntID struct {
	Val int64
	ID  uint64
}

// ImportIntID imports data into an int field in an unkeyed index.
func (c *Cluster) ImportIntID(t testing.TB, index, field string, pairs []IntID) {
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
	if err := c.GetCoordinator().API.ImportValue(context.Background(), nil, importRequest); err != nil {
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
func (c *Cluster) ImportIDKey(t testing.TB, index, field string, pairs []KeyID) {
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
	err := c.GetCoordinator().API.Import(context.Background(), nil, importRequest)
	if err != nil {
		t.Fatalf("importing IDKey data: %v", err)
	}
}

// CreateField creates the index (if necessary) and field specified.
func (c *Cluster) CreateField(t testing.TB, index string, iopts pilosa.IndexOptions, field string, fopts ...pilosa.FieldOption) *pilosa.Field {
	t.Helper()
	idx, err := c.GetCoordinator().API.CreateIndex(context.Background(), index, iopts)
	if err != nil && !strings.Contains(err.Error(), "index already exists") {
		t.Fatalf("creating index: %v", err)
	} else if err != nil { // index exists
		idx, err = c.GetCoordinator().API.Index(context.Background(), index)
		if err != nil {
			t.Fatalf("getting index: %v", err)
		}
	}
	if idx.Options() != iopts {
		t.Logf("existing index options:\n%v\ndon't match given opts:\n%v\n in pilosa/test.Cluster.CreateField", idx.Options(), iopts)
	}

	f, err := c.GetCoordinator().API.CreateField(context.Background(), index, field, fopts...)
	// we'll assume the field doesn't exist because checking if the options
	// match seems painful.
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	return f
}

// Start runs a Cluster
func (c *Cluster) Start() error {
	var eg errgroup.Group
	err := port.GetListeners(

		func(lsns []*net.TCPListener) (err0 error) {
			sliceOfPorts := NewPorts(lsns)
			defer func() {
				if err0 != nil {
					// going to retry. Close the still open listeners
					for _, ports := range sliceOfPorts {
						_ = ports.Close()
					}
				}
			}()
			portsCfg := GenPortsConfig(sliceOfPorts)

			for i, cc := range c.Nodes {
				cc := cc
				cc.Config.Etcd = portsCfg[i].Etcd
				cc.Config.Name = portsCfg[i].Name
				cc.Config.Cluster.Name = portsCfg[i].Cluster.Name
				cc.Config.BindGRPC = portsCfg[i].BindGRPC

				eg.Go(func() error {
					return cc.Start()
				})
			}

			return eg.Wait()
		}, 3*len(c.Nodes), 10)

	if err != nil {
		return err
	}

	return c.GetNode(0).AwaitState(string(pilosa.ClusterStateNormal), 30*time.Second)
}

// Close stops a Cluster
func (c *Cluster) Close() error {
	for i, cc := range c.Nodes {
		if err := cc.Close(); err != nil {
			return errors.Wrapf(err, "stopping server %d", i)
		}
	}
	return nil
}

func (c *Cluster) CloseAndRemoveNonCoordinator() error {
	for i, n := range c.Nodes {
		if !n.IsPrimary() {
			return c.CloseAndRemove(i)
		}
	}
	return errors.New("could not find non-coordinator node")
}

func (c *Cluster) CloseAndRemove(n int) error {
	if n < 0 || n >= len(c.Nodes) {
		return fmt.Errorf("close/remove from cluster: index %d out of range (len %d)", n, len(c.Nodes))
	}
	err := c.Nodes[n].Close()
	copy(c.Nodes[n:], c.Nodes[n+1:])
	c.Nodes = c.Nodes[:len(c.Nodes)-1]
	return err
}

// MustNewCluster creates a new cluster. If opts contains only one
// slice of command options, those options are used with every node.
// If it is empty, default options are used. Otherwise, it must contain size
// slices of command options, which are used with corresponding nodes.
func MustNewCluster(tb testing.TB, size int, opts ...[]server.CommandOption) *Cluster {
	tb.Helper()

	// We want tests to default to using the in-memory translate store, so we
	// prepend opts with that functional option. If a different translate store
	// has been specified, it will override this one.
	opts = prependOpts(opts, size)

	c, err := newCluster(tb, size, opts...)
	if err != nil {
		tb.Fatalf("new cluster: %v", err)
	}
	return c
}

// CheckClusterState polls a given cluster for its state until it
// receives a matching state. It polls up to n times before returning.
func CheckClusterState(m *Command, state string, n int) bool {
	for i := 0; i < n; i++ {

		apiState, err := m.API.State()
		if err != nil {
			return false
		}
		if apiState == state {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// newCluster creates a new cluster
func newCluster(tb testing.TB, size int, opts ...[]server.CommandOption) (*Cluster, error) {
	if size == 0 {
		return nil, errors.New("cluster must contain at least one node")
	}

	if len(opts) != size && len(opts) != 0 && len(opts) != 1 {
		return nil, errors.New("Slice of CommandOptions must be of length 0, 1, or equal to the number of cluster nodes")
	}

	cluster := &Cluster{Nodes: make([]*Command, size)}
	for i := 0; i < size; i++ {
		var commandOpts []server.CommandOption
		if len(opts) > 0 {
			commandOpts = opts[i%len(opts)]
		}
		m := NewCommandNode(tb, commandOpts...)
		cluster.Nodes[i] = m
	}

	return cluster, nil
}

// MustRunCluster creates and starts a new cluster. The opts parameter
// is slightly magical; see MustNewCluster.
func MustRunCluster(tb testing.TB, size int, opts ...[]server.CommandOption) *Cluster {
	cluster := MustNewCluster(tb, size, opts...)
	err := cluster.Start()
	if err != nil {
		tb.Fatalf("run cluster: %v", err)
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

// prependTestServerOpts prepends opts with the OpenInMemTranslateStore.
func prependTestServerOpts(opts []server.CommandOption) []server.CommandOption {
	defaultOpts := []server.CommandOption{
		server.OptCommandServerOptions(
			pilosa.OptServerOpenTranslateStore(pilosa.OpenInMemTranslateStore),
			pilosa.OptServerNodeDownRetries(5, 100*time.Millisecond),
			pilosa.OptServerStorageConfig(&storage.Config{
				Backend:      pilosa.CurrentBackendOrDefault(),
				FsyncEnabled: true,
			}),
		),
	}
	return append(defaultOpts, opts...)
}
