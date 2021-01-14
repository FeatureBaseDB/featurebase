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
	"io/ioutil"
	"math"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/api/client"
	"github.com/pilosa/pilosa/v2/proto"
	"github.com/pilosa/pilosa/v2/server"
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

	return c.Nodes[0].QueryAPI(t, &pilosa.QueryRequest{Index: index, Query: query})
}

// QueryHTTP executes a PQL query through the HTTP endpoint. It fails
// the test for explicit errors, but returns an error which has the
// response body if the HTTP call returns a non-OK status.
func (c *Cluster) QueryHTTP(t testing.TB, index, query string) (string, error) {
	t.Helper()
	if len(c.Nodes) == 0 {
		t.Fatal("must have at least one node in cluster to query")
	}

	return c.Nodes[0].Query(t, index, "", query)
}

// QueryGRPC executes a PQL query through the GRPC endpoint. It fails the
// test if there is an error.
func (c *Cluster) QueryGRPC(t testing.TB, index, query string) *proto.TableResponse {
	t.Helper()
	if len(c.Nodes) == 0 {
		t.Fatal("must have at least one node in cluster to query")
	}

	grpcClient, err := client.NewGRPCClient([]string{fmt.Sprintf("%s:%d", c.Nodes[0].Server.GRPCURI().Host, c.Nodes[0].Server.GRPCURI().Port)}, nil)
	if err != nil {
		t.Fatalf("getting GRPC client: %v", err)
	}

	tableResp, err := grpcClient.QueryUnary(context.Background(), index, query)
	if err != nil {
		t.Fatalf("querying unary: %v", err)
	}

	return tableResp
}

func (c *Cluster) GetNode(n int) *Command {
	return c.Nodes[n]
}

func (c *Cluster) GetHolder(n int) *Holder {
	return &Holder{Holder: c.Nodes[n].Server.Holder()}
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
		nodes, err := c.Nodes[0].API.ShardNodes(context.Background(), index, shard)
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
	err := c.Nodes[0].API.Import(context.Background(), nil, importRequest)
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
	if err := c.Nodes[0].API.ImportValue(context.Background(), nil, importRequest); err != nil {
		t.Fatalf("importing IntKey data: %v", err)
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
	err := c.Nodes[0].API.Import(context.Background(), nil, importRequest)
	if err != nil {
		t.Fatalf("importing IDKey data: %v", err)
	}
}

// CreateField creates the index (if necessary) and field specified.
func (c *Cluster) CreateField(t testing.TB, index string, iopts pilosa.IndexOptions, field string, fopts ...pilosa.FieldOption) *pilosa.Field {
	t.Helper()
	idx, err := c.Nodes[0].API.CreateIndex(context.Background(), index, iopts)
	if err != nil && !strings.Contains(err.Error(), "index already exists") {
		t.Fatalf("creating index: %v", err)
	} else if err != nil { // index exists
		idx, err = c.Nodes[0].API.Index(context.Background(), index)
		if err != nil {
			t.Fatalf("getting index: %v", err)
		}
	}
	if idx.Options() != iopts {
		t.Logf("existing index options:\n%v\ndon't match given opts:\n%v\n in pilosa/test.Cluster.CreateField", idx.Options(), iopts)
	}

	f, err := c.Nodes[0].API.CreateField(context.Background(), index, field, fopts...)
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
	err := port.GetPorts(func(ports []int) error {
		portsCfg := GenPortsConfig(NewPorts(ports))

		var gossipSeeds []string
		for i, cc := range c.Nodes {
			i := i
			// get the bind uri to use as the host portion of the gossip seed.
			uri, err := pilosa.AddressWithDefaults(cc.Config.Bind)
			if err != nil {
				return errors.Wrap(err, "processing bind address")
			}

			cc.Config.Gossip.Port = portsCfg[i].Gossip.Port
			gossipHost := uri.Host
			gossipPort := cc.Config.Gossip.Port

			gossipSeeds = append(gossipSeeds, fmt.Sprintf("%s:%s", gossipHost, gossipPort))
		}

		for i, cc := range c.Nodes {
			cc := cc
			cc.Config.DisCo = portsCfg[i].DisCo
			cc.Config.BindGRPC = portsCfg[i].BindGRPC

			eg.Go(func() error {
				fmt.Printf("DISCO CONFIG: %+v\n", cc.Config.DisCo)
				cc.Config.Gossip.Seeds = gossipSeeds

				return cc.Start()
			})
			// fixes race on gossip: time.Sleep(time.Second)
		}

		return eg.Wait()
	}, 4*len(c.Nodes), 10)
	if err != nil {
		return err
	}

	return c.AwaitState(pilosa.ClusterStateNormal, 10*time.Second)
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

func (c *Cluster) CloseAndRemove(n int) error {
	if n < 0 || n >= len(c.Nodes) {
		return fmt.Errorf("close/remove from cluster: index %d out of range (len %d)", n, len(c.Nodes))
	}
	err := c.Nodes[n].Close()
	copy(c.Nodes[n:], c.Nodes[n+1:])
	c.Nodes = c.Nodes[:len(c.Nodes)-1]
	return err
}

// AwaitState waits for the cluster coordinator (assumed to be the first
// node) to reach a specified state.
func (c *Cluster) AwaitCoordinatorState(expectedState string, timeout time.Duration) error {
	if len(c.Nodes) < 1 {
		return errors.New("can't await coordinator state on an empty cluster")
	}
	onlyCoordinator := &Cluster{Nodes: c.Nodes[:1]}
	return onlyCoordinator.AwaitState(expectedState, timeout)
}

// ExceptionalState returns an error if any node in the cluster is not
// in the expected state.
func (c *Cluster) ExceptionalState(expectedState string) error {
	for _, node := range c.Nodes {
		state := node.API.State()
		if state != expectedState {
			return fmt.Errorf("node %q: state %s", node.ID(), state)
		}
	}
	return nil
}

// AwaitState waits for the whole cluster to reach a specified state.
func (c *Cluster) AwaitState(expectedState string, timeout time.Duration) (err error) {
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
		time.Sleep(1 * time.Millisecond)
	}
	return fmt.Errorf("waited %v for cluster to reach state %q: %v",
		elapsed, expectedState, err)
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
		if m.API.State() == state {
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

	//opts = appendOpts(opts, GenDisCoConfig(size))

	if len(opts) != size && len(opts) != 0 && len(opts) != 1 {
		return nil, errors.New("Slice of CommandOptions must be of length 0, 1, or equal to the number of cluster nodes")
	}

	cluster := &Cluster{Nodes: make([]*Command, size)}
	name := tb.Name()
	for i := 0; i < size; i++ {
		var commandOpts []server.CommandOption
		if len(opts) > 0 {
			commandOpts = opts[i%len(opts)]
		}
		m := NewCommandNode(tb, i == 0, commandOpts...)
		err := ioutil.WriteFile(path.Join(m.Config.DataDir, ".id"), []byte(name+"__"+strconv.Itoa(i)), 0600)
		if err != nil {
			return nil, errors.Wrap(err, "writing node id")
		}
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
		println("len opts == 1, size = ", size)
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
		server.OptCommandServerOptions(pilosa.OptServerOpenTranslateStore(pilosa.OpenInMemTranslateStore), pilosa.OptServerNodeDownRetries(5, 100*time.Millisecond)),
	}
	return append(defaultOpts, opts...)
}
