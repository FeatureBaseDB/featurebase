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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	gohttp "net/http"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/encoding/proto"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////////////////
// Command represents a test wrapper for server.Command.
type Command struct {
	*server.Command

	commandOptions []server.CommandOption
}

func OptAllowedOrigins(origins []string) server.CommandOption {
	return func(m *server.Command) error {
		m.Config.Handler.AllowedOrigins = origins
		return nil
	}
}

// newCommand returns a new instance of Main with a temporary data directory and random port.
func newCommand(opts ...server.CommandOption) *Command {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	// Set aggressive close timeout by default to avoid hanging tests. This was
	// a problem with PDK tests which used go-pilosa as well. We put it at the
	// beginning of the option slice so that it can be overridden by user-passed
	// options.
	// Also set TranslateFile MapSize to a smaller number so memory allocation
	// does not fail on 32-bit systems.
	opts = append([]server.CommandOption{
		server.OptCommandCloseTimeout(time.Millisecond * 2),
	}, opts...)
	m := &Command{commandOptions: opts}
	m.Command = server.NewCommand(bytes.NewReader(nil), ioutil.Discard, ioutil.Discard, opts...)
	m.Config.DataDir = path
	defaultConf := server.NewConfig()
	if m.Config.Bind == defaultConf.Bind {
		m.Config.Bind = "http://localhost:0"
	}
	if m.Config.BindGRPC == defaultConf.BindGRPC {
		m.Config.BindGRPC = "http://localhost:0"
	}
	m.Config.Translation.MapSize = 140000
	m.Config.WorkerPoolSize = 2

	if testing.Verbose() {
		m.Command.Stdout = os.Stdout
		m.Command.Stderr = os.Stderr
	}

	return m
}

// NewCommandNode returns a new instance of Command with clustering enabled.
func NewCommandNode(isCoordinator bool, opts ...server.CommandOption) *Command {
	// We want tests to default to using the in-memory translate store, so we
	// prepend opts with that functional option. If a different translate store
	// has been specified, it will override this one.
	opts = prependWithMemStore(opts)
	m := newCommand(opts...)
	m.Config.Cluster.Disabled = false
	m.Config.Cluster.Coordinator = isCoordinator
	return m
}

// RunCommand returns a new, running Main. Panic on error.
func RunCommand(t *testing.T) *Command {
	t.Helper()
	m := newCommand(server.OptCommandServerOptions(pilosa.OptServerOpenTranslateStore(pilosa.OpenInMemTranslateStore)))
	m.Config.Metric.Diagnostics = false // Disable diagnostics.
	m.Config.Gossip.Port = "0"
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	return m
}

// GossipAddress returns the address on which gossip is listening after a Main
// has been setup. Useful to pass as a seed to other nodes when creating and
// testing clusters.
func (m *Command) GossipAddress() string {
	return m.GossipTransport().URI.String()
}

// Close closes the program and removes the underlying data directory.
func (m *Command) Close() error {
	defer os.RemoveAll(m.Config.DataDir)
	return m.Command.Close()
}

// Reopen closes the program and reopens it.
func (m *Command) Reopen() error {
	if err := m.Command.Close(); err != nil {
		return err
	}

	// Create new main with the same config.
	config := m.Command.Config
	m.Command = server.NewCommand(bytes.NewReader(nil), ioutil.Discard, ioutil.Discard, m.commandOptions...)
	m.Command.Config = config

	// Run new program.
	return m.Start()
}

// SoftOpen is like Reopen, but doesn't close the program first.
// This is useful in the case where a test needs to decouple the
// close from the re-open (for example, there may need to be
// actions which take place between those two steps).
func (m *Command) SoftOpen() error {
	// Create new main with the same config.
	config := m.Command.Config
	m.Command = server.NewCommand(bytes.NewReader(nil), ioutil.Discard, ioutil.Discard, m.commandOptions...)
	m.Command.Config = config

	// Run new program.
	return m.Start()
}

// MustCreateIndex uses this command's API to create an index and fails the test
// if there is an error.
func (m *Command) MustCreateIndex(tb testing.TB, name string, opts pilosa.IndexOptions) *pilosa.Index {
	tb.Helper()
	idx, err := m.API.CreateIndex(context.Background(), name, opts)
	if err != nil {
		tb.Fatalf("creating index: %v with options: %v, err: %v", name, opts, err)
	}
	return idx
}

// MustCreateField uses this command's API to create the field. The index must
// already exist - it fails the test if there is an error.
func (m *Command) MustCreateField(tb testing.TB, index, field string, opts ...pilosa.FieldOption) *pilosa.Field {
	tb.Helper()
	f, err := m.API.CreateField(context.Background(), index, field, opts...)
	if err != nil {
		tb.Fatalf("creating field: %s in index: %s err: %v", field, index, err)
	}
	return f
}

// QueryAPI uses this command's API to execute the given query request, failing
// if Query returns a non-nil error, otherwise returning the QueryResponse.
func (m *Command) QueryAPI(tb testing.TB, req *pilosa.QueryRequest) pilosa.QueryResponse {
	tb.Helper()
	resp, err := m.API.Query(context.Background(), req)
	if err != nil {
		tb.Fatalf("making query: %v, err: %v", req, err)
	}
	return resp
}

// MustRecalculateCaches calls RecalculateCaches on the command's API, and fails
// if there is an error.
func (m *Command) MustRecalculateCaches(tb testing.TB) {
	err := m.API.RecalculateCaches(context.Background())
	if err != nil {
		tb.Fatalf("recalcluating caches: %v", err)
	}
}

// URL returns the base URL string for accessing the running program.
func (m *Command) URL() string { return m.API.Node().URI.String() }

// Client returns a client to connect to the program.
func (m *Command) Client() *http.InternalClient {
	return m.Server.InternalClient().(*http.InternalClient)
}

// Query executes a query against the program through the HTTP API.
func (m *Command) Query(t *testing.T, index, rawQuery, query string) (string, error) {
	resp := Do(t, "POST", m.URL()+fmt.Sprintf("/index/%s/query?", index)+rawQuery, query)
	if resp.StatusCode != gohttp.StatusOK {
		return "", fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return resp.Body, nil
}

func (m *Command) QueryProtobuf(indexName string, query string) (*pilosa.QueryResponse, error) {
	var ser proto.Serializer
	queryReq := &pilosa.QueryRequest{
		Index: indexName,
		Query: query,
	}
	body, err := ser.Marshal(queryReq)
	if err != nil {
		return nil, err
	}

	req, err := gohttp.NewRequest(
		"POST",
		fmt.Sprintf("%s/index/%s/query", m.URL(), indexName),
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")

	resp, err := gohttp.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	response := &pilosa.QueryResponse{}
	err = ser.Unmarshal(buf, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// RecalculateCaches is deprecated. Use MustRecalculateCaches.
func (m *Command) RecalculateCaches(t *testing.T) error {
	resp := Do(t, "POST", fmt.Sprintf("%s/recalculate-caches", m.URL()), "")
	if resp.StatusCode != 204 {
		return fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return nil
}

// Cluster represents a Pilosa cluster (multiple Command instances)
type Cluster []*Command

// Query executes an API.Query through one of the cluster's node's API. It fails
// the test if there is an error.
func (c Cluster) Query(t testing.TB, index, query string) pilosa.QueryResponse {
	t.Helper()
	if len(c) == 0 {
		t.Fatal("must have at least one node in cluster to query")
	}

	return c[0].QueryAPI(t, &pilosa.QueryRequest{Index: index, Query: query})
}

func (c Cluster) ImportBits(t testing.TB, index, field string, rowcols [][2]uint64) {
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
		nodes, err := c[0].API.ShardNodes(context.Background(), index, shard)
		if err != nil {
			t.Fatalf("getting shard nodes: %v", err)
		}
		// TODO won't be necessary to do all nodes once that works hits
		for _, node := range nodes {
			for _, com := range c {
				if com.API.Node().ID != node.ID {
					continue
				}
				err := com.API.Import(context.Background(), &pilosa.ImportRequest{
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

// CreateField creates the index (if necessary) and field specified.
func (c Cluster) CreateField(t testing.TB, index string, iopts pilosa.IndexOptions, field string, fopts ...pilosa.FieldOption) *pilosa.Field {
	t.Helper()
	idx, err := c[0].API.CreateIndex(context.Background(), index, iopts)
	if err != nil && !strings.Contains(err.Error(), "index already exists") {
		t.Fatalf("creating index: %v", err)
	} else if err != nil { // index exists
		idx, err = c[0].API.Index(context.Background(), index)
		if err != nil {
			t.Fatalf("getting index: %v", err)
		}
	}
	if idx.Options() != iopts {
		t.Logf("existing index options:\n%v\ndon't match given opts:\n%v\n in pilosa/test.Cluster.CreateField", idx.Options(), iopts)
	}

	f, err := c[0].API.CreateField(context.Background(), index, field, fopts...)
	// we'll assume the field doesn't exist because checking if the options
	// match seems painful.
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	return f
}

// Start runs a Cluster
func (c Cluster) Start() error {
	var gossipSeeds = make([]string, len(c))
	for i, cc := range c {
		cc.Config.Gossip.Port = "0"
		cc.Config.Gossip.Seeds = gossipSeeds[:i]
		if err := cc.Start(); err != nil {
			return errors.Wrapf(err, "starting server %d", i)
		}
		gossipSeeds[i] = cc.GossipAddress()
	}
	return nil
}

// Stop stops a Cluster
func (c Cluster) Close() error {
	for i, cc := range c {
		if err := cc.Close(); err != nil {
			return errors.Wrapf(err, "stopping server %d", i)
		}
	}
	return nil
}

// MustNewCluster creates a new cluster
func MustNewCluster(tb testing.TB, size int, opts ...[]server.CommandOption) Cluster {
	tb.Helper()
	c, err := newCluster(size, opts...)
	if err != nil {
		tb.Fatalf("new cluster: %v", err)
	}
	return c
}

// newCluster creates a new cluster
func newCluster(size int, opts ...[]server.CommandOption) (Cluster, error) {
	if size == 0 {
		return nil, errors.New("cluster must contain at least one node")
	}
	if len(opts) != size && len(opts) != 0 && len(opts) != 1 {
		return nil, errors.New("Slice of CommandOptions must be of length 0, 1, or equal to the number of cluster nodes")
	}

	cluster := make(Cluster, size)
	for i := 0; i < size; i++ {
		var commandOpts []server.CommandOption
		if len(opts) > 0 {
			commandOpts = opts[i%len(opts)]
		}
		m := NewCommandNode(i == 0, commandOpts...)
		err := ioutil.WriteFile(path.Join(m.Config.DataDir, ".id"), []byte("node"+strconv.Itoa(i)), 0600)
		if err != nil {
			return nil, errors.Wrap(err, "writing node id")
		}
		cluster[i] = m
	}

	return cluster, nil
}

// runCluster creates and starts a new cluster
func runCluster(size int, opts ...[]server.CommandOption) (Cluster, error) {
	cluster, err := newCluster(size, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "new cluster")
	}

	if err = cluster.Start(); err != nil {
		return nil, errors.Wrap(err, "starting cluster")
	}
	return cluster, nil
}

// MustRunCluster creates and starts a new cluster
func MustRunCluster(tb testing.TB, size int, opts ...[]server.CommandOption) Cluster {
	// We want tests to default to using the in-memory translate store, so we
	// prepend opts with that functional option. If a different translate store
	// has been specified, it will override this one.
	opts = prependOpts(opts)

	tb.Helper()
	c, err := runCluster(size, opts...)
	if err != nil {
		tb.Fatalf("run cluster: %v", err)
	}
	return c
}

// prependOpts applies prependWithMemStore to each of the ops (one per
// node, or one for the entire cluser).
func prependOpts(opts [][]server.CommandOption) [][]server.CommandOption {
	if len(opts) == 0 {
		opts = [][]server.CommandOption{
			prependWithMemStore([]server.CommandOption{}),
		}
	} else {
		for i := range opts {
			opts[i] = prependWithMemStore(opts[i])
		}
	}
	return opts
}

// prependWithMemStore prepends opts with the OpenInMemTranslateStore.
func prependWithMemStore(opts []server.CommandOption) []server.CommandOption {
	defaultOpts := []server.CommandOption{
		server.OptCommandServerOptions(pilosa.OptServerOpenTranslateStore(pilosa.OpenInMemTranslateStore)),
	}
	return append(defaultOpts, opts...)
}

////////////////////////////////////////////////////////////////////////////////////

// Do executes http.Do() with an http.NewRequest().
func Do(t *testing.T, method, urlStr string, body string) *httpResponse {
	t.Helper()
	req, err := gohttp.NewRequest(
		method,
		urlStr,
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := gohttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return &httpResponse{Response: resp, Body: string(buf)}
}

func CheckGroupBy(t *testing.T, expected, results []pilosa.GroupCount) {
	t.Helper()
	if len(results) != len(expected) {
		t.Fatalf("number of groupings mismatch:\n got:%+v\nwant:%+v\n", results, expected)
	}
	for i, result := range results {
		if !reflect.DeepEqual(expected[i], result) {
			t.Fatalf("unexpected result at %d: \n got:%+v\nwant:%+v\n", i, result, expected[i])
		}
	}
}

// httpResponse is a wrapper for http.Response that holds the Body as a string.
type httpResponse struct {
	*gohttp.Response
	Body string
}

// RetryUntil repeatedly executes fn until it returns nil or timeout occurs.
func RetryUntil(timeout time.Duration, fn func() error) (err error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err = fn(); err == nil {
			return nil
		}

		select {
		case <-timer.C:
			return err
		case <-ticker.C:
		}
	}
}
