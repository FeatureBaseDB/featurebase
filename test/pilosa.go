// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	gohttp "net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/encoding/proto"
	"github.com/featurebasedb/featurebase/v3/server"
)

// //////////////////////////////////////////////////////////////////////////////////
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
func newCommand(tb DirCleaner, opts ...server.CommandOption) *Command {
	path := tb.TempDir()

	// Set aggressive close timeout by default to avoid hanging tests. This was
	// a problem with PDK tests which used pilosa/client as well. We put it at the
	// beginning of the option slice so that it can be overridden by user-passed
	// options.
	opts = append([]server.CommandOption{
		server.OptCommandCloseTimeout(time.Millisecond * 2),
	}, opts...)

	m := &Command{commandOptions: opts}
	output := io.Discard
	if testing.Verbose() {
		output = os.Stderr
	}
	m.Command = server.NewCommand(output, opts...)
	// pick etcd ports using a socket rather than a real port
	err := GetPortsGenConfigs(tb, []*Command{m})
	if err != nil {
		tb.Fatalf("generating config: %v", err)
	}
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

	return m
}

// NewCommandNode returns a new instance of Command with clustering enabled.
func NewCommandNode(tb DirCleaner, opts ...server.CommandOption) *Command {
	// We want tests to default to using the in-memory translate store, so we
	// prepend opts with that functional option. If a different translate store
	// has been specified, it will override this one.
	opts = prependTestServerOpts(opts)
	m := newCommand(tb, opts...)
	return m
}

// RunCommand returns a new, running Main. Panic on error.
func RunCommand(t *testing.T) *Command {
	t.Helper()

	// prefer MustRunCluster since it sets up for using etcd using
	// the GenDisCoConfig(size) option.
	return MustRunUnsharedCluster(t, 1).GetNode(0)
}

// Close closes the program and removes the underlying data directory.
func (m *Command) Close() error {
	// leave the removing part to the test logic. Some tests are closing and opening again the command
	// defer os.RemoveAll(m.Config.DataDir)
	return m.Command.Close()
}

// Reopen closes the program and reopens it.
func (m *Command) Reopen() error {
	if err := m.Command.Close(); err != nil {
		return err
	}

	// Create new main with the same config.
	config := m.Command.Config
	output := io.Discard
	if testing.Verbose() {
		output = os.Stderr
	}
	m.Command = server.NewCommand(output, m.commandOptions...)
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

// URL returns the base URL string for accessing the running program.
func (m *Command) URL() string { return m.API.Node().URI.String() }

// ID returns the node ID used by the running program.
func (m *Command) ID() string { return m.API.Node().ID }

// IsPrimary returns true if this is the primary.
func (m *Command) IsPrimary() bool {
	coord := m.API.PrimaryNode()
	if coord == nil {
		return false
	}
	return coord.ID == m.API.Node().ID
}

// Client returns a client to connect to the program.
func (m *Command) Client() *pilosa.InternalClient {
	return m.Server.InternalClient()
}

// Query executes a query against the program through the HTTP API.
func (m *Command) Query(t testing.TB, index, rawQuery, query string) (string, error) {
	resp := Do(t, "POST", fmt.Sprintf("%s/index/%s/query?%s", m.URL(), index, rawQuery), query)
	if resp.StatusCode != gohttp.StatusOK {
		return "", fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return resp.Body, nil
}

// Queryf is like Query, but with a format string.
func (m *Command) Queryf(t *testing.T, index, rawQuery, query string, params ...interface{}) (string, error) {
	query = fmt.Sprintf(query, params...)
	resp := Do(t, "POST", fmt.Sprintf("%s/index/%s/query?%s", m.URL(), index, rawQuery), query)
	if resp.StatusCode != gohttp.StatusOK {
		return "", fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return resp.Body, nil
}

// QueryExpect executes a query against the program through the HTTP API, and
// confirms that it got an expected response.
func (m *Command) QueryExpect(t *testing.T, index, rawQuery, query string, expected string) {
	resp := Do(t, "POST", fmt.Sprintf("%s/index/%s/query?%s", m.URL(), index, rawQuery), query)
	if resp.StatusCode != gohttp.StatusOK {
		t.Fatalf("invalid status from %s: %d, body=%q", m.ID(), resp.StatusCode, resp.Body)
	}
	last := len(resp.Body) - 1
	// Trim trailing newline so we don't need it to be present in the expected data.
	if last >= 0 && resp.Body[last] == '\n' {
		resp.Body = resp.Body[:last]
	}

	if resp.Body != expected {
		t.Fatalf("node %s, query %q: expected response %s, got %s", m.ID(), query, expected, resp.Body)
	}
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

	buf, err := io.ReadAll(resp.Body)
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

// Do executes http.Do() with an http.NewRequest().
func Do(t testing.TB, method, urlStr string, body string) *httpResponse {
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

	// set a timeout instead of allowing gohttp.Defaultclient to
	// potentially hang forever.
	hc := &gohttp.Client{
		Timeout: time.Second * 30,
	}
	resp, err := hc.Do(req)

	if err != nil {
		fmt.Printf(" hc.Do() err = '%v'\n", err)
		t.Fatal(err)
	}
	defer resp.Body.Close()

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return &httpResponse{Response: resp, Body: string(buf)}
}

// Do executes http.Do() with an http.NewRequest().
func DoProto(t testing.TB, method, urlStr string, body []byte) *gohttp.Response {
	t.Helper()
	req, err := gohttp.NewRequest(
		method,
		urlStr,
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")

	// set a timeout instead of allowing gohttp.Defaultclient to
	// potentially hang forever.
	hc := &gohttp.Client{
		Timeout: time.Second * 30,
	}
	resp, err := hc.Do(req)

	if err != nil {
		fmt.Printf(" hc.Do() err = '%v'\n", err)
		t.Fatal(err)
	}

	return resp
}

func CheckGroupBy(t *testing.T, expected, results []pilosa.GroupCount) {
	t.Helper()
	if len(results) != len(expected) {
		t.Fatalf("number of groupings mismatch:\n got:%+v\nwant:%+v\n", results, expected)
	}
	for i, result := range results {
		// have to check each field Row individually because FieldOptions is getting set
		for j := range expected[i].Group {
			// Field:"ppa", RowID:0x3, RowKey:"", Value:(*int64)(nil), FieldOptions:
			if !reflect.DeepEqual(expected[i].Group[j].Field, result.Group[j].Field) {
				t.Fatalf("unexpected result at %d: \n got:%+v\nwant:%+v\n", i, result, expected[i])
			}
			if !reflect.DeepEqual(expected[i].Group[j].RowKey, result.Group[j].RowKey) {
				t.Fatalf("unexpected result at %d: \n got:%+v\nwant:%+v\n", i, result, expected[i])
			}
			if !reflect.DeepEqual(expected[i].Group[j].Value, result.Group[j].Value) {
				t.Fatalf("unexpected result at %d: \n got:%+v\nwant:%+v\n", i, result, expected[i])
			}
		}

		if !reflect.DeepEqual(expected[i].Count, result.Count) {
			t.Fatalf("unexpected result at %d: \n got:%+v\nwant:%+v\n", i, result, expected[i])
		}
		if !reflect.DeepEqual(expected[i].Agg, result.Agg) {
			t.Fatalf("unexpected result at %d: \n got:%+v\nwant:%+v\n", i, result, expected[i])
		}
		if !reflect.DeepEqual(expected[i].DecimalAgg, result.DecimalAgg) {
			t.Fatalf("unexpected result at %d: \n got:%+v\nwant:%+v\n", i, result, expected[i])
		}
	}
}

// CheckGroupByOnKey is like CheckGroupBy, but it doen't enforce a match on the GroupBy.Group.RowID value.
// In cases where the Group has a RowKey, then the value of RowID is not consistently assigned. Instead,
// it depends on the order of key translation IDs based on shard allocation to the
func CheckGroupByOnKey(t *testing.T, expected, results []pilosa.GroupCount) {
	t.Helper()
	if len(results) != len(expected) {
		t.Fatalf("number of groupings mismatch:\n got:%+v\nwant:%+v\n", results, expected)
	}
	for i, result := range results {
		exp := expected[i]
		if len(exp.Group) != len(result.Group) {
			t.Fatalf("number of groups within GroupCount mismatch:\n got:%+v\nwant:%+v\n", result, exp)
		}
		if exp.Count != result.Count {
			t.Fatalf("GroupCount count mismatch:\n got:%+v\nwant:%+v\n", result, exp)
		}
		if exp.Agg != result.Agg {
			t.Fatalf("GroupCount aggregate mismatch:\n got:%+v\nwant:%+v\n", result, exp)
		}
		for j, grp := range result.Group {
			if grp.Field != exp.Group[j].Field || grp.RowKey != exp.Group[j].RowKey {
				t.Fatalf("GroupCount group value mismatch:\n got:%+v\nwant:%+v\n", result, exp)
			}
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

// AwaitState waits for the whole cluster to reach a specified state.
func (m *Command) AwaitState(expectedState disco.ClusterState, timeout time.Duration) (err error) {
	startTime := time.Now()
	var elapsed time.Duration
	for elapsed = 0; elapsed <= timeout; elapsed = time.Since(startTime) {
		// Counterintuitive: We're returning if the err *is* nil,
		// meaning we've reached the expected state.
		if err = m.exceptionalState(expectedState); err == nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("waited %v for command to reach state %q: %v",
		elapsed, expectedState, err)
}

// AssertState waits for the whole cluster to reach a specified state, or
// fails the calling test if it can't.
func (m *Command) AssertState(t testing.TB, expectedState disco.ClusterState, timeout time.Duration) {
	startTime := time.Now()
	var elapsed time.Duration
	var err error
	for elapsed = 0; elapsed <= timeout; elapsed = time.Since(startTime) {
		// Counterintuitive: We're returning if the err *is* nil,
		// meaning we've reached the expected state.
		if err = m.exceptionalState(expectedState); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("waited %v for command to reach state %q: %v", elapsed, expectedState, err)
}

// exceptionalState returns an error if the node is not in the expected state.
func (m *Command) exceptionalState(expectedState disco.ClusterState) error {
	state, err := m.API.State()
	if err != nil || state != expectedState {
		return fmt.Errorf("node %q: state %s: err %v", m.ID(), state, err)
	}
	return nil
}
