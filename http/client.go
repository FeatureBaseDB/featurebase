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

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/encoding/proto"
	"github.com/pilosa/pilosa/v2/tracing"
	"github.com/pkg/errors"
)

// InternalClient represents a client to the Pilosa cluster.
type InternalClient struct {
	defaultURI *pilosa.URI
	serializer pilosa.Serializer

	// The client to use for HTTP communication.
	httpClient *http.Client
}

// NewInternalClient returns a new instance of InternalClient to connect to host.
func NewInternalClient(host string, remoteClient *http.Client) (*InternalClient, error) {
	if host == "" {
		return nil, pilosa.ErrHostRequired
	}

	uri, err := pilosa.NewURIFromAddress(host)
	if err != nil {
		return nil, errors.Wrap(err, "getting URI")
	}

	client := NewInternalClientFromURI(uri, remoteClient)
	return client, nil
}

func NewInternalClientFromURI(defaultURI *pilosa.URI, remoteClient *http.Client) *InternalClient {
	return &InternalClient{
		defaultURI: defaultURI,
		serializer: proto.Serializer{},
		httpClient: remoteClient,
	}
}

// MaxShardByIndex returns the number of shards on a server by index.
func (c *InternalClient) MaxShardByIndex(ctx context.Context) (map[string]uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.MaxShardByIndex")
	defer span.Finish()
	return c.maxShardByIndex(ctx)
}

// maxShardByIndex returns the number of shards on a server by index.
func (c *InternalClient) maxShardByIndex(ctx context.Context) (map[string]uint64, error) {
	// Execute request against the host.
	u := uriPathToURL(c.defaultURI, "/internal/shards/max")

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)
	req.Header.Set("Accept", "application/json")

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rsp getShardsMaxResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}

	return rsp.Standard, nil
}

// Schema returns all index and field schema information.
func (c *InternalClient) Schema(ctx context.Context) ([]*pilosa.IndexInfo, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Schema")
	defer span.Finish()

	// Execute request against the host.
	u := c.defaultURI.Path("/schema")

	// Build request.
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)
	req.Header.Set("Accept", "application/json")

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rsp getSchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return rsp.Indexes, nil
}

// MutexCheck uses the mutex-check endpoint to request mutex collision data
// from a single node.
func (c *InternalClient) MutexCheck(ctx context.Context, uri *pilosa.URI, indexName string, fieldName string) (map[uint64]map[uint64][]uint64, error) {
	if uri == nil {
		uri = c.defaultURI
	}
	u := uri.Path(fmt.Sprintf("/internal/index/%s/field/%s/mutex-check", indexName, fieldName))
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected status code: %s", resp.Status)
	}
	var out map[uint64]map[uint64][]uint64
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&out)
	return out, err
}

func (c *InternalClient) PostSchema(ctx context.Context, uri *pilosa.URI, s *pilosa.Schema, remote bool) error {
	u := uri.Path(fmt.Sprintf("/schema?remote=%v", remote))
	buf, err := json.Marshal(s)
	if err != nil {
		return errors.Wrap(err, "marshalling schema")
	}
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return errors.Errorf("unexpected status code: %s", resp.Status)
	}
	return nil
}

// CreateIndex creates a new index on the server.
func (c *InternalClient) CreateIndex(ctx context.Context, index string, opt pilosa.IndexOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.CreateIndex")
	defer span.Finish()

	// Get the coordinator node. Schema changes must go through
	// coordinator to avoid weird race conditions.
	nodes, err := c.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("getting nodes: %s", err)
	}
	coord := getCoordinatorNode(nodes)
	if coord == nil {
		return fmt.Errorf("could not find the coordinator node")
	}

	// Encode query request.
	buf, err := json.Marshal(&postIndexRequest{
		Options: opt,
	})
	if err != nil {
		return errors.Wrap(err, "encoding request")
	}

	// Create URL & HTTP request.
	u := uriPathToURL(&coord.URI, fmt.Sprintf("/index/%s", index))
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusConflict {
			return pilosa.ErrIndexExists
		}
		return err
	}
	return errors.Wrap(resp.Body.Close(), "closing response body")
}

// FragmentNodes returns a list of nodes that own a shard.
func (c *InternalClient) FragmentNodes(ctx context.Context, index string, shard uint64) ([]*pilosa.Node, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FragmentNodes")
	defer span.Finish()

	// Execute request against the host.
	u := uriPathToURL(c.defaultURI, "/internal/fragment/nodes")
	u.RawQuery = (url.Values{"index": {index}, "shard": {strconv.FormatUint(shard, 10)}}).Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)
	req.Header.Set("Accept", "application/json")

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var a []*pilosa.Node
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return a, nil
}

// Nodes returns a list of all nodes.
func (c *InternalClient) Nodes(ctx context.Context) ([]*pilosa.Node, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Nodes")
	defer span.Finish()

	// Execute request against the host.
	u := uriPathToURL(c.defaultURI, "/internal/nodes")

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)
	req.Header.Set("Accept", "application/json")

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var a []*pilosa.Node
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return a, nil
}

// Query executes query against the index.
func (c *InternalClient) Query(ctx context.Context, index string, queryRequest *pilosa.QueryRequest) (*pilosa.QueryResponse, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Query")
	defer span.Finish()
	return c.QueryNode(ctx, c.defaultURI, index, queryRequest)
}

// QueryNode executes query against the index, sending the request to the node specified.
func (c *InternalClient) QueryNode(ctx context.Context, uri *pilosa.URI, index string, queryRequest *pilosa.QueryRequest) (*pilosa.QueryResponse, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "QueryNode")
	defer span.Finish()

	if index == "" {
		return nil, pilosa.ErrIndexRequired
	} else if queryRequest.Query == "" {
		return nil, pilosa.ErrQueryRequired
	}
	buf, err := c.serializer.Marshal(queryRequest)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling queryRequest")
	}

	// Create HTTP request.
	u := uri.Path(fmt.Sprintf("/index/%s/query", index))
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("X-Pilosa-Row", "roaring")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrapf(err, "'%s', shards %v", queryRequest.Query, queryRequest.Shards)
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	qresp := &pilosa.QueryResponse{}
	if err := c.serializer.Unmarshal(body, qresp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	} else if qresp.Err != nil {
		return nil, qresp.Err
	}

	return qresp, nil
}

// Import bulk imports bits for a single shard to a host.
func (c *InternalClient) Import(ctx context.Context, index, field string, shard uint64, bits []pilosa.Bit, opts ...pilosa.ImportOption) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Import")
	defer span.Finish()

	if index == "" {
		return pilosa.ErrIndexRequired
	} else if field == "" {
		return pilosa.ErrFieldRequired
	}

	// Set up import options.
	options := &pilosa.ImportOptions{}
	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return errors.Wrap(err, "applying option")
		}
	}

	buf, err := c.marshalImportPayload(index, field, shard, bits)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	// Retrieve a list of nodes that own the shard.
	nodes, err := c.FragmentNodes(ctx, index, shard)
	if err != nil {
		return fmt.Errorf("shard nodes: %s", err)
	}

	// Import to each node.
	for _, node := range nodes {
		if err := c.importNode(ctx, node, index, field, buf, options); err != nil {
			return fmt.Errorf("import node: host=%s, err=%s", node.URI, err)
		}
	}

	return nil
}

func getCoordinatorNode(nodes []*pilosa.Node) *pilosa.Node {
	for _, node := range nodes {
		if node.IsCoordinator {
			return node
		}
	}
	return nil
}

// ImportK bulk imports bits specified by string keys to a host.
func (c *InternalClient) ImportK(ctx context.Context, index, field string, bits []pilosa.Bit, opts ...pilosa.ImportOption) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ImportK")
	defer span.Finish()

	if index == "" {
		return pilosa.ErrIndexRequired
	} else if field == "" {
		return pilosa.ErrFieldRequired
	}

	// Set up import options.
	options := &pilosa.ImportOptions{}
	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return errors.Wrap(err, "applying option")
		}
	}

	buf, err := c.marshalImportPayload(index, field, 0, bits)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	// Get the coordinator node; all bits are sent to the
	// primary translate store (i.e. coordinator).
	// TODO... is that right^^?
	// RESPONSE: It looks like in ctl/import.go, we could change the
	// logic in ImportCommand.importBits() to only use ImportK
	// when useRowKeys = true. It's no longer necessary to
	// send column key translations to the coordinator (although
	// it should still work). As far as I know, the only thing
	// that uses ImportK is the pilosa import sub-command.
	nodes, err := c.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("getting nodes: %s", err)
	}
	coord := getCoordinatorNode(nodes)
	if coord == nil {
		return fmt.Errorf("could not find the coordinator node")
	}

	// Import to node.
	if err := c.importNode(ctx, coord, index, field, buf, options); err != nil {
		return fmt.Errorf("import node: host=%s, err=%s", coord.URI, err)
	}

	return nil
}

func (c *InternalClient) EnsureIndex(ctx context.Context, name string, options pilosa.IndexOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.EnsureIndex")
	defer span.Finish()

	err := c.CreateIndex(ctx, name, options)
	if err == nil || errors.Cause(err) == pilosa.ErrIndexExists {
		return nil
	}
	return err
}

func (c *InternalClient) EnsureField(ctx context.Context, indexName string, fieldName string) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.EnsureField")
	defer span.Finish()
	return c.EnsureFieldWithOptions(ctx, indexName, fieldName, pilosa.FieldOptions{})
}

func (c *InternalClient) EnsureFieldWithOptions(ctx context.Context, indexName string, fieldName string, opt pilosa.FieldOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.EnsureFieldWithOptions")
	defer span.Finish()
	err := c.CreateFieldWithOptions(ctx, indexName, fieldName, opt)
	if err == nil || errors.Cause(err) == pilosa.ErrFieldExists {
		return nil
	}
	return err
}

// marshalImportPayload marshalls the import parameters into a protobuf byte slice.
func (c *InternalClient) marshalImportPayload(index, field string, shard uint64, bits []pilosa.Bit) ([]byte, error) {
	// Separate row and column IDs to reduce allocations.
	rowIDs := Bits(bits).RowIDs()
	rowKeys := Bits(bits).RowKeys()
	columnIDs := Bits(bits).ColumnIDs()
	columnKeys := Bits(bits).ColumnKeys()
	timestamps := Bits(bits).Timestamps()

	// Marshal data to protobuf.
	buf, err := c.serializer.Marshal(&pilosa.ImportRequest{
		Index:      index,
		Field:      field,
		Shard:      shard,
		RowIDs:     rowIDs,
		RowKeys:    rowKeys,
		ColumnIDs:  columnIDs,
		ColumnKeys: columnKeys,
		Timestamps: timestamps,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal import request: %s", err)
	}
	return buf, nil
}

// importNode sends a pre-marshaled import request to a node.
func (c *InternalClient) importNode(ctx context.Context, node *pilosa.Node, index, field string, buf []byte, opts *pilosa.ImportOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.importNode")
	defer span.Finish()

	// Create URL & HTTP request.
	path := fmt.Sprintf("/index/%s/field/%s/import", index, field)
	u := nodePathToURL(node, path)

	vals := url.Values{}
	if opts.Clear {
		vals.Set("clear", "true")
	}
	if opts.IgnoreKeyCheck {
		vals.Set("ignoreKeyCheck", "true")
	}
	url := fmt.Sprintf("%s?%s", u.String(), vals.Encode())

	req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("X-Pilosa-Row", "roaring")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "reading")
	}

	var isresp pilosa.ImportResponse
	if err := c.serializer.Unmarshal(body, &isresp); err != nil {
		return fmt.Errorf("unmarshal import response: %s", err)
	} else if s := isresp.Err; s != "" {
		return errors.New(s)
	}

	return nil
}

// ImportValue bulk imports field values for a single shard to a host.
func (c *InternalClient) ImportValue(ctx context.Context, index, field string, shard uint64, vals []pilosa.FieldValue, opts ...pilosa.ImportOption) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ImportValue")
	defer span.Finish()

	if index == "" {
		return pilosa.ErrIndexRequired
	} else if field == "" {
		return pilosa.ErrFieldRequired
	}

	// Set up import options.
	options := &pilosa.ImportOptions{}
	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return errors.Wrap(err, "applying option")
		}
	}

	buf, err := c.marshalImportValuePayload(index, field, shard, vals)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	// Retrieve a list of nodes that own the shard.
	nodes, err := c.FragmentNodes(ctx, index, shard)
	if err != nil {
		return fmt.Errorf("shard nodes: %s", err)
	}

	// Import to each node.
	for _, node := range nodes {
		if err := c.importNode(ctx, node, index, field, buf, options); err != nil {
			return fmt.Errorf("import node: host=%s, err=%s", node.URI, err)
		}
	}

	return nil
}

// ImportValue2 is a simplified ImportValue method which just uses the
// ImportValueRequest instead of splitting up ImportValue and
// ImportValueK... it also supports importing float values. The idea
// being that (assuming it works) this will become the default (and be
// renamed) for 2.0, and we can deprecate the other methods.
func (c *InternalClient) ImportValue2(ctx context.Context, req *pilosa.ImportValueRequest, options *pilosa.ImportOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.NewImportValue")
	defer span.Finish()

	buf, err := c.serializer.Marshal(req)
	if err != nil {
		return errors.Errorf("marshal import request: %s", err)
	}

	// Retrieve a list of nodes that own the shard.
	nodes, err := c.FragmentNodes(ctx, req.Index, req.Shard)
	if err != nil {
		return errors.Errorf("shard nodes: %s", err)
	}

	// Import to each node.
	for _, node := range nodes {
		if err := c.importNode(ctx, node, req.Index, req.Field, buf, options); err != nil {
			return errors.Errorf("import node: host=%s, err=%s", node.URI, err)
		}
	}
	return nil
}

// ImportValueK bulk imports keyed field values to a host.
func (c *InternalClient) ImportValueK(ctx context.Context, index, field string, vals []pilosa.FieldValue, opts ...pilosa.ImportOption) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ImportValueK")
	defer span.Finish()

	buf, err := c.marshalImportValuePayload(index, field, 0, vals)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	// Set up import options.
	options := &pilosa.ImportOptions{}
	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return errors.Wrap(err, "applying option")
		}
	}

	// Get the coordinator node; all bits are sent to the
	// primary translate store (i.e. coordinator).
	nodes, err := c.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("getting nodes: %s", err)
	}
	coord := getCoordinatorNode(nodes)
	if coord == nil {
		return fmt.Errorf("could not find the coordinator node")
	}

	// Import to node.
	if err := c.importNode(ctx, coord, index, field, buf, options); err != nil {
		return fmt.Errorf("import node: host=%s, err=%s", coord.URI, err)
	}

	return nil
}

// marshalImportValuePayload marshalls the import parameters into a protobuf byte slice.
func (c *InternalClient) marshalImportValuePayload(index, field string, shard uint64, vals []pilosa.FieldValue) ([]byte, error) {
	// Separate row and column IDs to reduce allocations.
	columnIDs := FieldValues(vals).ColumnIDs()
	columnKeys := FieldValues(vals).ColumnKeys()
	values := FieldValues(vals).Values()

	// Marshal data to protobuf.
	buf, err := c.serializer.Marshal(&pilosa.ImportValueRequest{
		Index:      index,
		Field:      field,
		Shard:      shard,
		ColumnIDs:  columnIDs,
		ColumnKeys: columnKeys,
		Values:     values,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal import request: %s", err)
	}
	return buf, nil
}

// ImportRoaring does fast import of raw bits in roaring format (pilosa or
// official format, see API.ImportRoaring).
func (c *InternalClient) ImportRoaring(ctx context.Context, uri *pilosa.URI, index, field string, shard uint64, remote bool, req *pilosa.ImportRoaringRequest) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ImportRoaring")
	defer span.Finish()

	if index == "" {
		return pilosa.ErrIndexRequired
	} else if field == "" {
		return pilosa.ErrFieldRequired
	}
	if uri == nil {
		uri = c.defaultURI
	}

	vals := url.Values{}
	vals.Set("remote", strconv.FormatBool(remote))
	url := fmt.Sprintf("%s/index/%s/field/%s/import-roaring/%d?%s", uri, index, field, shard, vals.Encode())

	// Marshal data to protobuf.
	data, err := c.serializer.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshal import request")
	}

	// Generate HTTP request.
	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Accept", "application/x-protobuf")
	httpReq.Header.Set("X-Pilosa-Row", "roaring")
	httpReq.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	rbody := &pilosa.ImportResponse{}
	err = dec.Decode(rbody)
	// Decode can return EOF when no error occurred. helpful!
	if err != nil && err != io.EOF {
		return errors.Wrap(err, "decoding response body")
	}
	if rbody.Err != "" {
		return errors.Wrap(errors.New(rbody.Err), "importing roaring")
	}
	return nil
}

// ImportColumnAttrs does bulk import of column attrs
func (c *InternalClient) ImportColumnAttrs(ctx context.Context, uri *pilosa.URI, index string, req *pilosa.ImportColumnAttrsRequest) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ImportRoaring")
	defer span.Finish()

	if index == "" {
		return pilosa.ErrIndexRequired
	}
	if uri == nil {
		uri = c.defaultURI
	}

	url := fmt.Sprintf("%s/index/%s/import-column-attrs", uri, index)

	// Marshal data to protobuf.
	data, err := c.serializer.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshal import-column-attrs request")
	}

	// Generate HTTP request.
	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Accept", "application/x-protobuf")
	httpReq.Header.Set("X-Pilosa-Row", "roaring")
	httpReq.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	rbody := &pilosa.ImportResponse{}
	err = dec.Decode(rbody)
	// Decode can return EOF when no error occurred. helpful!
	if err != nil && err != io.EOF {
		return errors.Wrap(err, "decoding response body")
	}
	if rbody.Err != "" {
		return errors.Wrap(errors.New(rbody.Err), "importing roaring")
	}

	return nil
}

// ExportCSV bulk exports data for a single shard from a host to CSV format.
func (c *InternalClient) ExportCSV(ctx context.Context, index, field string, shard uint64, w io.Writer) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ExportCSV")
	defer span.Finish()

	if index == "" {
		return pilosa.ErrIndexRequired
	} else if field == "" {
		return pilosa.ErrFieldRequired
	}

	// Retrieve a list of nodes that own the shard.
	nodes, err := c.FragmentNodes(ctx, index, shard)
	if err != nil {
		return fmt.Errorf("shard nodes: %s", err)
	}

	// Attempt nodes in random order.
	var e error
	for _, i := range rand.Perm(len(nodes)) {
		node := nodes[i]

		if err := c.exportNodeCSV(ctx, node, index, field, shard, w); err != nil {
			e = fmt.Errorf("export node: host=%s, err=%s", node.URI, err)
			continue
		} else {
			return nil
		}
	}

	return e
}

// exportNode copies a CSV export from a node to w.
func (c *InternalClient) exportNodeCSV(ctx context.Context, node *pilosa.Node, index, field string, shard uint64, w io.Writer) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.exportNodeCSV")
	defer span.Finish()

	// Create URL.
	u := nodePathToURL(node, "/export")
	u.RawQuery = url.Values{
		"index": {index},
		"field": {field},
		"shard": {strconv.FormatUint(shard, 10)},
	}.Encode()

	// Generate HTTP request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Accept", "text/csv")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Copy body to writer.
	if _, err := io.Copy(w, resp.Body); err != nil {
		return errors.Wrap(err, "copying")
	}

	return nil
}

// RetrieveShardFromURI returns a ReadCloser which contains the data of the
// specified shard from the specified node. Caller *must* close the returned
// ReadCloser or risk leaking goroutines/tcp connections.
func (c *InternalClient) RetrieveShardFromURI(ctx context.Context, index, field, view string, shard uint64, uri pilosa.URI) (io.ReadCloser, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.RetrieveShardFromURI")
	defer span.Finish()

	node := &pilosa.Node{
		URI: uri,
	}

	u := nodePathToURL(node, "/internal/fragment/data")
	u.RawQuery = url.Values{
		"index": {index},
		"field": {field},
		"view":  {view},
		"shard": {strconv.FormatUint(shard, 10)},
	}.Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, pilosa.ErrFragmentNotFound
		}
		return nil, err
	}

	return resp.Body, nil
}

func (c *InternalClient) CreateField(ctx context.Context, index, field string) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.CreateField")
	defer span.Finish()
	return c.CreateFieldWithOptions(ctx, index, field, pilosa.FieldOptions{})
}

// CreateField creates a new field on the server.
func (c *InternalClient) CreateFieldWithOptions(ctx context.Context, index, field string, opt pilosa.FieldOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.CreateFieldWithOptions")
	defer span.Finish()

	if index == "" {
		return pilosa.ErrIndexRequired
	}

	// convert pilosa.FieldOptions to fieldOptions
	//
	// TODO this kind of sucks because it's one more place that needs
	// changes when we change anything with field options (and there
	// are a lot of places already). It's not clear to me that this is
	// providing a lot of value, but I think this kind of validation
	// should probably happen in the field anyway??
	fieldOpt := fieldOptions{
		Type: opt.Type,
		Keys: &opt.Keys,
	}
	if fieldOpt.Type == pilosa.FieldTypeSet {
		fieldOpt.CacheType = &opt.CacheType
		fieldOpt.CacheSize = &opt.CacheSize
	} else if fieldOpt.Type == pilosa.FieldTypeInt {
		fieldOpt.Min = &opt.Min
		fieldOpt.Max = &opt.Max
	} else if fieldOpt.Type == pilosa.FieldTypeTime {
		fieldOpt.TimeQuantum = &opt.TimeQuantum
	} else if fieldOpt.Type == pilosa.FieldTypeDecimal {
		fieldOpt.Min = &opt.Min
		fieldOpt.Max = &opt.Max
		fieldOpt.Scale = &opt.Scale
	}

	// TODO: remove buf completely? (depends on whether importer needs to create specific field types)
	// Encode query request.
	buf, err := json.Marshal(&postFieldRequest{
		Options: fieldOpt,
	})
	if err != nil {
		return errors.Wrap(err, "marshaling")
	}

	// Get the coordinator node. Schema changes must go through
	// coordinator to avoid weird race conditions.
	nodes, err := c.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("getting nodes: %s", err)
	}
	coord := getCoordinatorNode(nodes)
	if coord == nil {
		return fmt.Errorf("could not find the coordinator node")
	}

	// Create URL & HTTP request.
	u := uriPathToURL(&coord.URI, fmt.Sprintf("/index/%s/field/%s", index, field))
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusConflict {
			return pilosa.ErrFieldExists
		}
		return err
	}

	return errors.Wrap(resp.Body.Close(), "closing response body")
}

// FragmentBlocks returns a list of block checksums for a fragment on a host.
// Only returns blocks which contain data.
func (c *InternalClient) FragmentBlocks(ctx context.Context, uri *pilosa.URI, index, field, view string, shard uint64) ([]pilosa.FragmentBlock, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FragmentBlocks")
	defer span.Finish()

	if uri == nil {
		uri = c.defaultURI
	}
	u := uriPathToURL(uri, "/internal/fragment/blocks")
	u.RawQuery = url.Values{
		"index": {index},
		"field": {field},
		"view":  {view},
		"shard": {strconv.FormatUint(shard, 10)},
	}.Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)
	req.Header.Set("Accept", "application/json")

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		// Return the appropriate error.
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, pilosa.ErrFragmentNotFound
		}
		return nil, err
	}
	defer resp.Body.Close()

	// Decode response object.
	var rsp getFragmentBlocksResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, errors.Wrap(err, "decoding")
	}
	return rsp.Blocks, nil
}

// BlockData returns row/column id pairs for a block.
func (c *InternalClient) BlockData(ctx context.Context, uri *pilosa.URI, index, field, view string, shard uint64, block int) ([]uint64, []uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.BlockData")
	defer span.Finish()

	if uri == nil {
		panic("need to pass a URI to BlockData")
	}
	buf, err := c.serializer.Marshal(&pilosa.BlockDataRequest{
		Index: index,
		Field: field,
		View:  view,
		Shard: shard,
		Block: uint64(block),
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "marshaling")
	}

	u := uriPathToURL(uri, "/internal/fragment/block/data")
	req, err := http.NewRequest("GET", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, nil, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/protobuf")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Accept", "application/protobuf")
	req.Header.Set("X-Pilosa-Row", "roaring")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	defer resp.Body.Close()

	// Decode response object.
	var rsp pilosa.BlockDataResponse
	if body, err := ioutil.ReadAll(resp.Body); err != nil {
		return nil, nil, errors.Wrap(err, "reading")
	} else if err := c.serializer.Unmarshal(body, &rsp); err != nil {
		return nil, nil, errors.Wrap(err, "unmarshalling")
	}
	return rsp.RowIDs, rsp.ColumnIDs, nil
}

// ColumnAttrDiff returns data from differing blocks on a remote host.
func (c *InternalClient) ColumnAttrDiff(ctx context.Context, uri *pilosa.URI, index string, blks []pilosa.AttrBlock) (map[uint64]map[string]interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ColumnAttrDiff")
	defer span.Finish()

	if uri == nil {
		uri = c.defaultURI
	}
	u := uriPathToURL(uri, fmt.Sprintf("/internal/index/%s/attr/diff", index))

	// Encode request.
	buf, err := json.Marshal(postIndexAttrDiffRequest{Blocks: blks})
	if err != nil {
		return nil, errors.Wrap(err, "marshaling")
	}

	// Build request.
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)
	req.Header.Set("Accept", "application/json")

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Decode response object.
	var rsp postIndexAttrDiffResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, errors.Wrap(err, "decoding")
	}
	return rsp.Attrs, nil
}

// RowAttrDiff returns data from differing blocks on a remote host.
func (c *InternalClient) RowAttrDiff(ctx context.Context, uri *pilosa.URI, index, field string, blks []pilosa.AttrBlock) (map[uint64]map[string]interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.RowAttrDiff")
	defer span.Finish()

	if uri == nil {
		uri = c.defaultURI
	}
	u := uriPathToURL(uri, fmt.Sprintf("/internal/index/%s/field/%s/attr/diff", index, field))

	// Encode request.
	buf, err := json.Marshal(postFieldAttrDiffRequest{Blocks: blks})
	if err != nil {
		return nil, errors.Wrap(err, "marshaling")
	}

	// Build request.
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)
	req.Header.Set("Accept", "application/json")

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, errors.Wrap(pilosa.ErrFieldNotFound, field)
		}
		return nil, err
	}
	defer resp.Body.Close()

	// Decode response object.
	var rsp postFieldAttrDiffResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, errors.Wrap(err, "decoding")
	}
	return rsp.Attrs, nil
}

// SendMessage posts a message synchronously.
func (c *InternalClient) SendMessage(ctx context.Context, uri *pilosa.URI, msg []byte) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.SendMessage")
	defer span.Finish()

	u := uriPathToURL(uri, "/internal/cluster/message")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(msg))
	if err != nil {
		return errors.Wrap(err, "making new request")
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Connection", "keep-alive")

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()
	_, err = io.Copy(ioutil.Discard, resp.Body)
	return errors.Wrap(err, "draining SendMessage response body")
}

// TranslateKeysNode function is mainly called to translate keys from coordinator node.
// If coordinator node returns 404 error the function wraps it with pilosa.ErrTranslatingKeyNotFound.
func (c *InternalClient) TranslateKeysNode(ctx context.Context, uri *pilosa.URI, index, field string, keys []string, writable bool) ([]uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "TranslateKeysNode")
	defer span.Finish()

	if index == "" {
		return nil, pilosa.ErrIndexRequired
	}

	buf, err := c.serializer.Marshal(&pilosa.TranslateKeysRequest{
		Index:       index,
		Field:       field,
		Keys:        keys,
		NotWritable: !writable,
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshaling TranslateKeysRequest")
	}

	// Create HTTP request.
	u := uri.Path("/internal/translate/keys")
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("X-Pilosa-Row", "roaring")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, errors.Wrap(pilosa.ErrTranslatingKeyNotFound, err.Error())
		}
		return nil, err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	tkresp := &pilosa.TranslateKeysResponse{}
	if err := c.serializer.Unmarshal(body, tkresp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	}
	return tkresp.IDs, nil
}

// TranslateIDsNode sends an id translation request to a specific node.
func (c *InternalClient) TranslateIDsNode(ctx context.Context, uri *pilosa.URI, index, field string, ids []uint64) ([]string, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "TranslateIDsNode")
	defer span.Finish()

	if index == "" {
		return nil, pilosa.ErrIndexRequired
	}

	buf, err := c.serializer.Marshal(&pilosa.TranslateIDsRequest{
		Index: index,
		Field: field,
		IDs:   ids,
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshaling TranslateIDsRequest")
	}

	// Create HTTP request.
	u := uri.Path("/internal/translate/ids")
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("X-Pilosa-Row", "roaring")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	tkresp := &pilosa.TranslateIDsResponse{}
	if err := c.serializer.Unmarshal(body, tkresp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	}
	return tkresp.Keys, nil
}

// GetNodeUsage retrieves the size-on-disk information for the specified node.
func (c *InternalClient) GetNodeUsage(ctx context.Context, uri *pilosa.URI) (map[string]pilosa.NodeUsage, error) {
	u := uri.Path("/ui/usage?remote=true")
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	nodeUsages := make(map[string]pilosa.NodeUsage) // map of size 1
	if err := json.Unmarshal(body, &nodeUsages); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	}
	return nodeUsages, nil
}

// GetPastQueries retrieves the query history log for the specified node.
func (c *InternalClient) GetPastQueries(ctx context.Context, uri *pilosa.URI) ([]pilosa.PastQueryStatus, error) {
	u := uri.Path("/query-history?remote=true")
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	queries := make([]pilosa.PastQueryStatus, 100)
	if err := json.Unmarshal(body, &queries); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	}
	return queries, nil
}

func (c *InternalClient) FindIndexKeysNode(ctx context.Context, uri *pilosa.URI, index string, keys ...string) (transMap map[string]uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FindIndexKeysNode")
	defer span.Finish()

	// Create HTTP request.
	u := uriPathToURL(uri, fmt.Sprintf("/internal/translate/index/%s/keys/find", index))
	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(reqData))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	// Apply headers.
	req.Header.Set("Content-Length", strconv.Itoa(len(reqData)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Send the request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing response body")
		}
	}()

	// Read the response body.
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}

	// Decode the translations.
	transMap = make(map[string]uint64, len(keys))
	err = json.Unmarshal(result, &transMap)
	if err != nil {
		return nil, errors.Wrap(err, "json decoding")
	}

	return transMap, nil
}

func (c *InternalClient) FindFieldKeysNode(ctx context.Context, uri *pilosa.URI, index string, field string, keys ...string) (transMap map[string]uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FindFieldKeysNode")
	defer span.Finish()

	// Create HTTP request.
	u := uriPathToURL(uri, fmt.Sprintf("/internal/translate/field/%s/%s/keys/find", index, field))
	q := u.Query()
	q.Add("remote", "true")
	u.RawQuery = q.Encode()
	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(reqData))
	// Apply headers.
	req.Header.Set("Content-Length", strconv.Itoa(len(reqData)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Send the request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing response body")
		}
	}()

	// Read the response body.
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}

	// Decode the translations.
	transMap = make(map[string]uint64, len(keys))
	err = json.Unmarshal(result, &transMap)
	if err != nil {
		return nil, errors.Wrap(err, "json decoding")
	}

	return transMap, nil
}

func (c *InternalClient) CreateIndexKeysNode(ctx context.Context, uri *pilosa.URI, index string, keys ...string) (transMap map[string]uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.CreateIndexKeysNode")
	defer span.Finish()

	// Create HTTP request.
	u := uriPathToURL(uri, fmt.Sprintf("/internal/translate/index/%s/keys/create", index))
	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(reqData))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	// Apply headers.
	req.Header.Set("Content-Length", strconv.Itoa(len(reqData)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Send the request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing response body")
		}
	}()

	// Read the response body.
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}

	// Decode the translations.
	transMap = make(map[string]uint64, len(keys))
	err = json.Unmarshal(result, &transMap)
	if err != nil {
		return nil, errors.Wrap(err, "json decoding")
	}

	return transMap, nil
}

func (c *InternalClient) CreateFieldKeysNode(ctx context.Context, uri *pilosa.URI, index string, field string, keys ...string) (transMap map[string]uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.CreateFieldKeysNode")
	defer span.Finish()

	// Create HTTP request.
	u := uriPathToURL(uri, fmt.Sprintf("/internal/translate/field/%s/%s/keys/create", index, field))
	q := u.Query()
	q.Add("remote", "true")
	u.RawQuery = q.Encode()
	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(reqData))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	// Apply headers.
	req.Header.Set("Content-Length", strconv.Itoa(len(reqData)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Send the request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing response body")
		}
	}()

	// Read the response body.
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}

	// Decode the translations.
	transMap = make(map[string]uint64, len(keys))
	err = json.Unmarshal(result, &transMap)
	if err != nil {
		return nil, errors.Wrap(err, "json decoding")
	}

	return transMap, nil
}

func (c *InternalClient) Transactions(ctx context.Context) (map[string]*pilosa.Transaction, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Transactions")
	defer span.Finish()

	u := uriPathToURL(c.defaultURI, "/transactions")
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating transactions request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	trnsMap := make(map[string]*pilosa.Transaction)
	err = json.NewDecoder(resp.Body).Decode(&trnsMap)
	return trnsMap, errors.Wrap(err, "json decoding")
}

func (c *InternalClient) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool) (*pilosa.Transaction, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.StartTransaction")
	defer span.Finish()
	buf, err := json.Marshal(&pilosa.Transaction{
		ID:        id,
		Timeout:   timeout,
		Exclusive: exclusive,
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshalling payload")
	}
	// We're using the defaultURI here because this is only used by
	// tests, and we want to test requests against all hosts. A robust
	// client implementation would ensure that these requests go to
	// the coordinator.
	u := uriPathToURL(c.defaultURI, "/transaction/"+id)
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating post transaction request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	resp, err := c.executeRequest(req.WithContext(ctx), giveRawResponse(true))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	tr := &TransactionResponse{}
	err = json.NewDecoder(resp.Body).Decode(tr)
	if err != nil {
		return nil, errors.Wrap(err, "decoding response")
	}
	if resp.StatusCode == 409 {
		err = pilosa.ErrTransactionExclusive
	} else if tr.Error != "" {
		err = errors.New(tr.Error)
	}
	return tr.Transaction, err
}

func (c *InternalClient) FinishTransaction(ctx context.Context, id string) (*pilosa.Transaction, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FinishTransaction")
	defer span.Finish()

	u := uriPathToURL(c.defaultURI, "/transaction/"+id+"/finish")
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating finish transaction request")
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	resp, err := c.executeRequest(req.WithContext(ctx), giveRawResponse(true))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	tr := &TransactionResponse{}
	err = json.NewDecoder(resp.Body).Decode(tr)
	if err != nil {
		return nil, errors.Wrap(err, "decoding response")
	}

	if tr.Error != "" {
		err = errors.New(tr.Error)
	}
	return tr.Transaction, err
}

func (c *InternalClient) GetTransaction(ctx context.Context, id string) (*pilosa.Transaction, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.GetTransaction")
	defer span.Finish()

	// We're using the defaultURI here because this is only used by
	// tests, and we want to test requests against all hosts. A robust
	// client implementation would ensure that these requests go to
	// the coordinator.
	u := uriPathToURL(c.defaultURI, "/transaction/"+id)
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating get transaction request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	resp, err := c.executeRequest(req.WithContext(ctx), giveRawResponse(true))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	tr := &TransactionResponse{}
	err = json.NewDecoder(resp.Body).Decode(tr)
	if err != nil {
		return nil, errors.Wrap(err, "decoding response")
	}

	if tr.Error != "" {
		err = errors.New(tr.Error)
	}
	return tr.Transaction, err
}

type executeOpts struct {
	// giveRawResponse instructs executeRequest not to process the
	// respStatusCode and try to extract errors or whatever.
	giveRawResponse bool
}

type executeRequestOption func(*executeOpts)

func giveRawResponse(b bool) executeRequestOption {
	return func(eo *executeOpts) {
		eo.giveRawResponse = b
	}
}

// executeRequest executes the given request and checks the Response. For
// responses with non-2XX status, the body is read and closed, and an error is
// returned. If the error is nil, the caller must ensure that the response body
// is closed.
func (c *InternalClient) executeRequest(req *http.Request, opts ...executeRequestOption) (*http.Response, error) {
	eo := &executeOpts{}
	for _, opt := range opts {
		opt(eo)
	}

	tracing.GlobalTracer.InjectHTTPHeaders(req)
	req.Close = false
	resp, err := c.httpClient.Do(req)
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return nil, errors.Wrap(err, "getting response")
	}
	if eo.giveRawResponse {
		return resp, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return resp, errors.Wrapf(err, "bad status '%s' and err reading body", resp.Status)
		}
		var msg string
		// try to decode a JSON response
		var sr successResponse
		qr := &pilosa.QueryResponse{}
		if err = json.Unmarshal(buf, &sr); err == nil {
			msg = sr.Error.Error()
		} else if err := c.serializer.Unmarshal(buf, qr); err == nil {
			msg = qr.Err.Error()
		} else {
			msg = string(buf)
		}
		return resp, errors.Errorf("against %s %s: '%s'", req.URL.String(), resp.Status, msg)
	}
	return resp, nil
}

// Bits is a slice of Bit.
type Bits []pilosa.Bit

func (p Bits) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p Bits) Len() int      { return len(p) }

func (p Bits) Less(i, j int) bool {
	if p[i].RowID == p[j].RowID {
		if p[i].ColumnID < p[j].ColumnID {
			return p[i].Timestamp < p[j].Timestamp
		}
		return p[i].ColumnID < p[j].ColumnID
	}
	return p[i].RowID < p[j].RowID
}

// HasRowKeys returns true if any values use a row key.
func (p Bits) HasRowKeys() bool {
	for i := range p {
		if p[i].RowKey != "" {
			return true
		}
	}
	return false
}

// HasColumnKeys returns true if any values use a column key.
func (p Bits) HasColumnKeys() bool {
	for i := range p {
		if p[i].ColumnKey != "" {
			return true
		}
	}
	return false
}

// RowIDs returns a slice of all the row IDs.
func (p Bits) RowIDs() []uint64 {
	if p.HasRowKeys() {
		return nil
	}
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].RowID
	}
	return other
}

// ColumnIDs returns a slice of all the column IDs.
func (p Bits) ColumnIDs() []uint64 {
	if p.HasColumnKeys() {
		return nil
	}
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].ColumnID
	}
	return other
}

// RowKeys returns a slice of all the row keys.
func (p Bits) RowKeys() []string {
	if !p.HasRowKeys() {
		return nil
	}
	other := make([]string, len(p))
	for i := range p {
		other[i] = p[i].RowKey
	}
	return other
}

// ColumnKeys returns a slice of all the column keys.
func (p Bits) ColumnKeys() []string {
	if !p.HasColumnKeys() {
		return nil
	}
	other := make([]string, len(p))
	for i := range p {
		other[i] = p[i].ColumnKey
	}
	return other
}

// Timestamps returns a slice of all the timestamps.
func (p Bits) Timestamps() []int64 {
	other := make([]int64, len(p))
	for i := range p {
		other[i] = p[i].Timestamp
	}
	return other
}

// GroupByShard returns a map of bits by shard.
func (p Bits) GroupByShard() map[uint64][]pilosa.Bit {
	m := make(map[uint64][]pilosa.Bit)
	for _, bit := range p {
		shard := bit.ColumnID / pilosa.ShardWidth
		m[shard] = append(m[shard], bit)
	}

	for shard, bits := range m {
		sort.Sort(Bits(bits))
		m[shard] = bits
	}

	return m
}

// FieldValues represents a slice of field values.
type FieldValues []pilosa.FieldValue

func (p FieldValues) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p FieldValues) Len() int      { return len(p) }

func (p FieldValues) Less(i, j int) bool {
	return p[i].ColumnID < p[j].ColumnID
}

// HasColumnKeys returns true if any values use a column key.
func (p FieldValues) HasColumnKeys() bool {
	for i := range p {
		if p[i].ColumnKey != "" {
			return true
		}
	}
	return false
}

// ColumnIDs returns a slice of all the column IDs.
func (p FieldValues) ColumnIDs() []uint64 {
	if p.HasColumnKeys() {
		return nil
	}
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].ColumnID
	}
	return other
}

// ColumnKeys returns a slice of all the column keys.
func (p FieldValues) ColumnKeys() []string {
	if !p.HasColumnKeys() {
		return nil
	}
	other := make([]string, len(p))
	for i := range p {
		other[i] = p[i].ColumnKey
	}
	return other
}

// Values returns a slice of all the values.
func (p FieldValues) Values() []int64 {
	other := make([]int64, len(p))
	for i := range p {
		other[i] = p[i].Value
	}
	return other
}

// GroupByShard returns a map of field values by shard.
func (p FieldValues) GroupByShard() map[uint64][]pilosa.FieldValue {
	m := make(map[uint64][]pilosa.FieldValue)
	for _, val := range p {
		shard := val.ColumnID / pilosa.ShardWidth
		m[shard] = append(m[shard], val)
	}

	for shard, vals := range m {
		sort.Sort(FieldValues(vals))
		m[shard] = vals
	}

	return m
}

// BitsByPos is a slice of bits sorted row then column.
type BitsByPos []pilosa.Bit

func (p BitsByPos) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p BitsByPos) Len() int      { return len(p) }
func (p BitsByPos) Less(i, j int) bool {
	p0, p1 := pos(p[i].RowID, p[i].ColumnID), pos(p[j].RowID, p[j].ColumnID)
	if p0 == p1 {
		return p[i].Timestamp < p[j].Timestamp
	}
	return p0 < p1
}

// pos returns the row position of a row/column pair.
func pos(rowID, columnID uint64) uint64 {
	return (rowID * pilosa.ShardWidth) + (columnID % pilosa.ShardWidth)
}

func uriPathToURL(uri *pilosa.URI, path string) url.URL {
	return url.URL{
		Scheme: uri.Scheme,
		Host:   uri.HostPort(),
		Path:   path,
	}
}

func nodePathToURL(node *pilosa.Node, path string) url.URL {
	return url.URL{
		Scheme: node.URI.Scheme,
		Host:   node.URI.HostPort(),
		Path:   path,
	}
}

// RetrieveTranslatePartitionFromURI returns a ReadCloser which contains the data of the
// specified translate partition from the specified node. Caller *must* close the returned
// ReadCloser or risk leaking goroutines/tcp connections.
func (c *InternalClient) RetrieveTranslatePartitionFromURI(ctx context.Context, index string, partition int, uri pilosa.URI) (io.ReadCloser, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.RetrieveTranslatePartitionFromURI")
	defer span.Finish()

	node := &pilosa.Node{
		URI: uri,
	}

	u := nodePathToURL(node, "/internal/translate/data")
	u.RawQuery = url.Values{
		"index":     {index},
		"partition": {strconv.FormatInt(int64(partition), 10)},
	}.Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, pilosa.ErrFragmentNotFound
		}
		return nil, err
	}

	return resp.Body, nil
}
func (c *InternalClient) ImportIndexKeys(ctx context.Context, uri *pilosa.URI, index string, partitionID int, remote bool, rddbdata io.Reader) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ImportIndexKeys")
	defer span.Finish()

	if index == "" {
		return pilosa.ErrIndexRequired
	}

	if uri == nil {
		uri = c.defaultURI
	}

	vals := url.Values{}
	vals.Set("remote", strconv.FormatBool(remote))
	url := fmt.Sprintf("%s/internal/translate/index/%s/%d", uri, index, partitionID)

	// Generate HTTP request.
	httpReq, err := http.NewRequest("POST", url, rddbdata)
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	httpReq.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (c *InternalClient) ImportFieldKeys(ctx context.Context, uri *pilosa.URI, index, field string, remote bool, rddbdata io.Reader) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ImportFieldKeys")
	defer span.Finish()

	if index == "" {
		return pilosa.ErrIndexRequired
	}

	if uri == nil {
		uri = c.defaultURI
	}

	vals := url.Values{}
	vals.Set("remote", strconv.FormatBool(remote))
	url := fmt.Sprintf("%s/internal/translate/field/%s/%s", uri, index, field)

	// Generate HTTP request.
	httpReq, err := http.NewRequest("POST", url, rddbdata)
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	httpReq.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
