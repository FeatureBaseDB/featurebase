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

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/encoding/proto"
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

// CreateIndex creates a new index on the server.
func (c *InternalClient) CreateIndex(ctx context.Context, index string, opt pilosa.IndexOptions) error {
	// Encode query request.
	buf, err := json.Marshal(&postIndexRequest{
		Options: opt,
	})
	if err != nil {
		return errors.Wrap(err, "encoding request")
	}

	// Create URL & HTTP request.
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s", index))
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
	return nil
}

// FragmentNodes returns a list of nodes that own a shard.
func (c *InternalClient) FragmentNodes(ctx context.Context, index string, shard uint64) ([]*pilosa.Node, error) {
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
	return c.QueryNode(ctx, c.defaultURI, index, queryRequest)
}

// QueryNode executes query against the index, sending the request to the node specified.
func (c *InternalClient) QueryNode(ctx context.Context, uri *pilosa.URI, index string, queryRequest *pilosa.QueryRequest) (*pilosa.QueryResponse, error) {
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
	err := c.CreateIndex(ctx, name, options)
	if err == nil || errors.Cause(err) == pilosa.ErrIndexExists {
		return nil
	}
	return err
}

func (c *InternalClient) EnsureField(ctx context.Context, indexName string, fieldName string) error {
	return c.EnsureFieldWithOptions(ctx, indexName, fieldName, pilosa.FieldOptions{})
}

func (c *InternalClient) EnsureFieldWithOptions(ctx context.Context, indexName string, fieldName string, opt pilosa.FieldOptions) error {
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

// ImportValueK bulk imports keyed field values to a host.
func (c *InternalClient) ImportValueK(ctx context.Context, index, field string, vals []pilosa.FieldValue, opts ...pilosa.ImportOption) error {
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
func (c *InternalClient) ImportRoaring(ctx context.Context, uri *pilosa.URI, index, field string, shard uint64, remote bool, data []byte, opts ...pilosa.ImportOption) error {
	if index == "" {
		return pilosa.ErrIndexRequired
	} else if field == "" {
		return pilosa.ErrFieldRequired
	}
	if uri == nil {
		uri = c.defaultURI
	}

	// Set up import options.
	options := &pilosa.ImportOptions{}
	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return errors.Wrap(err, "applying option")
		}
	}

	vals := url.Values{}
	vals.Set("remote", strconv.FormatBool(remote))
	if options.Clear {
		vals.Set("clear", "true")
	}
	url := fmt.Sprintf("%s/index/%s/field/%s/import-roaring/%d?%s", uri, index, field, shard, vals.Encode())

	// Generate HTTP request.
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/x-binary")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	rbody := &pilosa.ImportResponse{}
	dec.Decode(rbody)
	if rbody.Err != "" {
		return errors.Errorf("importing roaring: %v", rbody.Err)
	}
	return nil
}

// ExportCSV bulk exports data for a single shard from a host to CSV format.
func (c *InternalClient) ExportCSV(ctx context.Context, index, field string, shard uint64, w io.Writer) error {
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

func (c *InternalClient) RetrieveShardFromURI(ctx context.Context, index, field string, shard uint64, uri pilosa.URI) (io.ReadCloser, error) {
	node := &pilosa.Node{
		URI: uri,
	}
	return c.backupShardNode(ctx, index, field, shard, node)
}

func (c *InternalClient) backupShardNode(ctx context.Context, index, field string, shard uint64, node *pilosa.Node) (io.ReadCloser, error) {
	u := nodePathToURL(node, "/fragment/data")
	u.RawQuery = url.Values{
		"index": {index},
		"field": {field},
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
	return c.CreateFieldWithOptions(ctx, index, field, pilosa.FieldOptions{})
}

// CreateField creates a new field on the server.
func (c *InternalClient) CreateFieldWithOptions(ctx context.Context, index, field string, opt pilosa.FieldOptions) error {
	if index == "" {
		return pilosa.ErrIndexRequired
	}

	// convert pilosa.FieldOptions to fieldOptions
	fieldOpt := fieldOptions{
		Type: opt.Type,
		Keys: &opt.Keys,
	}
	if fieldOpt.Type == "set" {
		fieldOpt.CacheType = &opt.CacheType
		fieldOpt.CacheSize = &opt.CacheSize
	} else if fieldOpt.Type == "int" {
		fieldOpt.Min = &opt.Min
		fieldOpt.Max = &opt.Max
	} else if fieldOpt.Type == "time" {
		fieldOpt.TimeQuantum = &opt.TimeQuantum
	}

	// TODO: remove buf completely? (depends on whether importer needs to create specific field types)
	// Encode query request.
	buf, err := json.Marshal(&postFieldRequest{
		Options: fieldOpt,
	})
	if err != nil {
		return errors.Wrap(err, "marshaling")
	}

	// Create URL & HTTP request.
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s/field/%s", index, field))
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

	return nil
}

// FragmentBlocks returns a list of block checksums for a fragment on a host.
// Only returns blocks which contain data.
func (c *InternalClient) FragmentBlocks(ctx context.Context, uri *pilosa.URI, index, field, view string, shard uint64) ([]pilosa.FragmentBlock, error) {
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
			return nil, pilosa.ErrFieldNotFound
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
	u := uriPathToURL(uri, "/internal/cluster/message")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(msg))
	if err != nil {
		return errors.Wrap(err, "making new request")
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+pilosa.Version)
	req.Header.Set("Accept", "application/json")

	// Execute request.
	_, err = c.executeRequest(req.WithContext(ctx))
	return err
}

// executeRequest executes the given request and checks the Response
func (c *InternalClient) executeRequest(req *http.Request) (*http.Response, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
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
		if err = json.Unmarshal(buf, &sr); err == nil {
			msg = sr.Error.Error()
		} else {
			msg = string(buf)
		}
		return resp, errors.Errorf("server error %s: '%s'", resp.Status, msg)
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
