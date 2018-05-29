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

	"crypto/tls"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
	"github.com/pkg/errors"
)

// ClientOptions represents the configuration for a InternalHTTPClient
type ClientOptions struct {
	TLS *tls.Config
}

// InternalHTTPClient represents a client to the Pilosa cluster.
type InternalHTTPClient struct {
	defaultURI *URI

	// The client to use for HTTP communication.
	HTTPClient *http.Client
}

// NewInternalHTTPClient returns a new instance of InternalHTTPClient to connect to host.
func NewInternalHTTPClient(host string, remoteClient *http.Client) (*InternalHTTPClient, error) {
	if host == "" {
		return nil, ErrHostRequired
	}

	uri, err := NewURIFromAddress(host)
	if err != nil {
		return nil, errors.Wrap(err, "getting URI")
	}

	client := NewInternalHTTPClientFromURI(uri, remoteClient)
	return client, nil
}

func NewInternalHTTPClientFromURI(defaultURI *URI, remoteClient *http.Client) *InternalHTTPClient {
	return &InternalHTTPClient{
		defaultURI: defaultURI,
		HTTPClient: remoteClient,
	}
}

// Host returns the host the client was initialized with.
func (c *InternalHTTPClient) Host() *URI { return c.defaultURI }

// MaxSliceByIndex returns the number of slices on a server by index.
func (c *InternalHTTPClient) MaxSliceByIndex(ctx context.Context) (map[string]uint64, error) {
	return c.maxSliceByIndex(ctx)
}

// maxSliceByIndex returns the number of slices on a server by index.
func (c *InternalHTTPClient) maxSliceByIndex(ctx context.Context) (map[string]uint64, error) {
	// Execute request against the host.
	u := uriPathToURL(c.defaultURI, "/slices/max")

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	var rsp getSlicesMaxResponse
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}

	return rsp.Standard, nil
}

// Schema returns all index and frame schema information.
func (c *InternalHTTPClient) Schema(ctx context.Context) ([]*IndexInfo, error) {
	// Execute request against the host.
	u := c.defaultURI.Path("/schema")

	// Build request.
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	var rsp getSchemaResponse
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return rsp.Indexes, nil
}

// CreateIndex creates a new index on the server.
func (c *InternalHTTPClient) CreateIndex(ctx context.Context, index string, opt IndexOptions) error {
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
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Read body.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "reading")
	}

	// Handle response based on status code.
	switch resp.StatusCode {
	case http.StatusOK:
		return nil // ok
	case http.StatusConflict:
		return ErrIndexExists
	default:
		return errors.New(string(body))
	}
}

// FragmentNodes returns a list of nodes that own a slice.
func (c *InternalHTTPClient) FragmentNodes(ctx context.Context, index string, slice uint64) ([]*Node, error) {
	// Execute request against the host.
	u := uriPathToURL(c.defaultURI, "/fragment/nodes")
	u.RawQuery = (url.Values{"index": {index}, "slice": {strconv.FormatUint(slice, 10)}}).Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	var a []*Node
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}

	return a, nil
}

// Query executes query against the index.
func (c *InternalHTTPClient) Query(ctx context.Context, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error) {
	return c.QueryNode(ctx, c.defaultURI, index, queryRequest)
}

// QueryNode executes query against the index, sending the request to the node specified.
func (c *InternalHTTPClient) QueryNode(ctx context.Context, uri *URI, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error) {
	if index == "" {
		return nil, ErrIndexRequired
	} else if queryRequest.Query == "" {
		return nil, ErrQueryRequired
	}

	// Encode request object.
	buf, err := proto.Marshal(queryRequest)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling")
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
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	} else if resp.StatusCode != http.StatusOK {
		return nil, errors.New(string(body))
	}

	qresp := &internal.QueryResponse{}
	if err := proto.Unmarshal(body, qresp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	} else if s := qresp.Err; s != "" {
		return nil, errors.New(s)
	}

	return qresp, nil
}

// Import bulk imports bits for a single slice to a host.
func (c *InternalHTTPClient) Import(ctx context.Context, index, frame string, slice uint64, bits []Bit) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	buf, err := marshalImportPayload(index, frame, slice, bits)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	// Retrieve a list of nodes that own the slice.
	nodes, err := c.FragmentNodes(ctx, index, slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Import to each node.
	for _, node := range nodes {
		if err := c.importNode(ctx, node, buf); err != nil {
			return fmt.Errorf("import node: host=%s, err=%s", node.URI, err)
		}
	}

	return nil
}

// ImportK bulk imports bits specified by string keys to a host.
func (c *InternalHTTPClient) ImportK(ctx context.Context, index, frame string, columns []Bit) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	buf, err := marshalImportPayloadK(index, frame, columns)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	node := &Node{
		URI: *c.defaultURI,
	}

	// Import to node.
	if err := c.importNode(ctx, node, buf); err != nil {
		return fmt.Errorf("import node: host=%s, err=%s", node.URI, err)
	}

	return nil
}

func (c *InternalHTTPClient) EnsureIndex(ctx context.Context, name string, options IndexOptions) error {
	err := c.CreateIndex(ctx, name, options)
	if err == nil || err == ErrIndexExists {
		return nil
	}
	return err
}

func (c *InternalHTTPClient) EnsureFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) error {
	err := c.CreateFrame(ctx, indexName, frameName, options)
	if err == nil || err == ErrFrameExists {
		return nil
	}
	return err
}

// marshalImportPayload marshalls the import parameters into a protobuf byte slice.
func marshalImportPayload(index, frame string, slice uint64, bits []Bit) ([]byte, error) {
	// Separate row and column IDs to reduce allocations.
	rowIDs := Bits(bits).RowIDs()
	columnIDs := Bits(bits).ColumnIDs()
	timestamps := Bits(bits).Timestamps()

	// Marshal data to protobuf.
	buf, err := proto.Marshal(&internal.ImportRequest{
		Index:      index,
		Frame:      frame,
		Slice:      slice,
		RowIDs:     rowIDs,
		ColumnIDs:  columnIDs,
		Timestamps: timestamps,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal import request: %s", err)
	}
	return buf, nil
}

// marshalImportPayloadK marshalls the import parameters into a protobuf byte slice.
func marshalImportPayloadK(index, frame string, bits []Bit) ([]byte, error) {
	// Separate row and column IDs to reduce allocations.
	rowKeys := Bits(bits).RowKeys()
	columnKeys := Bits(bits).ColumnKeys()
	timestamps := Bits(bits).Timestamps()

	// Marshal data to protobuf.
	buf, err := proto.Marshal(&internal.ImportRequest{
		Index:      index,
		Frame:      frame,
		RowKeys:    rowKeys,
		ColumnKeys: columnKeys,
		Timestamps: timestamps,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal import request: %s", err)
	}
	return buf, nil
}

// importNode sends a pre-marshaled import request to a node.
func (c *InternalHTTPClient) importNode(ctx context.Context, node *Node, buf []byte) error {
	// Create URL & HTTP request.
	u := nodePathToURL(node, "/import")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "reading")
	} else if resp.StatusCode != http.StatusOK {
		return errors.New(string(body))
	}

	var isresp internal.ImportResponse
	if err := proto.Unmarshal(body, &isresp); err != nil {
		return fmt.Errorf("unmarshal import response: %s", err)
	} else if s := isresp.Err; s != "" {
		return errors.New(s)
	}

	return nil
}

// ImportValue bulk imports field values for a single slice to a host.
func (c *InternalHTTPClient) ImportValue(ctx context.Context, index, frame, field string, slice uint64, vals []FieldValue) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	buf, err := marshalImportValuePayload(index, frame, field, slice, vals)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	// Retrieve a list of nodes that own the slice.
	nodes, err := c.FragmentNodes(ctx, index, slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Import to each node.
	for _, node := range nodes {
		if err := c.importValueNode(ctx, node, buf); err != nil {
			return fmt.Errorf("import node: host=%s, err=%s", node.URI, err)
		}
	}

	return nil
}

// marshalImportValuePayload marshalls the import parameters into a protobuf byte slice.
func marshalImportValuePayload(index, frame, field string, slice uint64, vals []FieldValue) ([]byte, error) {
	// Separate row and column IDs to reduce allocations.
	columnIDs := FieldValues(vals).ColumnIDs()
	values := FieldValues(vals).Values()

	// Marshal data to protobuf.
	buf, err := proto.Marshal(&internal.ImportValueRequest{
		Index:     index,
		Frame:     frame,
		Slice:     slice,
		Field:     field,
		ColumnIDs: columnIDs,
		Values:    values,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal import request: %s", err)
	}
	return buf, nil
}

// importValueNode sends a pre-marshaled import request to a node.
func (c *InternalHTTPClient) importValueNode(ctx context.Context, node *Node, buf []byte) error {
	// Create URL & HTTP request.
	u := nodePathToURL(node, "/import-value")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "reading")
	} else if resp.StatusCode != http.StatusOK {
		return errors.New(string(body))
	}

	var isresp internal.ImportResponse
	if err := proto.Unmarshal(body, &isresp); err != nil {
		return fmt.Errorf("unmarshal import response: %s", err)
	} else if s := isresp.Err; s != "" {
		return errors.New(s)
	}

	return nil
}

// ExportCSV bulk exports data for a single slice from a host to CSV format.
func (c *InternalHTTPClient) ExportCSV(ctx context.Context, index, frame, view string, slice uint64, w io.Writer) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	} else if view != ViewStandard {
		return ErrInvalidView
	}

	// Retrieve a list of nodes that own the slice.
	nodes, err := c.FragmentNodes(ctx, index, slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Attempt nodes in random order.
	var e error
	for _, i := range rand.Perm(len(nodes)) {
		node := nodes[i]

		if err := c.exportNodeCSV(ctx, node, index, frame, view, slice, w); err != nil {
			e = fmt.Errorf("export node: host=%s, err=%s", node.URI, err)
			continue
		} else {
			return nil
		}
	}

	return e
}

// exportNode copies a CSV export from a node to w.
func (c *InternalHTTPClient) exportNodeCSV(ctx context.Context, node *Node, index, frame, view string, slice uint64, w io.Writer) error {
	// Create URL.
	u := nodePathToURL(node, "/export")
	u.RawQuery = url.Values{
		"index": {index},
		"frame": {frame},
		"view":  {view},
		"slice": {strconv.FormatUint(slice, 10)},
	}.Encode()

	// Generate HTTP request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Accept", "text/csv")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Validate status code.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status: %d", resp.StatusCode)
	}

	// Copy body to writer.
	if _, err := io.Copy(w, resp.Body); err != nil {
		return errors.Wrap(err, "copying")
	}

	return nil
}

func (c *InternalHTTPClient) RetrieveSliceFromURI(ctx context.Context, index, frame, view string, slice uint64, uri URI) (io.ReadCloser, error) {
	node := &Node{
		URI: uri,
	}
	return c.backupSliceNode(ctx, index, frame, view, slice, node)
}

func (c *InternalHTTPClient) backupSliceNode(ctx context.Context, index, frame, view string, slice uint64, node *Node) (io.ReadCloser, error) {
	u := nodePathToURL(node, "/fragment/data")
	u.RawQuery = url.Values{
		"index": {index},
		"frame": {frame},
		"view":  {view},
		"slice": {strconv.FormatUint(slice, 10)},
	}.Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}

	// Return error if status is not OK.
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrFragmentNotFound
	} else if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected backup status code: host=%s, code=%d", node.URI, resp.StatusCode)
	}

	return resp.Body, nil
}

// CreateFrame creates a new frame on the server.
func (c *InternalHTTPClient) CreateFrame(ctx context.Context, index, frame string, opt FrameOptions) error {
	if index == "" {
		return ErrIndexRequired
	}

	// Encode query request.
	buf, err := json.Marshal(&postFrameRequest{
		Options: opt,
	})
	if err != nil {
		return errors.Wrap(err, "marshaling")
	}

	// Create URL & HTTP request.
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s/frame/%s", index, frame))
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Read body.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "reading")
	}

	// Handle response based on status code.
	switch resp.StatusCode {
	case http.StatusOK:
		return nil // ok
	case http.StatusConflict:
		return ErrFrameExists
	default:
		return errors.New(string(body))
	}
}

// RestoreFrame restores an entire frame from a host in another cluster.
func (c *InternalHTTPClient) RestoreFrame(ctx context.Context, host, index, frame string) error {
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s/frame/%s/restore", index, frame))
	u.RawQuery = url.Values{
		"host": {host},
	}.Encode()

	// Build request.
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	resp.Body.Close()

	// Return error if response not OK.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: host=%s, code=%d", host, resp.StatusCode)
	}

	return nil
}

// FrameViews returns a list of view names for a frame.
func (c *InternalHTTPClient) FrameViews(ctx context.Context, index, frame string) ([]string, error) {
	// Create URL & HTTP request.
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s/frame/%s/views", index, frame))
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Handle response based on status code.
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, ErrFrameNotFound
	default:
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.New(string(body))
	}

	// Decode response.
	var rsp getFrameViewsResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, errors.Wrap(err, "decoding")
	}
	return rsp.Views, nil
}

// FragmentBlocks returns a list of block checksums for a fragment on a host.
// Only returns blocks which contain data.
func (c *InternalHTTPClient) FragmentBlocks(ctx context.Context, index, frame, view string, slice uint64) ([]FragmentBlock, error) {
	u := uriPathToURL(c.defaultURI, "/fragment/blocks")
	u.RawQuery = url.Values{
		"index": {index},
		"frame": {frame},
		"view":  {view},
		"slice": {strconv.FormatUint(slice, 10)},
	}.Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Return error if status is not OK.
	switch resp.StatusCode {
	case http.StatusOK: // ok
	case http.StatusNotFound:
		return nil, ErrFragmentNotFound
	default:
		return nil, fmt.Errorf("unexpected status: code=%d", resp.StatusCode)
	}

	// Decode response object.
	var rsp getFragmentBlocksResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, errors.Wrap(err, "decoding")
	}
	return rsp.Blocks, nil
}

// BlockData returns row/column id pairs for a block.
func (c *InternalHTTPClient) BlockData(ctx context.Context, index, frame, view string, slice uint64, block int) ([]uint64, []uint64, error) {
	buf, err := proto.Marshal(&internal.BlockDataRequest{
		Index: index,
		Frame: frame,
		View:  view,
		Slice: slice,
		Block: uint64(block),
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "marshaling")
	}

	u := uriPathToURL(c.defaultURI, "/fragment/block/data")
	req, err := http.NewRequest("GET", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, nil, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/protobuf")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Accept", "application/protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Return error if status is not OK.
	switch resp.StatusCode {
	case http.StatusOK: // fallthrough
	case http.StatusNotFound:
		return nil, nil, nil
	default:
		return nil, nil, fmt.Errorf("unexpected status: code=%d", resp.StatusCode)
	}

	// Decode response object.
	var rsp internal.BlockDataResponse
	if body, err := ioutil.ReadAll(resp.Body); err != nil {
		return nil, nil, errors.Wrap(err, "reading")
	} else if err := proto.Unmarshal(body, &rsp); err != nil {
		return nil, nil, errors.Wrap(err, "unmarshalling")
	}
	return rsp.RowIDs, rsp.ColumnIDs, nil
}

// ColumnAttrDiff returns data from differing blocks on a remote host.
func (c *InternalHTTPClient) ColumnAttrDiff(ctx context.Context, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s/attr/diff", index))

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
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Return error if status is not OK.
	switch resp.StatusCode {
	case http.StatusOK: // ok
	default:
		return nil, fmt.Errorf("unexpected status: code=%d", resp.StatusCode)
	}

	// Decode response object.
	var rsp postIndexAttrDiffResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, errors.Wrap(err, "decoding")
	}
	return rsp.Attrs, nil
}

// RowAttrDiff returns data from differing blocks on a remote host.
func (c *InternalHTTPClient) RowAttrDiff(ctx context.Context, index, frame string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s/frame/%s/attr/diff", index, frame))

	// Encode request.
	buf, err := json.Marshal(postFrameAttrDiffRequest{Blocks: blks})
	if err != nil {
		return nil, errors.Wrap(err, "marshaling")
	}

	// Build request.
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	// Return error if status is not OK.
	switch resp.StatusCode {
	case http.StatusOK: // ok
	case http.StatusNotFound:
		return nil, ErrFrameNotFound
	default:
		return nil, fmt.Errorf("unexpected status: code=%d", resp.StatusCode)
	}

	// Decode response object.
	var rsp postFrameAttrDiffResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, errors.Wrap(err, "decoding")
	}
	return rsp.Attrs, nil
}

// SendMessage posts a message synchronously.
func (c *InternalHTTPClient) SendMessage(ctx context.Context, uri *URI, pb proto.Message) error {
	msg, err := MarshalMessage(pb)
	if err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}

	u := uriPathToURL(uri, "/cluster/message")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(msg))
	if err != nil {
		return errors.Wrap(err, "making new request")
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("executing http request: %v", err)
	}
	defer resp.Body.Close()

	// Read body.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	// Return error if status is not OK.
	switch resp.StatusCode {
	case http.StatusOK: // ok
	default:
		return fmt.Errorf("unexpected response status code: %d: %s", resp.StatusCode, body)
	}

	return nil
}

// Bit represents the intersection of a row and a column. It can be specifed by
// integer ids or string keys.
type Bit struct {
	RowID     uint64
	ColumnID  uint64
	RowKey    string
	ColumnKey string
	Timestamp int64
}

// Bits is a slice of Bit.
type Bits []Bit

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

// RowIDs returns a slice of all the row IDs.
func (p Bits) RowIDs() []uint64 {
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].RowID
	}
	return other
}

// ColumnIDs returns a slice of all the column IDs.
func (p Bits) ColumnIDs() []uint64 {
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].ColumnID
	}
	return other
}

// RowKeys returns a slice of all the row keys.
func (p Bits) RowKeys() []string {
	other := make([]string, len(p))
	for i := range p {
		other[i] = p[i].RowKey
	}
	return other
}

// ColumnKeys returns a slice of all the column keys.
func (p Bits) ColumnKeys() []string {
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

// GroupBySlice returns a map of bits by slice.
func (p Bits) GroupBySlice() map[uint64][]Bit {
	m := make(map[uint64][]Bit)
	for _, bit := range p {
		slice := bit.ColumnID / SliceWidth
		m[slice] = append(m[slice], bit)
	}

	for slice, bits := range m {
		sort.Sort(Bits(bits))
		m[slice] = bits
	}

	return m
}

// FieldValues represents the value for a column within a
// range-encoded frame.
type FieldValue struct {
	ColumnID uint64
	Value    int64
}

// FieldValues represents a slice of field values.
type FieldValues []FieldValue

func (p FieldValues) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p FieldValues) Len() int      { return len(p) }

func (p FieldValues) Less(i, j int) bool {
	return p[i].ColumnID < p[j].ColumnID
}

// ColumnIDs returns a slice of all the column IDs.
func (p FieldValues) ColumnIDs() []uint64 {
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].ColumnID
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

// GroupBySlice returns a map of field values by slice.
func (p FieldValues) GroupBySlice() map[uint64][]FieldValue {
	m := make(map[uint64][]FieldValue)
	for _, val := range p {
		slice := val.ColumnID / SliceWidth
		m[slice] = append(m[slice], val)
	}

	for slice, vals := range m {
		sort.Sort(FieldValues(vals))
		m[slice] = vals
	}

	return m
}

// BitsByPos is a slice of bits sorted row then column.
type BitsByPos []Bit

func (p BitsByPos) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p BitsByPos) Len() int      { return len(p) }
func (p BitsByPos) Less(i, j int) bool {
	p0, p1 := Pos(p[i].RowID, p[i].ColumnID), Pos(p[j].RowID, p[j].ColumnID)
	if p0 == p1 {
		return p[i].Timestamp < p[j].Timestamp
	}
	return p0 < p1
}

func uriPathToURL(uri *URI, path string) url.URL {
	return url.URL{
		Scheme: uri.Scheme(),
		Host:   uri.HostPort(),
		Path:   path,
	}
}

func nodePathToURL(node *Node, path string) url.URL {
	return url.URL{
		Scheme: node.URI.Scheme(),
		Host:   node.URI.HostPort(),
		Path:   path,
	}
}

// InternalClient should be implemented by any struct that enables any transport between nodes
// TODO: Refactor
// Note from Travis: Typically an interface containing more than two or three methods is an indication that
// something hasn't been architected correctly.
// While I understand that putting the entire Client behind an interface might require this many methods,
// I don't want to let it go unquestioned.
type InternalClient interface {
	MaxSliceByIndex(ctx context.Context) (map[string]uint64, error)
	Schema(ctx context.Context) ([]*IndexInfo, error)
	CreateIndex(ctx context.Context, index string, opt IndexOptions) error
	FragmentNodes(ctx context.Context, index string, slice uint64) ([]*Node, error)
	Query(ctx context.Context, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error)
	QueryNode(ctx context.Context, uri *URI, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error)
	Import(ctx context.Context, index, frame string, slice uint64, bits []Bit) error
	ImportK(ctx context.Context, index, frame string, bits []Bit) error
	EnsureIndex(ctx context.Context, name string, options IndexOptions) error
	EnsureFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) error
	ImportValue(ctx context.Context, index, frame, field string, slice uint64, vals []FieldValue) error
	ExportCSV(ctx context.Context, index, frame, view string, slice uint64, w io.Writer) error
	CreateFrame(ctx context.Context, index, frame string, opt FrameOptions) error
	RestoreFrame(ctx context.Context, host, index, frame string) error
	FrameViews(ctx context.Context, index, frame string) ([]string, error)
	FragmentBlocks(ctx context.Context, index, frame, view string, slice uint64) ([]FragmentBlock, error)
	BlockData(ctx context.Context, index, frame, view string, slice uint64, block int) ([]uint64, []uint64, error)
	ColumnAttrDiff(ctx context.Context, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	RowAttrDiff(ctx context.Context, index, frame string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	SendMessage(ctx context.Context, uri *URI, pb proto.Message) error
}
