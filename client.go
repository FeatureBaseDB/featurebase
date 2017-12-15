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
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"crypto/tls"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
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
		return nil, err
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
	return c.maxSliceByIndex(ctx, false)
}

// MaxInverseSliceByIndex returns the number of inverse slices on a server by index.
func (c *InternalHTTPClient) MaxInverseSliceByIndex(ctx context.Context) (map[string]uint64, error) {
	return c.maxSliceByIndex(ctx, true)
}

// maxSliceByIndex returns the number of slices on a server by index.
func (c *InternalHTTPClient) maxSliceByIndex(ctx context.Context, inverse bool) (map[string]uint64, error) {
	// Execute request against the host.
	u := uriPathToURL(c.clientURI(ctx), "/slices/max")
	u.RawQuery = (&url.Values{
		"inverse": {strconv.FormatBool(inverse)},
	}).Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rsp sliceMaxResponse
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}

	return rsp.MaxSlices, nil
}

// Schema returns all index and frame schema information.
func (c *InternalHTTPClient) Schema(ctx context.Context) ([]*IndexInfo, error) {
	// Execute request against the host.
	u := c.defaultURI.Path("/schema")

	// Build request.
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
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
		return err
	}

	// Create URL & HTTP request.
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s", index))
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read body.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
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
		return nil, err
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
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

// ExecuteQuery executes query against index on the server.
func (c *InternalHTTPClient) ExecuteQuery(ctx context.Context, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error) {
	if index == "" {
		return nil, ErrIndexRequired
	} else if queryRequest.Query == "" {
		return nil, ErrQueryRequired
	}

	// Encode request object.
	buf, err := proto.Marshal(queryRequest)
	if err != nil {
		return nil, err
	}

	// Create HTTP request.
	u := c.clientURI(ctx).Path(fmt.Sprintf("/index/%s/query", index))
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
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
			return fmt.Errorf("import node: host=%s, err=%s", node.Host, err)
		}
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

	// Marshal bits to protobufs.
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

// importNode sends a pre-marshaled import request to a node.
func (c *InternalHTTPClient) importNode(ctx context.Context, node *Node, buf []byte) error {
	// Create URL & HTTP request.
	u := nodePathToURL(node, "/import")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
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
			return fmt.Errorf("import node: host=%s, err=%s", node.Host, err)
		}
	}

	return nil
}

// marshalImportValuePayload marshalls the import parameters into a protobuf byte slice.
func marshalImportValuePayload(index, frame, field string, slice uint64, vals []FieldValue) ([]byte, error) {
	// Separate row and column IDs to reduce allocations.
	columnIDs := FieldValues(vals).ColumnIDs()
	values := FieldValues(vals).Values()

	// Marshal bits to protobufs.
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
		return err
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
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
	} else if !(view == ViewStandard || view == ViewInverse) {
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
			e = fmt.Errorf("export node: host=%s, err=%s", node.Host, err)
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
		return err
	}
	req.Header.Set("Accept", "text/csv")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Validate status code.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status: %d", resp.StatusCode)
	}

	// Copy body to writer.
	if _, err := io.Copy(w, resp.Body); err != nil {
		return err
	}

	return nil
}

// BackupTo backs up an entire frame from a cluster to w.
func (c *InternalHTTPClient) BackupTo(ctx context.Context, w io.Writer, index, frame, view string) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	// Create tar writer around writer.
	tw := tar.NewWriter(w)

	// Find the maximum number of slices.
	var maxSlices map[string]uint64
	var err error
	if view == ViewStandard {
		maxSlices, err = c.MaxSliceByIndex(ctx)
	} else if view == ViewInverse {
		maxSlices, err = c.MaxInverseSliceByIndex(ctx)
	} else {
		return ErrInvalidView
	}

	if err != nil {
		return fmt.Errorf("slice n: %s", err)
	}

	// Backup every slice to the tar file.
	for i := uint64(0); i <= maxSlices[index]; i++ {
		if err := c.backupSliceTo(ctx, tw, index, frame, view, i); err != nil {
			return err
		}
	}

	// Close tar file.
	if err := tw.Close(); err != nil {
		return err
	}

	return nil
}

// backupSliceTo backs up a single slice to tw.
func (c *InternalHTTPClient) backupSliceTo(ctx context.Context, tw *tar.Writer, index, frame, view string, slice uint64) error {
	// Return error if unable to backup from any slice.
	r, err := c.BackupSlice(ctx, index, frame, view, slice)
	if err != nil {
		return fmt.Errorf("backup slice: slice=%d, err=%s", slice, err)
	} else if r == nil {
		return nil
	}
	defer r.Close()

	// Read entire buffer to determine file size.
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	} else if err := r.Close(); err != nil {
		return err
	}

	// Write slice file header.
	if err := tw.WriteHeader(&tar.Header{
		Name:    strconv.FormatUint(slice, 10),
		Mode:    0666,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}); err != nil {
		return err
	}

	// Write buffer to file.
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("write buffer: %s", err)
	}

	return nil
}

// BackupSlice retrieves a streaming backup from a single slice.
// This function tries slice owners until one succeeds.
func (c *InternalHTTPClient) BackupSlice(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error) {
	// Retrieve a list of nodes that own the slice.
	nodes, err := c.FragmentNodes(ctx, index, slice)
	if err != nil {
		return nil, fmt.Errorf("slice nodes: %s", err)
	}

	// Try to backup slice from each one until successful.
	for _, i := range rand.Perm(len(nodes)) {
		r, err := c.backupSliceNode(ctx, index, frame, view, slice, nodes[i])
		if err == nil {
			return r, nil // successfully attached
		} else if err == ErrFragmentNotFound {
			return nil, nil // slice doesn't exist
		} else if err != nil {
			log.Println(err)
			continue
		}
	}

	return nil, fmt.Errorf("unable to connect to any owner")
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
		return nil, err
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	// Return error if status is not OK.
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrFragmentNotFound
	} else if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected backup status code: host=%s, code=%d", node.Host, resp.StatusCode)
	}

	return resp.Body, nil
}

// RestoreFrom restores a frame from a backup file to an entire cluster.
func (c *InternalHTTPClient) RestoreFrom(ctx context.Context, r io.Reader, index, frame, view string) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	// Create tar reader around input.
	tr := tar.NewReader(r)

	// Process each file.
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// Parse slice from entry name.
		slice, err := strconv.ParseUint(hdr.Name, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid backup entry: %s", hdr.Name)
		}

		// Read file into buffer.
		var buf bytes.Buffer
		if _, err := io.CopyN(&buf, tr, hdr.Size); err != nil {
			return err
		}

		// Restore file to all nodes that own it.
		if err := c.restoreSliceFrom(ctx, buf.Bytes(), index, frame, view, slice); err != nil {
			return err
		}
	}
}

// restoreSliceFrom restores a single slice to all owning nodes.
func (c *InternalHTTPClient) restoreSliceFrom(ctx context.Context, buf []byte, index, frame, view string, slice uint64) error {
	// Retrieve a list of nodes that own the slice.
	nodes, err := c.FragmentNodes(ctx, index, slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Restore slice to each owner.
	for _, node := range nodes {
		u := nodePathToURL(node, "/fragment/data")
		u.RawQuery = url.Values{
			"index": {index},
			"frame": {frame},
			"view":  {view},
			"slice": {strconv.FormatUint(slice, 10)},
		}.Encode()

		// Build request.
		req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("User-Agent", "pilosa/"+Version)

		resp, err := c.HTTPClient.Do(req.WithContext(ctx))
		if err != nil {
			return err
		}
		resp.Body.Close()

		// Return error if response not OK.
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: host=%s, code=%d", node.Host, resp.StatusCode)
		}
	}

	return nil
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
		return err
	}

	// Create URL & HTTP request.
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s/frame/%s", index, frame))
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read body.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
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
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
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
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
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
		return nil, err
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
		return nil, err
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
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
		return nil, err
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
		return nil, nil, err
	}

	u := uriPathToURL(c.defaultURI, "/fragment/block/data")
	req, err := http.NewRequest("GET", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/protobuf")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Accept", "application/protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	} else if err := proto.Unmarshal(body, &rsp); err != nil {
		return nil, nil, err
	}
	return rsp.RowIDs, rsp.ColumnIDs, nil
}

// ColumnAttrDiff returns data from differing blocks on a remote host.
func (c *InternalHTTPClient) ColumnAttrDiff(ctx context.Context, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s/attr/diff", index))

	// Encode request.
	buf, err := json.Marshal(postIndexAttrDiffRequest{Blocks: blks})
	if err != nil {
		return nil, err
	}

	// Build request.
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
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
		return nil, err
	}
	return rsp.Attrs, nil
}

// RowAttrDiff returns data from differing blocks on a remote host.
func (c *InternalHTTPClient) RowAttrDiff(ctx context.Context, index, frame string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	u := uriPathToURL(c.defaultURI, fmt.Sprintf("/index/%s/frame/%s/attr/diff", index, frame))

	// Encode request.
	buf, err := json.Marshal(postFrameAttrDiffRequest{Blocks: blks})
	if err != nil {
		return nil, err
	}

	// Build request.
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
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
		return nil, err
	}
	return rsp.Attrs, nil
}

// SendMessage posts a message synchronously.
func (c *InternalHTTPClient) SendMessage(ctx context.Context, pb proto.Message) error {
	msg, err := MarshalMessage(pb)
	if err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}

	u := uriPathToURL(ctx.Value("uri").(*URI), "/cluster/message")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(msg))
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

func (c *InternalHTTPClient) clientURI(ctx context.Context) *URI {
	clientURI := c.defaultURI
	if contextURI, ok := ctx.Value("uri").(*URI); ok {
		clientURI = contextURI
	}
	return clientURI
}

func (c *InternalHTTPClient) NodeID(uri *URI) (string, error) {
	u := uriPathToURL(uri, "/id")
	req, err := http.NewRequest("GET", u.String(), nil)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("executing http request: %v", err)
	}
	defer resp.Body.Close()

	// Read body.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading response body: %v", err)
	}

	// Return error if status is not OK.
	switch resp.StatusCode {
	case http.StatusOK: // ok
	default:
		return "", fmt.Errorf("unexpected response status code: %d: %s", resp.StatusCode, body)
	}
	return string(body), nil
}

// Bit represents the location of a single bit.
type Bit struct {
	RowID     uint64
	ColumnID  uint64
	Timestamp int64
}

// Bits represents a slice of bits.
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

// BitsByPos represents a slice of bits sorted by internal position.
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
		Scheme: node.Scheme,
		Host:   node.Host,
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
	MaxInverseSliceByIndex(ctx context.Context) (map[string]uint64, error)
	Schema(ctx context.Context) ([]*IndexInfo, error)
	CreateIndex(ctx context.Context, index string, opt IndexOptions) error
	FragmentNodes(ctx context.Context, index string, slice uint64) ([]*Node, error)
	ExecuteQuery(ctx context.Context, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error)
	Import(ctx context.Context, index, frame string, slice uint64, bits []Bit) error
	EnsureIndex(ctx context.Context, name string, options IndexOptions) error
	EnsureFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) error
	ImportValue(ctx context.Context, index, frame, field string, slice uint64, vals []FieldValue) error
	ExportCSV(ctx context.Context, index, frame, view string, slice uint64, w io.Writer) error
	BackupTo(ctx context.Context, w io.Writer, index, frame, view string) error
	BackupSlice(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error)
	RestoreFrom(ctx context.Context, r io.Reader, index, frame, view string) error
	CreateFrame(ctx context.Context, index, frame string, opt FrameOptions) error
	RestoreFrame(ctx context.Context, host, index, frame string) error
	FrameViews(ctx context.Context, index, frame string) ([]string, error)
	FragmentBlocks(ctx context.Context, index, frame, view string, slice uint64) ([]FragmentBlock, error)
	BlockData(ctx context.Context, index, frame, view string, slice uint64, block int) ([]uint64, []uint64, error)
	ColumnAttrDiff(ctx context.Context, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	RowAttrDiff(ctx context.Context, index, frame string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	SendMessage(ctx context.Context, pb proto.Message) error
	NodeID(uri *URI) (string, error)
}
