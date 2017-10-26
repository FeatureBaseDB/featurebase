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
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
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
	"strings"
	"time"

	"crypto/tls"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

var (
	ErrInvalidBackupFormat = errors.New("Invalid Backup Format")
)

// ClientOptions represents the configuration for a Client
type ClientOptions struct {
	TLS *tls.Config
}

// Client represents a client to the Pilosa cluster.
type Client struct {
	host    *URI
	options *ClientOptions

	// The client to use for HTTP communication.
	HTTPClient *http.Client
}

// NewClient returns a new instance of Client to connect to host.
func NewClient(host string, options *ClientOptions) (*Client, error) {
	if host == "" {
		return nil, ErrHostRequired
	}

	uri, err := NewURIFromAddress(host)
	if err != nil {
		return nil, err
	}

	return NewClientFromURI(uri, options)
}

func NewClientFromURI(uri *URI, options *ClientOptions) (*Client, error) {
	if uri == nil {
		return nil, ErrHostRequired
	}
	if options == nil {
		options = &ClientOptions{}
	}
	transport := &http.Transport{}
	if options.TLS != nil {
		transport.TLSClientConfig = options.TLS
	}
	client := &http.Client{Transport: transport}
	return &Client{
		host:       uri,
		HTTPClient: client,
	}, nil
}

// Host returns the host the client was initialized with.
func (c *Client) Host() *URI { return c.host }

// MaxSliceByIndex returns the number of slices on a server by index.
func (c *Client) MaxSliceByIndex(ctx context.Context) (map[string]uint64, error) {
	return c.maxSliceByIndex(ctx, false)
}

// MaxInverseSliceByIndex returns the number of inverse slices on a server by index.
func (c *Client) MaxInverseSliceByIndex(ctx context.Context) (map[string]uint64, error) {
	return c.maxSliceByIndex(ctx, true)
}

// maxSliceByIndex returns the number of slices on a server by index.
func (c *Client) maxSliceByIndex(ctx context.Context, inverse bool) (map[string]uint64, error) {
	// Execute request against the host.
	u := uriPathToURL(c.host, "/slices/max")
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
func (c *Client) Schema(ctx context.Context) ([]*IndexInfo, error) {
	// Execute request against the host.
	u := uriPathToURL(c.host, "/schema")

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

	var rsp getSchemaResponse
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return rsp.Indexes, nil
}

// CreateIndex creates a new index on the server.
func (c *Client) CreateIndex(ctx context.Context, index string, opt IndexOptions) error {
	// Encode query request.
	buf, err := json.Marshal(&postIndexRequest{
		Options: opt,
	})
	if err != nil {
		return err
	}

	// Create URL & HTTP request.
	u := uriPathToURL(c.host, fmt.Sprintf("/index/%s", index))
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
func (c *Client) FragmentNodes(ctx context.Context, index string, slice uint64) ([]*Node, error) {
	// Execute request against the host.
	u := uriPathToURL(c.host, "/fragment/nodes")
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
func (c *Client) ExecuteQuery(ctx context.Context, index, query string, allowRedirect bool) (result interface{}, err error) {
	if index == "" {
		return nil, ErrIndexRequired
	} else if query == "" {
		return nil, ErrQueryRequired
	}

	// Encode query request.
	buf, err := proto.Marshal(&internal.QueryRequest{
		Query:  query,
		Remote: !allowRedirect,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal: %s", err)
	}

	// Create URL & HTTP request.
	u := uriPathToURL(c.host, fmt.Sprintf("/index/%s/query", index))
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
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

	var qresp internal.QueryResponse
	if err := proto.Unmarshal(body, &qresp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	} else if s := qresp.Err; s != "" {
		return nil, errors.New(s)
	}

	return qresp, nil
}

// ExecutePQL executes query string against index on the server.
func (c *Client) ExecutePQL(ctx context.Context, index, query string) (interface{}, error) {
	u := uriPathToURL(c.host, "/query")
	u.RawQuery = url.Values{"index": {index}}.Encode()

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader([]byte(query)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "pilosa/"+Version)

	resp, err := c.HTTPClient.Do(req.WithContext(ctx))

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		return nil, errors.New(string(body))
	}
	return string(body), nil

}

// Import bulk imports bits for a single slice to a host.
func (c *Client) Import(ctx context.Context, index, frame string, slice uint64, bits []Bit) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	buf, err := MarshalImportPayload(index, frame, slice, bits)
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

func (c *Client) EnsureIndex(ctx context.Context, name string, options IndexOptions) error {
	err := c.CreateIndex(ctx, name, options)
	if err == nil || err == ErrIndexExists {
		return nil
	}
	return err
}

func (c *Client) EnsureFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) error {
	err := c.CreateFrame(ctx, indexName, frameName, options)
	if err == nil || err == ErrFrameExists {
		return nil
	}
	return err
}

// MarshalImportPayload marshalls the import parameters into a protobuf byte slice.
func MarshalImportPayload(index, frame string, slice uint64, bits []Bit) ([]byte, error) {
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
func (c *Client) importNode(ctx context.Context, node *Node, buf []byte) error {
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
func (c *Client) ImportValue(ctx context.Context, index, frame, field string, slice uint64, vals []FieldValue) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	buf, err := MarshalImportValuePayload(index, frame, field, slice, vals)
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

// MarshalImportValuePayload marshalls the import parameters into a protobuf byte slice.
func MarshalImportValuePayload(index, frame, field string, slice uint64, vals []FieldValue) ([]byte, error) {
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
func (c *Client) importValueNode(ctx context.Context, node *Node, buf []byte) error {
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
func (c *Client) ExportCSV(ctx context.Context, index, frame, view string, slice uint64, w io.Writer) error {
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
func (c *Client) exportNodeCSV(ctx context.Context, node *Node, index, frame, view string, slice uint64, w io.Writer) error {
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
func (c *Client) BackupTo(ctx context.Context, tw *tar.Writer, index, frame, view string) (err error) {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	// Find the maximum number of slices.
	var maxSlices map[string]uint64
	if view == ViewInverse {
		maxSlices, err = c.MaxInverseSliceByIndex(ctx)
	} else {
		maxSlices, err = c.MaxSliceByIndex(ctx)
	} /* else { //TODO validate view
		return ErrInvalidView
	}
	*/

	if err != nil {
		return fmt.Errorf("slice n: %s", err)
	}

	// Backup every slice to the tar file.
	for i := uint64(0); i <= maxSlices[index]; i++ {
		if err = c.backupSliceTo(ctx, tw, index, frame, view, i); err != nil {
			return
		}
	}
	return
}

// backupSliceTo backs up a single slice to tw.
func (c *Client) backupSliceTo(ctx context.Context, tw *tar.Writer, index, frame, view string, slice uint64) error {
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
		Name:    fmt.Sprintf("data/%s/%s/%s/%d", index, frame, view, slice),
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
func (c *Client) BackupSlice(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error) {
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

func (c *Client) backupSliceNode(ctx context.Context, index, frame, view string, slice uint64, node *Node) (io.ReadCloser, error) {
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
func (c *Client) RestoreFrom(ctx context.Context, r io.Reader) error {
	post := func(url string, jsonBytes []byte) (err error) {
		log.Println("POSTING", url)

		res, err := http.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(jsonBytes))
		if err != nil {
			return err
		}
		defer res.Body.Close()

		// Return error if response not OK.
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: url=%s, code=%d", url, res.StatusCode)
		}

		//need to look at the body?
		_, err = ioutil.ReadAll(res.Body)
		return err
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
		log.Println("READING", hdr.Name)
		parts := strings.Split(hdr.Name, "/")
		if parts[0] == "schema" {
			var buf bytes.Buffer
			dec := gob.NewDecoder(&buf)
			if _, err = io.CopyN(&buf, tr, hdr.Size); err != nil {
				return err
			}
			var schema []*IndexInfo
			err = dec.Decode(&schema)

			for i := range schema {
				idx := schema[i]
				//create index
				nodes, err := c.FragmentNodes(ctx, idx.Name, 0)
				if err != nil {
					return err
				}
				endPoint := fmt.Sprintf("/index/%s", idx.Name)
				url := nodePathToURL(nodes[0], endPoint)
				err = post(url.String(), []byte{})
				if err != nil {
					return err
				}

				for _, frame := range idx.Frames {
					endPoint := fmt.Sprintf("/index/%s/frame/%s", idx.Name, frame.Name)
					frameURL := nodePathToURL(nodes[0], endPoint)
					frameOptionsJSON, err := json.Marshal(map[string]interface{}{"options": frame.Options})
					if err != nil {
						return err
					}
					err = post(frameURL.String(), frameOptionsJSON)
					if err != nil {
						return err
					}

				}
			}
		} else if parts[0] == "colattr" {
			var buf bytes.Buffer
			if _, err = io.CopyN(&buf, tr, hdr.Size); err != nil {
				return err
			}
			_, err = c.SetRawColumnAttrs(ctx, parts[1], bufio.NewReader(&buf))
			if err != nil {
				return err
			}

		} else if parts[0] == "rowattr" {
			var buf bytes.Buffer
			if _, err = io.CopyN(&buf, tr, hdr.Size); err != nil {
				return err
			}
			_, err = c.SetRawRowAttrs(ctx, parts[1], parts[2], bufio.NewReader(&buf))
			if err != nil {
				return err
			}

		} else if parts[0] == "data" {

			index := parts[1]
			frame := parts[2]
			view := parts[3]
			slice, err := strconv.ParseUint(parts[4], 10, 64)

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
		} else {
			return ErrInvalidBackupFormat
		}
	}
}

// restoreSliceFrom restores a single slice to all owning nodes.
func (c *Client) restoreSliceFrom(ctx context.Context, buf []byte, index, frame, view string, slice uint64) error {
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
func (c *Client) CreateFrame(ctx context.Context, index, frame string, opt FrameOptions) error {
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
	u := uriPathToURL(c.host, fmt.Sprintf("/index/%s/frame/%s", index, frame))
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
func (c *Client) RestoreFrame(ctx context.Context, host, index, frame string) error {
	u := uriPathToURL(c.host, fmt.Sprintf("/index/%s/frame/%s/restore", index, frame))
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
func (c *Client) FrameViews(ctx context.Context, index, frame string) ([]string, error) {
	// Create URL & HTTP request.
	u := uriPathToURL(c.host, fmt.Sprintf("/index/%s/frame/%s/views", index, frame))
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
func (c *Client) FragmentBlocks(ctx context.Context, index, frame, view string, slice uint64) ([]FragmentBlock, error) {
	u := uriPathToURL(c.host, "/fragment/blocks")
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
func (c *Client) BlockData(ctx context.Context, index, frame, view string, slice uint64, block int) ([]uint64, []uint64, error) {
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

	u := uriPathToURL(c.host, "/fragment/block/data")
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
func (c *Client) ColumnAttrDiff(ctx context.Context, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	u := uriPathToURL(c.host, fmt.Sprintf("/index/%s/attr/diff", index))

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
func (c *Client) RowAttrDiff(ctx context.Context, index, frame string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	u := uriPathToURL(c.host, fmt.Sprintf("/index/%s/frame/%s/attr/diff", index, frame))

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

func (c *Client) getContents(ctx context.Context, u url.URL) ([]byte, error) {
	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
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
		return nil, ErrIndexNotFound
	default:
		return nil, fmt.Errorf("unexpected status: code=%d", resp.StatusCode)
	}

	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	if _, err = io.Copy(w, resp.Body); err != nil {
		return nil, err
	}
	w.Flush()
	return buf.Bytes(), nil

}

// FetchRawColumnAttrs retrieves the raw bolt data file contents of the Column Attributes
func (c *Client) FetchRawColumnAttrs(ctx context.Context, indexName string) ([]byte, error) {
	u := url.URL{
		Scheme: c.Host().Scheme(),
		Host:   c.Host().Host(),
		Path:   fmt.Sprintf("/index/%s/attr", indexName),
	}
	return c.getContents(ctx, u)
}

// FetchRawRowAttrs retrieves the raw bolt data file contents of the Row Attributes for the given frame
func (c *Client) FetchRawRowAttrs(ctx context.Context, indexName, frameName string) ([]byte, error) {
	u := url.URL{
		Scheme: c.Host().Scheme(),
		Host:   c.Host().Host(),
		Path:   fmt.Sprintf("/index/%s/frame/%s/attr", indexName, frameName),
	}
	return c.getContents(ctx, u)
}
func (c *Client) setContents(ctx context.Context, u url.URL, boltfile io.Reader) ([]byte, error) {
	// Build request.
	req, err := http.NewRequest("POST", u.String(), boltfile)
	if err != nil {
		return nil, err
	}
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
		return nil, ErrIndexNotFound
	default:
		return nil, fmt.Errorf("unexpected status: code=%d", resp.StatusCode)
	}

	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	if _, err = io.Copy(w, resp.Body); err != nil {
		return nil, err
	}
	w.Flush()
	return buf.Bytes(), nil

}

// SetRawColumnAttrs retrieves the raw bolt data file contents of the Column Attributes
func (c *Client) SetRawColumnAttrs(ctx context.Context, indexName string, boltfile io.Reader) ([]byte, error) {
	u := url.URL{
		Scheme: c.host.Scheme(),
		Host:   c.host.Host(),
		Path:   fmt.Sprintf("/index/%s/attr", indexName),
	}
	return c.setContents(ctx, u, boltfile)
}

// FetchRawRowAttrs retrieves the raw bolt data file contents of the Row Attributes for the given frame
func (c *Client) SetRawRowAttrs(ctx context.Context, indexName, frameName string, boltfile io.Reader) ([]byte, error) {
	u := url.URL{
		Scheme: c.host.Scheme(),
		Host:   c.host.Host(),
		Path:   fmt.Sprintf("/index/%s/frame/%s/attr", indexName, frameName),
	}
	return c.setContents(ctx, u, boltfile)
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

// FieldValue represents the value for a column within a
// range-encoded frame.
type FieldValue struct {
	ColumnID uint64
	Value    uint64
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
func (p FieldValues) Values() []uint64 {
	other := make([]uint64, len(p))
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
