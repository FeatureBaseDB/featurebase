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
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
	"github.com/pkg/errors"
)

const (
	// NodeState represents the state of a node during startup.
	NodeStateLoading = "LOADING"
	NodeStateReady   = "READY"
)

// Node represents a node in the cluster.
type Node struct {
	ID            string `json:"id"`
	URI           URI    `json:"uri"`
	IsCoordinator bool   `json:"isCoordinator"`

	api nodeAPI
}

// NodeOption is a functional option type for pilosa.Node.
type NodeOption func(n *Node) error

func OptNodeURI(uri *URI) NodeOption {
	return func(n *Node) error {
		n.URI = *uri
		return nil
	}
}

func OptNodeIsCoordinator(isCoord bool) NodeOption {
	return func(n *Node) error {
		n.IsCoordinator = isCoord
		return nil
	}
}

func OptNodeLocalAPI(api *API) NodeOption {
	return func(n *Node) error {
		n.api = &localAPI{
			api: api,
		}
		return nil
	}
}

func OptNodeRemoteAPI(uri *URI, client *http.Client) NodeOption {
	return func(n *Node) error {
		return n.SetRemoteAPI(uri, client)
	}
}

// NewNode returns a new Node.
func NewNode(id string, opts ...NodeOption) (*Node, error) {
	n := &Node{
		ID: id,
	}

	for _, opt := range opts {
		err := opt(n)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}

	return n, nil
}

func (n *Node) SetLocalAPI(a *API) {
	n.api = &localAPI{
		api: a,
	}
}

func (n *Node) SetRemoteAPI(uri *URI, client *http.Client) error {
	n.api = newRemoteAPI(uri, client)
	return nil
}

func (n Node) String() string {
	return fmt.Sprintf("Node: %s", n.ID)
}

// EncodeNodes converts a slice of Nodes into its internal representation.
func EncodeNodes(a []*Node) []*internal.Node {
	other := make([]*internal.Node, len(a))
	for i := range a {
		other[i] = EncodeNode(a[i])
	}
	return other
}

// EncodeNode converts a Node into its internal representation.
func EncodeNode(n *Node) *internal.Node {
	return &internal.Node{
		ID:            n.ID,
		URI:           n.URI.Encode(),
		IsCoordinator: n.IsCoordinator,
	}
}

// DecodeNodes converts a proto message into a slice of Nodes.
func DecodeNodes(a []*internal.Node) []*Node {
	if len(a) == 0 {
		return nil
	}
	other := make([]*Node, len(a))
	for i := range a {
		other[i] = DecodeNode(a[i])
	}
	return other
}

// DecodeNode converts a proto message into a Node.
func DecodeNode(node *internal.Node) *Node {
	return &Node{
		ID:            node.ID,
		URI:           decodeURI(node.URI),
		IsCoordinator: node.IsCoordinator,
	}
}

func DecodeNodeEvent(ne *internal.NodeEventMessage) *NodeEvent {
	return &NodeEvent{
		Event: NodeEventType(ne.Event),
		Node:  DecodeNode(ne.Node),
	}
}

// Nodes represents a list of nodes.
type Nodes []*Node

// Contains returns true if a node exists in the list.
func (a Nodes) Contains(n *Node) bool {
	for i := range a {
		if a[i] == n {
			return true
		}
	}
	return false
}

// ContainsID returns true if host matches one of the node's id.
func (a Nodes) ContainsID(id string) bool {
	for _, n := range a {
		if n.ID == id {
			return true
		}
	}
	return false
}

// Filter returns a new list of nodes with node removed.
func (a Nodes) Filter(n *Node) []*Node {
	other := make([]*Node, 0, len(a))
	for i := range a {
		if a[i] != n {
			other = append(other, a[i])
		}
	}
	return other
}

// FilterID returns a new list of nodes with ID removed.
func (a Nodes) FilterID(id string) []*Node {
	other := make([]*Node, 0, len(a))
	for _, node := range a {
		if node.ID != id {
			other = append(other, node)
		}
	}
	return other
}

// FilterURI returns a new list of nodes with URI removed.
func (a Nodes) FilterURI(uri URI) []*Node {
	other := make([]*Node, 0, len(a))
	for _, node := range a {
		if node.URI != uri {
			other = append(other, node)
		}
	}
	return other
}

// IDs returns a list of all node IDs.
func (a Nodes) IDs() []string {
	ids := make([]string, len(a))
	for i, n := range a {
		ids[i] = n.ID
	}
	return ids
}

// URIs returns a list of all uris.
func (a Nodes) URIs() []URI {
	uris := make([]URI, len(a))
	for i, n := range a {
		uris[i] = n.URI
	}
	return uris
}

// Clone returns a shallow copy of nodes.
func (a Nodes) Clone() []*Node {
	other := make([]*Node, len(a))
	copy(other, a)
	return other
}

// byID implements sort.Interface for []Node based on
// the ID field.
type byID []*Node

func (h byID) Len() int           { return len(h) }
func (h byID) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h byID) Less(i, j int) bool { return h[i].ID < h[j].ID }

// nodeAction represents a node that is joining or leaving the cluster.
type nodeAction struct {
	node   *Node
	action string
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

// nodeAPI is the interface of methods used internally between nodes.
type nodeAPI interface {
	BlockData(ctx context.Context, index, frame, view string, slice uint64, block int) ([]uint64, []uint64, error)
	ColumnAttrDiff(ctx context.Context, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	EnsureIndex(ctx context.Context, name string, options IndexOptions) error
	EnsureFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) error
	FragmentBlocks(ctx context.Context, index, frame, view string, slice uint64) ([]FragmentBlock, error)
	Query(ctx context.Context, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error)
	RetrieveSlice(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error)
	RowAttrDiff(ctx context.Context, index, frame string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	SendMessage(ctx context.Context, pb proto.Message) error
}

// Ensure localAPI and remoteAPI implement interface.
var _ nodeAPI = &localAPI{}
var _ nodeAPI = &remoteAPI{}

// localAPI is an implementation of nodeAPI used when the node needing to
// handle the execution of the method is local node.
type localAPI struct {
	api *API
}

func (a *localAPI) BlockData(ctx context.Context, index, frame, view string, slice uint64, block int) ([]uint64, []uint64, error) {
	// Retrieve fragment from holder.
	f := a.api.Holder.Fragment(index, frame, view, slice)
	if f == nil {
		return nil, nil, ErrFragmentNotFound
	}

	rowIDs, columnIDs := f.BlockData(block)

	return rowIDs, columnIDs, nil
}

func (a *localAPI) ColumnAttrDiff(ctx context.Context, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	return a.api.IndexAttrDiff(ctx, index, blks)
}

func (a *localAPI) createFrame(ctx context.Context, index, frame string, opt FrameOptions) error {
	_, err := a.api.CreateFrame(ctx, index, frame, opt)
	return err
}

func (a *localAPI) createIndex(ctx context.Context, index string, opt IndexOptions) error {
	_, err := a.api.CreateIndex(ctx, index, opt)
	return err
}

func (a *localAPI) EnsureFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) error {
	err := a.createFrame(ctx, indexName, frameName, options)
	if err == nil || err == ErrFrameExists {
		return nil
	}
	return err
}

func (a *localAPI) EnsureIndex(ctx context.Context, name string, options IndexOptions) error {
	err := a.createIndex(ctx, name, options)
	if err == nil || err == ErrIndexExists {
		return nil
	}
	return err
}

func (a *localAPI) FragmentBlocks(ctx context.Context, index, frame, view string, slice uint64) ([]FragmentBlock, error) {
	return a.api.FragmentBlocks(ctx, index, frame, view, slice)
}

// RetrieveSlice gets all slice data from a node.
func (a *localAPI) RetrieveSlice(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error) {
	// TODO: implement if/when needed
	return nil, nil
}

func (a *localAPI) RowAttrDiff(ctx context.Context, index, frame string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	return a.api.FrameAttrDiff(ctx, index, frame, blks)
}

func (a *localAPI) Query(ctx context.Context, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error) {
	req := decodeQueryRequest(queryRequest)
	req.Index = index

	resp, err := a.api.Query(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "querying")
	}
	return encodeQueryResponse(&resp), nil
}

func (a *localAPI) SendMessage(ctx context.Context, pb proto.Message) error {
	return a.api.BroadcastHandler.ReceiveMessage(pb)
}

// remoteAPI is an implementation of nodeAPI used when the node needing to
// handle the execution of the method is a remote node.
// Note that it uses shared methods found in `commonAPI`.
type remoteAPI struct {
	commonAPI
	uri    *URI
	client *http.Client
}

func newRemoteAPI(uri *URI, client *http.Client) *remoteAPI {
	r := &remoteAPI{
		uri:    uri,
		client: client,
	}
	r.commonAPI.uri = uri
	r.commonAPI.client = client
	return r
}

// BlockData returns row/column id pairs for a block.
func (a *remoteAPI) BlockData(ctx context.Context, index, frame, view string, slice uint64, block int) ([]uint64, []uint64, error) {
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

	u := uriPathToURL(a.uri, "/fragment/block/data")
	req, err := http.NewRequest("GET", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, nil, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/protobuf")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Accept", "application/protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	resp, err := a.client.Do(req.WithContext(ctx))
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
func (a *remoteAPI) ColumnAttrDiff(ctx context.Context, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	u := uriPathToURL(a.uri, fmt.Sprintf("/index/%s/attr/diff", index))

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
	resp, err := a.client.Do(req.WithContext(ctx))
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

// RetrieveSlice gets all slice data from a node.
func (a *remoteAPI) RetrieveSlice(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error) {
	node := &Node{
		URI: *a.uri,
	}
	return a.backupSliceNode(ctx, index, frame, view, slice, node)
}

// RowAttrDiff returns data from differing blocks on a remote host.
func (a *remoteAPI) RowAttrDiff(ctx context.Context, index, frame string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	u := uriPathToURL(a.uri, fmt.Sprintf("/index/%s/frame/%s/attr/diff", index, frame))

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
	resp, err := a.client.Do(req.WithContext(ctx))
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
func (a *remoteAPI) SendMessage(ctx context.Context, pb proto.Message) error {
	msg, err := MarshalMessage(pb)
	if err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}

	u := uriPathToURL(a.uri, "/cluster/message")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(msg))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := a.client.Do(req.WithContext(ctx))
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

// commonAPI is made up of those methods required to implement parts of both
// ExternalClient and nodeAPI.
type commonAPI struct {
	uri    *URI
	client *http.Client
}

// BackupSlice retrieves a streaming backup from a single slice.
// This function tries slice owners until one succeeds.
func (a *commonAPI) BackupSlice(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error) {
	// Retrieve a list of nodes that own the slice.
	nodes, err := a.fragmentNodes(ctx, index, slice)
	if err != nil {
		return nil, fmt.Errorf("slice nodes: %s", err)
	}

	// Try to backup slice from each one until successful.
	for _, i := range rand.Perm(len(nodes)) {
		r, err := a.backupSliceNode(ctx, index, frame, view, slice, nodes[i])
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

func (a *commonAPI) backupSliceNode(ctx context.Context, index, frame, view string, slice uint64, node *Node) (io.ReadCloser, error) {
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
	resp, err := a.client.Do(req.WithContext(ctx))
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

// createFrame creates a new frame on the server.
func (a *commonAPI) createFrame(ctx context.Context, index, frame string, opt FrameOptions) error {
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
	u := uriPathToURL(a.uri, fmt.Sprintf("/index/%s/frame/%s", index, frame))
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := a.client.Do(req.WithContext(ctx))
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

// createIndex creates a new index on the server.
func (a *commonAPI) createIndex(ctx context.Context, index string, opt IndexOptions) error {
	// Encode query request.
	buf, err := json.Marshal(&postIndexRequest{
		Options: opt,
	})
	if err != nil {
		return errors.Wrap(err, "marshaling")
	}

	// Create URL & HTTP request.
	u := uriPathToURL(a.uri, fmt.Sprintf("/index/%s", index))
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := a.client.Do(req.WithContext(ctx))
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

// EnsureFrame creates frame if it doesn't already exist.
func (a *commonAPI) EnsureFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) error {
	err := a.createFrame(ctx, indexName, frameName, options)
	if err == nil || err == ErrFrameExists {
		return nil
	}
	return err
}

// EnsureIndex creates index if it doesn't already exist.
func (a *commonAPI) EnsureIndex(ctx context.Context, name string, options IndexOptions) error {
	err := a.createIndex(ctx, name, options)
	if err == nil || err == ErrIndexExists {
		return nil
	}
	return err
}

// FragmentBlocks returns a list of block checksums for a fragment on a host.
// Only returns blocks which contain data.
func (a *commonAPI) FragmentBlocks(ctx context.Context, index, frame, view string, slice uint64) ([]FragmentBlock, error) {
	u := uriPathToURL(a.uri, "/fragment/blocks")
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
	resp, err := a.client.Do(req.WithContext(ctx))
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

// fragmentNodes returns a list of nodes that own a slice.
func (a *commonAPI) fragmentNodes(ctx context.Context, index string, slice uint64) ([]*Node, error) {
	// Execute request against the host.
	u := uriPathToURL(a.uri, "/fragment/nodes")
	u.RawQuery = (url.Values{"index": {index}, "slice": {strconv.FormatUint(slice, 10)}}).Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := a.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()

	var n []*Node
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&n); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}

	return n, nil
}

// Query executes query against the index, sending the request to the node specified.
func (a *commonAPI) Query(ctx context.Context, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error) {
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
	u := a.uri.Path(fmt.Sprintf("/index/%s/query", index))
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := a.client.Do(req.WithContext(ctx))
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
