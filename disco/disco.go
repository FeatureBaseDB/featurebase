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

package disco

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/pilosa/pilosa/v2/roaring"
)

var (
	ErrTooManyResults    error = fmt.Errorf("too many results")
	ErrNoResults         error = fmt.Errorf("no results")
	ErrKeyDeleted        error = fmt.Errorf("key deleted")
	ErrIndexExists       error = fmt.Errorf("index already exists")
	ErrIndexDoesNotExist error = fmt.Errorf("index does not exist")
	ErrFieldExists       error = fmt.Errorf("field already exists")
	ErrFieldDoesNotExist error = fmt.Errorf("field does not exist")
	ErrViewExists        error = fmt.Errorf("view already exists")
	ErrViewDoesNotExist  error = fmt.Errorf("view does not exist")
)

type Peer struct {
	URL string
	ID  string
}

func (p *Peer) String() string {
	return fmt.Sprintf(`{"ID": "%s", "URL": "%s"}`, p.ID, p.URL)
}

type DisCo interface {
	io.Closer

	Start(ctx context.Context) (InitialClusterState, error)
	IsLeader() bool
	ID() string
	Leader() *Peer
	Peers() []*Peer
	DeleteNode(ctx context.Context, id string) error
}

type (
	InitialClusterState string

	// ClusterState represents the state returned in the /status endpoint.
	ClusterState string
)

const (
	InitialClusterStateNew      InitialClusterState = "new"
	InitialClusterStateExisting InitialClusterState = "existing"

	ClusterStateUnknown  ClusterState = "UNKNOWN"  // default cluster state. It is returned when we are not able to get the real actual state.
	ClusterStateStarting ClusterState = "STARTING" // cluster is starting and some internal services are not ready yet.
	ClusterStateDegraded ClusterState = "DEGRADED" // cluster is running but we've lost some # of hosts >0 but < replicaN. Only read queries are allowed.
	ClusterStateNormal   ClusterState = "NORMAL"   // cluster is up and running.
	ClusterStateResizing ClusterState = "RESIZING" // cluster is replicating data to other nodes.
	ClusterStateDown     ClusterState = "DOWN"     // cluster is unable to serve queries.
)

type NodeState string

const (
	NodeStateUnknown  NodeState = "UNKNOWN"
	NodeStateStarting NodeState = "STARTING"
	NodeStateStarted  NodeState = "STARTED"
	NodeStateResizing NodeState = "RESIZING"
)

type Stator interface {

	// Started will mark the actual node as already started.
	// It must be called after all initialization processes
	// are up and running.
	Started(ctx context.Context) error

	// ClusterState considers the state of all nodes and gives
	// a general cluster state. The output calculation is as follows:
	// - If any of the nodes are still starting: "STARTING"
	// - If all nodes are up and running: "NORMAL"
	// - If number of DOWN nodes is lower than number of replicas: "DEGRADED"
	// - If number of unresponsive nodes is greater than (or equal to) the number of replicas: "DOWN"
	// - If any of the nodes started a resize operation, or a new
	// node was specifically added or removed from the cluster: "RESIZING"
	ClusterState(context.Context) (ClusterState, error)

	// NodeState returns the specific state of a node given its ID.
	NodeState(context.Context, string) (NodeState, error)

	// NodeStates will return all the states by node ID of the actual nodes in the cluster.
	NodeStates(context.Context) (map[string]NodeState, error)
}

// Schema is a map of all indexes, each of those being a map of fields, then
// views.
type Schema map[string]*Index

// Index is a struct which contains the data encoded for the index as well as
// for each of its fields.
type Index struct {
	Data   []byte
	Fields map[string]*Field
}

// Field is a struct which contains the data encoded for the field as well as
// for each of its views.
type Field struct {
	Data  []byte
	Views map[string]struct{}
}

// Schemator is the source of truth for different schema elements.
// All nodes will store and retrieve information from the same source,
// having the same information at the same time.
type Schemator interface {

	// Schema return the actual pilosa schema. If the schema is not present, an error is returned.
	Schema(ctx context.Context) (Schema, error)

	// Index gets a specific index data by name.
	Index(ctx context.Context, name string) ([]byte, error)

	CreateIndex(ctx context.Context, name string, val []byte) error
	DeleteIndex(ctx context.Context, name string) error
	Field(ctx context.Context, index, field string) ([]byte, error)
	CreateField(ctx context.Context, index, field string, val []byte) error
	DeleteField(ctx context.Context, index, field string) error
	View(ctx context.Context, index, field, view string) (bool, error)
	CreateView(ctx context.Context, index, field, view string) error
	DeleteView(ctx context.Context, index, field, view string) error
}

// Metadator is in charge of storing specific metadata per node.
// This metadata can be retrieved by any node using the specific peerID.
type Metadator interface {
	Metadata(ctx context.Context, peerID string) ([]byte, error)
	SetMetadata(ctx context.Context, metadata []byte) error
}

// Resizer triggers resizing the node and changes cluster state into RESIZING.
// We can also return some kind of handler from Resize function (e.g. key-value)
type Resizer interface {
	// Resize will trigger a resize event. Node state will change to RESIZE state.
	// The returned function can be used to send info about the resize process to other nodes.
	Resize(ctx context.Context) (func([]byte) error, error)

	// DoneResize will mark the resize event as done. This will be called when all the resize actions are done.
	DoneResize() error

	// Watch will give information about a resize event in another node, using its peerID.
	// onUpdate function will be called per each event sent by the node in RESIZE state.
	Watch(ctx context.Context, peerID string, onUpdate func([]byte) error) error
}

// Sharder is an interface used to maintain the set of availableShards bitmaps
// per field.
type Sharder interface {
	Shards(ctx context.Context, index, field string) (*roaring.Bitmap, error)
	AddShard(ctx context.Context, index, field string, shard uint64) error
	AddShards(ctx context.Context, index, field string, shards *roaring.Bitmap) (*roaring.Bitmap, error)
	RemoveShard(ctx context.Context, index, field string, shard uint64) error
}

// NopDisCo represents a DisCo that doesn't do anything.
var NopDisCo DisCo = &nopDisCo{}

type nopDisCo struct{}

// Close no-op.
func (n *nopDisCo) Close() error {
	return nil
}

// Start is a no-op implementation of the DisCo Start method.
func (n *nopDisCo) Start(ctx context.Context) (InitialClusterState, error) {
	return InitialClusterStateNew, nil
}

// ID is a no-op implementation of the DisCo ID method.
func (n *nopDisCo) ID() string {
	return ""
}

// IsLeader is a no-op implementation of the DisCo IsLeader method.
func (n *nopDisCo) IsLeader() bool {
	return false
}

// Leader is a no-op implementation of the DisCo Leader method.
func (n *nopDisCo) Leader() *Peer {
	return nil
}

// Peers is a no-op implementation of the DisCo Peers method.
func (n *nopDisCo) Peers() []*Peer {
	return nil
}

// DeleteNode a no-op implementation of the DisCo DeleteNode method.
func (n *nopDisCo) DeleteNode(context.Context, string) error {
	return nil
}

// NopStator represents a Stator that doesn't do anything.
var NopStator Stator = &nopStator{}

type nopStator struct{}

// ClusterState is a no-op implementation of the Stator ClusterState method.
func (n *nopStator) ClusterState(context.Context) (ClusterState, error) {
	return ClusterStateUnknown, nil
}

func (n *nopStator) Started(ctx context.Context) error {
	return nil
}

func (n *nopStator) NodeState(context.Context, string) (NodeState, error) {
	return NodeStateUnknown, nil
}

func (n *nopStator) NodeStates(context.Context) (map[string]NodeState, error) {
	return nil, nil
}

// NopMetadator represents a Metadator that doesn't do anything.
var NopMetadator Metadator = &nopMetadator{}

type nopMetadator struct{}

func (*nopMetadator) Metadata(context.Context, string) ([]byte, error) {
	return nil, nil
}
func (*nopMetadator) SetMetadata(context.Context, []byte) error {
	return nil
}

// NopResizer represents a Resizer that doesn't do anything.
var NopResizer Resizer = &nopResizer{}

type nopResizer struct{}

func (*nopResizer) Resize(context.Context) (func([]byte) error, error)      { return nil, nil }
func (*nopResizer) DoneResize() error                                       { return nil }
func (*nopResizer) Watch(context.Context, string, func([]byte) error) error { return nil }

// NopSharder represents a Sharder that doesn't do anything.
var NopSharder Sharder = &nopSharder{}

type nopSharder struct{}

// Shards is a no-op implementation of the Sharder Shards method.
func (n *nopSharder) Shards(ctx context.Context, index, field string) (*roaring.Bitmap, error) {
	return nil, nil
}

// AddShard is a no-op implementation of the Sharder AddShard method.
func (n *nopSharder) AddShard(ctx context.Context, index, field string, shard uint64) error {
	return nil
}

// AddShards is a no-op implementation of the Sharder AddShards method.
func (n *nopSharder) AddShards(ctx context.Context, index, field string, shards *roaring.Bitmap) (*roaring.Bitmap, error) {
	return nil, nil
}

// RemoveShard is a no-op implementation of the Sharder RemoveShard method.
func (n *nopSharder) RemoveShard(ctx context.Context, index, field string, shard uint64) error {
	return nil
}

// NopSchemator represents a Schemator that doesn't do anything.
var NopSchemator Schemator = &nopSchemator{}

type nopSchemator struct{}

// Schema is a no-op implementation of the Schemator Schema method.
func (*nopSchemator) Schema(ctx context.Context) (Schema, error) { return nil, nil }

// Index is a no-op implementation of the Schemator Index method.
func (*nopSchemator) Index(ctx context.Context, name string) ([]byte, error) { return nil, nil }

// CreateIndex is a no-op implementation of the Schemator CreateIndex method.
func (*nopSchemator) CreateIndex(ctx context.Context, name string, val []byte) error { return nil }

// DeleteIndex is a no-op implementation of the Schemator DeleteIndex method.
func (*nopSchemator) DeleteIndex(ctx context.Context, name string) error { return nil }

// Field is a no-op implementation of the Schemator Field method.
func (*nopSchemator) Field(ctx context.Context, index, field string) ([]byte, error) { return nil, nil }

// CreateField is a no-op implementation of the Schemator CreateField method.
func (*nopSchemator) CreateField(ctx context.Context, index, field string, val []byte) error {
	return nil
}

// DeleteField is a no-op implementation of the Schemator DeleteField method.
func (*nopSchemator) DeleteField(ctx context.Context, index, field string) error { return nil }

// View is a no-op implementation of the Schemator View method.
func (*nopSchemator) View(ctx context.Context, index, field, view string) (bool, error) {
	return false, nil
}

// CreateView is a no-op implementation of the Schemator CreateView method.
func (*nopSchemator) CreateView(ctx context.Context, index, field, view string) error {
	return nil
}

// DeleteView is a no-op implementation of the Schemator DeleteView method.
func (*nopSchemator) DeleteView(ctx context.Context, index, field, view string) error { return nil }

// InMemSchemator represents a Schemator that manages the schema in memory. The
// intention is that this would be used for testing.
var InMemSchemator Schemator = &inMemSchemator{
	schema: make(Schema),
}

type inMemSchemator struct {
	mu     sync.RWMutex
	schema Schema
}

// Schema is an in-memory implementation of the Schemator Schema method.
func (s *inMemSchemator) Schema(ctx context.Context) (Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.schema, nil
}

// Index is an in-memory implementation of the Schemator Index method.
func (s *inMemSchemator) Index(ctx context.Context, name string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, ok := s.schema[name]
	if !ok {
		return nil, ErrIndexDoesNotExist
	}
	return idx.Data, nil
}

// CreateIndex is an in-memory implementation of the Schemator CreateIndex method.
func (s *inMemSchemator) CreateIndex(ctx context.Context, name string, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx, ok := s.schema[name]; ok {
		// The current logic in pilosa doesn't allow us to return ErrIndexExists
		// here, so for now we just update the Data value if the index already
		// exists.
		idx.Data = val
		return nil
	}
	s.schema[name] = &Index{
		Data:   val,
		Fields: make(map[string]*Field),
	}
	return nil
}

// DeleteIndex is an in-memory implementation of the Schemator DeleteIndex method.
func (s *inMemSchemator) DeleteIndex(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.schema, name)
	return nil
}

// Field is an in-memory implementation of the Schemator Field method.
func (s *inMemSchemator) Field(ctx context.Context, index, field string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, ok := s.schema[index]
	if !ok {
		return nil, ErrIndexDoesNotExist
	}
	fld, ok := idx.Fields[field]
	if !ok {
		return nil, ErrFieldDoesNotExist
	}
	return fld.Data, nil
}

// CreateField is an in-memory implementation of the Schemator CreateField method.
func (s *inMemSchemator) CreateField(ctx context.Context, index, field string, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.schema[index]
	if !ok {
		return ErrIndexDoesNotExist
	}
	if fld, ok := idx.Fields[field]; ok {
		// The current logic in pilosa doesn't allow us to return ErrFieldExists
		// here, so for now we just update the Data value if the field already
		// exists.
		fld.Data = val
		return nil
	}
	idx.Fields[field] = &Field{
		Data:  val,
		Views: make(map[string]struct{}),
	}
	return nil
}

// DeleteField is an in-memory implementation of the Schemator DeleteField method.
func (s *inMemSchemator) DeleteField(ctx context.Context, index, field string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.schema[index]
	if !ok {
		return ErrIndexDoesNotExist
	}
	delete(idx.Fields, field)
	return nil
}

// View is an in-memory implementation of the Schemator View method.
func (s *inMemSchemator) View(ctx context.Context, index, field, view string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, ok := s.schema[index]
	if !ok {
		return false, ErrIndexDoesNotExist
	}
	fld, ok := idx.Fields[field]
	if !ok {
		return false, ErrFieldDoesNotExist
	}
	_, ok = fld.Views[view]
	return ok, nil
}

// CreateView is an in-memory implementation of the Schemator CreateView method.
func (s *inMemSchemator) CreateView(ctx context.Context, index, field, view string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.schema[index]
	if !ok {
		return ErrIndexDoesNotExist
	}
	fld, ok := idx.Fields[field]
	if !ok {
		return ErrFieldDoesNotExist
	}
	// The current logic in pilosa doesn't allow us to return ErrViewExists
	// here, so for now we just update the value if the view already exists.
	fld.Views[view] = struct{}{}
	return nil
}

// DeleteView is an in-memory implementation of the Schemator DeleteView method.
func (s *inMemSchemator) DeleteView(ctx context.Context, index, field, view string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.schema[index]
	if !ok {
		return ErrIndexDoesNotExist
	}
	fld, ok := idx.Fields[field]
	if !ok {
		return ErrFieldDoesNotExist
	}
	delete(fld.Views, view)
	return nil
}
