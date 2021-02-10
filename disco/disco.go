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

	"github.com/pilosa/pilosa/v2/roaring"
)

var (
	ErrTooManyResults error = fmt.Errorf("too many results")
	ErrNoResults      error = fmt.Errorf("no results")
	ErrKeyDeleted     error = fmt.Errorf("key deleted")
	ErrIndexExists    error = fmt.Errorf("index already exists")
	ErrFieldExists    error = fmt.Errorf("field already exists")
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
	ClusterState        string
)

const (
	InitialClusterStateNew      InitialClusterState = "new"
	InitialClusterStateExisting InitialClusterState = "existing"

	// ClusterState represents the state returned in the /status endpoint.
	ClusterStateUnknown  ClusterState = "UNKNOWN"
	ClusterStateStarting ClusterState = "STARTING"
	ClusterStateDegraded ClusterState = "DEGRADED" // cluster is running but we've lost some # of hosts >0 but < replicaN
	ClusterStateNormal   ClusterState = "NORMAL"
	ClusterStateResizing ClusterState = "RESIZING" // cluster is replicating data to other nodes
	ClusterStateDown     ClusterState = "DOWN"     // cluster is unable to serve queries
)

type NodeState string

const (
	NodeStateUnknown  NodeState = "UNKNOWN"
	NodeStateStarting NodeState = "STARTING"
	NodeStateStarted  NodeState = "STARTED"
	NodeStateResizing NodeState = "RESIZING"
)

type Stator interface {
	Started(ctx context.Context) error
	ClusterState(context.Context) (ClusterState, error)
	NodeState(context.Context, string) (NodeState, error)
	NodeStates(context.Context) (map[string]NodeState, error)
}

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
	Views map[string][]byte
}

type Schemator interface {
	Schema(ctx context.Context) (map[string]*Index, error)
	Index(ctx context.Context, name string) ([]byte, error)
	CreateIndex(ctx context.Context, name string, val []byte) error
	DeleteIndex(ctx context.Context, name string) error
	Field(ctx context.Context, index, field string) ([]byte, error)
	CreateField(ctx context.Context, index, field string, val []byte) error
	DeleteField(ctx context.Context, index, field string) error
	View(ctx context.Context, index, field, view string) ([]byte, error)
	CreateView(ctx context.Context, index, field, view string, val []byte) error
	DeleteView(ctx context.Context, index, field, view string) error
}

type Metadata interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type Metadator interface {
	Metadata(ctx context.Context, peerID string) ([]byte, error)
	SetMetadata(ctx context.Context, metadata []byte) error
}

// Resizer triggers resizing the node and changes cluster state into RESIZING.
// We can also return some kind of handler from Resize function (e.g. key-value)
type Resizer interface {
	Resize(ctx context.Context) (func([]byte) error, error)
	DoneResize() error
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
func (*nopSchemator) Schema(ctx context.Context) (map[string]*Index, error) { return nil, nil }

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
func (*nopSchemator) View(ctx context.Context, index, field, view string) ([]byte, error) {
	return nil, nil
}

// CreateView is a no-op implementation of the Schemator CreateView method.
func (*nopSchemator) CreateView(ctx context.Context, index, field, view string, val []byte) error {
	return nil
}

// DeleteView is a no-op implementation of the Schemator DeleteView method.
func (*nopSchemator) DeleteView(ctx context.Context, index, field, view string) error { return nil }
