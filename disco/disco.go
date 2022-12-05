// Copyright 2021 Molecula Corp. All rights reserved.
package disco

import (
	"context"
	"fmt"
	"io"
	"path"
	"sync"
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
	ErrKeyDoesNotExist   error = fmt.Errorf("key does not exist")
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
	ClusterStateDegraded ClusterState = "DEGRADED" // cluster is running but we've lost some # of hosts >0 but < replicaN. Only read queries are allowed.
	ClusterStateNormal   ClusterState = "NORMAL"   // cluster is up and running.
	ClusterStateDown     ClusterState = "DOWN"     // cluster is unable to serve queries.
)

type NodeState string

const (
	NodeStateUnknown  NodeState = "UNKNOWN"
	NodeStateStarting NodeState = "STARTING"
	NodeStateStarted  NodeState = "STARTED"
)

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
	CreateField(ctx context.Context, index, field string, fieldVal []byte) error
	UpdateField(ctx context.Context, index, field string, fieldVal []byte) error
	DeleteField(ctx context.Context, index, field string) error
	View(ctx context.Context, index, field, view string) (bool, error)
	CreateView(ctx context.Context, index, field, view string) error
	DeleteView(ctx context.Context, index, field, view string) error
}

// Sharder is an interface used to maintain the set of availableShards bitmaps
// per field.
type Sharder interface {
	Shards(ctx context.Context, index, field string) ([][]byte, error)
	SetShards(ctx context.Context, index, field string, shards []byte) error
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

// Ensure type implements interface.
var _ DisCo = &inMemDisCo{}

func NewInMemDisCo(id string) *inMemDisCo {
	return &inMemDisCo{
		id: id,
	}
}

// inMemDisCo represents a DisCo that is aware of itself.
type inMemDisCo struct {
	id string
}

// Close no-op.
func (n *inMemDisCo) Close() error {
	return nil
}

// Start is a no-op implementation of the DisCo Start method.
func (n *inMemDisCo) Start(ctx context.Context) (InitialClusterState, error) {
	return InitialClusterStateNew, nil
}

// ID is a no-op implementation of the DisCo ID method.
func (n *inMemDisCo) ID() string {
	return n.id
}

// IsLeader is a no-op implementation of the DisCo IsLeader method.
func (n *inMemDisCo) IsLeader() bool {
	return true
}

// Leader is a no-op implementation of the DisCo Leader method.
func (n *inMemDisCo) Leader() *Peer {
	return nil
}

// Peers is a no-op implementation of the DisCo Peers method.
func (n *inMemDisCo) Peers() []*Peer {
	return nil
}

// DeleteNode a no-op implementation of the DisCo DeleteNode method.
func (n *inMemDisCo) DeleteNode(context.Context, string) error {
	return nil
}

////////////////////////////////////////////////

// NopSharder represents a Sharder that doesn't do anything.
var NopSharder Sharder = &nopSharder{}

type nopSharder struct{}

// Shards is a no-op implementation of the Sharder Shards method.
func (n *nopSharder) Shards(ctx context.Context, index, field string) ([][]byte, error) {
	return nil, nil
}

// AddShards is a no-op implementation of the Sharder AddShards method.
func (n *nopSharder) SetShards(ctx context.Context, index, field string, shards []byte) error {
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
func (*nopSchemator) DeleteIndex(ctx context.Context, name string) error {
	return nil
}

// Field is a no-op implementation of the Schemator Field method.
func (*nopSchemator) Field(ctx context.Context, index, field string) ([]byte, error) { return nil, nil }

// CreateField is a no-op implementation of the Schemator CreateField method.
func (*nopSchemator) CreateField(ctx context.Context, index, field string, fieldVal []byte) error {
	return nil
}

// UpdateField is a no-op implementation of the Schemator UpdateField method.
func (*nopSchemator) UpdateField(ctx context.Context, index, field string, fieldVal []byte) error {
	return nil
}

// DeleteField is a no-op implementation of the Schemator DeleteField method.
func (*nopSchemator) DeleteField(ctx context.Context, index, field string) error {
	return nil
}

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

// NewInMemSchemator instantiates an InMemSchemator
// this allows new holders to have thier own, and not rely on a shared instance
func NewInMemSchemator() *inMemSchemator {
	return &inMemSchemator{
		schema: make(Schema),
	}
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
func (s *inMemSchemator) CreateField(ctx context.Context, index, field string, fieldVal []byte) error {
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
		fld.Data = fieldVal
		return nil
	}
	idx.Fields[field] = &Field{
		Data:  fieldVal,
		Views: make(map[string]struct{}),
	}
	return nil
}

func (s *inMemSchemator) UpdateField(ctx context.Context, index, field string, fieldVal []byte) error {
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
		fld.Data = fieldVal
		return nil
	} else {
		return ErrFieldDoesNotExist
	}
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

var InMemSharder Sharder = &inMemSharder{
	shards: make(map[string][]byte),
}

type inMemSharder struct {
	mu     sync.RWMutex
	shards map[string][]byte
}

func (s *inMemSharder) Shards(ctx context.Context, index, field string) ([][]byte, error) {
	key := path.Join("/shard/", index, field)
	s.mu.RLock()
	defer s.mu.RUnlock()

	b := s.shards[key]
	if b == nil {
		return nil, nil
	}
	return [][]byte{b}, nil
}

func (s *inMemSharder) SetShards(ctx context.Context, index, field string, shards []byte) error {
	key := path.Join("/shard/", index, field)
	s.mu.Lock()
	defer s.mu.Unlock()

	s.shards[key] = make([]byte, len(shards))
	copy(s.shards[key], shards)
	return nil
}
