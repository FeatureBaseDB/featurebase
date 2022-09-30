// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"hash/maphash"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

var prototypeHash maphash.Hash

// ObjectCache is a cache of interface{} values
type ObjectCache interface {
	// Put a new value in the cache
	PutObject(uint64, interface{}) error

	// Get the value with the given key
	GetObject(uint64) (interface{}, error)

	// Size returns the number of values in the cache
	Size() int
}

// RowCache is a cache of rows used during row iteration
type RowCache interface {
	Add(row types.Row) error

	// AllRows returns all rows.
	AllRows() []types.Row
}

// KeyedRowCache is a cache of keyed rows used during row iteration
type KeyedRowCache interface {
	// Put adds row to the cache at the given key.
	Put(key uint64, row types.Row) error

	// Get returns the rows specified by key.
	Get(key uint64) (types.Row, error)

	// Size returns the number of rows in the cache.
	Size() int
}

// Ensure type implements interface
var _ KeyedRowCache = (*inMemoryKeyedRowCache)(nil)

// default implementation of KeyedRowCache (in memory)
type inMemoryKeyedRowCache struct {
	store map[uint64][]interface{}
}

func newinMemoryKeyedRowCache() *inMemoryKeyedRowCache {
	return &inMemoryKeyedRowCache{
		store: make(map[uint64][]interface{}),
	}
}

func (m inMemoryKeyedRowCache) Put(u uint64, i types.Row) error {
	m.store[u] = i
	return nil
}

func (m inMemoryKeyedRowCache) Get(u uint64) (types.Row, error) {
	return m.store[u], nil
}

func (m inMemoryKeyedRowCache) Size() int {
	return len(m.store)
}

// Ensure type implements interface
var _ RowCache = (*inMemoryRowCache)(nil)

type inMemoryRowCache struct {
	rows []types.Row
}

func newInMemoryRowCache() *inMemoryRowCache {
	return &inMemoryRowCache{}
}

func (c *inMemoryRowCache) Add(row types.Row) error {
	c.rows = append(c.rows, row)
	return nil
}

func (c *inMemoryRowCache) AllRows() []types.Row {
	return c.rows
}

// Ensure type implements interface
var _ ObjectCache = (*mapObjectCache)(nil)

// mapObjectCache is a simple in-memory implementation of a cache
type mapObjectCache struct {
	cache map[uint64]interface{}
}

func (m mapObjectCache) PutObject(u uint64, i interface{}) error {
	m.cache[u] = i
	return nil
}

func (m mapObjectCache) GetObject(u uint64) (interface{}, error) {
	v, ok := m.cache[u]
	if !ok {
		return nil, sql3.NewErrCacheKeyNotFound(u)
	}
	return v, nil
}

func (m mapObjectCache) Size() int {
	return len(m.cache)
}

func NewMapObjectCache() mapObjectCache {
	return mapObjectCache{
		cache: make(map[uint64]interface{}),
	}
}
