// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import "github.com/featurebasedb/featurebase/v3/sql3/planner/types"

// RowCache is a cache of rows used during row iteration
type RowCache interface {
	Add(row types.Row) error

	// AllRows returns all rows.
	AllRows() []types.Row
}

// KeyedRowCache is a cache of keyed rows used during row iteration
type KeyedRowCache interface {
	// Put adds row to the cache at the given key.
	Put(key string, row types.Row) error

	// Get returns the rows specified by key.
	Get(key string) (types.Row, error)

	// Size returns the number of rows in the cache.
	Size() int
}

// Ensure type implements interface.
var _ KeyedRowCache = (*inMemoryKeyedRowCache)(nil)

// default implementation of KeyedRowCache (in memory)
type inMemoryKeyedRowCache struct {
	store map[string][]interface{}
}

func newinMemoryKeyedRowCache() *inMemoryKeyedRowCache {
	return &inMemoryKeyedRowCache{
		store: make(map[string][]interface{}),
	}
}

func (m inMemoryKeyedRowCache) Put(u string, i types.Row) error {
	m.store[u] = i
	return nil
}

func (m inMemoryKeyedRowCache) Get(u string) (types.Row, error) {
	return m.store[u], nil
}

func (m inMemoryKeyedRowCache) Size() int {
	return len(m.store)
}

// Ensure type implements interface.
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
