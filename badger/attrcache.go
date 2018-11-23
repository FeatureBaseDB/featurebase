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

package badger

import (
	"sync"

	"github.com/pkg/errors"
)

func newCache(s *attrStore) *attrCache {
	return &attrCache{
		attrStore: s,
		attrs:     make(map[uint64]map[string]interface{}),
		mu:        sync.RWMutex{},
	}
}

//attrCache wraps the attrstore and provides a cache
type attrCache struct {
	//cachableStore
	*attrStore
	mu    sync.RWMutex
	attrs map[uint64]map[string]interface{}
}

func (c *attrCache) Attrs(id uint64) (m map[string]interface{}, err error) {

	// Check cache for map.
	if m = c.get(id); m != nil {
		return m, nil
	}
	m, err = c.attrStore.Attrs(id)
	if err != nil {
		return nil, err
	}
	c.set(id, m)
	return m, err
}

func (c *attrCache) SetAttrs(id uint64, m map[string]interface{}) error {

	// Check if the attributes already exist under a read-only lock.
	if attr, err := c.Attrs(id); err != nil {
		return errors.Wrap(err, "checking attrs")
	} else if attr != nil && mapContains(attr, m) {
		return nil
	}

	m2, err := c.attrStore.SetAttrs(id, m)
	if err != nil {
		return err
	}
	c.set(id, m2)
	return nil
}

func (c *attrCache) SetBulkAttrs(m map[uint64]map[string]interface{}) error {

	m2, err := c.attrStore.SetBulkAttrs(m)
	if err != nil {
		return err
	}
	for k, v := range m2 {
		c.set(k, v)
	}
	return nil
}

// get returns the cached attributes for a given id.
func (c *attrCache) get(id uint64) map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	attrs := c.attrs[id]
	if attrs == nil {
		return nil
	}

	// Make a copy for safety
	ret := make(map[string]interface{})
	for k, v := range attrs {
		ret[k] = v
	}
	return ret
}

// set updates the cached attributes for a given id.
func (c *attrCache) set(id uint64, attrs map[string]interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attrs[id] = attrs
}

// mapContains returns true if all keys & values of subset are in m.
func mapContains(m, subset map[string]interface{}) bool {
	for k, v := range subset {
		value, ok := m[k]
		if !ok || value != v {
			return false
		}
	}
	return true
}
