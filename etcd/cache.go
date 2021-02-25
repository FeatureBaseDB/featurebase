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

package etcd

import (
	"context"
	"sync"
	"time"

	"github.com/pilosa/pilosa/v2/topology"
)

// EtcdWithCache is a wrapper around the Etcd type which will return a
// cached value when the number of requests come in below a configured
// frequency. It also breaks the cache after a configured TTL.
type EtcdWithCache struct {
	*Etcd

	peerMetadataMu sync.RWMutex
	peerMetadata   map[string][]byte

	peersMu sync.Mutex // peer-list cache updates

	nodes            []*topology.Node // unmarshalled Node data
	nodesTTL         int              // seconds
	nodesLastRequest time.Time        // last time requested
}

// NewEtcdWithCache returns a new instance of Cache.
func NewEtcdWithCache(opt Options, replicas int) *EtcdWithCache {
	return &EtcdWithCache{
		Etcd: NewEtcd(opt, replicas),

		nodesTTL: 6,

		peerMetadata: make(map[string][]byte),
	}
}

// Metadata is a cache wrapper around the Metadator.Metadata method.
func (c *EtcdWithCache) Metadata(ctx context.Context, peerID string) ([]byte, error) {
	c.peerMetadataMu.RLock()
	v, ok := c.peerMetadata[peerID]
	c.peerMetadataMu.RUnlock()
	if ok {
		return v, nil
	}
	v, err := c.Etcd.Metadata(ctx, peerID)
	if err == nil {
		c.peerMetadataMu.Lock()
		c.peerMetadata[peerID] = v
		c.peerMetadataMu.Unlock()
	}
	return v, err
}

// Nodes caches the result of the underlying implementation's node list.
func (c *EtcdWithCache) Nodes() []*topology.Node {
	c.peersMu.Lock()
	defer c.peersMu.Unlock()

	now := time.Now()
	if now.Sub(c.nodesLastRequest) > (time.Duration(c.nodesTTL) * time.Second) {
		c.nodes = c.Etcd.Nodes()
		c.nodesLastRequest = now
	}
	return c.nodes
}

// SetNodes implements the Noder interface as NOP
// (because we can't force to set nodes for etcd).
func (c *EtcdWithCache) SetNodes(nodes []*topology.Node) {}

// AppendNode implements the Noder interface as NOP
// (because resizer is responsible for adding new nodes).
func (c *EtcdWithCache) AppendNode(node *topology.Node) {}

// RemoveNode implements the Noder interface as NOP
// (because resizer is responsible for removing existing nodes)
func (c *EtcdWithCache) RemoveNode(nodeID string) bool {
	return false
}
