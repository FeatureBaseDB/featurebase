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
	"sync"
	"time"

	"github.com/pilosa/pilosa/v2/topology"
)

// EtcdWithCache is a wrapper around the Etcd type which will return a
// cached value when the number of requests come in below a configured
// frequency. It also breaks the cache after a configured TTL.
type EtcdWithCache struct {
	*Etcd

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
	}
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
