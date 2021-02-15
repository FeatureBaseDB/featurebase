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

	"github.com/pilosa/pilosa/v2/disco"
	"github.com/pilosa/pilosa/v2/topology"
)

// EtcdWithCache is a wrapper around the Etcd type which will return a
// cached value when the number of requests come in below a configured
// frequency. It also breaks the cache after a configured TTL.
type EtcdWithCache struct {
	*Etcd

	peerMetadataMu sync.RWMutex
	peerMetadata   map[string][]byte

	stateMu sync.Mutex // cluster state cache updates
	peersMu sync.Mutex // peer-list cache updates

	nodes              []*topology.Node // unmarshalled Node data
	nodesTTL           int              // seconds
	nodesLastRequest   time.Time        // last time requested
	nodeStates         map[string]nodeState
	nodeStateTTL       int // seconds
	nodeStateFrequency int // max requests per second allowed before using the cache

	clusterStateVal         disco.ClusterState
	clusterStateTTL         int // seconds
	clusterStateFrequency   int // max requests per second allowed before using the cache
	clusterStateLastRequest time.Time
	clusterStateLastCache   time.Time
}

type nodeState struct {
	val         disco.NodeState
	lastRequest time.Time
	lastCache   time.Time
}

// NewEtcdWithCache returns a new instance of Cache.
func NewEtcdWithCache(opt Options, replicas int) *EtcdWithCache {
	return &EtcdWithCache{
		Etcd: NewEtcd(opt, replicas),

		nodeStateTTL:          6,
		nodeStateFrequency:    1,
		clusterStateTTL:       6,
		clusterStateFrequency: 1,
		nodesTTL:              6,

		peerMetadata: make(map[string][]byte),
		nodeStates:   make(map[string]nodeState),
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

// ClusterState is a cache wrapper around the Stator.ClusterState method.
func (c *EtcdWithCache) ClusterState(ctx context.Context) (disco.ClusterState, error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	now := time.Now()
	if now.Sub(c.clusterStateLastCache) > (time.Duration(c.clusterStateTTL)*time.Second) ||
		now.Sub(c.clusterStateLastRequest) > (time.Second/time.Duration(c.clusterStateFrequency)) {
		v, err := c.Etcd.ClusterState(ctx)
		if err == nil {
			// In order to avoid NodeState() returning a cached value after
			// cluster state has changed, we reset the node state caches to
			// ensure that the next call to NodeState() returns the latest
			// value. And we only need to do this if the cluster state value has
			// actually changed.
			if c.clusterStateVal != v {
				for k, ns := range c.nodeStates {
					ns.lastCache = time.Time{}
					c.nodeStates[k] = ns
				}
			}

			c.clusterStateVal = v
			c.clusterStateLastCache = now
			c.clusterStateLastRequest = now
		}
		return v, err
	}
	c.clusterStateLastRequest = now
	return c.clusterStateVal, nil
}

// NodeState is a cache wrapper around the Stator.NodeState method.
func (c *EtcdWithCache) NodeState(ctx context.Context, peerID string) (disco.NodeState, error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	ns := c.nodeStates[peerID]

	now := time.Now()
	if now.Sub(ns.lastCache) > (time.Duration(c.nodeStateTTL)*time.Second) ||
		now.Sub(ns.lastRequest) > (time.Second/time.Duration(c.nodeStateFrequency)) {
		v, err := c.Etcd.NodeState(ctx, peerID)
		if err == nil {
			// In order to avoid ClusterState() returning a cached value after a
			// node state has changed, we reset the cluster state cache to
			// ensure that the next call to ClusterState() returns the latest
			// value. And we only need to do this if the node state value has
			// actually changed.
			if ns.val != v {
				c.clusterStateLastCache = time.Time{}
			}

			ns.val = v
			ns.lastCache = now
			ns.lastRequest = now
			c.nodeStates[peerID] = ns
		}
		return v, err
	}
	ns.lastRequest = now
	c.nodeStates[peerID] = ns
	return ns.val, nil
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
