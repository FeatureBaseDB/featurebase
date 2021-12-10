// Copyright 2021 Molecula Corp. All rights reserved.
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	"sync"

	pnet "github.com/molecula/featurebase/v2/net"
)

// Cluster contains hosts in a Pilosa cluster.
type Cluster struct {
	hosts       []*pnet.URI
	okList      []bool
	mutex       *sync.RWMutex
	lastHostIdx int
}

// DefaultCluster returns the default Cluster.
func DefaultCluster() *Cluster {
	return &Cluster{
		hosts:  make([]*pnet.URI, 0),
		okList: make([]bool, 0),
		mutex:  &sync.RWMutex{},
	}
}

// NewClusterWithHost returns a cluster with the given URIs.
func NewClusterWithHost(hosts ...*pnet.URI) *Cluster {
	cluster := DefaultCluster()
	for _, host := range hosts {
		cluster.AddHost(host)
	}
	return cluster
}

// AddHost adds a host to the cluster.
func (c *Cluster) AddHost(address *pnet.URI) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.hosts = append(c.hosts, address)
	c.okList = append(c.okList, true)
}

// Host returns a host in the cluster.
func (c *Cluster) Host() *pnet.URI {
	c.mutex.Lock()
	var host *pnet.URI
	for i := range c.okList {
		idx := (i + c.lastHostIdx) % len(c.okList)
		ok := c.okList[idx]
		if ok {
			host = c.hosts[idx]
			break
		}
	}
	c.lastHostIdx++
	c.mutex.Unlock()
	if host != nil {
		return host
	}
	c.reset()
	return host
}

// RemoveHost black lists the host with the given pnet.URI from the cluster.
func (c *Cluster) RemoveHost(address *pnet.URI) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, uri := range c.hosts {
		if uri.Equals(address) {
			c.okList[i] = false
			break
		}
	}
}

// Hosts returns all available hosts in the cluster.
func (c *Cluster) Hosts() []pnet.URI {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	hosts := make([]pnet.URI, 0, len(c.hosts))
	for i, host := range c.hosts {
		if c.okList[i] {
			hosts = append(hosts, *host)
		}
	}
	return hosts
}

func (c *Cluster) reset() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i := range c.okList {
		c.okList[i] = true
	}
}
