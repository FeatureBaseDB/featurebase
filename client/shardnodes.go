package client

import (
	"sync"

	pnet "github.com/pilosa/pilosa/v2/net"
)

type shardNodes struct {
	data map[string]map[uint64][]*pnet.URI
	mu   *sync.RWMutex
}

func newShardNodes() shardNodes {
	return shardNodes{
		data: make(map[string]map[uint64][]*pnet.URI),
		mu:   &sync.RWMutex{},
	}
}

func (s shardNodes) Get(index string, shard uint64) ([]*pnet.URI, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if idx, ok := s.data[index]; ok {
		if uris, ok := idx[shard]; ok {
			return uris, true
		}
	}
	return nil, false
}

func (s shardNodes) Put(index string, shard uint64, uris []*pnet.URI) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.data[index]
	if !ok {
		idx = make(map[uint64][]*pnet.URI)
	}
	idx[shard] = uris
	s.data[index] = idx
}

func (s shardNodes) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.data {
		delete(s.data, k)
	}
}
