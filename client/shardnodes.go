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

// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	"sync"

	pnet "github.com/molecula/featurebase/v2/net"
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
