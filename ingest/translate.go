// Copyright 2021 Molecula Corp.
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

package ingest

import (
	"fmt"
)

// stableTranslator implements a key translator that can be reused and
// will continue to give the same keys for the same values. Possibly
// surprisingly, it will invent new keys for IDs it is asked about but
// hasn't seen. This allows us to give a codec which would use keys on
// translation a request which contains arbitrary numbers, and request
// text that would parse into that request.
type stableTranslator struct {
	in   map[string]uint64
	out  map[uint64]string
	next uint64
}

func (s *stableTranslator) TranslateKeys(keys ...string) (map[string]uint64, error) {
	ret := make(map[string]uint64, len(keys))
	for _, key := range keys {
		if existing, ok := s.in[key]; ok {
			ret[key] = existing
			continue
		}
		id := s.next
		// but what if someone already translated that ID, so now it already
		// exists?
		if _, ok := s.out[id]; ok {
			for k := range s.out {
				if k > id {
					id = k
				}
			}
			// one larger than the largest we already have. this could
			// wrap around, in which case, it's your own fault.
			id++
		}
		s.next = id + 1
		s.in[key] = id
		s.out[id] = key
		ret[key] = id
	}
	return ret, nil
}

func (s *stableTranslator) TranslateIDs(ids ...uint64) (map[uint64]string, error) {
	ret := make(map[uint64]string, len(ids))
	for _, id := range ids {
		if existing, ok := s.out[id]; ok {
			ret[id] = existing
			continue
		}
		key := fmt.Sprintf("k-%d", id)
		s.in[key] = id
		s.out[id] = key
		ret[id] = key
		if id >= s.next {
			s.next = id + 1
		}
	}
	return ret, nil
}

// newStableTranslator produces a translator which can translate forwards
// and backwards and invent new things if it needs to. Don't use this.
func newStableTranslator() *stableTranslator {
	return &stableTranslator{
		in:  make(map[string]uint64),
		out: make(map[uint64]string),
	}
}
