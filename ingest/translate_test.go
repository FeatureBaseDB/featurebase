// Copyright 2021 Molecula Corp. All rights reserved.
package ingest

import (
	"fmt"
	"testing"
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

func TestTranslateReuse(t *testing.T) {
	// the original stable-translator design had a flaw in that it
	// assumed that each new ID would always come from a string translation,
	// never from a key translation, and that they'd show up sequentially.
	tr := newStableTranslator()
	orig, err := tr.TranslateIDs(1, 2)
	tr.next = 1 // intentionally break the translation logic for testing purposes
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var s [6]string
	stash := s[:0]
	for _, v := range orig {
		stash = append(stash, v)
	}
	for i := range s[len(orig):] {
		stash = append(stash, fmt.Sprintf("key-%d", i))
	}
	keys, err := tr.TranslateKeys(stash...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ids := make([]uint64, 0, len(keys))
	for _, v := range keys {
		ids = append(ids, v)
	}
	idMap, err := tr.TranslateIDs(ids...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for k, v := range keys {
		if idMap[v] != k {
			t.Fatalf("translate mismatch: keys %q->%d, ids %d->%q",
				k, v, v, idMap[v])
		}
	}
	for id, key := range idMap {
		if keys[key] != id {
			t.Fatalf("translate mismatch: ids %d->%q, keys %q->%d",
				id, key, key, keys[key])
		}
	}
	for id, key := range orig {
		if keys[key] != id {
			t.Fatalf("translate mismatch: original ids %d->%q, keys %q->%d",
				id, key, key, keys[key])
		}
	}
}
