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
	"testing"
)

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
