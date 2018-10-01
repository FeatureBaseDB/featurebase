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

package pilosa

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

// mustOpenView returns a new instance of View with a temporary path.
func mustOpenView(index, field, name string) *view {
	path, err := ioutil.TempDir("", "pilosa-view-")
	if err != nil {
		panic(err)
	}

	fo := FieldOptions{
		CacheType: DefaultCacheType,
		CacheSize: DefaultCacheSize,
	}

	v := newView(path, index, field, name, fo)
	if err := v.open(); err != nil {
		panic(err)
	}
	v.rowAttrStore = &memAttrStore{
		store: make(map[uint64]map[string]interface{}),
	}
	return v
}

// Ensure view can open and retrieve a fragment.
func TestView_DeleteFragment(t *testing.T) {
	v := mustOpenView("i", "f", "v")
	defer v.close()

	shard := uint64(9)

	// Create fragment.
	fragment, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		t.Fatal(err)
	} else if fragment == nil {
		t.Fatal("expected fragment")
	}

	err = v.deleteFragment(shard)
	if err != nil {
		t.Fatal(err)
	}

	if v.Fragment(shard) != nil {
		t.Fatal("fragment still exists in view")
	}

	// Recreate fragment with same shard, verify that the old fragment was not reused.
	fragment2, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		t.Fatal(err)
	} else if fragment == fragment2 {
		t.Fatal("failed to create new fragment")
	}
}

// Ensure view closes fragment after failed shard broadcast.
func TestView_CreateFragmentError(t *testing.T) {
	v := mustOpenView("i", "f", "v")
	defer v.close()

	// Use a broadcaster which intentionally fails.
	v.broadcaster = errorBroadcaster{}

	shard := uint64(0)

	// Create fragment (with error on broadcast).
	fragment, err := v.CreateFragmentIfNotExists(shard)
	if !strings.Contains(err.Error(), "intentional error") {
		if err != nil {
			t.Fatal(err)
		} else if fragment == nil {
			t.Fatal("expected fragment")
		} else {
			t.Fatal("expected intentional error")
		}
	}

	// Set the broadcaster back to no-op.
	v.broadcaster = nopBroadcaster{}

	// Try to create the fragment again.
	_, err = v.CreateFragmentIfNotExists(shard)
	if err != nil {
		t.Fatal(err)
	}
}

// errorBroadcaster is a broadcaster which always returns an error.
type errorBroadcaster struct {
	nopBroadcaster
}

// SendSync is an implementation of Broadcaster SendSync which always returns an error.
func (errorBroadcaster) SendSync(Message) error {
	return errors.New("intentional error")
}
