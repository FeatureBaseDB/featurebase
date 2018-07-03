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
	"testing"
)

// mustOpenView returns a new instance of View with a temporary path.
func mustOpenView(index, field, name string) *view {
	path, err := ioutil.TempDir("", "pilosa-view-")
	if err != nil {
		panic(err)
	}

	v := newView(path, index, field, name, DefaultCacheSize)
	if err := v.open(); err != nil {
		panic(err)
	}
	v.rowAttrStore = newMemAttrStore()
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
