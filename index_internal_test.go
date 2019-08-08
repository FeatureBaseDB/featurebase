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

// mustOpenIndex returns a new, opened index at a temporary path. Panic on error.
func mustOpenIndex(opt IndexOptions) *Index {
	path, err := ioutil.TempDir(*TempDir, "pilosa-index-")
	if err != nil {
		panic(err)
	}
	index, err := NewIndex(path, "i")
	if err != nil {
		panic(err)
	}

	index.keys = opt.Keys
	index.trackExistence = opt.TrackExistence
	index.shardDistributor = opt.ShardDistributor

	if err := index.Open(); err != nil {
		panic(err)
	}
	return index
}

// reopen closes the index and reopens it.
func (i *Index) reopen() error {
	if err := i.Close(); err != nil {
		return err
	}
	if err := i.Open(); err != nil {
		return err
	}
	return nil
}

// reopenNew closes the index and reopens it on a new Index object.
func (i *Index) reopenNew() (*Index, error) {
	if err := i.Close(); err != nil {
		return nil, err
	}
	index, err := NewIndex(i.path, "new-index")
	if err != nil {
		return nil, err
	}
	if err := index.Open(); err != nil {
		return nil, err
	}
	return index, nil
}

// Ensure that deleting the existence field is handled properly.
func TestIndex_Existence_Delete(t *testing.T) {
	// Create Index (with existence tracking).
	index := mustOpenIndex(IndexOptions{TrackExistence: true})
	defer index.Close()

	// Ensure existence field has been created.
	ef := index.Field(existenceFieldName)
	if ef == nil {
		t.Fatalf("expected field to have been created: %s", existenceFieldName)
	} else if !index.trackExistence {
		t.Fatalf("expected index.trackExistence to be true")
	} else if index.existenceFld == nil {
		t.Fatalf("expected index.existenceField to be non-nil")
	}

	// Delete existence field.
	if err := index.DeleteField(existenceFieldName); err != nil {
		t.Fatal(err)
	}

	// Re-open index.
	if err := index.reopen(); err != nil {
		t.Fatal(err)
	}

	// Ensure existence field no longer exists.
	ef = index.Field(existenceFieldName)
	if ef != nil {
		t.Fatalf("expected field to have been deleted: %s", existenceFieldName)
	} else if index.trackExistence {
		t.Fatalf("expected index.trackExistence to be false")
	} else if index.existenceFld != nil {
		t.Fatalf("expected index.existenceField to be nil")
	}
}

// Ensure that reopening an index preserves the metadata on its shard distributor.
func TestIndex_ShardDistributor(t *testing.T) {
	// In holder, if the index's shard distributor is empty, it will
	// automatically default to `holder.defaultShardDistributor`.
	// In this test, because there is no way for an index to know the
	// default shard distributor, that value is left blank.
	t.Run("existing empty shard distributor", func(t *testing.T) {
		index0 := mustOpenIndex(IndexOptions{})
		defer index0.Close()

		if index0.shardDistributor != "" {
			t.Fatalf(`expected "", got %v`, index0.shardDistributor)
		}

		newIndex0, err := index0.reopenNew()
		defer newIndex0.Close()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if newIndex0.shardDistributor != "" {
			t.Fatalf(`expected "", got %v`, newIndex0.shardDistributor)
		}
	})

	t.Run("existing shard distributor", func(t *testing.T) {
		index1 := mustOpenIndex(IndexOptions{ShardDistributor: "some-distributor"})
		defer index1.Close()

		if index1.shardDistributor != "some-distributor" {
			t.Fatalf("expected some-distributor, got %v", index1.shardDistributor)
		}

		newIndex1, err := index1.reopenNew()
		defer newIndex1.Close()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if newIndex1.shardDistributor != "some-distributor" {
			t.Fatalf("expected some-distributor, got %v", newIndex1.shardDistributor)
		}
	})
}
