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

package inmem_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa/v2/inmem"
)

func TestTranslateStore_TranslateColumn(t *testing.T) {
	s := inmem.NewTranslateStore()

	// First translation should start id at zero.
	if ids, err := s.TranslateColumnsToUint64("IDX0", []string{"foo"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Next translation on the same index should move to one.
	if ids, err := s.TranslateColumnsToUint64("IDX0", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{2}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Translation on a different index restarts at 0.
	if ids, err := s.TranslateColumnsToUint64("IDX1", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Ensure that string values can be looked up by ID.
	if value, err := s.TranslateColumnToString("IDX0", 2); err != nil {
		t.Fatal(err)
	} else if value != "bar" {
		t.Fatalf("unexpected value: %s", value)
	}
}

func TestTranslateStore_TranslateRow(t *testing.T) {
	s := inmem.NewTranslateStore()

	// First translation should start id at zero.
	if ids, err := s.TranslateRowsToUint64("IDX0", "FRAME0", []string{"foo"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Next translation on the same index should move to one.
	if ids, err := s.TranslateRowsToUint64("IDX0", "FRAME0", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{2}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Translation on a different index restarts at 0.
	if ids, err := s.TranslateRowsToUint64("IDX1", "FRAME0", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Translation on a different frame restarts at 0.
	if ids, err := s.TranslateRowsToUint64("IDX0", "FRAME1", []string{"bar"}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, []uint64{1}) {
		t.Fatalf("unexpected id: %#v", ids)
	}

	// Ensure that string values can be looked up by ID.
	if value, err := s.TranslateRowToString("IDX0", "FRAME0", 2); err != nil {
		t.Fatal(err)
	} else if value != "bar" {
		t.Fatalf("unexpected value: %s", value)
	}
}

func BenchmarkTranslateStore_TranslateColumnsToUint64(b *testing.B) {
	const batchSize = 1000

	s := inmem.NewTranslateStore()

	// Generate keys before benchmark begins
	keySets := make([][]string, b.N/1000)
	for i := range keySets {
		keySets[i] = make([]string, batchSize)
		for j, jv := range rand.New(rand.NewSource(0)).Perm(batchSize) {
			keySets[i][j] = fmt.Sprintf("%08d%08d", jv, i)
		}
	}

	b.ResetTimer()

	for _, keySet := range keySets {
		if _, err := s.TranslateColumnsToUint64("IDX0", keySet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTranslateStore_TranslateColumnToString(b *testing.B) {
	const batchSize = 1000

	s := inmem.NewTranslateStore()

	// Generate keys before benchmark begins
	for i := 0; i < b.N; i += batchSize {
		keySet := make([]string, batchSize)
		for j, jv := range rand.New(rand.NewSource(0)).Perm(batchSize) {
			keySet[j] = fmt.Sprintf("%08d%08d", jv, i)
		}
		if _, err := s.TranslateColumnsToUint64("IDX0", keySet); err != nil {
			b.Fatal(err)
		}
	}

	// Generate random key access.
	perm := rand.New(rand.NewSource(0)).Perm(b.N)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := s.TranslateColumnToString("IDX0", uint64(perm[i])); err != nil {
			b.Fatal(err)
		}
	}
}
