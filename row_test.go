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

package pilosa_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa/v2"
)

// Ensure a row can be merged
func TestRow_Merge(t *testing.T) {
	tests := []struct {
		r1  *pilosa.Row
		r2  *pilosa.Row
		exp uint64
	}{
		{
			r1:  pilosa.NewRow(1, 2, 3, ShardWidth+1, 2*ShardWidth),
			r2:  pilosa.NewRow(3, 4, 5),
			exp: 7,
		},
		{
			r1:  pilosa.NewRow(),
			r2:  pilosa.NewRow(2, 66000, 70000, 70001, 70002, 70003, 70004),
			exp: 7,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("#%d:", i), func(t *testing.T) {
			test.r1.Merge(test.r2)
			if cnt := test.r1.Count(); cnt != test.exp {
				t.Fatalf("merged count %d is not %d", cnt, test.exp)
			}
			if length := len(test.r1.Columns()); uint64(length) != test.exp {
				t.Fatalf("merged length %d is not %d", length, test.exp)
			}
		})
	}
}

// Ensure a row can Xor'ed
func TestRow_Xor(t *testing.T) {
	r1 := pilosa.NewRow(0, 1, ShardWidth)
	r2 := pilosa.NewRow(0, 2*ShardWidth)
	exp := []uint64{1, ShardWidth, 2 * ShardWidth}

	res := r1.Xor(r2)
	if res.Count() != 3 {
		t.Fatalf("Test 1 Count after xor %d != 3\n", res.Count())
	}

	if !reflect.DeepEqual(res.Columns(), exp) {
		t.Fatalf("Test 2 Results %v != expected %v\n", res.Columns(), exp)
	}
	res = r2.Xor(r1)
	if res.Count() != 3 {
		t.Fatalf("Test 3 Count after xor %d != 3\n", res.Count())
	}
	if !reflect.DeepEqual(res.Columns(), exp) {
		t.Fatalf("Test 4 Results %v != expected %v\n", res.Columns(), exp)
	}

}
func TestRow_Union_Segment(t *testing.T) {
	r1 := pilosa.NewRow(0, 1, ShardWidth)
	r2 := pilosa.NewRow(0, 2*ShardWidth)
	exp := []uint64{0, 1, ShardWidth, 2 * ShardWidth}
	res := r1.Union(r2)

	if res.Count() != 4 {
		t.Fatalf("Test 1 Count after Union %d != 5\n", res.Count())
	}
	if !reflect.DeepEqual(res.Columns(), exp) {
		t.Fatalf("Test 2 Union Results %v != expected %v\n", res.Columns(), exp)
	}
	res = r2.Union(r1)
	if res.Count() != 4 {
		t.Fatalf("Test 3 Count after xor %d != 5\n", res.Count())
	}
	if !reflect.DeepEqual(res.Columns(), exp) {
		t.Fatalf("Test 2 Union Results %v != expected %v\n", res.Columns(), exp)
	}
}

func TestRow_Difference_Segment(t *testing.T) {
	r1 := pilosa.NewRow(0, 1, ShardWidth)
	r2 := pilosa.NewRow(0, 2*ShardWidth)
	exp := []uint64{1, ShardWidth}
	res := r1.Difference(r2)

	if res.Count() != 2 {
		t.Fatalf("Test 1 Count after Difference %d != 5\n", res.Count())
	}
	if !reflect.DeepEqual(res.Columns(), exp) {
		t.Fatalf("Test 2 Difference Results %v != expected %v\n", res.Columns(), exp)
	}
}

func TestRow_IsEmpty(t *testing.T) {
	r1 := pilosa.NewRow(1, ShardWidth)
	r2 := pilosa.NewRow(0, 2*ShardWidth)
	res := r2.Intersect(r1)

	if r1.IsEmpty() {
		t.Fatal("r1 Should Not Be Empty\n")
	}
	if !res.IsEmpty() {
		t.Fatal("Result Should Be Empty\n")
	}

}
