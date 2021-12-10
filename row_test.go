// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/molecula/featurebase/v2"
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

func TestRow_Includes(t *testing.T) {
	row := pilosa.NewRow(0, 2*ShardWidth)

	if !row.Includes(0) {
		t.Fatal("row should include 0")
	}
	if row.Includes(1) {
		t.Fatal("row should not include 1")
	}
	if !row.Includes(2 * ShardWidth) {
		t.Fatalf("row should include %d", 2*ShardWidth)
	}
}

func TestRow_DifferenceInPlace(t *testing.T) {
	row0 := pilosa.NewRow(0)
	row1 := pilosa.NewRow()
	row2 := pilosa.NewRow(0)
	res := row0.Difference(row1, row2)

	if !row0.Includes(0) {
		t.Fatal("row should include 0")
	}
	if res.Count() != 0 {
		t.Fatal("results should be empty")
	}
}
func eq(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
func TestRow_DifferenceInPlace2(t *testing.T) {
	lucky := []uint64{1, 3, 7, 9, 13, 15, 21, 25, 31, 33, 37, 43, 49, 51, 63, 67, 69, 73, 75, 79, 87, 93, 99, 105, 111, 115, 127, 129, 133, 135, 141, 151, 159, 163, 169, 171, 189, 193, 195, 201, 205, 211, 219, 223, 231, 235, 237, 241, 259, 261, 267, 273, 283, 285, 289, 297}
	src := pilosa.NewRow(lucky...)
	row1 := pilosa.NewRow()
	m := uint64(9)
	for i := m; i <= lucky[len(lucky)-1]; i += m {
		row1.SetBit(i)
	}
	m = uint64(3)
	row2 := pilosa.NewRow()
	for i := m; i <= lucky[len(lucky)-1]; i += m {
		row2.SetBit(i)
	}
	row3 := pilosa.NewRow()
	m = uint64(5)
	for i := m; i <= lucky[len(lucky)-1]; i += m {
		row3.SetBit(i)
	}
	row4 := pilosa.NewRow()
	m = uint64(7)
	for i := m; i <= lucky[len(lucky)-1]; i += m {
		row4.SetBit(i)
	}
	res5 := src.Difference(row1, row2, row3, row4)
	res6 := src.Difference(row4, row3, row2, row1)
	res7 := src.Difference(row4).Difference(row3).Difference(row2).Difference(row1)
	if !eq(res5.Columns(), res6.Columns()) {
		t.Fatalf("results do not match: %v, %v", res5.Columns(), res6.Columns())
	}
	if !eq(res6.Columns(), res7.Columns()) {
		t.Fatalf("results do not match: %v, %v", res6.Columns(), res7.Columns())
	}
}
