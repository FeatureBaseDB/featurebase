// Copyright (C) 2012 Numerotron Inc.
// Use of this source code is governed by an MIT-style license
// that can be found in the LICENSE file.

package consistent

import (
	"runtime"
	"sort"
	"strconv"
	"testing"
	"testing/quick"
)

func checkNum(num, expected int, t *testing.T) {
	if num != expected {
		t.Errorf("expected %d, got %d", expected, num)
	}
}

func TestNew(t *testing.T) {
	x := New()
	if x == nil {
		t.Errorf("expected obj")
	}
	checkNum(x.NumberOfReplicas, 20, t)
}

func TestAdd(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	checkNum(len(x.circle), 20, t)
	checkNum(len(x.sortedHashes), 20, t)
	if sort.IsSorted(x.sortedHashes) == false {
		t.Errorf("expected sorted hashes to be sorted")
	}
	x.Add("qwer")
	checkNum(len(x.circle), 40, t)
	checkNum(len(x.sortedHashes), 40, t)
	if sort.IsSorted(x.sortedHashes) == false {
		t.Errorf("expected sorted hashes to be sorted")
	}
}

func TestRemove(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Remove("abcdefg")
	checkNum(len(x.circle), 0, t)
	checkNum(len(x.sortedHashes), 0, t)
}

func TestRemoveNonExisting(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Remove("abcdefghijk")
	checkNum(len(x.circle), 20, t)
}

func TestGetEmpty(t *testing.T) {
	x := New()
	_, err := x.Get("asdfsadfsadf")
	if err == nil {
		t.Errorf("expected error")
	}
	if err != ErrEmptyCircle {
		t.Errorf("expected empty circle error")
	}
}

func TestGetSingle(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	f := func(s string) bool {
		y, err := x.Get(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		t.Logf("s = %q, y = %q", s, y)
		return y == "abcdefg"
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGetMultiple(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("hijklmn")
	x.Add("opqrstu")
	result, err := x.Get("ggg")
	if err != nil {
		t.Fatal(err)
	}
	if result != "opqrstu" {
		t.Errorf("expected 'opqrstu', got %q", result)
	}
	result, err = x.Get("hhh")
	if err != nil {
		t.Fatal(err)
	}
	if result != "abcdefg" {
		t.Errorf("expected 'abcdefg', got %q", result)
	}
	result, err = x.Get("iiiiii")
	if err != nil {
		t.Fatal(err)
	}
	if result != "hijklmn" {
		t.Errorf("expected 'hijklmn', got %q", result)
	}
}

func TestGetMultipleQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("hijklmn")
	x.Add("opqrstu")
	f := func(s string) bool {
		y, err := x.Get(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		t.Logf("s = %q, y = %q", s, y)
		return y == "abcdefg" || y == "hijklmn" || y == "opqrstu"
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGetMultipleRemove(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("hijklmn")
	x.Add("opqrstu")
	result, err := x.Get("ggg")
	if err != nil {
		t.Fatal(err)
	}
	if result != "opqrstu" {
		t.Errorf("expected 'opqrstu', got %q", result)
	}
	result, err = x.Get("hhh")
	if err != nil {
		t.Fatal(err)
	}
	if result != "abcdefg" {
		t.Errorf("expected 'abcdefg', got %q", result)
	}
	result, err = x.Get("iiiiii")
	if err != nil {
		t.Fatal(err)
	}
	if result != "hijklmn" {
		t.Errorf("expected 'hijklmn', got %q", result)
	}
	x.Remove("hijklmn")
	result, err = x.Get("ggg")
	if err != nil {
		t.Fatal(err)
	}
	if result != "opqrstu" {
		t.Errorf("expected 'opqrstu', got %q", result)
	}
	result, err = x.Get("hhh")
	if err != nil {
		t.Fatal(err)
	}
	if result != "abcdefg" {
		t.Errorf("expected 'abcdefg', got %q", result)
	}
	result, err = x.Get("iiiiii")
	if err != nil {
		t.Fatal(err)
	}
	if result != "opqrstu" {
		t.Errorf("expected 'opqrstu', got %q", result)
	}
}

func TestGetMultipleRemoveQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("hijklmn")
	x.Add("opqrstu")
	x.Remove("opqrstu")
	f := func(s string) bool {
		y, err := x.Get(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		t.Logf("s = %q, y = %q", s, y)
		return y == "abcdefg" || y == "hijklmn"
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGetTwo(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("hijklmn")
	x.Add("opqrstu")
	a, b, err := x.GetTwo("99999999")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Errorf("a shouldn't equal b")
	}
	if a != "abcdefg" {
		t.Errorf("wrong a: %q", a)
	}
	if b != "opqrstu" {
		t.Errorf("wrong b: %q", b)
	}
}

func TestGetTwoQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("hijklmn")
	x.Add("opqrstu")
	f := func(s string) bool {
		a, b, err := x.GetTwo(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		if a == b {
			t.Logf("a == b")
			return false
		}
		if a != "abcdefg" && a != "hijklmn" && a != "opqrstu" {
			t.Logf("invalid a: %q", a)
			return false
		}

		if b != "abcdefg" && b != "hijklmn" && b != "opqrstu" {
			t.Logf("invalid b: %q", b)
			return false
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGetTwoOnlyTwoQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("hijklmn")
	f := func(s string) bool {
		a, b, err := x.GetTwo(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		if a == b {
			t.Logf("a == b")
			return false
		}
		if a != "abcdefg" && a != "hijklmn" {
			t.Logf("invalid a: %q", a)
			return false
		}

		if b != "abcdefg" && b != "hijklmn" {
			t.Logf("invalid b: %q", b)
			return false
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

func TestGetTwoOnlyOneInCircle(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	a, b, err := x.GetTwo("99999999")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Errorf("a shouldn't equal b")
	}
	if a != "abcdefg" {
		t.Errorf("wrong a: %q", a)
	}
	if b != "" {
		t.Errorf("wrong b: %q", b)
	}
}

func TestSet(t *testing.T) {
	x := New()
	x.Add("abc")
	x.Add("def")
	x.Add("ghi")
	x.Set([]string{"jkl", "mno"})
	if x.count != 2 {
		t.Errorf("expected 2 elts, got %d", x.count)
	}
	a, b, err := x.GetTwo("qwerqwerwqer")
	if err != nil {
		t.Fatal(err)
	}
	if a != "jkl" && a != "mno" {
		t.Errorf("expected jkl or mno, got %s", a)
	}
	if b != "jkl" && b != "mno" {
		t.Errorf("expected jkl or mno, got %s", b)
	}
	if a == b {
		t.Errorf("expected a != b, they were both %s", a)
	}
	x.Set([]string{"pqr", "mno"})
	if x.count != 2 {
		t.Errorf("expected 2 elts, got %d", x.count)
	}
	a, b, err = x.GetTwo("qwerqwerwqer")
	if err != nil {
		t.Fatal(err)
	}
	if a != "pqr" && a != "mno" {
		t.Errorf("expected jkl or mno, got %s", a)
	}
	if b != "pqr" && b != "mno" {
		t.Errorf("expected jkl or mno, got %s", b)
	}
	if a == b {
		t.Errorf("expected a != b, they were both %s", a)
	}
	x.Set([]string{"pqr", "mno"})
	if x.count != 2 {
		t.Errorf("expected 2 elts, got %d", x.count)
	}
	a, b, err = x.GetTwo("qwerqwerwqer")
	if err != nil {
		t.Fatal(err)
	}
	if a != "pqr" && a != "mno" {
		t.Errorf("expected jkl or mno, got %s", a)
	}
	if b != "pqr" && b != "mno" {
		t.Errorf("expected jkl or mno, got %s", b)
	}
	if a == b {
		t.Errorf("expected a != b, they were both %s", a)
	}
}

// allocBytes returns the number of bytes allocated by invoking f.
func allocBytes(f func()) uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	t := stats.TotalAlloc
	f()
	runtime.ReadMemStats(&stats)
	return stats.TotalAlloc - t
}

func mallocNum(f func()) uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	t := stats.Mallocs
	f()
	runtime.ReadMemStats(&stats)
	return stats.Mallocs - t
}

func BenchmarkAllocations(b *testing.B) {
	x := New()
	x.Add("stays")
	b.ResetTimer()
	allocSize := allocBytes(func() {
		for i := 0; i < b.N; i++ {
			x.Add("Foo")
			x.Remove("Foo")
		}
	})
	b.Logf("%d: Allocated %d bytes (%.2fx)", b.N, allocSize, float64(allocSize)/float64(b.N))
}

func BenchmarkMalloc(b *testing.B) {
	x := New()
	x.Add("stays")
	b.ResetTimer()
	mallocs := mallocNum(func() {
		for i := 0; i < b.N; i++ {
			x.Add("Foo")
			x.Remove("Foo")
		}
	})
	b.Logf("%d: Mallocd %d times (%.2fx)", b.N, mallocs, float64(mallocs)/float64(b.N))
}

func BenchmarkCycle(b *testing.B) {
	x := New()
	x.Add("nothing")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Add("foo" + strconv.Itoa(i))
		x.Remove("foo" + strconv.Itoa(i))
	}
}

func BenchmarkCycleLarge(b *testing.B) {
	x := New()
	for i := 0; i < 10; i++ {
		x.Add("start" + strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Add("foo" + strconv.Itoa(i))
		x.Remove("foo" + strconv.Itoa(i))
	}
}

func BenchmarkGet(b *testing.B) {
	x := New()
	x.Add("nothing")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Get("nothing")
	}
}

func BenchmarkGetLarge(b *testing.B) {
	x := New()
	for i := 0; i < 10; i++ {
		x.Add("start" + strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Get("nothing")
	}
}
