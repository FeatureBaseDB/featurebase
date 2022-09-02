// Copyright 2014 The b Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the NOTICE file.

package roaring

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"path"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"modernc.org/mathutil"
	"modernc.org/strutil"
)

var caller = func(s string, va ...interface{}) {
	_, fn, fl, _ := runtime.Caller(2)
	fmt.Printf("%s:%d: ", path.Base(fn), fl)
	fmt.Printf(s, va...)
	fmt.Println()
}

func dbg(s string, va ...interface{}) {
	if s == "" {
		s = strings.Repeat("%v ", len(va))
	}
	_, fn, fl, _ := runtime.Caller(1)
	fmt.Printf("%s:%d: ", path.Base(fn), fl)
	fmt.Printf(s, va...)
	fmt.Println()
}

func TODO(...interface{}) string { //TODOOK
	_, fn, fl, _ := runtime.Caller(1)
	return fmt.Sprintf("TODO: %s:%d:\n", path.Base(fn), fl) //TODOOK
}

func use(...interface{}) {}

func init() { // nolint: gochecknoinits
	use(caller, dbg, TODO, isNil, (*tree).dump) //TODOOK
}

// ============================================================================

func isNil(p interface{}) bool {
	switch x := p.(type) {
	case *x:
		if x == nil {
			return true
		}
	case *d:
		if x == nil {
			return true
		}
	}
	return false
}

func (t *tree) dump() string {
	var buf bytes.Buffer
	f := strutil.IndentFormatter(&buf, "\t")

	num := map[interface{}]int{}
	visited := map[interface{}]bool{}

	handle := func(p interface{}) int {
		if isNil(p) {
			return 0
		}

		if n, ok := num[p]; ok {
			return n
		}

		n := len(num) + 1
		num[p] = n
		return n
	}

	var pagedump func(interface{}, string)
	pagedump = func(p interface{}, pref string) {
		if isNil(p) || visited[p] {
			return
		}

		visited[p] = true
		switch x := p.(type) {
		case *x:
			h := handle(p)
			n := 0
			for i, v := range x.x {
				if v.ch != nil || v.k != 0 {
					n = i + 1
				}
			}
			_, _ = f.Format("%sX#%d(%p) n %d:%d {", pref, h, x, x.c, n)
			a := []interface{}{}
			for i, v := range x.x[:n] {
				a = append(a, v.ch)
				if i != 0 {
					_, _ = f.Format(" ")
				}
				_, _ = f.Format("(C#%d K %v)", handle(v.ch), v.k)
			}
			_, _ = f.Format("}\n")
			for _, p := range a {
				pagedump(p, pref+". ")
			}
		case *d:
			h := handle(p)
			n := 0
			for i, d := range x.d {
				if d.v != nil {
					n = i + 1
				}
			}
			_, _ = f.Format("%sD#%d(%p) P#%d N#%d n %d:%d {", pref, h, x, handle(x.p), handle(x.n), x.c, n)
			for i, d := range x.d[:n] {
				if i != 0 {
					_, _ = f.Format(" ")
				}
				_, _ = f.Format("%v:%v", d.k, d.v)
			}
			_, _ = f.Format("}\n")
		}
	}

	pagedump(t.r, "")
	s := buf.String()
	if s != "" {
		s = s[:len(s)-1]
	}
	return s
}

func rng() *mathutil.FC32 {
	x, err := mathutil.NewFC32(math.MinInt32/4, math.MaxInt32/4, false)
	if err != nil {
		panic(err)
	}

	return x
}

var dummyC [1000]Container

func getDummyC(i int) *Container {
	if i < 0 {
		i *= -1
	}
	return &dummyC[i%len(dummyC)]
}

func copyBenchmark(fn func(int64) uint64) func(b *testing.B) {
	return func(b *testing.B) {
		r := treeNew()
		for i := int64(0); i < int64(b.N); i++ {
			j := fn(i)
			r.Set(j, getDummyC(0))
		}
		b.Logf("copied %d de for N=%d\n", r.countCopies(), b.N)
	}
}

func BenchmarkBtreeDeCopies(b *testing.B) {
	b.Run("Linear", copyBenchmark(func(i int64) uint64 { return uint64(i) }))
	b.Run("Random", copyBenchmark(func(i int64) uint64 { return rand.Uint64() }))
}

func TestBtreeGet0(t *testing.T) {
	r := treeNew()
	if g, e := r.Len(), 0; g != e {
		t.Fatal(g, e)
	}

	_, ok := r.Get(42)
	if ok {
		t.Fatal(ok)
	}

}

func TestBtreeSetGet0(t *testing.T) {
	r := treeNew()
	set := r.Set
	set(42, getDummyC(0))
	if g, e := r.Len(), 1; g != e {
		t.Fatal(g, e)
	}

	v, ok := r.Get(42)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, getDummyC(0); g != e {
		t.Fatal(g, e)
	}

	set(42, getDummyC(1))
	if g, e := r.Len(), 1; g != e {
		t.Fatal(g, e)
	}

	v, ok = r.Get(42)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, getDummyC(1); g != e {
		t.Fatal(g, e)
	}

	set(420, getDummyC(2))
	if g, e := r.Len(), 2; g != e {
		t.Fatal(g, e)
	}

	v, ok = r.Get(42)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, getDummyC(1); g != e {
		t.Fatal(g, e)
	}

	v, ok = r.Get(420)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, getDummyC(2); g != e {
		t.Fatal(g, e)
	}
}

func TestBtreeSetGet1(t *testing.T) {
	const N = 40000
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := treeNew()
		set := r.Set
		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		for i, k := range a {
			set(uint64(k), getDummyC((k^x)%10))
			if g, e := r.Len(), i+1; g != e {
				t.Fatal(i, g, e)
			}
		}

		for i, k := range a {
			v, ok := r.Get(uint64(k))
			if !ok {
				t.Fatal(i, k, v, ok)
			}

			if g, e := v, getDummyC((k^x)%10); g != e {
				t.Fatal(i, g, e)
			}

			k |= 1
			_, ok = r.Get(uint64(k))
			if ok {
				t.Fatal(i, k)
			}

		}

		for _, k := range a {
			r.Set(uint64(k), getDummyC((k^x)+42))
		}

		for i, k := range a {
			v, ok := r.Get(uint64(k))
			if !ok {
				t.Fatal(i, k, v, ok)
			}

			if g, e := v, getDummyC((k^x)+42); g != e {
				t.Fatal(i, g, e)
			}

			k |= 1
			_, ok = r.Get(uint64(k))
			if ok {
				t.Fatal(i, k)
			}
		}
	}
}

func TestBtreePrealloc(*testing.T) {
	const n = 2e6
	rng := rng()
	a := make([]int, n)
	for i := range a {
		a[i] = rng.Next()
	}
	r := treeNew()
	for _, v := range a {
		r.Set(uint64(v), nil)
	}
	r.Close()
}

func TestBtreeSplitXOnEdge(t *testing.T) {
	// verify how splitX works when splitting X for k pointing directly at split edge
	tr := treeNew()

	set := tr.Set
	// one index page with 2*kx+2 elements (last has .k=∞  so x.c=2*kx+1)
	// which will splitX on next Set
	for i := 0; i <= (2*kx+1)*2*kd; i++ {
		// odd keys are left to be filled in second test
		set(uint64(2*i), getDummyC(2*i))
	}

	x0 := tr.r.(*x)
	if x0.c != 2*kx+1 {
		t.Fatalf("x0.c: %v  ; expected %v", x0.c, 2*kx+1)
	}

	// set element with k directly at x0[kx].k
	kedge := uint64(2 * (kx + 1) * (2 * kd))
	if x0.x[kx].k != kedge {
		t.Fatalf("edge key before splitX: %v  ; expected %v", x0.x[kx].k, kedge)
	}
	set(uint64(kedge), getDummyC(777))

	// if splitX was wrong kedge:777 would land into wrong place with Get failing
	v, ok := tr.Get(kedge)
	if !(v == getDummyC(777) && ok) {
		t.Fatalf("after splitX: Get(%v) -> %v, %v  ; expected 777, true", kedge, v, ok)
	}

	// now check the same when splitted X has parent
	xr := tr.r.(*x)
	if xr.c != 1 { // second x comes with k=∞ with .c index
		t.Fatalf("after splitX: xr.c: %v  ; expected 1", xr.c)
	}

	if xr.x[0].ch != x0 {
		t.Fatal("xr[0].ch is not x0")
	}

	for i := 0; i <= (2*kx)*kd; i++ {
		set(uint64(2*i+1), getDummyC(2*i+1))
	}

	// check x0 is in pre-splitX condition and still at the right place
	if x0.c != 2*kx+1 {
		t.Fatalf("x0.c: %v  ; expected %v", x0.c, 2*kx+1)
	}
	if xr.x[0].ch != x0 {
		t.Fatal("xr[0].ch is not x0")
	}

	// set element with k directly at x0[kx].k
	kedge = (kx + 1) * (2 * kd)
	if x0.x[kx].k != kedge {
		t.Fatalf("edge key before splitX: %v  ; expected %v", x0.x[kx].k, kedge)
	}
	set(uint64(kedge), getDummyC(888))

	// if splitX was wrong kedge:888 would land into wrong place
	v, ok = tr.Get(kedge)
	if !(v == getDummyC(888) && ok) {
		t.Fatalf("after splitX: Get(%v) -> %v, %v  ; expected 888, true", kedge, v, ok)
	}
}

func BenchmarkBtreeSetSeq1e3(b *testing.B) {
	benchmarkSetSeq(b, 1e3)
}

func BenchmarkBtreeSetSeq1e4(b *testing.B) {
	benchmarkSetSeq(b, 1e4)
}

func BenchmarkBtreeSetSeq1e5(b *testing.B) {
	benchmarkSetSeq(b, 1e5)
}

func BenchmarkBtreeSetSeq1e6(b *testing.B) {
	benchmarkSetSeq(b, 1e6)
}

func benchmarkSetSeq(b *testing.B, n int) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := treeNew()
		debug.FreeOSMemory()
		b.StartTimer()
		for j := 0; j < n; j++ {
			r.Set(uint64(j), getDummyC(j))
		}
		b.StopTimer()
		r.Close()
	}
	b.StopTimer()
}

func BenchmarkBtreeGetSeq1e3(b *testing.B) {
	benchmarkGetSeq(b, 1e3)
}

func BenchmarkBtreeGetSeq1e4(b *testing.B) {
	benchmarkGetSeq(b, 1e4)
}

func BenchmarkBtreeGetSeq1e5(b *testing.B) {
	benchmarkGetSeq(b, 1e5)
}

func BenchmarkBtreeGetSeq1e6(b *testing.B) {
	benchmarkGetSeq(b, 1e6)
}

func benchmarkGetSeq(b *testing.B, n int) {
	r := treeNew()
	for i := 0; i < n; i++ {
		r.Set(uint64(i), getDummyC(i))
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			_, _ = r.Get(uint64(j))
		}
	}
	b.StopTimer()
	r.Close()
}

func BenchmarkBtreeSetRnd1e3(b *testing.B) {
	benchmarkSetRnd(b, 1e3)
}

func BenchmarkBtreeSetRnd1e4(b *testing.B) {
	benchmarkSetRnd(b, 1e4)
}

func BenchmarkBtreeSetRnd1e5(b *testing.B) {
	benchmarkSetRnd(b, 1e5)
}

func BenchmarkBtreeSetRnd1e6(b *testing.B) {
	benchmarkSetRnd(b, 1e6)
}

func benchmarkSetRnd(b *testing.B, n int) {
	rng := rng()
	a := make([]int, n)
	for i := range a {
		a[i] = rng.Next()
	}
	b.ResetTimer()
	b.ReportAllocs()
	c := getDummyC(1)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := treeNew()
		debug.FreeOSMemory()
		b.StartTimer()
		for _, v := range a {
			r.Set(uint64(v), c)
		}
		b.StopTimer()
		r.Close()
	}
	b.StopTimer()
}

func BenchmarkBtreeGetRnd1e3(b *testing.B) {
	benchmarkGetRnd(b, 1e3)
}

func BenchmarkBtreeGetRnd1e4(b *testing.B) {
	benchmarkGetRnd(b, 1e4)
}

func BenchmarkBtreeGetRnd1e5(b *testing.B) {
	benchmarkGetRnd(b, 1e5)
}

func BenchmarkBtreeGetRnd1e6(b *testing.B) {
	benchmarkGetRnd(b, 1e6)
}

func benchmarkGetRnd(b *testing.B, n int) {
	r := treeNew()
	rng := rng()
	a := make([]int, n)
	for i := range a {
		a[i] = rng.Next()
	}
	c := getDummyC(1)
	for _, v := range a {
		r.Set(uint64(v), c)
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			_, _ = r.Get(uint64(v))
		}
	}
	b.StopTimer()
	r.Close()
}

func TestBtreeSetGet2(t *testing.T) {
	const N = 40000
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		rng := rng()
		r := treeNew()
		set := r.Set
		a := make([]int, N)
		for i := range a {
			a[i] = (rng.Next() ^ x) << 1
		}
		for i, k := range a {
			set(uint64(k), getDummyC(k^x))
			if g, e := r.Len(), i+1; g != e {
				t.Fatal(i, x, g, e)
			}
		}

		for i, k := range a {
			v, ok := r.Get(uint64(k))
			if !ok {
				t.Fatal(i, k, v, ok)
			}

			if g, e := v, getDummyC(k^x); g != e {
				t.Fatal(i, g, e)
			}

			k |= 1
			_, ok = r.Get(uint64(k))
			if ok {
				t.Fatal(i, k)
			}
		}

		for _, k := range a {
			r.Set(uint64(k), getDummyC((k^x)+42))
		}

		for i, k := range a {
			v, ok := r.Get(uint64(k))
			if !ok {
				t.Fatal(i, k, v, ok)
			}

			if g, e := v, getDummyC(k^x+42); g != e {
				t.Fatal(i, g, e)
			}

			k |= 1
			_, ok = r.Get(uint64(k))
			if ok {
				t.Fatal(i, k)
			}
		}
	}
}

func TestBtreeSetGet3(t *testing.T) {
	r := treeNew()
	set := r.Set
	var i int
	for i = 0; ; i++ {
		set(uint64(i), getDummyC(-i))
		if _, ok := r.r.(*x); ok {
			break
		}
	}
	for j := 0; j <= i; j++ {
		set(uint64(j), getDummyC(j))
	}

	for j := 0; j <= i; j++ {
		v, ok := r.Get(uint64(j))
		if !ok {
			t.Fatal(j)
		}

		if g, e := v, getDummyC(j); g != e {
			t.Fatal(g, e)
		}
	}
}

func TestBtreeDelete0(t *testing.T) {
	r := treeNew()
	if ok := r.Delete(0); ok {
		t.Fatal(ok)
	}

	if g, e := r.Len(), 0; g != e {
		t.Fatal(g, e)
	}

	r.Set(uint64(0), getDummyC(0))
	if ok := r.Delete(1); ok {
		t.Fatal(ok)
	}

	if g, e := r.Len(), 1; g != e {
		t.Fatal(g, e)
	}

	if ok := r.Delete(0); !ok {
		t.Fatal(ok)
	}

	if g, e := r.Len(), 0; g != e {
		t.Fatal(g, e)
	}

	if ok := r.Delete(0); ok {
		t.Fatal(ok)
	}

	r.Set(uint64(0), getDummyC(0))
	r.Set(uint64(1), getDummyC(1))
	if ok := r.Delete(1); !ok {
		t.Fatal(ok)
	}

	if g, e := r.Len(), 1; g != e {
		t.Fatal(g, e)
	}

	if ok := r.Delete(1); ok {
		t.Fatal(ok)
	}

	if ok := r.Delete(0); !ok {
		t.Fatal(ok)
	}

	if g, e := r.Len(), 0; g != e {
		t.Fatal(g, e)
	}

	if ok := r.Delete(0); ok {
		t.Fatal(ok)
	}

	r.Set(uint64(0), getDummyC(0))
	r.Set(uint64(1), getDummyC(1))
	if ok := r.Delete(0); !ok {
		t.Fatal(ok)
	}

	if g, e := r.Len(), 1; g != e {
		t.Fatal(g, e)
	}

	if ok := r.Delete(0); ok {
		t.Fatal(ok)
	}

	if ok := r.Delete(1); !ok {
		t.Fatal(ok)
	}

	if g, e := r.Len(), 0; g != e {
		t.Fatal(g, e)
	}

	if ok := r.Delete(1); ok {
		t.Fatal(ok)
	}
}

func TestBtreeDelete1(t *testing.T) {
	const N = 13000
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := treeNew()
		set := r.Set
		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		for _, k := range a {
			set(uint64(k), getDummyC(0))
		}

		for i, k := range a {
			ok := r.Delete(uint64(k))
			if !ok {
				t.Fatal(i, x, k)
			}

			if g, e := r.Len(), N-i-1; g != e {
				t.Fatal(i, g, e)
			}
		}
	}
}

func BenchmarkBtreeDelSeq1e3(b *testing.B) {
	benchmarkDelSeq(b, 1e3)
}

func BenchmarkBtreeDelSeq1e4(b *testing.B) {
	benchmarkDelSeq(b, 1e4)
}

func BenchmarkBtreeDelSeq1e5(b *testing.B) {
	benchmarkDelSeq(b, 1e5)
}

func BenchmarkBtreeDelSeq1e6(b *testing.B) {
	benchmarkDelSeq(b, 1e6)
}

func benchmarkDelSeq(b *testing.B, n int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := treeNew()
		for i := 0; i < n; i++ {
			r.Set(uint64(i), getDummyC(i))
		}
		debug.FreeOSMemory()
		b.StartTimer()
		for j := 0; j < n; j++ {
			r.Delete(uint64(j))
		}
	}
	b.StopTimer()
}

func BenchmarkBtreeDelRnd1e3(b *testing.B) {
	benchmarkDelRnd(b, 1e3)
}

func BenchmarkBtreeDelRnd1e4(b *testing.B) {
	benchmarkDelRnd(b, 1e4)
}

func BenchmarkBtreeDelRnd1e5(b *testing.B) {
	benchmarkDelRnd(b, 1e5)
}

func BenchmarkBtreeDelRnd1e6(b *testing.B) {
	benchmarkDelRnd(b, 1e6)
}

func benchmarkDelRnd(b *testing.B, n int) {
	rng := rng()
	a := make([]int, n)
	for i := range a {
		a[i] = rng.Next()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := treeNew()
		for _, v := range a {
			r.Set(uint64(v), getDummyC(0))
		}
		debug.FreeOSMemory()
		b.StartTimer()
		for _, v := range a {
			r.Delete(uint64(v))
		}
		b.StopTimer()
		r.Close()
	}
	b.StopTimer()
}

func TestBtreeDelete2(t *testing.T) {
	const N = 10000
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := treeNew()
		set := r.Set
		a := make([]int, N)
		rng := rng()
		for i := range a {
			a[i] = (rng.Next() ^ x) << 1
		}
		for _, k := range a {
			set(uint64(k), getDummyC(0))
		}

		for i, k := range a {
			ok := r.Delete(uint64(k))
			if !ok {
				t.Fatal(i, x, k)
			}

			if g, e := r.Len(), N-i-1; g != e {
				t.Fatal(i, g, e)
			}
		}
	}
}

func TestBtreeEnumeratorNext(t *testing.T) {
	// seeking within 3 keys: 10, 20, 30
	table := []struct {
		k    int
		hit  bool
		keys []int
	}{
		{5, false, []int{10, 20, 30}},
		{10, true, []int{10, 20, 30}},
		{15, false, []int{20, 30}},
		{20, true, []int{20, 30}},
		{25, false, []int{30}},
		{30, true, []int{30}},
		{35, false, []int{}},
	}

	for i, test := range table {
		up := test.keys
		r := treeNew()

		r.Set(uint64(10), getDummyC(100))
		r.Set(uint64(20), getDummyC(200))
		r.Set(uint64(30), getDummyC(300))

		for verChange := 0; verChange < 16; verChange++ {
			en, hit := r.Seek(uint64(test.k))

			if g, e := hit, test.hit; g != e {
				t.Fatal(i, g, e)
			}

			j := 0
			for {
				if verChange&(1<<uint(j)) != 0 {
					r.Set(uint64(20), getDummyC(200))
				}

				k, v, err := en.Next()
				if err != nil {
					if err != io.EOF {
						t.Fatal(i, err)
					}

					break
				}

				if j >= len(up) {
					t.Fatal(i, j, verChange)
				}

				if g, e := k, uint64(up[j]); g != e {
					t.Fatal(i, j, verChange, g, e)
				}

				if g, e := v, getDummyC(10*up[j]); g != e {
					t.Fatal(i, g, e)
				}

				j++

			}

			if g, e := j, len(up); g != e {
				t.Fatal(i, j, g, e)
			}
		}

	}
}

func TestBtreeEnumeratorPrev(t *testing.T) {
	// seeking within 3 keys: 10, 20, 30
	table := []struct {
		k    int
		hit  bool
		keys []int
	}{
		{5, false, []int{}},
		{10, true, []int{10}},
		{15, false, []int{10}},
		{20, true, []int{20, 10}},
		{25, false, []int{20, 10}},
		{30, true, []int{30, 20, 10}},
		{35, false, []int{30, 20, 10}},
	}

	for i, test := range table {
		dn := test.keys
		r := treeNew()

		r.Set(uint64(10), getDummyC(100))
		r.Set(uint64(20), getDummyC(200))
		r.Set(uint64(30), getDummyC(300))

		for verChange := 0; verChange < 16; verChange++ {
			en, hit := r.Seek(uint64(test.k))

			if g, e := hit, test.hit; g != e {
				t.Fatal(i, g, e)
			}

			j := 0
			for {
				if verChange&(1<<uint(j)) != 0 {
					r.Set(uint64(20), getDummyC(200))
				}

				k, v, err := en.Prev()
				if err != nil {
					if err != io.EOF {
						t.Fatal(i, err)
					}

					break
				}

				if j >= len(dn) {
					t.Fatal(i, j, verChange)
				}

				if g, e := k, uint64(dn[j]); g != e {
					t.Fatal(i, j, verChange, g, e)
				}

				if g, e := v, getDummyC(10*dn[j]); g != e {
					t.Fatal(i, g, e)
				}

				j++

			}

			if g, e := j, len(dn); g != e {
				t.Fatal(i, j, g, e)
			}
		}

	}
}

func TestBtreeEnumeratorPrevSanity(t *testing.T) {
	// seeking within 3 keys: 10, 20, 30
	table := []struct {
		k        int
		hit      bool
		keyOut   uint64
		valueOut *Container
		errorOut error
	}{
		{10, true, 10, getDummyC(100), nil},
		{20, true, 20, getDummyC(200), nil},
		{30, true, 30, getDummyC(300), nil},
		{35, false, 30, getDummyC(300), nil},
		{25, false, 20, getDummyC(200), nil},
		{15, false, 10, getDummyC(100), nil},
		{5, false, 0, nil, io.EOF},
	}

	for i, test := range table {
		r := treeNew()

		r.Set(uint64(10), getDummyC(100))
		r.Set(uint64(20), getDummyC(200))
		r.Set(uint64(30), getDummyC(300))

		en, hit := r.Seek(uint64(test.k))

		if g, e := hit, test.hit; g != e {
			t.Fatal(i, g, e)
		}

		k, v, err := en.Prev()

		if g, e := err, test.errorOut; g != e {
			t.Fatal(i, g, e)
		}
		if g, e := k, test.keyOut; g != e {
			t.Fatal(i, g, e)
		}
		if g, e := v, test.valueOut; g != e {
			t.Fatal(i, g, e)
		}
	}
}

// TestBtreeEnumeratorEveryRegression is a regression test for a "use-after-free" bug.
// Previously, deleting a container would cause some values to be skipped (and sometimes trigger a race condition).
func TestBtreeEnumeratorEveryRegression(t *testing.T) {
	r := treeNew()

	r.Set(uint64(10), getDummyC(100))
	r.Set(uint64(20), getDummyC(200))
	r.Set(uint64(30), getDummyC(300))

	e, _ := r.Seek(0)
	expect := []uint64{10, 20, 30}
	var found []uint64
	_ = e.Every(func(key uint64, oldV *Container, exists bool) (*Container, bool) {
		found = append(found, key)
		return nil, true
	})

	if !reflect.DeepEqual(expect, found) { // Before the fix, this skipped the 20.
		t.Errorf("had %v in bitmap; only found %v", expect, found)
	}
}

func BenchmarkBtreeSeekSeq1e3(b *testing.B) {
	benchmarkSeekSeq(b, 1e3)
}

func BenchmarkBtreeSeekSeq1e4(b *testing.B) {
	benchmarkSeekSeq(b, 1e4)
}

func BenchmarkBtreeSeekSeq1e5(b *testing.B) {
	benchmarkSeekSeq(b, 1e5)
}

func BenchmarkBtreeSeekSeq1e6(b *testing.B) {
	benchmarkSeekSeq(b, 1e6)
}

func benchmarkSeekSeq(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		t := treeNew()
		for j := 0; j < n; j++ {
			t.Set(uint64(j), getDummyC(0))
		}
		debug.FreeOSMemory()
		b.StartTimer()
		for j := 0; j < n; j++ {
			e, _ := t.Seek(uint64(j))
			e.Close()
		}
		b.StopTimer()
		t.Close()
	}
	b.StopTimer()
}

func BenchmarkBtreeSeekRnd1e3(b *testing.B) {
	benchmarkSeekRnd(b, 1e3)
}

func BenchmarkBtreeSeekRnd1e4(b *testing.B) {
	benchmarkSeekRnd(b, 1e4)
}

func BenchmarkBtreeSeekRnd1e5(b *testing.B) {
	benchmarkSeekRnd(b, 1e5)
}

func BenchmarkBtreeSeekRnd1e6(b *testing.B) {
	benchmarkSeekRnd(b, 1e6)
}

func benchmarkSeekRnd(b *testing.B, n int) {
	r := treeNew()
	rng := rng()
	a := make([]int, n)
	for i := range a {
		a[i] = rng.Next()
	}
	for _, v := range a {
		r.Set(uint64(v), getDummyC(0))
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range a {
			e, _ := r.Seek(uint64(v))
			e.Close()
		}
	}
	b.StopTimer()
	r.Close()
}

func BenchmarkBtreeNext1e3(b *testing.B) {
	benchmarkNext(b, 1e3)
}

func BenchmarkBtreeNext1e4(b *testing.B) {
	benchmarkNext(b, 1e4)
}

func BenchmarkBtreeNext1e5(b *testing.B) {
	benchmarkNext(b, 1e5)
}

func BenchmarkBtreeNext1e6(b *testing.B) {
	benchmarkNext(b, 1e6)
}

func benchmarkNext(b *testing.B, n int) {
	t := treeNew()
	for i := 0; i < n; i++ {
		t.Set(uint64(i), getDummyC(0))
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		en, err := t.SeekFirst()
		if err != nil {
			b.Fatal(err)
		}

		m := 0
		for {
			if _, _, err = en.Next(); err != nil {
				break
			}
			m++
		}
		if m != n {
			b.Fatal(m)
		}
	}
	b.StopTimer()
	t.Close()
}

func BenchmarkBtreePrev1e3(b *testing.B) {
	benchmarkPrev(b, 1e3)
}

func BenchmarkBtreePrev1e4(b *testing.B) {
	benchmarkPrev(b, 1e4)
}

func BenchmarkBtreePrev1e5(b *testing.B) {
	benchmarkPrev(b, 1e5)
}

func BenchmarkBtreePrev1e6(b *testing.B) {
	benchmarkPrev(b, 1e6)
}

func benchmarkPrev(b *testing.B, n int) {
	t := treeNew()
	for i := 0; i < n; i++ {
		t.Set(uint64(i), getDummyC(0))
	}
	debug.FreeOSMemory()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		en, err := t.SeekLast()
		if err != nil {
			b.Fatal(err)
		}

		m := 0
		for {
			if _, _, err = en.Prev(); err != nil {
				break
			}
			m++
		}
		if m != n {
			b.Fatal(m)
		}
	}
}

func TestBtreeSeekFirst0(t *testing.T) {
	b := treeNew()
	_, err := b.SeekFirst()
	if g, e := err, io.EOF; g != e {
		t.Fatal(g, e)
	}
}

func TestBtreeSeekFirst1(t *testing.T) {
	b := treeNew()
	b.Set(uint64(1), getDummyC(10))
	en, err := b.SeekFirst()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Next()
	if k != 1 || v != getDummyC(10) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestBtreeSeekFirst2(t *testing.T) {
	b := treeNew()
	b.Set(uint64(1), getDummyC(10))
	b.Set(uint64(2), getDummyC(20))
	en, err := b.SeekFirst()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Next()
	if k != 1 || v != getDummyC(10) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if k != 2 || v != getDummyC(20) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestBtreeSeekFirst3(t *testing.T) {
	b := treeNew()
	b.Set(uint64(2), getDummyC(20))
	b.Set(uint64(3), getDummyC(30))
	b.Set(uint64(1), getDummyC(10))
	en, err := b.SeekFirst()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Next()
	if k != 1 || v != getDummyC(10) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if k != 2 || v != getDummyC(20) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if k != 3 || v != getDummyC(30) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestBtreeSeekLast0(t *testing.T) {
	b := treeNew()
	_, err := b.SeekLast()
	if g, e := err, io.EOF; g != e {
		t.Fatal(g, e)
	}
}

func TestBtreeSeekLast1(t *testing.T) {
	b := treeNew()
	b.Set(uint64(1), getDummyC(10))
	en, err := b.SeekLast()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Prev()
	if k != 1 || v != getDummyC(10) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestBtreeSeekLast2(t *testing.T) {
	b := treeNew()
	b.Set(uint64(1), getDummyC(10))
	b.Set(uint64(2), getDummyC(20))
	en, err := b.SeekLast()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Prev()
	if k != 2 || v != getDummyC(20) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if k != 1 || v != getDummyC(10) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestBtreeSeekLast3(t *testing.T) {
	b := treeNew()
	b.Set(uint64(2), getDummyC(20))
	b.Set(uint64(3), getDummyC(30))
	b.Set(uint64(1), getDummyC(10))
	en, err := b.SeekLast()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Prev()
	if k != 3 || v != getDummyC(30) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if k != 2 || v != getDummyC(20) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if k != 1 || v != getDummyC(10) || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestBtreePut(t *testing.T) {
	tab := []struct {
		pre    []int // even index: K, odd index: V
		newK   int   // Put(newK, ...
		oldV   int   // Put()->oldV
		exists bool  // upd(exists)
		write  bool  // upd()->write
		post   []int // even index: K, odd index: V
	}{
		// 0
		{
			[]int{},
			1, 0, false, false,
			[]int{},
		},
		{
			[]int{},
			1, 0, false, true,
			[]int{1, -1},
		},
		{
			[]int{1, 10},
			0, 0, false, false,
			[]int{1, 10},
		},
		{
			[]int{1, 10},
			0, 0, false, true,
			[]int{0, -1, 1, 10},
		},
		{
			[]int{1, 10},
			1, 10, true, false,
			[]int{1, 10},
		},

		// 5
		{
			[]int{1, 10},
			1, 10, true, true,
			[]int{1, -1},
		},
		{
			[]int{1, 10},
			2, 0, false, false,
			[]int{1, 10},
		},
		{
			[]int{1, 10},
			2, 0, false, true,
			[]int{1, 10, 2, -1},
		},
	}

	for iTest, test := range tab {
		tr := treeNew()
		for i := 0; i < len(test.pre); i += 2 {
			k, v := test.pre[i], test.pre[i+1]
			tr.Set(uint64(k), getDummyC(v))
		}

		oldV, written := tr.Put(uint64(test.newK), func(old *Container, exists bool) (newV *Container, write bool) {
			if g, e := exists, test.exists; g != e {
				t.Fatal(iTest, g, e)
			}

			if exists {
				if g, e := old, getDummyC(test.oldV); g != e {
					t.Fatal(iTest, g, e)
				}
			}
			return getDummyC(99), test.write
		})
		if test.exists {
			if g, e := oldV, getDummyC(test.oldV); g != e {
				t.Fatal(iTest, g, e)
			}
		}

		if g, e := written, test.write; g != e {
			t.Fatal(iTest, g, e)
		}

		n := len(test.post)
		en, err := tr.SeekFirst()
		if err != nil {
			if n == 0 && err == io.EOF {
				continue
			}

			t.Fatal(iTest, err)
		}

		for i := 0; i < len(test.post); i += 2 {
			k, v, err := en.Next()
			if err != nil {
				t.Fatal(iTest, err)
			}

			if g, e := k, uint64(test.post[i]); g != e {
				t.Fatal(iTest, g, e)
			}

			var e *Container
			if test.post[i+1] != -1 {
				e = getDummyC(test.post[i+1])
			} else {
				e = getDummyC(99)
			}
			if g := v; g != e {
				t.Fatal(iTest, g, e)
			}
		}

		_, _, err = en.Next()
		if g, e := err, io.EOF; g != e {
			t.Fatal(iTest, g, e)
		}
	}
}

func TestBtreeSeek(t *testing.T) {
	const N = 1 << 11
	tr := treeNew()
	for i := 0; i < N; i++ {
		k := 2*i + 1
		tr.Set(uint64(k), getDummyC(1))
	}
	for i := 0; i < N; i++ {
		k := 2 * i
		e, ok := tr.Seek(uint64(k))
		if ok {
			t.Fatal(k)
		}

		for j := i; j < N; j++ {
			k2, _, err := e.Next()
			if err != nil {
				t.Fatal(k, err)
			}

			if g, e := k2, uint64(2*j+1); g != e {
				t.Fatal(j, g, e)
			}
		}

		_, _, err := e.Next()
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
	}
}

func TestBtreePR4(t *testing.T) {
	tr := treeNew()
	for i := 0; i < 2*kd+1; i++ {
		k := 1000 * i
		tr.Set(uint64(k), getDummyC(1))
	}
	tr.Delete(1000 * kd)
	for i := 0; i < kd; i++ {
		tr.Set(uint64(1000*(kd+1)-1-i), getDummyC(1))
	}
	k := 1000*(kd+1) - 1 - kd
	tr.Set(uint64(k), getDummyC(1))
	if _, ok := tr.Get(uint64(k)); !ok {
		t.Fatalf("key lost: %v", k)
	}
}
