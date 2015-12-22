package pilosa

import "testing"

func BenchmarkPopcntAsm(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		popcntAsm(0xdeadbeef)
	}
}

func BenchmarkPopcntGo(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		popcntGo(0xdeadbeef)
	}
}

func getData() []uint64 {
	return []uint64{
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
	}
}

func BenchmarkPopcntSliceGo(b *testing.B) {
	d := getData()
	for n := 0; n < b.N; n++ {
		popcntSliceGo(d)
	}
}

func BenchmarkPopcntSliceAsm(b *testing.B) {
	d := getData()
	for n := 0; n < b.N; n++ {
		popcntSliceAsm(d)
	}
}

func BenchmarkPopcntSlice(b *testing.B) {
	d := getData()
	for n := 0; n < b.N; n++ {
		popcntSlice(d)
	}
}
