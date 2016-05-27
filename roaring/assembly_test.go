package roaring

import "testing"

func TestBSFQ(t *testing.T) {
	result := BSFQ(2)
	if result != 1 {
		t.Fatalf("BSF INCORRECT: %d", result)
	}
}

func TestBSFQ_CompareGo(t *testing.T) {
	v := uint64(1)
	for i := 0; i < 64; i++ {
		if BSFQ(v) != trailingZeroN(v) {
			t.Fatalf("BSF INCORRECT: %d %d", BSFQ(v), trailingZeroN(v))
		}
		if v == 0 {
			v = 1
		} else {
			v *= 2
		}
	}
	/*
		if bsfq(0) != trailingZeroN(0) {
			fmt.Println(bsfq(0))
			t.Fatalf("BSF INCORRECT")
		}
	*/
}
func BenchmarkBSF(b *testing.B) {
	for i := 0; i < b.N; i++ {
		BSFQ(uint64(i))
	}
}

func BenchmarkTrailingZeroN(b *testing.B) {
	for i := 0; i < b.N; i++ {
		trailingZeroN(uint64(i))
	}
}

func BenchmarkPOPCNTQ(b *testing.B) {
	for i := 0; i < b.N; i++ {
		POPCNTQ(uint64(i))
	}
}

func BenchmarkPopcount(b *testing.B) {
	for i := 0; i < b.N; i++ {
		popcount(uint64(i))
	}
}
