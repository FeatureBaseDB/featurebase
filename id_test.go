package pilosa_test

import (
	"fmt"
	"testing"

	"github.com/umbel/pilosa"
)

// Ensure id can be parsed from string.
func TestId_Small(t *testing.T) {
	if v := pilosa.Hex_to_SUUID("1"); v != 1 {
		t.Fatalf("unexpected SUUID: %v", v)
	}
}

// Ensure generated IDs are unique.
func TestId_Unique(t *testing.T) {
	a, b := pilosa.Id(), pilosa.Id()
	if a == b {
		t.Fatalf("ids should be unique: %v != %v", a, b)
	}
}

// Ensure ids can be converted to and from hex.
func TestId_Hex(t *testing.T) {
	a := pilosa.Id()
	b := pilosa.Hex_to_SUUID(pilosa.SUUID_to_Hex(a))
	if a != b {
		t.Fatalf("ids not equal: %v != %v", a, b)
	}
}

// Ensure ids can be generated in sequence.
func TestId_Multiple(t *testing.T) {
	for i := 0; i < 10; i++ {
		println(pilosa.SUUID_to_Hex(pilosa.Id()))
	}
}

// Ensure a random UUID can be converted to a string.
func TestRandomUUID_String(t *testing.T) {
	fmt.Println(pilosa.RandomUUID().String())
}

func BenchmarkId(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		pilosa.Id()
	}
}
