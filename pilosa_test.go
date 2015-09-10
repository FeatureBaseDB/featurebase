package pilosa_test

import (
	"fmt"
	"testing"

	"github.com/umbel/pilosa"
)

// Ensure id can be parsed from string.
func TestSUUID_Small(t *testing.T) {
	if v := pilosa.ParseSUUID("1"); v != 1 {
		t.Fatalf("unexpected SUUID: %v", v)
	}
}

// Ensure generated IDs are unique.
func TestSUUID_Unique(t *testing.T) {
	a, b := pilosa.NewSUUID(), pilosa.NewSUUID()
	if a == b {
		t.Fatalf("ids should be unique: %v != %v", a, b)
	}
}

// Ensure ids can be converted to and from hex.
func TestSUUID_Hex(t *testing.T) {
	a := pilosa.NewSUUID()
	b := pilosa.ParseSUUID(a.String())
	if a != b {
		t.Fatalf("ids not equal: %v != %v", a, b)
	}
}

// Ensure ids can be generated in sequence.
func TestSUUID_Multiple(t *testing.T) {
	for i := 0; i < 10; i++ {
		println(pilosa.NewSUUID().String())
	}
}

// Ensure a random GUID can be converted to a string.
func TestGUID_String(t *testing.T) {
	fmt.Println(pilosa.NewGUID().String())
}

func BenchmarkSUUID(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		pilosa.NewSUUID()
	}
}
