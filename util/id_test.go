package util

import (
	"fmt"
	"testing"
)

// Ensure id can be parsed from string.
func TestId_Small(t *testing.T) {
	if v := Hex_to_SUUID("1"); v != 1 {
		t.Fatalf("unexpected SUUID: %v", v)
	}
}

// Ensure generated IDs are unique.
func TestId_Unique(t *testing.T) {
	a, b := Id(), Id()
	if a == b {
		t.Fatalf("ids should be unique: %v != %v", a, b)
	}
}

// Ensure ids can be converted to and from hex.
func TestId_Hex(t *testing.T) {
	a := Id()
	b := Hex_to_SUUID(SUUID_to_Hex(a))
	if a != b {
		t.Fatalf("ids not equal: %v != %v", a, b)
	}
}

// Ensure ids can be generated in sequence.
func TestId_Multiple(t *testing.T) {
	for i := 0; i < 10; i++ {
		println(SUUID_to_Hex(Id()))
	}
}

// Ensure a random UUID can be converted to a string.
func TestRandomUUID_String(t *testing.T) {
	fmt.Println(RandomUUID().String())
}

func BenchmarkId(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		Id()
	}
}
