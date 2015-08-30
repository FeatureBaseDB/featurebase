package mem_test

import (
	"testing"

	"github.com/umbel/pilosa/index/storage/mem"
)

// Ensure a bitmap can be retrieved from storage.
func TestStorage_Fetch(t *testing.T) {
	s := mem.NewStorage()

	// Retrieving a non-existent bitmap should create a new one.
	b, _ := s.Fetch(1, "d", "f", 0)
	if b == nil {
		t.Fatal("expected bitmap")
	}

	// Retrieve the bitmap again.
	other, _ := s.Fetch(1, "d", "f", 0)
	if b != other {
		t.Fatal("expected same bitmap")
	}

	// Retrieving a different bitmap should return a different reference.
	b2, _ := s.Fetch(2, "d", "f", 0)
	if b == b2 {
		t.Fatal("expected new bitmap")
	}
}
