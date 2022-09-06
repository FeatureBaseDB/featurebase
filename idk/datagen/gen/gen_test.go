package gen_test

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/idk/datagen/gen"
)

func TestGen(t *testing.T) {
	g := gen.New()

	byteSlice := make([]byte, 6)

	// Test AlphaUpper
	g.AlphaUpper(byteSlice)
	if len(byteSlice) != 6 {
		t.Errorf("AlphaUpper modified byte slice length: %s", byteSlice)
	}
	for i, b := range byteSlice {
		if b < 65 || b > 90 {
			t.Errorf("non uppercase char at position %d: %d", i, b)
		}
	}

	// Test AlphaLower
	g.AlphaLower(byteSlice)
	if len(byteSlice) != 6 {
		t.Errorf("AlphaLower modified byte slice length: %s", byteSlice)
	}
	for i, b := range byteSlice {
		if b < 97 || b > 122 {
			t.Errorf("non lowercase char at position %d: %d", i, b)
		}
	}

	// Test StringSliceFromList
	var input []string
	list := []string{"a", "b", "c", "d", "e", "f", "g"}

	minLen, maxLen := 8, -1
	for i := 0; i < 100; i++ {
		randList := g.StringSliceFromList(input, list)
		if len(randList) < minLen {
			minLen = len(randList)
		}
		if len(randList) > maxLen {
			maxLen = len(randList)
		}
		for j, item := range randList {
			if len(item) != 1 || item[0] < 97 || item[0] > 103 {
				t.Errorf("Unexpected list item at %d: %s. List: %v", j, item, randList)
			}
		}
	}
	if minLen != 0 {
		t.Errorf("Unexpected minimum length of %d", minLen)
	}
	if maxLen != 7 {
		t.Errorf("Unexpected maximum length of %d", maxLen)
	}

	// TestStringFromListWeighted
	seen := make(map[string]int)
	for i := 0; i < 100; i++ {
		s := g.StringFromListWeighted([]string{"blah", "bleh", "blue"})
		seen[s]++
		if s != "blah" && s != "bleh" && s != "blue" {
			t.Errorf("Unexpected string %s", s)
		}
	}
	if len(seen) != 3 {
		t.Errorf("Should have seen 3 different strings but saw: %v", seen)
	}

	seen = make(map[string]int)
	for i := 0; i < 100; i++ {
		s := g.StringFromListWeighted([]string{"blah", "bleh", "blue", "bler"})
		seen[s]++
		if s != "blah" && s != "bleh" && s != "blue" && s != "bler" {
			t.Errorf("Unexpected string %s", s)
		}
	}
	if len(seen) != 4 {
		t.Errorf("Should have seen 4 different strings but saw: %v", seen)
	}

}
