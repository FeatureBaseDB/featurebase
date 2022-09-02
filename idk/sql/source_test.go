package sql

import (
	"reflect"
	"testing"
)

func TestSplitStringArray(t *testing.T) {
	s := Source{separator: ",."}
	inputString := "abc,def.ghjkl.op"
	expected := []string{"abc", "def", "ghjkl", "op"}
	result := s.splitStringArray(inputString)
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("splitStringArray did not split string array on separators properly. expected %v, got %v", expected, result)
	}
}

func TestSplitter(t *testing.T) {
	split := Splitter(",.x")
	if !split(',') {
		t.Fatal("split should split on ,")
	}
	if !split('.') {
		t.Fatal("split should split on .")
	}
	if !split('x') {
		t.Fatal("split should split on x")
	}
	if split('o') {
		t.Fatal("split should NOT split on o")
	}
}
