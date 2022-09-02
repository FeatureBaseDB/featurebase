package server

import (
	"fmt"
	"testing"
)

// unit tests for internal functions
func TestIsAllowed(t *testing.T) {
	cases := []struct {
		requested []string
		allowed   []string
		expected  bool
	}{
		{
			requested: []string{"a", "b", "c"},
			allowed:   []string{"a", "b", "c", "d", "e"},
			expected:  true,
		},
		{
			requested: []string{"a", "b", "c", "f"},
			allowed:   []string{"a", "b", "c", "d", "e"},
			expected:  false,
		},
		{
			requested: []string{"a", "b", "c"},
			allowed:   []string{},
			expected:  false,
		},
	}

	for i, test := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if res := isAllowed(test.requested, test.allowed); res != test.expected {
				t.Errorf("expected %v, got %v", test.expected, res)
			}
		})
	}

}
