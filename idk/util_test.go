package idk

import (
	"fmt"
	"testing"
)

func TestScaledStringToInt(t *testing.T) {
	cases := []struct {
		scale    int64
		num      string
		expected int64
		err      error
	}{
		{
			scale:    2,
			num:      "80.74",
			expected: 8074,
		},
		{
			scale:    2,
			num:      "8.7",
			expected: 870,
		},
		{
			scale:    2,
			num:      "8.7498",
			expected: 874,
		},
		{
			scale:    2,
			num:      "1.235",
			expected: 123,
		},
		{
			scale:    2,
			num:      "00001.235",
			expected: 123,
		},
		{
			scale:    2,
			num:      "-00001.235000000",
			expected: -123,
		},
		{
			scale:    2,
			num:      "+00001.235000000",
			expected: 123,
		},
		{
			scale:    2,
			num:      "01.235000.000",
			expected: 0,
			err:      fmt.Errorf("invalid decimal string: %v", "01.235000.000"),
		},
	}

	for _, test := range cases {
		t.Run(test.num, func(t *testing.T) {
			res, err := scaledStringToInt(test.scale, test.num)
			if res != test.expected {
				t.Errorf("expected %v, got %v", test.expected, res)
			}
			if (test.err == nil && err != nil) || (test.err != nil && err == nil) {
				t.Fatalf("expected %v, but got %v", test.err, err)
			} else if test.err != nil && err != nil && test.err.Error() != err.Error() {
				t.Fatalf("expected %v, but got %v", test.err, err)
			}
		})
	}
}
