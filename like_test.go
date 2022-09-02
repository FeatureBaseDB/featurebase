// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"reflect"
	"testing"
)

func TestPlanLike(t *testing.T) {
	cases := []struct {
		name            string
		like            string
		plan            []filterStep
		match, nonmatch []string
	}{
		{
			name:     "Empty",
			like:     "",
			plan:     []filterStep{},
			match:    []string{""},
			nonmatch: []string{"a", " "},
		},
		{
			name: "Exact",
			like: "x",
			plan: []filterStep{
				{
					kind: filterStepPrefix,
					str:  []byte("x"),
				},
			},
			match:    []string{"x"},
			nonmatch: []string{"", "y", "z", "xy", "yx"},
		},
		{
			name: "Anything",
			like: "%",
			plan: []filterStep{
				{
					kind: filterStepMinLength,
					n:    0,
				},
			},
			match: []string{"", "a", "b", "ab"},
		},
		{
			name: "Prefix",
			like: "x%",
			plan: []filterStep{
				{
					kind: filterStepPrefix,
					str:  []byte("x"),
				},
				{
					kind: filterStepMinLength,
					n:    0,
				},
			},
			match:    []string{"xy", "xyz", "xyzzy"},
			nonmatch: []string{"plugh", "yx", ""},
		},
		{
			name: "Suffix",
			like: "%x",
			plan: []filterStep{
				{
					kind: filterStepSuffix,
					str:  []byte("x"),
				},
			},
			match:    []string{"x", "xx", "ax"},
			nonmatch: []string{"", "a", "x^"},
		},
		{
			name: "Sandwich",
			like: "x%y",
			plan: []filterStep{
				{
					kind: filterStepPrefix,
					str:  []byte("x"),
				},
				{
					kind: filterStepSuffix,
					str:  []byte("y"),
				},
			},
			match:    []string{"xy", "xzy", "xyzzy"},
			nonmatch: []string{"plugh", ".xy.", ".x.y", "x.y."},
		},
		{
			name: "DoubleDeckerSandwich",
			like: "x%y%z",
			plan: []filterStep{
				{
					kind: filterStepPrefix,
					str:  []byte("x"),
				},
				{
					kind: filterStepSkipThrough,
					str:  []byte("y"),
				},
				{
					kind: filterStepSuffix,
					str:  []byte("z"),
				},
			},
			match:    []string{"xyz", "xzyzz", "x.y.z", "x.y.y..z"},
			nonmatch: []string{"plugh", ".xyz.", ".x.y.z", "x.y.z."},
		},
		{
			name: "Skips",
			like: "a_b_%_c_%_%_d",
			plan: []filterStep{
				{
					kind: filterStepPrefix,
					str:  []byte("a"),
				},
				{
					kind: filterStepSkipN,
					n:    1,
				},
				{
					kind: filterStepPrefix,
					str:  []byte("b"),
				},
				{
					kind: filterStepSkipThrough,
					str:  []byte("c"),
					n:    2,
				},
				{
					kind: filterStepSuffix,
					str:  []byte("d"),
					n:    3,
				},
			},
			match:    []string{"a1b234c5678d"},
			nonmatch: []string{"abcd", "a1b2345678d", "a1b2c5678d"},
		},
		{
			name: "SingleRune",
			like: "_",
			plan: []filterStep{
				{
					kind: filterStepSkipN,
					n:    1,
				},
			},
			match:    []string{"a", "á", "☺"},
			nonmatch: []string{"ab", "á", "h̷", ""},
		},
		{
			name: "DoubleRune",
			like: "__",
			plan: []filterStep{
				{
					kind: filterStepSkipN,
					n:    2,
				},
			},
			match:    []string{"ab", "á", "h̷"},
			nonmatch: []string{"a", "á", "☺", "abc"},
		},
		{
			name: "MiddleBlank",
			like: "x_y",
			plan: []filterStep{
				{
					kind: filterStepPrefix,
					str:  []byte("x"),
				},
				{
					kind: filterStepSkipN,
					n:    1,
				},
				{
					kind: filterStepPrefix,
					str:  []byte("y"),
				},
			},
			match:    []string{"x.y", "xay", "x y", "x⊕y"},
			nonmatch: []string{"x++y", "", "a"},
		},
		{
			name: "MinLength",
			like: "_%_",
			plan: []filterStep{
				{
					kind: filterStepMinLength,
					n:    2,
				},
			},
			match:    []string{"ab", "á", "abc", "pilosa"},
			nonmatch: []string{"h", "á", ".", "☺"},
		},
	}
	t.Run("Plan", func(t *testing.T) {
		t.Parallel()

		for _, c := range cases {
			c := c
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()

				plan := planLike(c.like)
				if !reflect.DeepEqual(plan, c.plan) {
					t.Errorf("incorrect plan: %v", plan)
				}
			})
		}
	})
	t.Run("Match", func(t *testing.T) {
		t.Parallel()

		for _, c := range cases {
			c := c
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()

				for _, m := range c.match {
					if !matchLike([]byte(m), c.plan...) {
						t.Errorf("key %q was not matched", m)
					}
				}
				for _, nm := range c.nonmatch {
					if matchLike([]byte(nm), c.plan...) {
						t.Errorf("key %q was matched", nm)
					}
				}
			})
		}
	})
}
