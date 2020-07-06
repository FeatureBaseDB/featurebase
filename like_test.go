package pilosa

import (
	"reflect"
	"testing"
)

func TestPlanLike(t *testing.T) {
	t.Parallel()

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
					str:  "x",
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
					str:  "x",
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
					kind: filterStepSkipThrough,
					str:  "x",
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
					str:  "x",
				},
				{
					kind: filterStepSkipThrough,
					str:  "y",
				},
			},
			match:    []string{"xy", "xzy", "xyzzy"},
			nonmatch: []string{"plugh", ".xy.", ".x.y", "x.y."},
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
			nonmatch: []string{"ab", "á", "h̷"},
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
					str:  "x",
				},
				{
					kind: filterStepSkipN,
					n:    1,
				},
				{
					kind: filterStepPrefix,
					str:  "y",
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
					if !matchLike(m, c.plan...) {
						t.Errorf("key %q was not matched", m)
					}
				}
				for _, nm := range c.nonmatch {
					if matchLike(nm, c.plan...) {
						t.Errorf("key %q was matched", nm)
					}
				}
			})
		}
	})
}
