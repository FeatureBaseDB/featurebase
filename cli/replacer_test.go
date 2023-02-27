package cli

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReplacer(t *testing.T) {
	t.Run("general replace function", func(t *testing.T) {

		m := map[string]string{
			"v1": "newVone",
			"v2": "newVtwo",
		}

		tests := []struct {
			s   string
			m   map[string]string
			exp string
		}{
			{
				// no variables present
				s:   "foo",
				m:   m,
				exp: "foo",
			},
			{
				// variable prefix, but not in map
				s:   ":foo",
				m:   m,
				exp: ":foo",
			},
			{
				// variable name match, but missing prefix
				s:   "v1",
				m:   m,
				exp: "v1",
			},
			{
				// variable name match
				s:   ":v1",
				m:   m,
				exp: "newVone",
			},
			{
				// two variables, the same, no space
				s:   ":v1:v1",
				m:   m,
				exp: "newVonenewVone",
			},
			{
				// two variables, different, no space
				s:   ":v1:v2",
				m:   m,
				exp: "newVonenewVtwo",
			},
			{
				// two variables, different, spaces
				s:   ":v1  :v2",
				m:   m,
				exp: "newVone  newVtwo",
			},
			{
				// one variable, one non-variable, no space
				s:   ":v1:foo",
				m:   m,
				exp: "newVone:foo",
			},
			{
				// one non-variable, one variable, no space
				s:   "foo:v1",
				m:   m,
				exp: "foonewVone",
			},
			{
				// two variables, different, comma
				s:   ":v1, :v2",
				m:   m,
				exp: "newVone, newVtwo",
			},
			{
				// single quotes
				s:   ":'v1'",
				m:   m,
				exp: "'newVone'",
			},
			{
				// double quotes
				s:   `:"v2"`,
				m:   m,
				exp: `"newVtwo"`,
			},
			{
				// more quotes
				s:   `start :v1,:'two', :"v2" ::four :: `,
				m:   m,
				exp: `start newVone,:'two', "newVtwo" ::four :: `,
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				replacer := newReplacer(test.m)
				assert.Equal(t, test.exp, replacer.replace(test.s))
			})
		}
	})
}
