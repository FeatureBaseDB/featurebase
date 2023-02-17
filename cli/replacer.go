package cli

import "strings"

// replacer can replace parts of a string based on some rules and the provided
// map[string]string. For example, the Command can replace strings with values
// in its `variables` map.
type replacer struct {
	m map[string]string
}

func newReplacer(m map[string]string) *replacer {
	return &replacer{
		m: m,
	}
}

// replace replaces all instances of the string pattern `:key` with the value at
// m[key].
func (r *replacer) replace(s string) string {
	designator := ":"
	words := strings.Split(s, " ")
	for _, word := range words {
		if len(word) == 0 {
			continue
		}
		// split the word on ":" in case there are multiple variables
		// back-to-back, like `:v1:v2`. Always ignore the first item in the
		// split list (ex: `foo:v1` should replace `:v1`, but leave `foo` as
		// is).
		keys := strings.Split(word, designator)
		for i, key := range keys {
			if i == 0 {
				continue
			}
			if v, ok := r.m[key]; ok {
				s = strings.Replace(s, designator+key, v, 1)
			}
		}
	}
	return s
}
