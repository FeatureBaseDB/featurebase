package cli

import "strings"

// replacer is implemented by any type which can replace parts of a string based
// on some rules. For example, the Command can replace strings with values in
// its `variables` map.
type replacer interface {
	replace(s string) string
}

type nopReplacer struct{}

func newNopReplacer() *nopReplacer {
	return &nopReplacer{}
}

func (n *nopReplacer) replace(s string) string {
	return s
}

// replace is a general replacement function that can be used by implementations
// of the replacer interface. It replaces all instances of the string pattern
// `:key` with the value at m[key].
func replace(s string, m map[string]string) string {
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
			if v, ok := m[key]; ok {
				s = strings.Replace(s, designator+key, v, 1)
			}
		}
	}
	return s
}
