package cli

import (
	"strings"

	"github.com/benhoyt/goawk/lexer"
)

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
// m[key]. For example we want something like this:
//
// GIVEN: `start :one,:'two', :"three" ::four ::`
//
// with map
//
//	map[string]string{
//		"one":   "repl1",
//		"three": "repl3",
//	}
//
// WANT: `start repl1,:'two', "repl3" ::four ::`
func (r *replacer) replace(s string) string {
	// If no variables have been added to the map, there's no need to parse the
	// string for variable replacement.
	if len(r.m) == 0 {
		return s
	}

	line := []byte(s)
	lex := lexer.NewLexer(line)

	// finger contains the index into line at the start of non-variable text
	// that we want to include, as-is in the output.
	var finger int

	// sb builds the string which will be the final output.
	var sb strings.Builder
	for {
		pos, tok, _ := lex.Scan()

		switch tok {
		case lexer.COLON:
			// last is the last normal character position before the colon.
			last := pos.Column - 1

			// Get the next byte to see if the colon value is quoted, and if so,
			// whether its has single or double quotes.
			b := lex.PeekByte()

			// padding is the amount of padding we have to consider around the
			// variable name. If the variable is not quoted, it doesn't require
			// any padding. But if it has quotes, it needs 2 characters of
			// paddings to accomodate the quotes.
			padding := 0

			// quote holds the character to use to quote the final, replaced
			// output value. Because the lexer doesn't tell us how a certain
			// `string` token was quoted, we need to keep track of that here so
			// we can put them back.
			quote := ""
			switch b {
			case byte('\''): // single quote
				quote = `'`
				padding = 2
			case byte('"'): // double quote
				quote = `"`
				padding = 2
			}

			pos, tok, key := lex.Scan()
			switch tok {
			case lexer.NAME, lexer.STRING:
				// Write the normal text up to the variable replacement
				// position.
				sb.Write(line[finger:last])

				if v, ok := r.m[key]; ok {
					// Write replaced variable with the quotes it had.
					sb.WriteString(quote + v + quote)
				} else {
					// Since the variable was not found in the map, just write
					// back what was already there.
					sb.WriteString(":" + quote + key + quote)
				}

				// Reset finger to point to the next position after the
				// variable.
				finger = pos.Column + len(key) + padding - 1
			}
		case lexer.EOF:
			// Write the remainder of the string and return.
			sb.Write(line[finger:])
			return sb.String()
		}
	}
}
