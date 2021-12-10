package pql

// Token is a lexical token of the PQL language.
type Token int

const (
	// Special tokens
	ILLEGAL Token = iota

	ASSIGN // =
	EQ     // ==
	NEQ    // !=
	LT     // <
	LTE    // <=
	GT     // >
	GTE    // >=

	BETWEEN // ><  (this is like a <= x <= b)

	// not used in lexing/parsing, but so that the parser can signal
	// to the executor how to treat the arguments. We used to just add
	// 1 to the arguments if they were LT so the executor could assume
	// it was always <=, <=, but then we needed to support
	// floats/decimals and couldn't do that any more.
	BTWN_LT_LTE // a < x <= b
	BTWN_LTE_LT // a <= x < b
	BTWN_LT_LT  // a < x < b
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",

	ASSIGN:  "=",
	EQ:      "==",
	NEQ:     "!=",
	LT:      "<",
	LTE:     "<=",
	GT:      ">",
	GTE:     ">=",
	BETWEEN: "><",
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}
