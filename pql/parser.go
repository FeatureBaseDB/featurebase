package pql

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// TimeFormat is the go-style time format used to parse string dates.
const TimeFormat = "2006-01-02T15:04"

// Parser represents a parser for the PQL language.
type Parser struct {
	scanner *bufScanner
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{
		scanner: newBufScanner(r),
	}
}

// ParseString parses s into a query.
func ParseString(s string) (*Query, error) {
	return NewParser(strings.NewReader(s)).Parse()
}

// Parse parses the next node in the query.
func (p *Parser) Parse() (*Query, error) {
	fn, err := p.parseCall()
	if err != nil {
		return nil, err
	}
	return &Query{Root: fn}, nil
}

// parseCall parses the next function call.
func (p *Parser) parseCall() (Call, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != IDENT {
		return nil, &ParseError{Message: fmt.Sprintf("expected identifier, found: %s", lit), Pos: pos}
	}

	switch lit {
	case "Bitmap":
		return p.parseBitmapCall()
	case "Count":
		return p.parseCountCall()
	case "ClearBit":
		return p.parseClearBitCall()
	case "Difference":
		return p.parseDifferenceCall()
	case "Intersect":
		return p.parseIntersectCall()
	case "Profile":
		return p.parseProfileCall()
	case "Range":
		return p.parseRangeCall()
	case "SetBit":
		return p.parseSetBitCall()
	case "SetBitmapAttrs":
		return p.parseSetBitmapAttrsCall()
	case "SetProfileAttrs":
		return p.parseSetProfileAttrsCall()
	case "TopN":
		return p.parseTopNCall()
	case "Union":
		return p.parseUnionCall()
	default:
		return nil, &ParseError{Message: fmt.Sprintf("function not found: %s", lit), Pos: pos}
	}
}

// parseBitmapCall parses a Bitmap() function call.
func (p *Parser) parseBitmapCall() (*Bitmap, error) {
	c := &Bitmap{}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		switch arg.key {
		case 0, "id":
			if err := decodeUint64(arg.value, &c.ID); err != nil {
				return nil, parseErrorf(pos, "id: %s", err)
			}
		case 1, "frame":
			if err := decodeString(arg.value, &c.Frame); err != nil {
				return nil, parseErrorf(pos, "frame: %s", err)
			}
		default:
			return nil, parseErrorf(pos, "invalid Bitmap() arg: %v", arg.key)
		}
	}

	return c, nil
}

// parseClearBitCall parses a ClearBit() function call.
func (p *Parser) parseClearBitCall() (*ClearBit, error) {
	c := &ClearBit{}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		switch arg.key {
		case 0, "id":
			if err := decodeUint64(arg.value, &c.ID); err != nil {
				return nil, parseErrorf(pos, "id: %s", err)
			}
		case 1, "frame":
			if err := decodeString(arg.value, &c.Frame); err != nil {
				return nil, parseErrorf(pos, "frame: %s", err)
			}
		case 2, "profileID":
			if err := decodeUint64(arg.value, &c.ProfileID); err != nil {
				return nil, parseErrorf(pos, "profileID: %s", err)
			}
		default:
			return nil, parseErrorf(pos, "invalid ClearBit() arg: %v", arg.key)
		}
	}

	return c, nil
}

// parseCount parses a Count() function call.
func (p *Parser) parseCountCall() (*Count, error) {
	c := &Count{}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	} else if len(args) != 1 {
		return nil, parseErrorf(pos, "count requires one argument")
	}

	// Copy argument to AST.
	input, ok := args[0].value.(BitmapCall)
	if !ok {
		return nil, parseErrorf(pos, "invalid count arg: %s", args[0].value)
	}
	c.Input = input

	return c, nil
}

// parseDifference parses a Difference() function call.
func (p *Parser) parseDifferenceCall() (*Difference, error) {
	c := &Difference{}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		if v, ok := arg.value.(BitmapCall); ok {
			c.Inputs = append(c.Inputs, v)
		} else {
			return nil, parseErrorf(pos, "invalid Difference() arg: %v", arg.value)
		}
	}

	return c, nil
}

// parseIntersect parses a Intersect() function call.
func (p *Parser) parseIntersectCall() (*Intersect, error) {
	c := &Intersect{}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		if v, ok := arg.value.(BitmapCall); ok {
			c.Inputs = append(c.Inputs, v)
		} else {
			return nil, parseErrorf(pos, "invalid Intersect() arg: %v", arg.value)
		}
	}

	return c, nil
}

// parseProfileCall parses a Profile() function call.
func (p *Parser) parseProfileCall() (*Profile, error) {
	c := &Profile{}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		switch arg.key {
		case 0, "id":
			if err := decodeUint64(arg.value, &c.ID); err != nil {
				return nil, parseErrorf(pos, "id: %s", err)
			}
		default:
			return nil, parseErrorf(pos, "invalid Profile() arg: %v", arg.key)
		}
	}

	return c, nil
}

// parseRangeCall parses a Range() function call.
func (p *Parser) parseRangeCall() (*Range, error) {
	c := &Range{}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		switch arg.key {
		case 0, "id":
			if err := decodeUint64(arg.value, &c.ID); err != nil {
				return nil, parseErrorf(pos, "start: %s", err)
			}
		case 1, "frame":
			if err := decodeString(arg.value, &c.Frame); err != nil {
				return nil, parseErrorf(pos, "frame: %s", err)
			}
		case 2, "start":
			if err := decodeDate(arg.value, &c.StartTime); err != nil {
				return nil, parseErrorf(pos, "start: %s", err)
			}
		case 3, "end":
			if err := decodeDate(arg.value, &c.EndTime); err != nil {
				return nil, parseErrorf(pos, "end: %s", err)
			}
		default:
			return nil, parseErrorf(pos, "invalid Range() arg: %v", arg.key)
		}
	}

	return c, nil
}

// parseSetBitCall parses a SetBit() function call.
func (p *Parser) parseSetBitCall() (*SetBit, error) {
	c := &SetBit{}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		switch arg.key {
		case 0, "id":
			if err := decodeUint64(arg.value, &c.ID); err != nil {
				return nil, parseErrorf(pos, "id: %s", err)
			}
		case 1, "frame":
			if err := decodeString(arg.value, &c.Frame); err != nil {
				return nil, parseErrorf(pos, "frame: %s", err)
			}
		case 2, "profileID":
			if err := decodeUint64(arg.value, &c.ProfileID); err != nil {
				return nil, parseErrorf(pos, "profileID: %s", err)
			}
		default:
			return nil, parseErrorf(pos, "invalid SetBit() arg: %v", arg.key)
		}
	}

	return c, nil
}

// parseSetBitmapAttrsCall parses a SetBitmapAttrs() function call.
func (p *Parser) parseSetBitmapAttrsCall() (*SetBitmapAttrs, error) {
	c := &SetBitmapAttrs{
		Attrs: make(map[string]interface{}),
	}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		switch arg.key {
		case 0, "id":
			if err := decodeUint64(arg.value, &c.ID); err != nil {
				return nil, parseErrorf(pos, "id: %s", err)
			}
		case 1, "frame":
			if err := decodeString(arg.value, &c.Frame); err != nil {
				return nil, parseErrorf(pos, "frame: %s", err)
			}
		default:
			key, ok := arg.key.(string)
			if !ok {
				return nil, parseErrorf(pos, "invalid attr arg: %v", arg.key)
			}

			// Special handling for nil values.
			if arg.value == nil {
				c.Attrs[key] = nil
				continue
			}

			switch v := arg.value.(type) {
			case string, bool:
				c.Attrs[key] = v
			case uint64:
				c.Attrs[key] = int64(v)
			default:
				return nil, parseErrorf(pos, "invalid SetBitmapAttrs() arg: %v", arg.key)
			}
		}
	}

	return c, nil
}

// parseSetProfileAttrsCall parses a SetProfileAttrs() function call.
func (p *Parser) parseSetProfileAttrsCall() (*SetProfileAttrs, error) {
	c := &SetProfileAttrs{
		Attrs: make(map[string]interface{}),
	}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		switch arg.key {
		case 0, "id":
			if err := decodeUint64(arg.value, &c.ID); err != nil {
				return nil, parseErrorf(pos, "id: %s", err)
			}
		default:
			key, ok := arg.key.(string)
			if !ok {
				return nil, parseErrorf(pos, "invalid attr arg: %v", arg.key)
			}

			// Special handling for nil values.
			if arg.value == nil {
				c.Attrs[key] = nil
				continue
			}

			switch v := arg.value.(type) {
			case string, bool:
				c.Attrs[key] = v
			case uint64:
				c.Attrs[key] = int64(v)
			default:
				return nil, parseErrorf(pos, "invalid SetProfileAttrs() arg: %v", arg.key)
			}
		}
	}

	return c, nil
}

// parseTopNCall parses a TopN() function call.
func (p *Parser) parseTopNCall() (*TopN, error) {
	c := &TopN{}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		if v, ok := arg.value.(BitmapCall); ok {
			c.Src = v
			continue
		}
		if v, ok := arg.value.([]interface{}); ok {
			c.Filters = v
			continue
		}

		switch arg.key {
		case 0, "frame":
			if err := decodeString(arg.value, &c.Frame); err != nil {
				return nil, parseErrorf(pos, "frame: %s", err)
			}
		case 1, "n":
			if err := decodeInt(arg.value, &c.N); err != nil {
				return nil, parseErrorf(pos, "n: %s", err)
			}
		case 2, "field":
			if err := decodeString(arg.value, &c.Field); err != nil {
				return nil, parseErrorf(pos, "n: %s", err)
			}
		default:
			return nil, parseErrorf(pos, "invalid TopN() arg: %v", arg.key)
		}
	}

	return c, nil
}

// parseUnion parses a Union() function call.
func (p *Parser) parseUnionCall() (*Union, error) {
	c := &Union{}
	pos := p.pos()

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}

	// Copy arguments to AST.
	for _, arg := range args {
		if v, ok := arg.value.(BitmapCall); ok {
			c.Inputs = append(c.Inputs, v)
		} else {
			return nil, parseErrorf(pos, "invalid Union() arg: %v", arg.value)
		}
	}

	return c, nil
}

// parseArgs arguments to a function call.
func (p *Parser) parseArgs() ([]arg, error) {
	var i int
	var args []arg
	for {
		// Parse next argument.
		arg, err := p.parseArg()
		if err != nil {
			return nil, err
		}

		// If it's a primitive type without a key then index it.
		if arg.key == nil {
			switch arg.value.(type) {
			case uint64, string:
				arg.key = i
				i++
			}
		}

		// Append argument to list.
		args = append(args, arg)

		// If next token is a closing parenthesis, then exit.
		// Otherwise expect a comma.
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok == RPAREN {
			break
		} else if tok != COMMA {
			return nil, parseErrorf(pos, "expected COMMA, found %q", lit)
		}
	}

	return args, nil
}

// parseArg parses a single argument to a function call.
func (p *Parser) parseArg() (arg, error) {
	var key, value interface{}

	// Read identifier and check if there's a following "=" or "(".
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case IDENT:
		// If a left paren immediately follows then it's a function call.
		if tok, _, _ := p.scan(); tok == LPAREN {
			p.unscan(2)
			c, err := p.parseCall()
			if err != nil {
				return arg{}, err
			}
			return arg{value: c}, nil
		}

		// If it's not a left paren, rescan ignoring whitespace and look for "=",
		p.unscan(1)
		if tok, _, _ := p.scanIgnoreWhitespace(); tok == EQ {
			key = lit // keyed arg
		} else {
			p.unscan(1)
		}
	default:
		p.unscan(1)
	}

	// Read value token.
	tok, pos, lit = p.scanIgnoreWhitespace()
	switch tok {
	case IDENT:
		if lit == "true" {
			value = true
		} else if lit == "false" {
			value = false
		} else if lit == "null" {
			value = nil
		} else {
			value = lit
		}
	case STRING:
		value = lit
	case NUMBER:
		v, err := strconv.ParseUint(lit, 10, 64)
		if err != nil {
			return arg{}, err
		}
		value = v
	case LBRACK:
		v, err := p.parseList()
		if err != nil {
			return arg{}, err
		}
		value = v
	default:
		return arg{}, parseErrorf(pos, "invalid value: %q", lit)
	}

	return arg{key: key, value: value}, nil
}

// parseListArg parses a list of primitives. This is used by the TopN() filters.
func (p *Parser) parseList() ([]interface{}, error) {
	var values []interface{}
	for {
		// Read next value.
		tok, pos, lit := p.scanIgnoreWhitespace()
		switch tok {
		case IDENT:
			if lit == "true" {
				values = append(values, true)
			} else if lit == "false" {
				values = append(values, false)
			} else {
				values = append(values, lit)
			}
		case STRING:
			values = append(values, lit)
		case NUMBER:
			v, err := strconv.ParseUint(lit, 10, 64)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		default:
			return nil, parseErrorf(pos, "invalid list value: %q", lit)
		}

		// Expect a comma or closing bracket next.
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok == RBRACK {
			break
		} else if tok != COMMA {
			return nil, parseErrorf(pos, "expected COMMA, found %q", lit)
		}
	}
	return values, nil
}

// scan returns the next token from the scanner.
func (p *Parser) scan() (tok Token, pos Pos, lit string) { return p.scanner.Scan() }

// scanIgnoreWhitespace returns the next non-whitespace token from the scanner.
func (p *Parser) scanIgnoreWhitespace() (tok Token, pos Pos, lit string) {
	tok, pos, lit = p.scan()
	if tok == WS {
		tok, pos, lit = p.scan()
	}
	return
}

// unscan returns the last n tokens back to the scanner.
func (p *Parser) unscan(n int) {
	for i := 0; i < n; i++ {
		p.scanner.unscan()
	}
}

// expect returns an error if the next token is not exp.
func (p *Parser) expect(exp Token) error {
	if tok, pos, lit := p.scan(); tok != exp {
		return parseErrorf(pos, "expected %s, found %q", exp.String(), lit)
	}
	return nil
}

// expectIgnoreWhitespace returns an error if the next non-whitespace token is not exp.
func (p *Parser) expectIgnoreWhitespace(exp Token) error {
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != exp {
		return parseErrorf(pos, "expected %s, found %q", exp.String(), lit)
	}
	return nil
}

// pos returns the current position.
func (p *Parser) pos() Pos { return p.scanner.pos() }

// arg represents an call argument.
// The key can be the index or the string key.
// The value can be a uint64, []uint64, string, Call, or Calls.
type arg struct {
	key   interface{}
	value interface{}
}

// ParseError represents an error that occurred while parsing a PQL query.
type ParseError struct {
	Message string
	Pos     Pos
}

// Error returns a string representation of e.
func (e *ParseError) Error() string {
	return fmt.Sprintf("%s occurred at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
}

// parseErrorf returns a formatted parse error.
func parseErrorf(pos Pos, format string, args ...interface{}) *ParseError {
	return &ParseError{
		Message: fmt.Sprintf(format, args...),
		Pos:     pos,
	}
}

// decodeInt type converts v to target.
func decodeInt(v interface{}, target *int) error {
	if v, ok := v.(uint64); ok {
		*target = int(v)
		return nil
	}
	return fmt.Errorf("invalid int value: %v", v)
}

// decodeUint64 type converts v to target.
func decodeUint64(v interface{}, target *uint64) error {
	if v, ok := v.(uint64); ok {
		*target = v
		return nil
	}
	return fmt.Errorf("invalid int value: %v", v)
}

// decodeString type converts v to target.
func decodeString(v interface{}, target *string) error {
	if v, ok := v.(string); ok {
		*target = v
		return nil
	}
	return fmt.Errorf("invalid string value: %v", v)
}

// decodeDate type converts v to target.
func decodeDate(v interface{}, target *time.Time) error {
	if v, ok := v.(string); ok {
		t, err := time.Parse(TimeFormat, v)
		if err != nil {
			return fmt.Errorf("invalid date format: %s", v)
		}
		*target = t
		return nil
	}
	return fmt.Errorf("invalid date value: %v", v)
}
