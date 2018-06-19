package pql

//go:generate peg -inline pql.peg

import (
	"fmt"
	"math"
	"sort"
	"strconv"
)

const endSymbol rune = 1114112

/* The rule types inferred from the grammar are below. */
type pegRule uint8

const (
	ruleUnknown pegRule = iota
	ruleCalls
	ruleCall
	ruleallargs
	ruleargs
	rulearg
	ruleCOND
	ruleconditional
	rulecondint
	rulecondLT
	rulecondfield
	ruletimerange
	rulevalue
	rulelist
	ruleitem
	ruledoublequotedstring
	rulesinglequotedstring
	rulefieldExpr
	rulefield
	ruleposfield
	ruleuint
	ruleuintrow
	ruleuintcol
	ruleopen
	ruleclose
	rulesp
	rulecomma
	rulelbrack
	rulerbrack
	rulewhitesp
	ruleIDENT
	ruletimestampbasicfmt
	ruletimestampfmt
	ruletimestamp
	ruleAction0
	ruleAction1
	ruleAction2
	ruleAction3
	ruleAction4
	ruleAction5
	ruleAction6
	ruleAction7
	ruleAction8
	ruleAction9
	ruleAction10
	ruleAction11
	rulePegText
	ruleAction12
	ruleAction13
	ruleAction14
	ruleAction15
	ruleAction16
	ruleAction17
	ruleAction18
	ruleAction19
	ruleAction20
	ruleAction21
	ruleAction22
	ruleAction23
	ruleAction24
	ruleAction25
	ruleAction26
	ruleAction27
	ruleAction28
	ruleAction29
	ruleAction30
	ruleAction31
	ruleAction32
	ruleAction33
	ruleAction34
	ruleAction35
	ruleAction36
	ruleAction37
	ruleAction38
	ruleAction39
	ruleAction40
	ruleAction41
	ruleAction42
)

var rul3s = [...]string{
	"Unknown",
	"Calls",
	"Call",
	"allargs",
	"args",
	"arg",
	"COND",
	"conditional",
	"condint",
	"condLT",
	"condfield",
	"timerange",
	"value",
	"list",
	"item",
	"doublequotedstring",
	"singlequotedstring",
	"fieldExpr",
	"field",
	"posfield",
	"uint",
	"uintrow",
	"uintcol",
	"open",
	"close",
	"sp",
	"comma",
	"lbrack",
	"rbrack",
	"whitesp",
	"IDENT",
	"timestampbasicfmt",
	"timestampfmt",
	"timestamp",
	"Action0",
	"Action1",
	"Action2",
	"Action3",
	"Action4",
	"Action5",
	"Action6",
	"Action7",
	"Action8",
	"Action9",
	"Action10",
	"Action11",
	"PegText",
	"Action12",
	"Action13",
	"Action14",
	"Action15",
	"Action16",
	"Action17",
	"Action18",
	"Action19",
	"Action20",
	"Action21",
	"Action22",
	"Action23",
	"Action24",
	"Action25",
	"Action26",
	"Action27",
	"Action28",
	"Action29",
	"Action30",
	"Action31",
	"Action32",
	"Action33",
	"Action34",
	"Action35",
	"Action36",
	"Action37",
	"Action38",
	"Action39",
	"Action40",
	"Action41",
	"Action42",
}

type token32 struct {
	pegRule
	begin, end uint32
}

func (t *token32) String() string {
	return fmt.Sprintf("\x1B[34m%v\x1B[m %v %v", rul3s[t.pegRule], t.begin, t.end)
}

type node32 struct {
	token32
	up, next *node32
}

func (node *node32) print(pretty bool, buffer string) {
	var print func(node *node32, depth int)
	print = func(node *node32, depth int) {
		for node != nil {
			for c := 0; c < depth; c++ {
				fmt.Printf(" ")
			}
			rule := rul3s[node.pegRule]
			quote := strconv.Quote(string(([]rune(buffer)[node.begin:node.end])))
			if !pretty {
				fmt.Printf("%v %v\n", rule, quote)
			} else {
				fmt.Printf("\x1B[34m%v\x1B[m %v\n", rule, quote)
			}
			if node.up != nil {
				print(node.up, depth+1)
			}
			node = node.next
		}
	}
	print(node, 0)
}

func (node *node32) Print(buffer string) {
	node.print(false, buffer)
}

func (node *node32) PrettyPrint(buffer string) {
	node.print(true, buffer)
}

type tokens32 struct {
	tree []token32
}

func (t *tokens32) Trim(length uint32) {
	t.tree = t.tree[:length]
}

func (t *tokens32) Print() {
	for _, token := range t.tree {
		fmt.Println(token.String())
	}
}

func (t *tokens32) AST() *node32 {
	type element struct {
		node *node32
		down *element
	}
	tokens := t.Tokens()
	var stack *element
	for _, token := range tokens {
		if token.begin == token.end {
			continue
		}
		node := &node32{token32: token}
		for stack != nil && stack.node.begin >= token.begin && stack.node.end <= token.end {
			stack.node.next = node.up
			node.up = stack.node
			stack = stack.down
		}
		stack = &element{node: node, down: stack}
	}
	if stack != nil {
		return stack.node
	}
	return nil
}

func (t *tokens32) PrintSyntaxTree(buffer string) {
	t.AST().Print(buffer)
}

func (t *tokens32) PrettyPrintSyntaxTree(buffer string) {
	t.AST().PrettyPrint(buffer)
}

func (t *tokens32) Add(rule pegRule, begin, end, index uint32) {
	if tree := t.tree; int(index) >= len(tree) {
		expanded := make([]token32, 2*len(tree))
		copy(expanded, tree)
		t.tree = expanded
	}
	t.tree[index] = token32{
		pegRule: rule,
		begin:   begin,
		end:     end,
	}
}

func (t *tokens32) Tokens() []token32 {
	return t.tree
}

type PQL struct {
	Query

	Buffer string
	buffer []rune
	rules  [78]func() bool
	parse  func(rule ...int) error
	reset  func()
	Pretty bool
	tokens32
}

func (p *PQL) Parse(rule ...int) error {
	return p.parse(rule...)
}

func (p *PQL) Reset() {
	p.reset()
}

type textPosition struct {
	line, symbol int
}

type textPositionMap map[int]textPosition

func translatePositions(buffer []rune, positions []int) textPositionMap {
	length, translations, j, line, symbol := len(positions), make(textPositionMap, len(positions)), 0, 1, 0
	sort.Ints(positions)

search:
	for i, c := range buffer {
		if c == '\n' {
			line, symbol = line+1, 0
		} else {
			symbol++
		}
		if i == positions[j] {
			translations[positions[j]] = textPosition{line, symbol}
			for j++; j < length; j++ {
				if i != positions[j] {
					continue search
				}
			}
			break search
		}
	}

	return translations
}

type parseError struct {
	p   *PQL
	max token32
}

func (e *parseError) Error() string {
	tokens, error := []token32{e.max}, "\n"
	positions, p := make([]int, 2*len(tokens)), 0
	for _, token := range tokens {
		positions[p], p = int(token.begin), p+1
		positions[p], p = int(token.end), p+1
	}
	translations := translatePositions(e.p.buffer, positions)
	format := "parse error near %v (line %v symbol %v - line %v symbol %v):\n%v\n"
	if e.p.Pretty {
		format = "parse error near \x1B[34m%v\x1B[m (line %v symbol %v - line %v symbol %v):\n%v\n"
	}
	for _, token := range tokens {
		begin, end := int(token.begin), int(token.end)
		error += fmt.Sprintf(format,
			rul3s[token.pegRule],
			translations[begin].line, translations[begin].symbol,
			translations[end].line, translations[end].symbol,
			strconv.Quote(string(e.p.buffer[begin:end])))
	}

	return error
}

func (p *PQL) PrintSyntaxTree() {
	if p.Pretty {
		p.tokens32.PrettyPrintSyntaxTree(p.Buffer)
	} else {
		p.tokens32.PrintSyntaxTree(p.Buffer)
	}
}

func (p *PQL) Execute() {
	buffer, _buffer, text, begin, end := p.Buffer, p.buffer, "", 0, 0
	for _, token := range p.Tokens() {
		switch token.pegRule {

		case rulePegText:
			begin, end = int(token.begin), int(token.end)
			text = string(_buffer[begin:end])

		case ruleAction0:
			p.startCall("Set")
		case ruleAction1:
			p.endCall()
		case ruleAction2:
			p.startCall("SetRowAttrs")
		case ruleAction3:
			p.endCall()
		case ruleAction4:
			p.startCall("SetColAttrs")
		case ruleAction5:
			p.endCall()
		case ruleAction6:
			p.startCall("Clear")
		case ruleAction7:
			p.endCall()
		case ruleAction8:
			p.startCall("TopN")
		case ruleAction9:
			p.endCall()
		case ruleAction10:
			p.startCall("Range")
		case ruleAction11:
			p.endCall()
		case ruleAction12:
			p.startCall(buffer[begin:end])
		case ruleAction13:
			p.endCall()
		case ruleAction14:
			p.addBTWN()
		case ruleAction15:
			p.addLTE()
		case ruleAction16:
			p.addGTE()
		case ruleAction17:
			p.addEQ()
		case ruleAction18:
			p.addNEQ()
		case ruleAction19:
			p.addLT()
		case ruleAction20:
			p.addGT()
		case ruleAction21:
			p.startConditional()
		case ruleAction22:
			p.endConditional()
		case ruleAction23:
			p.condAdd(buffer[begin:end])
		case ruleAction24:
			p.condAdd(buffer[begin:end])
		case ruleAction25:
			p.condAdd(buffer[begin:end])
		case ruleAction26:
			p.addPosStr("_start", buffer[begin:end])
		case ruleAction27:
			p.addPosStr("_end", buffer[begin:end])
		case ruleAction28:
			p.startList()
		case ruleAction29:
			p.endList()
		case ruleAction30:
			p.addVal(nil)
		case ruleAction31:
			p.addVal(true)
		case ruleAction32:
			p.addVal(false)
		case ruleAction33:
			p.addNumVal(buffer[begin:end])
		case ruleAction34:
			p.addNumVal(buffer[begin:end])
		case ruleAction35:
			p.addVal(buffer[begin:end])
		case ruleAction36:
			p.addVal(buffer[begin:end])
		case ruleAction37:
			p.addVal(buffer[begin:end])
		case ruleAction38:
			p.addField(buffer[begin:end])
		case ruleAction39:
			p.addPosStr("_field", buffer[begin:end])
		case ruleAction40:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction41:
			p.addPosNum("_col", buffer[begin:end])
		case ruleAction42:
			p.addPosStr("_timestamp", buffer[begin:end])

		}
	}
	_, _, _, _, _ = buffer, _buffer, text, begin, end
}

func (p *PQL) Init() {
	var (
		max                  token32
		position, tokenIndex uint32
		buffer               []rune
	)
	p.reset = func() {
		max = token32{}
		position, tokenIndex = 0, 0

		p.buffer = []rune(p.Buffer)
		if len(p.buffer) == 0 || p.buffer[len(p.buffer)-1] != endSymbol {
			p.buffer = append(p.buffer, endSymbol)
		}
		buffer = p.buffer
	}
	p.reset()

	_rules := p.rules
	tree := tokens32{tree: make([]token32, math.MaxInt16)}
	p.parse = func(rule ...int) error {
		r := 1
		if len(rule) > 0 {
			r = rule[0]
		}
		matches := p.rules[r]()
		p.tokens32 = tree
		if matches {
			p.Trim(tokenIndex)
			return nil
		}
		return &parseError{p, max}
	}

	add := func(rule pegRule, begin uint32) {
		tree.Add(rule, begin, position, tokenIndex)
		tokenIndex++
		if begin != position && position > max.end {
			max = token32{rule, begin, position}
		}
	}

	matchDot := func() bool {
		if buffer[position] != endSymbol {
			position++
			return true
		}
		return false
	}

	/*matchChar := func(c byte) bool {
		if buffer[position] == c {
			position++
			return true
		}
		return false
	}*/

	/*matchRange := func(lower byte, upper byte) bool {
		if c := buffer[position]; c >= lower && c <= upper {
			position++
			return true
		}
		return false
	}*/

	_rules = [...]func() bool{
		nil,
		/* 0 Calls <- <(whitesp (Call whitesp)* !.)> */
		func() bool {
			position0, tokenIndex0 := position, tokenIndex
			{
				position1 := position
				if !_rules[rulewhitesp]() {
					goto l0
				}
			l2:
				{
					position3, tokenIndex3 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l3
					}
					if !_rules[rulewhitesp]() {
						goto l3
					}
					goto l2
				l3:
					position, tokenIndex = position3, tokenIndex3
				}
				{
					position4, tokenIndex4 := position, tokenIndex
					if !matchDot() {
						goto l4
					}
					goto l0
				l4:
					position, tokenIndex = position4, tokenIndex4
				}
				add(ruleCalls, position1)
			}
			return true
		l0:
			position, tokenIndex = position0, tokenIndex0
			return false
		},
		/* 1 Call <- <(('S' 'e' 't' Action0 open uintcol comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma uintrow comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'A' 't' 't' 'r' 's' Action4 open posfield comma uintcol comma args close Action5) / ('C' 'l' 'e' 'a' 'r' Action6 open uintcol comma args close Action7) / ('T' 'o' 'p' 'N' Action8 open posfield (comma allargs)? close Action9) / ('R' 'a' 'n' 'g' 'e' Action10 open (timerange / conditional / arg) close Action11) / (<IDENT> Action12 open allargs comma? close Action13))> */
		func() bool {
			position5, tokenIndex5 := position, tokenIndex
			{
				position6 := position
				{
					position7, tokenIndex7 := position, tokenIndex
					if buffer[position] != rune('S') {
						goto l8
					}
					position++
					if buffer[position] != rune('e') {
						goto l8
					}
					position++
					if buffer[position] != rune('t') {
						goto l8
					}
					position++
					{
						add(ruleAction0, position)
					}
					if !_rules[ruleopen]() {
						goto l8
					}
					if !_rules[ruleuintcol]() {
						goto l8
					}
					if !_rules[rulecomma]() {
						goto l8
					}
					if !_rules[ruleargs]() {
						goto l8
					}
					{
						position10, tokenIndex10 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l10
						}
						{
							position12 := position
							{
								position13 := position
								if !_rules[ruletimestampfmt]() {
									goto l10
								}
								add(rulePegText, position13)
							}
							{
								add(ruleAction42, position)
							}
							add(ruletimestamp, position12)
						}
						goto l11
					l10:
						position, tokenIndex = position10, tokenIndex10
					}
				l11:
					if !_rules[ruleclose]() {
						goto l8
					}
					{
						add(ruleAction1, position)
					}
					goto l7
				l8:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('S') {
						goto l16
					}
					position++
					if buffer[position] != rune('e') {
						goto l16
					}
					position++
					if buffer[position] != rune('t') {
						goto l16
					}
					position++
					if buffer[position] != rune('R') {
						goto l16
					}
					position++
					if buffer[position] != rune('o') {
						goto l16
					}
					position++
					if buffer[position] != rune('w') {
						goto l16
					}
					position++
					if buffer[position] != rune('A') {
						goto l16
					}
					position++
					if buffer[position] != rune('t') {
						goto l16
					}
					position++
					if buffer[position] != rune('t') {
						goto l16
					}
					position++
					if buffer[position] != rune('r') {
						goto l16
					}
					position++
					if buffer[position] != rune('s') {
						goto l16
					}
					position++
					{
						add(ruleAction2, position)
					}
					if !_rules[ruleopen]() {
						goto l16
					}
					if !_rules[ruleposfield]() {
						goto l16
					}
					if !_rules[rulecomma]() {
						goto l16
					}
					{
						position18 := position
						{
							position19 := position
							if !_rules[ruleuint]() {
								goto l16
							}
							add(rulePegText, position19)
						}
						{
							add(ruleAction40, position)
						}
						add(ruleuintrow, position18)
					}
					if !_rules[rulecomma]() {
						goto l16
					}
					if !_rules[ruleargs]() {
						goto l16
					}
					if !_rules[ruleclose]() {
						goto l16
					}
					{
						add(ruleAction3, position)
					}
					goto l7
				l16:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('S') {
						goto l22
					}
					position++
					if buffer[position] != rune('e') {
						goto l22
					}
					position++
					if buffer[position] != rune('t') {
						goto l22
					}
					position++
					if buffer[position] != rune('C') {
						goto l22
					}
					position++
					if buffer[position] != rune('o') {
						goto l22
					}
					position++
					if buffer[position] != rune('l') {
						goto l22
					}
					position++
					if buffer[position] != rune('A') {
						goto l22
					}
					position++
					if buffer[position] != rune('t') {
						goto l22
					}
					position++
					if buffer[position] != rune('t') {
						goto l22
					}
					position++
					if buffer[position] != rune('r') {
						goto l22
					}
					position++
					if buffer[position] != rune('s') {
						goto l22
					}
					position++
					{
						add(ruleAction4, position)
					}
					if !_rules[ruleopen]() {
						goto l22
					}
					if !_rules[ruleposfield]() {
						goto l22
					}
					if !_rules[rulecomma]() {
						goto l22
					}
					if !_rules[ruleuintcol]() {
						goto l22
					}
					if !_rules[rulecomma]() {
						goto l22
					}
					if !_rules[ruleargs]() {
						goto l22
					}
					if !_rules[ruleclose]() {
						goto l22
					}
					{
						add(ruleAction5, position)
					}
					goto l7
				l22:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('C') {
						goto l25
					}
					position++
					if buffer[position] != rune('l') {
						goto l25
					}
					position++
					if buffer[position] != rune('e') {
						goto l25
					}
					position++
					if buffer[position] != rune('a') {
						goto l25
					}
					position++
					if buffer[position] != rune('r') {
						goto l25
					}
					position++
					{
						add(ruleAction6, position)
					}
					if !_rules[ruleopen]() {
						goto l25
					}
					if !_rules[ruleuintcol]() {
						goto l25
					}
					if !_rules[rulecomma]() {
						goto l25
					}
					if !_rules[ruleargs]() {
						goto l25
					}
					if !_rules[ruleclose]() {
						goto l25
					}
					{
						add(ruleAction7, position)
					}
					goto l7
				l25:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('T') {
						goto l28
					}
					position++
					if buffer[position] != rune('o') {
						goto l28
					}
					position++
					if buffer[position] != rune('p') {
						goto l28
					}
					position++
					if buffer[position] != rune('N') {
						goto l28
					}
					position++
					{
						add(ruleAction8, position)
					}
					if !_rules[ruleopen]() {
						goto l28
					}
					if !_rules[ruleposfield]() {
						goto l28
					}
					{
						position30, tokenIndex30 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l30
						}
						if !_rules[ruleallargs]() {
							goto l30
						}
						goto l31
					l30:
						position, tokenIndex = position30, tokenIndex30
					}
				l31:
					if !_rules[ruleclose]() {
						goto l28
					}
					{
						add(ruleAction9, position)
					}
					goto l7
				l28:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('R') {
						goto l33
					}
					position++
					if buffer[position] != rune('a') {
						goto l33
					}
					position++
					if buffer[position] != rune('n') {
						goto l33
					}
					position++
					if buffer[position] != rune('g') {
						goto l33
					}
					position++
					if buffer[position] != rune('e') {
						goto l33
					}
					position++
					{
						add(ruleAction10, position)
					}
					if !_rules[ruleopen]() {
						goto l33
					}
					{
						position35, tokenIndex35 := position, tokenIndex
						{
							position37 := position
							if !_rules[rulefield]() {
								goto l36
							}
							if !_rules[rulesp]() {
								goto l36
							}
							if buffer[position] != rune('=') {
								goto l36
							}
							position++
							if !_rules[rulesp]() {
								goto l36
							}
							if !_rules[rulevalue]() {
								goto l36
							}
							if !_rules[rulecomma]() {
								goto l36
							}
							{
								position38 := position
								if !_rules[ruletimestampfmt]() {
									goto l36
								}
								add(rulePegText, position38)
							}
							{
								add(ruleAction26, position)
							}
							if !_rules[rulecomma]() {
								goto l36
							}
							{
								position40 := position
								if !_rules[ruletimestampfmt]() {
									goto l36
								}
								add(rulePegText, position40)
							}
							{
								add(ruleAction27, position)
							}
							add(ruletimerange, position37)
						}
						goto l35
					l36:
						position, tokenIndex = position35, tokenIndex35
						{
							position43 := position
							{
								add(ruleAction21, position)
							}
							if !_rules[rulecondint]() {
								goto l42
							}
							if !_rules[rulecondLT]() {
								goto l42
							}
							{
								position45 := position
								{
									position46 := position
									if !_rules[rulefieldExpr]() {
										goto l42
									}
									add(rulePegText, position46)
								}
								if !_rules[rulesp]() {
									goto l42
								}
								{
									add(ruleAction25, position)
								}
								add(rulecondfield, position45)
							}
							if !_rules[rulecondLT]() {
								goto l42
							}
							if !_rules[rulecondint]() {
								goto l42
							}
							{
								add(ruleAction22, position)
							}
							add(ruleconditional, position43)
						}
						goto l35
					l42:
						position, tokenIndex = position35, tokenIndex35
						if !_rules[rulearg]() {
							goto l33
						}
					}
				l35:
					if !_rules[ruleclose]() {
						goto l33
					}
					{
						add(ruleAction11, position)
					}
					goto l7
				l33:
					position, tokenIndex = position7, tokenIndex7
					{
						position50 := position
						{
							position51 := position
							{
								position52, tokenIndex52 := position, tokenIndex
								{
									position53, tokenIndex53 := position, tokenIndex
									if buffer[position] != rune('S') {
										goto l54
									}
									position++
									if buffer[position] != rune('e') {
										goto l54
									}
									position++
									if buffer[position] != rune('t') {
										goto l54
									}
									position++
									if buffer[position] != rune('(') {
										goto l54
									}
									position++
									goto l53
								l54:
									position, tokenIndex = position53, tokenIndex53
									if buffer[position] != rune('S') {
										goto l55
									}
									position++
									if buffer[position] != rune('e') {
										goto l55
									}
									position++
									if buffer[position] != rune('t') {
										goto l55
									}
									position++
									if buffer[position] != rune('R') {
										goto l55
									}
									position++
									if buffer[position] != rune('o') {
										goto l55
									}
									position++
									if buffer[position] != rune('w') {
										goto l55
									}
									position++
									if buffer[position] != rune('A') {
										goto l55
									}
									position++
									if buffer[position] != rune('t') {
										goto l55
									}
									position++
									if buffer[position] != rune('t') {
										goto l55
									}
									position++
									if buffer[position] != rune('r') {
										goto l55
									}
									position++
									if buffer[position] != rune('s') {
										goto l55
									}
									position++
									if buffer[position] != rune('(') {
										goto l55
									}
									position++
									goto l53
								l55:
									position, tokenIndex = position53, tokenIndex53
									if buffer[position] != rune('S') {
										goto l56
									}
									position++
									if buffer[position] != rune('e') {
										goto l56
									}
									position++
									if buffer[position] != rune('t') {
										goto l56
									}
									position++
									if buffer[position] != rune('C') {
										goto l56
									}
									position++
									if buffer[position] != rune('o') {
										goto l56
									}
									position++
									if buffer[position] != rune('l') {
										goto l56
									}
									position++
									if buffer[position] != rune('A') {
										goto l56
									}
									position++
									if buffer[position] != rune('t') {
										goto l56
									}
									position++
									if buffer[position] != rune('t') {
										goto l56
									}
									position++
									if buffer[position] != rune('r') {
										goto l56
									}
									position++
									if buffer[position] != rune('s') {
										goto l56
									}
									position++
									if buffer[position] != rune('(') {
										goto l56
									}
									position++
									goto l53
								l56:
									position, tokenIndex = position53, tokenIndex53
									if buffer[position] != rune('C') {
										goto l57
									}
									position++
									if buffer[position] != rune('l') {
										goto l57
									}
									position++
									if buffer[position] != rune('e') {
										goto l57
									}
									position++
									if buffer[position] != rune('a') {
										goto l57
									}
									position++
									if buffer[position] != rune('r') {
										goto l57
									}
									position++
									if buffer[position] != rune('(') {
										goto l57
									}
									position++
									goto l53
								l57:
									position, tokenIndex = position53, tokenIndex53
									if buffer[position] != rune('T') {
										goto l58
									}
									position++
									if buffer[position] != rune('o') {
										goto l58
									}
									position++
									if buffer[position] != rune('p') {
										goto l58
									}
									position++
									if buffer[position] != rune('N') {
										goto l58
									}
									position++
									if buffer[position] != rune('(') {
										goto l58
									}
									position++
									goto l53
								l58:
									position, tokenIndex = position53, tokenIndex53
									if buffer[position] != rune('R') {
										goto l52
									}
									position++
									if buffer[position] != rune('a') {
										goto l52
									}
									position++
									if buffer[position] != rune('n') {
										goto l52
									}
									position++
									if buffer[position] != rune('g') {
										goto l52
									}
									position++
									if buffer[position] != rune('e') {
										goto l52
									}
									position++
									if buffer[position] != rune('(') {
										goto l52
									}
									position++
								}
							l53:
								goto l5
							l52:
								position, tokenIndex = position52, tokenIndex52
							}
							{
								position59, tokenIndex59 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l60
								}
								position++
								goto l59
							l60:
								position, tokenIndex = position59, tokenIndex59
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l5
								}
								position++
							}
						l59:
						l61:
							{
								position62, tokenIndex62 := position, tokenIndex
								{
									position63, tokenIndex63 := position, tokenIndex
									if c := buffer[position]; c < rune('a') || c > rune('z') {
										goto l64
									}
									position++
									goto l63
								l64:
									position, tokenIndex = position63, tokenIndex63
									if c := buffer[position]; c < rune('A') || c > rune('Z') {
										goto l65
									}
									position++
									goto l63
								l65:
									position, tokenIndex = position63, tokenIndex63
									if c := buffer[position]; c < rune('0') || c > rune('9') {
										goto l62
									}
									position++
								}
							l63:
								goto l61
							l62:
								position, tokenIndex = position62, tokenIndex62
							}
							add(ruleIDENT, position51)
						}
						add(rulePegText, position50)
					}
					{
						add(ruleAction12, position)
					}
					if !_rules[ruleopen]() {
						goto l5
					}
					if !_rules[ruleallargs]() {
						goto l5
					}
					{
						position67, tokenIndex67 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l67
						}
						goto l68
					l67:
						position, tokenIndex = position67, tokenIndex67
					}
				l68:
					if !_rules[ruleclose]() {
						goto l5
					}
					{
						add(ruleAction13, position)
					}
				}
			l7:
				add(ruleCall, position6)
			}
			return true
		l5:
			position, tokenIndex = position5, tokenIndex5
			return false
		},
		/* 2 allargs <- <((Call (comma Call)* (comma args)?) / args / sp)> */
		func() bool {
			position70, tokenIndex70 := position, tokenIndex
			{
				position71 := position
				{
					position72, tokenIndex72 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l73
					}
				l74:
					{
						position75, tokenIndex75 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l75
						}
						if !_rules[ruleCall]() {
							goto l75
						}
						goto l74
					l75:
						position, tokenIndex = position75, tokenIndex75
					}
					{
						position76, tokenIndex76 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l76
						}
						if !_rules[ruleargs]() {
							goto l76
						}
						goto l77
					l76:
						position, tokenIndex = position76, tokenIndex76
					}
				l77:
					goto l72
				l73:
					position, tokenIndex = position72, tokenIndex72
					if !_rules[ruleargs]() {
						goto l78
					}
					goto l72
				l78:
					position, tokenIndex = position72, tokenIndex72
					if !_rules[rulesp]() {
						goto l70
					}
				}
			l72:
				add(ruleallargs, position71)
			}
			return true
		l70:
			position, tokenIndex = position70, tokenIndex70
			return false
		},
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position79, tokenIndex79 := position, tokenIndex
			{
				position80 := position
				if !_rules[rulearg]() {
					goto l79
				}
				{
					position81, tokenIndex81 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l81
					}
					if !_rules[ruleargs]() {
						goto l81
					}
					goto l82
				l81:
					position, tokenIndex = position81, tokenIndex81
				}
			l82:
				if !_rules[rulesp]() {
					goto l79
				}
				add(ruleargs, position80)
			}
			return true
		l79:
			position, tokenIndex = position79, tokenIndex79
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value))> */
		func() bool {
			position83, tokenIndex83 := position, tokenIndex
			{
				position84 := position
				{
					position85, tokenIndex85 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l86
					}
					if !_rules[rulesp]() {
						goto l86
					}
					if buffer[position] != rune('=') {
						goto l86
					}
					position++
					if !_rules[rulesp]() {
						goto l86
					}
					if !_rules[rulevalue]() {
						goto l86
					}
					goto l85
				l86:
					position, tokenIndex = position85, tokenIndex85
					if !_rules[rulefield]() {
						goto l83
					}
					if !_rules[rulesp]() {
						goto l83
					}
					{
						position87 := position
						{
							position88, tokenIndex88 := position, tokenIndex
							if buffer[position] != rune('>') {
								goto l89
							}
							position++
							if buffer[position] != rune('<') {
								goto l89
							}
							position++
							{
								add(ruleAction14, position)
							}
							goto l88
						l89:
							position, tokenIndex = position88, tokenIndex88
							if buffer[position] != rune('<') {
								goto l91
							}
							position++
							if buffer[position] != rune('=') {
								goto l91
							}
							position++
							{
								add(ruleAction15, position)
							}
							goto l88
						l91:
							position, tokenIndex = position88, tokenIndex88
							if buffer[position] != rune('>') {
								goto l93
							}
							position++
							if buffer[position] != rune('=') {
								goto l93
							}
							position++
							{
								add(ruleAction16, position)
							}
							goto l88
						l93:
							position, tokenIndex = position88, tokenIndex88
							if buffer[position] != rune('=') {
								goto l95
							}
							position++
							if buffer[position] != rune('=') {
								goto l95
							}
							position++
							{
								add(ruleAction17, position)
							}
							goto l88
						l95:
							position, tokenIndex = position88, tokenIndex88
							if buffer[position] != rune('!') {
								goto l97
							}
							position++
							if buffer[position] != rune('=') {
								goto l97
							}
							position++
							{
								add(ruleAction18, position)
							}
							goto l88
						l97:
							position, tokenIndex = position88, tokenIndex88
							if buffer[position] != rune('<') {
								goto l99
							}
							position++
							{
								add(ruleAction19, position)
							}
							goto l88
						l99:
							position, tokenIndex = position88, tokenIndex88
							if buffer[position] != rune('>') {
								goto l83
							}
							position++
							{
								add(ruleAction20, position)
							}
						}
					l88:
						add(ruleCOND, position87)
					}
					if !_rules[rulesp]() {
						goto l83
					}
					if !_rules[rulevalue]() {
						goto l83
					}
				}
			l85:
				add(rulearg, position84)
			}
			return true
		l83:
			position, tokenIndex = position83, tokenIndex83
			return false
		},
		/* 5 COND <- <(('>' '<' Action14) / ('<' '=' Action15) / ('>' '=' Action16) / ('=' '=' Action17) / ('!' '=' Action18) / ('<' Action19) / ('>' Action20))> */
		nil,
		/* 6 conditional <- <(Action21 condint condLT condfield condLT condint Action22)> */
		nil,
		/* 7 condint <- <(<(('-'? [1-9] [0-9]*) / '0')> sp Action23)> */
		func() bool {
			position104, tokenIndex104 := position, tokenIndex
			{
				position105 := position
				{
					position106 := position
					{
						position107, tokenIndex107 := position, tokenIndex
						{
							position109, tokenIndex109 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l109
							}
							position++
							goto l110
						l109:
							position, tokenIndex = position109, tokenIndex109
						}
					l110:
						if c := buffer[position]; c < rune('1') || c > rune('9') {
							goto l108
						}
						position++
					l111:
						{
							position112, tokenIndex112 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l112
							}
							position++
							goto l111
						l112:
							position, tokenIndex = position112, tokenIndex112
						}
						goto l107
					l108:
						position, tokenIndex = position107, tokenIndex107
						if buffer[position] != rune('0') {
							goto l104
						}
						position++
					}
				l107:
					add(rulePegText, position106)
				}
				if !_rules[rulesp]() {
					goto l104
				}
				{
					add(ruleAction23, position)
				}
				add(rulecondint, position105)
			}
			return true
		l104:
			position, tokenIndex = position104, tokenIndex104
			return false
		},
		/* 8 condLT <- <(<(('<' '=') / '<')> sp Action24)> */
		func() bool {
			position114, tokenIndex114 := position, tokenIndex
			{
				position115 := position
				{
					position116 := position
					{
						position117, tokenIndex117 := position, tokenIndex
						if buffer[position] != rune('<') {
							goto l118
						}
						position++
						if buffer[position] != rune('=') {
							goto l118
						}
						position++
						goto l117
					l118:
						position, tokenIndex = position117, tokenIndex117
						if buffer[position] != rune('<') {
							goto l114
						}
						position++
					}
				l117:
					add(rulePegText, position116)
				}
				if !_rules[rulesp]() {
					goto l114
				}
				{
					add(ruleAction24, position)
				}
				add(rulecondLT, position115)
			}
			return true
		l114:
			position, tokenIndex = position114, tokenIndex114
			return false
		},
		/* 9 condfield <- <(<fieldExpr> sp Action25)> */
		nil,
		/* 10 timerange <- <(field sp '=' sp value comma <timestampfmt> Action26 comma <timestampfmt> Action27)> */
		nil,
		/* 11 value <- <(item / (lbrack Action28 list rbrack Action29))> */
		func() bool {
			position122, tokenIndex122 := position, tokenIndex
			{
				position123 := position
				{
					position124, tokenIndex124 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l125
					}
					goto l124
				l125:
					position, tokenIndex = position124, tokenIndex124
					{
						position126 := position
						if buffer[position] != rune('[') {
							goto l122
						}
						position++
						if !_rules[rulesp]() {
							goto l122
						}
						add(rulelbrack, position126)
					}
					{
						add(ruleAction28, position)
					}
					if !_rules[rulelist]() {
						goto l122
					}
					{
						position128 := position
						if !_rules[rulesp]() {
							goto l122
						}
						if buffer[position] != rune(']') {
							goto l122
						}
						position++
						if !_rules[rulesp]() {
							goto l122
						}
						add(rulerbrack, position128)
					}
					{
						add(ruleAction29, position)
					}
				}
			l124:
				add(rulevalue, position123)
			}
			return true
		l122:
			position, tokenIndex = position122, tokenIndex122
			return false
		},
		/* 12 list <- <(item (comma list)?)> */
		func() bool {
			position130, tokenIndex130 := position, tokenIndex
			{
				position131 := position
				if !_rules[ruleitem]() {
					goto l130
				}
				{
					position132, tokenIndex132 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l132
					}
					if !_rules[rulelist]() {
						goto l132
					}
					goto l133
				l132:
					position, tokenIndex = position132, tokenIndex132
				}
			l133:
				add(rulelist, position131)
			}
			return true
		l130:
			position, tokenIndex = position130, tokenIndex130
			return false
		},
		/* 13 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action30) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action31) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action32) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action33) / (<('-'? '.' [0-9]+)> Action34) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action35) / ('"' <doublequotedstring> '"' Action36) / ('\'' <singlequotedstring> '\'' Action37))> */
		func() bool {
			position134, tokenIndex134 := position, tokenIndex
			{
				position135 := position
				{
					position136, tokenIndex136 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l137
					}
					position++
					if buffer[position] != rune('u') {
						goto l137
					}
					position++
					if buffer[position] != rune('l') {
						goto l137
					}
					position++
					if buffer[position] != rune('l') {
						goto l137
					}
					position++
					{
						position138, tokenIndex138 := position, tokenIndex
						{
							position139, tokenIndex139 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l140
							}
							goto l139
						l140:
							position, tokenIndex = position139, tokenIndex139
							if !_rules[rulesp]() {
								goto l137
							}
							if !_rules[ruleclose]() {
								goto l137
							}
						}
					l139:
						position, tokenIndex = position138, tokenIndex138
					}
					{
						add(ruleAction30, position)
					}
					goto l136
				l137:
					position, tokenIndex = position136, tokenIndex136
					if buffer[position] != rune('t') {
						goto l142
					}
					position++
					if buffer[position] != rune('r') {
						goto l142
					}
					position++
					if buffer[position] != rune('u') {
						goto l142
					}
					position++
					if buffer[position] != rune('e') {
						goto l142
					}
					position++
					{
						position143, tokenIndex143 := position, tokenIndex
						{
							position144, tokenIndex144 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l145
							}
							goto l144
						l145:
							position, tokenIndex = position144, tokenIndex144
							if !_rules[rulesp]() {
								goto l142
							}
							if !_rules[ruleclose]() {
								goto l142
							}
						}
					l144:
						position, tokenIndex = position143, tokenIndex143
					}
					{
						add(ruleAction31, position)
					}
					goto l136
				l142:
					position, tokenIndex = position136, tokenIndex136
					if buffer[position] != rune('f') {
						goto l147
					}
					position++
					if buffer[position] != rune('a') {
						goto l147
					}
					position++
					if buffer[position] != rune('l') {
						goto l147
					}
					position++
					if buffer[position] != rune('s') {
						goto l147
					}
					position++
					if buffer[position] != rune('e') {
						goto l147
					}
					position++
					{
						position148, tokenIndex148 := position, tokenIndex
						{
							position149, tokenIndex149 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l150
							}
							goto l149
						l150:
							position, tokenIndex = position149, tokenIndex149
							if !_rules[rulesp]() {
								goto l147
							}
							if !_rules[ruleclose]() {
								goto l147
							}
						}
					l149:
						position, tokenIndex = position148, tokenIndex148
					}
					{
						add(ruleAction32, position)
					}
					goto l136
				l147:
					position, tokenIndex = position136, tokenIndex136
					{
						position153 := position
						{
							position154, tokenIndex154 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l154
							}
							position++
							goto l155
						l154:
							position, tokenIndex = position154, tokenIndex154
						}
					l155:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l152
						}
						position++
					l156:
						{
							position157, tokenIndex157 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l157
							}
							position++
							goto l156
						l157:
							position, tokenIndex = position157, tokenIndex157
						}
						{
							position158, tokenIndex158 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l158
							}
							position++
						l160:
							{
								position161, tokenIndex161 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l161
								}
								position++
								goto l160
							l161:
								position, tokenIndex = position161, tokenIndex161
							}
							goto l159
						l158:
							position, tokenIndex = position158, tokenIndex158
						}
					l159:
						add(rulePegText, position153)
					}
					{
						add(ruleAction33, position)
					}
					goto l136
				l152:
					position, tokenIndex = position136, tokenIndex136
					{
						position164 := position
						{
							position165, tokenIndex165 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l165
							}
							position++
							goto l166
						l165:
							position, tokenIndex = position165, tokenIndex165
						}
					l166:
						if buffer[position] != rune('.') {
							goto l163
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l163
						}
						position++
					l167:
						{
							position168, tokenIndex168 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l168
							}
							position++
							goto l167
						l168:
							position, tokenIndex = position168, tokenIndex168
						}
						add(rulePegText, position164)
					}
					{
						add(ruleAction34, position)
					}
					goto l136
				l163:
					position, tokenIndex = position136, tokenIndex136
					{
						position171 := position
						{
							position174, tokenIndex174 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l175
							}
							position++
							goto l174
						l175:
							position, tokenIndex = position174, tokenIndex174
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l176
							}
							position++
							goto l174
						l176:
							position, tokenIndex = position174, tokenIndex174
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l177
							}
							position++
							goto l174
						l177:
							position, tokenIndex = position174, tokenIndex174
							if buffer[position] != rune('-') {
								goto l178
							}
							position++
							goto l174
						l178:
							position, tokenIndex = position174, tokenIndex174
							if buffer[position] != rune('_') {
								goto l179
							}
							position++
							goto l174
						l179:
							position, tokenIndex = position174, tokenIndex174
							if buffer[position] != rune(':') {
								goto l170
							}
							position++
						}
					l174:
					l172:
						{
							position173, tokenIndex173 := position, tokenIndex
							{
								position180, tokenIndex180 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l181
								}
								position++
								goto l180
							l181:
								position, tokenIndex = position180, tokenIndex180
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l182
								}
								position++
								goto l180
							l182:
								position, tokenIndex = position180, tokenIndex180
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l183
								}
								position++
								goto l180
							l183:
								position, tokenIndex = position180, tokenIndex180
								if buffer[position] != rune('-') {
									goto l184
								}
								position++
								goto l180
							l184:
								position, tokenIndex = position180, tokenIndex180
								if buffer[position] != rune('_') {
									goto l185
								}
								position++
								goto l180
							l185:
								position, tokenIndex = position180, tokenIndex180
								if buffer[position] != rune(':') {
									goto l173
								}
								position++
							}
						l180:
							goto l172
						l173:
							position, tokenIndex = position173, tokenIndex173
						}
						add(rulePegText, position171)
					}
					{
						add(ruleAction35, position)
					}
					goto l136
				l170:
					position, tokenIndex = position136, tokenIndex136
					if buffer[position] != rune('"') {
						goto l187
					}
					position++
					{
						position188 := position
						{
							position189 := position
						l190:
							{
								position191, tokenIndex191 := position, tokenIndex
								{
									position192, tokenIndex192 := position, tokenIndex
									{
										position194, tokenIndex194 := position, tokenIndex
										{
											position195, tokenIndex195 := position, tokenIndex
											if buffer[position] != rune('"') {
												goto l196
											}
											position++
											goto l195
										l196:
											position, tokenIndex = position195, tokenIndex195
											if buffer[position] != rune('\\') {
												goto l197
											}
											position++
											goto l195
										l197:
											position, tokenIndex = position195, tokenIndex195
											if buffer[position] != rune('\n') {
												goto l194
											}
											position++
										}
									l195:
										goto l193
									l194:
										position, tokenIndex = position194, tokenIndex194
									}
									if !matchDot() {
										goto l193
									}
									goto l192
								l193:
									position, tokenIndex = position192, tokenIndex192
									if buffer[position] != rune('\\') {
										goto l198
									}
									position++
									if buffer[position] != rune('n') {
										goto l198
									}
									position++
									goto l192
								l198:
									position, tokenIndex = position192, tokenIndex192
									if buffer[position] != rune('\\') {
										goto l199
									}
									position++
									if buffer[position] != rune('"') {
										goto l199
									}
									position++
									goto l192
								l199:
									position, tokenIndex = position192, tokenIndex192
									if buffer[position] != rune('\\') {
										goto l200
									}
									position++
									if buffer[position] != rune('\'') {
										goto l200
									}
									position++
									goto l192
								l200:
									position, tokenIndex = position192, tokenIndex192
									if buffer[position] != rune('\\') {
										goto l191
									}
									position++
									if buffer[position] != rune('\\') {
										goto l191
									}
									position++
								}
							l192:
								goto l190
							l191:
								position, tokenIndex = position191, tokenIndex191
							}
							add(ruledoublequotedstring, position189)
						}
						add(rulePegText, position188)
					}
					if buffer[position] != rune('"') {
						goto l187
					}
					position++
					{
						add(ruleAction36, position)
					}
					goto l136
				l187:
					position, tokenIndex = position136, tokenIndex136
					if buffer[position] != rune('\'') {
						goto l134
					}
					position++
					{
						position202 := position
						{
							position203 := position
						l204:
							{
								position205, tokenIndex205 := position, tokenIndex
								{
									position206, tokenIndex206 := position, tokenIndex
									{
										position208, tokenIndex208 := position, tokenIndex
										{
											position209, tokenIndex209 := position, tokenIndex
											if buffer[position] != rune('\'') {
												goto l210
											}
											position++
											goto l209
										l210:
											position, tokenIndex = position209, tokenIndex209
											if buffer[position] != rune('\\') {
												goto l211
											}
											position++
											goto l209
										l211:
											position, tokenIndex = position209, tokenIndex209
											if buffer[position] != rune('\n') {
												goto l208
											}
											position++
										}
									l209:
										goto l207
									l208:
										position, tokenIndex = position208, tokenIndex208
									}
									if !matchDot() {
										goto l207
									}
									goto l206
								l207:
									position, tokenIndex = position206, tokenIndex206
									if buffer[position] != rune('\\') {
										goto l212
									}
									position++
									if buffer[position] != rune('n') {
										goto l212
									}
									position++
									goto l206
								l212:
									position, tokenIndex = position206, tokenIndex206
									if buffer[position] != rune('\\') {
										goto l213
									}
									position++
									if buffer[position] != rune('"') {
										goto l213
									}
									position++
									goto l206
								l213:
									position, tokenIndex = position206, tokenIndex206
									if buffer[position] != rune('\\') {
										goto l214
									}
									position++
									if buffer[position] != rune('\'') {
										goto l214
									}
									position++
									goto l206
								l214:
									position, tokenIndex = position206, tokenIndex206
									if buffer[position] != rune('\\') {
										goto l205
									}
									position++
									if buffer[position] != rune('\\') {
										goto l205
									}
									position++
								}
							l206:
								goto l204
							l205:
								position, tokenIndex = position205, tokenIndex205
							}
							add(rulesinglequotedstring, position203)
						}
						add(rulePegText, position202)
					}
					if buffer[position] != rune('\'') {
						goto l134
					}
					position++
					{
						add(ruleAction37, position)
					}
				}
			l136:
				add(ruleitem, position135)
			}
			return true
		l134:
			position, tokenIndex = position134, tokenIndex134
			return false
		},
		/* 14 doublequotedstring <- <((!('"' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		nil,
		/* 15 singlequotedstring <- <((!('\'' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		nil,
		/* 16 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_')*)> */
		func() bool {
			position218, tokenIndex218 := position, tokenIndex
			{
				position219 := position
				{
					position220, tokenIndex220 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l221
					}
					position++
					goto l220
				l221:
					position, tokenIndex = position220, tokenIndex220
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l218
					}
					position++
				}
			l220:
			l222:
				{
					position223, tokenIndex223 := position, tokenIndex
					{
						position224, tokenIndex224 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l225
						}
						position++
						goto l224
					l225:
						position, tokenIndex = position224, tokenIndex224
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l226
						}
						position++
						goto l224
					l226:
						position, tokenIndex = position224, tokenIndex224
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l227
						}
						position++
						goto l224
					l227:
						position, tokenIndex = position224, tokenIndex224
						if buffer[position] != rune('_') {
							goto l223
						}
						position++
					}
				l224:
					goto l222
				l223:
					position, tokenIndex = position223, tokenIndex223
				}
				add(rulefieldExpr, position219)
			}
			return true
		l218:
			position, tokenIndex = position218, tokenIndex218
			return false
		},
		/* 17 field <- <(<fieldExpr> Action38)> */
		func() bool {
			position228, tokenIndex228 := position, tokenIndex
			{
				position229 := position
				{
					position230 := position
					if !_rules[rulefieldExpr]() {
						goto l228
					}
					add(rulePegText, position230)
				}
				{
					add(ruleAction38, position)
				}
				add(rulefield, position229)
			}
			return true
		l228:
			position, tokenIndex = position228, tokenIndex228
			return false
		},
		/* 18 posfield <- <(<fieldExpr> Action39)> */
		func() bool {
			position232, tokenIndex232 := position, tokenIndex
			{
				position233 := position
				{
					position234 := position
					if !_rules[rulefieldExpr]() {
						goto l232
					}
					add(rulePegText, position234)
				}
				{
					add(ruleAction39, position)
				}
				add(ruleposfield, position233)
			}
			return true
		l232:
			position, tokenIndex = position232, tokenIndex232
			return false
		},
		/* 19 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position236, tokenIndex236 := position, tokenIndex
			{
				position237 := position
				{
					position238, tokenIndex238 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l239
					}
					position++
				l240:
					{
						position241, tokenIndex241 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l241
						}
						position++
						goto l240
					l241:
						position, tokenIndex = position241, tokenIndex241
					}
					goto l238
				l239:
					position, tokenIndex = position238, tokenIndex238
					if buffer[position] != rune('0') {
						goto l236
					}
					position++
				}
			l238:
				add(ruleuint, position237)
			}
			return true
		l236:
			position, tokenIndex = position236, tokenIndex236
			return false
		},
		/* 20 uintrow <- <(<uint> Action40)> */
		nil,
		/* 21 uintcol <- <(<uint> Action41)> */
		func() bool {
			position243, tokenIndex243 := position, tokenIndex
			{
				position244 := position
				{
					position245 := position
					if !_rules[ruleuint]() {
						goto l243
					}
					add(rulePegText, position245)
				}
				{
					add(ruleAction41, position)
				}
				add(ruleuintcol, position244)
			}
			return true
		l243:
			position, tokenIndex = position243, tokenIndex243
			return false
		},
		/* 22 open <- <('(' sp)> */
		func() bool {
			position247, tokenIndex247 := position, tokenIndex
			{
				position248 := position
				if buffer[position] != rune('(') {
					goto l247
				}
				position++
				if !_rules[rulesp]() {
					goto l247
				}
				add(ruleopen, position248)
			}
			return true
		l247:
			position, tokenIndex = position247, tokenIndex247
			return false
		},
		/* 23 close <- <(')' sp)> */
		func() bool {
			position249, tokenIndex249 := position, tokenIndex
			{
				position250 := position
				if buffer[position] != rune(')') {
					goto l249
				}
				position++
				if !_rules[rulesp]() {
					goto l249
				}
				add(ruleclose, position250)
			}
			return true
		l249:
			position, tokenIndex = position249, tokenIndex249
			return false
		},
		/* 24 sp <- <(' ' / '\t')*> */
		func() bool {
			{
				position252 := position
			l253:
				{
					position254, tokenIndex254 := position, tokenIndex
					{
						position255, tokenIndex255 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l256
						}
						position++
						goto l255
					l256:
						position, tokenIndex = position255, tokenIndex255
						if buffer[position] != rune('\t') {
							goto l254
						}
						position++
					}
				l255:
					goto l253
				l254:
					position, tokenIndex = position254, tokenIndex254
				}
				add(rulesp, position252)
			}
			return true
		},
		/* 25 comma <- <(sp ',' whitesp)> */
		func() bool {
			position257, tokenIndex257 := position, tokenIndex
			{
				position258 := position
				if !_rules[rulesp]() {
					goto l257
				}
				if buffer[position] != rune(',') {
					goto l257
				}
				position++
				if !_rules[rulewhitesp]() {
					goto l257
				}
				add(rulecomma, position258)
			}
			return true
		l257:
			position, tokenIndex = position257, tokenIndex257
			return false
		},
		/* 26 lbrack <- <('[' sp)> */
		nil,
		/* 27 rbrack <- <(sp ']' sp)> */
		nil,
		/* 28 whitesp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position262 := position
			l263:
				{
					position264, tokenIndex264 := position, tokenIndex
					{
						position265, tokenIndex265 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l266
						}
						position++
						goto l265
					l266:
						position, tokenIndex = position265, tokenIndex265
						if buffer[position] != rune('\t') {
							goto l267
						}
						position++
						goto l265
					l267:
						position, tokenIndex = position265, tokenIndex265
						if buffer[position] != rune('\n') {
							goto l264
						}
						position++
					}
				l265:
					goto l263
				l264:
					position, tokenIndex = position264, tokenIndex264
				}
				add(rulewhitesp, position262)
			}
			return true
		},
		/* 29 IDENT <- <(!(('S' 'e' 't' '(') / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' '(') / ('S' 'e' 't' 'C' 'o' 'l' 'A' 't' 't' 'r' 's' '(') / ('C' 'l' 'e' 'a' 'r' '(') / ('T' 'o' 'p' 'N' '(') / ('R' 'a' 'n' 'g' 'e' '(')) ([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9])*)> */
		nil,
		/* 30 timestampbasicfmt <- <([0-9] [0-9] [0-9] [0-9] '-' ('0' / '1') [0-9] '-' [0-3] [0-9] 'T' [0-9] [0-9] ':' [0-9] [0-9])> */
		func() bool {
			position269, tokenIndex269 := position, tokenIndex
			{
				position270 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l269
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l269
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l269
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l269
				}
				position++
				if buffer[position] != rune('-') {
					goto l269
				}
				position++
				{
					position271, tokenIndex271 := position, tokenIndex
					if buffer[position] != rune('0') {
						goto l272
					}
					position++
					goto l271
				l272:
					position, tokenIndex = position271, tokenIndex271
					if buffer[position] != rune('1') {
						goto l269
					}
					position++
				}
			l271:
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l269
				}
				position++
				if buffer[position] != rune('-') {
					goto l269
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('3') {
					goto l269
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l269
				}
				position++
				if buffer[position] != rune('T') {
					goto l269
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l269
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l269
				}
				position++
				if buffer[position] != rune(':') {
					goto l269
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l269
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l269
				}
				position++
				add(ruletimestampbasicfmt, position270)
			}
			return true
		l269:
			position, tokenIndex = position269, tokenIndex269
			return false
		},
		/* 31 timestampfmt <- <(('"' timestampbasicfmt '"') / ('\'' timestampbasicfmt '\'') / timestampbasicfmt)> */
		func() bool {
			position273, tokenIndex273 := position, tokenIndex
			{
				position274 := position
				{
					position275, tokenIndex275 := position, tokenIndex
					if buffer[position] != rune('"') {
						goto l276
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l276
					}
					if buffer[position] != rune('"') {
						goto l276
					}
					position++
					goto l275
				l276:
					position, tokenIndex = position275, tokenIndex275
					if buffer[position] != rune('\'') {
						goto l277
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l277
					}
					if buffer[position] != rune('\'') {
						goto l277
					}
					position++
					goto l275
				l277:
					position, tokenIndex = position275, tokenIndex275
					if !_rules[ruletimestampbasicfmt]() {
						goto l273
					}
				}
			l275:
				add(ruletimestampfmt, position274)
			}
			return true
		l273:
			position, tokenIndex = position273, tokenIndex273
			return false
		},
		/* 32 timestamp <- <(<timestampfmt> Action42)> */
		nil,
		/* 34 Action0 <- <{p.startCall("Set")}> */
		nil,
		/* 35 Action1 <- <{p.endCall()}> */
		nil,
		/* 36 Action2 <- <{p.startCall("SetRowAttrs")}> */
		nil,
		/* 37 Action3 <- <{p.endCall()}> */
		nil,
		/* 38 Action4 <- <{p.startCall("SetColAttrs")}> */
		nil,
		/* 39 Action5 <- <{p.endCall()}> */
		nil,
		/* 40 Action6 <- <{p.startCall("Clear")}> */
		nil,
		/* 41 Action7 <- <{p.endCall()}> */
		nil,
		/* 42 Action8 <- <{p.startCall("TopN")}> */
		nil,
		/* 43 Action9 <- <{p.endCall()}> */
		nil,
		/* 44 Action10 <- <{p.startCall("Range")}> */
		nil,
		/* 45 Action11 <- <{p.endCall()}> */
		nil,
		nil,
		/* 47 Action12 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 48 Action13 <- <{ p.endCall() }> */
		nil,
		/* 49 Action14 <- <{ p.addBTWN() }> */
		nil,
		/* 50 Action15 <- <{ p.addLTE() }> */
		nil,
		/* 51 Action16 <- <{ p.addGTE() }> */
		nil,
		/* 52 Action17 <- <{ p.addEQ() }> */
		nil,
		/* 53 Action18 <- <{ p.addNEQ() }> */
		nil,
		/* 54 Action19 <- <{ p.addLT() }> */
		nil,
		/* 55 Action20 <- <{ p.addGT() }> */
		nil,
		/* 56 Action21 <- <{p.startConditional()}> */
		nil,
		/* 57 Action22 <- <{p.endConditional()}> */
		nil,
		/* 58 Action23 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 59 Action24 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 60 Action25 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 61 Action26 <- <{p.addPosStr("_start", buffer[begin:end])}> */
		nil,
		/* 62 Action27 <- <{p.addPosStr("_end", buffer[begin:end])}> */
		nil,
		/* 63 Action28 <- <{ p.startList() }> */
		nil,
		/* 64 Action29 <- <{ p.endList() }> */
		nil,
		/* 65 Action30 <- <{ p.addVal(nil) }> */
		nil,
		/* 66 Action31 <- <{ p.addVal(true) }> */
		nil,
		/* 67 Action32 <- <{ p.addVal(false) }> */
		nil,
		/* 68 Action33 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 69 Action34 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 70 Action35 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 71 Action36 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 72 Action37 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 73 Action38 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 74 Action39 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 75 Action40 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 76 Action41 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 77 Action42 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
