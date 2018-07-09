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
	rulereserved
	ruleposfield
	ruleuint
	ruleuintrow
	rulecol
	ruleopen
	ruleclose
	rulesp
	rulecomma
	rulelbrack
	rulerbrack
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
	ruleAction43
	ruleAction44
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
	"reserved",
	"posfield",
	"uint",
	"uintrow",
	"col",
	"open",
	"close",
	"sp",
	"comma",
	"lbrack",
	"rbrack",
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
	"Action43",
	"Action44",
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
	rules  [80]func() bool
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
			p.startCall("SetColumnAttrs")
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
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction43:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction44:
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
		/* 0 Calls <- <(sp (Call sp)* !.)> */
		func() bool {
			position0, tokenIndex0 := position, tokenIndex
			{
				position1 := position
				if !_rules[rulesp]() {
					goto l0
				}
			l2:
				{
					position3, tokenIndex3 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l3
					}
					if !_rules[rulesp]() {
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
		/* 1 Call <- <(('S' 'e' 't' Action0 open col comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma uintrow comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'u' 'm' 'n' 'A' 't' 't' 'r' 's' Action4 open col comma args close Action5) / ('C' 'l' 'e' 'a' 'r' Action6 open col comma args close Action7) / ('T' 'o' 'p' 'N' Action8 open posfield (comma allargs)? close Action9) / ('R' 'a' 'n' 'g' 'e' Action10 open (timerange / conditional / arg) close Action11) / (<IDENT> Action12 open allargs comma? close Action13))> */
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
					if !_rules[rulecol]() {
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
								add(ruleAction44, position)
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
					if buffer[position] != rune('u') {
						goto l22
					}
					position++
					if buffer[position] != rune('m') {
						goto l22
					}
					position++
					if buffer[position] != rune('n') {
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
					if !_rules[rulecol]() {
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
					if !_rules[rulecol]() {
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
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l53
								}
								position++
								goto l52
							l53:
								position, tokenIndex = position52, tokenIndex52
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l5
								}
								position++
							}
						l52:
						l54:
							{
								position55, tokenIndex55 := position, tokenIndex
								{
									position56, tokenIndex56 := position, tokenIndex
									if c := buffer[position]; c < rune('a') || c > rune('z') {
										goto l57
									}
									position++
									goto l56
								l57:
									position, tokenIndex = position56, tokenIndex56
									if c := buffer[position]; c < rune('A') || c > rune('Z') {
										goto l58
									}
									position++
									goto l56
								l58:
									position, tokenIndex = position56, tokenIndex56
									if c := buffer[position]; c < rune('0') || c > rune('9') {
										goto l55
									}
									position++
								}
							l56:
								goto l54
							l55:
								position, tokenIndex = position55, tokenIndex55
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
						position60, tokenIndex60 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l60
						}
						goto l61
					l60:
						position, tokenIndex = position60, tokenIndex60
					}
				l61:
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
			position63, tokenIndex63 := position, tokenIndex
			{
				position64 := position
				{
					position65, tokenIndex65 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l66
					}
				l67:
					{
						position68, tokenIndex68 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l68
						}
						if !_rules[ruleCall]() {
							goto l68
						}
						goto l67
					l68:
						position, tokenIndex = position68, tokenIndex68
					}
					{
						position69, tokenIndex69 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l69
						}
						if !_rules[ruleargs]() {
							goto l69
						}
						goto l70
					l69:
						position, tokenIndex = position69, tokenIndex69
					}
				l70:
					goto l65
				l66:
					position, tokenIndex = position65, tokenIndex65
					if !_rules[ruleargs]() {
						goto l71
					}
					goto l65
				l71:
					position, tokenIndex = position65, tokenIndex65
					if !_rules[rulesp]() {
						goto l63
					}
				}
			l65:
				add(ruleallargs, position64)
			}
			return true
		l63:
			position, tokenIndex = position63, tokenIndex63
			return false
		},
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position72, tokenIndex72 := position, tokenIndex
			{
				position73 := position
				if !_rules[rulearg]() {
					goto l72
				}
				{
					position74, tokenIndex74 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l74
					}
					if !_rules[ruleargs]() {
						goto l74
					}
					goto l75
				l74:
					position, tokenIndex = position74, tokenIndex74
				}
			l75:
				if !_rules[rulesp]() {
					goto l72
				}
				add(ruleargs, position73)
			}
			return true
		l72:
			position, tokenIndex = position72, tokenIndex72
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value))> */
		func() bool {
			position76, tokenIndex76 := position, tokenIndex
			{
				position77 := position
				{
					position78, tokenIndex78 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l79
					}
					if !_rules[rulesp]() {
						goto l79
					}
					if buffer[position] != rune('=') {
						goto l79
					}
					position++
					if !_rules[rulesp]() {
						goto l79
					}
					if !_rules[rulevalue]() {
						goto l79
					}
					goto l78
				l79:
					position, tokenIndex = position78, tokenIndex78
					if !_rules[rulefield]() {
						goto l76
					}
					if !_rules[rulesp]() {
						goto l76
					}
					{
						position80 := position
						{
							position81, tokenIndex81 := position, tokenIndex
							if buffer[position] != rune('>') {
								goto l82
							}
							position++
							if buffer[position] != rune('<') {
								goto l82
							}
							position++
							{
								add(ruleAction14, position)
							}
							goto l81
						l82:
							position, tokenIndex = position81, tokenIndex81
							if buffer[position] != rune('<') {
								goto l84
							}
							position++
							if buffer[position] != rune('=') {
								goto l84
							}
							position++
							{
								add(ruleAction15, position)
							}
							goto l81
						l84:
							position, tokenIndex = position81, tokenIndex81
							if buffer[position] != rune('>') {
								goto l86
							}
							position++
							if buffer[position] != rune('=') {
								goto l86
							}
							position++
							{
								add(ruleAction16, position)
							}
							goto l81
						l86:
							position, tokenIndex = position81, tokenIndex81
							if buffer[position] != rune('=') {
								goto l88
							}
							position++
							if buffer[position] != rune('=') {
								goto l88
							}
							position++
							{
								add(ruleAction17, position)
							}
							goto l81
						l88:
							position, tokenIndex = position81, tokenIndex81
							if buffer[position] != rune('!') {
								goto l90
							}
							position++
							if buffer[position] != rune('=') {
								goto l90
							}
							position++
							{
								add(ruleAction18, position)
							}
							goto l81
						l90:
							position, tokenIndex = position81, tokenIndex81
							if buffer[position] != rune('<') {
								goto l92
							}
							position++
							{
								add(ruleAction19, position)
							}
							goto l81
						l92:
							position, tokenIndex = position81, tokenIndex81
							if buffer[position] != rune('>') {
								goto l76
							}
							position++
							{
								add(ruleAction20, position)
							}
						}
					l81:
						add(ruleCOND, position80)
					}
					if !_rules[rulesp]() {
						goto l76
					}
					if !_rules[rulevalue]() {
						goto l76
					}
				}
			l78:
				add(rulearg, position77)
			}
			return true
		l76:
			position, tokenIndex = position76, tokenIndex76
			return false
		},
		/* 5 COND <- <(('>' '<' Action14) / ('<' '=' Action15) / ('>' '=' Action16) / ('=' '=' Action17) / ('!' '=' Action18) / ('<' Action19) / ('>' Action20))> */
		nil,
		/* 6 conditional <- <(Action21 condint condLT condfield condLT condint Action22)> */
		nil,
		/* 7 condint <- <(<(('-'? [1-9] [0-9]*) / '0')> sp Action23)> */
		func() bool {
			position97, tokenIndex97 := position, tokenIndex
			{
				position98 := position
				{
					position99 := position
					{
						position100, tokenIndex100 := position, tokenIndex
						{
							position102, tokenIndex102 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l102
							}
							position++
							goto l103
						l102:
							position, tokenIndex = position102, tokenIndex102
						}
					l103:
						if c := buffer[position]; c < rune('1') || c > rune('9') {
							goto l101
						}
						position++
					l104:
						{
							position105, tokenIndex105 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l105
							}
							position++
							goto l104
						l105:
							position, tokenIndex = position105, tokenIndex105
						}
						goto l100
					l101:
						position, tokenIndex = position100, tokenIndex100
						if buffer[position] != rune('0') {
							goto l97
						}
						position++
					}
				l100:
					add(rulePegText, position99)
				}
				if !_rules[rulesp]() {
					goto l97
				}
				{
					add(ruleAction23, position)
				}
				add(rulecondint, position98)
			}
			return true
		l97:
			position, tokenIndex = position97, tokenIndex97
			return false
		},
		/* 8 condLT <- <(<(('<' '=') / '<')> sp Action24)> */
		func() bool {
			position107, tokenIndex107 := position, tokenIndex
			{
				position108 := position
				{
					position109 := position
					{
						position110, tokenIndex110 := position, tokenIndex
						if buffer[position] != rune('<') {
							goto l111
						}
						position++
						if buffer[position] != rune('=') {
							goto l111
						}
						position++
						goto l110
					l111:
						position, tokenIndex = position110, tokenIndex110
						if buffer[position] != rune('<') {
							goto l107
						}
						position++
					}
				l110:
					add(rulePegText, position109)
				}
				if !_rules[rulesp]() {
					goto l107
				}
				{
					add(ruleAction24, position)
				}
				add(rulecondLT, position108)
			}
			return true
		l107:
			position, tokenIndex = position107, tokenIndex107
			return false
		},
		/* 9 condfield <- <(<fieldExpr> sp Action25)> */
		nil,
		/* 10 timerange <- <(field sp '=' sp value comma <timestampfmt> Action26 comma <timestampfmt> Action27)> */
		nil,
		/* 11 value <- <(item / (lbrack Action28 list rbrack Action29))> */
		func() bool {
			position115, tokenIndex115 := position, tokenIndex
			{
				position116 := position
				{
					position117, tokenIndex117 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l118
					}
					goto l117
				l118:
					position, tokenIndex = position117, tokenIndex117
					{
						position119 := position
						if buffer[position] != rune('[') {
							goto l115
						}
						position++
						if !_rules[rulesp]() {
							goto l115
						}
						add(rulelbrack, position119)
					}
					{
						add(ruleAction28, position)
					}
					if !_rules[rulelist]() {
						goto l115
					}
					{
						position121 := position
						if !_rules[rulesp]() {
							goto l115
						}
						if buffer[position] != rune(']') {
							goto l115
						}
						position++
						if !_rules[rulesp]() {
							goto l115
						}
						add(rulerbrack, position121)
					}
					{
						add(ruleAction29, position)
					}
				}
			l117:
				add(rulevalue, position116)
			}
			return true
		l115:
			position, tokenIndex = position115, tokenIndex115
			return false
		},
		/* 12 list <- <(item (comma list)?)> */
		func() bool {
			position123, tokenIndex123 := position, tokenIndex
			{
				position124 := position
				if !_rules[ruleitem]() {
					goto l123
				}
				{
					position125, tokenIndex125 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l125
					}
					if !_rules[rulelist]() {
						goto l125
					}
					goto l126
				l125:
					position, tokenIndex = position125, tokenIndex125
				}
			l126:
				add(rulelist, position124)
			}
			return true
		l123:
			position, tokenIndex = position123, tokenIndex123
			return false
		},
		/* 13 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action30) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action31) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action32) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action33) / (<('-'? '.' [0-9]+)> Action34) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action35) / ('"' <doublequotedstring> '"' Action36) / ('\'' <singlequotedstring> '\'' Action37))> */
		func() bool {
			position127, tokenIndex127 := position, tokenIndex
			{
				position128 := position
				{
					position129, tokenIndex129 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l130
					}
					position++
					if buffer[position] != rune('u') {
						goto l130
					}
					position++
					if buffer[position] != rune('l') {
						goto l130
					}
					position++
					if buffer[position] != rune('l') {
						goto l130
					}
					position++
					{
						position131, tokenIndex131 := position, tokenIndex
						{
							position132, tokenIndex132 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l133
							}
							goto l132
						l133:
							position, tokenIndex = position132, tokenIndex132
							if !_rules[rulesp]() {
								goto l130
							}
							if !_rules[ruleclose]() {
								goto l130
							}
						}
					l132:
						position, tokenIndex = position131, tokenIndex131
					}
					{
						add(ruleAction30, position)
					}
					goto l129
				l130:
					position, tokenIndex = position129, tokenIndex129
					if buffer[position] != rune('t') {
						goto l135
					}
					position++
					if buffer[position] != rune('r') {
						goto l135
					}
					position++
					if buffer[position] != rune('u') {
						goto l135
					}
					position++
					if buffer[position] != rune('e') {
						goto l135
					}
					position++
					{
						position136, tokenIndex136 := position, tokenIndex
						{
							position137, tokenIndex137 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l138
							}
							goto l137
						l138:
							position, tokenIndex = position137, tokenIndex137
							if !_rules[rulesp]() {
								goto l135
							}
							if !_rules[ruleclose]() {
								goto l135
							}
						}
					l137:
						position, tokenIndex = position136, tokenIndex136
					}
					{
						add(ruleAction31, position)
					}
					goto l129
				l135:
					position, tokenIndex = position129, tokenIndex129
					if buffer[position] != rune('f') {
						goto l140
					}
					position++
					if buffer[position] != rune('a') {
						goto l140
					}
					position++
					if buffer[position] != rune('l') {
						goto l140
					}
					position++
					if buffer[position] != rune('s') {
						goto l140
					}
					position++
					if buffer[position] != rune('e') {
						goto l140
					}
					position++
					{
						position141, tokenIndex141 := position, tokenIndex
						{
							position142, tokenIndex142 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l143
							}
							goto l142
						l143:
							position, tokenIndex = position142, tokenIndex142
							if !_rules[rulesp]() {
								goto l140
							}
							if !_rules[ruleclose]() {
								goto l140
							}
						}
					l142:
						position, tokenIndex = position141, tokenIndex141
					}
					{
						add(ruleAction32, position)
					}
					goto l129
				l140:
					position, tokenIndex = position129, tokenIndex129
					{
						position146 := position
						{
							position147, tokenIndex147 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l147
							}
							position++
							goto l148
						l147:
							position, tokenIndex = position147, tokenIndex147
						}
					l148:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l145
						}
						position++
					l149:
						{
							position150, tokenIndex150 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l150
							}
							position++
							goto l149
						l150:
							position, tokenIndex = position150, tokenIndex150
						}
						{
							position151, tokenIndex151 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l151
							}
							position++
						l153:
							{
								position154, tokenIndex154 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l154
								}
								position++
								goto l153
							l154:
								position, tokenIndex = position154, tokenIndex154
							}
							goto l152
						l151:
							position, tokenIndex = position151, tokenIndex151
						}
					l152:
						add(rulePegText, position146)
					}
					{
						add(ruleAction33, position)
					}
					goto l129
				l145:
					position, tokenIndex = position129, tokenIndex129
					{
						position157 := position
						{
							position158, tokenIndex158 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l158
							}
							position++
							goto l159
						l158:
							position, tokenIndex = position158, tokenIndex158
						}
					l159:
						if buffer[position] != rune('.') {
							goto l156
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l156
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
						add(rulePegText, position157)
					}
					{
						add(ruleAction34, position)
					}
					goto l129
				l156:
					position, tokenIndex = position129, tokenIndex129
					{
						position164 := position
						{
							position167, tokenIndex167 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l168
							}
							position++
							goto l167
						l168:
							position, tokenIndex = position167, tokenIndex167
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l169
							}
							position++
							goto l167
						l169:
							position, tokenIndex = position167, tokenIndex167
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l170
							}
							position++
							goto l167
						l170:
							position, tokenIndex = position167, tokenIndex167
							if buffer[position] != rune('-') {
								goto l171
							}
							position++
							goto l167
						l171:
							position, tokenIndex = position167, tokenIndex167
							if buffer[position] != rune('_') {
								goto l172
							}
							position++
							goto l167
						l172:
							position, tokenIndex = position167, tokenIndex167
							if buffer[position] != rune(':') {
								goto l163
							}
							position++
						}
					l167:
					l165:
						{
							position166, tokenIndex166 := position, tokenIndex
							{
								position173, tokenIndex173 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l174
								}
								position++
								goto l173
							l174:
								position, tokenIndex = position173, tokenIndex173
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l175
								}
								position++
								goto l173
							l175:
								position, tokenIndex = position173, tokenIndex173
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l176
								}
								position++
								goto l173
							l176:
								position, tokenIndex = position173, tokenIndex173
								if buffer[position] != rune('-') {
									goto l177
								}
								position++
								goto l173
							l177:
								position, tokenIndex = position173, tokenIndex173
								if buffer[position] != rune('_') {
									goto l178
								}
								position++
								goto l173
							l178:
								position, tokenIndex = position173, tokenIndex173
								if buffer[position] != rune(':') {
									goto l166
								}
								position++
							}
						l173:
							goto l165
						l166:
							position, tokenIndex = position166, tokenIndex166
						}
						add(rulePegText, position164)
					}
					{
						add(ruleAction35, position)
					}
					goto l129
				l163:
					position, tokenIndex = position129, tokenIndex129
					if buffer[position] != rune('"') {
						goto l180
					}
					position++
					{
						position181 := position
						if !_rules[ruledoublequotedstring]() {
							goto l180
						}
						add(rulePegText, position181)
					}
					if buffer[position] != rune('"') {
						goto l180
					}
					position++
					{
						add(ruleAction36, position)
					}
					goto l129
				l180:
					position, tokenIndex = position129, tokenIndex129
					if buffer[position] != rune('\'') {
						goto l127
					}
					position++
					{
						position183 := position
						if !_rules[rulesinglequotedstring]() {
							goto l127
						}
						add(rulePegText, position183)
					}
					if buffer[position] != rune('\'') {
						goto l127
					}
					position++
					{
						add(ruleAction37, position)
					}
				}
			l129:
				add(ruleitem, position128)
			}
			return true
		l127:
			position, tokenIndex = position127, tokenIndex127
			return false
		},
		/* 14 doublequotedstring <- <((!('"' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		func() bool {
			{
				position186 := position
			l187:
				{
					position188, tokenIndex188 := position, tokenIndex
					{
						position189, tokenIndex189 := position, tokenIndex
						{
							position191, tokenIndex191 := position, tokenIndex
							{
								position192, tokenIndex192 := position, tokenIndex
								if buffer[position] != rune('"') {
									goto l193
								}
								position++
								goto l192
							l193:
								position, tokenIndex = position192, tokenIndex192
								if buffer[position] != rune('\\') {
									goto l194
								}
								position++
								goto l192
							l194:
								position, tokenIndex = position192, tokenIndex192
								if buffer[position] != rune('\n') {
									goto l191
								}
								position++
							}
						l192:
							goto l190
						l191:
							position, tokenIndex = position191, tokenIndex191
						}
						if !matchDot() {
							goto l190
						}
						goto l189
					l190:
						position, tokenIndex = position189, tokenIndex189
						if buffer[position] != rune('\\') {
							goto l195
						}
						position++
						if buffer[position] != rune('n') {
							goto l195
						}
						position++
						goto l189
					l195:
						position, tokenIndex = position189, tokenIndex189
						if buffer[position] != rune('\\') {
							goto l196
						}
						position++
						if buffer[position] != rune('"') {
							goto l196
						}
						position++
						goto l189
					l196:
						position, tokenIndex = position189, tokenIndex189
						if buffer[position] != rune('\\') {
							goto l197
						}
						position++
						if buffer[position] != rune('\'') {
							goto l197
						}
						position++
						goto l189
					l197:
						position, tokenIndex = position189, tokenIndex189
						if buffer[position] != rune('\\') {
							goto l188
						}
						position++
						if buffer[position] != rune('\\') {
							goto l188
						}
						position++
					}
				l189:
					goto l187
				l188:
					position, tokenIndex = position188, tokenIndex188
				}
				add(ruledoublequotedstring, position186)
			}
			return true
		},
		/* 15 singlequotedstring <- <((!('\'' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		func() bool {
			{
				position199 := position
			l200:
				{
					position201, tokenIndex201 := position, tokenIndex
					{
						position202, tokenIndex202 := position, tokenIndex
						{
							position204, tokenIndex204 := position, tokenIndex
							{
								position205, tokenIndex205 := position, tokenIndex
								if buffer[position] != rune('\'') {
									goto l206
								}
								position++
								goto l205
							l206:
								position, tokenIndex = position205, tokenIndex205
								if buffer[position] != rune('\\') {
									goto l207
								}
								position++
								goto l205
							l207:
								position, tokenIndex = position205, tokenIndex205
								if buffer[position] != rune('\n') {
									goto l204
								}
								position++
							}
						l205:
							goto l203
						l204:
							position, tokenIndex = position204, tokenIndex204
						}
						if !matchDot() {
							goto l203
						}
						goto l202
					l203:
						position, tokenIndex = position202, tokenIndex202
						if buffer[position] != rune('\\') {
							goto l208
						}
						position++
						if buffer[position] != rune('n') {
							goto l208
						}
						position++
						goto l202
					l208:
						position, tokenIndex = position202, tokenIndex202
						if buffer[position] != rune('\\') {
							goto l209
						}
						position++
						if buffer[position] != rune('"') {
							goto l209
						}
						position++
						goto l202
					l209:
						position, tokenIndex = position202, tokenIndex202
						if buffer[position] != rune('\\') {
							goto l210
						}
						position++
						if buffer[position] != rune('\'') {
							goto l210
						}
						position++
						goto l202
					l210:
						position, tokenIndex = position202, tokenIndex202
						if buffer[position] != rune('\\') {
							goto l201
						}
						position++
						if buffer[position] != rune('\\') {
							goto l201
						}
						position++
					}
				l202:
					goto l200
				l201:
					position, tokenIndex = position201, tokenIndex201
				}
				add(rulesinglequotedstring, position199)
			}
			return true
		},
		/* 16 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_' / '-')*)> */
		func() bool {
			position211, tokenIndex211 := position, tokenIndex
			{
				position212 := position
				{
					position213, tokenIndex213 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l214
					}
					position++
					goto l213
				l214:
					position, tokenIndex = position213, tokenIndex213
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l211
					}
					position++
				}
			l213:
			l215:
				{
					position216, tokenIndex216 := position, tokenIndex
					{
						position217, tokenIndex217 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l218
						}
						position++
						goto l217
					l218:
						position, tokenIndex = position217, tokenIndex217
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l219
						}
						position++
						goto l217
					l219:
						position, tokenIndex = position217, tokenIndex217
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l220
						}
						position++
						goto l217
					l220:
						position, tokenIndex = position217, tokenIndex217
						if buffer[position] != rune('_') {
							goto l221
						}
						position++
						goto l217
					l221:
						position, tokenIndex = position217, tokenIndex217
						if buffer[position] != rune('-') {
							goto l216
						}
						position++
					}
				l217:
					goto l215
				l216:
					position, tokenIndex = position216, tokenIndex216
				}
				add(rulefieldExpr, position212)
			}
			return true
		l211:
			position, tokenIndex = position211, tokenIndex211
			return false
		},
		/* 17 field <- <(<(fieldExpr / reserved)> Action38)> */
		func() bool {
			position222, tokenIndex222 := position, tokenIndex
			{
				position223 := position
				{
					position224 := position
					{
						position225, tokenIndex225 := position, tokenIndex
						if !_rules[rulefieldExpr]() {
							goto l226
						}
						goto l225
					l226:
						position, tokenIndex = position225, tokenIndex225
						{
							position227 := position
							{
								position228, tokenIndex228 := position, tokenIndex
								if buffer[position] != rune('_') {
									goto l229
								}
								position++
								if buffer[position] != rune('r') {
									goto l229
								}
								position++
								if buffer[position] != rune('o') {
									goto l229
								}
								position++
								if buffer[position] != rune('w') {
									goto l229
								}
								position++
								goto l228
							l229:
								position, tokenIndex = position228, tokenIndex228
								if buffer[position] != rune('_') {
									goto l230
								}
								position++
								if buffer[position] != rune('c') {
									goto l230
								}
								position++
								if buffer[position] != rune('o') {
									goto l230
								}
								position++
								if buffer[position] != rune('l') {
									goto l230
								}
								position++
								goto l228
							l230:
								position, tokenIndex = position228, tokenIndex228
								if buffer[position] != rune('_') {
									goto l231
								}
								position++
								if buffer[position] != rune('s') {
									goto l231
								}
								position++
								if buffer[position] != rune('t') {
									goto l231
								}
								position++
								if buffer[position] != rune('a') {
									goto l231
								}
								position++
								if buffer[position] != rune('r') {
									goto l231
								}
								position++
								if buffer[position] != rune('t') {
									goto l231
								}
								position++
								goto l228
							l231:
								position, tokenIndex = position228, tokenIndex228
								if buffer[position] != rune('_') {
									goto l232
								}
								position++
								if buffer[position] != rune('e') {
									goto l232
								}
								position++
								if buffer[position] != rune('n') {
									goto l232
								}
								position++
								if buffer[position] != rune('d') {
									goto l232
								}
								position++
								goto l228
							l232:
								position, tokenIndex = position228, tokenIndex228
								if buffer[position] != rune('_') {
									goto l233
								}
								position++
								if buffer[position] != rune('t') {
									goto l233
								}
								position++
								if buffer[position] != rune('i') {
									goto l233
								}
								position++
								if buffer[position] != rune('m') {
									goto l233
								}
								position++
								if buffer[position] != rune('e') {
									goto l233
								}
								position++
								if buffer[position] != rune('s') {
									goto l233
								}
								position++
								if buffer[position] != rune('t') {
									goto l233
								}
								position++
								if buffer[position] != rune('a') {
									goto l233
								}
								position++
								if buffer[position] != rune('m') {
									goto l233
								}
								position++
								if buffer[position] != rune('p') {
									goto l233
								}
								position++
								goto l228
							l233:
								position, tokenIndex = position228, tokenIndex228
								if buffer[position] != rune('_') {
									goto l222
								}
								position++
								if buffer[position] != rune('f') {
									goto l222
								}
								position++
								if buffer[position] != rune('i') {
									goto l222
								}
								position++
								if buffer[position] != rune('e') {
									goto l222
								}
								position++
								if buffer[position] != rune('l') {
									goto l222
								}
								position++
								if buffer[position] != rune('d') {
									goto l222
								}
								position++
							}
						l228:
							add(rulereserved, position227)
						}
					}
				l225:
					add(rulePegText, position224)
				}
				{
					add(ruleAction38, position)
				}
				add(rulefield, position223)
			}
			return true
		l222:
			position, tokenIndex = position222, tokenIndex222
			return false
		},
		/* 18 reserved <- <(('_' 'r' 'o' 'w') / ('_' 'c' 'o' 'l') / ('_' 's' 't' 'a' 'r' 't') / ('_' 'e' 'n' 'd') / ('_' 't' 'i' 'm' 'e' 's' 't' 'a' 'm' 'p') / ('_' 'f' 'i' 'e' 'l' 'd'))> */
		nil,
		/* 19 posfield <- <(<fieldExpr> Action39)> */
		func() bool {
			position236, tokenIndex236 := position, tokenIndex
			{
				position237 := position
				{
					position238 := position
					if !_rules[rulefieldExpr]() {
						goto l236
					}
					add(rulePegText, position238)
				}
				{
					add(ruleAction39, position)
				}
				add(ruleposfield, position237)
			}
			return true
		l236:
			position, tokenIndex = position236, tokenIndex236
			return false
		},
		/* 20 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position240, tokenIndex240 := position, tokenIndex
			{
				position241 := position
				{
					position242, tokenIndex242 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l243
					}
					position++
				l244:
					{
						position245, tokenIndex245 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l245
						}
						position++
						goto l244
					l245:
						position, tokenIndex = position245, tokenIndex245
					}
					goto l242
				l243:
					position, tokenIndex = position242, tokenIndex242
					if buffer[position] != rune('0') {
						goto l240
					}
					position++
				}
			l242:
				add(ruleuint, position241)
			}
			return true
		l240:
			position, tokenIndex = position240, tokenIndex240
			return false
		},
		/* 21 uintrow <- <(<uint> Action40)> */
		nil,
		/* 22 col <- <((<uint> Action41) / ('\'' <singlequotedstring> '\'' Action42) / ('"' <doublequotedstring> '"' Action43))> */
		func() bool {
			position247, tokenIndex247 := position, tokenIndex
			{
				position248 := position
				{
					position249, tokenIndex249 := position, tokenIndex
					{
						position251 := position
						if !_rules[ruleuint]() {
							goto l250
						}
						add(rulePegText, position251)
					}
					{
						add(ruleAction41, position)
					}
					goto l249
				l250:
					position, tokenIndex = position249, tokenIndex249
					if buffer[position] != rune('\'') {
						goto l253
					}
					position++
					{
						position254 := position
						if !_rules[rulesinglequotedstring]() {
							goto l253
						}
						add(rulePegText, position254)
					}
					if buffer[position] != rune('\'') {
						goto l253
					}
					position++
					{
						add(ruleAction42, position)
					}
					goto l249
				l253:
					position, tokenIndex = position249, tokenIndex249
					if buffer[position] != rune('"') {
						goto l247
					}
					position++
					{
						position256 := position
						if !_rules[ruledoublequotedstring]() {
							goto l247
						}
						add(rulePegText, position256)
					}
					if buffer[position] != rune('"') {
						goto l247
					}
					position++
					{
						add(ruleAction43, position)
					}
				}
			l249:
				add(rulecol, position248)
			}
			return true
		l247:
			position, tokenIndex = position247, tokenIndex247
			return false
		},
		/* 23 open <- <('(' sp)> */
		func() bool {
			position258, tokenIndex258 := position, tokenIndex
			{
				position259 := position
				if buffer[position] != rune('(') {
					goto l258
				}
				position++
				if !_rules[rulesp]() {
					goto l258
				}
				add(ruleopen, position259)
			}
			return true
		l258:
			position, tokenIndex = position258, tokenIndex258
			return false
		},
		/* 24 close <- <(')' sp)> */
		func() bool {
			position260, tokenIndex260 := position, tokenIndex
			{
				position261 := position
				if buffer[position] != rune(')') {
					goto l260
				}
				position++
				if !_rules[rulesp]() {
					goto l260
				}
				add(ruleclose, position261)
			}
			return true
		l260:
			position, tokenIndex = position260, tokenIndex260
			return false
		},
		/* 25 sp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position263 := position
			l264:
				{
					position265, tokenIndex265 := position, tokenIndex
					{
						position266, tokenIndex266 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l267
						}
						position++
						goto l266
					l267:
						position, tokenIndex = position266, tokenIndex266
						if buffer[position] != rune('\t') {
							goto l268
						}
						position++
						goto l266
					l268:
						position, tokenIndex = position266, tokenIndex266
						if buffer[position] != rune('\n') {
							goto l265
						}
						position++
					}
				l266:
					goto l264
				l265:
					position, tokenIndex = position265, tokenIndex265
				}
				add(rulesp, position263)
			}
			return true
		},
		/* 26 comma <- <(sp ',' sp)> */
		func() bool {
			position269, tokenIndex269 := position, tokenIndex
			{
				position270 := position
				if !_rules[rulesp]() {
					goto l269
				}
				if buffer[position] != rune(',') {
					goto l269
				}
				position++
				if !_rules[rulesp]() {
					goto l269
				}
				add(rulecomma, position270)
			}
			return true
		l269:
			position, tokenIndex = position269, tokenIndex269
			return false
		},
		/* 27 lbrack <- <('[' sp)> */
		nil,
		/* 28 rbrack <- <(sp ']' sp)> */
		nil,
		/* 29 IDENT <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9])*)> */
		nil,
		/* 30 timestampbasicfmt <- <([0-9] [0-9] [0-9] [0-9] '-' ('0' / '1') [0-9] '-' [0-3] [0-9] 'T' [0-9] [0-9] ':' [0-9] [0-9])> */
		func() bool {
			position274, tokenIndex274 := position, tokenIndex
			{
				position275 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l274
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l274
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l274
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l274
				}
				position++
				if buffer[position] != rune('-') {
					goto l274
				}
				position++
				{
					position276, tokenIndex276 := position, tokenIndex
					if buffer[position] != rune('0') {
						goto l277
					}
					position++
					goto l276
				l277:
					position, tokenIndex = position276, tokenIndex276
					if buffer[position] != rune('1') {
						goto l274
					}
					position++
				}
			l276:
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l274
				}
				position++
				if buffer[position] != rune('-') {
					goto l274
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('3') {
					goto l274
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l274
				}
				position++
				if buffer[position] != rune('T') {
					goto l274
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l274
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l274
				}
				position++
				if buffer[position] != rune(':') {
					goto l274
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l274
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l274
				}
				position++
				add(ruletimestampbasicfmt, position275)
			}
			return true
		l274:
			position, tokenIndex = position274, tokenIndex274
			return false
		},
		/* 31 timestampfmt <- <(('"' timestampbasicfmt '"') / ('\'' timestampbasicfmt '\'') / timestampbasicfmt)> */
		func() bool {
			position278, tokenIndex278 := position, tokenIndex
			{
				position279 := position
				{
					position280, tokenIndex280 := position, tokenIndex
					if buffer[position] != rune('"') {
						goto l281
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l281
					}
					if buffer[position] != rune('"') {
						goto l281
					}
					position++
					goto l280
				l281:
					position, tokenIndex = position280, tokenIndex280
					if buffer[position] != rune('\'') {
						goto l282
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l282
					}
					if buffer[position] != rune('\'') {
						goto l282
					}
					position++
					goto l280
				l282:
					position, tokenIndex = position280, tokenIndex280
					if !_rules[ruletimestampbasicfmt]() {
						goto l278
					}
				}
			l280:
				add(ruletimestampfmt, position279)
			}
			return true
		l278:
			position, tokenIndex = position278, tokenIndex278
			return false
		},
		/* 32 timestamp <- <(<timestampfmt> Action44)> */
		nil,
		/* 34 Action0 <- <{p.startCall("Set")}> */
		nil,
		/* 35 Action1 <- <{p.endCall()}> */
		nil,
		/* 36 Action2 <- <{p.startCall("SetRowAttrs")}> */
		nil,
		/* 37 Action3 <- <{p.endCall()}> */
		nil,
		/* 38 Action4 <- <{p.startCall("SetColumnAttrs")}> */
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
		/* 77 Action42 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 78 Action43 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 79 Action44 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
