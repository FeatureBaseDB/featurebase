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
	ruleopen
	rulevalue
	rulelist
	ruleitem
	ruledoublequotedstring
	rulesinglequotedstring
	rulefieldExpr
	rulefield
	ruleposfield
	ruleuint
	ruleint
	ruleuintrow
	ruleuintcol
	ruleclose
	rulesp
	rulecomma
	rulelbrack
	rulerbrack
	rulewhitesp
	ruleIDENT
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
	"open",
	"value",
	"list",
	"item",
	"doublequotedstring",
	"singlequotedstring",
	"fieldExpr",
	"field",
	"posfield",
	"uint",
	"int",
	"uintrow",
	"uintcol",
	"close",
	"sp",
	"comma",
	"lbrack",
	"rbrack",
	"whitesp",
	"IDENT",
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
	rules  [68]func() bool
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
			p.startCall("ClearBit")
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
			p.startList()
		case ruleAction24:
			p.endList()
		case ruleAction25:
			p.addVal(nil)
		case ruleAction26:
			p.addVal(true)
		case ruleAction27:
			p.addVal(false)
		case ruleAction28:
			p.addNumVal(buffer[begin:end])
		case ruleAction29:
			p.addNumVal(buffer[begin:end])
		case ruleAction30:
			p.addVal(buffer[begin:end])
		case ruleAction31:
			p.addVal(buffer[begin:end])
		case ruleAction32:
			p.addVal(buffer[begin:end])
		case ruleAction33:
			p.addField(buffer[begin:end])
		case ruleAction34:
			p.addPosStr("_field", buffer[begin:end])
		case ruleAction35:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction36:
			p.addPosNum("_col", buffer[begin:end])
		case ruleAction37:
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
		/* 1 Call <- <(('S' 'e' 't' Action0 open uintcol comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma uintrow comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'A' 't' 't' 'r' 's' Action4 open posfield comma uintcol comma args close Action5) / ('C' 'l' 'e' 'a' 'r' 'B' 'i' 't' Action6 open uintcol comma args close Action7) / ('T' 'o' 'p' 'N' Action8 open posfield (comma args)? close Action9) / ('R' 'a' 'n' 'g' 'e' Action10 open (arg / conditional) close Action11) / (<IDENT> Action12 open allargs comma? close Action13))> */
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
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l10
								}
								position++
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l10
								}
								position++
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l10
								}
								position++
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l10
								}
								position++
								if buffer[position] != rune('-') {
									goto l10
								}
								position++
								{
									position14, tokenIndex14 := position, tokenIndex
									if buffer[position] != rune('0') {
										goto l15
									}
									position++
									goto l14
								l15:
									position, tokenIndex = position14, tokenIndex14
									if buffer[position] != rune('1') {
										goto l10
									}
									position++
								}
							l14:
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l10
								}
								position++
								if buffer[position] != rune('-') {
									goto l10
								}
								position++
								if c := buffer[position]; c < rune('0') || c > rune('3') {
									goto l10
								}
								position++
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l10
								}
								position++
								if buffer[position] != rune('T') {
									goto l10
								}
								position++
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l10
								}
								position++
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l10
								}
								position++
								if buffer[position] != rune(':') {
									goto l10
								}
								position++
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l10
								}
								position++
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l10
								}
								position++
								add(rulePegText, position13)
							}
							{
								add(ruleAction37, position)
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
						goto l18
					}
					position++
					if buffer[position] != rune('e') {
						goto l18
					}
					position++
					if buffer[position] != rune('t') {
						goto l18
					}
					position++
					if buffer[position] != rune('R') {
						goto l18
					}
					position++
					if buffer[position] != rune('o') {
						goto l18
					}
					position++
					if buffer[position] != rune('w') {
						goto l18
					}
					position++
					if buffer[position] != rune('A') {
						goto l18
					}
					position++
					if buffer[position] != rune('t') {
						goto l18
					}
					position++
					if buffer[position] != rune('t') {
						goto l18
					}
					position++
					if buffer[position] != rune('r') {
						goto l18
					}
					position++
					if buffer[position] != rune('s') {
						goto l18
					}
					position++
					{
						add(ruleAction2, position)
					}
					if !_rules[ruleopen]() {
						goto l18
					}
					if !_rules[ruleposfield]() {
						goto l18
					}
					if !_rules[rulecomma]() {
						goto l18
					}
					{
						position20 := position
						{
							position21 := position
							if !_rules[ruleuint]() {
								goto l18
							}
							add(rulePegText, position21)
						}
						{
							add(ruleAction35, position)
						}
						add(ruleuintrow, position20)
					}
					if !_rules[rulecomma]() {
						goto l18
					}
					if !_rules[ruleargs]() {
						goto l18
					}
					if !_rules[ruleclose]() {
						goto l18
					}
					{
						add(ruleAction3, position)
					}
					goto l7
				l18:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('S') {
						goto l24
					}
					position++
					if buffer[position] != rune('e') {
						goto l24
					}
					position++
					if buffer[position] != rune('t') {
						goto l24
					}
					position++
					if buffer[position] != rune('C') {
						goto l24
					}
					position++
					if buffer[position] != rune('o') {
						goto l24
					}
					position++
					if buffer[position] != rune('l') {
						goto l24
					}
					position++
					if buffer[position] != rune('A') {
						goto l24
					}
					position++
					if buffer[position] != rune('t') {
						goto l24
					}
					position++
					if buffer[position] != rune('t') {
						goto l24
					}
					position++
					if buffer[position] != rune('r') {
						goto l24
					}
					position++
					if buffer[position] != rune('s') {
						goto l24
					}
					position++
					{
						add(ruleAction4, position)
					}
					if !_rules[ruleopen]() {
						goto l24
					}
					if !_rules[ruleposfield]() {
						goto l24
					}
					if !_rules[rulecomma]() {
						goto l24
					}
					if !_rules[ruleuintcol]() {
						goto l24
					}
					if !_rules[rulecomma]() {
						goto l24
					}
					if !_rules[ruleargs]() {
						goto l24
					}
					if !_rules[ruleclose]() {
						goto l24
					}
					{
						add(ruleAction5, position)
					}
					goto l7
				l24:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('C') {
						goto l27
					}
					position++
					if buffer[position] != rune('l') {
						goto l27
					}
					position++
					if buffer[position] != rune('e') {
						goto l27
					}
					position++
					if buffer[position] != rune('a') {
						goto l27
					}
					position++
					if buffer[position] != rune('r') {
						goto l27
					}
					position++
					if buffer[position] != rune('B') {
						goto l27
					}
					position++
					if buffer[position] != rune('i') {
						goto l27
					}
					position++
					if buffer[position] != rune('t') {
						goto l27
					}
					position++
					{
						add(ruleAction6, position)
					}
					if !_rules[ruleopen]() {
						goto l27
					}
					if !_rules[ruleuintcol]() {
						goto l27
					}
					if !_rules[rulecomma]() {
						goto l27
					}
					if !_rules[ruleargs]() {
						goto l27
					}
					if !_rules[ruleclose]() {
						goto l27
					}
					{
						add(ruleAction7, position)
					}
					goto l7
				l27:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('T') {
						goto l30
					}
					position++
					if buffer[position] != rune('o') {
						goto l30
					}
					position++
					if buffer[position] != rune('p') {
						goto l30
					}
					position++
					if buffer[position] != rune('N') {
						goto l30
					}
					position++
					{
						add(ruleAction8, position)
					}
					if !_rules[ruleopen]() {
						goto l30
					}
					if !_rules[ruleposfield]() {
						goto l30
					}
					{
						position32, tokenIndex32 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l32
						}
						if !_rules[ruleargs]() {
							goto l32
						}
						goto l33
					l32:
						position, tokenIndex = position32, tokenIndex32
					}
				l33:
					if !_rules[ruleclose]() {
						goto l30
					}
					{
						add(ruleAction9, position)
					}
					goto l7
				l30:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('R') {
						goto l35
					}
					position++
					if buffer[position] != rune('a') {
						goto l35
					}
					position++
					if buffer[position] != rune('n') {
						goto l35
					}
					position++
					if buffer[position] != rune('g') {
						goto l35
					}
					position++
					if buffer[position] != rune('e') {
						goto l35
					}
					position++
					{
						add(ruleAction10, position)
					}
					if !_rules[ruleopen]() {
						goto l35
					}
					{
						position37, tokenIndex37 := position, tokenIndex
						if !_rules[rulearg]() {
							goto l38
						}
						goto l37
					l38:
						position, tokenIndex = position37, tokenIndex37
						{
							position39 := position
							{
								add(ruleAction21, position)
							}
							if !_rules[ruleint]() {
								goto l35
							}
							{
								position41, tokenIndex41 := position, tokenIndex
								if buffer[position] != rune('<') {
									goto l42
								}
								position++
								if buffer[position] != rune('=') {
									goto l42
								}
								position++
								goto l41
							l42:
								position, tokenIndex = position41, tokenIndex41
								if buffer[position] != rune('<') {
									goto l35
								}
								position++
							}
						l41:
							if !_rules[rulefieldExpr]() {
								goto l35
							}
							{
								position43, tokenIndex43 := position, tokenIndex
								if buffer[position] != rune('<') {
									goto l44
								}
								position++
								if buffer[position] != rune('=') {
									goto l44
								}
								position++
								goto l43
							l44:
								position, tokenIndex = position43, tokenIndex43
								if buffer[position] != rune('<') {
									goto l35
								}
								position++
							}
						l43:
							if !_rules[ruleint]() {
								goto l35
							}
							{
								add(ruleAction22, position)
							}
							add(ruleconditional, position39)
						}
					}
				l37:
					if !_rules[ruleclose]() {
						goto l35
					}
					{
						add(ruleAction11, position)
					}
					goto l7
				l35:
					position, tokenIndex = position7, tokenIndex7
					{
						position47 := position
						{
							position48 := position
							{
								position49, tokenIndex49 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l50
								}
								position++
								goto l49
							l50:
								position, tokenIndex = position49, tokenIndex49
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l5
								}
								position++
							}
						l49:
						l51:
							{
								position52, tokenIndex52 := position, tokenIndex
								{
									position53, tokenIndex53 := position, tokenIndex
									if c := buffer[position]; c < rune('a') || c > rune('z') {
										goto l54
									}
									position++
									goto l53
								l54:
									position, tokenIndex = position53, tokenIndex53
									if c := buffer[position]; c < rune('A') || c > rune('Z') {
										goto l55
									}
									position++
									goto l53
								l55:
									position, tokenIndex = position53, tokenIndex53
									if c := buffer[position]; c < rune('0') || c > rune('9') {
										goto l56
									}
									position++
									goto l53
								l56:
									position, tokenIndex = position53, tokenIndex53
									if buffer[position] != rune('-') {
										goto l57
									}
									position++
									goto l53
								l57:
									position, tokenIndex = position53, tokenIndex53
									if buffer[position] != rune('_') {
										goto l58
									}
									position++
									goto l53
								l58:
									position, tokenIndex = position53, tokenIndex53
									if buffer[position] != rune('.') {
										goto l52
									}
									position++
								}
							l53:
								goto l51
							l52:
								position, tokenIndex = position52, tokenIndex52
							}
							add(ruleIDENT, position48)
						}
						add(rulePegText, position47)
					}
					{
						add(ruleAction12, position)
					}
					if !_rules[ruleopen]() {
						goto l5
					}
					{
						position60 := position
						{
							position61, tokenIndex61 := position, tokenIndex
							if !_rules[ruleCall]() {
								goto l62
							}
						l63:
							{
								position64, tokenIndex64 := position, tokenIndex
								if !_rules[rulecomma]() {
									goto l64
								}
								if !_rules[ruleCall]() {
									goto l64
								}
								goto l63
							l64:
								position, tokenIndex = position64, tokenIndex64
							}
							{
								position65, tokenIndex65 := position, tokenIndex
								if !_rules[rulecomma]() {
									goto l65
								}
								if !_rules[ruleargs]() {
									goto l65
								}
								goto l66
							l65:
								position, tokenIndex = position65, tokenIndex65
							}
						l66:
							goto l61
						l62:
							position, tokenIndex = position61, tokenIndex61
							{
								position68, tokenIndex68 := position, tokenIndex
								if !_rules[rulecomma]() {
									goto l68
								}
								goto l69
							l68:
								position, tokenIndex = position68, tokenIndex68
							}
						l69:
							if !_rules[ruleargs]() {
								goto l67
							}
							goto l61
						l67:
							position, tokenIndex = position61, tokenIndex61
							if !_rules[rulesp]() {
								goto l5
							}
						}
					l61:
						add(ruleallargs, position60)
					}
					{
						position70, tokenIndex70 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l70
						}
						goto l71
					l70:
						position, tokenIndex = position70, tokenIndex70
					}
				l71:
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
		/* 2 allargs <- <((Call (comma Call)* (comma args)?) / (comma? args) / sp)> */
		nil,
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position74, tokenIndex74 := position, tokenIndex
			{
				position75 := position
				if !_rules[rulearg]() {
					goto l74
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
				if !_rules[rulesp]() {
					goto l74
				}
				add(ruleargs, position75)
			}
			return true
		l74:
			position, tokenIndex = position74, tokenIndex74
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value))> */
		func() bool {
			position78, tokenIndex78 := position, tokenIndex
			{
				position79 := position
				{
					position80, tokenIndex80 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l81
					}
					if !_rules[rulesp]() {
						goto l81
					}
					if buffer[position] != rune('=') {
						goto l81
					}
					position++
					if !_rules[rulesp]() {
						goto l81
					}
					if !_rules[rulevalue]() {
						goto l81
					}
					goto l80
				l81:
					position, tokenIndex = position80, tokenIndex80
					if !_rules[rulefield]() {
						goto l78
					}
					if !_rules[rulesp]() {
						goto l78
					}
					{
						position82 := position
						{
							position83, tokenIndex83 := position, tokenIndex
							if buffer[position] != rune('>') {
								goto l84
							}
							position++
							if buffer[position] != rune('<') {
								goto l84
							}
							position++
							{
								add(ruleAction14, position)
							}
							goto l83
						l84:
							position, tokenIndex = position83, tokenIndex83
							if buffer[position] != rune('<') {
								goto l86
							}
							position++
							if buffer[position] != rune('=') {
								goto l86
							}
							position++
							{
								add(ruleAction15, position)
							}
							goto l83
						l86:
							position, tokenIndex = position83, tokenIndex83
							if buffer[position] != rune('>') {
								goto l88
							}
							position++
							if buffer[position] != rune('=') {
								goto l88
							}
							position++
							{
								add(ruleAction16, position)
							}
							goto l83
						l88:
							position, tokenIndex = position83, tokenIndex83
							if buffer[position] != rune('=') {
								goto l90
							}
							position++
							if buffer[position] != rune('=') {
								goto l90
							}
							position++
							{
								add(ruleAction17, position)
							}
							goto l83
						l90:
							position, tokenIndex = position83, tokenIndex83
							if buffer[position] != rune('!') {
								goto l92
							}
							position++
							if buffer[position] != rune('=') {
								goto l92
							}
							position++
							{
								add(ruleAction18, position)
							}
							goto l83
						l92:
							position, tokenIndex = position83, tokenIndex83
							if buffer[position] != rune('<') {
								goto l94
							}
							position++
							{
								add(ruleAction19, position)
							}
							goto l83
						l94:
							position, tokenIndex = position83, tokenIndex83
							if buffer[position] != rune('>') {
								goto l78
							}
							position++
							{
								add(ruleAction20, position)
							}
						}
					l83:
						add(ruleCOND, position82)
					}
					if !_rules[rulesp]() {
						goto l78
					}
					if !_rules[rulevalue]() {
						goto l78
					}
				}
			l80:
				add(rulearg, position79)
			}
			return true
		l78:
			position, tokenIndex = position78, tokenIndex78
			return false
		},
		/* 5 COND <- <(('>' '<' Action14) / ('<' '=' Action15) / ('>' '=' Action16) / ('=' '=' Action17) / ('!' '=' Action18) / ('<' Action19) / ('>' Action20))> */
		nil,
		/* 6 conditional <- <(Action21 int (('<' '=') / '<') fieldExpr (('<' '=') / '<') int Action22)> */
		nil,
		/* 7 open <- <('(' sp)> */
		func() bool {
			position99, tokenIndex99 := position, tokenIndex
			{
				position100 := position
				if buffer[position] != rune('(') {
					goto l99
				}
				position++
				if !_rules[rulesp]() {
					goto l99
				}
				add(ruleopen, position100)
			}
			return true
		l99:
			position, tokenIndex = position99, tokenIndex99
			return false
		},
		/* 8 value <- <(item / (lbrack Action23 list rbrack Action24))> */
		func() bool {
			position101, tokenIndex101 := position, tokenIndex
			{
				position102 := position
				{
					position103, tokenIndex103 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l104
					}
					goto l103
				l104:
					position, tokenIndex = position103, tokenIndex103
					{
						position105 := position
						if buffer[position] != rune('[') {
							goto l101
						}
						position++
						if !_rules[rulesp]() {
							goto l101
						}
						add(rulelbrack, position105)
					}
					{
						add(ruleAction23, position)
					}
					if !_rules[rulelist]() {
						goto l101
					}
					{
						position107 := position
						if !_rules[rulesp]() {
							goto l101
						}
						if buffer[position] != rune(']') {
							goto l101
						}
						position++
						if !_rules[rulesp]() {
							goto l101
						}
						add(rulerbrack, position107)
					}
					{
						add(ruleAction24, position)
					}
				}
			l103:
				add(rulevalue, position102)
			}
			return true
		l101:
			position, tokenIndex = position101, tokenIndex101
			return false
		},
		/* 9 list <- <(item (comma list)?)> */
		func() bool {
			position109, tokenIndex109 := position, tokenIndex
			{
				position110 := position
				if !_rules[ruleitem]() {
					goto l109
				}
				{
					position111, tokenIndex111 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l111
					}
					if !_rules[rulelist]() {
						goto l111
					}
					goto l112
				l111:
					position, tokenIndex = position111, tokenIndex111
				}
			l112:
				add(rulelist, position110)
			}
			return true
		l109:
			position, tokenIndex = position109, tokenIndex109
			return false
		},
		/* 10 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action25) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action26) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action27) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action28) / (<('-'? '.' [0-9]+)> Action29) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action30) / ('"' <doublequotedstring> '"' Action31) / ('\'' <singlequotedstring> '\'' Action32))> */
		func() bool {
			position113, tokenIndex113 := position, tokenIndex
			{
				position114 := position
				{
					position115, tokenIndex115 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l116
					}
					position++
					if buffer[position] != rune('u') {
						goto l116
					}
					position++
					if buffer[position] != rune('l') {
						goto l116
					}
					position++
					if buffer[position] != rune('l') {
						goto l116
					}
					position++
					{
						position117, tokenIndex117 := position, tokenIndex
						{
							position118, tokenIndex118 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l119
							}
							goto l118
						l119:
							position, tokenIndex = position118, tokenIndex118
							if !_rules[rulesp]() {
								goto l116
							}
							if !_rules[ruleclose]() {
								goto l116
							}
						}
					l118:
						position, tokenIndex = position117, tokenIndex117
					}
					{
						add(ruleAction25, position)
					}
					goto l115
				l116:
					position, tokenIndex = position115, tokenIndex115
					if buffer[position] != rune('t') {
						goto l121
					}
					position++
					if buffer[position] != rune('r') {
						goto l121
					}
					position++
					if buffer[position] != rune('u') {
						goto l121
					}
					position++
					if buffer[position] != rune('e') {
						goto l121
					}
					position++
					{
						position122, tokenIndex122 := position, tokenIndex
						{
							position123, tokenIndex123 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l124
							}
							goto l123
						l124:
							position, tokenIndex = position123, tokenIndex123
							if !_rules[rulesp]() {
								goto l121
							}
							if !_rules[ruleclose]() {
								goto l121
							}
						}
					l123:
						position, tokenIndex = position122, tokenIndex122
					}
					{
						add(ruleAction26, position)
					}
					goto l115
				l121:
					position, tokenIndex = position115, tokenIndex115
					if buffer[position] != rune('f') {
						goto l126
					}
					position++
					if buffer[position] != rune('a') {
						goto l126
					}
					position++
					if buffer[position] != rune('l') {
						goto l126
					}
					position++
					if buffer[position] != rune('s') {
						goto l126
					}
					position++
					if buffer[position] != rune('e') {
						goto l126
					}
					position++
					{
						position127, tokenIndex127 := position, tokenIndex
						{
							position128, tokenIndex128 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l129
							}
							goto l128
						l129:
							position, tokenIndex = position128, tokenIndex128
							if !_rules[rulesp]() {
								goto l126
							}
							if !_rules[ruleclose]() {
								goto l126
							}
						}
					l128:
						position, tokenIndex = position127, tokenIndex127
					}
					{
						add(ruleAction27, position)
					}
					goto l115
				l126:
					position, tokenIndex = position115, tokenIndex115
					{
						position132 := position
						{
							position133, tokenIndex133 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l133
							}
							position++
							goto l134
						l133:
							position, tokenIndex = position133, tokenIndex133
						}
					l134:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l131
						}
						position++
					l135:
						{
							position136, tokenIndex136 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l136
							}
							position++
							goto l135
						l136:
							position, tokenIndex = position136, tokenIndex136
						}
						{
							position137, tokenIndex137 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l137
							}
							position++
						l139:
							{
								position140, tokenIndex140 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l140
								}
								position++
								goto l139
							l140:
								position, tokenIndex = position140, tokenIndex140
							}
							goto l138
						l137:
							position, tokenIndex = position137, tokenIndex137
						}
					l138:
						add(rulePegText, position132)
					}
					{
						add(ruleAction28, position)
					}
					goto l115
				l131:
					position, tokenIndex = position115, tokenIndex115
					{
						position143 := position
						{
							position144, tokenIndex144 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l144
							}
							position++
							goto l145
						l144:
							position, tokenIndex = position144, tokenIndex144
						}
					l145:
						if buffer[position] != rune('.') {
							goto l142
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l142
						}
						position++
					l146:
						{
							position147, tokenIndex147 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l147
							}
							position++
							goto l146
						l147:
							position, tokenIndex = position147, tokenIndex147
						}
						add(rulePegText, position143)
					}
					{
						add(ruleAction29, position)
					}
					goto l115
				l142:
					position, tokenIndex = position115, tokenIndex115
					{
						position150 := position
						{
							position153, tokenIndex153 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l154
							}
							position++
							goto l153
						l154:
							position, tokenIndex = position153, tokenIndex153
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l155
							}
							position++
							goto l153
						l155:
							position, tokenIndex = position153, tokenIndex153
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l156
							}
							position++
							goto l153
						l156:
							position, tokenIndex = position153, tokenIndex153
							if buffer[position] != rune('-') {
								goto l157
							}
							position++
							goto l153
						l157:
							position, tokenIndex = position153, tokenIndex153
							if buffer[position] != rune('_') {
								goto l158
							}
							position++
							goto l153
						l158:
							position, tokenIndex = position153, tokenIndex153
							if buffer[position] != rune(':') {
								goto l149
							}
							position++
						}
					l153:
					l151:
						{
							position152, tokenIndex152 := position, tokenIndex
							{
								position159, tokenIndex159 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l160
								}
								position++
								goto l159
							l160:
								position, tokenIndex = position159, tokenIndex159
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l161
								}
								position++
								goto l159
							l161:
								position, tokenIndex = position159, tokenIndex159
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l162
								}
								position++
								goto l159
							l162:
								position, tokenIndex = position159, tokenIndex159
								if buffer[position] != rune('-') {
									goto l163
								}
								position++
								goto l159
							l163:
								position, tokenIndex = position159, tokenIndex159
								if buffer[position] != rune('_') {
									goto l164
								}
								position++
								goto l159
							l164:
								position, tokenIndex = position159, tokenIndex159
								if buffer[position] != rune(':') {
									goto l152
								}
								position++
							}
						l159:
							goto l151
						l152:
							position, tokenIndex = position152, tokenIndex152
						}
						add(rulePegText, position150)
					}
					{
						add(ruleAction30, position)
					}
					goto l115
				l149:
					position, tokenIndex = position115, tokenIndex115
					if buffer[position] != rune('"') {
						goto l166
					}
					position++
					{
						position167 := position
						{
							position168 := position
						l169:
							{
								position170, tokenIndex170 := position, tokenIndex
								{
									position171, tokenIndex171 := position, tokenIndex
									{
										position173, tokenIndex173 := position, tokenIndex
										{
											position174, tokenIndex174 := position, tokenIndex
											if buffer[position] != rune('"') {
												goto l175
											}
											position++
											goto l174
										l175:
											position, tokenIndex = position174, tokenIndex174
											if buffer[position] != rune('\\') {
												goto l176
											}
											position++
											goto l174
										l176:
											position, tokenIndex = position174, tokenIndex174
											if buffer[position] != rune('\n') {
												goto l173
											}
											position++
										}
									l174:
										goto l172
									l173:
										position, tokenIndex = position173, tokenIndex173
									}
									if !matchDot() {
										goto l172
									}
									goto l171
								l172:
									position, tokenIndex = position171, tokenIndex171
									if buffer[position] != rune('\\') {
										goto l177
									}
									position++
									if buffer[position] != rune('n') {
										goto l177
									}
									position++
									goto l171
								l177:
									position, tokenIndex = position171, tokenIndex171
									if buffer[position] != rune('\\') {
										goto l178
									}
									position++
									if buffer[position] != rune('"') {
										goto l178
									}
									position++
									goto l171
								l178:
									position, tokenIndex = position171, tokenIndex171
									if buffer[position] != rune('\\') {
										goto l179
									}
									position++
									if buffer[position] != rune('\'') {
										goto l179
									}
									position++
									goto l171
								l179:
									position, tokenIndex = position171, tokenIndex171
									if buffer[position] != rune('\\') {
										goto l170
									}
									position++
									if buffer[position] != rune('\\') {
										goto l170
									}
									position++
								}
							l171:
								goto l169
							l170:
								position, tokenIndex = position170, tokenIndex170
							}
							add(ruledoublequotedstring, position168)
						}
						add(rulePegText, position167)
					}
					if buffer[position] != rune('"') {
						goto l166
					}
					position++
					{
						add(ruleAction31, position)
					}
					goto l115
				l166:
					position, tokenIndex = position115, tokenIndex115
					if buffer[position] != rune('\'') {
						goto l113
					}
					position++
					{
						position181 := position
						{
							position182 := position
						l183:
							{
								position184, tokenIndex184 := position, tokenIndex
								{
									position185, tokenIndex185 := position, tokenIndex
									{
										position187, tokenIndex187 := position, tokenIndex
										{
											position188, tokenIndex188 := position, tokenIndex
											if buffer[position] != rune('\'') {
												goto l189
											}
											position++
											goto l188
										l189:
											position, tokenIndex = position188, tokenIndex188
											if buffer[position] != rune('\\') {
												goto l190
											}
											position++
											goto l188
										l190:
											position, tokenIndex = position188, tokenIndex188
											if buffer[position] != rune('\n') {
												goto l187
											}
											position++
										}
									l188:
										goto l186
									l187:
										position, tokenIndex = position187, tokenIndex187
									}
									if !matchDot() {
										goto l186
									}
									goto l185
								l186:
									position, tokenIndex = position185, tokenIndex185
									if buffer[position] != rune('\\') {
										goto l191
									}
									position++
									if buffer[position] != rune('n') {
										goto l191
									}
									position++
									goto l185
								l191:
									position, tokenIndex = position185, tokenIndex185
									if buffer[position] != rune('\\') {
										goto l192
									}
									position++
									if buffer[position] != rune('"') {
										goto l192
									}
									position++
									goto l185
								l192:
									position, tokenIndex = position185, tokenIndex185
									if buffer[position] != rune('\\') {
										goto l193
									}
									position++
									if buffer[position] != rune('\'') {
										goto l193
									}
									position++
									goto l185
								l193:
									position, tokenIndex = position185, tokenIndex185
									if buffer[position] != rune('\\') {
										goto l184
									}
									position++
									if buffer[position] != rune('\\') {
										goto l184
									}
									position++
								}
							l185:
								goto l183
							l184:
								position, tokenIndex = position184, tokenIndex184
							}
							add(rulesinglequotedstring, position182)
						}
						add(rulePegText, position181)
					}
					if buffer[position] != rune('\'') {
						goto l113
					}
					position++
					{
						add(ruleAction32, position)
					}
				}
			l115:
				add(ruleitem, position114)
			}
			return true
		l113:
			position, tokenIndex = position113, tokenIndex113
			return false
		},
		/* 11 doublequotedstring <- <((!('"' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		nil,
		/* 12 singlequotedstring <- <((!('\'' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		nil,
		/* 13 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_')*)> */
		func() bool {
			position197, tokenIndex197 := position, tokenIndex
			{
				position198 := position
				{
					position199, tokenIndex199 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l200
					}
					position++
					goto l199
				l200:
					position, tokenIndex = position199, tokenIndex199
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l197
					}
					position++
				}
			l199:
			l201:
				{
					position202, tokenIndex202 := position, tokenIndex
					{
						position203, tokenIndex203 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l204
						}
						position++
						goto l203
					l204:
						position, tokenIndex = position203, tokenIndex203
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l205
						}
						position++
						goto l203
					l205:
						position, tokenIndex = position203, tokenIndex203
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l206
						}
						position++
						goto l203
					l206:
						position, tokenIndex = position203, tokenIndex203
						if buffer[position] != rune('_') {
							goto l202
						}
						position++
					}
				l203:
					goto l201
				l202:
					position, tokenIndex = position202, tokenIndex202
				}
				add(rulefieldExpr, position198)
			}
			return true
		l197:
			position, tokenIndex = position197, tokenIndex197
			return false
		},
		/* 14 field <- <(<fieldExpr> Action33)> */
		func() bool {
			position207, tokenIndex207 := position, tokenIndex
			{
				position208 := position
				{
					position209 := position
					if !_rules[rulefieldExpr]() {
						goto l207
					}
					add(rulePegText, position209)
				}
				{
					add(ruleAction33, position)
				}
				add(rulefield, position208)
			}
			return true
		l207:
			position, tokenIndex = position207, tokenIndex207
			return false
		},
		/* 15 posfield <- <(<fieldExpr> Action34)> */
		func() bool {
			position211, tokenIndex211 := position, tokenIndex
			{
				position212 := position
				{
					position213 := position
					if !_rules[rulefieldExpr]() {
						goto l211
					}
					add(rulePegText, position213)
				}
				{
					add(ruleAction34, position)
				}
				add(ruleposfield, position212)
			}
			return true
		l211:
			position, tokenIndex = position211, tokenIndex211
			return false
		},
		/* 16 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position215, tokenIndex215 := position, tokenIndex
			{
				position216 := position
				{
					position217, tokenIndex217 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l218
					}
					position++
				l219:
					{
						position220, tokenIndex220 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l220
						}
						position++
						goto l219
					l220:
						position, tokenIndex = position220, tokenIndex220
					}
					goto l217
				l218:
					position, tokenIndex = position217, tokenIndex217
					if buffer[position] != rune('0') {
						goto l215
					}
					position++
				}
			l217:
				add(ruleuint, position216)
			}
			return true
		l215:
			position, tokenIndex = position215, tokenIndex215
			return false
		},
		/* 17 int <- <(('-'? [1-9] [0-9]*) / '0')> */
		func() bool {
			position221, tokenIndex221 := position, tokenIndex
			{
				position222 := position
				{
					position223, tokenIndex223 := position, tokenIndex
					{
						position225, tokenIndex225 := position, tokenIndex
						if buffer[position] != rune('-') {
							goto l225
						}
						position++
						goto l226
					l225:
						position, tokenIndex = position225, tokenIndex225
					}
				l226:
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l224
					}
					position++
				l227:
					{
						position228, tokenIndex228 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l228
						}
						position++
						goto l227
					l228:
						position, tokenIndex = position228, tokenIndex228
					}
					goto l223
				l224:
					position, tokenIndex = position223, tokenIndex223
					if buffer[position] != rune('0') {
						goto l221
					}
					position++
				}
			l223:
				add(ruleint, position222)
			}
			return true
		l221:
			position, tokenIndex = position221, tokenIndex221
			return false
		},
		/* 18 uintrow <- <(<uint> Action35)> */
		nil,
		/* 19 uintcol <- <(<uint> Action36)> */
		func() bool {
			position230, tokenIndex230 := position, tokenIndex
			{
				position231 := position
				{
					position232 := position
					if !_rules[ruleuint]() {
						goto l230
					}
					add(rulePegText, position232)
				}
				{
					add(ruleAction36, position)
				}
				add(ruleuintcol, position231)
			}
			return true
		l230:
			position, tokenIndex = position230, tokenIndex230
			return false
		},
		/* 20 close <- <(')' sp)> */
		func() bool {
			position234, tokenIndex234 := position, tokenIndex
			{
				position235 := position
				if buffer[position] != rune(')') {
					goto l234
				}
				position++
				if !_rules[rulesp]() {
					goto l234
				}
				add(ruleclose, position235)
			}
			return true
		l234:
			position, tokenIndex = position234, tokenIndex234
			return false
		},
		/* 21 sp <- <(' ' / '\t')*> */
		func() bool {
			{
				position237 := position
			l238:
				{
					position239, tokenIndex239 := position, tokenIndex
					{
						position240, tokenIndex240 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l241
						}
						position++
						goto l240
					l241:
						position, tokenIndex = position240, tokenIndex240
						if buffer[position] != rune('\t') {
							goto l239
						}
						position++
					}
				l240:
					goto l238
				l239:
					position, tokenIndex = position239, tokenIndex239
				}
				add(rulesp, position237)
			}
			return true
		},
		/* 22 comma <- <(sp ',' whitesp)> */
		func() bool {
			position242, tokenIndex242 := position, tokenIndex
			{
				position243 := position
				if !_rules[rulesp]() {
					goto l242
				}
				if buffer[position] != rune(',') {
					goto l242
				}
				position++
				if !_rules[rulewhitesp]() {
					goto l242
				}
				add(rulecomma, position243)
			}
			return true
		l242:
			position, tokenIndex = position242, tokenIndex242
			return false
		},
		/* 23 lbrack <- <('[' sp)> */
		nil,
		/* 24 rbrack <- <(sp ']' sp)> */
		nil,
		/* 25 whitesp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position247 := position
			l248:
				{
					position249, tokenIndex249 := position, tokenIndex
					{
						position250, tokenIndex250 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l251
						}
						position++
						goto l250
					l251:
						position, tokenIndex = position250, tokenIndex250
						if buffer[position] != rune('\t') {
							goto l252
						}
						position++
						goto l250
					l252:
						position, tokenIndex = position250, tokenIndex250
						if buffer[position] != rune('\n') {
							goto l249
						}
						position++
					}
				l250:
					goto l248
				l249:
					position, tokenIndex = position249, tokenIndex249
				}
				add(rulewhitesp, position247)
			}
			return true
		},
		/* 26 IDENT <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '-' / '_' / '.')*)> */
		nil,
		/* 27 timestamp <- <(<([0-9] [0-9] [0-9] [0-9] '-' ('0' / '1') [0-9] '-' [0-3] [0-9] 'T' [0-9] [0-9] ':' [0-9] [0-9])> Action37)> */
		nil,
		/* 29 Action0 <- <{p.startCall("Set")}> */
		nil,
		/* 30 Action1 <- <{p.endCall()}> */
		nil,
		/* 31 Action2 <- <{p.startCall("SetRowAttrs")}> */
		nil,
		/* 32 Action3 <- <{p.endCall()}> */
		nil,
		/* 33 Action4 <- <{p.startCall("SetColAttrs")}> */
		nil,
		/* 34 Action5 <- <{p.endCall()}> */
		nil,
		/* 35 Action6 <- <{p.startCall("ClearBit")}> */
		nil,
		/* 36 Action7 <- <{p.endCall()}> */
		nil,
		/* 37 Action8 <- <{p.startCall("TopN")}> */
		nil,
		/* 38 Action9 <- <{p.endCall()}> */
		nil,
		/* 39 Action10 <- <{p.startCall("Range")}> */
		nil,
		/* 40 Action11 <- <{p.endCall()}> */
		nil,
		nil,
		/* 42 Action12 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 43 Action13 <- <{ p.endCall() }> */
		nil,
		/* 44 Action14 <- <{ p.addBTWN() }> */
		nil,
		/* 45 Action15 <- <{ p.addLTE() }> */
		nil,
		/* 46 Action16 <- <{ p.addGTE() }> */
		nil,
		/* 47 Action17 <- <{ p.addEQ() }> */
		nil,
		/* 48 Action18 <- <{ p.addNEQ() }> */
		nil,
		/* 49 Action19 <- <{ p.addLT() }> */
		nil,
		/* 50 Action20 <- <{ p.addGT() }> */
		nil,
		/* 51 Action21 <- <{p.startConditional()}> */
		nil,
		/* 52 Action22 <- <{p.endConditional()}> */
		nil,
		/* 53 Action23 <- <{ p.startList() }> */
		nil,
		/* 54 Action24 <- <{ p.endList() }> */
		nil,
		/* 55 Action25 <- <{ p.addVal(nil) }> */
		nil,
		/* 56 Action26 <- <{ p.addVal(true) }> */
		nil,
		/* 57 Action27 <- <{ p.addVal(false) }> */
		nil,
		/* 58 Action28 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 59 Action29 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 60 Action30 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 61 Action31 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 62 Action32 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 63 Action33 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 64 Action34 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 65 Action35 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 66 Action36 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 67 Action37 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
