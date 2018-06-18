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
	rules  [73]func() bool
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
			p.startList()
		case ruleAction27:
			p.endList()
		case ruleAction28:
			p.addVal(nil)
		case ruleAction29:
			p.addVal(true)
		case ruleAction30:
			p.addVal(false)
		case ruleAction31:
			p.addNumVal(buffer[begin:end])
		case ruleAction32:
			p.addNumVal(buffer[begin:end])
		case ruleAction33:
			p.addVal(buffer[begin:end])
		case ruleAction34:
			p.addVal(buffer[begin:end])
		case ruleAction35:
			p.addVal(buffer[begin:end])
		case ruleAction36:
			p.addField(buffer[begin:end])
		case ruleAction37:
			p.addPosStr("_field", buffer[begin:end])
		case ruleAction38:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction39:
			p.addPosNum("_col", buffer[begin:end])
		case ruleAction40:
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
		/* 1 Call <- <(('S' 'e' 't' Action0 open uintcol comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma uintrow comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'A' 't' 't' 'r' 's' Action4 open posfield comma uintcol comma args close Action5) / ('C' 'l' 'e' 'a' 'r' Action6 open uintcol comma args close Action7) / ('T' 'o' 'p' 'N' Action8 open posfield (comma allargs)? close Action9) / ('R' 'a' 'n' 'g' 'e' Action10 open (arg / conditional) close Action11) / (<IDENT> Action12 open allargs comma? close Action13))> */
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
								add(ruleAction40, position)
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
							add(ruleAction38, position)
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
						if !_rules[ruleallargs]() {
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
							if !_rules[rulecondint]() {
								goto l35
							}
							if !_rules[rulecondLT]() {
								goto l35
							}
							{
								position41 := position
								{
									position42 := position
									if !_rules[rulefieldExpr]() {
										goto l35
									}
									add(rulePegText, position42)
								}
								if !_rules[rulesp]() {
									goto l35
								}
								{
									add(ruleAction25, position)
								}
								add(rulecondfield, position41)
							}
							if !_rules[rulecondLT]() {
								goto l35
							}
							if !_rules[rulecondint]() {
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
						position46 := position
						{
							position47 := position
							{
								position48, tokenIndex48 := position, tokenIndex
								{
									position49, tokenIndex49 := position, tokenIndex
									if buffer[position] != rune('S') {
										goto l50
									}
									position++
									if buffer[position] != rune('e') {
										goto l50
									}
									position++
									if buffer[position] != rune('t') {
										goto l50
									}
									position++
									if buffer[position] != rune('(') {
										goto l50
									}
									position++
									goto l49
								l50:
									position, tokenIndex = position49, tokenIndex49
									if buffer[position] != rune('S') {
										goto l51
									}
									position++
									if buffer[position] != rune('e') {
										goto l51
									}
									position++
									if buffer[position] != rune('t') {
										goto l51
									}
									position++
									if buffer[position] != rune('R') {
										goto l51
									}
									position++
									if buffer[position] != rune('o') {
										goto l51
									}
									position++
									if buffer[position] != rune('w') {
										goto l51
									}
									position++
									if buffer[position] != rune('A') {
										goto l51
									}
									position++
									if buffer[position] != rune('t') {
										goto l51
									}
									position++
									if buffer[position] != rune('t') {
										goto l51
									}
									position++
									if buffer[position] != rune('r') {
										goto l51
									}
									position++
									if buffer[position] != rune('s') {
										goto l51
									}
									position++
									if buffer[position] != rune('(') {
										goto l51
									}
									position++
									goto l49
								l51:
									position, tokenIndex = position49, tokenIndex49
									if buffer[position] != rune('S') {
										goto l52
									}
									position++
									if buffer[position] != rune('e') {
										goto l52
									}
									position++
									if buffer[position] != rune('t') {
										goto l52
									}
									position++
									if buffer[position] != rune('C') {
										goto l52
									}
									position++
									if buffer[position] != rune('o') {
										goto l52
									}
									position++
									if buffer[position] != rune('l') {
										goto l52
									}
									position++
									if buffer[position] != rune('A') {
										goto l52
									}
									position++
									if buffer[position] != rune('t') {
										goto l52
									}
									position++
									if buffer[position] != rune('t') {
										goto l52
									}
									position++
									if buffer[position] != rune('r') {
										goto l52
									}
									position++
									if buffer[position] != rune('s') {
										goto l52
									}
									position++
									if buffer[position] != rune('(') {
										goto l52
									}
									position++
									goto l49
								l52:
									position, tokenIndex = position49, tokenIndex49
									if buffer[position] != rune('C') {
										goto l53
									}
									position++
									if buffer[position] != rune('l') {
										goto l53
									}
									position++
									if buffer[position] != rune('e') {
										goto l53
									}
									position++
									if buffer[position] != rune('a') {
										goto l53
									}
									position++
									if buffer[position] != rune('r') {
										goto l53
									}
									position++
									if buffer[position] != rune('(') {
										goto l53
									}
									position++
									goto l49
								l53:
									position, tokenIndex = position49, tokenIndex49
									if buffer[position] != rune('T') {
										goto l54
									}
									position++
									if buffer[position] != rune('o') {
										goto l54
									}
									position++
									if buffer[position] != rune('p') {
										goto l54
									}
									position++
									if buffer[position] != rune('N') {
										goto l54
									}
									position++
									if buffer[position] != rune('(') {
										goto l54
									}
									position++
									goto l49
								l54:
									position, tokenIndex = position49, tokenIndex49
									if buffer[position] != rune('R') {
										goto l48
									}
									position++
									if buffer[position] != rune('a') {
										goto l48
									}
									position++
									if buffer[position] != rune('n') {
										goto l48
									}
									position++
									if buffer[position] != rune('g') {
										goto l48
									}
									position++
									if buffer[position] != rune('e') {
										goto l48
									}
									position++
									if buffer[position] != rune('(') {
										goto l48
									}
									position++
								}
							l49:
								goto l5
							l48:
								position, tokenIndex = position48, tokenIndex48
							}
							{
								position55, tokenIndex55 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l56
								}
								position++
								goto l55
							l56:
								position, tokenIndex = position55, tokenIndex55
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l5
								}
								position++
							}
						l55:
						l57:
							{
								position58, tokenIndex58 := position, tokenIndex
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
										goto l61
									}
									position++
									goto l59
								l61:
									position, tokenIndex = position59, tokenIndex59
									if c := buffer[position]; c < rune('0') || c > rune('9') {
										goto l58
									}
									position++
								}
							l59:
								goto l57
							l58:
								position, tokenIndex = position58, tokenIndex58
							}
							add(ruleIDENT, position47)
						}
						add(rulePegText, position46)
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
						position63, tokenIndex63 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l63
						}
						goto l64
					l63:
						position, tokenIndex = position63, tokenIndex63
					}
				l64:
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
			position66, tokenIndex66 := position, tokenIndex
			{
				position67 := position
				{
					position68, tokenIndex68 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l69
					}
				l70:
					{
						position71, tokenIndex71 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l71
						}
						if !_rules[ruleCall]() {
							goto l71
						}
						goto l70
					l71:
						position, tokenIndex = position71, tokenIndex71
					}
					{
						position72, tokenIndex72 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l72
						}
						if !_rules[ruleargs]() {
							goto l72
						}
						goto l73
					l72:
						position, tokenIndex = position72, tokenIndex72
					}
				l73:
					goto l68
				l69:
					position, tokenIndex = position68, tokenIndex68
					if !_rules[ruleargs]() {
						goto l74
					}
					goto l68
				l74:
					position, tokenIndex = position68, tokenIndex68
					if !_rules[rulesp]() {
						goto l66
					}
				}
			l68:
				add(ruleallargs, position67)
			}
			return true
		l66:
			position, tokenIndex = position66, tokenIndex66
			return false
		},
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position75, tokenIndex75 := position, tokenIndex
			{
				position76 := position
				if !_rules[rulearg]() {
					goto l75
				}
				{
					position77, tokenIndex77 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l77
					}
					if !_rules[ruleargs]() {
						goto l77
					}
					goto l78
				l77:
					position, tokenIndex = position77, tokenIndex77
				}
			l78:
				if !_rules[rulesp]() {
					goto l75
				}
				add(ruleargs, position76)
			}
			return true
		l75:
			position, tokenIndex = position75, tokenIndex75
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value))> */
		func() bool {
			position79, tokenIndex79 := position, tokenIndex
			{
				position80 := position
				{
					position81, tokenIndex81 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l82
					}
					if !_rules[rulesp]() {
						goto l82
					}
					if buffer[position] != rune('=') {
						goto l82
					}
					position++
					if !_rules[rulesp]() {
						goto l82
					}
					if !_rules[rulevalue]() {
						goto l82
					}
					goto l81
				l82:
					position, tokenIndex = position81, tokenIndex81
					if !_rules[rulefield]() {
						goto l79
					}
					if !_rules[rulesp]() {
						goto l79
					}
					{
						position83 := position
						{
							position84, tokenIndex84 := position, tokenIndex
							if buffer[position] != rune('>') {
								goto l85
							}
							position++
							if buffer[position] != rune('<') {
								goto l85
							}
							position++
							{
								add(ruleAction14, position)
							}
							goto l84
						l85:
							position, tokenIndex = position84, tokenIndex84
							if buffer[position] != rune('<') {
								goto l87
							}
							position++
							if buffer[position] != rune('=') {
								goto l87
							}
							position++
							{
								add(ruleAction15, position)
							}
							goto l84
						l87:
							position, tokenIndex = position84, tokenIndex84
							if buffer[position] != rune('>') {
								goto l89
							}
							position++
							if buffer[position] != rune('=') {
								goto l89
							}
							position++
							{
								add(ruleAction16, position)
							}
							goto l84
						l89:
							position, tokenIndex = position84, tokenIndex84
							if buffer[position] != rune('=') {
								goto l91
							}
							position++
							if buffer[position] != rune('=') {
								goto l91
							}
							position++
							{
								add(ruleAction17, position)
							}
							goto l84
						l91:
							position, tokenIndex = position84, tokenIndex84
							if buffer[position] != rune('!') {
								goto l93
							}
							position++
							if buffer[position] != rune('=') {
								goto l93
							}
							position++
							{
								add(ruleAction18, position)
							}
							goto l84
						l93:
							position, tokenIndex = position84, tokenIndex84
							if buffer[position] != rune('<') {
								goto l95
							}
							position++
							{
								add(ruleAction19, position)
							}
							goto l84
						l95:
							position, tokenIndex = position84, tokenIndex84
							if buffer[position] != rune('>') {
								goto l79
							}
							position++
							{
								add(ruleAction20, position)
							}
						}
					l84:
						add(ruleCOND, position83)
					}
					if !_rules[rulesp]() {
						goto l79
					}
					if !_rules[rulevalue]() {
						goto l79
					}
				}
			l81:
				add(rulearg, position80)
			}
			return true
		l79:
			position, tokenIndex = position79, tokenIndex79
			return false
		},
		/* 5 COND <- <(('>' '<' Action14) / ('<' '=' Action15) / ('>' '=' Action16) / ('=' '=' Action17) / ('!' '=' Action18) / ('<' Action19) / ('>' Action20))> */
		nil,
		/* 6 conditional <- <(Action21 condint condLT condfield condLT condint Action22)> */
		nil,
		/* 7 condint <- <(<(('-'? [1-9] [0-9]*) / '0')> sp Action23)> */
		func() bool {
			position100, tokenIndex100 := position, tokenIndex
			{
				position101 := position
				{
					position102 := position
					{
						position103, tokenIndex103 := position, tokenIndex
						{
							position105, tokenIndex105 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l105
							}
							position++
							goto l106
						l105:
							position, tokenIndex = position105, tokenIndex105
						}
					l106:
						if c := buffer[position]; c < rune('1') || c > rune('9') {
							goto l104
						}
						position++
					l107:
						{
							position108, tokenIndex108 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l108
							}
							position++
							goto l107
						l108:
							position, tokenIndex = position108, tokenIndex108
						}
						goto l103
					l104:
						position, tokenIndex = position103, tokenIndex103
						if buffer[position] != rune('0') {
							goto l100
						}
						position++
					}
				l103:
					add(rulePegText, position102)
				}
				if !_rules[rulesp]() {
					goto l100
				}
				{
					add(ruleAction23, position)
				}
				add(rulecondint, position101)
			}
			return true
		l100:
			position, tokenIndex = position100, tokenIndex100
			return false
		},
		/* 8 condLT <- <(<(('<' '=') / '<')> sp Action24)> */
		func() bool {
			position110, tokenIndex110 := position, tokenIndex
			{
				position111 := position
				{
					position112 := position
					{
						position113, tokenIndex113 := position, tokenIndex
						if buffer[position] != rune('<') {
							goto l114
						}
						position++
						if buffer[position] != rune('=') {
							goto l114
						}
						position++
						goto l113
					l114:
						position, tokenIndex = position113, tokenIndex113
						if buffer[position] != rune('<') {
							goto l110
						}
						position++
					}
				l113:
					add(rulePegText, position112)
				}
				if !_rules[rulesp]() {
					goto l110
				}
				{
					add(ruleAction24, position)
				}
				add(rulecondLT, position111)
			}
			return true
		l110:
			position, tokenIndex = position110, tokenIndex110
			return false
		},
		/* 9 condfield <- <(<fieldExpr> sp Action25)> */
		nil,
		/* 10 value <- <(item / (lbrack Action26 list rbrack Action27))> */
		func() bool {
			position117, tokenIndex117 := position, tokenIndex
			{
				position118 := position
				{
					position119, tokenIndex119 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l120
					}
					goto l119
				l120:
					position, tokenIndex = position119, tokenIndex119
					{
						position121 := position
						if buffer[position] != rune('[') {
							goto l117
						}
						position++
						if !_rules[rulesp]() {
							goto l117
						}
						add(rulelbrack, position121)
					}
					{
						add(ruleAction26, position)
					}
					if !_rules[rulelist]() {
						goto l117
					}
					{
						position123 := position
						if !_rules[rulesp]() {
							goto l117
						}
						if buffer[position] != rune(']') {
							goto l117
						}
						position++
						if !_rules[rulesp]() {
							goto l117
						}
						add(rulerbrack, position123)
					}
					{
						add(ruleAction27, position)
					}
				}
			l119:
				add(rulevalue, position118)
			}
			return true
		l117:
			position, tokenIndex = position117, tokenIndex117
			return false
		},
		/* 11 list <- <(item (comma list)?)> */
		func() bool {
			position125, tokenIndex125 := position, tokenIndex
			{
				position126 := position
				if !_rules[ruleitem]() {
					goto l125
				}
				{
					position127, tokenIndex127 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l127
					}
					if !_rules[rulelist]() {
						goto l127
					}
					goto l128
				l127:
					position, tokenIndex = position127, tokenIndex127
				}
			l128:
				add(rulelist, position126)
			}
			return true
		l125:
			position, tokenIndex = position125, tokenIndex125
			return false
		},
		/* 12 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action28) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action29) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action30) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action31) / (<('-'? '.' [0-9]+)> Action32) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action33) / ('"' <doublequotedstring> '"' Action34) / ('\'' <singlequotedstring> '\'' Action35))> */
		func() bool {
			position129, tokenIndex129 := position, tokenIndex
			{
				position130 := position
				{
					position131, tokenIndex131 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l132
					}
					position++
					if buffer[position] != rune('u') {
						goto l132
					}
					position++
					if buffer[position] != rune('l') {
						goto l132
					}
					position++
					if buffer[position] != rune('l') {
						goto l132
					}
					position++
					{
						position133, tokenIndex133 := position, tokenIndex
						{
							position134, tokenIndex134 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l135
							}
							goto l134
						l135:
							position, tokenIndex = position134, tokenIndex134
							if !_rules[rulesp]() {
								goto l132
							}
							if !_rules[ruleclose]() {
								goto l132
							}
						}
					l134:
						position, tokenIndex = position133, tokenIndex133
					}
					{
						add(ruleAction28, position)
					}
					goto l131
				l132:
					position, tokenIndex = position131, tokenIndex131
					if buffer[position] != rune('t') {
						goto l137
					}
					position++
					if buffer[position] != rune('r') {
						goto l137
					}
					position++
					if buffer[position] != rune('u') {
						goto l137
					}
					position++
					if buffer[position] != rune('e') {
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
						add(ruleAction29, position)
					}
					goto l131
				l137:
					position, tokenIndex = position131, tokenIndex131
					if buffer[position] != rune('f') {
						goto l142
					}
					position++
					if buffer[position] != rune('a') {
						goto l142
					}
					position++
					if buffer[position] != rune('l') {
						goto l142
					}
					position++
					if buffer[position] != rune('s') {
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
						add(ruleAction30, position)
					}
					goto l131
				l142:
					position, tokenIndex = position131, tokenIndex131
					{
						position148 := position
						{
							position149, tokenIndex149 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l149
							}
							position++
							goto l150
						l149:
							position, tokenIndex = position149, tokenIndex149
						}
					l150:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l147
						}
						position++
					l151:
						{
							position152, tokenIndex152 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l152
							}
							position++
							goto l151
						l152:
							position, tokenIndex = position152, tokenIndex152
						}
						{
							position153, tokenIndex153 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l153
							}
							position++
						l155:
							{
								position156, tokenIndex156 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l156
								}
								position++
								goto l155
							l156:
								position, tokenIndex = position156, tokenIndex156
							}
							goto l154
						l153:
							position, tokenIndex = position153, tokenIndex153
						}
					l154:
						add(rulePegText, position148)
					}
					{
						add(ruleAction31, position)
					}
					goto l131
				l147:
					position, tokenIndex = position131, tokenIndex131
					{
						position159 := position
						{
							position160, tokenIndex160 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l160
							}
							position++
							goto l161
						l160:
							position, tokenIndex = position160, tokenIndex160
						}
					l161:
						if buffer[position] != rune('.') {
							goto l158
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l158
						}
						position++
					l162:
						{
							position163, tokenIndex163 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l163
							}
							position++
							goto l162
						l163:
							position, tokenIndex = position163, tokenIndex163
						}
						add(rulePegText, position159)
					}
					{
						add(ruleAction32, position)
					}
					goto l131
				l158:
					position, tokenIndex = position131, tokenIndex131
					{
						position166 := position
						{
							position169, tokenIndex169 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l170
							}
							position++
							goto l169
						l170:
							position, tokenIndex = position169, tokenIndex169
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l171
							}
							position++
							goto l169
						l171:
							position, tokenIndex = position169, tokenIndex169
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l172
							}
							position++
							goto l169
						l172:
							position, tokenIndex = position169, tokenIndex169
							if buffer[position] != rune('-') {
								goto l173
							}
							position++
							goto l169
						l173:
							position, tokenIndex = position169, tokenIndex169
							if buffer[position] != rune('_') {
								goto l174
							}
							position++
							goto l169
						l174:
							position, tokenIndex = position169, tokenIndex169
							if buffer[position] != rune(':') {
								goto l165
							}
							position++
						}
					l169:
					l167:
						{
							position168, tokenIndex168 := position, tokenIndex
							{
								position175, tokenIndex175 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l176
								}
								position++
								goto l175
							l176:
								position, tokenIndex = position175, tokenIndex175
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l177
								}
								position++
								goto l175
							l177:
								position, tokenIndex = position175, tokenIndex175
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l178
								}
								position++
								goto l175
							l178:
								position, tokenIndex = position175, tokenIndex175
								if buffer[position] != rune('-') {
									goto l179
								}
								position++
								goto l175
							l179:
								position, tokenIndex = position175, tokenIndex175
								if buffer[position] != rune('_') {
									goto l180
								}
								position++
								goto l175
							l180:
								position, tokenIndex = position175, tokenIndex175
								if buffer[position] != rune(':') {
									goto l168
								}
								position++
							}
						l175:
							goto l167
						l168:
							position, tokenIndex = position168, tokenIndex168
						}
						add(rulePegText, position166)
					}
					{
						add(ruleAction33, position)
					}
					goto l131
				l165:
					position, tokenIndex = position131, tokenIndex131
					if buffer[position] != rune('"') {
						goto l182
					}
					position++
					{
						position183 := position
						{
							position184 := position
						l185:
							{
								position186, tokenIndex186 := position, tokenIndex
								{
									position187, tokenIndex187 := position, tokenIndex
									{
										position189, tokenIndex189 := position, tokenIndex
										{
											position190, tokenIndex190 := position, tokenIndex
											if buffer[position] != rune('"') {
												goto l191
											}
											position++
											goto l190
										l191:
											position, tokenIndex = position190, tokenIndex190
											if buffer[position] != rune('\\') {
												goto l192
											}
											position++
											goto l190
										l192:
											position, tokenIndex = position190, tokenIndex190
											if buffer[position] != rune('\n') {
												goto l189
											}
											position++
										}
									l190:
										goto l188
									l189:
										position, tokenIndex = position189, tokenIndex189
									}
									if !matchDot() {
										goto l188
									}
									goto l187
								l188:
									position, tokenIndex = position187, tokenIndex187
									if buffer[position] != rune('\\') {
										goto l193
									}
									position++
									if buffer[position] != rune('n') {
										goto l193
									}
									position++
									goto l187
								l193:
									position, tokenIndex = position187, tokenIndex187
									if buffer[position] != rune('\\') {
										goto l194
									}
									position++
									if buffer[position] != rune('"') {
										goto l194
									}
									position++
									goto l187
								l194:
									position, tokenIndex = position187, tokenIndex187
									if buffer[position] != rune('\\') {
										goto l195
									}
									position++
									if buffer[position] != rune('\'') {
										goto l195
									}
									position++
									goto l187
								l195:
									position, tokenIndex = position187, tokenIndex187
									if buffer[position] != rune('\\') {
										goto l186
									}
									position++
									if buffer[position] != rune('\\') {
										goto l186
									}
									position++
								}
							l187:
								goto l185
							l186:
								position, tokenIndex = position186, tokenIndex186
							}
							add(ruledoublequotedstring, position184)
						}
						add(rulePegText, position183)
					}
					if buffer[position] != rune('"') {
						goto l182
					}
					position++
					{
						add(ruleAction34, position)
					}
					goto l131
				l182:
					position, tokenIndex = position131, tokenIndex131
					if buffer[position] != rune('\'') {
						goto l129
					}
					position++
					{
						position197 := position
						{
							position198 := position
						l199:
							{
								position200, tokenIndex200 := position, tokenIndex
								{
									position201, tokenIndex201 := position, tokenIndex
									{
										position203, tokenIndex203 := position, tokenIndex
										{
											position204, tokenIndex204 := position, tokenIndex
											if buffer[position] != rune('\'') {
												goto l205
											}
											position++
											goto l204
										l205:
											position, tokenIndex = position204, tokenIndex204
											if buffer[position] != rune('\\') {
												goto l206
											}
											position++
											goto l204
										l206:
											position, tokenIndex = position204, tokenIndex204
											if buffer[position] != rune('\n') {
												goto l203
											}
											position++
										}
									l204:
										goto l202
									l203:
										position, tokenIndex = position203, tokenIndex203
									}
									if !matchDot() {
										goto l202
									}
									goto l201
								l202:
									position, tokenIndex = position201, tokenIndex201
									if buffer[position] != rune('\\') {
										goto l207
									}
									position++
									if buffer[position] != rune('n') {
										goto l207
									}
									position++
									goto l201
								l207:
									position, tokenIndex = position201, tokenIndex201
									if buffer[position] != rune('\\') {
										goto l208
									}
									position++
									if buffer[position] != rune('"') {
										goto l208
									}
									position++
									goto l201
								l208:
									position, tokenIndex = position201, tokenIndex201
									if buffer[position] != rune('\\') {
										goto l209
									}
									position++
									if buffer[position] != rune('\'') {
										goto l209
									}
									position++
									goto l201
								l209:
									position, tokenIndex = position201, tokenIndex201
									if buffer[position] != rune('\\') {
										goto l200
									}
									position++
									if buffer[position] != rune('\\') {
										goto l200
									}
									position++
								}
							l201:
								goto l199
							l200:
								position, tokenIndex = position200, tokenIndex200
							}
							add(rulesinglequotedstring, position198)
						}
						add(rulePegText, position197)
					}
					if buffer[position] != rune('\'') {
						goto l129
					}
					position++
					{
						add(ruleAction35, position)
					}
				}
			l131:
				add(ruleitem, position130)
			}
			return true
		l129:
			position, tokenIndex = position129, tokenIndex129
			return false
		},
		/* 13 doublequotedstring <- <((!('"' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		nil,
		/* 14 singlequotedstring <- <((!('\'' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		nil,
		/* 15 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_')*)> */
		func() bool {
			position213, tokenIndex213 := position, tokenIndex
			{
				position214 := position
				{
					position215, tokenIndex215 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l216
					}
					position++
					goto l215
				l216:
					position, tokenIndex = position215, tokenIndex215
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l213
					}
					position++
				}
			l215:
			l217:
				{
					position218, tokenIndex218 := position, tokenIndex
					{
						position219, tokenIndex219 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l220
						}
						position++
						goto l219
					l220:
						position, tokenIndex = position219, tokenIndex219
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l221
						}
						position++
						goto l219
					l221:
						position, tokenIndex = position219, tokenIndex219
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l222
						}
						position++
						goto l219
					l222:
						position, tokenIndex = position219, tokenIndex219
						if buffer[position] != rune('_') {
							goto l218
						}
						position++
					}
				l219:
					goto l217
				l218:
					position, tokenIndex = position218, tokenIndex218
				}
				add(rulefieldExpr, position214)
			}
			return true
		l213:
			position, tokenIndex = position213, tokenIndex213
			return false
		},
		/* 16 field <- <(<fieldExpr> Action36)> */
		func() bool {
			position223, tokenIndex223 := position, tokenIndex
			{
				position224 := position
				{
					position225 := position
					if !_rules[rulefieldExpr]() {
						goto l223
					}
					add(rulePegText, position225)
				}
				{
					add(ruleAction36, position)
				}
				add(rulefield, position224)
			}
			return true
		l223:
			position, tokenIndex = position223, tokenIndex223
			return false
		},
		/* 17 posfield <- <(<fieldExpr> Action37)> */
		func() bool {
			position227, tokenIndex227 := position, tokenIndex
			{
				position228 := position
				{
					position229 := position
					if !_rules[rulefieldExpr]() {
						goto l227
					}
					add(rulePegText, position229)
				}
				{
					add(ruleAction37, position)
				}
				add(ruleposfield, position228)
			}
			return true
		l227:
			position, tokenIndex = position227, tokenIndex227
			return false
		},
		/* 18 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position231, tokenIndex231 := position, tokenIndex
			{
				position232 := position
				{
					position233, tokenIndex233 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l234
					}
					position++
				l235:
					{
						position236, tokenIndex236 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l236
						}
						position++
						goto l235
					l236:
						position, tokenIndex = position236, tokenIndex236
					}
					goto l233
				l234:
					position, tokenIndex = position233, tokenIndex233
					if buffer[position] != rune('0') {
						goto l231
					}
					position++
				}
			l233:
				add(ruleuint, position232)
			}
			return true
		l231:
			position, tokenIndex = position231, tokenIndex231
			return false
		},
		/* 19 uintrow <- <(<uint> Action38)> */
		nil,
		/* 20 uintcol <- <(<uint> Action39)> */
		func() bool {
			position238, tokenIndex238 := position, tokenIndex
			{
				position239 := position
				{
					position240 := position
					if !_rules[ruleuint]() {
						goto l238
					}
					add(rulePegText, position240)
				}
				{
					add(ruleAction39, position)
				}
				add(ruleuintcol, position239)
			}
			return true
		l238:
			position, tokenIndex = position238, tokenIndex238
			return false
		},
		/* 21 open <- <('(' sp)> */
		func() bool {
			position242, tokenIndex242 := position, tokenIndex
			{
				position243 := position
				if buffer[position] != rune('(') {
					goto l242
				}
				position++
				if !_rules[rulesp]() {
					goto l242
				}
				add(ruleopen, position243)
			}
			return true
		l242:
			position, tokenIndex = position242, tokenIndex242
			return false
		},
		/* 22 close <- <(')' sp)> */
		func() bool {
			position244, tokenIndex244 := position, tokenIndex
			{
				position245 := position
				if buffer[position] != rune(')') {
					goto l244
				}
				position++
				if !_rules[rulesp]() {
					goto l244
				}
				add(ruleclose, position245)
			}
			return true
		l244:
			position, tokenIndex = position244, tokenIndex244
			return false
		},
		/* 23 sp <- <(' ' / '\t')*> */
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
							goto l249
						}
						position++
					}
				l250:
					goto l248
				l249:
					position, tokenIndex = position249, tokenIndex249
				}
				add(rulesp, position247)
			}
			return true
		},
		/* 24 comma <- <(sp ',' whitesp)> */
		func() bool {
			position252, tokenIndex252 := position, tokenIndex
			{
				position253 := position
				if !_rules[rulesp]() {
					goto l252
				}
				if buffer[position] != rune(',') {
					goto l252
				}
				position++
				if !_rules[rulewhitesp]() {
					goto l252
				}
				add(rulecomma, position253)
			}
			return true
		l252:
			position, tokenIndex = position252, tokenIndex252
			return false
		},
		/* 25 lbrack <- <('[' sp)> */
		nil,
		/* 26 rbrack <- <(sp ']' sp)> */
		nil,
		/* 27 whitesp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position257 := position
			l258:
				{
					position259, tokenIndex259 := position, tokenIndex
					{
						position260, tokenIndex260 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l261
						}
						position++
						goto l260
					l261:
						position, tokenIndex = position260, tokenIndex260
						if buffer[position] != rune('\t') {
							goto l262
						}
						position++
						goto l260
					l262:
						position, tokenIndex = position260, tokenIndex260
						if buffer[position] != rune('\n') {
							goto l259
						}
						position++
					}
				l260:
					goto l258
				l259:
					position, tokenIndex = position259, tokenIndex259
				}
				add(rulewhitesp, position257)
			}
			return true
		},
		/* 28 IDENT <- <(!(('S' 'e' 't' '(') / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' '(') / ('S' 'e' 't' 'C' 'o' 'l' 'A' 't' 't' 'r' 's' '(') / ('C' 'l' 'e' 'a' 'r' '(') / ('T' 'o' 'p' 'N' '(') / ('R' 'a' 'n' 'g' 'e' '(')) ([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9])*)> */
		nil,
		/* 29 timestamp <- <(<([0-9] [0-9] [0-9] [0-9] '-' ('0' / '1') [0-9] '-' [0-3] [0-9] 'T' [0-9] [0-9] ':' [0-9] [0-9])> Action40)> */
		nil,
		/* 31 Action0 <- <{p.startCall("Set")}> */
		nil,
		/* 32 Action1 <- <{p.endCall()}> */
		nil,
		/* 33 Action2 <- <{p.startCall("SetRowAttrs")}> */
		nil,
		/* 34 Action3 <- <{p.endCall()}> */
		nil,
		/* 35 Action4 <- <{p.startCall("SetColAttrs")}> */
		nil,
		/* 36 Action5 <- <{p.endCall()}> */
		nil,
		/* 37 Action6 <- <{p.startCall("Clear")}> */
		nil,
		/* 38 Action7 <- <{p.endCall()}> */
		nil,
		/* 39 Action8 <- <{p.startCall("TopN")}> */
		nil,
		/* 40 Action9 <- <{p.endCall()}> */
		nil,
		/* 41 Action10 <- <{p.startCall("Range")}> */
		nil,
		/* 42 Action11 <- <{p.endCall()}> */
		nil,
		nil,
		/* 44 Action12 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 45 Action13 <- <{ p.endCall() }> */
		nil,
		/* 46 Action14 <- <{ p.addBTWN() }> */
		nil,
		/* 47 Action15 <- <{ p.addLTE() }> */
		nil,
		/* 48 Action16 <- <{ p.addGTE() }> */
		nil,
		/* 49 Action17 <- <{ p.addEQ() }> */
		nil,
		/* 50 Action18 <- <{ p.addNEQ() }> */
		nil,
		/* 51 Action19 <- <{ p.addLT() }> */
		nil,
		/* 52 Action20 <- <{ p.addGT() }> */
		nil,
		/* 53 Action21 <- <{p.startConditional()}> */
		nil,
		/* 54 Action22 <- <{p.endConditional()}> */
		nil,
		/* 55 Action23 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 56 Action24 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 57 Action25 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 58 Action26 <- <{ p.startList() }> */
		nil,
		/* 59 Action27 <- <{ p.endList() }> */
		nil,
		/* 60 Action28 <- <{ p.addVal(nil) }> */
		nil,
		/* 61 Action29 <- <{ p.addVal(true) }> */
		nil,
		/* 62 Action30 <- <{ p.addVal(false) }> */
		nil,
		/* 63 Action31 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 64 Action32 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 65 Action33 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 66 Action34 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 67 Action35 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 68 Action36 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 69 Action37 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 70 Action38 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 71 Action39 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 72 Action40 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
