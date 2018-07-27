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
	rulerow
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
	ruleAction45
	ruleAction46
	ruleAction47
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
	"row",
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
	"Action45",
	"Action46",
	"Action47",
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
	rules  [84]func() bool
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
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction45:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction46:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction47:
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
		/* 1 Call <- <(('S' 'e' 't' Action0 open col comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma row comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'u' 'm' 'n' 'A' 't' 't' 'r' 's' Action4 open col comma args close Action5) / ('C' 'l' 'e' 'a' 'r' Action6 open col comma args close Action7) / ('T' 'o' 'p' 'N' Action8 open posfield (comma allargs)? close Action9) / ('R' 'a' 'n' 'g' 'e' Action10 open (timerange / conditional / arg) close Action11) / (<IDENT> Action12 open allargs comma? close Action13))> */
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
								add(ruleAction47, position)
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
							position19, tokenIndex19 := position, tokenIndex
							{
								position21 := position
								if !_rules[ruleuint]() {
									goto l20
								}
								add(rulePegText, position21)
							}
							{
								add(ruleAction44, position)
							}
							goto l19
						l20:
							position, tokenIndex = position19, tokenIndex19
							if buffer[position] != rune('\'') {
								goto l23
							}
							position++
							{
								position24 := position
								if !_rules[rulesinglequotedstring]() {
									goto l23
								}
								add(rulePegText, position24)
							}
							if buffer[position] != rune('\'') {
								goto l23
							}
							position++
							{
								add(ruleAction45, position)
							}
							goto l19
						l23:
							position, tokenIndex = position19, tokenIndex19
							if buffer[position] != rune('"') {
								goto l16
							}
							position++
							{
								position26 := position
								if !_rules[ruledoublequotedstring]() {
									goto l16
								}
								add(rulePegText, position26)
							}
							if buffer[position] != rune('"') {
								goto l16
							}
							position++
							{
								add(ruleAction46, position)
							}
						}
					l19:
						add(rulerow, position18)
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
						goto l29
					}
					position++
					if buffer[position] != rune('e') {
						goto l29
					}
					position++
					if buffer[position] != rune('t') {
						goto l29
					}
					position++
					if buffer[position] != rune('C') {
						goto l29
					}
					position++
					if buffer[position] != rune('o') {
						goto l29
					}
					position++
					if buffer[position] != rune('l') {
						goto l29
					}
					position++
					if buffer[position] != rune('u') {
						goto l29
					}
					position++
					if buffer[position] != rune('m') {
						goto l29
					}
					position++
					if buffer[position] != rune('n') {
						goto l29
					}
					position++
					if buffer[position] != rune('A') {
						goto l29
					}
					position++
					if buffer[position] != rune('t') {
						goto l29
					}
					position++
					if buffer[position] != rune('t') {
						goto l29
					}
					position++
					if buffer[position] != rune('r') {
						goto l29
					}
					position++
					if buffer[position] != rune('s') {
						goto l29
					}
					position++
					{
						add(ruleAction4, position)
					}
					if !_rules[ruleopen]() {
						goto l29
					}
					if !_rules[rulecol]() {
						goto l29
					}
					if !_rules[rulecomma]() {
						goto l29
					}
					if !_rules[ruleargs]() {
						goto l29
					}
					if !_rules[ruleclose]() {
						goto l29
					}
					{
						add(ruleAction5, position)
					}
					goto l7
				l29:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('C') {
						goto l32
					}
					position++
					if buffer[position] != rune('l') {
						goto l32
					}
					position++
					if buffer[position] != rune('e') {
						goto l32
					}
					position++
					if buffer[position] != rune('a') {
						goto l32
					}
					position++
					if buffer[position] != rune('r') {
						goto l32
					}
					position++
					{
						add(ruleAction6, position)
					}
					if !_rules[ruleopen]() {
						goto l32
					}
					if !_rules[rulecol]() {
						goto l32
					}
					if !_rules[rulecomma]() {
						goto l32
					}
					if !_rules[ruleargs]() {
						goto l32
					}
					if !_rules[ruleclose]() {
						goto l32
					}
					{
						add(ruleAction7, position)
					}
					goto l7
				l32:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('T') {
						goto l35
					}
					position++
					if buffer[position] != rune('o') {
						goto l35
					}
					position++
					if buffer[position] != rune('p') {
						goto l35
					}
					position++
					if buffer[position] != rune('N') {
						goto l35
					}
					position++
					{
						add(ruleAction8, position)
					}
					if !_rules[ruleopen]() {
						goto l35
					}
					if !_rules[ruleposfield]() {
						goto l35
					}
					{
						position37, tokenIndex37 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l37
						}
						if !_rules[ruleallargs]() {
							goto l37
						}
						goto l38
					l37:
						position, tokenIndex = position37, tokenIndex37
					}
				l38:
					if !_rules[ruleclose]() {
						goto l35
					}
					{
						add(ruleAction9, position)
					}
					goto l7
				l35:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('R') {
						goto l40
					}
					position++
					if buffer[position] != rune('a') {
						goto l40
					}
					position++
					if buffer[position] != rune('n') {
						goto l40
					}
					position++
					if buffer[position] != rune('g') {
						goto l40
					}
					position++
					if buffer[position] != rune('e') {
						goto l40
					}
					position++
					{
						add(ruleAction10, position)
					}
					if !_rules[ruleopen]() {
						goto l40
					}
					{
						position42, tokenIndex42 := position, tokenIndex
						{
							position44 := position
							if !_rules[rulefield]() {
								goto l43
							}
							if !_rules[rulesp]() {
								goto l43
							}
							if buffer[position] != rune('=') {
								goto l43
							}
							position++
							if !_rules[rulesp]() {
								goto l43
							}
							if !_rules[rulevalue]() {
								goto l43
							}
							if !_rules[rulecomma]() {
								goto l43
							}
							{
								position45 := position
								if !_rules[ruletimestampfmt]() {
									goto l43
								}
								add(rulePegText, position45)
							}
							{
								add(ruleAction26, position)
							}
							if !_rules[rulecomma]() {
								goto l43
							}
							{
								position47 := position
								if !_rules[ruletimestampfmt]() {
									goto l43
								}
								add(rulePegText, position47)
							}
							{
								add(ruleAction27, position)
							}
							add(ruletimerange, position44)
						}
						goto l42
					l43:
						position, tokenIndex = position42, tokenIndex42
						{
							position50 := position
							{
								add(ruleAction21, position)
							}
							if !_rules[rulecondint]() {
								goto l49
							}
							if !_rules[rulecondLT]() {
								goto l49
							}
							{
								position52 := position
								{
									position53 := position
									if !_rules[rulefieldExpr]() {
										goto l49
									}
									add(rulePegText, position53)
								}
								if !_rules[rulesp]() {
									goto l49
								}
								{
									add(ruleAction25, position)
								}
								add(rulecondfield, position52)
							}
							if !_rules[rulecondLT]() {
								goto l49
							}
							if !_rules[rulecondint]() {
								goto l49
							}
							{
								add(ruleAction22, position)
							}
							add(ruleconditional, position50)
						}
						goto l42
					l49:
						position, tokenIndex = position42, tokenIndex42
						if !_rules[rulearg]() {
							goto l40
						}
					}
				l42:
					if !_rules[ruleclose]() {
						goto l40
					}
					{
						add(ruleAction11, position)
					}
					goto l7
				l40:
					position, tokenIndex = position7, tokenIndex7
					{
						position57 := position
						{
							position58 := position
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
							add(ruleIDENT, position58)
						}
						add(rulePegText, position57)
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
						if !_rules[ruledoublequotedstring]() {
							goto l187
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
						position190 := position
						if !_rules[rulesinglequotedstring]() {
							goto l134
						}
						add(rulePegText, position190)
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
		func() bool {
			{
				position193 := position
			l194:
				{
					position195, tokenIndex195 := position, tokenIndex
					{
						position196, tokenIndex196 := position, tokenIndex
						{
							position198, tokenIndex198 := position, tokenIndex
							{
								position199, tokenIndex199 := position, tokenIndex
								if buffer[position] != rune('"') {
									goto l200
								}
								position++
								goto l199
							l200:
								position, tokenIndex = position199, tokenIndex199
								if buffer[position] != rune('\\') {
									goto l201
								}
								position++
								goto l199
							l201:
								position, tokenIndex = position199, tokenIndex199
								if buffer[position] != rune('\n') {
									goto l198
								}
								position++
							}
						l199:
							goto l197
						l198:
							position, tokenIndex = position198, tokenIndex198
						}
						if !matchDot() {
							goto l197
						}
						goto l196
					l197:
						position, tokenIndex = position196, tokenIndex196
						if buffer[position] != rune('\\') {
							goto l202
						}
						position++
						if buffer[position] != rune('n') {
							goto l202
						}
						position++
						goto l196
					l202:
						position, tokenIndex = position196, tokenIndex196
						if buffer[position] != rune('\\') {
							goto l203
						}
						position++
						if buffer[position] != rune('"') {
							goto l203
						}
						position++
						goto l196
					l203:
						position, tokenIndex = position196, tokenIndex196
						if buffer[position] != rune('\\') {
							goto l204
						}
						position++
						if buffer[position] != rune('\'') {
							goto l204
						}
						position++
						goto l196
					l204:
						position, tokenIndex = position196, tokenIndex196
						if buffer[position] != rune('\\') {
							goto l195
						}
						position++
						if buffer[position] != rune('\\') {
							goto l195
						}
						position++
					}
				l196:
					goto l194
				l195:
					position, tokenIndex = position195, tokenIndex195
				}
				add(ruledoublequotedstring, position193)
			}
			return true
		},
		/* 15 singlequotedstring <- <((!('\'' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		func() bool {
			{
				position206 := position
			l207:
				{
					position208, tokenIndex208 := position, tokenIndex
					{
						position209, tokenIndex209 := position, tokenIndex
						{
							position211, tokenIndex211 := position, tokenIndex
							{
								position212, tokenIndex212 := position, tokenIndex
								if buffer[position] != rune('\'') {
									goto l213
								}
								position++
								goto l212
							l213:
								position, tokenIndex = position212, tokenIndex212
								if buffer[position] != rune('\\') {
									goto l214
								}
								position++
								goto l212
							l214:
								position, tokenIndex = position212, tokenIndex212
								if buffer[position] != rune('\n') {
									goto l211
								}
								position++
							}
						l212:
							goto l210
						l211:
							position, tokenIndex = position211, tokenIndex211
						}
						if !matchDot() {
							goto l210
						}
						goto l209
					l210:
						position, tokenIndex = position209, tokenIndex209
						if buffer[position] != rune('\\') {
							goto l215
						}
						position++
						if buffer[position] != rune('n') {
							goto l215
						}
						position++
						goto l209
					l215:
						position, tokenIndex = position209, tokenIndex209
						if buffer[position] != rune('\\') {
							goto l216
						}
						position++
						if buffer[position] != rune('"') {
							goto l216
						}
						position++
						goto l209
					l216:
						position, tokenIndex = position209, tokenIndex209
						if buffer[position] != rune('\\') {
							goto l217
						}
						position++
						if buffer[position] != rune('\'') {
							goto l217
						}
						position++
						goto l209
					l217:
						position, tokenIndex = position209, tokenIndex209
						if buffer[position] != rune('\\') {
							goto l208
						}
						position++
						if buffer[position] != rune('\\') {
							goto l208
						}
						position++
					}
				l209:
					goto l207
				l208:
					position, tokenIndex = position208, tokenIndex208
				}
				add(rulesinglequotedstring, position206)
			}
			return true
		},
		/* 16 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_' / '-')*)> */
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
							goto l228
						}
						position++
						goto l224
					l228:
						position, tokenIndex = position224, tokenIndex224
						if buffer[position] != rune('-') {
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
		/* 17 field <- <(<(fieldExpr / reserved)> Action38)> */
		func() bool {
			position229, tokenIndex229 := position, tokenIndex
			{
				position230 := position
				{
					position231 := position
					{
						position232, tokenIndex232 := position, tokenIndex
						if !_rules[rulefieldExpr]() {
							goto l233
						}
						goto l232
					l233:
						position, tokenIndex = position232, tokenIndex232
						{
							position234 := position
							{
								position235, tokenIndex235 := position, tokenIndex
								if buffer[position] != rune('_') {
									goto l236
								}
								position++
								if buffer[position] != rune('r') {
									goto l236
								}
								position++
								if buffer[position] != rune('o') {
									goto l236
								}
								position++
								if buffer[position] != rune('w') {
									goto l236
								}
								position++
								goto l235
							l236:
								position, tokenIndex = position235, tokenIndex235
								if buffer[position] != rune('_') {
									goto l237
								}
								position++
								if buffer[position] != rune('c') {
									goto l237
								}
								position++
								if buffer[position] != rune('o') {
									goto l237
								}
								position++
								if buffer[position] != rune('l') {
									goto l237
								}
								position++
								goto l235
							l237:
								position, tokenIndex = position235, tokenIndex235
								if buffer[position] != rune('_') {
									goto l238
								}
								position++
								if buffer[position] != rune('s') {
									goto l238
								}
								position++
								if buffer[position] != rune('t') {
									goto l238
								}
								position++
								if buffer[position] != rune('a') {
									goto l238
								}
								position++
								if buffer[position] != rune('r') {
									goto l238
								}
								position++
								if buffer[position] != rune('t') {
									goto l238
								}
								position++
								goto l235
							l238:
								position, tokenIndex = position235, tokenIndex235
								if buffer[position] != rune('_') {
									goto l239
								}
								position++
								if buffer[position] != rune('e') {
									goto l239
								}
								position++
								if buffer[position] != rune('n') {
									goto l239
								}
								position++
								if buffer[position] != rune('d') {
									goto l239
								}
								position++
								goto l235
							l239:
								position, tokenIndex = position235, tokenIndex235
								if buffer[position] != rune('_') {
									goto l240
								}
								position++
								if buffer[position] != rune('t') {
									goto l240
								}
								position++
								if buffer[position] != rune('i') {
									goto l240
								}
								position++
								if buffer[position] != rune('m') {
									goto l240
								}
								position++
								if buffer[position] != rune('e') {
									goto l240
								}
								position++
								if buffer[position] != rune('s') {
									goto l240
								}
								position++
								if buffer[position] != rune('t') {
									goto l240
								}
								position++
								if buffer[position] != rune('a') {
									goto l240
								}
								position++
								if buffer[position] != rune('m') {
									goto l240
								}
								position++
								if buffer[position] != rune('p') {
									goto l240
								}
								position++
								goto l235
							l240:
								position, tokenIndex = position235, tokenIndex235
								if buffer[position] != rune('_') {
									goto l229
								}
								position++
								if buffer[position] != rune('f') {
									goto l229
								}
								position++
								if buffer[position] != rune('i') {
									goto l229
								}
								position++
								if buffer[position] != rune('e') {
									goto l229
								}
								position++
								if buffer[position] != rune('l') {
									goto l229
								}
								position++
								if buffer[position] != rune('d') {
									goto l229
								}
								position++
							}
						l235:
							add(rulereserved, position234)
						}
					}
				l232:
					add(rulePegText, position231)
				}
				{
					add(ruleAction38, position)
				}
				add(rulefield, position230)
			}
			return true
		l229:
			position, tokenIndex = position229, tokenIndex229
			return false
		},
		/* 18 reserved <- <(('_' 'r' 'o' 'w') / ('_' 'c' 'o' 'l') / ('_' 's' 't' 'a' 'r' 't') / ('_' 'e' 'n' 'd') / ('_' 't' 'i' 'm' 'e' 's' 't' 'a' 'm' 'p') / ('_' 'f' 'i' 'e' 'l' 'd'))> */
		nil,
		/* 19 posfield <- <(<fieldExpr> Action39)> */
		func() bool {
			position243, tokenIndex243 := position, tokenIndex
			{
				position244 := position
				{
					position245 := position
					if !_rules[rulefieldExpr]() {
						goto l243
					}
					add(rulePegText, position245)
				}
				{
					add(ruleAction39, position)
				}
				add(ruleposfield, position244)
			}
			return true
		l243:
			position, tokenIndex = position243, tokenIndex243
			return false
		},
		/* 20 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position247, tokenIndex247 := position, tokenIndex
			{
				position248 := position
				{
					position249, tokenIndex249 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l250
					}
					position++
				l251:
					{
						position252, tokenIndex252 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l252
						}
						position++
						goto l251
					l252:
						position, tokenIndex = position252, tokenIndex252
					}
					goto l249
				l250:
					position, tokenIndex = position249, tokenIndex249
					if buffer[position] != rune('0') {
						goto l247
					}
					position++
				}
			l249:
				add(ruleuint, position248)
			}
			return true
		l247:
			position, tokenIndex = position247, tokenIndex247
			return false
		},
		/* 21 uintrow <- <(<uint> Action40)> */
		nil,
		/* 22 col <- <((<uint> Action41) / ('\'' <singlequotedstring> '\'' Action42) / ('"' <doublequotedstring> '"' Action43))> */
		func() bool {
			position254, tokenIndex254 := position, tokenIndex
			{
				position255 := position
				{
					position256, tokenIndex256 := position, tokenIndex
					{
						position258 := position
						if !_rules[ruleuint]() {
							goto l257
						}
						add(rulePegText, position258)
					}
					{
						add(ruleAction41, position)
					}
					goto l256
				l257:
					position, tokenIndex = position256, tokenIndex256
					if buffer[position] != rune('\'') {
						goto l260
					}
					position++
					{
						position261 := position
						if !_rules[rulesinglequotedstring]() {
							goto l260
						}
						add(rulePegText, position261)
					}
					if buffer[position] != rune('\'') {
						goto l260
					}
					position++
					{
						add(ruleAction42, position)
					}
					goto l256
				l260:
					position, tokenIndex = position256, tokenIndex256
					if buffer[position] != rune('"') {
						goto l254
					}
					position++
					{
						position263 := position
						if !_rules[ruledoublequotedstring]() {
							goto l254
						}
						add(rulePegText, position263)
					}
					if buffer[position] != rune('"') {
						goto l254
					}
					position++
					{
						add(ruleAction43, position)
					}
				}
			l256:
				add(rulecol, position255)
			}
			return true
		l254:
			position, tokenIndex = position254, tokenIndex254
			return false
		},
		/* 23 row <- <((<uint> Action44) / ('\'' <singlequotedstring> '\'' Action45) / ('"' <doublequotedstring> '"' Action46))> */
		nil,
		/* 24 open <- <('(' sp)> */
		func() bool {
			position266, tokenIndex266 := position, tokenIndex
			{
				position267 := position
				if buffer[position] != rune('(') {
					goto l266
				}
				position++
				if !_rules[rulesp]() {
					goto l266
				}
				add(ruleopen, position267)
			}
			return true
		l266:
			position, tokenIndex = position266, tokenIndex266
			return false
		},
		/* 25 close <- <(')' sp)> */
		func() bool {
			position268, tokenIndex268 := position, tokenIndex
			{
				position269 := position
				if buffer[position] != rune(')') {
					goto l268
				}
				position++
				if !_rules[rulesp]() {
					goto l268
				}
				add(ruleclose, position269)
			}
			return true
		l268:
			position, tokenIndex = position268, tokenIndex268
			return false
		},
		/* 26 sp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position271 := position
			l272:
				{
					position273, tokenIndex273 := position, tokenIndex
					{
						position274, tokenIndex274 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l275
						}
						position++
						goto l274
					l275:
						position, tokenIndex = position274, tokenIndex274
						if buffer[position] != rune('\t') {
							goto l276
						}
						position++
						goto l274
					l276:
						position, tokenIndex = position274, tokenIndex274
						if buffer[position] != rune('\n') {
							goto l273
						}
						position++
					}
				l274:
					goto l272
				l273:
					position, tokenIndex = position273, tokenIndex273
				}
				add(rulesp, position271)
			}
			return true
		},
		/* 27 comma <- <(sp ',' sp)> */
		func() bool {
			position277, tokenIndex277 := position, tokenIndex
			{
				position278 := position
				if !_rules[rulesp]() {
					goto l277
				}
				if buffer[position] != rune(',') {
					goto l277
				}
				position++
				if !_rules[rulesp]() {
					goto l277
				}
				add(rulecomma, position278)
			}
			return true
		l277:
			position, tokenIndex = position277, tokenIndex277
			return false
		},
		/* 28 lbrack <- <('[' sp)> */
		nil,
		/* 29 rbrack <- <(sp ']' sp)> */
		nil,
		/* 30 IDENT <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9])*)> */
		nil,
		/* 31 timestampbasicfmt <- <([0-9] [0-9] [0-9] [0-9] '-' ('0' / '1') [0-9] '-' [0-3] [0-9] 'T' [0-9] [0-9] ':' [0-9] [0-9])> */
		func() bool {
			position282, tokenIndex282 := position, tokenIndex
			{
				position283 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l282
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l282
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l282
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l282
				}
				position++
				if buffer[position] != rune('-') {
					goto l282
				}
				position++
				{
					position284, tokenIndex284 := position, tokenIndex
					if buffer[position] != rune('0') {
						goto l285
					}
					position++
					goto l284
				l285:
					position, tokenIndex = position284, tokenIndex284
					if buffer[position] != rune('1') {
						goto l282
					}
					position++
				}
			l284:
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l282
				}
				position++
				if buffer[position] != rune('-') {
					goto l282
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('3') {
					goto l282
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l282
				}
				position++
				if buffer[position] != rune('T') {
					goto l282
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l282
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l282
				}
				position++
				if buffer[position] != rune(':') {
					goto l282
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l282
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l282
				}
				position++
				add(ruletimestampbasicfmt, position283)
			}
			return true
		l282:
			position, tokenIndex = position282, tokenIndex282
			return false
		},
		/* 32 timestampfmt <- <(('"' timestampbasicfmt '"') / ('\'' timestampbasicfmt '\'') / timestampbasicfmt)> */
		func() bool {
			position286, tokenIndex286 := position, tokenIndex
			{
				position287 := position
				{
					position288, tokenIndex288 := position, tokenIndex
					if buffer[position] != rune('"') {
						goto l289
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l289
					}
					if buffer[position] != rune('"') {
						goto l289
					}
					position++
					goto l288
				l289:
					position, tokenIndex = position288, tokenIndex288
					if buffer[position] != rune('\'') {
						goto l290
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l290
					}
					if buffer[position] != rune('\'') {
						goto l290
					}
					position++
					goto l288
				l290:
					position, tokenIndex = position288, tokenIndex288
					if !_rules[ruletimestampbasicfmt]() {
						goto l286
					}
				}
			l288:
				add(ruletimestampfmt, position287)
			}
			return true
		l286:
			position, tokenIndex = position286, tokenIndex286
			return false
		},
		/* 33 timestamp <- <(<timestampfmt> Action47)> */
		nil,
		/* 35 Action0 <- <{p.startCall("Set")}> */
		nil,
		/* 36 Action1 <- <{p.endCall()}> */
		nil,
		/* 37 Action2 <- <{p.startCall("SetRowAttrs")}> */
		nil,
		/* 38 Action3 <- <{p.endCall()}> */
		nil,
		/* 39 Action4 <- <{p.startCall("SetColumnAttrs")}> */
		nil,
		/* 40 Action5 <- <{p.endCall()}> */
		nil,
		/* 41 Action6 <- <{p.startCall("Clear")}> */
		nil,
		/* 42 Action7 <- <{p.endCall()}> */
		nil,
		/* 43 Action8 <- <{p.startCall("TopN")}> */
		nil,
		/* 44 Action9 <- <{p.endCall()}> */
		nil,
		/* 45 Action10 <- <{p.startCall("Range")}> */
		nil,
		/* 46 Action11 <- <{p.endCall()}> */
		nil,
		nil,
		/* 48 Action12 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 49 Action13 <- <{ p.endCall() }> */
		nil,
		/* 50 Action14 <- <{ p.addBTWN() }> */
		nil,
		/* 51 Action15 <- <{ p.addLTE() }> */
		nil,
		/* 52 Action16 <- <{ p.addGTE() }> */
		nil,
		/* 53 Action17 <- <{ p.addEQ() }> */
		nil,
		/* 54 Action18 <- <{ p.addNEQ() }> */
		nil,
		/* 55 Action19 <- <{ p.addLT() }> */
		nil,
		/* 56 Action20 <- <{ p.addGT() }> */
		nil,
		/* 57 Action21 <- <{p.startConditional()}> */
		nil,
		/* 58 Action22 <- <{p.endConditional()}> */
		nil,
		/* 59 Action23 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 60 Action24 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 61 Action25 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 62 Action26 <- <{p.addPosStr("_start", buffer[begin:end])}> */
		nil,
		/* 63 Action27 <- <{p.addPosStr("_end", buffer[begin:end])}> */
		nil,
		/* 64 Action28 <- <{ p.startList() }> */
		nil,
		/* 65 Action29 <- <{ p.endList() }> */
		nil,
		/* 66 Action30 <- <{ p.addVal(nil) }> */
		nil,
		/* 67 Action31 <- <{ p.addVal(true) }> */
		nil,
		/* 68 Action32 <- <{ p.addVal(false) }> */
		nil,
		/* 69 Action33 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 70 Action34 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 71 Action35 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 72 Action36 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 73 Action37 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 74 Action38 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 75 Action39 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 76 Action40 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 77 Action41 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 78 Action42 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 79 Action43 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 80 Action44 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 81 Action45 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 82 Action46 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 83 Action47 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
