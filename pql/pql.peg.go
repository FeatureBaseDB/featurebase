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
	rulereserved
	ruleposfield
	ruleuint
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
	ruleAction12
	ruleAction13
	rulePegText
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
	ruleAction48
	ruleAction49
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
	"reserved",
	"posfield",
	"uint",
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
	"Action12",
	"Action13",
	"PegText",
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
	"Action48",
	"Action49",
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
			p.startCall("ClearRow")
		case ruleAction9:
			p.endCall()
		case ruleAction10:
			p.startCall("Store")
		case ruleAction11:
			p.endCall()
		case ruleAction12:
			p.startCall("TopN")
		case ruleAction13:
			p.endCall()
		case ruleAction14:
			p.startCall(buffer[begin:end])
		case ruleAction15:
			p.endCall()
		case ruleAction16:
			p.addBTWN()
		case ruleAction17:
			p.addLTE()
		case ruleAction18:
			p.addGTE()
		case ruleAction19:
			p.addEQ()
		case ruleAction20:
			p.addNEQ()
		case ruleAction21:
			p.addLT()
		case ruleAction22:
			p.addGT()
		case ruleAction23:
			p.startConditional()
		case ruleAction24:
			p.endConditional()
		case ruleAction25:
			p.condAdd(buffer[begin:end])
		case ruleAction26:
			p.condAdd(buffer[begin:end])
		case ruleAction27:
			p.condAdd(buffer[begin:end])
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
			p.addVal(buffer[begin:end])
		case ruleAction34:
			p.addNumVal(buffer[begin:end])
		case ruleAction35:
			p.addNumVal(buffer[begin:end])
		case ruleAction36:
			p.startCall(buffer[begin:end])
		case ruleAction37:
			p.addVal(p.endCall())
		case ruleAction38:
			p.addVal(buffer[begin:end])
		case ruleAction39:
			s, _ := strconv.Unquote(buffer[begin:end])
			p.addVal(s)
		case ruleAction40:
			p.addVal(buffer[begin:end])
		case ruleAction41:
			p.addField(buffer[begin:end])
		case ruleAction42:
			p.addPosStr("_field", buffer[begin:end])
		case ruleAction43:
			p.addPosNum("_col", buffer[begin:end])
		case ruleAction44:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction45:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction46:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction47:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction48:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction49:
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
		/* 1 Call <- <(('S' 'e' 't' Action0 open col comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma row comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'u' 'm' 'n' 'A' 't' 't' 'r' 's' Action4 open col comma args close Action5) / ('C' 'l' 'e' 'a' 'r' Action6 open col comma args close Action7) / ('C' 'l' 'e' 'a' 'r' 'R' 'o' 'w' Action8 open arg close Action9) / ('S' 't' 'o' 'r' 'e' Action10 open Call comma arg close Action11) / ('T' 'o' 'p' 'N' Action12 open posfield (comma allargs)? close Action13) / (<IDENT> Action14 open allargs comma? close Action15))> */
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
								add(ruleAction49, position)
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
								add(ruleAction46, position)
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
								add(ruleAction47, position)
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
								add(ruleAction48, position)
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
					if buffer[position] != rune('C') {
						goto l35
					}
					position++
					if buffer[position] != rune('l') {
						goto l35
					}
					position++
					if buffer[position] != rune('e') {
						goto l35
					}
					position++
					if buffer[position] != rune('a') {
						goto l35
					}
					position++
					if buffer[position] != rune('r') {
						goto l35
					}
					position++
					if buffer[position] != rune('R') {
						goto l35
					}
					position++
					if buffer[position] != rune('o') {
						goto l35
					}
					position++
					if buffer[position] != rune('w') {
						goto l35
					}
					position++
					{
						add(ruleAction8, position)
					}
					if !_rules[ruleopen]() {
						goto l35
					}
					if !_rules[rulearg]() {
						goto l35
					}
					if !_rules[ruleclose]() {
						goto l35
					}
					{
						add(ruleAction9, position)
					}
					goto l7
				l35:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('S') {
						goto l38
					}
					position++
					if buffer[position] != rune('t') {
						goto l38
					}
					position++
					if buffer[position] != rune('o') {
						goto l38
					}
					position++
					if buffer[position] != rune('r') {
						goto l38
					}
					position++
					if buffer[position] != rune('e') {
						goto l38
					}
					position++
					{
						add(ruleAction10, position)
					}
					if !_rules[ruleopen]() {
						goto l38
					}
					if !_rules[ruleCall]() {
						goto l38
					}
					if !_rules[rulecomma]() {
						goto l38
					}
					if !_rules[rulearg]() {
						goto l38
					}
					if !_rules[ruleclose]() {
						goto l38
					}
					{
						add(ruleAction11, position)
					}
					goto l7
				l38:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('T') {
						goto l41
					}
					position++
					if buffer[position] != rune('o') {
						goto l41
					}
					position++
					if buffer[position] != rune('p') {
						goto l41
					}
					position++
					if buffer[position] != rune('N') {
						goto l41
					}
					position++
					{
						add(ruleAction12, position)
					}
					if !_rules[ruleopen]() {
						goto l41
					}
					if !_rules[ruleposfield]() {
						goto l41
					}
					{
						position43, tokenIndex43 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l43
						}
						if !_rules[ruleallargs]() {
							goto l43
						}
						goto l44
					l43:
						position, tokenIndex = position43, tokenIndex43
					}
				l44:
					if !_rules[ruleclose]() {
						goto l41
					}
					{
						add(ruleAction13, position)
					}
					goto l7
				l41:
					position, tokenIndex = position7, tokenIndex7
					{
						position46 := position
						if !_rules[ruleIDENT]() {
							goto l5
						}
						add(rulePegText, position46)
					}
					{
						add(ruleAction14, position)
					}
					if !_rules[ruleopen]() {
						goto l5
					}
					if !_rules[ruleallargs]() {
						goto l5
					}
					{
						position48, tokenIndex48 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l48
						}
						goto l49
					l48:
						position, tokenIndex = position48, tokenIndex48
					}
				l49:
					if !_rules[ruleclose]() {
						goto l5
					}
					{
						add(ruleAction15, position)
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
			position51, tokenIndex51 := position, tokenIndex
			{
				position52 := position
				{
					position53, tokenIndex53 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l54
					}
				l55:
					{
						position56, tokenIndex56 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l56
						}
						if !_rules[ruleCall]() {
							goto l56
						}
						goto l55
					l56:
						position, tokenIndex = position56, tokenIndex56
					}
					{
						position57, tokenIndex57 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l57
						}
						if !_rules[ruleargs]() {
							goto l57
						}
						goto l58
					l57:
						position, tokenIndex = position57, tokenIndex57
					}
				l58:
					goto l53
				l54:
					position, tokenIndex = position53, tokenIndex53
					if !_rules[ruleargs]() {
						goto l59
					}
					goto l53
				l59:
					position, tokenIndex = position53, tokenIndex53
					if !_rules[rulesp]() {
						goto l51
					}
				}
			l53:
				add(ruleallargs, position52)
			}
			return true
		l51:
			position, tokenIndex = position51, tokenIndex51
			return false
		},
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position60, tokenIndex60 := position, tokenIndex
			{
				position61 := position
				if !_rules[rulearg]() {
					goto l60
				}
				{
					position62, tokenIndex62 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l62
					}
					if !_rules[ruleargs]() {
						goto l62
					}
					goto l63
				l62:
					position, tokenIndex = position62, tokenIndex62
				}
			l63:
				if !_rules[rulesp]() {
					goto l60
				}
				add(ruleargs, position61)
			}
			return true
		l60:
			position, tokenIndex = position60, tokenIndex60
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value) / conditional)> */
		func() bool {
			position64, tokenIndex64 := position, tokenIndex
			{
				position65 := position
				{
					position66, tokenIndex66 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l67
					}
					if !_rules[rulesp]() {
						goto l67
					}
					if buffer[position] != rune('=') {
						goto l67
					}
					position++
					if !_rules[rulesp]() {
						goto l67
					}
					if !_rules[rulevalue]() {
						goto l67
					}
					goto l66
				l67:
					position, tokenIndex = position66, tokenIndex66
					if !_rules[rulefield]() {
						goto l68
					}
					if !_rules[rulesp]() {
						goto l68
					}
					{
						position69 := position
						{
							position70, tokenIndex70 := position, tokenIndex
							if buffer[position] != rune('>') {
								goto l71
							}
							position++
							if buffer[position] != rune('<') {
								goto l71
							}
							position++
							{
								add(ruleAction16, position)
							}
							goto l70
						l71:
							position, tokenIndex = position70, tokenIndex70
							if buffer[position] != rune('<') {
								goto l73
							}
							position++
							if buffer[position] != rune('=') {
								goto l73
							}
							position++
							{
								add(ruleAction17, position)
							}
							goto l70
						l73:
							position, tokenIndex = position70, tokenIndex70
							if buffer[position] != rune('>') {
								goto l75
							}
							position++
							if buffer[position] != rune('=') {
								goto l75
							}
							position++
							{
								add(ruleAction18, position)
							}
							goto l70
						l75:
							position, tokenIndex = position70, tokenIndex70
							if buffer[position] != rune('=') {
								goto l77
							}
							position++
							if buffer[position] != rune('=') {
								goto l77
							}
							position++
							{
								add(ruleAction19, position)
							}
							goto l70
						l77:
							position, tokenIndex = position70, tokenIndex70
							if buffer[position] != rune('!') {
								goto l79
							}
							position++
							if buffer[position] != rune('=') {
								goto l79
							}
							position++
							{
								add(ruleAction20, position)
							}
							goto l70
						l79:
							position, tokenIndex = position70, tokenIndex70
							if buffer[position] != rune('<') {
								goto l81
							}
							position++
							{
								add(ruleAction21, position)
							}
							goto l70
						l81:
							position, tokenIndex = position70, tokenIndex70
							if buffer[position] != rune('>') {
								goto l68
							}
							position++
							{
								add(ruleAction22, position)
							}
						}
					l70:
						add(ruleCOND, position69)
					}
					if !_rules[rulesp]() {
						goto l68
					}
					if !_rules[rulevalue]() {
						goto l68
					}
					goto l66
				l68:
					position, tokenIndex = position66, tokenIndex66
					{
						position84 := position
						{
							add(ruleAction23, position)
						}
						if !_rules[rulecondint]() {
							goto l64
						}
						if !_rules[rulecondLT]() {
							goto l64
						}
						{
							position86 := position
							{
								position87 := position
								if !_rules[rulefieldExpr]() {
									goto l64
								}
								add(rulePegText, position87)
							}
							if !_rules[rulesp]() {
								goto l64
							}
							{
								add(ruleAction27, position)
							}
							add(rulecondfield, position86)
						}
						if !_rules[rulecondLT]() {
							goto l64
						}
						if !_rules[rulecondint]() {
							goto l64
						}
						{
							add(ruleAction24, position)
						}
						add(ruleconditional, position84)
					}
				}
			l66:
				add(rulearg, position65)
			}
			return true
		l64:
			position, tokenIndex = position64, tokenIndex64
			return false
		},
		/* 5 COND <- <(('>' '<' Action16) / ('<' '=' Action17) / ('>' '=' Action18) / ('=' '=' Action19) / ('!' '=' Action20) / ('<' Action21) / ('>' Action22))> */
		nil,
		/* 6 conditional <- <(Action23 condint condLT condfield condLT condint Action24)> */
		nil,
		/* 7 condint <- <(<(('-'? [1-9] [0-9]*) / '0')> sp Action25)> */
		func() bool {
			position92, tokenIndex92 := position, tokenIndex
			{
				position93 := position
				{
					position94 := position
					{
						position95, tokenIndex95 := position, tokenIndex
						{
							position97, tokenIndex97 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l97
							}
							position++
							goto l98
						l97:
							position, tokenIndex = position97, tokenIndex97
						}
					l98:
						if c := buffer[position]; c < rune('1') || c > rune('9') {
							goto l96
						}
						position++
					l99:
						{
							position100, tokenIndex100 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l100
							}
							position++
							goto l99
						l100:
							position, tokenIndex = position100, tokenIndex100
						}
						goto l95
					l96:
						position, tokenIndex = position95, tokenIndex95
						if buffer[position] != rune('0') {
							goto l92
						}
						position++
					}
				l95:
					add(rulePegText, position94)
				}
				if !_rules[rulesp]() {
					goto l92
				}
				{
					add(ruleAction25, position)
				}
				add(rulecondint, position93)
			}
			return true
		l92:
			position, tokenIndex = position92, tokenIndex92
			return false
		},
		/* 8 condLT <- <(<(('<' '=') / '<')> sp Action26)> */
		func() bool {
			position102, tokenIndex102 := position, tokenIndex
			{
				position103 := position
				{
					position104 := position
					{
						position105, tokenIndex105 := position, tokenIndex
						if buffer[position] != rune('<') {
							goto l106
						}
						position++
						if buffer[position] != rune('=') {
							goto l106
						}
						position++
						goto l105
					l106:
						position, tokenIndex = position105, tokenIndex105
						if buffer[position] != rune('<') {
							goto l102
						}
						position++
					}
				l105:
					add(rulePegText, position104)
				}
				if !_rules[rulesp]() {
					goto l102
				}
				{
					add(ruleAction26, position)
				}
				add(rulecondLT, position103)
			}
			return true
		l102:
			position, tokenIndex = position102, tokenIndex102
			return false
		},
		/* 9 condfield <- <(<fieldExpr> sp Action27)> */
		nil,
		/* 10 value <- <(item / (lbrack Action28 list rbrack Action29))> */
		func() bool {
			position109, tokenIndex109 := position, tokenIndex
			{
				position110 := position
				{
					position111, tokenIndex111 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l112
					}
					goto l111
				l112:
					position, tokenIndex = position111, tokenIndex111
					{
						position113 := position
						if buffer[position] != rune('[') {
							goto l109
						}
						position++
						if !_rules[rulesp]() {
							goto l109
						}
						add(rulelbrack, position113)
					}
					{
						add(ruleAction28, position)
					}
					if !_rules[rulelist]() {
						goto l109
					}
					{
						position115 := position
						if !_rules[rulesp]() {
							goto l109
						}
						if buffer[position] != rune(']') {
							goto l109
						}
						position++
						if !_rules[rulesp]() {
							goto l109
						}
						add(rulerbrack, position115)
					}
					{
						add(ruleAction29, position)
					}
				}
			l111:
				add(rulevalue, position110)
			}
			return true
		l109:
			position, tokenIndex = position109, tokenIndex109
			return false
		},
		/* 11 list <- <(item (comma list)?)> */
		func() bool {
			position117, tokenIndex117 := position, tokenIndex
			{
				position118 := position
				if !_rules[ruleitem]() {
					goto l117
				}
				{
					position119, tokenIndex119 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l119
					}
					if !_rules[rulelist]() {
						goto l119
					}
					goto l120
				l119:
					position, tokenIndex = position119, tokenIndex119
				}
			l120:
				add(rulelist, position118)
			}
			return true
		l117:
			position, tokenIndex = position117, tokenIndex117
			return false
		},
		/* 12 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action30) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action31) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action32) / (timestampfmt Action33) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action34) / (<('-'? '.' [0-9]+)> Action35) / (<IDENT> Action36 open allargs comma? close Action37) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action38) / (<('"' doublequotedstring '"')> Action39) / ('\'' <singlequotedstring> '\'' Action40))> */
		func() bool {
			position121, tokenIndex121 := position, tokenIndex
			{
				position122 := position
				{
					position123, tokenIndex123 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l124
					}
					position++
					if buffer[position] != rune('u') {
						goto l124
					}
					position++
					if buffer[position] != rune('l') {
						goto l124
					}
					position++
					if buffer[position] != rune('l') {
						goto l124
					}
					position++
					{
						position125, tokenIndex125 := position, tokenIndex
						{
							position126, tokenIndex126 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l127
							}
							goto l126
						l127:
							position, tokenIndex = position126, tokenIndex126
							if !_rules[rulesp]() {
								goto l124
							}
							if !_rules[ruleclose]() {
								goto l124
							}
						}
					l126:
						position, tokenIndex = position125, tokenIndex125
					}
					{
						add(ruleAction30, position)
					}
					goto l123
				l124:
					position, tokenIndex = position123, tokenIndex123
					if buffer[position] != rune('t') {
						goto l129
					}
					position++
					if buffer[position] != rune('r') {
						goto l129
					}
					position++
					if buffer[position] != rune('u') {
						goto l129
					}
					position++
					if buffer[position] != rune('e') {
						goto l129
					}
					position++
					{
						position130, tokenIndex130 := position, tokenIndex
						{
							position131, tokenIndex131 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l132
							}
							goto l131
						l132:
							position, tokenIndex = position131, tokenIndex131
							if !_rules[rulesp]() {
								goto l129
							}
							if !_rules[ruleclose]() {
								goto l129
							}
						}
					l131:
						position, tokenIndex = position130, tokenIndex130
					}
					{
						add(ruleAction31, position)
					}
					goto l123
				l129:
					position, tokenIndex = position123, tokenIndex123
					if buffer[position] != rune('f') {
						goto l134
					}
					position++
					if buffer[position] != rune('a') {
						goto l134
					}
					position++
					if buffer[position] != rune('l') {
						goto l134
					}
					position++
					if buffer[position] != rune('s') {
						goto l134
					}
					position++
					if buffer[position] != rune('e') {
						goto l134
					}
					position++
					{
						position135, tokenIndex135 := position, tokenIndex
						{
							position136, tokenIndex136 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l137
							}
							goto l136
						l137:
							position, tokenIndex = position136, tokenIndex136
							if !_rules[rulesp]() {
								goto l134
							}
							if !_rules[ruleclose]() {
								goto l134
							}
						}
					l136:
						position, tokenIndex = position135, tokenIndex135
					}
					{
						add(ruleAction32, position)
					}
					goto l123
				l134:
					position, tokenIndex = position123, tokenIndex123
					if !_rules[ruletimestampfmt]() {
						goto l139
					}
					{
						add(ruleAction33, position)
					}
					goto l123
				l139:
					position, tokenIndex = position123, tokenIndex123
					{
						position142 := position
						{
							position143, tokenIndex143 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l143
							}
							position++
							goto l144
						l143:
							position, tokenIndex = position143, tokenIndex143
						}
					l144:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l141
						}
						position++
					l145:
						{
							position146, tokenIndex146 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l146
							}
							position++
							goto l145
						l146:
							position, tokenIndex = position146, tokenIndex146
						}
						{
							position147, tokenIndex147 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l147
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
							goto l148
						l147:
							position, tokenIndex = position147, tokenIndex147
						}
					l148:
						add(rulePegText, position142)
					}
					{
						add(ruleAction34, position)
					}
					goto l123
				l141:
					position, tokenIndex = position123, tokenIndex123
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
						if buffer[position] != rune('.') {
							goto l152
						}
						position++
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
						add(rulePegText, position153)
					}
					{
						add(ruleAction35, position)
					}
					goto l123
				l152:
					position, tokenIndex = position123, tokenIndex123
					{
						position160 := position
						if !_rules[ruleIDENT]() {
							goto l159
						}
						add(rulePegText, position160)
					}
					{
						add(ruleAction36, position)
					}
					if !_rules[ruleopen]() {
						goto l159
					}
					if !_rules[ruleallargs]() {
						goto l159
					}
					{
						position162, tokenIndex162 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l162
						}
						goto l163
					l162:
						position, tokenIndex = position162, tokenIndex162
					}
				l163:
					if !_rules[ruleclose]() {
						goto l159
					}
					{
						add(ruleAction37, position)
					}
					goto l123
				l159:
					position, tokenIndex = position123, tokenIndex123
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
						add(ruleAction38, position)
					}
					goto l123
				l165:
					position, tokenIndex = position123, tokenIndex123
					{
						position183 := position
						if buffer[position] != rune('"') {
							goto l182
						}
						position++
						if !_rules[ruledoublequotedstring]() {
							goto l182
						}
						if buffer[position] != rune('"') {
							goto l182
						}
						position++
						add(rulePegText, position183)
					}
					{
						add(ruleAction39, position)
					}
					goto l123
				l182:
					position, tokenIndex = position123, tokenIndex123
					if buffer[position] != rune('\'') {
						goto l121
					}
					position++
					{
						position185 := position
						if !_rules[rulesinglequotedstring]() {
							goto l121
						}
						add(rulePegText, position185)
					}
					if buffer[position] != rune('\'') {
						goto l121
					}
					position++
					{
						add(ruleAction40, position)
					}
				}
			l123:
				add(ruleitem, position122)
			}
			return true
		l121:
			position, tokenIndex = position121, tokenIndex121
			return false
		},
		/* 13 doublequotedstring <- <(('\\' '"') / ('\\' '\\') / (!'"' .))*> */
		func() bool {
			{
				position188 := position
			l189:
				{
					position190, tokenIndex190 := position, tokenIndex
					{
						position191, tokenIndex191 := position, tokenIndex
						if buffer[position] != rune('\\') {
							goto l192
						}
						position++
						if buffer[position] != rune('"') {
							goto l192
						}
						position++
						goto l191
					l192:
						position, tokenIndex = position191, tokenIndex191
						if buffer[position] != rune('\\') {
							goto l193
						}
						position++
						if buffer[position] != rune('\\') {
							goto l193
						}
						position++
						goto l191
					l193:
						position, tokenIndex = position191, tokenIndex191
						{
							position194, tokenIndex194 := position, tokenIndex
							if buffer[position] != rune('"') {
								goto l194
							}
							position++
							goto l190
						l194:
							position, tokenIndex = position194, tokenIndex194
						}
						if !matchDot() {
							goto l190
						}
					}
				l191:
					goto l189
				l190:
					position, tokenIndex = position190, tokenIndex190
				}
				add(ruledoublequotedstring, position188)
			}
			return true
		},
		/* 14 singlequotedstring <- <(('\\' '\'') / ('\\' '\\') / (!'\'' .))*> */
		func() bool {
			{
				position196 := position
			l197:
				{
					position198, tokenIndex198 := position, tokenIndex
					{
						position199, tokenIndex199 := position, tokenIndex
						if buffer[position] != rune('\\') {
							goto l200
						}
						position++
						if buffer[position] != rune('\'') {
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
						if buffer[position] != rune('\\') {
							goto l201
						}
						position++
						goto l199
					l201:
						position, tokenIndex = position199, tokenIndex199
						{
							position202, tokenIndex202 := position, tokenIndex
							if buffer[position] != rune('\'') {
								goto l202
							}
							position++
							goto l198
						l202:
							position, tokenIndex = position202, tokenIndex202
						}
						if !matchDot() {
							goto l198
						}
					}
				l199:
					goto l197
				l198:
					position, tokenIndex = position198, tokenIndex198
				}
				add(rulesinglequotedstring, position196)
			}
			return true
		},
		/* 15 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_' / '-')*)> */
		func() bool {
			position203, tokenIndex203 := position, tokenIndex
			{
				position204 := position
				{
					position205, tokenIndex205 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l206
					}
					position++
					goto l205
				l206:
					position, tokenIndex = position205, tokenIndex205
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l203
					}
					position++
				}
			l205:
			l207:
				{
					position208, tokenIndex208 := position, tokenIndex
					{
						position209, tokenIndex209 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l210
						}
						position++
						goto l209
					l210:
						position, tokenIndex = position209, tokenIndex209
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l211
						}
						position++
						goto l209
					l211:
						position, tokenIndex = position209, tokenIndex209
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l212
						}
						position++
						goto l209
					l212:
						position, tokenIndex = position209, tokenIndex209
						if buffer[position] != rune('_') {
							goto l213
						}
						position++
						goto l209
					l213:
						position, tokenIndex = position209, tokenIndex209
						if buffer[position] != rune('-') {
							goto l208
						}
						position++
					}
				l209:
					goto l207
				l208:
					position, tokenIndex = position208, tokenIndex208
				}
				add(rulefieldExpr, position204)
			}
			return true
		l203:
			position, tokenIndex = position203, tokenIndex203
			return false
		},
		/* 16 field <- <(<(fieldExpr / reserved)> Action41)> */
		func() bool {
			position214, tokenIndex214 := position, tokenIndex
			{
				position215 := position
				{
					position216 := position
					{
						position217, tokenIndex217 := position, tokenIndex
						if !_rules[rulefieldExpr]() {
							goto l218
						}
						goto l217
					l218:
						position, tokenIndex = position217, tokenIndex217
						{
							position219 := position
							{
								position220, tokenIndex220 := position, tokenIndex
								if buffer[position] != rune('_') {
									goto l221
								}
								position++
								if buffer[position] != rune('r') {
									goto l221
								}
								position++
								if buffer[position] != rune('o') {
									goto l221
								}
								position++
								if buffer[position] != rune('w') {
									goto l221
								}
								position++
								goto l220
							l221:
								position, tokenIndex = position220, tokenIndex220
								if buffer[position] != rune('_') {
									goto l222
								}
								position++
								if buffer[position] != rune('c') {
									goto l222
								}
								position++
								if buffer[position] != rune('o') {
									goto l222
								}
								position++
								if buffer[position] != rune('l') {
									goto l222
								}
								position++
								goto l220
							l222:
								position, tokenIndex = position220, tokenIndex220
								if buffer[position] != rune('_') {
									goto l223
								}
								position++
								if buffer[position] != rune('s') {
									goto l223
								}
								position++
								if buffer[position] != rune('t') {
									goto l223
								}
								position++
								if buffer[position] != rune('a') {
									goto l223
								}
								position++
								if buffer[position] != rune('r') {
									goto l223
								}
								position++
								if buffer[position] != rune('t') {
									goto l223
								}
								position++
								goto l220
							l223:
								position, tokenIndex = position220, tokenIndex220
								if buffer[position] != rune('_') {
									goto l224
								}
								position++
								if buffer[position] != rune('e') {
									goto l224
								}
								position++
								if buffer[position] != rune('n') {
									goto l224
								}
								position++
								if buffer[position] != rune('d') {
									goto l224
								}
								position++
								goto l220
							l224:
								position, tokenIndex = position220, tokenIndex220
								if buffer[position] != rune('_') {
									goto l225
								}
								position++
								if buffer[position] != rune('t') {
									goto l225
								}
								position++
								if buffer[position] != rune('i') {
									goto l225
								}
								position++
								if buffer[position] != rune('m') {
									goto l225
								}
								position++
								if buffer[position] != rune('e') {
									goto l225
								}
								position++
								if buffer[position] != rune('s') {
									goto l225
								}
								position++
								if buffer[position] != rune('t') {
									goto l225
								}
								position++
								if buffer[position] != rune('a') {
									goto l225
								}
								position++
								if buffer[position] != rune('m') {
									goto l225
								}
								position++
								if buffer[position] != rune('p') {
									goto l225
								}
								position++
								goto l220
							l225:
								position, tokenIndex = position220, tokenIndex220
								if buffer[position] != rune('_') {
									goto l214
								}
								position++
								if buffer[position] != rune('f') {
									goto l214
								}
								position++
								if buffer[position] != rune('i') {
									goto l214
								}
								position++
								if buffer[position] != rune('e') {
									goto l214
								}
								position++
								if buffer[position] != rune('l') {
									goto l214
								}
								position++
								if buffer[position] != rune('d') {
									goto l214
								}
								position++
							}
						l220:
							add(rulereserved, position219)
						}
					}
				l217:
					add(rulePegText, position216)
				}
				{
					add(ruleAction41, position)
				}
				add(rulefield, position215)
			}
			return true
		l214:
			position, tokenIndex = position214, tokenIndex214
			return false
		},
		/* 17 reserved <- <(('_' 'r' 'o' 'w') / ('_' 'c' 'o' 'l') / ('_' 's' 't' 'a' 'r' 't') / ('_' 'e' 'n' 'd') / ('_' 't' 'i' 'm' 'e' 's' 't' 'a' 'm' 'p') / ('_' 'f' 'i' 'e' 'l' 'd'))> */
		nil,
		/* 18 posfield <- <(<fieldExpr> Action42)> */
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
					add(ruleAction42, position)
				}
				add(ruleposfield, position229)
			}
			return true
		l228:
			position, tokenIndex = position228, tokenIndex228
			return false
		},
		/* 19 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position232, tokenIndex232 := position, tokenIndex
			{
				position233 := position
				{
					position234, tokenIndex234 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l235
					}
					position++
				l236:
					{
						position237, tokenIndex237 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l237
						}
						position++
						goto l236
					l237:
						position, tokenIndex = position237, tokenIndex237
					}
					goto l234
				l235:
					position, tokenIndex = position234, tokenIndex234
					if buffer[position] != rune('0') {
						goto l232
					}
					position++
				}
			l234:
				add(ruleuint, position233)
			}
			return true
		l232:
			position, tokenIndex = position232, tokenIndex232
			return false
		},
		/* 20 col <- <((<uint> Action43) / ('\'' <singlequotedstring> '\'' Action44) / ('"' <doublequotedstring> '"' Action45))> */
		func() bool {
			position238, tokenIndex238 := position, tokenIndex
			{
				position239 := position
				{
					position240, tokenIndex240 := position, tokenIndex
					{
						position242 := position
						if !_rules[ruleuint]() {
							goto l241
						}
						add(rulePegText, position242)
					}
					{
						add(ruleAction43, position)
					}
					goto l240
				l241:
					position, tokenIndex = position240, tokenIndex240
					if buffer[position] != rune('\'') {
						goto l244
					}
					position++
					{
						position245 := position
						if !_rules[rulesinglequotedstring]() {
							goto l244
						}
						add(rulePegText, position245)
					}
					if buffer[position] != rune('\'') {
						goto l244
					}
					position++
					{
						add(ruleAction44, position)
					}
					goto l240
				l244:
					position, tokenIndex = position240, tokenIndex240
					if buffer[position] != rune('"') {
						goto l238
					}
					position++
					{
						position247 := position
						if !_rules[ruledoublequotedstring]() {
							goto l238
						}
						add(rulePegText, position247)
					}
					if buffer[position] != rune('"') {
						goto l238
					}
					position++
					{
						add(ruleAction45, position)
					}
				}
			l240:
				add(rulecol, position239)
			}
			return true
		l238:
			position, tokenIndex = position238, tokenIndex238
			return false
		},
		/* 21 row <- <((<uint> Action46) / ('\'' <singlequotedstring> '\'' Action47) / ('"' <doublequotedstring> '"' Action48))> */
		nil,
		/* 22 open <- <('(' sp)> */
		func() bool {
			position250, tokenIndex250 := position, tokenIndex
			{
				position251 := position
				if buffer[position] != rune('(') {
					goto l250
				}
				position++
				if !_rules[rulesp]() {
					goto l250
				}
				add(ruleopen, position251)
			}
			return true
		l250:
			position, tokenIndex = position250, tokenIndex250
			return false
		},
		/* 23 close <- <(')' sp)> */
		func() bool {
			position252, tokenIndex252 := position, tokenIndex
			{
				position253 := position
				if buffer[position] != rune(')') {
					goto l252
				}
				position++
				if !_rules[rulesp]() {
					goto l252
				}
				add(ruleclose, position253)
			}
			return true
		l252:
			position, tokenIndex = position252, tokenIndex252
			return false
		},
		/* 24 sp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position255 := position
			l256:
				{
					position257, tokenIndex257 := position, tokenIndex
					{
						position258, tokenIndex258 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l259
						}
						position++
						goto l258
					l259:
						position, tokenIndex = position258, tokenIndex258
						if buffer[position] != rune('\t') {
							goto l260
						}
						position++
						goto l258
					l260:
						position, tokenIndex = position258, tokenIndex258
						if buffer[position] != rune('\n') {
							goto l257
						}
						position++
					}
				l258:
					goto l256
				l257:
					position, tokenIndex = position257, tokenIndex257
				}
				add(rulesp, position255)
			}
			return true
		},
		/* 25 comma <- <(sp ',' sp)> */
		func() bool {
			position261, tokenIndex261 := position, tokenIndex
			{
				position262 := position
				if !_rules[rulesp]() {
					goto l261
				}
				if buffer[position] != rune(',') {
					goto l261
				}
				position++
				if !_rules[rulesp]() {
					goto l261
				}
				add(rulecomma, position262)
			}
			return true
		l261:
			position, tokenIndex = position261, tokenIndex261
			return false
		},
		/* 26 lbrack <- <('[' sp)> */
		nil,
		/* 27 rbrack <- <(sp ']' sp)> */
		nil,
		/* 28 IDENT <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9])*)> */
		func() bool {
			position265, tokenIndex265 := position, tokenIndex
			{
				position266 := position
				{
					position267, tokenIndex267 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l268
					}
					position++
					goto l267
				l268:
					position, tokenIndex = position267, tokenIndex267
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l265
					}
					position++
				}
			l267:
			l269:
				{
					position270, tokenIndex270 := position, tokenIndex
					{
						position271, tokenIndex271 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l272
						}
						position++
						goto l271
					l272:
						position, tokenIndex = position271, tokenIndex271
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l273
						}
						position++
						goto l271
					l273:
						position, tokenIndex = position271, tokenIndex271
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l270
						}
						position++
					}
				l271:
					goto l269
				l270:
					position, tokenIndex = position270, tokenIndex270
				}
				add(ruleIDENT, position266)
			}
			return true
		l265:
			position, tokenIndex = position265, tokenIndex265
			return false
		},
		/* 29 timestampbasicfmt <- <([0-9] [0-9] [0-9] [0-9] '-' ('0' / '1') [0-9] '-' [0-3] [0-9] 'T' [0-9] [0-9] ':' [0-9] [0-9])> */
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
		/* 30 timestampfmt <- <(('"' <timestampbasicfmt> '"') / ('\'' <timestampbasicfmt> '\'') / <timestampbasicfmt>)> */
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
					{
						position282 := position
						if !_rules[ruletimestampbasicfmt]() {
							goto l281
						}
						add(rulePegText, position282)
					}
					if buffer[position] != rune('"') {
						goto l281
					}
					position++
					goto l280
				l281:
					position, tokenIndex = position280, tokenIndex280
					if buffer[position] != rune('\'') {
						goto l283
					}
					position++
					{
						position284 := position
						if !_rules[ruletimestampbasicfmt]() {
							goto l283
						}
						add(rulePegText, position284)
					}
					if buffer[position] != rune('\'') {
						goto l283
					}
					position++
					goto l280
				l283:
					position, tokenIndex = position280, tokenIndex280
					{
						position285 := position
						if !_rules[ruletimestampbasicfmt]() {
							goto l278
						}
						add(rulePegText, position285)
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
		/* 31 timestamp <- <(<timestampfmt> Action49)> */
		nil,
		/* 33 Action0 <- <{p.startCall("Set")}> */
		nil,
		/* 34 Action1 <- <{p.endCall()}> */
		nil,
		/* 35 Action2 <- <{p.startCall("SetRowAttrs")}> */
		nil,
		/* 36 Action3 <- <{p.endCall()}> */
		nil,
		/* 37 Action4 <- <{p.startCall("SetColumnAttrs")}> */
		nil,
		/* 38 Action5 <- <{p.endCall()}> */
		nil,
		/* 39 Action6 <- <{p.startCall("Clear")}> */
		nil,
		/* 40 Action7 <- <{p.endCall()}> */
		nil,
		/* 41 Action8 <- <{p.startCall("ClearRow")}> */
		nil,
		/* 42 Action9 <- <{p.endCall()}> */
		nil,
		/* 43 Action10 <- <{p.startCall("Store")}> */
		nil,
		/* 44 Action11 <- <{p.endCall()}> */
		nil,
		/* 45 Action12 <- <{p.startCall("TopN")}> */
		nil,
		/* 46 Action13 <- <{p.endCall()}> */
		nil,
		nil,
		/* 48 Action14 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 49 Action15 <- <{ p.endCall() }> */
		nil,
		/* 50 Action16 <- <{ p.addBTWN() }> */
		nil,
		/* 51 Action17 <- <{ p.addLTE() }> */
		nil,
		/* 52 Action18 <- <{ p.addGTE() }> */
		nil,
		/* 53 Action19 <- <{ p.addEQ() }> */
		nil,
		/* 54 Action20 <- <{ p.addNEQ() }> */
		nil,
		/* 55 Action21 <- <{ p.addLT() }> */
		nil,
		/* 56 Action22 <- <{ p.addGT() }> */
		nil,
		/* 57 Action23 <- <{p.startConditional()}> */
		nil,
		/* 58 Action24 <- <{p.endConditional()}> */
		nil,
		/* 59 Action25 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 60 Action26 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 61 Action27 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 62 Action28 <- <{ p.startList() }> */
		nil,
		/* 63 Action29 <- <{ p.endList() }> */
		nil,
		/* 64 Action30 <- <{ p.addVal(nil) }> */
		nil,
		/* 65 Action31 <- <{ p.addVal(true) }> */
		nil,
		/* 66 Action32 <- <{ p.addVal(false) }> */
		nil,
		/* 67 Action33 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 68 Action34 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 69 Action35 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 70 Action36 <- <{ p.startCall(buffer[begin:end]) }> */
		nil,
		/* 71 Action37 <- <{ p.addVal(p.endCall()) }> */
		nil,
		/* 72 Action38 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 73 Action39 <- <{ s, _ := strconv.Unquote(buffer[begin:end]); p.addVal(s) }> */
		nil,
		/* 74 Action40 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 75 Action41 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 76 Action42 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 77 Action43 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 78 Action44 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 79 Action45 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 80 Action46 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 81 Action47 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 82 Action48 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 83 Action49 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
