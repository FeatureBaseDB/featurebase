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
	ruleAction12
	ruleAction13
	ruleAction14
	ruleAction15
	rulePegText
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
	ruleAction50
	ruleAction51
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
	"Action12",
	"Action13",
	"Action14",
	"Action15",
	"PegText",
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
	"Action50",
	"Action51",
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
	rules  [88]func() bool
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
			p.startCall("Range")
		case ruleAction15:
			p.endCall()
		case ruleAction16:
			p.startCall(buffer[begin:end])
		case ruleAction17:
			p.endCall()
		case ruleAction18:
			p.addBTWN()
		case ruleAction19:
			p.addLTE()
		case ruleAction20:
			p.addGTE()
		case ruleAction21:
			p.addEQ()
		case ruleAction22:
			p.addNEQ()
		case ruleAction23:
			p.addLT()
		case ruleAction24:
			p.addGT()
		case ruleAction25:
			p.startConditional()
		case ruleAction26:
			p.endConditional()
		case ruleAction27:
			p.condAdd(buffer[begin:end])
		case ruleAction28:
			p.condAdd(buffer[begin:end])
		case ruleAction29:
			p.condAdd(buffer[begin:end])
		case ruleAction30:
			p.addPosStr("_start", buffer[begin:end])
		case ruleAction31:
			p.addPosStr("_end", buffer[begin:end])
		case ruleAction32:
			p.startList()
		case ruleAction33:
			p.endList()
		case ruleAction34:
			p.addVal(nil)
		case ruleAction35:
			p.addVal(true)
		case ruleAction36:
			p.addVal(false)
		case ruleAction37:
			p.addNumVal(buffer[begin:end])
		case ruleAction38:
			p.addNumVal(buffer[begin:end])
		case ruleAction39:
			p.addVal(buffer[begin:end])
		case ruleAction40:
			p.addVal(buffer[begin:end])
		case ruleAction41:
			p.addVal(buffer[begin:end])
		case ruleAction42:
			p.addField(buffer[begin:end])
		case ruleAction43:
			p.addPosStr("_field", buffer[begin:end])
		case ruleAction44:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction45:
			p.addPosNum("_col", buffer[begin:end])
		case ruleAction46:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction47:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction48:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction49:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction50:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction51:
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
		/* 1 Call <- <(('S' 'e' 't' Action0 open col comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma row comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'u' 'm' 'n' 'A' 't' 't' 'r' 's' Action4 open col comma args close Action5) / ('C' 'l' 'e' 'a' 'r' Action6 open col comma args close Action7) / ('C' 'l' 'e' 'a' 'r' 'R' 'o' 'w' Action8 open arg close Action9) / ('S' 't' 'o' 'r' 'e' Action10 open Call comma arg close Action11) / ('T' 'o' 'p' 'N' Action12 open posfield (comma allargs)? close Action13) / ('R' 'a' 'n' 'g' 'e' Action14 open (timerange / conditional / arg) close Action15) / (<IDENT> Action16 open allargs comma? close Action17))> */
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
								add(ruleAction51, position)
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
								add(ruleAction48, position)
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
								add(ruleAction49, position)
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
								add(ruleAction50, position)
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
					if buffer[position] != rune('R') {
						goto l46
					}
					position++
					if buffer[position] != rune('a') {
						goto l46
					}
					position++
					if buffer[position] != rune('n') {
						goto l46
					}
					position++
					if buffer[position] != rune('g') {
						goto l46
					}
					position++
					if buffer[position] != rune('e') {
						goto l46
					}
					position++
					{
						add(ruleAction14, position)
					}
					if !_rules[ruleopen]() {
						goto l46
					}
					{
						position48, tokenIndex48 := position, tokenIndex
						{
							position50 := position
							if !_rules[rulefield]() {
								goto l49
							}
							if !_rules[rulesp]() {
								goto l49
							}
							if buffer[position] != rune('=') {
								goto l49
							}
							position++
							if !_rules[rulesp]() {
								goto l49
							}
							if !_rules[rulevalue]() {
								goto l49
							}
							if !_rules[rulecomma]() {
								goto l49
							}
							{
								position51 := position
								if !_rules[ruletimestampfmt]() {
									goto l49
								}
								add(rulePegText, position51)
							}
							{
								add(ruleAction30, position)
							}
							if !_rules[rulecomma]() {
								goto l49
							}
							{
								position53 := position
								if !_rules[ruletimestampfmt]() {
									goto l49
								}
								add(rulePegText, position53)
							}
							{
								add(ruleAction31, position)
							}
							add(ruletimerange, position50)
						}
						goto l48
					l49:
						position, tokenIndex = position48, tokenIndex48
						{
							position56 := position
							{
								add(ruleAction25, position)
							}
							if !_rules[rulecondint]() {
								goto l55
							}
							if !_rules[rulecondLT]() {
								goto l55
							}
							{
								position58 := position
								{
									position59 := position
									if !_rules[rulefieldExpr]() {
										goto l55
									}
									add(rulePegText, position59)
								}
								if !_rules[rulesp]() {
									goto l55
								}
								{
									add(ruleAction29, position)
								}
								add(rulecondfield, position58)
							}
							if !_rules[rulecondLT]() {
								goto l55
							}
							if !_rules[rulecondint]() {
								goto l55
							}
							{
								add(ruleAction26, position)
							}
							add(ruleconditional, position56)
						}
						goto l48
					l55:
						position, tokenIndex = position48, tokenIndex48
						if !_rules[rulearg]() {
							goto l46
						}
					}
				l48:
					if !_rules[ruleclose]() {
						goto l46
					}
					{
						add(ruleAction15, position)
					}
					goto l7
				l46:
					position, tokenIndex = position7, tokenIndex7
					{
						position63 := position
						{
							position64 := position
							{
								position65, tokenIndex65 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l66
								}
								position++
								goto l65
							l66:
								position, tokenIndex = position65, tokenIndex65
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l5
								}
								position++
							}
						l65:
						l67:
							{
								position68, tokenIndex68 := position, tokenIndex
								{
									position69, tokenIndex69 := position, tokenIndex
									if c := buffer[position]; c < rune('a') || c > rune('z') {
										goto l70
									}
									position++
									goto l69
								l70:
									position, tokenIndex = position69, tokenIndex69
									if c := buffer[position]; c < rune('A') || c > rune('Z') {
										goto l71
									}
									position++
									goto l69
								l71:
									position, tokenIndex = position69, tokenIndex69
									if c := buffer[position]; c < rune('0') || c > rune('9') {
										goto l68
									}
									position++
								}
							l69:
								goto l67
							l68:
								position, tokenIndex = position68, tokenIndex68
							}
							add(ruleIDENT, position64)
						}
						add(rulePegText, position63)
					}
					{
						add(ruleAction16, position)
					}
					if !_rules[ruleopen]() {
						goto l5
					}
					if !_rules[ruleallargs]() {
						goto l5
					}
					{
						position73, tokenIndex73 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l73
						}
						goto l74
					l73:
						position, tokenIndex = position73, tokenIndex73
					}
				l74:
					if !_rules[ruleclose]() {
						goto l5
					}
					{
						add(ruleAction17, position)
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
			position76, tokenIndex76 := position, tokenIndex
			{
				position77 := position
				{
					position78, tokenIndex78 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l79
					}
				l80:
					{
						position81, tokenIndex81 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l81
						}
						if !_rules[ruleCall]() {
							goto l81
						}
						goto l80
					l81:
						position, tokenIndex = position81, tokenIndex81
					}
					{
						position82, tokenIndex82 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l82
						}
						if !_rules[ruleargs]() {
							goto l82
						}
						goto l83
					l82:
						position, tokenIndex = position82, tokenIndex82
					}
				l83:
					goto l78
				l79:
					position, tokenIndex = position78, tokenIndex78
					if !_rules[ruleargs]() {
						goto l84
					}
					goto l78
				l84:
					position, tokenIndex = position78, tokenIndex78
					if !_rules[rulesp]() {
						goto l76
					}
				}
			l78:
				add(ruleallargs, position77)
			}
			return true
		l76:
			position, tokenIndex = position76, tokenIndex76
			return false
		},
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position85, tokenIndex85 := position, tokenIndex
			{
				position86 := position
				if !_rules[rulearg]() {
					goto l85
				}
				{
					position87, tokenIndex87 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l87
					}
					if !_rules[ruleargs]() {
						goto l87
					}
					goto l88
				l87:
					position, tokenIndex = position87, tokenIndex87
				}
			l88:
				if !_rules[rulesp]() {
					goto l85
				}
				add(ruleargs, position86)
			}
			return true
		l85:
			position, tokenIndex = position85, tokenIndex85
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value))> */
		func() bool {
			position89, tokenIndex89 := position, tokenIndex
			{
				position90 := position
				{
					position91, tokenIndex91 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l92
					}
					if !_rules[rulesp]() {
						goto l92
					}
					if buffer[position] != rune('=') {
						goto l92
					}
					position++
					if !_rules[rulesp]() {
						goto l92
					}
					if !_rules[rulevalue]() {
						goto l92
					}
					goto l91
				l92:
					position, tokenIndex = position91, tokenIndex91
					if !_rules[rulefield]() {
						goto l89
					}
					if !_rules[rulesp]() {
						goto l89
					}
					{
						position93 := position
						{
							position94, tokenIndex94 := position, tokenIndex
							if buffer[position] != rune('>') {
								goto l95
							}
							position++
							if buffer[position] != rune('<') {
								goto l95
							}
							position++
							{
								add(ruleAction18, position)
							}
							goto l94
						l95:
							position, tokenIndex = position94, tokenIndex94
							if buffer[position] != rune('<') {
								goto l97
							}
							position++
							if buffer[position] != rune('=') {
								goto l97
							}
							position++
							{
								add(ruleAction19, position)
							}
							goto l94
						l97:
							position, tokenIndex = position94, tokenIndex94
							if buffer[position] != rune('>') {
								goto l99
							}
							position++
							if buffer[position] != rune('=') {
								goto l99
							}
							position++
							{
								add(ruleAction20, position)
							}
							goto l94
						l99:
							position, tokenIndex = position94, tokenIndex94
							if buffer[position] != rune('=') {
								goto l101
							}
							position++
							if buffer[position] != rune('=') {
								goto l101
							}
							position++
							{
								add(ruleAction21, position)
							}
							goto l94
						l101:
							position, tokenIndex = position94, tokenIndex94
							if buffer[position] != rune('!') {
								goto l103
							}
							position++
							if buffer[position] != rune('=') {
								goto l103
							}
							position++
							{
								add(ruleAction22, position)
							}
							goto l94
						l103:
							position, tokenIndex = position94, tokenIndex94
							if buffer[position] != rune('<') {
								goto l105
							}
							position++
							{
								add(ruleAction23, position)
							}
							goto l94
						l105:
							position, tokenIndex = position94, tokenIndex94
							if buffer[position] != rune('>') {
								goto l89
							}
							position++
							{
								add(ruleAction24, position)
							}
						}
					l94:
						add(ruleCOND, position93)
					}
					if !_rules[rulesp]() {
						goto l89
					}
					if !_rules[rulevalue]() {
						goto l89
					}
				}
			l91:
				add(rulearg, position90)
			}
			return true
		l89:
			position, tokenIndex = position89, tokenIndex89
			return false
		},
		/* 5 COND <- <(('>' '<' Action18) / ('<' '=' Action19) / ('>' '=' Action20) / ('=' '=' Action21) / ('!' '=' Action22) / ('<' Action23) / ('>' Action24))> */
		nil,
		/* 6 conditional <- <(Action25 condint condLT condfield condLT condint Action26)> */
		nil,
		/* 7 condint <- <(<(('-'? [1-9] [0-9]*) / '0')> sp Action27)> */
		func() bool {
			position110, tokenIndex110 := position, tokenIndex
			{
				position111 := position
				{
					position112 := position
					{
						position113, tokenIndex113 := position, tokenIndex
						{
							position115, tokenIndex115 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l115
							}
							position++
							goto l116
						l115:
							position, tokenIndex = position115, tokenIndex115
						}
					l116:
						if c := buffer[position]; c < rune('1') || c > rune('9') {
							goto l114
						}
						position++
					l117:
						{
							position118, tokenIndex118 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l118
							}
							position++
							goto l117
						l118:
							position, tokenIndex = position118, tokenIndex118
						}
						goto l113
					l114:
						position, tokenIndex = position113, tokenIndex113
						if buffer[position] != rune('0') {
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
					add(ruleAction27, position)
				}
				add(rulecondint, position111)
			}
			return true
		l110:
			position, tokenIndex = position110, tokenIndex110
			return false
		},
		/* 8 condLT <- <(<(('<' '=') / '<')> sp Action28)> */
		func() bool {
			position120, tokenIndex120 := position, tokenIndex
			{
				position121 := position
				{
					position122 := position
					{
						position123, tokenIndex123 := position, tokenIndex
						if buffer[position] != rune('<') {
							goto l124
						}
						position++
						if buffer[position] != rune('=') {
							goto l124
						}
						position++
						goto l123
					l124:
						position, tokenIndex = position123, tokenIndex123
						if buffer[position] != rune('<') {
							goto l120
						}
						position++
					}
				l123:
					add(rulePegText, position122)
				}
				if !_rules[rulesp]() {
					goto l120
				}
				{
					add(ruleAction28, position)
				}
				add(rulecondLT, position121)
			}
			return true
		l120:
			position, tokenIndex = position120, tokenIndex120
			return false
		},
		/* 9 condfield <- <(<fieldExpr> sp Action29)> */
		nil,
		/* 10 timerange <- <(field sp '=' sp value comma <timestampfmt> Action30 comma <timestampfmt> Action31)> */
		nil,
		/* 11 value <- <(item / (lbrack Action32 list rbrack Action33))> */
		func() bool {
			position128, tokenIndex128 := position, tokenIndex
			{
				position129 := position
				{
					position130, tokenIndex130 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l131
					}
					goto l130
				l131:
					position, tokenIndex = position130, tokenIndex130
					{
						position132 := position
						if buffer[position] != rune('[') {
							goto l128
						}
						position++
						if !_rules[rulesp]() {
							goto l128
						}
						add(rulelbrack, position132)
					}
					{
						add(ruleAction32, position)
					}
					if !_rules[rulelist]() {
						goto l128
					}
					{
						position134 := position
						if !_rules[rulesp]() {
							goto l128
						}
						if buffer[position] != rune(']') {
							goto l128
						}
						position++
						if !_rules[rulesp]() {
							goto l128
						}
						add(rulerbrack, position134)
					}
					{
						add(ruleAction33, position)
					}
				}
			l130:
				add(rulevalue, position129)
			}
			return true
		l128:
			position, tokenIndex = position128, tokenIndex128
			return false
		},
		/* 12 list <- <(item (comma list)?)> */
		func() bool {
			position136, tokenIndex136 := position, tokenIndex
			{
				position137 := position
				if !_rules[ruleitem]() {
					goto l136
				}
				{
					position138, tokenIndex138 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l138
					}
					if !_rules[rulelist]() {
						goto l138
					}
					goto l139
				l138:
					position, tokenIndex = position138, tokenIndex138
				}
			l139:
				add(rulelist, position137)
			}
			return true
		l136:
			position, tokenIndex = position136, tokenIndex136
			return false
		},
		/* 13 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action34) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action35) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action36) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action37) / (<('-'? '.' [0-9]+)> Action38) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action39) / ('"' <doublequotedstring> '"' Action40) / ('\'' <singlequotedstring> '\'' Action41))> */
		func() bool {
			position140, tokenIndex140 := position, tokenIndex
			{
				position141 := position
				{
					position142, tokenIndex142 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l143
					}
					position++
					if buffer[position] != rune('u') {
						goto l143
					}
					position++
					if buffer[position] != rune('l') {
						goto l143
					}
					position++
					if buffer[position] != rune('l') {
						goto l143
					}
					position++
					{
						position144, tokenIndex144 := position, tokenIndex
						{
							position145, tokenIndex145 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l146
							}
							goto l145
						l146:
							position, tokenIndex = position145, tokenIndex145
							if !_rules[rulesp]() {
								goto l143
							}
							if !_rules[ruleclose]() {
								goto l143
							}
						}
					l145:
						position, tokenIndex = position144, tokenIndex144
					}
					{
						add(ruleAction34, position)
					}
					goto l142
				l143:
					position, tokenIndex = position142, tokenIndex142
					if buffer[position] != rune('t') {
						goto l148
					}
					position++
					if buffer[position] != rune('r') {
						goto l148
					}
					position++
					if buffer[position] != rune('u') {
						goto l148
					}
					position++
					if buffer[position] != rune('e') {
						goto l148
					}
					position++
					{
						position149, tokenIndex149 := position, tokenIndex
						{
							position150, tokenIndex150 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l151
							}
							goto l150
						l151:
							position, tokenIndex = position150, tokenIndex150
							if !_rules[rulesp]() {
								goto l148
							}
							if !_rules[ruleclose]() {
								goto l148
							}
						}
					l150:
						position, tokenIndex = position149, tokenIndex149
					}
					{
						add(ruleAction35, position)
					}
					goto l142
				l148:
					position, tokenIndex = position142, tokenIndex142
					if buffer[position] != rune('f') {
						goto l153
					}
					position++
					if buffer[position] != rune('a') {
						goto l153
					}
					position++
					if buffer[position] != rune('l') {
						goto l153
					}
					position++
					if buffer[position] != rune('s') {
						goto l153
					}
					position++
					if buffer[position] != rune('e') {
						goto l153
					}
					position++
					{
						position154, tokenIndex154 := position, tokenIndex
						{
							position155, tokenIndex155 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l156
							}
							goto l155
						l156:
							position, tokenIndex = position155, tokenIndex155
							if !_rules[rulesp]() {
								goto l153
							}
							if !_rules[ruleclose]() {
								goto l153
							}
						}
					l155:
						position, tokenIndex = position154, tokenIndex154
					}
					{
						add(ruleAction36, position)
					}
					goto l142
				l153:
					position, tokenIndex = position142, tokenIndex142
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
						{
							position164, tokenIndex164 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l164
							}
							position++
						l166:
							{
								position167, tokenIndex167 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l167
								}
								position++
								goto l166
							l167:
								position, tokenIndex = position167, tokenIndex167
							}
							goto l165
						l164:
							position, tokenIndex = position164, tokenIndex164
						}
					l165:
						add(rulePegText, position159)
					}
					{
						add(ruleAction37, position)
					}
					goto l142
				l158:
					position, tokenIndex = position142, tokenIndex142
					{
						position170 := position
						{
							position171, tokenIndex171 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l171
							}
							position++
							goto l172
						l171:
							position, tokenIndex = position171, tokenIndex171
						}
					l172:
						if buffer[position] != rune('.') {
							goto l169
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l169
						}
						position++
					l173:
						{
							position174, tokenIndex174 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l174
							}
							position++
							goto l173
						l174:
							position, tokenIndex = position174, tokenIndex174
						}
						add(rulePegText, position170)
					}
					{
						add(ruleAction38, position)
					}
					goto l142
				l169:
					position, tokenIndex = position142, tokenIndex142
					{
						position177 := position
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
								goto l176
							}
							position++
						}
					l180:
					l178:
						{
							position179, tokenIndex179 := position, tokenIndex
							{
								position186, tokenIndex186 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l187
								}
								position++
								goto l186
							l187:
								position, tokenIndex = position186, tokenIndex186
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l188
								}
								position++
								goto l186
							l188:
								position, tokenIndex = position186, tokenIndex186
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l189
								}
								position++
								goto l186
							l189:
								position, tokenIndex = position186, tokenIndex186
								if buffer[position] != rune('-') {
									goto l190
								}
								position++
								goto l186
							l190:
								position, tokenIndex = position186, tokenIndex186
								if buffer[position] != rune('_') {
									goto l191
								}
								position++
								goto l186
							l191:
								position, tokenIndex = position186, tokenIndex186
								if buffer[position] != rune(':') {
									goto l179
								}
								position++
							}
						l186:
							goto l178
						l179:
							position, tokenIndex = position179, tokenIndex179
						}
						add(rulePegText, position177)
					}
					{
						add(ruleAction39, position)
					}
					goto l142
				l176:
					position, tokenIndex = position142, tokenIndex142
					if buffer[position] != rune('"') {
						goto l193
					}
					position++
					{
						position194 := position
						if !_rules[ruledoublequotedstring]() {
							goto l193
						}
						add(rulePegText, position194)
					}
					if buffer[position] != rune('"') {
						goto l193
					}
					position++
					{
						add(ruleAction40, position)
					}
					goto l142
				l193:
					position, tokenIndex = position142, tokenIndex142
					if buffer[position] != rune('\'') {
						goto l140
					}
					position++
					{
						position196 := position
						if !_rules[rulesinglequotedstring]() {
							goto l140
						}
						add(rulePegText, position196)
					}
					if buffer[position] != rune('\'') {
						goto l140
					}
					position++
					{
						add(ruleAction41, position)
					}
				}
			l142:
				add(ruleitem, position141)
			}
			return true
		l140:
			position, tokenIndex = position140, tokenIndex140
			return false
		},
		/* 14 doublequotedstring <- <((!('"' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
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
								if buffer[position] != rune('"') {
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
				add(ruledoublequotedstring, position199)
			}
			return true
		},
		/* 15 singlequotedstring <- <((!('\'' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		func() bool {
			{
				position212 := position
			l213:
				{
					position214, tokenIndex214 := position, tokenIndex
					{
						position215, tokenIndex215 := position, tokenIndex
						{
							position217, tokenIndex217 := position, tokenIndex
							{
								position218, tokenIndex218 := position, tokenIndex
								if buffer[position] != rune('\'') {
									goto l219
								}
								position++
								goto l218
							l219:
								position, tokenIndex = position218, tokenIndex218
								if buffer[position] != rune('\\') {
									goto l220
								}
								position++
								goto l218
							l220:
								position, tokenIndex = position218, tokenIndex218
								if buffer[position] != rune('\n') {
									goto l217
								}
								position++
							}
						l218:
							goto l216
						l217:
							position, tokenIndex = position217, tokenIndex217
						}
						if !matchDot() {
							goto l216
						}
						goto l215
					l216:
						position, tokenIndex = position215, tokenIndex215
						if buffer[position] != rune('\\') {
							goto l221
						}
						position++
						if buffer[position] != rune('n') {
							goto l221
						}
						position++
						goto l215
					l221:
						position, tokenIndex = position215, tokenIndex215
						if buffer[position] != rune('\\') {
							goto l222
						}
						position++
						if buffer[position] != rune('"') {
							goto l222
						}
						position++
						goto l215
					l222:
						position, tokenIndex = position215, tokenIndex215
						if buffer[position] != rune('\\') {
							goto l223
						}
						position++
						if buffer[position] != rune('\'') {
							goto l223
						}
						position++
						goto l215
					l223:
						position, tokenIndex = position215, tokenIndex215
						if buffer[position] != rune('\\') {
							goto l214
						}
						position++
						if buffer[position] != rune('\\') {
							goto l214
						}
						position++
					}
				l215:
					goto l213
				l214:
					position, tokenIndex = position214, tokenIndex214
				}
				add(rulesinglequotedstring, position212)
			}
			return true
		},
		/* 16 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_' / '-')*)> */
		func() bool {
			position224, tokenIndex224 := position, tokenIndex
			{
				position225 := position
				{
					position226, tokenIndex226 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l227
					}
					position++
					goto l226
				l227:
					position, tokenIndex = position226, tokenIndex226
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l224
					}
					position++
				}
			l226:
			l228:
				{
					position229, tokenIndex229 := position, tokenIndex
					{
						position230, tokenIndex230 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l231
						}
						position++
						goto l230
					l231:
						position, tokenIndex = position230, tokenIndex230
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l232
						}
						position++
						goto l230
					l232:
						position, tokenIndex = position230, tokenIndex230
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l233
						}
						position++
						goto l230
					l233:
						position, tokenIndex = position230, tokenIndex230
						if buffer[position] != rune('_') {
							goto l234
						}
						position++
						goto l230
					l234:
						position, tokenIndex = position230, tokenIndex230
						if buffer[position] != rune('-') {
							goto l229
						}
						position++
					}
				l230:
					goto l228
				l229:
					position, tokenIndex = position229, tokenIndex229
				}
				add(rulefieldExpr, position225)
			}
			return true
		l224:
			position, tokenIndex = position224, tokenIndex224
			return false
		},
		/* 17 field <- <(<(fieldExpr / reserved)> Action42)> */
		func() bool {
			position235, tokenIndex235 := position, tokenIndex
			{
				position236 := position
				{
					position237 := position
					{
						position238, tokenIndex238 := position, tokenIndex
						if !_rules[rulefieldExpr]() {
							goto l239
						}
						goto l238
					l239:
						position, tokenIndex = position238, tokenIndex238
						{
							position240 := position
							{
								position241, tokenIndex241 := position, tokenIndex
								if buffer[position] != rune('_') {
									goto l242
								}
								position++
								if buffer[position] != rune('r') {
									goto l242
								}
								position++
								if buffer[position] != rune('o') {
									goto l242
								}
								position++
								if buffer[position] != rune('w') {
									goto l242
								}
								position++
								goto l241
							l242:
								position, tokenIndex = position241, tokenIndex241
								if buffer[position] != rune('_') {
									goto l243
								}
								position++
								if buffer[position] != rune('c') {
									goto l243
								}
								position++
								if buffer[position] != rune('o') {
									goto l243
								}
								position++
								if buffer[position] != rune('l') {
									goto l243
								}
								position++
								goto l241
							l243:
								position, tokenIndex = position241, tokenIndex241
								if buffer[position] != rune('_') {
									goto l244
								}
								position++
								if buffer[position] != rune('s') {
									goto l244
								}
								position++
								if buffer[position] != rune('t') {
									goto l244
								}
								position++
								if buffer[position] != rune('a') {
									goto l244
								}
								position++
								if buffer[position] != rune('r') {
									goto l244
								}
								position++
								if buffer[position] != rune('t') {
									goto l244
								}
								position++
								goto l241
							l244:
								position, tokenIndex = position241, tokenIndex241
								if buffer[position] != rune('_') {
									goto l245
								}
								position++
								if buffer[position] != rune('e') {
									goto l245
								}
								position++
								if buffer[position] != rune('n') {
									goto l245
								}
								position++
								if buffer[position] != rune('d') {
									goto l245
								}
								position++
								goto l241
							l245:
								position, tokenIndex = position241, tokenIndex241
								if buffer[position] != rune('_') {
									goto l246
								}
								position++
								if buffer[position] != rune('t') {
									goto l246
								}
								position++
								if buffer[position] != rune('i') {
									goto l246
								}
								position++
								if buffer[position] != rune('m') {
									goto l246
								}
								position++
								if buffer[position] != rune('e') {
									goto l246
								}
								position++
								if buffer[position] != rune('s') {
									goto l246
								}
								position++
								if buffer[position] != rune('t') {
									goto l246
								}
								position++
								if buffer[position] != rune('a') {
									goto l246
								}
								position++
								if buffer[position] != rune('m') {
									goto l246
								}
								position++
								if buffer[position] != rune('p') {
									goto l246
								}
								position++
								goto l241
							l246:
								position, tokenIndex = position241, tokenIndex241
								if buffer[position] != rune('_') {
									goto l235
								}
								position++
								if buffer[position] != rune('f') {
									goto l235
								}
								position++
								if buffer[position] != rune('i') {
									goto l235
								}
								position++
								if buffer[position] != rune('e') {
									goto l235
								}
								position++
								if buffer[position] != rune('l') {
									goto l235
								}
								position++
								if buffer[position] != rune('d') {
									goto l235
								}
								position++
							}
						l241:
							add(rulereserved, position240)
						}
					}
				l238:
					add(rulePegText, position237)
				}
				{
					add(ruleAction42, position)
				}
				add(rulefield, position236)
			}
			return true
		l235:
			position, tokenIndex = position235, tokenIndex235
			return false
		},
		/* 18 reserved <- <(('_' 'r' 'o' 'w') / ('_' 'c' 'o' 'l') / ('_' 's' 't' 'a' 'r' 't') / ('_' 'e' 'n' 'd') / ('_' 't' 'i' 'm' 'e' 's' 't' 'a' 'm' 'p') / ('_' 'f' 'i' 'e' 'l' 'd'))> */
		nil,
		/* 19 posfield <- <(<fieldExpr> Action43)> */
		func() bool {
			position249, tokenIndex249 := position, tokenIndex
			{
				position250 := position
				{
					position251 := position
					if !_rules[rulefieldExpr]() {
						goto l249
					}
					add(rulePegText, position251)
				}
				{
					add(ruleAction43, position)
				}
				add(ruleposfield, position250)
			}
			return true
		l249:
			position, tokenIndex = position249, tokenIndex249
			return false
		},
		/* 20 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position253, tokenIndex253 := position, tokenIndex
			{
				position254 := position
				{
					position255, tokenIndex255 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l256
					}
					position++
				l257:
					{
						position258, tokenIndex258 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l258
						}
						position++
						goto l257
					l258:
						position, tokenIndex = position258, tokenIndex258
					}
					goto l255
				l256:
					position, tokenIndex = position255, tokenIndex255
					if buffer[position] != rune('0') {
						goto l253
					}
					position++
				}
			l255:
				add(ruleuint, position254)
			}
			return true
		l253:
			position, tokenIndex = position253, tokenIndex253
			return false
		},
		/* 21 uintrow <- <(<uint> Action44)> */
		nil,
		/* 22 col <- <((<uint> Action45) / ('\'' <singlequotedstring> '\'' Action46) / ('"' <doublequotedstring> '"' Action47))> */
		func() bool {
			position260, tokenIndex260 := position, tokenIndex
			{
				position261 := position
				{
					position262, tokenIndex262 := position, tokenIndex
					{
						position264 := position
						if !_rules[ruleuint]() {
							goto l263
						}
						add(rulePegText, position264)
					}
					{
						add(ruleAction45, position)
					}
					goto l262
				l263:
					position, tokenIndex = position262, tokenIndex262
					if buffer[position] != rune('\'') {
						goto l266
					}
					position++
					{
						position267 := position
						if !_rules[rulesinglequotedstring]() {
							goto l266
						}
						add(rulePegText, position267)
					}
					if buffer[position] != rune('\'') {
						goto l266
					}
					position++
					{
						add(ruleAction46, position)
					}
					goto l262
				l266:
					position, tokenIndex = position262, tokenIndex262
					if buffer[position] != rune('"') {
						goto l260
					}
					position++
					{
						position269 := position
						if !_rules[ruledoublequotedstring]() {
							goto l260
						}
						add(rulePegText, position269)
					}
					if buffer[position] != rune('"') {
						goto l260
					}
					position++
					{
						add(ruleAction47, position)
					}
				}
			l262:
				add(rulecol, position261)
			}
			return true
		l260:
			position, tokenIndex = position260, tokenIndex260
			return false
		},
		/* 23 row <- <((<uint> Action48) / ('\'' <singlequotedstring> '\'' Action49) / ('"' <doublequotedstring> '"' Action50))> */
		nil,
		/* 24 open <- <('(' sp)> */
		func() bool {
			position272, tokenIndex272 := position, tokenIndex
			{
				position273 := position
				if buffer[position] != rune('(') {
					goto l272
				}
				position++
				if !_rules[rulesp]() {
					goto l272
				}
				add(ruleopen, position273)
			}
			return true
		l272:
			position, tokenIndex = position272, tokenIndex272
			return false
		},
		/* 25 close <- <(')' sp)> */
		func() bool {
			position274, tokenIndex274 := position, tokenIndex
			{
				position275 := position
				if buffer[position] != rune(')') {
					goto l274
				}
				position++
				if !_rules[rulesp]() {
					goto l274
				}
				add(ruleclose, position275)
			}
			return true
		l274:
			position, tokenIndex = position274, tokenIndex274
			return false
		},
		/* 26 sp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position277 := position
			l278:
				{
					position279, tokenIndex279 := position, tokenIndex
					{
						position280, tokenIndex280 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l281
						}
						position++
						goto l280
					l281:
						position, tokenIndex = position280, tokenIndex280
						if buffer[position] != rune('\t') {
							goto l282
						}
						position++
						goto l280
					l282:
						position, tokenIndex = position280, tokenIndex280
						if buffer[position] != rune('\n') {
							goto l279
						}
						position++
					}
				l280:
					goto l278
				l279:
					position, tokenIndex = position279, tokenIndex279
				}
				add(rulesp, position277)
			}
			return true
		},
		/* 27 comma <- <(sp ',' sp)> */
		func() bool {
			position283, tokenIndex283 := position, tokenIndex
			{
				position284 := position
				if !_rules[rulesp]() {
					goto l283
				}
				if buffer[position] != rune(',') {
					goto l283
				}
				position++
				if !_rules[rulesp]() {
					goto l283
				}
				add(rulecomma, position284)
			}
			return true
		l283:
			position, tokenIndex = position283, tokenIndex283
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
			position288, tokenIndex288 := position, tokenIndex
			{
				position289 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l288
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l288
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l288
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l288
				}
				position++
				if buffer[position] != rune('-') {
					goto l288
				}
				position++
				{
					position290, tokenIndex290 := position, tokenIndex
					if buffer[position] != rune('0') {
						goto l291
					}
					position++
					goto l290
				l291:
					position, tokenIndex = position290, tokenIndex290
					if buffer[position] != rune('1') {
						goto l288
					}
					position++
				}
			l290:
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l288
				}
				position++
				if buffer[position] != rune('-') {
					goto l288
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('3') {
					goto l288
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l288
				}
				position++
				if buffer[position] != rune('T') {
					goto l288
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l288
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l288
				}
				position++
				if buffer[position] != rune(':') {
					goto l288
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l288
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l288
				}
				position++
				add(ruletimestampbasicfmt, position289)
			}
			return true
		l288:
			position, tokenIndex = position288, tokenIndex288
			return false
		},
		/* 32 timestampfmt <- <(('"' timestampbasicfmt '"') / ('\'' timestampbasicfmt '\'') / timestampbasicfmt)> */
		func() bool {
			position292, tokenIndex292 := position, tokenIndex
			{
				position293 := position
				{
					position294, tokenIndex294 := position, tokenIndex
					if buffer[position] != rune('"') {
						goto l295
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l295
					}
					if buffer[position] != rune('"') {
						goto l295
					}
					position++
					goto l294
				l295:
					position, tokenIndex = position294, tokenIndex294
					if buffer[position] != rune('\'') {
						goto l296
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l296
					}
					if buffer[position] != rune('\'') {
						goto l296
					}
					position++
					goto l294
				l296:
					position, tokenIndex = position294, tokenIndex294
					if !_rules[ruletimestampbasicfmt]() {
						goto l292
					}
				}
			l294:
				add(ruletimestampfmt, position293)
			}
			return true
		l292:
			position, tokenIndex = position292, tokenIndex292
			return false
		},
		/* 33 timestamp <- <(<timestampfmt> Action51)> */
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
		/* 43 Action8 <- <{p.startCall("ClearRow")}> */
		nil,
		/* 44 Action9 <- <{p.endCall()}> */
		nil,
		/* 45 Action10 <- <{p.startCall("Store")}> */
		nil,
		/* 46 Action11 <- <{p.endCall()}> */
		nil,
		/* 47 Action12 <- <{p.startCall("TopN")}> */
		nil,
		/* 48 Action13 <- <{p.endCall()}> */
		nil,
		/* 49 Action14 <- <{p.startCall("Range")}> */
		nil,
		/* 50 Action15 <- <{p.endCall()}> */
		nil,
		nil,
		/* 52 Action16 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 53 Action17 <- <{ p.endCall() }> */
		nil,
		/* 54 Action18 <- <{ p.addBTWN() }> */
		nil,
		/* 55 Action19 <- <{ p.addLTE() }> */
		nil,
		/* 56 Action20 <- <{ p.addGTE() }> */
		nil,
		/* 57 Action21 <- <{ p.addEQ() }> */
		nil,
		/* 58 Action22 <- <{ p.addNEQ() }> */
		nil,
		/* 59 Action23 <- <{ p.addLT() }> */
		nil,
		/* 60 Action24 <- <{ p.addGT() }> */
		nil,
		/* 61 Action25 <- <{p.startConditional()}> */
		nil,
		/* 62 Action26 <- <{p.endConditional()}> */
		nil,
		/* 63 Action27 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 64 Action28 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 65 Action29 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 66 Action30 <- <{p.addPosStr("_start", buffer[begin:end])}> */
		nil,
		/* 67 Action31 <- <{p.addPosStr("_end", buffer[begin:end])}> */
		nil,
		/* 68 Action32 <- <{ p.startList() }> */
		nil,
		/* 69 Action33 <- <{ p.endList() }> */
		nil,
		/* 70 Action34 <- <{ p.addVal(nil) }> */
		nil,
		/* 71 Action35 <- <{ p.addVal(true) }> */
		nil,
		/* 72 Action36 <- <{ p.addVal(false) }> */
		nil,
		/* 73 Action37 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 74 Action38 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 75 Action39 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 76 Action40 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 77 Action41 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 78 Action42 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 79 Action43 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 80 Action44 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 81 Action45 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 82 Action46 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 83 Action47 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 84 Action48 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 85 Action49 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 86 Action50 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 87 Action51 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
