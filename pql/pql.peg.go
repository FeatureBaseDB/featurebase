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
	rules  [86]func() bool
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
			s, _ := strconv.Unquote(buffer[begin:end])
			p.addVal(s)
		case ruleAction41:
			p.addVal(buffer[begin:end])
		case ruleAction42:
			p.addField(buffer[begin:end])
		case ruleAction43:
			p.addPosStr("_field", buffer[begin:end])
		case ruleAction44:
			p.addPosNum("_col", buffer[begin:end])
		case ruleAction45:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction46:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction47:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction48:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction49:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction50:
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
								add(ruleAction50, position)
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
								add(ruleAction47, position)
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
								add(ruleAction48, position)
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
								add(ruleAction49, position)
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
		/* 13 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action34) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action35) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action36) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action37) / (<('-'? '.' [0-9]+)> Action38) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action39) / (<('"' doublequotedstring '"')> Action40) / ('\'' <singlequotedstring> '\'' Action41))> */
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
					{
						position194 := position
						if buffer[position] != rune('"') {
							goto l193
						}
						position++
						if !_rules[ruledoublequotedstring]() {
							goto l193
						}
						if buffer[position] != rune('"') {
							goto l193
						}
						position++
						add(rulePegText, position194)
					}
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
		/* 14 doublequotedstring <- <(('\\' '"') / ('\\' '\\') / (!'"' .))*> */
		func() bool {
			{
				position199 := position
			l200:
				{
					position201, tokenIndex201 := position, tokenIndex
					{
						position202, tokenIndex202 := position, tokenIndex
						if buffer[position] != rune('\\') {
							goto l203
						}
						position++
						if buffer[position] != rune('"') {
							goto l203
						}
						position++
						goto l202
					l203:
						position, tokenIndex = position202, tokenIndex202
						if buffer[position] != rune('\\') {
							goto l204
						}
						position++
						if buffer[position] != rune('\\') {
							goto l204
						}
						position++
						goto l202
					l204:
						position, tokenIndex = position202, tokenIndex202
						{
							position205, tokenIndex205 := position, tokenIndex
							if buffer[position] != rune('"') {
								goto l205
							}
							position++
							goto l201
						l205:
							position, tokenIndex = position205, tokenIndex205
						}
						if !matchDot() {
							goto l201
						}
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
		/* 15 singlequotedstring <- <(('\\' '\'') / ('\\' '\\') / (!'\'' .))*> */
		func() bool {
			{
				position207 := position
			l208:
				{
					position209, tokenIndex209 := position, tokenIndex
					{
						position210, tokenIndex210 := position, tokenIndex
						if buffer[position] != rune('\\') {
							goto l211
						}
						position++
						if buffer[position] != rune('\'') {
							goto l211
						}
						position++
						goto l210
					l211:
						position, tokenIndex = position210, tokenIndex210
						if buffer[position] != rune('\\') {
							goto l212
						}
						position++
						if buffer[position] != rune('\\') {
							goto l212
						}
						position++
						goto l210
					l212:
						position, tokenIndex = position210, tokenIndex210
						{
							position213, tokenIndex213 := position, tokenIndex
							if buffer[position] != rune('\'') {
								goto l213
							}
							position++
							goto l209
						l213:
							position, tokenIndex = position213, tokenIndex213
						}
						if !matchDot() {
							goto l209
						}
					}
				l210:
					goto l208
				l209:
					position, tokenIndex = position209, tokenIndex209
				}
				add(rulesinglequotedstring, position207)
			}
			return true
		},
		/* 16 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_' / '-')*)> */
		func() bool {
			position214, tokenIndex214 := position, tokenIndex
			{
				position215 := position
				{
					position216, tokenIndex216 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l217
					}
					position++
					goto l216
				l217:
					position, tokenIndex = position216, tokenIndex216
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l214
					}
					position++
				}
			l216:
			l218:
				{
					position219, tokenIndex219 := position, tokenIndex
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
							goto l222
						}
						position++
						goto l220
					l222:
						position, tokenIndex = position220, tokenIndex220
						if c := buffer[position]; c < rune('0') || c > rune('9') {
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
						goto l220
					l224:
						position, tokenIndex = position220, tokenIndex220
						if buffer[position] != rune('-') {
							goto l219
						}
						position++
					}
				l220:
					goto l218
				l219:
					position, tokenIndex = position219, tokenIndex219
				}
				add(rulefieldExpr, position215)
			}
			return true
		l214:
			position, tokenIndex = position214, tokenIndex214
			return false
		},
		/* 17 field <- <(<(fieldExpr / reserved)> Action42)> */
		func() bool {
			position225, tokenIndex225 := position, tokenIndex
			{
				position226 := position
				{
					position227 := position
					{
						position228, tokenIndex228 := position, tokenIndex
						if !_rules[rulefieldExpr]() {
							goto l229
						}
						goto l228
					l229:
						position, tokenIndex = position228, tokenIndex228
						{
							position230 := position
							{
								position231, tokenIndex231 := position, tokenIndex
								if buffer[position] != rune('_') {
									goto l232
								}
								position++
								if buffer[position] != rune('r') {
									goto l232
								}
								position++
								if buffer[position] != rune('o') {
									goto l232
								}
								position++
								if buffer[position] != rune('w') {
									goto l232
								}
								position++
								goto l231
							l232:
								position, tokenIndex = position231, tokenIndex231
								if buffer[position] != rune('_') {
									goto l233
								}
								position++
								if buffer[position] != rune('c') {
									goto l233
								}
								position++
								if buffer[position] != rune('o') {
									goto l233
								}
								position++
								if buffer[position] != rune('l') {
									goto l233
								}
								position++
								goto l231
							l233:
								position, tokenIndex = position231, tokenIndex231
								if buffer[position] != rune('_') {
									goto l234
								}
								position++
								if buffer[position] != rune('s') {
									goto l234
								}
								position++
								if buffer[position] != rune('t') {
									goto l234
								}
								position++
								if buffer[position] != rune('a') {
									goto l234
								}
								position++
								if buffer[position] != rune('r') {
									goto l234
								}
								position++
								if buffer[position] != rune('t') {
									goto l234
								}
								position++
								goto l231
							l234:
								position, tokenIndex = position231, tokenIndex231
								if buffer[position] != rune('_') {
									goto l235
								}
								position++
								if buffer[position] != rune('e') {
									goto l235
								}
								position++
								if buffer[position] != rune('n') {
									goto l235
								}
								position++
								if buffer[position] != rune('d') {
									goto l235
								}
								position++
								goto l231
							l235:
								position, tokenIndex = position231, tokenIndex231
								if buffer[position] != rune('_') {
									goto l236
								}
								position++
								if buffer[position] != rune('t') {
									goto l236
								}
								position++
								if buffer[position] != rune('i') {
									goto l236
								}
								position++
								if buffer[position] != rune('m') {
									goto l236
								}
								position++
								if buffer[position] != rune('e') {
									goto l236
								}
								position++
								if buffer[position] != rune('s') {
									goto l236
								}
								position++
								if buffer[position] != rune('t') {
									goto l236
								}
								position++
								if buffer[position] != rune('a') {
									goto l236
								}
								position++
								if buffer[position] != rune('m') {
									goto l236
								}
								position++
								if buffer[position] != rune('p') {
									goto l236
								}
								position++
								goto l231
							l236:
								position, tokenIndex = position231, tokenIndex231
								if buffer[position] != rune('_') {
									goto l225
								}
								position++
								if buffer[position] != rune('f') {
									goto l225
								}
								position++
								if buffer[position] != rune('i') {
									goto l225
								}
								position++
								if buffer[position] != rune('e') {
									goto l225
								}
								position++
								if buffer[position] != rune('l') {
									goto l225
								}
								position++
								if buffer[position] != rune('d') {
									goto l225
								}
								position++
							}
						l231:
							add(rulereserved, position230)
						}
					}
				l228:
					add(rulePegText, position227)
				}
				{
					add(ruleAction42, position)
				}
				add(rulefield, position226)
			}
			return true
		l225:
			position, tokenIndex = position225, tokenIndex225
			return false
		},
		/* 18 reserved <- <(('_' 'r' 'o' 'w') / ('_' 'c' 'o' 'l') / ('_' 's' 't' 'a' 'r' 't') / ('_' 'e' 'n' 'd') / ('_' 't' 'i' 'm' 'e' 's' 't' 'a' 'm' 'p') / ('_' 'f' 'i' 'e' 'l' 'd'))> */
		nil,
		/* 19 posfield <- <(<fieldExpr> Action43)> */
		func() bool {
			position239, tokenIndex239 := position, tokenIndex
			{
				position240 := position
				{
					position241 := position
					if !_rules[rulefieldExpr]() {
						goto l239
					}
					add(rulePegText, position241)
				}
				{
					add(ruleAction43, position)
				}
				add(ruleposfield, position240)
			}
			return true
		l239:
			position, tokenIndex = position239, tokenIndex239
			return false
		},
		/* 20 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position243, tokenIndex243 := position, tokenIndex
			{
				position244 := position
				{
					position245, tokenIndex245 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l246
					}
					position++
				l247:
					{
						position248, tokenIndex248 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l248
						}
						position++
						goto l247
					l248:
						position, tokenIndex = position248, tokenIndex248
					}
					goto l245
				l246:
					position, tokenIndex = position245, tokenIndex245
					if buffer[position] != rune('0') {
						goto l243
					}
					position++
				}
			l245:
				add(ruleuint, position244)
			}
			return true
		l243:
			position, tokenIndex = position243, tokenIndex243
			return false
		},
		/* 21 col <- <((<uint> Action44) / ('\'' <singlequotedstring> '\'' Action45) / ('"' <doublequotedstring> '"' Action46))> */
		func() bool {
			position249, tokenIndex249 := position, tokenIndex
			{
				position250 := position
				{
					position251, tokenIndex251 := position, tokenIndex
					{
						position253 := position
						if !_rules[ruleuint]() {
							goto l252
						}
						add(rulePegText, position253)
					}
					{
						add(ruleAction44, position)
					}
					goto l251
				l252:
					position, tokenIndex = position251, tokenIndex251
					if buffer[position] != rune('\'') {
						goto l255
					}
					position++
					{
						position256 := position
						if !_rules[rulesinglequotedstring]() {
							goto l255
						}
						add(rulePegText, position256)
					}
					if buffer[position] != rune('\'') {
						goto l255
					}
					position++
					{
						add(ruleAction45, position)
					}
					goto l251
				l255:
					position, tokenIndex = position251, tokenIndex251
					if buffer[position] != rune('"') {
						goto l249
					}
					position++
					{
						position258 := position
						if !_rules[ruledoublequotedstring]() {
							goto l249
						}
						add(rulePegText, position258)
					}
					if buffer[position] != rune('"') {
						goto l249
					}
					position++
					{
						add(ruleAction46, position)
					}
				}
			l251:
				add(rulecol, position250)
			}
			return true
		l249:
			position, tokenIndex = position249, tokenIndex249
			return false
		},
		/* 22 row <- <((<uint> Action47) / ('\'' <singlequotedstring> '\'' Action48) / ('"' <doublequotedstring> '"' Action49))> */
		nil,
		/* 23 open <- <('(' sp)> */
		func() bool {
			position261, tokenIndex261 := position, tokenIndex
			{
				position262 := position
				if buffer[position] != rune('(') {
					goto l261
				}
				position++
				if !_rules[rulesp]() {
					goto l261
				}
				add(ruleopen, position262)
			}
			return true
		l261:
			position, tokenIndex = position261, tokenIndex261
			return false
		},
		/* 24 close <- <(')' sp)> */
		func() bool {
			position263, tokenIndex263 := position, tokenIndex
			{
				position264 := position
				if buffer[position] != rune(')') {
					goto l263
				}
				position++
				if !_rules[rulesp]() {
					goto l263
				}
				add(ruleclose, position264)
			}
			return true
		l263:
			position, tokenIndex = position263, tokenIndex263
			return false
		},
		/* 25 sp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position266 := position
			l267:
				{
					position268, tokenIndex268 := position, tokenIndex
					{
						position269, tokenIndex269 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l270
						}
						position++
						goto l269
					l270:
						position, tokenIndex = position269, tokenIndex269
						if buffer[position] != rune('\t') {
							goto l271
						}
						position++
						goto l269
					l271:
						position, tokenIndex = position269, tokenIndex269
						if buffer[position] != rune('\n') {
							goto l268
						}
						position++
					}
				l269:
					goto l267
				l268:
					position, tokenIndex = position268, tokenIndex268
				}
				add(rulesp, position266)
			}
			return true
		},
		/* 26 comma <- <(sp ',' sp)> */
		func() bool {
			position272, tokenIndex272 := position, tokenIndex
			{
				position273 := position
				if !_rules[rulesp]() {
					goto l272
				}
				if buffer[position] != rune(',') {
					goto l272
				}
				position++
				if !_rules[rulesp]() {
					goto l272
				}
				add(rulecomma, position273)
			}
			return true
		l272:
			position, tokenIndex = position272, tokenIndex272
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
			position277, tokenIndex277 := position, tokenIndex
			{
				position278 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l277
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l277
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l277
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l277
				}
				position++
				if buffer[position] != rune('-') {
					goto l277
				}
				position++
				{
					position279, tokenIndex279 := position, tokenIndex
					if buffer[position] != rune('0') {
						goto l280
					}
					position++
					goto l279
				l280:
					position, tokenIndex = position279, tokenIndex279
					if buffer[position] != rune('1') {
						goto l277
					}
					position++
				}
			l279:
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l277
				}
				position++
				if buffer[position] != rune('-') {
					goto l277
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('3') {
					goto l277
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l277
				}
				position++
				if buffer[position] != rune('T') {
					goto l277
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l277
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l277
				}
				position++
				if buffer[position] != rune(':') {
					goto l277
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l277
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l277
				}
				position++
				add(ruletimestampbasicfmt, position278)
			}
			return true
		l277:
			position, tokenIndex = position277, tokenIndex277
			return false
		},
		/* 31 timestampfmt <- <(('"' timestampbasicfmt '"') / ('\'' timestampbasicfmt '\'') / timestampbasicfmt)> */
		func() bool {
			position281, tokenIndex281 := position, tokenIndex
			{
				position282 := position
				{
					position283, tokenIndex283 := position, tokenIndex
					if buffer[position] != rune('"') {
						goto l284
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l284
					}
					if buffer[position] != rune('"') {
						goto l284
					}
					position++
					goto l283
				l284:
					position, tokenIndex = position283, tokenIndex283
					if buffer[position] != rune('\'') {
						goto l285
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l285
					}
					if buffer[position] != rune('\'') {
						goto l285
					}
					position++
					goto l283
				l285:
					position, tokenIndex = position283, tokenIndex283
					if !_rules[ruletimestampbasicfmt]() {
						goto l281
					}
				}
			l283:
				add(ruletimestampfmt, position282)
			}
			return true
		l281:
			position, tokenIndex = position281, tokenIndex281
			return false
		},
		/* 32 timestamp <- <(<timestampfmt> Action50)> */
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
		/* 42 Action8 <- <{p.startCall("ClearRow")}> */
		nil,
		/* 43 Action9 <- <{p.endCall()}> */
		nil,
		/* 44 Action10 <- <{p.startCall("Store")}> */
		nil,
		/* 45 Action11 <- <{p.endCall()}> */
		nil,
		/* 46 Action12 <- <{p.startCall("TopN")}> */
		nil,
		/* 47 Action13 <- <{p.endCall()}> */
		nil,
		/* 48 Action14 <- <{p.startCall("Range")}> */
		nil,
		/* 49 Action15 <- <{p.endCall()}> */
		nil,
		nil,
		/* 51 Action16 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 52 Action17 <- <{ p.endCall() }> */
		nil,
		/* 53 Action18 <- <{ p.addBTWN() }> */
		nil,
		/* 54 Action19 <- <{ p.addLTE() }> */
		nil,
		/* 55 Action20 <- <{ p.addGTE() }> */
		nil,
		/* 56 Action21 <- <{ p.addEQ() }> */
		nil,
		/* 57 Action22 <- <{ p.addNEQ() }> */
		nil,
		/* 58 Action23 <- <{ p.addLT() }> */
		nil,
		/* 59 Action24 <- <{ p.addGT() }> */
		nil,
		/* 60 Action25 <- <{p.startConditional()}> */
		nil,
		/* 61 Action26 <- <{p.endConditional()}> */
		nil,
		/* 62 Action27 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 63 Action28 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 64 Action29 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 65 Action30 <- <{p.addPosStr("_start", buffer[begin:end])}> */
		nil,
		/* 66 Action31 <- <{p.addPosStr("_end", buffer[begin:end])}> */
		nil,
		/* 67 Action32 <- <{ p.startList() }> */
		nil,
		/* 68 Action33 <- <{ p.endList() }> */
		nil,
		/* 69 Action34 <- <{ p.addVal(nil) }> */
		nil,
		/* 70 Action35 <- <{ p.addVal(true) }> */
		nil,
		/* 71 Action36 <- <{ p.addVal(false) }> */
		nil,
		/* 72 Action37 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 73 Action38 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 74 Action39 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 75 Action40 <- <{ s, _ := strconv.Unquote(buffer[begin:end]); p.addVal(s) }> */
		nil,
		/* 76 Action41 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 77 Action42 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 78 Action43 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 79 Action44 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 80 Action45 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 81 Action46 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 82 Action47 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 83 Action48 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 84 Action49 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 85 Action50 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
