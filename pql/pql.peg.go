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
			p.startCall("Rows")
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
			p.startList()
		case ruleAction31:
			p.endList()
		case ruleAction32:
			p.addVal(nil)
		case ruleAction33:
			p.addVal(true)
		case ruleAction34:
			p.addVal(false)
		case ruleAction35:
			p.addVal(buffer[begin:end])
		case ruleAction36:
			p.addNumVal(buffer[begin:end])
		case ruleAction37:
			p.addNumVal(buffer[begin:end])
		case ruleAction38:
			p.startCall(buffer[begin:end])
		case ruleAction39:
			p.addVal(p.endCall())
		case ruleAction40:
			p.addVal(buffer[begin:end])
		case ruleAction41:
			s, _ := strconv.Unquote(buffer[begin:end])
			p.addVal(s)
		case ruleAction42:
			p.addVal(buffer[begin:end])
		case ruleAction43:
			p.addField(buffer[begin:end])
		case ruleAction44:
			p.addPosStr("_field", buffer[begin:end])
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
		/* 1 Call <- <(('S' 'e' 't' Action0 open col comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma row comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'u' 'm' 'n' 'A' 't' 't' 'r' 's' Action4 open col comma args close Action5) / ('C' 'l' 'e' 'a' 'r' Action6 open col comma args close Action7) / ('C' 'l' 'e' 'a' 'r' 'R' 'o' 'w' Action8 open arg close Action9) / ('S' 't' 'o' 'r' 'e' Action10 open Call comma arg close Action11) / ('T' 'o' 'p' 'N' Action12 open posfield (comma allargs)? close Action13) / ('R' 'o' 'w' 's' Action14 open posfield (comma allargs)? close Action15) / (<IDENT> Action16 open allargs comma? close Action17))> */
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
					if buffer[position] != rune('o') {
						goto l46
					}
					position++
					if buffer[position] != rune('w') {
						goto l46
					}
					position++
					if buffer[position] != rune('s') {
						goto l46
					}
					position++
					{
						add(ruleAction14, position)
					}
					if !_rules[ruleopen]() {
						goto l46
					}
					if !_rules[ruleposfield]() {
						goto l46
					}
					{
						position48, tokenIndex48 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l48
						}
						if !_rules[ruleallargs]() {
							goto l48
						}
						goto l49
					l48:
						position, tokenIndex = position48, tokenIndex48
					}
				l49:
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
						position51 := position
						if !_rules[ruleIDENT]() {
							goto l5
						}
						add(rulePegText, position51)
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
						position53, tokenIndex53 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l53
						}
						goto l54
					l53:
						position, tokenIndex = position53, tokenIndex53
					}
				l54:
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
			position56, tokenIndex56 := position, tokenIndex
			{
				position57 := position
				{
					position58, tokenIndex58 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l59
					}
				l60:
					{
						position61, tokenIndex61 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l61
						}
						if !_rules[ruleCall]() {
							goto l61
						}
						goto l60
					l61:
						position, tokenIndex = position61, tokenIndex61
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
					goto l58
				l59:
					position, tokenIndex = position58, tokenIndex58
					if !_rules[ruleargs]() {
						goto l64
					}
					goto l58
				l64:
					position, tokenIndex = position58, tokenIndex58
					if !_rules[rulesp]() {
						goto l56
					}
				}
			l58:
				add(ruleallargs, position57)
			}
			return true
		l56:
			position, tokenIndex = position56, tokenIndex56
			return false
		},
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position65, tokenIndex65 := position, tokenIndex
			{
				position66 := position
				if !_rules[rulearg]() {
					goto l65
				}
				{
					position67, tokenIndex67 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l67
					}
					if !_rules[ruleargs]() {
						goto l67
					}
					goto l68
				l67:
					position, tokenIndex = position67, tokenIndex67
				}
			l68:
				if !_rules[rulesp]() {
					goto l65
				}
				add(ruleargs, position66)
			}
			return true
		l65:
			position, tokenIndex = position65, tokenIndex65
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value) / conditional)> */
		func() bool {
			position69, tokenIndex69 := position, tokenIndex
			{
				position70 := position
				{
					position71, tokenIndex71 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l72
					}
					if !_rules[rulesp]() {
						goto l72
					}
					if buffer[position] != rune('=') {
						goto l72
					}
					position++
					if !_rules[rulesp]() {
						goto l72
					}
					if !_rules[rulevalue]() {
						goto l72
					}
					goto l71
				l72:
					position, tokenIndex = position71, tokenIndex71
					if !_rules[rulefield]() {
						goto l73
					}
					if !_rules[rulesp]() {
						goto l73
					}
					{
						position74 := position
						{
							position75, tokenIndex75 := position, tokenIndex
							if buffer[position] != rune('>') {
								goto l76
							}
							position++
							if buffer[position] != rune('<') {
								goto l76
							}
							position++
							{
								add(ruleAction18, position)
							}
							goto l75
						l76:
							position, tokenIndex = position75, tokenIndex75
							if buffer[position] != rune('<') {
								goto l78
							}
							position++
							if buffer[position] != rune('=') {
								goto l78
							}
							position++
							{
								add(ruleAction19, position)
							}
							goto l75
						l78:
							position, tokenIndex = position75, tokenIndex75
							if buffer[position] != rune('>') {
								goto l80
							}
							position++
							if buffer[position] != rune('=') {
								goto l80
							}
							position++
							{
								add(ruleAction20, position)
							}
							goto l75
						l80:
							position, tokenIndex = position75, tokenIndex75
							if buffer[position] != rune('=') {
								goto l82
							}
							position++
							if buffer[position] != rune('=') {
								goto l82
							}
							position++
							{
								add(ruleAction21, position)
							}
							goto l75
						l82:
							position, tokenIndex = position75, tokenIndex75
							if buffer[position] != rune('!') {
								goto l84
							}
							position++
							if buffer[position] != rune('=') {
								goto l84
							}
							position++
							{
								add(ruleAction22, position)
							}
							goto l75
						l84:
							position, tokenIndex = position75, tokenIndex75
							if buffer[position] != rune('<') {
								goto l86
							}
							position++
							{
								add(ruleAction23, position)
							}
							goto l75
						l86:
							position, tokenIndex = position75, tokenIndex75
							if buffer[position] != rune('>') {
								goto l73
							}
							position++
							{
								add(ruleAction24, position)
							}
						}
					l75:
						add(ruleCOND, position74)
					}
					if !_rules[rulesp]() {
						goto l73
					}
					if !_rules[rulevalue]() {
						goto l73
					}
					goto l71
				l73:
					position, tokenIndex = position71, tokenIndex71
					{
						position89 := position
						{
							add(ruleAction25, position)
						}
						if !_rules[rulecondint]() {
							goto l69
						}
						if !_rules[rulecondLT]() {
							goto l69
						}
						{
							position91 := position
							{
								position92 := position
								if !_rules[rulefieldExpr]() {
									goto l69
								}
								add(rulePegText, position92)
							}
							if !_rules[rulesp]() {
								goto l69
							}
							{
								add(ruleAction29, position)
							}
							add(rulecondfield, position91)
						}
						if !_rules[rulecondLT]() {
							goto l69
						}
						if !_rules[rulecondint]() {
							goto l69
						}
						{
							add(ruleAction26, position)
						}
						add(ruleconditional, position89)
					}
				}
			l71:
				add(rulearg, position70)
			}
			return true
		l69:
			position, tokenIndex = position69, tokenIndex69
			return false
		},
		/* 5 COND <- <(('>' '<' Action18) / ('<' '=' Action19) / ('>' '=' Action20) / ('=' '=' Action21) / ('!' '=' Action22) / ('<' Action23) / ('>' Action24))> */
		nil,
		/* 6 conditional <- <(Action25 condint condLT condfield condLT condint Action26)> */
		nil,
		/* 7 condint <- <(<(('-'? [1-9] [0-9]*) / '0')> sp Action27)> */
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
					add(ruleAction27, position)
				}
				add(rulecondint, position98)
			}
			return true
		l97:
			position, tokenIndex = position97, tokenIndex97
			return false
		},
		/* 8 condLT <- <(<(('<' '=') / '<')> sp Action28)> */
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
					add(ruleAction28, position)
				}
				add(rulecondLT, position108)
			}
			return true
		l107:
			position, tokenIndex = position107, tokenIndex107
			return false
		},
		/* 9 condfield <- <(<fieldExpr> sp Action29)> */
		nil,
		/* 10 value <- <(item / (lbrack Action30 list rbrack Action31))> */
		func() bool {
			position114, tokenIndex114 := position, tokenIndex
			{
				position115 := position
				{
					position116, tokenIndex116 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l117
					}
					goto l116
				l117:
					position, tokenIndex = position116, tokenIndex116
					{
						position118 := position
						if buffer[position] != rune('[') {
							goto l114
						}
						position++
						if !_rules[rulesp]() {
							goto l114
						}
						add(rulelbrack, position118)
					}
					{
						add(ruleAction30, position)
					}
					if !_rules[rulelist]() {
						goto l114
					}
					{
						position120 := position
						if !_rules[rulesp]() {
							goto l114
						}
						if buffer[position] != rune(']') {
							goto l114
						}
						position++
						if !_rules[rulesp]() {
							goto l114
						}
						add(rulerbrack, position120)
					}
					{
						add(ruleAction31, position)
					}
				}
			l116:
				add(rulevalue, position115)
			}
			return true
		l114:
			position, tokenIndex = position114, tokenIndex114
			return false
		},
		/* 11 list <- <(item (comma list)?)> */
		func() bool {
			position122, tokenIndex122 := position, tokenIndex
			{
				position123 := position
				if !_rules[ruleitem]() {
					goto l122
				}
				{
					position124, tokenIndex124 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l124
					}
					if !_rules[rulelist]() {
						goto l124
					}
					goto l125
				l124:
					position, tokenIndex = position124, tokenIndex124
				}
			l125:
				add(rulelist, position123)
			}
			return true
		l122:
			position, tokenIndex = position122, tokenIndex122
			return false
		},
		/* 12 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action32) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action33) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action34) / (timestampfmt Action35) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action36) / (<('-'? '.' [0-9]+)> Action37) / (<IDENT> Action38 open allargs comma? close Action39) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action40) / (<('"' doublequotedstring '"')> Action41) / ('\'' <singlequotedstring> '\'' Action42))> */
		func() bool {
			position126, tokenIndex126 := position, tokenIndex
			{
				position127 := position
				{
					position128, tokenIndex128 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l129
					}
					position++
					if buffer[position] != rune('u') {
						goto l129
					}
					position++
					if buffer[position] != rune('l') {
						goto l129
					}
					position++
					if buffer[position] != rune('l') {
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
						add(ruleAction32, position)
					}
					goto l128
				l129:
					position, tokenIndex = position128, tokenIndex128
					if buffer[position] != rune('t') {
						goto l134
					}
					position++
					if buffer[position] != rune('r') {
						goto l134
					}
					position++
					if buffer[position] != rune('u') {
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
						add(ruleAction33, position)
					}
					goto l128
				l134:
					position, tokenIndex = position128, tokenIndex128
					if buffer[position] != rune('f') {
						goto l139
					}
					position++
					if buffer[position] != rune('a') {
						goto l139
					}
					position++
					if buffer[position] != rune('l') {
						goto l139
					}
					position++
					if buffer[position] != rune('s') {
						goto l139
					}
					position++
					if buffer[position] != rune('e') {
						goto l139
					}
					position++
					{
						position140, tokenIndex140 := position, tokenIndex
						{
							position141, tokenIndex141 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l142
							}
							goto l141
						l142:
							position, tokenIndex = position141, tokenIndex141
							if !_rules[rulesp]() {
								goto l139
							}
							if !_rules[ruleclose]() {
								goto l139
							}
						}
					l141:
						position, tokenIndex = position140, tokenIndex140
					}
					{
						add(ruleAction34, position)
					}
					goto l128
				l139:
					position, tokenIndex = position128, tokenIndex128
					if !_rules[ruletimestampfmt]() {
						goto l144
					}
					{
						add(ruleAction35, position)
					}
					goto l128
				l144:
					position, tokenIndex = position128, tokenIndex128
					{
						position147 := position
						{
							position148, tokenIndex148 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l148
							}
							position++
							goto l149
						l148:
							position, tokenIndex = position148, tokenIndex148
						}
					l149:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l146
						}
						position++
					l150:
						{
							position151, tokenIndex151 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l151
							}
							position++
							goto l150
						l151:
							position, tokenIndex = position151, tokenIndex151
						}
						{
							position152, tokenIndex152 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l152
							}
							position++
						l154:
							{
								position155, tokenIndex155 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l155
								}
								position++
								goto l154
							l155:
								position, tokenIndex = position155, tokenIndex155
							}
							goto l153
						l152:
							position, tokenIndex = position152, tokenIndex152
						}
					l153:
						add(rulePegText, position147)
					}
					{
						add(ruleAction36, position)
					}
					goto l128
				l146:
					position, tokenIndex = position128, tokenIndex128
					{
						position158 := position
						{
							position159, tokenIndex159 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l159
							}
							position++
							goto l160
						l159:
							position, tokenIndex = position159, tokenIndex159
						}
					l160:
						if buffer[position] != rune('.') {
							goto l157
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l157
						}
						position++
					l161:
						{
							position162, tokenIndex162 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l162
							}
							position++
							goto l161
						l162:
							position, tokenIndex = position162, tokenIndex162
						}
						add(rulePegText, position158)
					}
					{
						add(ruleAction37, position)
					}
					goto l128
				l157:
					position, tokenIndex = position128, tokenIndex128
					{
						position165 := position
						if !_rules[ruleIDENT]() {
							goto l164
						}
						add(rulePegText, position165)
					}
					{
						add(ruleAction38, position)
					}
					if !_rules[ruleopen]() {
						goto l164
					}
					if !_rules[ruleallargs]() {
						goto l164
					}
					{
						position167, tokenIndex167 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l167
						}
						goto l168
					l167:
						position, tokenIndex = position167, tokenIndex167
					}
				l168:
					if !_rules[ruleclose]() {
						goto l164
					}
					{
						add(ruleAction39, position)
					}
					goto l128
				l164:
					position, tokenIndex = position128, tokenIndex128
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
						add(ruleAction40, position)
					}
					goto l128
				l170:
					position, tokenIndex = position128, tokenIndex128
					{
						position188 := position
						if buffer[position] != rune('"') {
							goto l187
						}
						position++
						if !_rules[ruledoublequotedstring]() {
							goto l187
						}
						if buffer[position] != rune('"') {
							goto l187
						}
						position++
						add(rulePegText, position188)
					}
					{
						add(ruleAction41, position)
					}
					goto l128
				l187:
					position, tokenIndex = position128, tokenIndex128
					if buffer[position] != rune('\'') {
						goto l126
					}
					position++
					{
						position190 := position
						if !_rules[rulesinglequotedstring]() {
							goto l126
						}
						add(rulePegText, position190)
					}
					if buffer[position] != rune('\'') {
						goto l126
					}
					position++
					{
						add(ruleAction42, position)
					}
				}
			l128:
				add(ruleitem, position127)
			}
			return true
		l126:
			position, tokenIndex = position126, tokenIndex126
			return false
		},
		/* 13 doublequotedstring <- <(('\\' '"') / ('\\' '\\') / (!'"' .))*> */
		func() bool {
			{
				position193 := position
			l194:
				{
					position195, tokenIndex195 := position, tokenIndex
					{
						position196, tokenIndex196 := position, tokenIndex
						if buffer[position] != rune('\\') {
							goto l197
						}
						position++
						if buffer[position] != rune('"') {
							goto l197
						}
						position++
						goto l196
					l197:
						position, tokenIndex = position196, tokenIndex196
						if buffer[position] != rune('\\') {
							goto l198
						}
						position++
						if buffer[position] != rune('\\') {
							goto l198
						}
						position++
						goto l196
					l198:
						position, tokenIndex = position196, tokenIndex196
						{
							position199, tokenIndex199 := position, tokenIndex
							if buffer[position] != rune('"') {
								goto l199
							}
							position++
							goto l195
						l199:
							position, tokenIndex = position199, tokenIndex199
						}
						if !matchDot() {
							goto l195
						}
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
		/* 14 singlequotedstring <- <(('\\' '\'') / ('\\' '\\') / (!'\'' .))*> */
		func() bool {
			{
				position201 := position
			l202:
				{
					position203, tokenIndex203 := position, tokenIndex
					{
						position204, tokenIndex204 := position, tokenIndex
						if buffer[position] != rune('\\') {
							goto l205
						}
						position++
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
						if buffer[position] != rune('\\') {
							goto l206
						}
						position++
						goto l204
					l206:
						position, tokenIndex = position204, tokenIndex204
						{
							position207, tokenIndex207 := position, tokenIndex
							if buffer[position] != rune('\'') {
								goto l207
							}
							position++
							goto l203
						l207:
							position, tokenIndex = position207, tokenIndex207
						}
						if !matchDot() {
							goto l203
						}
					}
				l204:
					goto l202
				l203:
					position, tokenIndex = position203, tokenIndex203
				}
				add(rulesinglequotedstring, position201)
			}
			return true
		},
		/* 15 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_' / '-')*)> */
		func() bool {
			position208, tokenIndex208 := position, tokenIndex
			{
				position209 := position
				{
					position210, tokenIndex210 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l211
					}
					position++
					goto l210
				l211:
					position, tokenIndex = position210, tokenIndex210
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l208
					}
					position++
				}
			l210:
			l212:
				{
					position213, tokenIndex213 := position, tokenIndex
					{
						position214, tokenIndex214 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l215
						}
						position++
						goto l214
					l215:
						position, tokenIndex = position214, tokenIndex214
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l216
						}
						position++
						goto l214
					l216:
						position, tokenIndex = position214, tokenIndex214
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l217
						}
						position++
						goto l214
					l217:
						position, tokenIndex = position214, tokenIndex214
						if buffer[position] != rune('_') {
							goto l218
						}
						position++
						goto l214
					l218:
						position, tokenIndex = position214, tokenIndex214
						if buffer[position] != rune('-') {
							goto l213
						}
						position++
					}
				l214:
					goto l212
				l213:
					position, tokenIndex = position213, tokenIndex213
				}
				add(rulefieldExpr, position209)
			}
			return true
		l208:
			position, tokenIndex = position208, tokenIndex208
			return false
		},
		/* 16 field <- <(<(fieldExpr / reserved)> Action43)> */
		func() bool {
			position219, tokenIndex219 := position, tokenIndex
			{
				position220 := position
				{
					position221 := position
					{
						position222, tokenIndex222 := position, tokenIndex
						if !_rules[rulefieldExpr]() {
							goto l223
						}
						goto l222
					l223:
						position, tokenIndex = position222, tokenIndex222
						{
							position224 := position
							{
								position225, tokenIndex225 := position, tokenIndex
								if buffer[position] != rune('_') {
									goto l226
								}
								position++
								if buffer[position] != rune('r') {
									goto l226
								}
								position++
								if buffer[position] != rune('o') {
									goto l226
								}
								position++
								if buffer[position] != rune('w') {
									goto l226
								}
								position++
								goto l225
							l226:
								position, tokenIndex = position225, tokenIndex225
								if buffer[position] != rune('_') {
									goto l227
								}
								position++
								if buffer[position] != rune('c') {
									goto l227
								}
								position++
								if buffer[position] != rune('o') {
									goto l227
								}
								position++
								if buffer[position] != rune('l') {
									goto l227
								}
								position++
								goto l225
							l227:
								position, tokenIndex = position225, tokenIndex225
								if buffer[position] != rune('_') {
									goto l228
								}
								position++
								if buffer[position] != rune('s') {
									goto l228
								}
								position++
								if buffer[position] != rune('t') {
									goto l228
								}
								position++
								if buffer[position] != rune('a') {
									goto l228
								}
								position++
								if buffer[position] != rune('r') {
									goto l228
								}
								position++
								if buffer[position] != rune('t') {
									goto l228
								}
								position++
								goto l225
							l228:
								position, tokenIndex = position225, tokenIndex225
								if buffer[position] != rune('_') {
									goto l229
								}
								position++
								if buffer[position] != rune('e') {
									goto l229
								}
								position++
								if buffer[position] != rune('n') {
									goto l229
								}
								position++
								if buffer[position] != rune('d') {
									goto l229
								}
								position++
								goto l225
							l229:
								position, tokenIndex = position225, tokenIndex225
								if buffer[position] != rune('_') {
									goto l230
								}
								position++
								if buffer[position] != rune('t') {
									goto l230
								}
								position++
								if buffer[position] != rune('i') {
									goto l230
								}
								position++
								if buffer[position] != rune('m') {
									goto l230
								}
								position++
								if buffer[position] != rune('e') {
									goto l230
								}
								position++
								if buffer[position] != rune('s') {
									goto l230
								}
								position++
								if buffer[position] != rune('t') {
									goto l230
								}
								position++
								if buffer[position] != rune('a') {
									goto l230
								}
								position++
								if buffer[position] != rune('m') {
									goto l230
								}
								position++
								if buffer[position] != rune('p') {
									goto l230
								}
								position++
								goto l225
							l230:
								position, tokenIndex = position225, tokenIndex225
								if buffer[position] != rune('_') {
									goto l219
								}
								position++
								if buffer[position] != rune('f') {
									goto l219
								}
								position++
								if buffer[position] != rune('i') {
									goto l219
								}
								position++
								if buffer[position] != rune('e') {
									goto l219
								}
								position++
								if buffer[position] != rune('l') {
									goto l219
								}
								position++
								if buffer[position] != rune('d') {
									goto l219
								}
								position++
							}
						l225:
							add(rulereserved, position224)
						}
					}
				l222:
					add(rulePegText, position221)
				}
				{
					add(ruleAction43, position)
				}
				add(rulefield, position220)
			}
			return true
		l219:
			position, tokenIndex = position219, tokenIndex219
			return false
		},
		/* 17 reserved <- <(('_' 'r' 'o' 'w') / ('_' 'c' 'o' 'l') / ('_' 's' 't' 'a' 'r' 't') / ('_' 'e' 'n' 'd') / ('_' 't' 'i' 'm' 'e' 's' 't' 'a' 'm' 'p') / ('_' 'f' 'i' 'e' 'l' 'd'))> */
		nil,
		/* 18 posfield <- <(<fieldExpr> Action44)> */
		func() bool {
			position233, tokenIndex233 := position, tokenIndex
			{
				position234 := position
				{
					position235 := position
					if !_rules[rulefieldExpr]() {
						goto l233
					}
					add(rulePegText, position235)
				}
				{
					add(ruleAction44, position)
				}
				add(ruleposfield, position234)
			}
			return true
		l233:
			position, tokenIndex = position233, tokenIndex233
			return false
		},
		/* 19 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position237, tokenIndex237 := position, tokenIndex
			{
				position238 := position
				{
					position239, tokenIndex239 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l240
					}
					position++
				l241:
					{
						position242, tokenIndex242 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l242
						}
						position++
						goto l241
					l242:
						position, tokenIndex = position242, tokenIndex242
					}
					goto l239
				l240:
					position, tokenIndex = position239, tokenIndex239
					if buffer[position] != rune('0') {
						goto l237
					}
					position++
				}
			l239:
				add(ruleuint, position238)
			}
			return true
		l237:
			position, tokenIndex = position237, tokenIndex237
			return false
		},
		/* 20 col <- <((<uint> Action45) / ('\'' <singlequotedstring> '\'' Action46) / ('"' <doublequotedstring> '"' Action47))> */
		func() bool {
			position243, tokenIndex243 := position, tokenIndex
			{
				position244 := position
				{
					position245, tokenIndex245 := position, tokenIndex
					{
						position247 := position
						if !_rules[ruleuint]() {
							goto l246
						}
						add(rulePegText, position247)
					}
					{
						add(ruleAction45, position)
					}
					goto l245
				l246:
					position, tokenIndex = position245, tokenIndex245
					if buffer[position] != rune('\'') {
						goto l249
					}
					position++
					{
						position250 := position
						if !_rules[rulesinglequotedstring]() {
							goto l249
						}
						add(rulePegText, position250)
					}
					if buffer[position] != rune('\'') {
						goto l249
					}
					position++
					{
						add(ruleAction46, position)
					}
					goto l245
				l249:
					position, tokenIndex = position245, tokenIndex245
					if buffer[position] != rune('"') {
						goto l243
					}
					position++
					{
						position252 := position
						if !_rules[ruledoublequotedstring]() {
							goto l243
						}
						add(rulePegText, position252)
					}
					if buffer[position] != rune('"') {
						goto l243
					}
					position++
					{
						add(ruleAction47, position)
					}
				}
			l245:
				add(rulecol, position244)
			}
			return true
		l243:
			position, tokenIndex = position243, tokenIndex243
			return false
		},
		/* 21 row <- <((<uint> Action48) / ('\'' <singlequotedstring> '\'' Action49) / ('"' <doublequotedstring> '"' Action50))> */
		nil,
		/* 22 open <- <('(' sp)> */
		func() bool {
			position255, tokenIndex255 := position, tokenIndex
			{
				position256 := position
				if buffer[position] != rune('(') {
					goto l255
				}
				position++
				if !_rules[rulesp]() {
					goto l255
				}
				add(ruleopen, position256)
			}
			return true
		l255:
			position, tokenIndex = position255, tokenIndex255
			return false
		},
		/* 23 close <- <(')' sp)> */
		func() bool {
			position257, tokenIndex257 := position, tokenIndex
			{
				position258 := position
				if buffer[position] != rune(')') {
					goto l257
				}
				position++
				if !_rules[rulesp]() {
					goto l257
				}
				add(ruleclose, position258)
			}
			return true
		l257:
			position, tokenIndex = position257, tokenIndex257
			return false
		},
		/* 24 sp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position260 := position
			l261:
				{
					position262, tokenIndex262 := position, tokenIndex
					{
						position263, tokenIndex263 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l264
						}
						position++
						goto l263
					l264:
						position, tokenIndex = position263, tokenIndex263
						if buffer[position] != rune('\t') {
							goto l265
						}
						position++
						goto l263
					l265:
						position, tokenIndex = position263, tokenIndex263
						if buffer[position] != rune('\n') {
							goto l262
						}
						position++
					}
				l263:
					goto l261
				l262:
					position, tokenIndex = position262, tokenIndex262
				}
				add(rulesp, position260)
			}
			return true
		},
		/* 25 comma <- <(sp ',' sp)> */
		func() bool {
			position266, tokenIndex266 := position, tokenIndex
			{
				position267 := position
				if !_rules[rulesp]() {
					goto l266
				}
				if buffer[position] != rune(',') {
					goto l266
				}
				position++
				if !_rules[rulesp]() {
					goto l266
				}
				add(rulecomma, position267)
			}
			return true
		l266:
			position, tokenIndex = position266, tokenIndex266
			return false
		},
		/* 26 lbrack <- <('[' sp)> */
		nil,
		/* 27 rbrack <- <(sp ']' sp)> */
		nil,
		/* 28 IDENT <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9])*)> */
		func() bool {
			position270, tokenIndex270 := position, tokenIndex
			{
				position271 := position
				{
					position272, tokenIndex272 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l273
					}
					position++
					goto l272
				l273:
					position, tokenIndex = position272, tokenIndex272
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l270
					}
					position++
				}
			l272:
			l274:
				{
					position275, tokenIndex275 := position, tokenIndex
					{
						position276, tokenIndex276 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l277
						}
						position++
						goto l276
					l277:
						position, tokenIndex = position276, tokenIndex276
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l278
						}
						position++
						goto l276
					l278:
						position, tokenIndex = position276, tokenIndex276
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l275
						}
						position++
					}
				l276:
					goto l274
				l275:
					position, tokenIndex = position275, tokenIndex275
				}
				add(ruleIDENT, position271)
			}
			return true
		l270:
			position, tokenIndex = position270, tokenIndex270
			return false
		},
		/* 29 timestampbasicfmt <- <([0-9] [0-9] [0-9] [0-9] '-' ('0' / '1') [0-9] '-' [0-3] [0-9] 'T' [0-9] [0-9] ':' [0-9] [0-9])> */
		func() bool {
			position279, tokenIndex279 := position, tokenIndex
			{
				position280 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l279
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l279
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l279
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l279
				}
				position++
				if buffer[position] != rune('-') {
					goto l279
				}
				position++
				{
					position281, tokenIndex281 := position, tokenIndex
					if buffer[position] != rune('0') {
						goto l282
					}
					position++
					goto l281
				l282:
					position, tokenIndex = position281, tokenIndex281
					if buffer[position] != rune('1') {
						goto l279
					}
					position++
				}
			l281:
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l279
				}
				position++
				if buffer[position] != rune('-') {
					goto l279
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('3') {
					goto l279
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l279
				}
				position++
				if buffer[position] != rune('T') {
					goto l279
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l279
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l279
				}
				position++
				if buffer[position] != rune(':') {
					goto l279
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l279
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l279
				}
				position++
				add(ruletimestampbasicfmt, position280)
			}
			return true
		l279:
			position, tokenIndex = position279, tokenIndex279
			return false
		},
		/* 30 timestampfmt <- <(('"' <timestampbasicfmt> '"') / ('\'' <timestampbasicfmt> '\'') / <timestampbasicfmt>)> */
		func() bool {
			position283, tokenIndex283 := position, tokenIndex
			{
				position284 := position
				{
					position285, tokenIndex285 := position, tokenIndex
					if buffer[position] != rune('"') {
						goto l286
					}
					position++
					{
						position287 := position
						if !_rules[ruletimestampbasicfmt]() {
							goto l286
						}
						add(rulePegText, position287)
					}
					if buffer[position] != rune('"') {
						goto l286
					}
					position++
					goto l285
				l286:
					position, tokenIndex = position285, tokenIndex285
					if buffer[position] != rune('\'') {
						goto l288
					}
					position++
					{
						position289 := position
						if !_rules[ruletimestampbasicfmt]() {
							goto l288
						}
						add(rulePegText, position289)
					}
					if buffer[position] != rune('\'') {
						goto l288
					}
					position++
					goto l285
				l288:
					position, tokenIndex = position285, tokenIndex285
					{
						position290 := position
						if !_rules[ruletimestampbasicfmt]() {
							goto l283
						}
						add(rulePegText, position290)
					}
				}
			l285:
				add(ruletimestampfmt, position284)
			}
			return true
		l283:
			position, tokenIndex = position283, tokenIndex283
			return false
		},
		/* 31 timestamp <- <(<timestampfmt> Action51)> */
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
		/* 47 Action14 <- <{p.startCall("Rows")}> */
		nil,
		/* 48 Action15 <- <{p.endCall()}> */
		nil,
		nil,
		/* 50 Action16 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 51 Action17 <- <{ p.endCall() }> */
		nil,
		/* 52 Action18 <- <{ p.addBTWN() }> */
		nil,
		/* 53 Action19 <- <{ p.addLTE() }> */
		nil,
		/* 54 Action20 <- <{ p.addGTE() }> */
		nil,
		/* 55 Action21 <- <{ p.addEQ() }> */
		nil,
		/* 56 Action22 <- <{ p.addNEQ() }> */
		nil,
		/* 57 Action23 <- <{ p.addLT() }> */
		nil,
		/* 58 Action24 <- <{ p.addGT() }> */
		nil,
		/* 59 Action25 <- <{p.startConditional()}> */
		nil,
		/* 60 Action26 <- <{p.endConditional()}> */
		nil,
		/* 61 Action27 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 62 Action28 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 63 Action29 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 64 Action30 <- <{ p.startList() }> */
		nil,
		/* 65 Action31 <- <{ p.endList() }> */
		nil,
		/* 66 Action32 <- <{ p.addVal(nil) }> */
		nil,
		/* 67 Action33 <- <{ p.addVal(true) }> */
		nil,
		/* 68 Action34 <- <{ p.addVal(false) }> */
		nil,
		/* 69 Action35 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 70 Action36 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 71 Action37 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 72 Action38 <- <{ p.startCall(buffer[begin:end]) }> */
		nil,
		/* 73 Action39 <- <{ p.addVal(p.endCall()) }> */
		nil,
		/* 74 Action40 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 75 Action41 <- <{ s, _ := strconv.Unquote(buffer[begin:end]); p.addVal(s) }> */
		nil,
		/* 76 Action42 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 77 Action43 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 78 Action44 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 79 Action45 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 80 Action46 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 81 Action47 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 82 Action48 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 83 Action49 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 84 Action50 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 85 Action51 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
