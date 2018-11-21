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
	ruleAction51
	ruleAction52
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
	"Action51",
	"Action52",
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
			p.startCall(buffer[begin:end])
		case ruleAction40:
			p.addVal(p.endCall())
		case ruleAction41:
			p.addVal(buffer[begin:end])
		case ruleAction42:
			s, _ := strconv.Unquote(buffer[begin:end])
			p.addVal(s)
		case ruleAction43:
			p.addVal(buffer[begin:end])
		case ruleAction44:
			p.addField(buffer[begin:end])
		case ruleAction45:
			p.addPosStr("_field", buffer[begin:end])
		case ruleAction46:
			p.addPosNum("_col", buffer[begin:end])
		case ruleAction47:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction48:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction49:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction50:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction51:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction52:
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
								add(ruleAction52, position)
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
								add(ruleAction49, position)
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
								add(ruleAction50, position)
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
								add(ruleAction51, position)
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
						if !_rules[ruleIDENT]() {
							goto l5
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
						position65, tokenIndex65 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l65
						}
						goto l66
					l65:
						position, tokenIndex = position65, tokenIndex65
					}
				l66:
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
			position68, tokenIndex68 := position, tokenIndex
			{
				position69 := position
				{
					position70, tokenIndex70 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l71
					}
				l72:
					{
						position73, tokenIndex73 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l73
						}
						if !_rules[ruleCall]() {
							goto l73
						}
						goto l72
					l73:
						position, tokenIndex = position73, tokenIndex73
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
					goto l70
				l71:
					position, tokenIndex = position70, tokenIndex70
					if !_rules[ruleargs]() {
						goto l76
					}
					goto l70
				l76:
					position, tokenIndex = position70, tokenIndex70
					if !_rules[rulesp]() {
						goto l68
					}
				}
			l70:
				add(ruleallargs, position69)
			}
			return true
		l68:
			position, tokenIndex = position68, tokenIndex68
			return false
		},
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position77, tokenIndex77 := position, tokenIndex
			{
				position78 := position
				if !_rules[rulearg]() {
					goto l77
				}
				{
					position79, tokenIndex79 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l79
					}
					if !_rules[ruleargs]() {
						goto l79
					}
					goto l80
				l79:
					position, tokenIndex = position79, tokenIndex79
				}
			l80:
				if !_rules[rulesp]() {
					goto l77
				}
				add(ruleargs, position78)
			}
			return true
		l77:
			position, tokenIndex = position77, tokenIndex77
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value))> */
		func() bool {
			position81, tokenIndex81 := position, tokenIndex
			{
				position82 := position
				{
					position83, tokenIndex83 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l84
					}
					if !_rules[rulesp]() {
						goto l84
					}
					if buffer[position] != rune('=') {
						goto l84
					}
					position++
					if !_rules[rulesp]() {
						goto l84
					}
					if !_rules[rulevalue]() {
						goto l84
					}
					goto l83
				l84:
					position, tokenIndex = position83, tokenIndex83
					if !_rules[rulefield]() {
						goto l81
					}
					if !_rules[rulesp]() {
						goto l81
					}
					{
						position85 := position
						{
							position86, tokenIndex86 := position, tokenIndex
							if buffer[position] != rune('>') {
								goto l87
							}
							position++
							if buffer[position] != rune('<') {
								goto l87
							}
							position++
							{
								add(ruleAction18, position)
							}
							goto l86
						l87:
							position, tokenIndex = position86, tokenIndex86
							if buffer[position] != rune('<') {
								goto l89
							}
							position++
							if buffer[position] != rune('=') {
								goto l89
							}
							position++
							{
								add(ruleAction19, position)
							}
							goto l86
						l89:
							position, tokenIndex = position86, tokenIndex86
							if buffer[position] != rune('>') {
								goto l91
							}
							position++
							if buffer[position] != rune('=') {
								goto l91
							}
							position++
							{
								add(ruleAction20, position)
							}
							goto l86
						l91:
							position, tokenIndex = position86, tokenIndex86
							if buffer[position] != rune('=') {
								goto l93
							}
							position++
							if buffer[position] != rune('=') {
								goto l93
							}
							position++
							{
								add(ruleAction21, position)
							}
							goto l86
						l93:
							position, tokenIndex = position86, tokenIndex86
							if buffer[position] != rune('!') {
								goto l95
							}
							position++
							if buffer[position] != rune('=') {
								goto l95
							}
							position++
							{
								add(ruleAction22, position)
							}
							goto l86
						l95:
							position, tokenIndex = position86, tokenIndex86
							if buffer[position] != rune('<') {
								goto l97
							}
							position++
							{
								add(ruleAction23, position)
							}
							goto l86
						l97:
							position, tokenIndex = position86, tokenIndex86
							if buffer[position] != rune('>') {
								goto l81
							}
							position++
							{
								add(ruleAction24, position)
							}
						}
					l86:
						add(ruleCOND, position85)
					}
					if !_rules[rulesp]() {
						goto l81
					}
					if !_rules[rulevalue]() {
						goto l81
					}
				}
			l83:
				add(rulearg, position82)
			}
			return true
		l81:
			position, tokenIndex = position81, tokenIndex81
			return false
		},
		/* 5 COND <- <(('>' '<' Action18) / ('<' '=' Action19) / ('>' '=' Action20) / ('=' '=' Action21) / ('!' '=' Action22) / ('<' Action23) / ('>' Action24))> */
		nil,
		/* 6 conditional <- <(Action25 condint condLT condfield condLT condint Action26)> */
		nil,
		/* 7 condint <- <(<(('-'? [1-9] [0-9]*) / '0')> sp Action27)> */
		func() bool {
			position102, tokenIndex102 := position, tokenIndex
			{
				position103 := position
				{
					position104 := position
					{
						position105, tokenIndex105 := position, tokenIndex
						{
							position107, tokenIndex107 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l107
							}
							position++
							goto l108
						l107:
							position, tokenIndex = position107, tokenIndex107
						}
					l108:
						if c := buffer[position]; c < rune('1') || c > rune('9') {
							goto l106
						}
						position++
					l109:
						{
							position110, tokenIndex110 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l110
							}
							position++
							goto l109
						l110:
							position, tokenIndex = position110, tokenIndex110
						}
						goto l105
					l106:
						position, tokenIndex = position105, tokenIndex105
						if buffer[position] != rune('0') {
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
					add(ruleAction27, position)
				}
				add(rulecondint, position103)
			}
			return true
		l102:
			position, tokenIndex = position102, tokenIndex102
			return false
		},
		/* 8 condLT <- <(<(('<' '=') / '<')> sp Action28)> */
		func() bool {
			position112, tokenIndex112 := position, tokenIndex
			{
				position113 := position
				{
					position114 := position
					{
						position115, tokenIndex115 := position, tokenIndex
						if buffer[position] != rune('<') {
							goto l116
						}
						position++
						if buffer[position] != rune('=') {
							goto l116
						}
						position++
						goto l115
					l116:
						position, tokenIndex = position115, tokenIndex115
						if buffer[position] != rune('<') {
							goto l112
						}
						position++
					}
				l115:
					add(rulePegText, position114)
				}
				if !_rules[rulesp]() {
					goto l112
				}
				{
					add(ruleAction28, position)
				}
				add(rulecondLT, position113)
			}
			return true
		l112:
			position, tokenIndex = position112, tokenIndex112
			return false
		},
		/* 9 condfield <- <(<fieldExpr> sp Action29)> */
		nil,
		/* 10 timerange <- <(field sp '=' sp value comma <timestampfmt> Action30 comma <timestampfmt> Action31)> */
		nil,
		/* 11 value <- <(item / (lbrack Action32 list rbrack Action33))> */
		func() bool {
			position120, tokenIndex120 := position, tokenIndex
			{
				position121 := position
				{
					position122, tokenIndex122 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l123
					}
					goto l122
				l123:
					position, tokenIndex = position122, tokenIndex122
					{
						position124 := position
						if buffer[position] != rune('[') {
							goto l120
						}
						position++
						if !_rules[rulesp]() {
							goto l120
						}
						add(rulelbrack, position124)
					}
					{
						add(ruleAction32, position)
					}
					if !_rules[rulelist]() {
						goto l120
					}
					{
						position126 := position
						if !_rules[rulesp]() {
							goto l120
						}
						if buffer[position] != rune(']') {
							goto l120
						}
						position++
						if !_rules[rulesp]() {
							goto l120
						}
						add(rulerbrack, position126)
					}
					{
						add(ruleAction33, position)
					}
				}
			l122:
				add(rulevalue, position121)
			}
			return true
		l120:
			position, tokenIndex = position120, tokenIndex120
			return false
		},
		/* 12 list <- <(item (comma list)?)> */
		func() bool {
			position128, tokenIndex128 := position, tokenIndex
			{
				position129 := position
				if !_rules[ruleitem]() {
					goto l128
				}
				{
					position130, tokenIndex130 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l130
					}
					if !_rules[rulelist]() {
						goto l130
					}
					goto l131
				l130:
					position, tokenIndex = position130, tokenIndex130
				}
			l131:
				add(rulelist, position129)
			}
			return true
		l128:
			position, tokenIndex = position128, tokenIndex128
			return false
		},
		/* 13 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action34) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action35) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action36) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action37) / (<('-'? '.' [0-9]+)> Action38) / (<IDENT> Action39 open allargs comma? close Action40) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action41) / (<('"' doublequotedstring '"')> Action42) / ('\'' <singlequotedstring> '\'' Action43))> */
		func() bool {
			position132, tokenIndex132 := position, tokenIndex
			{
				position133 := position
				{
					position134, tokenIndex134 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l135
					}
					position++
					if buffer[position] != rune('u') {
						goto l135
					}
					position++
					if buffer[position] != rune('l') {
						goto l135
					}
					position++
					if buffer[position] != rune('l') {
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
						add(ruleAction34, position)
					}
					goto l134
				l135:
					position, tokenIndex = position134, tokenIndex134
					if buffer[position] != rune('t') {
						goto l140
					}
					position++
					if buffer[position] != rune('r') {
						goto l140
					}
					position++
					if buffer[position] != rune('u') {
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
						add(ruleAction35, position)
					}
					goto l134
				l140:
					position, tokenIndex = position134, tokenIndex134
					if buffer[position] != rune('f') {
						goto l145
					}
					position++
					if buffer[position] != rune('a') {
						goto l145
					}
					position++
					if buffer[position] != rune('l') {
						goto l145
					}
					position++
					if buffer[position] != rune('s') {
						goto l145
					}
					position++
					if buffer[position] != rune('e') {
						goto l145
					}
					position++
					{
						position146, tokenIndex146 := position, tokenIndex
						{
							position147, tokenIndex147 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l148
							}
							goto l147
						l148:
							position, tokenIndex = position147, tokenIndex147
							if !_rules[rulesp]() {
								goto l145
							}
							if !_rules[ruleclose]() {
								goto l145
							}
						}
					l147:
						position, tokenIndex = position146, tokenIndex146
					}
					{
						add(ruleAction36, position)
					}
					goto l134
				l145:
					position, tokenIndex = position134, tokenIndex134
					{
						position151 := position
						{
							position152, tokenIndex152 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l152
							}
							position++
							goto l153
						l152:
							position, tokenIndex = position152, tokenIndex152
						}
					l153:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l150
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
						{
							position156, tokenIndex156 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l156
							}
							position++
						l158:
							{
								position159, tokenIndex159 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l159
								}
								position++
								goto l158
							l159:
								position, tokenIndex = position159, tokenIndex159
							}
							goto l157
						l156:
							position, tokenIndex = position156, tokenIndex156
						}
					l157:
						add(rulePegText, position151)
					}
					{
						add(ruleAction37, position)
					}
					goto l134
				l150:
					position, tokenIndex = position134, tokenIndex134
					{
						position162 := position
						{
							position163, tokenIndex163 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l163
							}
							position++
							goto l164
						l163:
							position, tokenIndex = position163, tokenIndex163
						}
					l164:
						if buffer[position] != rune('.') {
							goto l161
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l161
						}
						position++
					l165:
						{
							position166, tokenIndex166 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l166
							}
							position++
							goto l165
						l166:
							position, tokenIndex = position166, tokenIndex166
						}
						add(rulePegText, position162)
					}
					{
						add(ruleAction38, position)
					}
					goto l134
				l161:
					position, tokenIndex = position134, tokenIndex134
					{
						position169 := position
						if !_rules[ruleIDENT]() {
							goto l168
						}
						add(rulePegText, position169)
					}
					{
						add(ruleAction39, position)
					}
					if !_rules[ruleopen]() {
						goto l168
					}
					if !_rules[ruleallargs]() {
						goto l168
					}
					{
						position171, tokenIndex171 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l171
						}
						goto l172
					l171:
						position, tokenIndex = position171, tokenIndex171
					}
				l172:
					if !_rules[ruleclose]() {
						goto l168
					}
					{
						add(ruleAction40, position)
					}
					goto l134
				l168:
					position, tokenIndex = position134, tokenIndex134
					{
						position175 := position
						{
							position178, tokenIndex178 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l179
							}
							position++
							goto l178
						l179:
							position, tokenIndex = position178, tokenIndex178
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l180
							}
							position++
							goto l178
						l180:
							position, tokenIndex = position178, tokenIndex178
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l181
							}
							position++
							goto l178
						l181:
							position, tokenIndex = position178, tokenIndex178
							if buffer[position] != rune('-') {
								goto l182
							}
							position++
							goto l178
						l182:
							position, tokenIndex = position178, tokenIndex178
							if buffer[position] != rune('_') {
								goto l183
							}
							position++
							goto l178
						l183:
							position, tokenIndex = position178, tokenIndex178
							if buffer[position] != rune(':') {
								goto l174
							}
							position++
						}
					l178:
					l176:
						{
							position177, tokenIndex177 := position, tokenIndex
							{
								position184, tokenIndex184 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l185
								}
								position++
								goto l184
							l185:
								position, tokenIndex = position184, tokenIndex184
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l186
								}
								position++
								goto l184
							l186:
								position, tokenIndex = position184, tokenIndex184
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l187
								}
								position++
								goto l184
							l187:
								position, tokenIndex = position184, tokenIndex184
								if buffer[position] != rune('-') {
									goto l188
								}
								position++
								goto l184
							l188:
								position, tokenIndex = position184, tokenIndex184
								if buffer[position] != rune('_') {
									goto l189
								}
								position++
								goto l184
							l189:
								position, tokenIndex = position184, tokenIndex184
								if buffer[position] != rune(':') {
									goto l177
								}
								position++
							}
						l184:
							goto l176
						l177:
							position, tokenIndex = position177, tokenIndex177
						}
						add(rulePegText, position175)
					}
					{
						add(ruleAction41, position)
					}
					goto l134
				l174:
					position, tokenIndex = position134, tokenIndex134
					{
						position192 := position
						if buffer[position] != rune('"') {
							goto l191
						}
						position++
						if !_rules[ruledoublequotedstring]() {
							goto l191
						}
						if buffer[position] != rune('"') {
							goto l191
						}
						position++
						add(rulePegText, position192)
					}
					{
						add(ruleAction42, position)
					}
					goto l134
				l191:
					position, tokenIndex = position134, tokenIndex134
					if buffer[position] != rune('\'') {
						goto l132
					}
					position++
					{
						position194 := position
						if !_rules[rulesinglequotedstring]() {
							goto l132
						}
						add(rulePegText, position194)
					}
					if buffer[position] != rune('\'') {
						goto l132
					}
					position++
					{
						add(ruleAction43, position)
					}
				}
			l134:
				add(ruleitem, position133)
			}
			return true
		l132:
			position, tokenIndex = position132, tokenIndex132
			return false
		},
		/* 14 doublequotedstring <- <(('\\' '"') / ('\\' '\\') / (!'"' .))*> */
		func() bool {
			{
				position197 := position
			l198:
				{
					position199, tokenIndex199 := position, tokenIndex
					{
						position200, tokenIndex200 := position, tokenIndex
						if buffer[position] != rune('\\') {
							goto l201
						}
						position++
						if buffer[position] != rune('"') {
							goto l201
						}
						position++
						goto l200
					l201:
						position, tokenIndex = position200, tokenIndex200
						if buffer[position] != rune('\\') {
							goto l202
						}
						position++
						if buffer[position] != rune('\\') {
							goto l202
						}
						position++
						goto l200
					l202:
						position, tokenIndex = position200, tokenIndex200
						{
							position203, tokenIndex203 := position, tokenIndex
							if buffer[position] != rune('"') {
								goto l203
							}
							position++
							goto l199
						l203:
							position, tokenIndex = position203, tokenIndex203
						}
						if !matchDot() {
							goto l199
						}
					}
				l200:
					goto l198
				l199:
					position, tokenIndex = position199, tokenIndex199
				}
				add(ruledoublequotedstring, position197)
			}
			return true
		},
		/* 15 singlequotedstring <- <(('\\' '\'') / ('\\' '\\') / (!'\'' .))*> */
		func() bool {
			{
				position205 := position
			l206:
				{
					position207, tokenIndex207 := position, tokenIndex
					{
						position208, tokenIndex208 := position, tokenIndex
						if buffer[position] != rune('\\') {
							goto l209
						}
						position++
						if buffer[position] != rune('\'') {
							goto l209
						}
						position++
						goto l208
					l209:
						position, tokenIndex = position208, tokenIndex208
						if buffer[position] != rune('\\') {
							goto l210
						}
						position++
						if buffer[position] != rune('\\') {
							goto l210
						}
						position++
						goto l208
					l210:
						position, tokenIndex = position208, tokenIndex208
						{
							position211, tokenIndex211 := position, tokenIndex
							if buffer[position] != rune('\'') {
								goto l211
							}
							position++
							goto l207
						l211:
							position, tokenIndex = position211, tokenIndex211
						}
						if !matchDot() {
							goto l207
						}
					}
				l208:
					goto l206
				l207:
					position, tokenIndex = position207, tokenIndex207
				}
				add(rulesinglequotedstring, position205)
			}
			return true
		},
		/* 16 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_' / '-')*)> */
		func() bool {
			position212, tokenIndex212 := position, tokenIndex
			{
				position213 := position
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
						goto l212
					}
					position++
				}
			l214:
			l216:
				{
					position217, tokenIndex217 := position, tokenIndex
					{
						position218, tokenIndex218 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l219
						}
						position++
						goto l218
					l219:
						position, tokenIndex = position218, tokenIndex218
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l220
						}
						position++
						goto l218
					l220:
						position, tokenIndex = position218, tokenIndex218
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l221
						}
						position++
						goto l218
					l221:
						position, tokenIndex = position218, tokenIndex218
						if buffer[position] != rune('_') {
							goto l222
						}
						position++
						goto l218
					l222:
						position, tokenIndex = position218, tokenIndex218
						if buffer[position] != rune('-') {
							goto l217
						}
						position++
					}
				l218:
					goto l216
				l217:
					position, tokenIndex = position217, tokenIndex217
				}
				add(rulefieldExpr, position213)
			}
			return true
		l212:
			position, tokenIndex = position212, tokenIndex212
			return false
		},
		/* 17 field <- <(<(fieldExpr / reserved)> Action44)> */
		func() bool {
			position223, tokenIndex223 := position, tokenIndex
			{
				position224 := position
				{
					position225 := position
					{
						position226, tokenIndex226 := position, tokenIndex
						if !_rules[rulefieldExpr]() {
							goto l227
						}
						goto l226
					l227:
						position, tokenIndex = position226, tokenIndex226
						{
							position228 := position
							{
								position229, tokenIndex229 := position, tokenIndex
								if buffer[position] != rune('_') {
									goto l230
								}
								position++
								if buffer[position] != rune('r') {
									goto l230
								}
								position++
								if buffer[position] != rune('o') {
									goto l230
								}
								position++
								if buffer[position] != rune('w') {
									goto l230
								}
								position++
								goto l229
							l230:
								position, tokenIndex = position229, tokenIndex229
								if buffer[position] != rune('_') {
									goto l231
								}
								position++
								if buffer[position] != rune('c') {
									goto l231
								}
								position++
								if buffer[position] != rune('o') {
									goto l231
								}
								position++
								if buffer[position] != rune('l') {
									goto l231
								}
								position++
								goto l229
							l231:
								position, tokenIndex = position229, tokenIndex229
								if buffer[position] != rune('_') {
									goto l232
								}
								position++
								if buffer[position] != rune('s') {
									goto l232
								}
								position++
								if buffer[position] != rune('t') {
									goto l232
								}
								position++
								if buffer[position] != rune('a') {
									goto l232
								}
								position++
								if buffer[position] != rune('r') {
									goto l232
								}
								position++
								if buffer[position] != rune('t') {
									goto l232
								}
								position++
								goto l229
							l232:
								position, tokenIndex = position229, tokenIndex229
								if buffer[position] != rune('_') {
									goto l233
								}
								position++
								if buffer[position] != rune('e') {
									goto l233
								}
								position++
								if buffer[position] != rune('n') {
									goto l233
								}
								position++
								if buffer[position] != rune('d') {
									goto l233
								}
								position++
								goto l229
							l233:
								position, tokenIndex = position229, tokenIndex229
								if buffer[position] != rune('_') {
									goto l234
								}
								position++
								if buffer[position] != rune('t') {
									goto l234
								}
								position++
								if buffer[position] != rune('i') {
									goto l234
								}
								position++
								if buffer[position] != rune('m') {
									goto l234
								}
								position++
								if buffer[position] != rune('e') {
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
								if buffer[position] != rune('m') {
									goto l234
								}
								position++
								if buffer[position] != rune('p') {
									goto l234
								}
								position++
								goto l229
							l234:
								position, tokenIndex = position229, tokenIndex229
								if buffer[position] != rune('_') {
									goto l223
								}
								position++
								if buffer[position] != rune('f') {
									goto l223
								}
								position++
								if buffer[position] != rune('i') {
									goto l223
								}
								position++
								if buffer[position] != rune('e') {
									goto l223
								}
								position++
								if buffer[position] != rune('l') {
									goto l223
								}
								position++
								if buffer[position] != rune('d') {
									goto l223
								}
								position++
							}
						l229:
							add(rulereserved, position228)
						}
					}
				l226:
					add(rulePegText, position225)
				}
				{
					add(ruleAction44, position)
				}
				add(rulefield, position224)
			}
			return true
		l223:
			position, tokenIndex = position223, tokenIndex223
			return false
		},
		/* 18 reserved <- <(('_' 'r' 'o' 'w') / ('_' 'c' 'o' 'l') / ('_' 's' 't' 'a' 'r' 't') / ('_' 'e' 'n' 'd') / ('_' 't' 'i' 'm' 'e' 's' 't' 'a' 'm' 'p') / ('_' 'f' 'i' 'e' 'l' 'd'))> */
		nil,
		/* 19 posfield <- <(<fieldExpr> Action45)> */
		func() bool {
			position237, tokenIndex237 := position, tokenIndex
			{
				position238 := position
				{
					position239 := position
					if !_rules[rulefieldExpr]() {
						goto l237
					}
					add(rulePegText, position239)
				}
				{
					add(ruleAction45, position)
				}
				add(ruleposfield, position238)
			}
			return true
		l237:
			position, tokenIndex = position237, tokenIndex237
			return false
		},
		/* 20 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position241, tokenIndex241 := position, tokenIndex
			{
				position242 := position
				{
					position243, tokenIndex243 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l244
					}
					position++
				l245:
					{
						position246, tokenIndex246 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l246
						}
						position++
						goto l245
					l246:
						position, tokenIndex = position246, tokenIndex246
					}
					goto l243
				l244:
					position, tokenIndex = position243, tokenIndex243
					if buffer[position] != rune('0') {
						goto l241
					}
					position++
				}
			l243:
				add(ruleuint, position242)
			}
			return true
		l241:
			position, tokenIndex = position241, tokenIndex241
			return false
		},
		/* 21 col <- <((<uint> Action46) / ('\'' <singlequotedstring> '\'' Action47) / ('"' <doublequotedstring> '"' Action48))> */
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
						add(ruleAction46, position)
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
						add(ruleAction47, position)
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
						add(ruleAction48, position)
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
		/* 22 row <- <((<uint> Action49) / ('\'' <singlequotedstring> '\'' Action50) / ('"' <doublequotedstring> '"' Action51))> */
		nil,
		/* 23 open <- <('(' sp)> */
		func() bool {
			position259, tokenIndex259 := position, tokenIndex
			{
				position260 := position
				if buffer[position] != rune('(') {
					goto l259
				}
				position++
				if !_rules[rulesp]() {
					goto l259
				}
				add(ruleopen, position260)
			}
			return true
		l259:
			position, tokenIndex = position259, tokenIndex259
			return false
		},
		/* 24 close <- <(')' sp)> */
		func() bool {
			position261, tokenIndex261 := position, tokenIndex
			{
				position262 := position
				if buffer[position] != rune(')') {
					goto l261
				}
				position++
				if !_rules[rulesp]() {
					goto l261
				}
				add(ruleclose, position262)
			}
			return true
		l261:
			position, tokenIndex = position261, tokenIndex261
			return false
		},
		/* 25 sp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position264 := position
			l265:
				{
					position266, tokenIndex266 := position, tokenIndex
					{
						position267, tokenIndex267 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l268
						}
						position++
						goto l267
					l268:
						position, tokenIndex = position267, tokenIndex267
						if buffer[position] != rune('\t') {
							goto l269
						}
						position++
						goto l267
					l269:
						position, tokenIndex = position267, tokenIndex267
						if buffer[position] != rune('\n') {
							goto l266
						}
						position++
					}
				l267:
					goto l265
				l266:
					position, tokenIndex = position266, tokenIndex266
				}
				add(rulesp, position264)
			}
			return true
		},
		/* 26 comma <- <(sp ',' sp)> */
		func() bool {
			position270, tokenIndex270 := position, tokenIndex
			{
				position271 := position
				if !_rules[rulesp]() {
					goto l270
				}
				if buffer[position] != rune(',') {
					goto l270
				}
				position++
				if !_rules[rulesp]() {
					goto l270
				}
				add(rulecomma, position271)
			}
			return true
		l270:
			position, tokenIndex = position270, tokenIndex270
			return false
		},
		/* 27 lbrack <- <('[' sp)> */
		nil,
		/* 28 rbrack <- <(sp ']' sp)> */
		nil,
		/* 29 IDENT <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9])*)> */
		func() bool {
			position274, tokenIndex274 := position, tokenIndex
			{
				position275 := position
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
						goto l274
					}
					position++
				}
			l276:
			l278:
				{
					position279, tokenIndex279 := position, tokenIndex
					{
						position280, tokenIndex280 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l281
						}
						position++
						goto l280
					l281:
						position, tokenIndex = position280, tokenIndex280
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l282
						}
						position++
						goto l280
					l282:
						position, tokenIndex = position280, tokenIndex280
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l279
						}
						position++
					}
				l280:
					goto l278
				l279:
					position, tokenIndex = position279, tokenIndex279
				}
				add(ruleIDENT, position275)
			}
			return true
		l274:
			position, tokenIndex = position274, tokenIndex274
			return false
		},
		/* 30 timestampbasicfmt <- <([0-9] [0-9] [0-9] [0-9] '-' ('0' / '1') [0-9] '-' [0-3] [0-9] 'T' [0-9] [0-9] ':' [0-9] [0-9])> */
		func() bool {
			position283, tokenIndex283 := position, tokenIndex
			{
				position284 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l283
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l283
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l283
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l283
				}
				position++
				if buffer[position] != rune('-') {
					goto l283
				}
				position++
				{
					position285, tokenIndex285 := position, tokenIndex
					if buffer[position] != rune('0') {
						goto l286
					}
					position++
					goto l285
				l286:
					position, tokenIndex = position285, tokenIndex285
					if buffer[position] != rune('1') {
						goto l283
					}
					position++
				}
			l285:
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l283
				}
				position++
				if buffer[position] != rune('-') {
					goto l283
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('3') {
					goto l283
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l283
				}
				position++
				if buffer[position] != rune('T') {
					goto l283
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l283
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l283
				}
				position++
				if buffer[position] != rune(':') {
					goto l283
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l283
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l283
				}
				position++
				add(ruletimestampbasicfmt, position284)
			}
			return true
		l283:
			position, tokenIndex = position283, tokenIndex283
			return false
		},
		/* 31 timestampfmt <- <(('"' timestampbasicfmt '"') / ('\'' timestampbasicfmt '\'') / timestampbasicfmt)> */
		func() bool {
			position287, tokenIndex287 := position, tokenIndex
			{
				position288 := position
				{
					position289, tokenIndex289 := position, tokenIndex
					if buffer[position] != rune('"') {
						goto l290
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l290
					}
					if buffer[position] != rune('"') {
						goto l290
					}
					position++
					goto l289
				l290:
					position, tokenIndex = position289, tokenIndex289
					if buffer[position] != rune('\'') {
						goto l291
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l291
					}
					if buffer[position] != rune('\'') {
						goto l291
					}
					position++
					goto l289
				l291:
					position, tokenIndex = position289, tokenIndex289
					if !_rules[ruletimestampbasicfmt]() {
						goto l287
					}
				}
			l289:
				add(ruletimestampfmt, position288)
			}
			return true
		l287:
			position, tokenIndex = position287, tokenIndex287
			return false
		},
		/* 32 timestamp <- <(<timestampfmt> Action52)> */
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
		/* 74 Action39 <- <{ p.startCall(buffer[begin:end]) }> */
		nil,
		/* 75 Action40 <- <{ p.addVal(p.endCall()) }> */
		nil,
		/* 76 Action41 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 77 Action42 <- <{ s, _ := strconv.Unquote(buffer[begin:end]); p.addVal(s) }> */
		nil,
		/* 78 Action43 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 79 Action44 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 80 Action45 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 81 Action46 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 82 Action47 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 83 Action48 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 84 Action49 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 85 Action50 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 86 Action51 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 87 Action52 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
