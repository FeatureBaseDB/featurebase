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
	ruleAction16
	ruleAction17
	rulePegText
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
	ruleAction53
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
	"Action16",
	"Action17",
	"PegText",
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
	"Action53",
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
	rules  [90]func() bool
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
			p.startCall("TopXor")
		case ruleAction15:
			p.endCall()
		case ruleAction16:
			p.startCall("Range")
		case ruleAction17:
			p.endCall()
		case ruleAction18:
			p.startCall(buffer[begin:end])
		case ruleAction19:
			p.endCall()
		case ruleAction20:
			p.addBTWN()
		case ruleAction21:
			p.addLTE()
		case ruleAction22:
			p.addGTE()
		case ruleAction23:
			p.addEQ()
		case ruleAction24:
			p.addNEQ()
		case ruleAction25:
			p.addLT()
		case ruleAction26:
			p.addGT()
		case ruleAction27:
			p.startConditional()
		case ruleAction28:
			p.endConditional()
		case ruleAction29:
			p.condAdd(buffer[begin:end])
		case ruleAction30:
			p.condAdd(buffer[begin:end])
		case ruleAction31:
			p.condAdd(buffer[begin:end])
		case ruleAction32:
			p.addPosStr("_start", buffer[begin:end])
		case ruleAction33:
			p.addPosStr("_end", buffer[begin:end])
		case ruleAction34:
			p.startList()
		case ruleAction35:
			p.endList()
		case ruleAction36:
			p.addVal(nil)
		case ruleAction37:
			p.addVal(true)
		case ruleAction38:
			p.addVal(false)
		case ruleAction39:
			p.addNumVal(buffer[begin:end])
		case ruleAction40:
			p.addNumVal(buffer[begin:end])
		case ruleAction41:
			p.addVal(buffer[begin:end])
		case ruleAction42:
			p.addVal(buffer[begin:end])
		case ruleAction43:
			p.addVal(buffer[begin:end])
		case ruleAction44:
			p.addField(buffer[begin:end])
		case ruleAction45:
			p.addPosStr("_field", buffer[begin:end])
		case ruleAction46:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction47:
			p.addPosNum("_col", buffer[begin:end])
		case ruleAction48:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction49:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction50:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction51:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction52:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction53:
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
		/* 1 Call <- <(('S' 'e' 't' Action0 open col comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma row comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'u' 'm' 'n' 'A' 't' 't' 'r' 's' Action4 open col comma args close Action5) / ('C' 'l' 'e' 'a' 'r' Action6 open col comma args close Action7) / ('C' 'l' 'e' 'a' 'r' 'R' 'o' 'w' Action8 open arg close Action9) / ('S' 't' 'o' 'r' 'e' Action10 open Call comma arg close Action11) / ('T' 'o' 'p' 'N' Action12 open posfield (comma allargs)? close Action13) / ('T' 'o' 'p' 'X' 'o' 'r' Action14 open posfield comma Call (comma allargs)? close Action15) / ('R' 'a' 'n' 'g' 'e' Action16 open (timerange / conditional / arg) close Action17) / (<IDENT> Action18 open allargs comma? close Action19))> */
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
								add(ruleAction53, position)
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
								add(ruleAction50, position)
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
								add(ruleAction51, position)
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
								add(ruleAction52, position)
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
					if buffer[position] != rune('T') {
						goto l46
					}
					position++
					if buffer[position] != rune('o') {
						goto l46
					}
					position++
					if buffer[position] != rune('p') {
						goto l46
					}
					position++
					if buffer[position] != rune('X') {
						goto l46
					}
					position++
					if buffer[position] != rune('o') {
						goto l46
					}
					position++
					if buffer[position] != rune('r') {
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
					if !_rules[rulecomma]() {
						goto l46
					}
					if !_rules[ruleCall]() {
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
					if buffer[position] != rune('R') {
						goto l51
					}
					position++
					if buffer[position] != rune('a') {
						goto l51
					}
					position++
					if buffer[position] != rune('n') {
						goto l51
					}
					position++
					if buffer[position] != rune('g') {
						goto l51
					}
					position++
					if buffer[position] != rune('e') {
						goto l51
					}
					position++
					{
						add(ruleAction16, position)
					}
					if !_rules[ruleopen]() {
						goto l51
					}
					{
						position53, tokenIndex53 := position, tokenIndex
						{
							position55 := position
							if !_rules[rulefield]() {
								goto l54
							}
							if !_rules[rulesp]() {
								goto l54
							}
							if buffer[position] != rune('=') {
								goto l54
							}
							position++
							if !_rules[rulesp]() {
								goto l54
							}
							if !_rules[rulevalue]() {
								goto l54
							}
							if !_rules[rulecomma]() {
								goto l54
							}
							{
								position56 := position
								if !_rules[ruletimestampfmt]() {
									goto l54
								}
								add(rulePegText, position56)
							}
							{
								add(ruleAction32, position)
							}
							if !_rules[rulecomma]() {
								goto l54
							}
							{
								position58 := position
								if !_rules[ruletimestampfmt]() {
									goto l54
								}
								add(rulePegText, position58)
							}
							{
								add(ruleAction33, position)
							}
							add(ruletimerange, position55)
						}
						goto l53
					l54:
						position, tokenIndex = position53, tokenIndex53
						{
							position61 := position
							{
								add(ruleAction27, position)
							}
							if !_rules[rulecondint]() {
								goto l60
							}
							if !_rules[rulecondLT]() {
								goto l60
							}
							{
								position63 := position
								{
									position64 := position
									if !_rules[rulefieldExpr]() {
										goto l60
									}
									add(rulePegText, position64)
								}
								if !_rules[rulesp]() {
									goto l60
								}
								{
									add(ruleAction31, position)
								}
								add(rulecondfield, position63)
							}
							if !_rules[rulecondLT]() {
								goto l60
							}
							if !_rules[rulecondint]() {
								goto l60
							}
							{
								add(ruleAction28, position)
							}
							add(ruleconditional, position61)
						}
						goto l53
					l60:
						position, tokenIndex = position53, tokenIndex53
						if !_rules[rulearg]() {
							goto l51
						}
					}
				l53:
					if !_rules[ruleclose]() {
						goto l51
					}
					{
						add(ruleAction17, position)
					}
					goto l7
				l51:
					position, tokenIndex = position7, tokenIndex7
					{
						position68 := position
						{
							position69 := position
							{
								position70, tokenIndex70 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l71
								}
								position++
								goto l70
							l71:
								position, tokenIndex = position70, tokenIndex70
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l5
								}
								position++
							}
						l70:
						l72:
							{
								position73, tokenIndex73 := position, tokenIndex
								{
									position74, tokenIndex74 := position, tokenIndex
									if c := buffer[position]; c < rune('a') || c > rune('z') {
										goto l75
									}
									position++
									goto l74
								l75:
									position, tokenIndex = position74, tokenIndex74
									if c := buffer[position]; c < rune('A') || c > rune('Z') {
										goto l76
									}
									position++
									goto l74
								l76:
									position, tokenIndex = position74, tokenIndex74
									if c := buffer[position]; c < rune('0') || c > rune('9') {
										goto l73
									}
									position++
								}
							l74:
								goto l72
							l73:
								position, tokenIndex = position73, tokenIndex73
							}
							add(ruleIDENT, position69)
						}
						add(rulePegText, position68)
					}
					{
						add(ruleAction18, position)
					}
					if !_rules[ruleopen]() {
						goto l5
					}
					if !_rules[ruleallargs]() {
						goto l5
					}
					{
						position78, tokenIndex78 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l78
						}
						goto l79
					l78:
						position, tokenIndex = position78, tokenIndex78
					}
				l79:
					if !_rules[ruleclose]() {
						goto l5
					}
					{
						add(ruleAction19, position)
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
			position81, tokenIndex81 := position, tokenIndex
			{
				position82 := position
				{
					position83, tokenIndex83 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l84
					}
				l85:
					{
						position86, tokenIndex86 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l86
						}
						if !_rules[ruleCall]() {
							goto l86
						}
						goto l85
					l86:
						position, tokenIndex = position86, tokenIndex86
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
					goto l83
				l84:
					position, tokenIndex = position83, tokenIndex83
					if !_rules[ruleargs]() {
						goto l89
					}
					goto l83
				l89:
					position, tokenIndex = position83, tokenIndex83
					if !_rules[rulesp]() {
						goto l81
					}
				}
			l83:
				add(ruleallargs, position82)
			}
			return true
		l81:
			position, tokenIndex = position81, tokenIndex81
			return false
		},
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position90, tokenIndex90 := position, tokenIndex
			{
				position91 := position
				if !_rules[rulearg]() {
					goto l90
				}
				{
					position92, tokenIndex92 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l92
					}
					if !_rules[ruleargs]() {
						goto l92
					}
					goto l93
				l92:
					position, tokenIndex = position92, tokenIndex92
				}
			l93:
				if !_rules[rulesp]() {
					goto l90
				}
				add(ruleargs, position91)
			}
			return true
		l90:
			position, tokenIndex = position90, tokenIndex90
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value))> */
		func() bool {
			position94, tokenIndex94 := position, tokenIndex
			{
				position95 := position
				{
					position96, tokenIndex96 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l97
					}
					if !_rules[rulesp]() {
						goto l97
					}
					if buffer[position] != rune('=') {
						goto l97
					}
					position++
					if !_rules[rulesp]() {
						goto l97
					}
					if !_rules[rulevalue]() {
						goto l97
					}
					goto l96
				l97:
					position, tokenIndex = position96, tokenIndex96
					if !_rules[rulefield]() {
						goto l94
					}
					if !_rules[rulesp]() {
						goto l94
					}
					{
						position98 := position
						{
							position99, tokenIndex99 := position, tokenIndex
							if buffer[position] != rune('>') {
								goto l100
							}
							position++
							if buffer[position] != rune('<') {
								goto l100
							}
							position++
							{
								add(ruleAction20, position)
							}
							goto l99
						l100:
							position, tokenIndex = position99, tokenIndex99
							if buffer[position] != rune('<') {
								goto l102
							}
							position++
							if buffer[position] != rune('=') {
								goto l102
							}
							position++
							{
								add(ruleAction21, position)
							}
							goto l99
						l102:
							position, tokenIndex = position99, tokenIndex99
							if buffer[position] != rune('>') {
								goto l104
							}
							position++
							if buffer[position] != rune('=') {
								goto l104
							}
							position++
							{
								add(ruleAction22, position)
							}
							goto l99
						l104:
							position, tokenIndex = position99, tokenIndex99
							if buffer[position] != rune('=') {
								goto l106
							}
							position++
							if buffer[position] != rune('=') {
								goto l106
							}
							position++
							{
								add(ruleAction23, position)
							}
							goto l99
						l106:
							position, tokenIndex = position99, tokenIndex99
							if buffer[position] != rune('!') {
								goto l108
							}
							position++
							if buffer[position] != rune('=') {
								goto l108
							}
							position++
							{
								add(ruleAction24, position)
							}
							goto l99
						l108:
							position, tokenIndex = position99, tokenIndex99
							if buffer[position] != rune('<') {
								goto l110
							}
							position++
							{
								add(ruleAction25, position)
							}
							goto l99
						l110:
							position, tokenIndex = position99, tokenIndex99
							if buffer[position] != rune('>') {
								goto l94
							}
							position++
							{
								add(ruleAction26, position)
							}
						}
					l99:
						add(ruleCOND, position98)
					}
					if !_rules[rulesp]() {
						goto l94
					}
					if !_rules[rulevalue]() {
						goto l94
					}
				}
			l96:
				add(rulearg, position95)
			}
			return true
		l94:
			position, tokenIndex = position94, tokenIndex94
			return false
		},
		/* 5 COND <- <(('>' '<' Action20) / ('<' '=' Action21) / ('>' '=' Action22) / ('=' '=' Action23) / ('!' '=' Action24) / ('<' Action25) / ('>' Action26))> */
		nil,
		/* 6 conditional <- <(Action27 condint condLT condfield condLT condint Action28)> */
		nil,
		/* 7 condint <- <(<(('-'? [1-9] [0-9]*) / '0')> sp Action29)> */
		func() bool {
			position115, tokenIndex115 := position, tokenIndex
			{
				position116 := position
				{
					position117 := position
					{
						position118, tokenIndex118 := position, tokenIndex
						{
							position120, tokenIndex120 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l120
							}
							position++
							goto l121
						l120:
							position, tokenIndex = position120, tokenIndex120
						}
					l121:
						if c := buffer[position]; c < rune('1') || c > rune('9') {
							goto l119
						}
						position++
					l122:
						{
							position123, tokenIndex123 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l123
							}
							position++
							goto l122
						l123:
							position, tokenIndex = position123, tokenIndex123
						}
						goto l118
					l119:
						position, tokenIndex = position118, tokenIndex118
						if buffer[position] != rune('0') {
							goto l115
						}
						position++
					}
				l118:
					add(rulePegText, position117)
				}
				if !_rules[rulesp]() {
					goto l115
				}
				{
					add(ruleAction29, position)
				}
				add(rulecondint, position116)
			}
			return true
		l115:
			position, tokenIndex = position115, tokenIndex115
			return false
		},
		/* 8 condLT <- <(<(('<' '=') / '<')> sp Action30)> */
		func() bool {
			position125, tokenIndex125 := position, tokenIndex
			{
				position126 := position
				{
					position127 := position
					{
						position128, tokenIndex128 := position, tokenIndex
						if buffer[position] != rune('<') {
							goto l129
						}
						position++
						if buffer[position] != rune('=') {
							goto l129
						}
						position++
						goto l128
					l129:
						position, tokenIndex = position128, tokenIndex128
						if buffer[position] != rune('<') {
							goto l125
						}
						position++
					}
				l128:
					add(rulePegText, position127)
				}
				if !_rules[rulesp]() {
					goto l125
				}
				{
					add(ruleAction30, position)
				}
				add(rulecondLT, position126)
			}
			return true
		l125:
			position, tokenIndex = position125, tokenIndex125
			return false
		},
		/* 9 condfield <- <(<fieldExpr> sp Action31)> */
		nil,
		/* 10 timerange <- <(field sp '=' sp value comma <timestampfmt> Action32 comma <timestampfmt> Action33)> */
		nil,
		/* 11 value <- <(item / (lbrack Action34 list rbrack Action35))> */
		func() bool {
			position133, tokenIndex133 := position, tokenIndex
			{
				position134 := position
				{
					position135, tokenIndex135 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l136
					}
					goto l135
				l136:
					position, tokenIndex = position135, tokenIndex135
					{
						position137 := position
						if buffer[position] != rune('[') {
							goto l133
						}
						position++
						if !_rules[rulesp]() {
							goto l133
						}
						add(rulelbrack, position137)
					}
					{
						add(ruleAction34, position)
					}
					if !_rules[rulelist]() {
						goto l133
					}
					{
						position139 := position
						if !_rules[rulesp]() {
							goto l133
						}
						if buffer[position] != rune(']') {
							goto l133
						}
						position++
						if !_rules[rulesp]() {
							goto l133
						}
						add(rulerbrack, position139)
					}
					{
						add(ruleAction35, position)
					}
				}
			l135:
				add(rulevalue, position134)
			}
			return true
		l133:
			position, tokenIndex = position133, tokenIndex133
			return false
		},
		/* 12 list <- <(item (comma list)?)> */
		func() bool {
			position141, tokenIndex141 := position, tokenIndex
			{
				position142 := position
				if !_rules[ruleitem]() {
					goto l141
				}
				{
					position143, tokenIndex143 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l143
					}
					if !_rules[rulelist]() {
						goto l143
					}
					goto l144
				l143:
					position, tokenIndex = position143, tokenIndex143
				}
			l144:
				add(rulelist, position142)
			}
			return true
		l141:
			position, tokenIndex = position141, tokenIndex141
			return false
		},
		/* 13 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action36) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action37) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action38) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action39) / (<('-'? '.' [0-9]+)> Action40) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action41) / ('"' <doublequotedstring> '"' Action42) / ('\'' <singlequotedstring> '\'' Action43))> */
		func() bool {
			position145, tokenIndex145 := position, tokenIndex
			{
				position146 := position
				{
					position147, tokenIndex147 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l148
					}
					position++
					if buffer[position] != rune('u') {
						goto l148
					}
					position++
					if buffer[position] != rune('l') {
						goto l148
					}
					position++
					if buffer[position] != rune('l') {
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
						add(ruleAction36, position)
					}
					goto l147
				l148:
					position, tokenIndex = position147, tokenIndex147
					if buffer[position] != rune('t') {
						goto l153
					}
					position++
					if buffer[position] != rune('r') {
						goto l153
					}
					position++
					if buffer[position] != rune('u') {
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
						add(ruleAction37, position)
					}
					goto l147
				l153:
					position, tokenIndex = position147, tokenIndex147
					if buffer[position] != rune('f') {
						goto l158
					}
					position++
					if buffer[position] != rune('a') {
						goto l158
					}
					position++
					if buffer[position] != rune('l') {
						goto l158
					}
					position++
					if buffer[position] != rune('s') {
						goto l158
					}
					position++
					if buffer[position] != rune('e') {
						goto l158
					}
					position++
					{
						position159, tokenIndex159 := position, tokenIndex
						{
							position160, tokenIndex160 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l161
							}
							goto l160
						l161:
							position, tokenIndex = position160, tokenIndex160
							if !_rules[rulesp]() {
								goto l158
							}
							if !_rules[ruleclose]() {
								goto l158
							}
						}
					l160:
						position, tokenIndex = position159, tokenIndex159
					}
					{
						add(ruleAction38, position)
					}
					goto l147
				l158:
					position, tokenIndex = position147, tokenIndex147
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
						{
							position169, tokenIndex169 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l169
							}
							position++
						l171:
							{
								position172, tokenIndex172 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l172
								}
								position++
								goto l171
							l172:
								position, tokenIndex = position172, tokenIndex172
							}
							goto l170
						l169:
							position, tokenIndex = position169, tokenIndex169
						}
					l170:
						add(rulePegText, position164)
					}
					{
						add(ruleAction39, position)
					}
					goto l147
				l163:
					position, tokenIndex = position147, tokenIndex147
					{
						position175 := position
						{
							position176, tokenIndex176 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l176
							}
							position++
							goto l177
						l176:
							position, tokenIndex = position176, tokenIndex176
						}
					l177:
						if buffer[position] != rune('.') {
							goto l174
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l174
						}
						position++
					l178:
						{
							position179, tokenIndex179 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l179
							}
							position++
							goto l178
						l179:
							position, tokenIndex = position179, tokenIndex179
						}
						add(rulePegText, position175)
					}
					{
						add(ruleAction40, position)
					}
					goto l147
				l174:
					position, tokenIndex = position147, tokenIndex147
					{
						position182 := position
						{
							position185, tokenIndex185 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l186
							}
							position++
							goto l185
						l186:
							position, tokenIndex = position185, tokenIndex185
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l187
							}
							position++
							goto l185
						l187:
							position, tokenIndex = position185, tokenIndex185
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l188
							}
							position++
							goto l185
						l188:
							position, tokenIndex = position185, tokenIndex185
							if buffer[position] != rune('-') {
								goto l189
							}
							position++
							goto l185
						l189:
							position, tokenIndex = position185, tokenIndex185
							if buffer[position] != rune('_') {
								goto l190
							}
							position++
							goto l185
						l190:
							position, tokenIndex = position185, tokenIndex185
							if buffer[position] != rune(':') {
								goto l181
							}
							position++
						}
					l185:
					l183:
						{
							position184, tokenIndex184 := position, tokenIndex
							{
								position191, tokenIndex191 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l192
								}
								position++
								goto l191
							l192:
								position, tokenIndex = position191, tokenIndex191
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l193
								}
								position++
								goto l191
							l193:
								position, tokenIndex = position191, tokenIndex191
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l194
								}
								position++
								goto l191
							l194:
								position, tokenIndex = position191, tokenIndex191
								if buffer[position] != rune('-') {
									goto l195
								}
								position++
								goto l191
							l195:
								position, tokenIndex = position191, tokenIndex191
								if buffer[position] != rune('_') {
									goto l196
								}
								position++
								goto l191
							l196:
								position, tokenIndex = position191, tokenIndex191
								if buffer[position] != rune(':') {
									goto l184
								}
								position++
							}
						l191:
							goto l183
						l184:
							position, tokenIndex = position184, tokenIndex184
						}
						add(rulePegText, position182)
					}
					{
						add(ruleAction41, position)
					}
					goto l147
				l181:
					position, tokenIndex = position147, tokenIndex147
					if buffer[position] != rune('"') {
						goto l198
					}
					position++
					{
						position199 := position
						if !_rules[ruledoublequotedstring]() {
							goto l198
						}
						add(rulePegText, position199)
					}
					if buffer[position] != rune('"') {
						goto l198
					}
					position++
					{
						add(ruleAction42, position)
					}
					goto l147
				l198:
					position, tokenIndex = position147, tokenIndex147
					if buffer[position] != rune('\'') {
						goto l145
					}
					position++
					{
						position201 := position
						if !_rules[rulesinglequotedstring]() {
							goto l145
						}
						add(rulePegText, position201)
					}
					if buffer[position] != rune('\'') {
						goto l145
					}
					position++
					{
						add(ruleAction43, position)
					}
				}
			l147:
				add(ruleitem, position146)
			}
			return true
		l145:
			position, tokenIndex = position145, tokenIndex145
			return false
		},
		/* 14 doublequotedstring <- <((!('"' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		func() bool {
			{
				position204 := position
			l205:
				{
					position206, tokenIndex206 := position, tokenIndex
					{
						position207, tokenIndex207 := position, tokenIndex
						{
							position209, tokenIndex209 := position, tokenIndex
							{
								position210, tokenIndex210 := position, tokenIndex
								if buffer[position] != rune('"') {
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
								goto l210
							l212:
								position, tokenIndex = position210, tokenIndex210
								if buffer[position] != rune('\n') {
									goto l209
								}
								position++
							}
						l210:
							goto l208
						l209:
							position, tokenIndex = position209, tokenIndex209
						}
						if !matchDot() {
							goto l208
						}
						goto l207
					l208:
						position, tokenIndex = position207, tokenIndex207
						if buffer[position] != rune('\\') {
							goto l213
						}
						position++
						if buffer[position] != rune('n') {
							goto l213
						}
						position++
						goto l207
					l213:
						position, tokenIndex = position207, tokenIndex207
						if buffer[position] != rune('\\') {
							goto l214
						}
						position++
						if buffer[position] != rune('"') {
							goto l214
						}
						position++
						goto l207
					l214:
						position, tokenIndex = position207, tokenIndex207
						if buffer[position] != rune('\\') {
							goto l215
						}
						position++
						if buffer[position] != rune('\'') {
							goto l215
						}
						position++
						goto l207
					l215:
						position, tokenIndex = position207, tokenIndex207
						if buffer[position] != rune('\\') {
							goto l206
						}
						position++
						if buffer[position] != rune('\\') {
							goto l206
						}
						position++
					}
				l207:
					goto l205
				l206:
					position, tokenIndex = position206, tokenIndex206
				}
				add(ruledoublequotedstring, position204)
			}
			return true
		},
		/* 15 singlequotedstring <- <((!('\'' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		func() bool {
			{
				position217 := position
			l218:
				{
					position219, tokenIndex219 := position, tokenIndex
					{
						position220, tokenIndex220 := position, tokenIndex
						{
							position222, tokenIndex222 := position, tokenIndex
							{
								position223, tokenIndex223 := position, tokenIndex
								if buffer[position] != rune('\'') {
									goto l224
								}
								position++
								goto l223
							l224:
								position, tokenIndex = position223, tokenIndex223
								if buffer[position] != rune('\\') {
									goto l225
								}
								position++
								goto l223
							l225:
								position, tokenIndex = position223, tokenIndex223
								if buffer[position] != rune('\n') {
									goto l222
								}
								position++
							}
						l223:
							goto l221
						l222:
							position, tokenIndex = position222, tokenIndex222
						}
						if !matchDot() {
							goto l221
						}
						goto l220
					l221:
						position, tokenIndex = position220, tokenIndex220
						if buffer[position] != rune('\\') {
							goto l226
						}
						position++
						if buffer[position] != rune('n') {
							goto l226
						}
						position++
						goto l220
					l226:
						position, tokenIndex = position220, tokenIndex220
						if buffer[position] != rune('\\') {
							goto l227
						}
						position++
						if buffer[position] != rune('"') {
							goto l227
						}
						position++
						goto l220
					l227:
						position, tokenIndex = position220, tokenIndex220
						if buffer[position] != rune('\\') {
							goto l228
						}
						position++
						if buffer[position] != rune('\'') {
							goto l228
						}
						position++
						goto l220
					l228:
						position, tokenIndex = position220, tokenIndex220
						if buffer[position] != rune('\\') {
							goto l219
						}
						position++
						if buffer[position] != rune('\\') {
							goto l219
						}
						position++
					}
				l220:
					goto l218
				l219:
					position, tokenIndex = position219, tokenIndex219
				}
				add(rulesinglequotedstring, position217)
			}
			return true
		},
		/* 16 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_' / '-')*)> */
		func() bool {
			position229, tokenIndex229 := position, tokenIndex
			{
				position230 := position
				{
					position231, tokenIndex231 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l232
					}
					position++
					goto l231
				l232:
					position, tokenIndex = position231, tokenIndex231
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l229
					}
					position++
				}
			l231:
			l233:
				{
					position234, tokenIndex234 := position, tokenIndex
					{
						position235, tokenIndex235 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l236
						}
						position++
						goto l235
					l236:
						position, tokenIndex = position235, tokenIndex235
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l237
						}
						position++
						goto l235
					l237:
						position, tokenIndex = position235, tokenIndex235
						if c := buffer[position]; c < rune('0') || c > rune('9') {
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
						goto l235
					l239:
						position, tokenIndex = position235, tokenIndex235
						if buffer[position] != rune('-') {
							goto l234
						}
						position++
					}
				l235:
					goto l233
				l234:
					position, tokenIndex = position234, tokenIndex234
				}
				add(rulefieldExpr, position230)
			}
			return true
		l229:
			position, tokenIndex = position229, tokenIndex229
			return false
		},
		/* 17 field <- <(<(fieldExpr / reserved)> Action44)> */
		func() bool {
			position240, tokenIndex240 := position, tokenIndex
			{
				position241 := position
				{
					position242 := position
					{
						position243, tokenIndex243 := position, tokenIndex
						if !_rules[rulefieldExpr]() {
							goto l244
						}
						goto l243
					l244:
						position, tokenIndex = position243, tokenIndex243
						{
							position245 := position
							{
								position246, tokenIndex246 := position, tokenIndex
								if buffer[position] != rune('_') {
									goto l247
								}
								position++
								if buffer[position] != rune('r') {
									goto l247
								}
								position++
								if buffer[position] != rune('o') {
									goto l247
								}
								position++
								if buffer[position] != rune('w') {
									goto l247
								}
								position++
								goto l246
							l247:
								position, tokenIndex = position246, tokenIndex246
								if buffer[position] != rune('_') {
									goto l248
								}
								position++
								if buffer[position] != rune('c') {
									goto l248
								}
								position++
								if buffer[position] != rune('o') {
									goto l248
								}
								position++
								if buffer[position] != rune('l') {
									goto l248
								}
								position++
								goto l246
							l248:
								position, tokenIndex = position246, tokenIndex246
								if buffer[position] != rune('_') {
									goto l249
								}
								position++
								if buffer[position] != rune('s') {
									goto l249
								}
								position++
								if buffer[position] != rune('t') {
									goto l249
								}
								position++
								if buffer[position] != rune('a') {
									goto l249
								}
								position++
								if buffer[position] != rune('r') {
									goto l249
								}
								position++
								if buffer[position] != rune('t') {
									goto l249
								}
								position++
								goto l246
							l249:
								position, tokenIndex = position246, tokenIndex246
								if buffer[position] != rune('_') {
									goto l250
								}
								position++
								if buffer[position] != rune('e') {
									goto l250
								}
								position++
								if buffer[position] != rune('n') {
									goto l250
								}
								position++
								if buffer[position] != rune('d') {
									goto l250
								}
								position++
								goto l246
							l250:
								position, tokenIndex = position246, tokenIndex246
								if buffer[position] != rune('_') {
									goto l251
								}
								position++
								if buffer[position] != rune('t') {
									goto l251
								}
								position++
								if buffer[position] != rune('i') {
									goto l251
								}
								position++
								if buffer[position] != rune('m') {
									goto l251
								}
								position++
								if buffer[position] != rune('e') {
									goto l251
								}
								position++
								if buffer[position] != rune('s') {
									goto l251
								}
								position++
								if buffer[position] != rune('t') {
									goto l251
								}
								position++
								if buffer[position] != rune('a') {
									goto l251
								}
								position++
								if buffer[position] != rune('m') {
									goto l251
								}
								position++
								if buffer[position] != rune('p') {
									goto l251
								}
								position++
								goto l246
							l251:
								position, tokenIndex = position246, tokenIndex246
								if buffer[position] != rune('_') {
									goto l240
								}
								position++
								if buffer[position] != rune('f') {
									goto l240
								}
								position++
								if buffer[position] != rune('i') {
									goto l240
								}
								position++
								if buffer[position] != rune('e') {
									goto l240
								}
								position++
								if buffer[position] != rune('l') {
									goto l240
								}
								position++
								if buffer[position] != rune('d') {
									goto l240
								}
								position++
							}
						l246:
							add(rulereserved, position245)
						}
					}
				l243:
					add(rulePegText, position242)
				}
				{
					add(ruleAction44, position)
				}
				add(rulefield, position241)
			}
			return true
		l240:
			position, tokenIndex = position240, tokenIndex240
			return false
		},
		/* 18 reserved <- <(('_' 'r' 'o' 'w') / ('_' 'c' 'o' 'l') / ('_' 's' 't' 'a' 'r' 't') / ('_' 'e' 'n' 'd') / ('_' 't' 'i' 'm' 'e' 's' 't' 'a' 'm' 'p') / ('_' 'f' 'i' 'e' 'l' 'd'))> */
		nil,
		/* 19 posfield <- <(<fieldExpr> Action45)> */
		func() bool {
			position254, tokenIndex254 := position, tokenIndex
			{
				position255 := position
				{
					position256 := position
					if !_rules[rulefieldExpr]() {
						goto l254
					}
					add(rulePegText, position256)
				}
				{
					add(ruleAction45, position)
				}
				add(ruleposfield, position255)
			}
			return true
		l254:
			position, tokenIndex = position254, tokenIndex254
			return false
		},
		/* 20 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position258, tokenIndex258 := position, tokenIndex
			{
				position259 := position
				{
					position260, tokenIndex260 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l261
					}
					position++
				l262:
					{
						position263, tokenIndex263 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l263
						}
						position++
						goto l262
					l263:
						position, tokenIndex = position263, tokenIndex263
					}
					goto l260
				l261:
					position, tokenIndex = position260, tokenIndex260
					if buffer[position] != rune('0') {
						goto l258
					}
					position++
				}
			l260:
				add(ruleuint, position259)
			}
			return true
		l258:
			position, tokenIndex = position258, tokenIndex258
			return false
		},
		/* 21 uintrow <- <(<uint> Action46)> */
		nil,
		/* 22 col <- <((<uint> Action47) / ('\'' <singlequotedstring> '\'' Action48) / ('"' <doublequotedstring> '"' Action49))> */
		func() bool {
			position265, tokenIndex265 := position, tokenIndex
			{
				position266 := position
				{
					position267, tokenIndex267 := position, tokenIndex
					{
						position269 := position
						if !_rules[ruleuint]() {
							goto l268
						}
						add(rulePegText, position269)
					}
					{
						add(ruleAction47, position)
					}
					goto l267
				l268:
					position, tokenIndex = position267, tokenIndex267
					if buffer[position] != rune('\'') {
						goto l271
					}
					position++
					{
						position272 := position
						if !_rules[rulesinglequotedstring]() {
							goto l271
						}
						add(rulePegText, position272)
					}
					if buffer[position] != rune('\'') {
						goto l271
					}
					position++
					{
						add(ruleAction48, position)
					}
					goto l267
				l271:
					position, tokenIndex = position267, tokenIndex267
					if buffer[position] != rune('"') {
						goto l265
					}
					position++
					{
						position274 := position
						if !_rules[ruledoublequotedstring]() {
							goto l265
						}
						add(rulePegText, position274)
					}
					if buffer[position] != rune('"') {
						goto l265
					}
					position++
					{
						add(ruleAction49, position)
					}
				}
			l267:
				add(rulecol, position266)
			}
			return true
		l265:
			position, tokenIndex = position265, tokenIndex265
			return false
		},
		/* 23 row <- <((<uint> Action50) / ('\'' <singlequotedstring> '\'' Action51) / ('"' <doublequotedstring> '"' Action52))> */
		nil,
		/* 24 open <- <('(' sp)> */
		func() bool {
			position277, tokenIndex277 := position, tokenIndex
			{
				position278 := position
				if buffer[position] != rune('(') {
					goto l277
				}
				position++
				if !_rules[rulesp]() {
					goto l277
				}
				add(ruleopen, position278)
			}
			return true
		l277:
			position, tokenIndex = position277, tokenIndex277
			return false
		},
		/* 25 close <- <(')' sp)> */
		func() bool {
			position279, tokenIndex279 := position, tokenIndex
			{
				position280 := position
				if buffer[position] != rune(')') {
					goto l279
				}
				position++
				if !_rules[rulesp]() {
					goto l279
				}
				add(ruleclose, position280)
			}
			return true
		l279:
			position, tokenIndex = position279, tokenIndex279
			return false
		},
		/* 26 sp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position282 := position
			l283:
				{
					position284, tokenIndex284 := position, tokenIndex
					{
						position285, tokenIndex285 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l286
						}
						position++
						goto l285
					l286:
						position, tokenIndex = position285, tokenIndex285
						if buffer[position] != rune('\t') {
							goto l287
						}
						position++
						goto l285
					l287:
						position, tokenIndex = position285, tokenIndex285
						if buffer[position] != rune('\n') {
							goto l284
						}
						position++
					}
				l285:
					goto l283
				l284:
					position, tokenIndex = position284, tokenIndex284
				}
				add(rulesp, position282)
			}
			return true
		},
		/* 27 comma <- <(sp ',' sp)> */
		func() bool {
			position288, tokenIndex288 := position, tokenIndex
			{
				position289 := position
				if !_rules[rulesp]() {
					goto l288
				}
				if buffer[position] != rune(',') {
					goto l288
				}
				position++
				if !_rules[rulesp]() {
					goto l288
				}
				add(rulecomma, position289)
			}
			return true
		l288:
			position, tokenIndex = position288, tokenIndex288
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
			position293, tokenIndex293 := position, tokenIndex
			{
				position294 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l293
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l293
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l293
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l293
				}
				position++
				if buffer[position] != rune('-') {
					goto l293
				}
				position++
				{
					position295, tokenIndex295 := position, tokenIndex
					if buffer[position] != rune('0') {
						goto l296
					}
					position++
					goto l295
				l296:
					position, tokenIndex = position295, tokenIndex295
					if buffer[position] != rune('1') {
						goto l293
					}
					position++
				}
			l295:
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l293
				}
				position++
				if buffer[position] != rune('-') {
					goto l293
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('3') {
					goto l293
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l293
				}
				position++
				if buffer[position] != rune('T') {
					goto l293
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l293
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l293
				}
				position++
				if buffer[position] != rune(':') {
					goto l293
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l293
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l293
				}
				position++
				add(ruletimestampbasicfmt, position294)
			}
			return true
		l293:
			position, tokenIndex = position293, tokenIndex293
			return false
		},
		/* 32 timestampfmt <- <(('"' timestampbasicfmt '"') / ('\'' timestampbasicfmt '\'') / timestampbasicfmt)> */
		func() bool {
			position297, tokenIndex297 := position, tokenIndex
			{
				position298 := position
				{
					position299, tokenIndex299 := position, tokenIndex
					if buffer[position] != rune('"') {
						goto l300
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l300
					}
					if buffer[position] != rune('"') {
						goto l300
					}
					position++
					goto l299
				l300:
					position, tokenIndex = position299, tokenIndex299
					if buffer[position] != rune('\'') {
						goto l301
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l301
					}
					if buffer[position] != rune('\'') {
						goto l301
					}
					position++
					goto l299
				l301:
					position, tokenIndex = position299, tokenIndex299
					if !_rules[ruletimestampbasicfmt]() {
						goto l297
					}
				}
			l299:
				add(ruletimestampfmt, position298)
			}
			return true
		l297:
			position, tokenIndex = position297, tokenIndex297
			return false
		},
		/* 33 timestamp <- <(<timestampfmt> Action53)> */
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
		/* 49 Action14 <- <{p.startCall("TopXor")}> */
		nil,
		/* 50 Action15 <- <{p.endCall()}> */
		nil,
		/* 51 Action16 <- <{p.startCall("Range")}> */
		nil,
		/* 52 Action17 <- <{p.endCall()}> */
		nil,
		nil,
		/* 54 Action18 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 55 Action19 <- <{ p.endCall() }> */
		nil,
		/* 56 Action20 <- <{ p.addBTWN() }> */
		nil,
		/* 57 Action21 <- <{ p.addLTE() }> */
		nil,
		/* 58 Action22 <- <{ p.addGTE() }> */
		nil,
		/* 59 Action23 <- <{ p.addEQ() }> */
		nil,
		/* 60 Action24 <- <{ p.addNEQ() }> */
		nil,
		/* 61 Action25 <- <{ p.addLT() }> */
		nil,
		/* 62 Action26 <- <{ p.addGT() }> */
		nil,
		/* 63 Action27 <- <{p.startConditional()}> */
		nil,
		/* 64 Action28 <- <{p.endConditional()}> */
		nil,
		/* 65 Action29 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 66 Action30 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 67 Action31 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 68 Action32 <- <{p.addPosStr("_start", buffer[begin:end])}> */
		nil,
		/* 69 Action33 <- <{p.addPosStr("_end", buffer[begin:end])}> */
		nil,
		/* 70 Action34 <- <{ p.startList() }> */
		nil,
		/* 71 Action35 <- <{ p.endList() }> */
		nil,
		/* 72 Action36 <- <{ p.addVal(nil) }> */
		nil,
		/* 73 Action37 <- <{ p.addVal(true) }> */
		nil,
		/* 74 Action38 <- <{ p.addVal(false) }> */
		nil,
		/* 75 Action39 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 76 Action40 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 77 Action41 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 78 Action42 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 79 Action43 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 80 Action44 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 81 Action45 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 82 Action46 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 83 Action47 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 84 Action48 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 85 Action49 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 86 Action50 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 87 Action51 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 88 Action52 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 89 Action53 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
