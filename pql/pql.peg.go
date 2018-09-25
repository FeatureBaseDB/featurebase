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
			p.startCall("TopN")
		case ruleAction11:
			p.endCall()
		case ruleAction12:
			p.startCall("Range")
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
			p.addPosStr("_start", buffer[begin:end])
		case ruleAction29:
			p.addPosStr("_end", buffer[begin:end])
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
			p.addNumVal(buffer[begin:end])
		case ruleAction36:
			p.addNumVal(buffer[begin:end])
		case ruleAction37:
			p.addVal(buffer[begin:end])
		case ruleAction38:
			p.addVal(buffer[begin:end])
		case ruleAction39:
			p.addVal(buffer[begin:end])
		case ruleAction40:
			p.addField(buffer[begin:end])
		case ruleAction41:
			p.addPosStr("_field", buffer[begin:end])
		case ruleAction42:
			p.addPosNum("_row", buffer[begin:end])
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
		/* 1 Call <- <(('S' 'e' 't' Action0 open col comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma row comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'u' 'm' 'n' 'A' 't' 't' 'r' 's' Action4 open col comma args close Action5) / ('C' 'l' 'e' 'a' 'r' Action6 open col comma args close Action7) / ('C' 'l' 'e' 'a' 'r' 'R' 'o' 'w' Action8 open arg close Action9) / ('T' 'o' 'p' 'N' Action10 open posfield (comma allargs)? close Action11) / ('R' 'a' 'n' 'g' 'e' Action12 open (timerange / conditional / arg) close Action13) / (<IDENT> Action14 open allargs comma? close Action15))> */
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
					if buffer[position] != rune('T') {
						goto l38
					}
					position++
					if buffer[position] != rune('o') {
						goto l38
					}
					position++
					if buffer[position] != rune('p') {
						goto l38
					}
					position++
					if buffer[position] != rune('N') {
						goto l38
					}
					position++
					{
						add(ruleAction10, position)
					}
					if !_rules[ruleopen]() {
						goto l38
					}
					if !_rules[ruleposfield]() {
						goto l38
					}
					{
						position40, tokenIndex40 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l40
						}
						if !_rules[ruleallargs]() {
							goto l40
						}
						goto l41
					l40:
						position, tokenIndex = position40, tokenIndex40
					}
				l41:
					if !_rules[ruleclose]() {
						goto l38
					}
					{
						add(ruleAction11, position)
					}
					goto l7
				l38:
					position, tokenIndex = position7, tokenIndex7
					if buffer[position] != rune('R') {
						goto l43
					}
					position++
					if buffer[position] != rune('a') {
						goto l43
					}
					position++
					if buffer[position] != rune('n') {
						goto l43
					}
					position++
					if buffer[position] != rune('g') {
						goto l43
					}
					position++
					if buffer[position] != rune('e') {
						goto l43
					}
					position++
					{
						add(ruleAction12, position)
					}
					if !_rules[ruleopen]() {
						goto l43
					}
					{
						position45, tokenIndex45 := position, tokenIndex
						{
							position47 := position
							if !_rules[rulefield]() {
								goto l46
							}
							if !_rules[rulesp]() {
								goto l46
							}
							if buffer[position] != rune('=') {
								goto l46
							}
							position++
							if !_rules[rulesp]() {
								goto l46
							}
							if !_rules[rulevalue]() {
								goto l46
							}
							if !_rules[rulecomma]() {
								goto l46
							}
							{
								position48 := position
								if !_rules[ruletimestampfmt]() {
									goto l46
								}
								add(rulePegText, position48)
							}
							{
								add(ruleAction28, position)
							}
							if !_rules[rulecomma]() {
								goto l46
							}
							{
								position50 := position
								if !_rules[ruletimestampfmt]() {
									goto l46
								}
								add(rulePegText, position50)
							}
							{
								add(ruleAction29, position)
							}
							add(ruletimerange, position47)
						}
						goto l45
					l46:
						position, tokenIndex = position45, tokenIndex45
						{
							position53 := position
							{
								add(ruleAction23, position)
							}
							if !_rules[rulecondint]() {
								goto l52
							}
							if !_rules[rulecondLT]() {
								goto l52
							}
							{
								position55 := position
								{
									position56 := position
									if !_rules[rulefieldExpr]() {
										goto l52
									}
									add(rulePegText, position56)
								}
								if !_rules[rulesp]() {
									goto l52
								}
								{
									add(ruleAction27, position)
								}
								add(rulecondfield, position55)
							}
							if !_rules[rulecondLT]() {
								goto l52
							}
							if !_rules[rulecondint]() {
								goto l52
							}
							{
								add(ruleAction24, position)
							}
							add(ruleconditional, position53)
						}
						goto l45
					l52:
						position, tokenIndex = position45, tokenIndex45
						if !_rules[rulearg]() {
							goto l43
						}
					}
				l45:
					if !_rules[ruleclose]() {
						goto l43
					}
					{
						add(ruleAction13, position)
					}
					goto l7
				l43:
					position, tokenIndex = position7, tokenIndex7
					{
						position60 := position
						{
							position61 := position
							{
								position62, tokenIndex62 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l63
								}
								position++
								goto l62
							l63:
								position, tokenIndex = position62, tokenIndex62
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l5
								}
								position++
							}
						l62:
						l64:
							{
								position65, tokenIndex65 := position, tokenIndex
								{
									position66, tokenIndex66 := position, tokenIndex
									if c := buffer[position]; c < rune('a') || c > rune('z') {
										goto l67
									}
									position++
									goto l66
								l67:
									position, tokenIndex = position66, tokenIndex66
									if c := buffer[position]; c < rune('A') || c > rune('Z') {
										goto l68
									}
									position++
									goto l66
								l68:
									position, tokenIndex = position66, tokenIndex66
									if c := buffer[position]; c < rune('0') || c > rune('9') {
										goto l65
									}
									position++
								}
							l66:
								goto l64
							l65:
								position, tokenIndex = position65, tokenIndex65
							}
							add(ruleIDENT, position61)
						}
						add(rulePegText, position60)
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
			position73, tokenIndex73 := position, tokenIndex
			{
				position74 := position
				{
					position75, tokenIndex75 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l76
					}
				l77:
					{
						position78, tokenIndex78 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l78
						}
						if !_rules[ruleCall]() {
							goto l78
						}
						goto l77
					l78:
						position, tokenIndex = position78, tokenIndex78
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
					goto l75
				l76:
					position, tokenIndex = position75, tokenIndex75
					if !_rules[ruleargs]() {
						goto l81
					}
					goto l75
				l81:
					position, tokenIndex = position75, tokenIndex75
					if !_rules[rulesp]() {
						goto l73
					}
				}
			l75:
				add(ruleallargs, position74)
			}
			return true
		l73:
			position, tokenIndex = position73, tokenIndex73
			return false
		},
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position82, tokenIndex82 := position, tokenIndex
			{
				position83 := position
				if !_rules[rulearg]() {
					goto l82
				}
				{
					position84, tokenIndex84 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l84
					}
					if !_rules[ruleargs]() {
						goto l84
					}
					goto l85
				l84:
					position, tokenIndex = position84, tokenIndex84
				}
			l85:
				if !_rules[rulesp]() {
					goto l82
				}
				add(ruleargs, position83)
			}
			return true
		l82:
			position, tokenIndex = position82, tokenIndex82
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value))> */
		func() bool {
			position86, tokenIndex86 := position, tokenIndex
			{
				position87 := position
				{
					position88, tokenIndex88 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l89
					}
					if !_rules[rulesp]() {
						goto l89
					}
					if buffer[position] != rune('=') {
						goto l89
					}
					position++
					if !_rules[rulesp]() {
						goto l89
					}
					if !_rules[rulevalue]() {
						goto l89
					}
					goto l88
				l89:
					position, tokenIndex = position88, tokenIndex88
					if !_rules[rulefield]() {
						goto l86
					}
					if !_rules[rulesp]() {
						goto l86
					}
					{
						position90 := position
						{
							position91, tokenIndex91 := position, tokenIndex
							if buffer[position] != rune('>') {
								goto l92
							}
							position++
							if buffer[position] != rune('<') {
								goto l92
							}
							position++
							{
								add(ruleAction16, position)
							}
							goto l91
						l92:
							position, tokenIndex = position91, tokenIndex91
							if buffer[position] != rune('<') {
								goto l94
							}
							position++
							if buffer[position] != rune('=') {
								goto l94
							}
							position++
							{
								add(ruleAction17, position)
							}
							goto l91
						l94:
							position, tokenIndex = position91, tokenIndex91
							if buffer[position] != rune('>') {
								goto l96
							}
							position++
							if buffer[position] != rune('=') {
								goto l96
							}
							position++
							{
								add(ruleAction18, position)
							}
							goto l91
						l96:
							position, tokenIndex = position91, tokenIndex91
							if buffer[position] != rune('=') {
								goto l98
							}
							position++
							if buffer[position] != rune('=') {
								goto l98
							}
							position++
							{
								add(ruleAction19, position)
							}
							goto l91
						l98:
							position, tokenIndex = position91, tokenIndex91
							if buffer[position] != rune('!') {
								goto l100
							}
							position++
							if buffer[position] != rune('=') {
								goto l100
							}
							position++
							{
								add(ruleAction20, position)
							}
							goto l91
						l100:
							position, tokenIndex = position91, tokenIndex91
							if buffer[position] != rune('<') {
								goto l102
							}
							position++
							{
								add(ruleAction21, position)
							}
							goto l91
						l102:
							position, tokenIndex = position91, tokenIndex91
							if buffer[position] != rune('>') {
								goto l86
							}
							position++
							{
								add(ruleAction22, position)
							}
						}
					l91:
						add(ruleCOND, position90)
					}
					if !_rules[rulesp]() {
						goto l86
					}
					if !_rules[rulevalue]() {
						goto l86
					}
				}
			l88:
				add(rulearg, position87)
			}
			return true
		l86:
			position, tokenIndex = position86, tokenIndex86
			return false
		},
		/* 5 COND <- <(('>' '<' Action16) / ('<' '=' Action17) / ('>' '=' Action18) / ('=' '=' Action19) / ('!' '=' Action20) / ('<' Action21) / ('>' Action22))> */
		nil,
		/* 6 conditional <- <(Action23 condint condLT condfield condLT condint Action24)> */
		nil,
		/* 7 condint <- <(<(('-'? [1-9] [0-9]*) / '0')> sp Action25)> */
		func() bool {
			position107, tokenIndex107 := position, tokenIndex
			{
				position108 := position
				{
					position109 := position
					{
						position110, tokenIndex110 := position, tokenIndex
						{
							position112, tokenIndex112 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l112
							}
							position++
							goto l113
						l112:
							position, tokenIndex = position112, tokenIndex112
						}
					l113:
						if c := buffer[position]; c < rune('1') || c > rune('9') {
							goto l111
						}
						position++
					l114:
						{
							position115, tokenIndex115 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l115
							}
							position++
							goto l114
						l115:
							position, tokenIndex = position115, tokenIndex115
						}
						goto l110
					l111:
						position, tokenIndex = position110, tokenIndex110
						if buffer[position] != rune('0') {
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
					add(ruleAction25, position)
				}
				add(rulecondint, position108)
			}
			return true
		l107:
			position, tokenIndex = position107, tokenIndex107
			return false
		},
		/* 8 condLT <- <(<(('<' '=') / '<')> sp Action26)> */
		func() bool {
			position117, tokenIndex117 := position, tokenIndex
			{
				position118 := position
				{
					position119 := position
					{
						position120, tokenIndex120 := position, tokenIndex
						if buffer[position] != rune('<') {
							goto l121
						}
						position++
						if buffer[position] != rune('=') {
							goto l121
						}
						position++
						goto l120
					l121:
						position, tokenIndex = position120, tokenIndex120
						if buffer[position] != rune('<') {
							goto l117
						}
						position++
					}
				l120:
					add(rulePegText, position119)
				}
				if !_rules[rulesp]() {
					goto l117
				}
				{
					add(ruleAction26, position)
				}
				add(rulecondLT, position118)
			}
			return true
		l117:
			position, tokenIndex = position117, tokenIndex117
			return false
		},
		/* 9 condfield <- <(<fieldExpr> sp Action27)> */
		nil,
		/* 10 timerange <- <(field sp '=' sp value comma <timestampfmt> Action28 comma <timestampfmt> Action29)> */
		nil,
		/* 11 value <- <(item / (lbrack Action30 list rbrack Action31))> */
		func() bool {
			position125, tokenIndex125 := position, tokenIndex
			{
				position126 := position
				{
					position127, tokenIndex127 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l128
					}
					goto l127
				l128:
					position, tokenIndex = position127, tokenIndex127
					{
						position129 := position
						if buffer[position] != rune('[') {
							goto l125
						}
						position++
						if !_rules[rulesp]() {
							goto l125
						}
						add(rulelbrack, position129)
					}
					{
						add(ruleAction30, position)
					}
					if !_rules[rulelist]() {
						goto l125
					}
					{
						position131 := position
						if !_rules[rulesp]() {
							goto l125
						}
						if buffer[position] != rune(']') {
							goto l125
						}
						position++
						if !_rules[rulesp]() {
							goto l125
						}
						add(rulerbrack, position131)
					}
					{
						add(ruleAction31, position)
					}
				}
			l127:
				add(rulevalue, position126)
			}
			return true
		l125:
			position, tokenIndex = position125, tokenIndex125
			return false
		},
		/* 12 list <- <(item (comma list)?)> */
		func() bool {
			position133, tokenIndex133 := position, tokenIndex
			{
				position134 := position
				if !_rules[ruleitem]() {
					goto l133
				}
				{
					position135, tokenIndex135 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l135
					}
					if !_rules[rulelist]() {
						goto l135
					}
					goto l136
				l135:
					position, tokenIndex = position135, tokenIndex135
				}
			l136:
				add(rulelist, position134)
			}
			return true
		l133:
			position, tokenIndex = position133, tokenIndex133
			return false
		},
		/* 13 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action32) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action33) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action34) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action35) / (<('-'? '.' [0-9]+)> Action36) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action37) / ('"' <doublequotedstring> '"' Action38) / ('\'' <singlequotedstring> '\'' Action39))> */
		func() bool {
			position137, tokenIndex137 := position, tokenIndex
			{
				position138 := position
				{
					position139, tokenIndex139 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l140
					}
					position++
					if buffer[position] != rune('u') {
						goto l140
					}
					position++
					if buffer[position] != rune('l') {
						goto l140
					}
					position++
					if buffer[position] != rune('l') {
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
					goto l139
				l140:
					position, tokenIndex = position139, tokenIndex139
					if buffer[position] != rune('t') {
						goto l145
					}
					position++
					if buffer[position] != rune('r') {
						goto l145
					}
					position++
					if buffer[position] != rune('u') {
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
						add(ruleAction33, position)
					}
					goto l139
				l145:
					position, tokenIndex = position139, tokenIndex139
					if buffer[position] != rune('f') {
						goto l150
					}
					position++
					if buffer[position] != rune('a') {
						goto l150
					}
					position++
					if buffer[position] != rune('l') {
						goto l150
					}
					position++
					if buffer[position] != rune('s') {
						goto l150
					}
					position++
					if buffer[position] != rune('e') {
						goto l150
					}
					position++
					{
						position151, tokenIndex151 := position, tokenIndex
						{
							position152, tokenIndex152 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l153
							}
							goto l152
						l153:
							position, tokenIndex = position152, tokenIndex152
							if !_rules[rulesp]() {
								goto l150
							}
							if !_rules[ruleclose]() {
								goto l150
							}
						}
					l152:
						position, tokenIndex = position151, tokenIndex151
					}
					{
						add(ruleAction34, position)
					}
					goto l139
				l150:
					position, tokenIndex = position139, tokenIndex139
					{
						position156 := position
						{
							position157, tokenIndex157 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l157
							}
							position++
							goto l158
						l157:
							position, tokenIndex = position157, tokenIndex157
						}
					l158:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l155
						}
						position++
					l159:
						{
							position160, tokenIndex160 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l160
							}
							position++
							goto l159
						l160:
							position, tokenIndex = position160, tokenIndex160
						}
						{
							position161, tokenIndex161 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l161
							}
							position++
						l163:
							{
								position164, tokenIndex164 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l164
								}
								position++
								goto l163
							l164:
								position, tokenIndex = position164, tokenIndex164
							}
							goto l162
						l161:
							position, tokenIndex = position161, tokenIndex161
						}
					l162:
						add(rulePegText, position156)
					}
					{
						add(ruleAction35, position)
					}
					goto l139
				l155:
					position, tokenIndex = position139, tokenIndex139
					{
						position167 := position
						{
							position168, tokenIndex168 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l168
							}
							position++
							goto l169
						l168:
							position, tokenIndex = position168, tokenIndex168
						}
					l169:
						if buffer[position] != rune('.') {
							goto l166
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l166
						}
						position++
					l170:
						{
							position171, tokenIndex171 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l171
							}
							position++
							goto l170
						l171:
							position, tokenIndex = position171, tokenIndex171
						}
						add(rulePegText, position167)
					}
					{
						add(ruleAction36, position)
					}
					goto l139
				l166:
					position, tokenIndex = position139, tokenIndex139
					{
						position174 := position
						{
							position177, tokenIndex177 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l178
							}
							position++
							goto l177
						l178:
							position, tokenIndex = position177, tokenIndex177
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l179
							}
							position++
							goto l177
						l179:
							position, tokenIndex = position177, tokenIndex177
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l180
							}
							position++
							goto l177
						l180:
							position, tokenIndex = position177, tokenIndex177
							if buffer[position] != rune('-') {
								goto l181
							}
							position++
							goto l177
						l181:
							position, tokenIndex = position177, tokenIndex177
							if buffer[position] != rune('_') {
								goto l182
							}
							position++
							goto l177
						l182:
							position, tokenIndex = position177, tokenIndex177
							if buffer[position] != rune(':') {
								goto l173
							}
							position++
						}
					l177:
					l175:
						{
							position176, tokenIndex176 := position, tokenIndex
							{
								position183, tokenIndex183 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l184
								}
								position++
								goto l183
							l184:
								position, tokenIndex = position183, tokenIndex183
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l185
								}
								position++
								goto l183
							l185:
								position, tokenIndex = position183, tokenIndex183
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l186
								}
								position++
								goto l183
							l186:
								position, tokenIndex = position183, tokenIndex183
								if buffer[position] != rune('-') {
									goto l187
								}
								position++
								goto l183
							l187:
								position, tokenIndex = position183, tokenIndex183
								if buffer[position] != rune('_') {
									goto l188
								}
								position++
								goto l183
							l188:
								position, tokenIndex = position183, tokenIndex183
								if buffer[position] != rune(':') {
									goto l176
								}
								position++
							}
						l183:
							goto l175
						l176:
							position, tokenIndex = position176, tokenIndex176
						}
						add(rulePegText, position174)
					}
					{
						add(ruleAction37, position)
					}
					goto l139
				l173:
					position, tokenIndex = position139, tokenIndex139
					if buffer[position] != rune('"') {
						goto l190
					}
					position++
					{
						position191 := position
						if !_rules[ruledoublequotedstring]() {
							goto l190
						}
						add(rulePegText, position191)
					}
					if buffer[position] != rune('"') {
						goto l190
					}
					position++
					{
						add(ruleAction38, position)
					}
					goto l139
				l190:
					position, tokenIndex = position139, tokenIndex139
					if buffer[position] != rune('\'') {
						goto l137
					}
					position++
					{
						position193 := position
						if !_rules[rulesinglequotedstring]() {
							goto l137
						}
						add(rulePegText, position193)
					}
					if buffer[position] != rune('\'') {
						goto l137
					}
					position++
					{
						add(ruleAction39, position)
					}
				}
			l139:
				add(ruleitem, position138)
			}
			return true
		l137:
			position, tokenIndex = position137, tokenIndex137
			return false
		},
		/* 14 doublequotedstring <- <((!('"' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		func() bool {
			{
				position196 := position
			l197:
				{
					position198, tokenIndex198 := position, tokenIndex
					{
						position199, tokenIndex199 := position, tokenIndex
						{
							position201, tokenIndex201 := position, tokenIndex
							{
								position202, tokenIndex202 := position, tokenIndex
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
								goto l202
							l204:
								position, tokenIndex = position202, tokenIndex202
								if buffer[position] != rune('\n') {
									goto l201
								}
								position++
							}
						l202:
							goto l200
						l201:
							position, tokenIndex = position201, tokenIndex201
						}
						if !matchDot() {
							goto l200
						}
						goto l199
					l200:
						position, tokenIndex = position199, tokenIndex199
						if buffer[position] != rune('\\') {
							goto l205
						}
						position++
						if buffer[position] != rune('n') {
							goto l205
						}
						position++
						goto l199
					l205:
						position, tokenIndex = position199, tokenIndex199
						if buffer[position] != rune('\\') {
							goto l206
						}
						position++
						if buffer[position] != rune('"') {
							goto l206
						}
						position++
						goto l199
					l206:
						position, tokenIndex = position199, tokenIndex199
						if buffer[position] != rune('\\') {
							goto l207
						}
						position++
						if buffer[position] != rune('\'') {
							goto l207
						}
						position++
						goto l199
					l207:
						position, tokenIndex = position199, tokenIndex199
						if buffer[position] != rune('\\') {
							goto l198
						}
						position++
						if buffer[position] != rune('\\') {
							goto l198
						}
						position++
					}
				l199:
					goto l197
				l198:
					position, tokenIndex = position198, tokenIndex198
				}
				add(ruledoublequotedstring, position196)
			}
			return true
		},
		/* 15 singlequotedstring <- <((!('\'' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		func() bool {
			{
				position209 := position
			l210:
				{
					position211, tokenIndex211 := position, tokenIndex
					{
						position212, tokenIndex212 := position, tokenIndex
						{
							position214, tokenIndex214 := position, tokenIndex
							{
								position215, tokenIndex215 := position, tokenIndex
								if buffer[position] != rune('\'') {
									goto l216
								}
								position++
								goto l215
							l216:
								position, tokenIndex = position215, tokenIndex215
								if buffer[position] != rune('\\') {
									goto l217
								}
								position++
								goto l215
							l217:
								position, tokenIndex = position215, tokenIndex215
								if buffer[position] != rune('\n') {
									goto l214
								}
								position++
							}
						l215:
							goto l213
						l214:
							position, tokenIndex = position214, tokenIndex214
						}
						if !matchDot() {
							goto l213
						}
						goto l212
					l213:
						position, tokenIndex = position212, tokenIndex212
						if buffer[position] != rune('\\') {
							goto l218
						}
						position++
						if buffer[position] != rune('n') {
							goto l218
						}
						position++
						goto l212
					l218:
						position, tokenIndex = position212, tokenIndex212
						if buffer[position] != rune('\\') {
							goto l219
						}
						position++
						if buffer[position] != rune('"') {
							goto l219
						}
						position++
						goto l212
					l219:
						position, tokenIndex = position212, tokenIndex212
						if buffer[position] != rune('\\') {
							goto l220
						}
						position++
						if buffer[position] != rune('\'') {
							goto l220
						}
						position++
						goto l212
					l220:
						position, tokenIndex = position212, tokenIndex212
						if buffer[position] != rune('\\') {
							goto l211
						}
						position++
						if buffer[position] != rune('\\') {
							goto l211
						}
						position++
					}
				l212:
					goto l210
				l211:
					position, tokenIndex = position211, tokenIndex211
				}
				add(rulesinglequotedstring, position209)
			}
			return true
		},
		/* 16 fieldExpr <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_' / '-')*)> */
		func() bool {
			position221, tokenIndex221 := position, tokenIndex
			{
				position222 := position
				{
					position223, tokenIndex223 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l224
					}
					position++
					goto l223
				l224:
					position, tokenIndex = position223, tokenIndex223
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l221
					}
					position++
				}
			l223:
			l225:
				{
					position226, tokenIndex226 := position, tokenIndex
					{
						position227, tokenIndex227 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l228
						}
						position++
						goto l227
					l228:
						position, tokenIndex = position227, tokenIndex227
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l229
						}
						position++
						goto l227
					l229:
						position, tokenIndex = position227, tokenIndex227
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l230
						}
						position++
						goto l227
					l230:
						position, tokenIndex = position227, tokenIndex227
						if buffer[position] != rune('_') {
							goto l231
						}
						position++
						goto l227
					l231:
						position, tokenIndex = position227, tokenIndex227
						if buffer[position] != rune('-') {
							goto l226
						}
						position++
					}
				l227:
					goto l225
				l226:
					position, tokenIndex = position226, tokenIndex226
				}
				add(rulefieldExpr, position222)
			}
			return true
		l221:
			position, tokenIndex = position221, tokenIndex221
			return false
		},
		/* 17 field <- <(<(fieldExpr / reserved)> Action40)> */
		func() bool {
			position232, tokenIndex232 := position, tokenIndex
			{
				position233 := position
				{
					position234 := position
					{
						position235, tokenIndex235 := position, tokenIndex
						if !_rules[rulefieldExpr]() {
							goto l236
						}
						goto l235
					l236:
						position, tokenIndex = position235, tokenIndex235
						{
							position237 := position
							{
								position238, tokenIndex238 := position, tokenIndex
								if buffer[position] != rune('_') {
									goto l239
								}
								position++
								if buffer[position] != rune('r') {
									goto l239
								}
								position++
								if buffer[position] != rune('o') {
									goto l239
								}
								position++
								if buffer[position] != rune('w') {
									goto l239
								}
								position++
								goto l238
							l239:
								position, tokenIndex = position238, tokenIndex238
								if buffer[position] != rune('_') {
									goto l240
								}
								position++
								if buffer[position] != rune('c') {
									goto l240
								}
								position++
								if buffer[position] != rune('o') {
									goto l240
								}
								position++
								if buffer[position] != rune('l') {
									goto l240
								}
								position++
								goto l238
							l240:
								position, tokenIndex = position238, tokenIndex238
								if buffer[position] != rune('_') {
									goto l241
								}
								position++
								if buffer[position] != rune('s') {
									goto l241
								}
								position++
								if buffer[position] != rune('t') {
									goto l241
								}
								position++
								if buffer[position] != rune('a') {
									goto l241
								}
								position++
								if buffer[position] != rune('r') {
									goto l241
								}
								position++
								if buffer[position] != rune('t') {
									goto l241
								}
								position++
								goto l238
							l241:
								position, tokenIndex = position238, tokenIndex238
								if buffer[position] != rune('_') {
									goto l242
								}
								position++
								if buffer[position] != rune('e') {
									goto l242
								}
								position++
								if buffer[position] != rune('n') {
									goto l242
								}
								position++
								if buffer[position] != rune('d') {
									goto l242
								}
								position++
								goto l238
							l242:
								position, tokenIndex = position238, tokenIndex238
								if buffer[position] != rune('_') {
									goto l243
								}
								position++
								if buffer[position] != rune('t') {
									goto l243
								}
								position++
								if buffer[position] != rune('i') {
									goto l243
								}
								position++
								if buffer[position] != rune('m') {
									goto l243
								}
								position++
								if buffer[position] != rune('e') {
									goto l243
								}
								position++
								if buffer[position] != rune('s') {
									goto l243
								}
								position++
								if buffer[position] != rune('t') {
									goto l243
								}
								position++
								if buffer[position] != rune('a') {
									goto l243
								}
								position++
								if buffer[position] != rune('m') {
									goto l243
								}
								position++
								if buffer[position] != rune('p') {
									goto l243
								}
								position++
								goto l238
							l243:
								position, tokenIndex = position238, tokenIndex238
								if buffer[position] != rune('_') {
									goto l232
								}
								position++
								if buffer[position] != rune('f') {
									goto l232
								}
								position++
								if buffer[position] != rune('i') {
									goto l232
								}
								position++
								if buffer[position] != rune('e') {
									goto l232
								}
								position++
								if buffer[position] != rune('l') {
									goto l232
								}
								position++
								if buffer[position] != rune('d') {
									goto l232
								}
								position++
							}
						l238:
							add(rulereserved, position237)
						}
					}
				l235:
					add(rulePegText, position234)
				}
				{
					add(ruleAction40, position)
				}
				add(rulefield, position233)
			}
			return true
		l232:
			position, tokenIndex = position232, tokenIndex232
			return false
		},
		/* 18 reserved <- <(('_' 'r' 'o' 'w') / ('_' 'c' 'o' 'l') / ('_' 's' 't' 'a' 'r' 't') / ('_' 'e' 'n' 'd') / ('_' 't' 'i' 'm' 'e' 's' 't' 'a' 'm' 'p') / ('_' 'f' 'i' 'e' 'l' 'd'))> */
		nil,
		/* 19 posfield <- <(<fieldExpr> Action41)> */
		func() bool {
			position246, tokenIndex246 := position, tokenIndex
			{
				position247 := position
				{
					position248 := position
					if !_rules[rulefieldExpr]() {
						goto l246
					}
					add(rulePegText, position248)
				}
				{
					add(ruleAction41, position)
				}
				add(ruleposfield, position247)
			}
			return true
		l246:
			position, tokenIndex = position246, tokenIndex246
			return false
		},
		/* 20 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position250, tokenIndex250 := position, tokenIndex
			{
				position251 := position
				{
					position252, tokenIndex252 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l253
					}
					position++
				l254:
					{
						position255, tokenIndex255 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l255
						}
						position++
						goto l254
					l255:
						position, tokenIndex = position255, tokenIndex255
					}
					goto l252
				l253:
					position, tokenIndex = position252, tokenIndex252
					if buffer[position] != rune('0') {
						goto l250
					}
					position++
				}
			l252:
				add(ruleuint, position251)
			}
			return true
		l250:
			position, tokenIndex = position250, tokenIndex250
			return false
		},
		/* 21 uintrow <- <(<uint> Action42)> */
		nil,
		/* 22 col <- <((<uint> Action43) / ('\'' <singlequotedstring> '\'' Action44) / ('"' <doublequotedstring> '"' Action45))> */
		func() bool {
			position257, tokenIndex257 := position, tokenIndex
			{
				position258 := position
				{
					position259, tokenIndex259 := position, tokenIndex
					{
						position261 := position
						if !_rules[ruleuint]() {
							goto l260
						}
						add(rulePegText, position261)
					}
					{
						add(ruleAction43, position)
					}
					goto l259
				l260:
					position, tokenIndex = position259, tokenIndex259
					if buffer[position] != rune('\'') {
						goto l263
					}
					position++
					{
						position264 := position
						if !_rules[rulesinglequotedstring]() {
							goto l263
						}
						add(rulePegText, position264)
					}
					if buffer[position] != rune('\'') {
						goto l263
					}
					position++
					{
						add(ruleAction44, position)
					}
					goto l259
				l263:
					position, tokenIndex = position259, tokenIndex259
					if buffer[position] != rune('"') {
						goto l257
					}
					position++
					{
						position266 := position
						if !_rules[ruledoublequotedstring]() {
							goto l257
						}
						add(rulePegText, position266)
					}
					if buffer[position] != rune('"') {
						goto l257
					}
					position++
					{
						add(ruleAction45, position)
					}
				}
			l259:
				add(rulecol, position258)
			}
			return true
		l257:
			position, tokenIndex = position257, tokenIndex257
			return false
		},
		/* 23 row <- <((<uint> Action46) / ('\'' <singlequotedstring> '\'' Action47) / ('"' <doublequotedstring> '"' Action48))> */
		nil,
		/* 24 open <- <('(' sp)> */
		func() bool {
			position269, tokenIndex269 := position, tokenIndex
			{
				position270 := position
				if buffer[position] != rune('(') {
					goto l269
				}
				position++
				if !_rules[rulesp]() {
					goto l269
				}
				add(ruleopen, position270)
			}
			return true
		l269:
			position, tokenIndex = position269, tokenIndex269
			return false
		},
		/* 25 close <- <(')' sp)> */
		func() bool {
			position271, tokenIndex271 := position, tokenIndex
			{
				position272 := position
				if buffer[position] != rune(')') {
					goto l271
				}
				position++
				if !_rules[rulesp]() {
					goto l271
				}
				add(ruleclose, position272)
			}
			return true
		l271:
			position, tokenIndex = position271, tokenIndex271
			return false
		},
		/* 26 sp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position274 := position
			l275:
				{
					position276, tokenIndex276 := position, tokenIndex
					{
						position277, tokenIndex277 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l278
						}
						position++
						goto l277
					l278:
						position, tokenIndex = position277, tokenIndex277
						if buffer[position] != rune('\t') {
							goto l279
						}
						position++
						goto l277
					l279:
						position, tokenIndex = position277, tokenIndex277
						if buffer[position] != rune('\n') {
							goto l276
						}
						position++
					}
				l277:
					goto l275
				l276:
					position, tokenIndex = position276, tokenIndex276
				}
				add(rulesp, position274)
			}
			return true
		},
		/* 27 comma <- <(sp ',' sp)> */
		func() bool {
			position280, tokenIndex280 := position, tokenIndex
			{
				position281 := position
				if !_rules[rulesp]() {
					goto l280
				}
				if buffer[position] != rune(',') {
					goto l280
				}
				position++
				if !_rules[rulesp]() {
					goto l280
				}
				add(rulecomma, position281)
			}
			return true
		l280:
			position, tokenIndex = position280, tokenIndex280
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
			position285, tokenIndex285 := position, tokenIndex
			{
				position286 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l285
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l285
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l285
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l285
				}
				position++
				if buffer[position] != rune('-') {
					goto l285
				}
				position++
				{
					position287, tokenIndex287 := position, tokenIndex
					if buffer[position] != rune('0') {
						goto l288
					}
					position++
					goto l287
				l288:
					position, tokenIndex = position287, tokenIndex287
					if buffer[position] != rune('1') {
						goto l285
					}
					position++
				}
			l287:
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l285
				}
				position++
				if buffer[position] != rune('-') {
					goto l285
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('3') {
					goto l285
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l285
				}
				position++
				if buffer[position] != rune('T') {
					goto l285
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l285
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l285
				}
				position++
				if buffer[position] != rune(':') {
					goto l285
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l285
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l285
				}
				position++
				add(ruletimestampbasicfmt, position286)
			}
			return true
		l285:
			position, tokenIndex = position285, tokenIndex285
			return false
		},
		/* 32 timestampfmt <- <(('"' timestampbasicfmt '"') / ('\'' timestampbasicfmt '\'') / timestampbasicfmt)> */
		func() bool {
			position289, tokenIndex289 := position, tokenIndex
			{
				position290 := position
				{
					position291, tokenIndex291 := position, tokenIndex
					if buffer[position] != rune('"') {
						goto l292
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l292
					}
					if buffer[position] != rune('"') {
						goto l292
					}
					position++
					goto l291
				l292:
					position, tokenIndex = position291, tokenIndex291
					if buffer[position] != rune('\'') {
						goto l293
					}
					position++
					if !_rules[ruletimestampbasicfmt]() {
						goto l293
					}
					if buffer[position] != rune('\'') {
						goto l293
					}
					position++
					goto l291
				l293:
					position, tokenIndex = position291, tokenIndex291
					if !_rules[ruletimestampbasicfmt]() {
						goto l289
					}
				}
			l291:
				add(ruletimestampfmt, position290)
			}
			return true
		l289:
			position, tokenIndex = position289, tokenIndex289
			return false
		},
		/* 33 timestamp <- <(<timestampfmt> Action49)> */
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
		/* 45 Action10 <- <{p.startCall("TopN")}> */
		nil,
		/* 46 Action11 <- <{p.endCall()}> */
		nil,
		/* 47 Action12 <- <{p.startCall("Range")}> */
		nil,
		/* 48 Action13 <- <{p.endCall()}> */
		nil,
		nil,
		/* 50 Action14 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 51 Action15 <- <{ p.endCall() }> */
		nil,
		/* 52 Action16 <- <{ p.addBTWN() }> */
		nil,
		/* 53 Action17 <- <{ p.addLTE() }> */
		nil,
		/* 54 Action18 <- <{ p.addGTE() }> */
		nil,
		/* 55 Action19 <- <{ p.addEQ() }> */
		nil,
		/* 56 Action20 <- <{ p.addNEQ() }> */
		nil,
		/* 57 Action21 <- <{ p.addLT() }> */
		nil,
		/* 58 Action22 <- <{ p.addGT() }> */
		nil,
		/* 59 Action23 <- <{p.startConditional()}> */
		nil,
		/* 60 Action24 <- <{p.endConditional()}> */
		nil,
		/* 61 Action25 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 62 Action26 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 63 Action27 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 64 Action28 <- <{p.addPosStr("_start", buffer[begin:end])}> */
		nil,
		/* 65 Action29 <- <{p.addPosStr("_end", buffer[begin:end])}> */
		nil,
		/* 66 Action30 <- <{ p.startList() }> */
		nil,
		/* 67 Action31 <- <{ p.endList() }> */
		nil,
		/* 68 Action32 <- <{ p.addVal(nil) }> */
		nil,
		/* 69 Action33 <- <{ p.addVal(true) }> */
		nil,
		/* 70 Action34 <- <{ p.addVal(false) }> */
		nil,
		/* 71 Action35 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 72 Action36 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 73 Action37 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 74 Action38 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 75 Action39 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 76 Action40 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 77 Action41 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 78 Action42 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 79 Action43 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 80 Action44 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 81 Action45 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 82 Action46 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 83 Action47 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 84 Action48 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 85 Action49 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
