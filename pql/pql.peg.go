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
	ruleAction16
	ruleAction17
	ruleAction18
	ruleAction19
	ruleAction20
	ruleAction21
	rulePegText
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
	ruleAction54
	ruleAction55
	ruleAction56
	ruleAction57
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
	"Action16",
	"Action17",
	"Action18",
	"Action19",
	"Action20",
	"Action21",
	"PegText",
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
	"Action54",
	"Action55",
	"Action56",
	"Action57",
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
	rules  [92]func() bool
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
			p.startCall("Range")
		case ruleAction17:
			p.addField("from")
		case ruleAction18:
			p.addVal(buffer[begin:end])
		case ruleAction19:
			p.addField("to")
		case ruleAction20:
			p.addVal(buffer[begin:end])
		case ruleAction21:
			p.endCall()
		case ruleAction22:
			p.startCall(buffer[begin:end])
		case ruleAction23:
			p.endCall()
		case ruleAction24:
			p.addBTWN()
		case ruleAction25:
			p.addLTE()
		case ruleAction26:
			p.addGTE()
		case ruleAction27:
			p.addEQ()
		case ruleAction28:
			p.addNEQ()
		case ruleAction29:
			p.addLT()
		case ruleAction30:
			p.addGT()
		case ruleAction31:
			p.startConditional()
		case ruleAction32:
			p.endConditional()
		case ruleAction33:
			p.condAdd(buffer[begin:end])
		case ruleAction34:
			p.condAdd(buffer[begin:end])
		case ruleAction35:
			p.condAdd(buffer[begin:end])
		case ruleAction36:
			p.startList()
		case ruleAction37:
			p.endList()
		case ruleAction38:
			p.addVal(nil)
		case ruleAction39:
			p.addVal(true)
		case ruleAction40:
			p.addVal(false)
		case ruleAction41:
			p.addVal(buffer[begin:end])
		case ruleAction42:
			p.addNumVal(buffer[begin:end])
		case ruleAction43:
			p.addNumVal(buffer[begin:end])
		case ruleAction44:
			p.startCall(buffer[begin:end])
		case ruleAction45:
			p.addVal(p.endCall())
		case ruleAction46:
			p.addVal(buffer[begin:end])
		case ruleAction47:
			s, _ := strconv.Unquote(buffer[begin:end])
			p.addVal(s)
		case ruleAction48:
			p.addVal(buffer[begin:end])
		case ruleAction49:
			p.addField(buffer[begin:end])
		case ruleAction50:
			p.addPosStr("_field", buffer[begin:end])
		case ruleAction51:
			p.addPosNum("_col", buffer[begin:end])
		case ruleAction52:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction53:
			p.addPosStr("_col", buffer[begin:end])
		case ruleAction54:
			p.addPosNum("_row", buffer[begin:end])
		case ruleAction55:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction56:
			p.addPosStr("_row", buffer[begin:end])
		case ruleAction57:
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
		/* 1 Call <- <(('S' 'e' 't' Action0 open col comma args (comma timestamp)? close Action1) / ('S' 'e' 't' 'R' 'o' 'w' 'A' 't' 't' 'r' 's' Action2 open posfield comma row comma args close Action3) / ('S' 'e' 't' 'C' 'o' 'l' 'u' 'm' 'n' 'A' 't' 't' 'r' 's' Action4 open col comma args close Action5) / ('C' 'l' 'e' 'a' 'r' Action6 open col comma args close Action7) / ('C' 'l' 'e' 'a' 'r' 'R' 'o' 'w' Action8 open arg close Action9) / ('S' 't' 'o' 'r' 'e' Action10 open Call comma arg close Action11) / ('T' 'o' 'p' 'N' Action12 open posfield (comma allargs)? close Action13) / ('R' 'o' 'w' 's' Action14 open posfield (comma allargs)? close Action15) / ('R' 'a' 'n' 'g' 'e' Action16 open field sp '=' sp value comma ('f' 'r' 'o' 'm' '=')? Action17 timestampfmt Action18 comma ('t' 'o' '=')? sp Action19 timestampfmt Action20 close Action21) / (<IDENT> Action22 open allargs comma? close Action23))> */
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
								add(ruleAction57, position)
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
								add(ruleAction54, position)
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
								add(ruleAction55, position)
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
								add(ruleAction56, position)
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
					if !_rules[rulefield]() {
						goto l51
					}
					if !_rules[rulesp]() {
						goto l51
					}
					if buffer[position] != rune('=') {
						goto l51
					}
					position++
					if !_rules[rulesp]() {
						goto l51
					}
					if !_rules[rulevalue]() {
						goto l51
					}
					if !_rules[rulecomma]() {
						goto l51
					}
					{
						position53, tokenIndex53 := position, tokenIndex
						if buffer[position] != rune('f') {
							goto l53
						}
						position++
						if buffer[position] != rune('r') {
							goto l53
						}
						position++
						if buffer[position] != rune('o') {
							goto l53
						}
						position++
						if buffer[position] != rune('m') {
							goto l53
						}
						position++
						if buffer[position] != rune('=') {
							goto l53
						}
						position++
						goto l54
					l53:
						position, tokenIndex = position53, tokenIndex53
					}
				l54:
					{
						add(ruleAction17, position)
					}
					if !_rules[ruletimestampfmt]() {
						goto l51
					}
					{
						add(ruleAction18, position)
					}
					if !_rules[rulecomma]() {
						goto l51
					}
					{
						position57, tokenIndex57 := position, tokenIndex
						if buffer[position] != rune('t') {
							goto l57
						}
						position++
						if buffer[position] != rune('o') {
							goto l57
						}
						position++
						if buffer[position] != rune('=') {
							goto l57
						}
						position++
						goto l58
					l57:
						position, tokenIndex = position57, tokenIndex57
					}
				l58:
					if !_rules[rulesp]() {
						goto l51
					}
					{
						add(ruleAction19, position)
					}
					if !_rules[ruletimestampfmt]() {
						goto l51
					}
					{
						add(ruleAction20, position)
					}
					if !_rules[ruleclose]() {
						goto l51
					}
					{
						add(ruleAction21, position)
					}
					goto l7
				l51:
					position, tokenIndex = position7, tokenIndex7
					{
						position62 := position
						if !_rules[ruleIDENT]() {
							goto l5
						}
						add(rulePegText, position62)
					}
					{
						add(ruleAction22, position)
					}
					if !_rules[ruleopen]() {
						goto l5
					}
					if !_rules[ruleallargs]() {
						goto l5
					}
					{
						position64, tokenIndex64 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l64
						}
						goto l65
					l64:
						position, tokenIndex = position64, tokenIndex64
					}
				l65:
					if !_rules[ruleclose]() {
						goto l5
					}
					{
						add(ruleAction23, position)
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
			position67, tokenIndex67 := position, tokenIndex
			{
				position68 := position
				{
					position69, tokenIndex69 := position, tokenIndex
					if !_rules[ruleCall]() {
						goto l70
					}
				l71:
					{
						position72, tokenIndex72 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l72
						}
						if !_rules[ruleCall]() {
							goto l72
						}
						goto l71
					l72:
						position, tokenIndex = position72, tokenIndex72
					}
					{
						position73, tokenIndex73 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l73
						}
						if !_rules[ruleargs]() {
							goto l73
						}
						goto l74
					l73:
						position, tokenIndex = position73, tokenIndex73
					}
				l74:
					goto l69
				l70:
					position, tokenIndex = position69, tokenIndex69
					if !_rules[ruleargs]() {
						goto l75
					}
					goto l69
				l75:
					position, tokenIndex = position69, tokenIndex69
					if !_rules[rulesp]() {
						goto l67
					}
				}
			l69:
				add(ruleallargs, position68)
			}
			return true
		l67:
			position, tokenIndex = position67, tokenIndex67
			return false
		},
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position76, tokenIndex76 := position, tokenIndex
			{
				position77 := position
				if !_rules[rulearg]() {
					goto l76
				}
				{
					position78, tokenIndex78 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l78
					}
					if !_rules[ruleargs]() {
						goto l78
					}
					goto l79
				l78:
					position, tokenIndex = position78, tokenIndex78
				}
			l79:
				if !_rules[rulesp]() {
					goto l76
				}
				add(ruleargs, position77)
			}
			return true
		l76:
			position, tokenIndex = position76, tokenIndex76
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value) / conditional)> */
		func() bool {
			position80, tokenIndex80 := position, tokenIndex
			{
				position81 := position
				{
					position82, tokenIndex82 := position, tokenIndex
					if !_rules[rulefield]() {
						goto l83
					}
					if !_rules[rulesp]() {
						goto l83
					}
					if buffer[position] != rune('=') {
						goto l83
					}
					position++
					if !_rules[rulesp]() {
						goto l83
					}
					if !_rules[rulevalue]() {
						goto l83
					}
					goto l82
				l83:
					position, tokenIndex = position82, tokenIndex82
					if !_rules[rulefield]() {
						goto l84
					}
					if !_rules[rulesp]() {
						goto l84
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
								add(ruleAction24, position)
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
								add(ruleAction25, position)
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
								add(ruleAction26, position)
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
								add(ruleAction27, position)
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
								add(ruleAction28, position)
							}
							goto l86
						l95:
							position, tokenIndex = position86, tokenIndex86
							if buffer[position] != rune('<') {
								goto l97
							}
							position++
							{
								add(ruleAction29, position)
							}
							goto l86
						l97:
							position, tokenIndex = position86, tokenIndex86
							if buffer[position] != rune('>') {
								goto l84
							}
							position++
							{
								add(ruleAction30, position)
							}
						}
					l86:
						add(ruleCOND, position85)
					}
					if !_rules[rulesp]() {
						goto l84
					}
					if !_rules[rulevalue]() {
						goto l84
					}
					goto l82
				l84:
					position, tokenIndex = position82, tokenIndex82
					{
						position100 := position
						{
							add(ruleAction31, position)
						}
						if !_rules[rulecondint]() {
							goto l80
						}
						if !_rules[rulecondLT]() {
							goto l80
						}
						{
							position102 := position
							{
								position103 := position
								if !_rules[rulefieldExpr]() {
									goto l80
								}
								add(rulePegText, position103)
							}
							if !_rules[rulesp]() {
								goto l80
							}
							{
								add(ruleAction35, position)
							}
							add(rulecondfield, position102)
						}
						if !_rules[rulecondLT]() {
							goto l80
						}
						if !_rules[rulecondint]() {
							goto l80
						}
						{
							add(ruleAction32, position)
						}
						add(ruleconditional, position100)
					}
				}
			l82:
				add(rulearg, position81)
			}
			return true
		l80:
			position, tokenIndex = position80, tokenIndex80
			return false
		},
		/* 5 COND <- <(('>' '<' Action24) / ('<' '=' Action25) / ('>' '=' Action26) / ('=' '=' Action27) / ('!' '=' Action28) / ('<' Action29) / ('>' Action30))> */
		nil,
		/* 6 conditional <- <(Action31 condint condLT condfield condLT condint Action32)> */
		nil,
		/* 7 condint <- <(<(('-'? [0-9]* '.' [0-9]+) / '0' / ('-'? [1-9] [0-9]*))> sp Action33)> */
		func() bool {
			position108, tokenIndex108 := position, tokenIndex
			{
				position109 := position
				{
					position110 := position
					{
						position111, tokenIndex111 := position, tokenIndex
						{
							position113, tokenIndex113 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l113
							}
							position++
							goto l114
						l113:
							position, tokenIndex = position113, tokenIndex113
						}
					l114:
					l115:
						{
							position116, tokenIndex116 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l116
							}
							position++
							goto l115
						l116:
							position, tokenIndex = position116, tokenIndex116
						}
						if buffer[position] != rune('.') {
							goto l112
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l112
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
						goto l111
					l112:
						position, tokenIndex = position111, tokenIndex111
						if buffer[position] != rune('0') {
							goto l119
						}
						position++
						goto l111
					l119:
						position, tokenIndex = position111, tokenIndex111
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
							goto l108
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
					}
				l111:
					add(rulePegText, position110)
				}
				if !_rules[rulesp]() {
					goto l108
				}
				{
					add(ruleAction33, position)
				}
				add(rulecondint, position109)
			}
			return true
		l108:
			position, tokenIndex = position108, tokenIndex108
			return false
		},
		/* 8 condLT <- <(<(('<' '=') / '<')> sp Action34)> */
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
					add(ruleAction34, position)
				}
				add(rulecondLT, position126)
			}
			return true
		l125:
			position, tokenIndex = position125, tokenIndex125
			return false
		},
		/* 9 condfield <- <(<fieldExpr> sp Action35)> */
		nil,
		/* 10 value <- <(item / (lbrack Action36 list rbrack Action37))> */
		func() bool {
			position132, tokenIndex132 := position, tokenIndex
			{
				position133 := position
				{
					position134, tokenIndex134 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l135
					}
					goto l134
				l135:
					position, tokenIndex = position134, tokenIndex134
					{
						position136 := position
						if buffer[position] != rune('[') {
							goto l132
						}
						position++
						if !_rules[rulesp]() {
							goto l132
						}
						add(rulelbrack, position136)
					}
					{
						add(ruleAction36, position)
					}
					if !_rules[rulelist]() {
						goto l132
					}
					{
						position138 := position
						if !_rules[rulesp]() {
							goto l132
						}
						if buffer[position] != rune(']') {
							goto l132
						}
						position++
						if !_rules[rulesp]() {
							goto l132
						}
						add(rulerbrack, position138)
					}
					{
						add(ruleAction37, position)
					}
				}
			l134:
				add(rulevalue, position133)
			}
			return true
		l132:
			position, tokenIndex = position132, tokenIndex132
			return false
		},
		/* 11 list <- <(item (comma list)?)> */
		func() bool {
			position140, tokenIndex140 := position, tokenIndex
			{
				position141 := position
				if !_rules[ruleitem]() {
					goto l140
				}
				{
					position142, tokenIndex142 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l142
					}
					if !_rules[rulelist]() {
						goto l142
					}
					goto l143
				l142:
					position, tokenIndex = position142, tokenIndex142
				}
			l143:
				add(rulelist, position141)
			}
			return true
		l140:
			position, tokenIndex = position140, tokenIndex140
			return false
		},
		/* 12 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action38) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action39) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action40) / (timestampfmt Action41) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action42) / (<('-'? '.' [0-9]+)> Action43) / (<IDENT> Action44 open allargs comma? close Action45) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action46) / (<('"' doublequotedstring '"')> Action47) / ('\'' <singlequotedstring> '\'' Action48))> */
		func() bool {
			position144, tokenIndex144 := position, tokenIndex
			{
				position145 := position
				{
					position146, tokenIndex146 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l147
					}
					position++
					if buffer[position] != rune('u') {
						goto l147
					}
					position++
					if buffer[position] != rune('l') {
						goto l147
					}
					position++
					if buffer[position] != rune('l') {
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
						add(ruleAction38, position)
					}
					goto l146
				l147:
					position, tokenIndex = position146, tokenIndex146
					if buffer[position] != rune('t') {
						goto l152
					}
					position++
					if buffer[position] != rune('r') {
						goto l152
					}
					position++
					if buffer[position] != rune('u') {
						goto l152
					}
					position++
					if buffer[position] != rune('e') {
						goto l152
					}
					position++
					{
						position153, tokenIndex153 := position, tokenIndex
						{
							position154, tokenIndex154 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l155
							}
							goto l154
						l155:
							position, tokenIndex = position154, tokenIndex154
							if !_rules[rulesp]() {
								goto l152
							}
							if !_rules[ruleclose]() {
								goto l152
							}
						}
					l154:
						position, tokenIndex = position153, tokenIndex153
					}
					{
						add(ruleAction39, position)
					}
					goto l146
				l152:
					position, tokenIndex = position146, tokenIndex146
					if buffer[position] != rune('f') {
						goto l157
					}
					position++
					if buffer[position] != rune('a') {
						goto l157
					}
					position++
					if buffer[position] != rune('l') {
						goto l157
					}
					position++
					if buffer[position] != rune('s') {
						goto l157
					}
					position++
					if buffer[position] != rune('e') {
						goto l157
					}
					position++
					{
						position158, tokenIndex158 := position, tokenIndex
						{
							position159, tokenIndex159 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l160
							}
							goto l159
						l160:
							position, tokenIndex = position159, tokenIndex159
							if !_rules[rulesp]() {
								goto l157
							}
							if !_rules[ruleclose]() {
								goto l157
							}
						}
					l159:
						position, tokenIndex = position158, tokenIndex158
					}
					{
						add(ruleAction40, position)
					}
					goto l146
				l157:
					position, tokenIndex = position146, tokenIndex146
					if !_rules[ruletimestampfmt]() {
						goto l162
					}
					{
						add(ruleAction41, position)
					}
					goto l146
				l162:
					position, tokenIndex = position146, tokenIndex146
					{
						position165 := position
						{
							position166, tokenIndex166 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l166
							}
							position++
							goto l167
						l166:
							position, tokenIndex = position166, tokenIndex166
						}
					l167:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l164
						}
						position++
					l168:
						{
							position169, tokenIndex169 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l169
							}
							position++
							goto l168
						l169:
							position, tokenIndex = position169, tokenIndex169
						}
						{
							position170, tokenIndex170 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l170
							}
							position++
						l172:
							{
								position173, tokenIndex173 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l173
								}
								position++
								goto l172
							l173:
								position, tokenIndex = position173, tokenIndex173
							}
							goto l171
						l170:
							position, tokenIndex = position170, tokenIndex170
						}
					l171:
						add(rulePegText, position165)
					}
					{
						add(ruleAction42, position)
					}
					goto l146
				l164:
					position, tokenIndex = position146, tokenIndex146
					{
						position176 := position
						{
							position177, tokenIndex177 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l177
							}
							position++
							goto l178
						l177:
							position, tokenIndex = position177, tokenIndex177
						}
					l178:
						if buffer[position] != rune('.') {
							goto l175
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l175
						}
						position++
					l179:
						{
							position180, tokenIndex180 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l180
							}
							position++
							goto l179
						l180:
							position, tokenIndex = position180, tokenIndex180
						}
						add(rulePegText, position176)
					}
					{
						add(ruleAction43, position)
					}
					goto l146
				l175:
					position, tokenIndex = position146, tokenIndex146
					{
						position183 := position
						if !_rules[ruleIDENT]() {
							goto l182
						}
						add(rulePegText, position183)
					}
					{
						add(ruleAction44, position)
					}
					if !_rules[ruleopen]() {
						goto l182
					}
					if !_rules[ruleallargs]() {
						goto l182
					}
					{
						position185, tokenIndex185 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l185
						}
						goto l186
					l185:
						position, tokenIndex = position185, tokenIndex185
					}
				l186:
					if !_rules[ruleclose]() {
						goto l182
					}
					{
						add(ruleAction45, position)
					}
					goto l146
				l182:
					position, tokenIndex = position146, tokenIndex146
					{
						position189 := position
						{
							position192, tokenIndex192 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l193
							}
							position++
							goto l192
						l193:
							position, tokenIndex = position192, tokenIndex192
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l194
							}
							position++
							goto l192
						l194:
							position, tokenIndex = position192, tokenIndex192
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l195
							}
							position++
							goto l192
						l195:
							position, tokenIndex = position192, tokenIndex192
							if buffer[position] != rune('-') {
								goto l196
							}
							position++
							goto l192
						l196:
							position, tokenIndex = position192, tokenIndex192
							if buffer[position] != rune('_') {
								goto l197
							}
							position++
							goto l192
						l197:
							position, tokenIndex = position192, tokenIndex192
							if buffer[position] != rune(':') {
								goto l188
							}
							position++
						}
					l192:
					l190:
						{
							position191, tokenIndex191 := position, tokenIndex
							{
								position198, tokenIndex198 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l199
								}
								position++
								goto l198
							l199:
								position, tokenIndex = position198, tokenIndex198
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l200
								}
								position++
								goto l198
							l200:
								position, tokenIndex = position198, tokenIndex198
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l201
								}
								position++
								goto l198
							l201:
								position, tokenIndex = position198, tokenIndex198
								if buffer[position] != rune('-') {
									goto l202
								}
								position++
								goto l198
							l202:
								position, tokenIndex = position198, tokenIndex198
								if buffer[position] != rune('_') {
									goto l203
								}
								position++
								goto l198
							l203:
								position, tokenIndex = position198, tokenIndex198
								if buffer[position] != rune(':') {
									goto l191
								}
								position++
							}
						l198:
							goto l190
						l191:
							position, tokenIndex = position191, tokenIndex191
						}
						add(rulePegText, position189)
					}
					{
						add(ruleAction46, position)
					}
					goto l146
				l188:
					position, tokenIndex = position146, tokenIndex146
					{
						position206 := position
						if buffer[position] != rune('"') {
							goto l205
						}
						position++
						if !_rules[ruledoublequotedstring]() {
							goto l205
						}
						if buffer[position] != rune('"') {
							goto l205
						}
						position++
						add(rulePegText, position206)
					}
					{
						add(ruleAction47, position)
					}
					goto l146
				l205:
					position, tokenIndex = position146, tokenIndex146
					if buffer[position] != rune('\'') {
						goto l144
					}
					position++
					{
						position208 := position
						if !_rules[rulesinglequotedstring]() {
							goto l144
						}
						add(rulePegText, position208)
					}
					if buffer[position] != rune('\'') {
						goto l144
					}
					position++
					{
						add(ruleAction48, position)
					}
				}
			l146:
				add(ruleitem, position145)
			}
			return true
		l144:
			position, tokenIndex = position144, tokenIndex144
			return false
		},
		/* 13 doublequotedstring <- <(('\\' '"') / ('\\' '\\') / (!'"' .))*> */
		func() bool {
			{
				position211 := position
			l212:
				{
					position213, tokenIndex213 := position, tokenIndex
					{
						position214, tokenIndex214 := position, tokenIndex
						if buffer[position] != rune('\\') {
							goto l215
						}
						position++
						if buffer[position] != rune('"') {
							goto l215
						}
						position++
						goto l214
					l215:
						position, tokenIndex = position214, tokenIndex214
						if buffer[position] != rune('\\') {
							goto l216
						}
						position++
						if buffer[position] != rune('\\') {
							goto l216
						}
						position++
						goto l214
					l216:
						position, tokenIndex = position214, tokenIndex214
						{
							position217, tokenIndex217 := position, tokenIndex
							if buffer[position] != rune('"') {
								goto l217
							}
							position++
							goto l213
						l217:
							position, tokenIndex = position217, tokenIndex217
						}
						if !matchDot() {
							goto l213
						}
					}
				l214:
					goto l212
				l213:
					position, tokenIndex = position213, tokenIndex213
				}
				add(ruledoublequotedstring, position211)
			}
			return true
		},
		/* 14 singlequotedstring <- <(('\\' '\'') / ('\\' '\\') / (!'\'' .))*> */
		func() bool {
			{
				position219 := position
			l220:
				{
					position221, tokenIndex221 := position, tokenIndex
					{
						position222, tokenIndex222 := position, tokenIndex
						if buffer[position] != rune('\\') {
							goto l223
						}
						position++
						if buffer[position] != rune('\'') {
							goto l223
						}
						position++
						goto l222
					l223:
						position, tokenIndex = position222, tokenIndex222
						if buffer[position] != rune('\\') {
							goto l224
						}
						position++
						if buffer[position] != rune('\\') {
							goto l224
						}
						position++
						goto l222
					l224:
						position, tokenIndex = position222, tokenIndex222
						{
							position225, tokenIndex225 := position, tokenIndex
							if buffer[position] != rune('\'') {
								goto l225
							}
							position++
							goto l221
						l225:
							position, tokenIndex = position225, tokenIndex225
						}
						if !matchDot() {
							goto l221
						}
					}
				l222:
					goto l220
				l221:
					position, tokenIndex = position221, tokenIndex221
				}
				add(rulesinglequotedstring, position219)
			}
			return true
		},
		/* 15 fieldExpr <- <(([a-z] / [A-Z] / '_') ([a-z] / [A-Z] / [0-9] / '_' / '-')*)> */
		func() bool {
			position226, tokenIndex226 := position, tokenIndex
			{
				position227 := position
				{
					position228, tokenIndex228 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l229
					}
					position++
					goto l228
				l229:
					position, tokenIndex = position228, tokenIndex228
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l230
					}
					position++
					goto l228
				l230:
					position, tokenIndex = position228, tokenIndex228
					if buffer[position] != rune('_') {
						goto l226
					}
					position++
				}
			l228:
			l231:
				{
					position232, tokenIndex232 := position, tokenIndex
					{
						position233, tokenIndex233 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l234
						}
						position++
						goto l233
					l234:
						position, tokenIndex = position233, tokenIndex233
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l235
						}
						position++
						goto l233
					l235:
						position, tokenIndex = position233, tokenIndex233
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l236
						}
						position++
						goto l233
					l236:
						position, tokenIndex = position233, tokenIndex233
						if buffer[position] != rune('_') {
							goto l237
						}
						position++
						goto l233
					l237:
						position, tokenIndex = position233, tokenIndex233
						if buffer[position] != rune('-') {
							goto l232
						}
						position++
					}
				l233:
					goto l231
				l232:
					position, tokenIndex = position232, tokenIndex232
				}
				add(rulefieldExpr, position227)
			}
			return true
		l226:
			position, tokenIndex = position226, tokenIndex226
			return false
		},
		/* 16 field <- <(<(fieldExpr / reserved)> Action49)> */
		func() bool {
			position238, tokenIndex238 := position, tokenIndex
			{
				position239 := position
				{
					position240 := position
					{
						position241, tokenIndex241 := position, tokenIndex
						if !_rules[rulefieldExpr]() {
							goto l242
						}
						goto l241
					l242:
						position, tokenIndex = position241, tokenIndex241
						{
							position243 := position
							{
								position244, tokenIndex244 := position, tokenIndex
								if buffer[position] != rune('_') {
									goto l245
								}
								position++
								if buffer[position] != rune('r') {
									goto l245
								}
								position++
								if buffer[position] != rune('o') {
									goto l245
								}
								position++
								if buffer[position] != rune('w') {
									goto l245
								}
								position++
								goto l244
							l245:
								position, tokenIndex = position244, tokenIndex244
								if buffer[position] != rune('_') {
									goto l246
								}
								position++
								if buffer[position] != rune('c') {
									goto l246
								}
								position++
								if buffer[position] != rune('o') {
									goto l246
								}
								position++
								if buffer[position] != rune('l') {
									goto l246
								}
								position++
								goto l244
							l246:
								position, tokenIndex = position244, tokenIndex244
								if buffer[position] != rune('_') {
									goto l247
								}
								position++
								if buffer[position] != rune('s') {
									goto l247
								}
								position++
								if buffer[position] != rune('t') {
									goto l247
								}
								position++
								if buffer[position] != rune('a') {
									goto l247
								}
								position++
								if buffer[position] != rune('r') {
									goto l247
								}
								position++
								if buffer[position] != rune('t') {
									goto l247
								}
								position++
								goto l244
							l247:
								position, tokenIndex = position244, tokenIndex244
								if buffer[position] != rune('_') {
									goto l248
								}
								position++
								if buffer[position] != rune('e') {
									goto l248
								}
								position++
								if buffer[position] != rune('n') {
									goto l248
								}
								position++
								if buffer[position] != rune('d') {
									goto l248
								}
								position++
								goto l244
							l248:
								position, tokenIndex = position244, tokenIndex244
								if buffer[position] != rune('_') {
									goto l249
								}
								position++
								if buffer[position] != rune('t') {
									goto l249
								}
								position++
								if buffer[position] != rune('i') {
									goto l249
								}
								position++
								if buffer[position] != rune('m') {
									goto l249
								}
								position++
								if buffer[position] != rune('e') {
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
								if buffer[position] != rune('m') {
									goto l249
								}
								position++
								if buffer[position] != rune('p') {
									goto l249
								}
								position++
								goto l244
							l249:
								position, tokenIndex = position244, tokenIndex244
								if buffer[position] != rune('_') {
									goto l238
								}
								position++
								if buffer[position] != rune('f') {
									goto l238
								}
								position++
								if buffer[position] != rune('i') {
									goto l238
								}
								position++
								if buffer[position] != rune('e') {
									goto l238
								}
								position++
								if buffer[position] != rune('l') {
									goto l238
								}
								position++
								if buffer[position] != rune('d') {
									goto l238
								}
								position++
							}
						l244:
							add(rulereserved, position243)
						}
					}
				l241:
					add(rulePegText, position240)
				}
				{
					add(ruleAction49, position)
				}
				add(rulefield, position239)
			}
			return true
		l238:
			position, tokenIndex = position238, tokenIndex238
			return false
		},
		/* 17 reserved <- <(('_' 'r' 'o' 'w') / ('_' 'c' 'o' 'l') / ('_' 's' 't' 'a' 'r' 't') / ('_' 'e' 'n' 'd') / ('_' 't' 'i' 'm' 'e' 's' 't' 'a' 'm' 'p') / ('_' 'f' 'i' 'e' 'l' 'd'))> */
		nil,
		/* 18 posfield <- <(<fieldExpr> Action50)> */
		func() bool {
			position252, tokenIndex252 := position, tokenIndex
			{
				position253 := position
				{
					position254 := position
					if !_rules[rulefieldExpr]() {
						goto l252
					}
					add(rulePegText, position254)
				}
				{
					add(ruleAction50, position)
				}
				add(ruleposfield, position253)
			}
			return true
		l252:
			position, tokenIndex = position252, tokenIndex252
			return false
		},
		/* 19 uint <- <(([1-9] [0-9]*) / '0')> */
		func() bool {
			position256, tokenIndex256 := position, tokenIndex
			{
				position257 := position
				{
					position258, tokenIndex258 := position, tokenIndex
					if c := buffer[position]; c < rune('1') || c > rune('9') {
						goto l259
					}
					position++
				l260:
					{
						position261, tokenIndex261 := position, tokenIndex
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l261
						}
						position++
						goto l260
					l261:
						position, tokenIndex = position261, tokenIndex261
					}
					goto l258
				l259:
					position, tokenIndex = position258, tokenIndex258
					if buffer[position] != rune('0') {
						goto l256
					}
					position++
				}
			l258:
				add(ruleuint, position257)
			}
			return true
		l256:
			position, tokenIndex = position256, tokenIndex256
			return false
		},
		/* 20 col <- <((<uint> Action51) / ('\'' <singlequotedstring> '\'' Action52) / ('"' <doublequotedstring> '"' Action53))> */
		func() bool {
			position262, tokenIndex262 := position, tokenIndex
			{
				position263 := position
				{
					position264, tokenIndex264 := position, tokenIndex
					{
						position266 := position
						if !_rules[ruleuint]() {
							goto l265
						}
						add(rulePegText, position266)
					}
					{
						add(ruleAction51, position)
					}
					goto l264
				l265:
					position, tokenIndex = position264, tokenIndex264
					if buffer[position] != rune('\'') {
						goto l268
					}
					position++
					{
						position269 := position
						if !_rules[rulesinglequotedstring]() {
							goto l268
						}
						add(rulePegText, position269)
					}
					if buffer[position] != rune('\'') {
						goto l268
					}
					position++
					{
						add(ruleAction52, position)
					}
					goto l264
				l268:
					position, tokenIndex = position264, tokenIndex264
					if buffer[position] != rune('"') {
						goto l262
					}
					position++
					{
						position271 := position
						if !_rules[ruledoublequotedstring]() {
							goto l262
						}
						add(rulePegText, position271)
					}
					if buffer[position] != rune('"') {
						goto l262
					}
					position++
					{
						add(ruleAction53, position)
					}
				}
			l264:
				add(rulecol, position263)
			}
			return true
		l262:
			position, tokenIndex = position262, tokenIndex262
			return false
		},
		/* 21 row <- <((<uint> Action54) / ('\'' <singlequotedstring> '\'' Action55) / ('"' <doublequotedstring> '"' Action56))> */
		nil,
		/* 22 open <- <('(' sp)> */
		func() bool {
			position274, tokenIndex274 := position, tokenIndex
			{
				position275 := position
				if buffer[position] != rune('(') {
					goto l274
				}
				position++
				if !_rules[rulesp]() {
					goto l274
				}
				add(ruleopen, position275)
			}
			return true
		l274:
			position, tokenIndex = position274, tokenIndex274
			return false
		},
		/* 23 close <- <(')' sp)> */
		func() bool {
			position276, tokenIndex276 := position, tokenIndex
			{
				position277 := position
				if buffer[position] != rune(')') {
					goto l276
				}
				position++
				if !_rules[rulesp]() {
					goto l276
				}
				add(ruleclose, position277)
			}
			return true
		l276:
			position, tokenIndex = position276, tokenIndex276
			return false
		},
		/* 24 sp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position279 := position
			l280:
				{
					position281, tokenIndex281 := position, tokenIndex
					{
						position282, tokenIndex282 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l283
						}
						position++
						goto l282
					l283:
						position, tokenIndex = position282, tokenIndex282
						if buffer[position] != rune('\t') {
							goto l284
						}
						position++
						goto l282
					l284:
						position, tokenIndex = position282, tokenIndex282
						if buffer[position] != rune('\n') {
							goto l281
						}
						position++
					}
				l282:
					goto l280
				l281:
					position, tokenIndex = position281, tokenIndex281
				}
				add(rulesp, position279)
			}
			return true
		},
		/* 25 comma <- <(sp ',' sp)> */
		func() bool {
			position285, tokenIndex285 := position, tokenIndex
			{
				position286 := position
				if !_rules[rulesp]() {
					goto l285
				}
				if buffer[position] != rune(',') {
					goto l285
				}
				position++
				if !_rules[rulesp]() {
					goto l285
				}
				add(rulecomma, position286)
			}
			return true
		l285:
			position, tokenIndex = position285, tokenIndex285
			return false
		},
		/* 26 lbrack <- <('[' sp)> */
		nil,
		/* 27 rbrack <- <(sp ']' sp)> */
		nil,
		/* 28 IDENT <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9])*)> */
		func() bool {
			position289, tokenIndex289 := position, tokenIndex
			{
				position290 := position
				{
					position291, tokenIndex291 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('z') {
						goto l292
					}
					position++
					goto l291
				l292:
					position, tokenIndex = position291, tokenIndex291
					if c := buffer[position]; c < rune('A') || c > rune('Z') {
						goto l289
					}
					position++
				}
			l291:
			l293:
				{
					position294, tokenIndex294 := position, tokenIndex
					{
						position295, tokenIndex295 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l296
						}
						position++
						goto l295
					l296:
						position, tokenIndex = position295, tokenIndex295
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l297
						}
						position++
						goto l295
					l297:
						position, tokenIndex = position295, tokenIndex295
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l294
						}
						position++
					}
				l295:
					goto l293
				l294:
					position, tokenIndex = position294, tokenIndex294
				}
				add(ruleIDENT, position290)
			}
			return true
		l289:
			position, tokenIndex = position289, tokenIndex289
			return false
		},
		/* 29 timestampbasicfmt <- <([0-9] [0-9] [0-9] [0-9] '-' ('0' / '1') [0-9] '-' [0-3] [0-9] 'T' [0-9] [0-9] ':' [0-9] [0-9])> */
		func() bool {
			position298, tokenIndex298 := position, tokenIndex
			{
				position299 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l298
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l298
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l298
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l298
				}
				position++
				if buffer[position] != rune('-') {
					goto l298
				}
				position++
				{
					position300, tokenIndex300 := position, tokenIndex
					if buffer[position] != rune('0') {
						goto l301
					}
					position++
					goto l300
				l301:
					position, tokenIndex = position300, tokenIndex300
					if buffer[position] != rune('1') {
						goto l298
					}
					position++
				}
			l300:
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l298
				}
				position++
				if buffer[position] != rune('-') {
					goto l298
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('3') {
					goto l298
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l298
				}
				position++
				if buffer[position] != rune('T') {
					goto l298
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l298
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l298
				}
				position++
				if buffer[position] != rune(':') {
					goto l298
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l298
				}
				position++
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l298
				}
				position++
				add(ruletimestampbasicfmt, position299)
			}
			return true
		l298:
			position, tokenIndex = position298, tokenIndex298
			return false
		},
		/* 30 timestampfmt <- <(('"' <timestampbasicfmt> '"') / ('\'' <timestampbasicfmt> '\'') / <timestampbasicfmt>)> */
		func() bool {
			position302, tokenIndex302 := position, tokenIndex
			{
				position303 := position
				{
					position304, tokenIndex304 := position, tokenIndex
					if buffer[position] != rune('"') {
						goto l305
					}
					position++
					{
						position306 := position
						if !_rules[ruletimestampbasicfmt]() {
							goto l305
						}
						add(rulePegText, position306)
					}
					if buffer[position] != rune('"') {
						goto l305
					}
					position++
					goto l304
				l305:
					position, tokenIndex = position304, tokenIndex304
					if buffer[position] != rune('\'') {
						goto l307
					}
					position++
					{
						position308 := position
						if !_rules[ruletimestampbasicfmt]() {
							goto l307
						}
						add(rulePegText, position308)
					}
					if buffer[position] != rune('\'') {
						goto l307
					}
					position++
					goto l304
				l307:
					position, tokenIndex = position304, tokenIndex304
					{
						position309 := position
						if !_rules[ruletimestampbasicfmt]() {
							goto l302
						}
						add(rulePegText, position309)
					}
				}
			l304:
				add(ruletimestampfmt, position303)
			}
			return true
		l302:
			position, tokenIndex = position302, tokenIndex302
			return false
		},
		/* 31 timestamp <- <(<timestampfmt> Action57)> */
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
		/* 49 Action16 <- <{p.startCall("Range")}> */
		nil,
		/* 50 Action17 <- <{p.addField("from")}> */
		nil,
		/* 51 Action18 <- <{p.addVal(buffer[begin:end])}> */
		nil,
		/* 52 Action19 <- <{p.addField("to")}> */
		nil,
		/* 53 Action20 <- <{p.addVal(buffer[begin:end])}> */
		nil,
		/* 54 Action21 <- <{p.endCall()}> */
		nil,
		nil,
		/* 56 Action22 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 57 Action23 <- <{ p.endCall() }> */
		nil,
		/* 58 Action24 <- <{ p.addBTWN() }> */
		nil,
		/* 59 Action25 <- <{ p.addLTE() }> */
		nil,
		/* 60 Action26 <- <{ p.addGTE() }> */
		nil,
		/* 61 Action27 <- <{ p.addEQ() }> */
		nil,
		/* 62 Action28 <- <{ p.addNEQ() }> */
		nil,
		/* 63 Action29 <- <{ p.addLT() }> */
		nil,
		/* 64 Action30 <- <{ p.addGT() }> */
		nil,
		/* 65 Action31 <- <{p.startConditional()}> */
		nil,
		/* 66 Action32 <- <{p.endConditional()}> */
		nil,
		/* 67 Action33 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 68 Action34 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 69 Action35 <- <{p.condAdd(buffer[begin:end])}> */
		nil,
		/* 70 Action36 <- <{ p.startList() }> */
		nil,
		/* 71 Action37 <- <{ p.endList() }> */
		nil,
		/* 72 Action38 <- <{ p.addVal(nil) }> */
		nil,
		/* 73 Action39 <- <{ p.addVal(true) }> */
		nil,
		/* 74 Action40 <- <{ p.addVal(false) }> */
		nil,
		/* 75 Action41 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 76 Action42 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 77 Action43 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 78 Action44 <- <{ p.startCall(buffer[begin:end]) }> */
		nil,
		/* 79 Action45 <- <{ p.addVal(p.endCall()) }> */
		nil,
		/* 80 Action46 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 81 Action47 <- <{ s, _ := strconv.Unquote(buffer[begin:end]); p.addVal(s) }> */
		nil,
		/* 82 Action48 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 83 Action49 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
		/* 84 Action50 <- <{ p.addPosStr("_field", buffer[begin:end]) }> */
		nil,
		/* 85 Action51 <- <{p.addPosNum("_col", buffer[begin:end])}> */
		nil,
		/* 86 Action52 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 87 Action53 <- <{p.addPosStr("_col", buffer[begin:end])}> */
		nil,
		/* 88 Action54 <- <{p.addPosNum("_row", buffer[begin:end])}> */
		nil,
		/* 89 Action55 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 90 Action56 <- <{p.addPosStr("_row", buffer[begin:end])}> */
		nil,
		/* 91 Action57 <- <{p.addPosStr("_timestamp", buffer[begin:end])}> */
		nil,
	}
	p.rules = _rules
}
