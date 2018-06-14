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
	ruleopen
	rulevalue
	rulelist
	ruleitem
	ruledoublequotedstring
	rulesinglequotedstring
	rulefield
	ruleclose
	rulesp
	rulecomma
	rulelbrack
	rulerbrack
	rulewhitesp
	ruleIDENT
	rulePegText
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
)

var rul3s = [...]string{
	"Unknown",
	"Calls",
	"Call",
	"allargs",
	"args",
	"arg",
	"COND",
	"open",
	"value",
	"list",
	"item",
	"doublequotedstring",
	"singlequotedstring",
	"field",
	"close",
	"sp",
	"comma",
	"lbrack",
	"rbrack",
	"whitesp",
	"IDENT",
	"PegText",
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
	rules  [42]func() bool
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
			p.startCall(buffer[begin:end])
		case ruleAction1:
			p.endCall()
		case ruleAction2:
			p.addBTWN()
		case ruleAction3:
			p.addLTE()
		case ruleAction4:
			p.addGTE()
		case ruleAction5:
			p.addEQ()
		case ruleAction6:
			p.addNEQ()
		case ruleAction7:
			p.addLT()
		case ruleAction8:
			p.addGT()
		case ruleAction9:
			p.startList()
		case ruleAction10:
			p.endList()
		case ruleAction11:
			p.addVal(nil)
		case ruleAction12:
			p.addVal(true)
		case ruleAction13:
			p.addVal(false)
		case ruleAction14:
			p.addNumVal(buffer[begin:end])
		case ruleAction15:
			p.addNumVal(buffer[begin:end])
		case ruleAction16:
			p.addVal(buffer[begin:end])
		case ruleAction17:
			p.addVal(buffer[begin:end])
		case ruleAction18:
			p.addVal(buffer[begin:end])
		case ruleAction19:
			p.addField(buffer[begin:end])

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
		/* 0 Calls <- <(Call* !.)> */
		func() bool {
			position0, tokenIndex0 := position, tokenIndex
			{
				position1 := position
			l2:
				{
					position3, tokenIndex3 := position, tokenIndex
					if !_rules[ruleCall]() {
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
		/* 1 Call <- <(whitesp <IDENT> Action0 open allargs comma? close whitesp Action1)> */
		func() bool {
			position5, tokenIndex5 := position, tokenIndex
			{
				position6 := position
				if !_rules[rulewhitesp]() {
					goto l5
				}
				{
					position7 := position
					{
						position8 := position
						{
							position9, tokenIndex9 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l10
							}
							position++
							goto l9
						l10:
							position, tokenIndex = position9, tokenIndex9
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l5
							}
							position++
						}
					l9:
					l11:
						{
							position12, tokenIndex12 := position, tokenIndex
							{
								position13, tokenIndex13 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l14
								}
								position++
								goto l13
							l14:
								position, tokenIndex = position13, tokenIndex13
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l15
								}
								position++
								goto l13
							l15:
								position, tokenIndex = position13, tokenIndex13
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l16
								}
								position++
								goto l13
							l16:
								position, tokenIndex = position13, tokenIndex13
								if buffer[position] != rune('-') {
									goto l17
								}
								position++
								goto l13
							l17:
								position, tokenIndex = position13, tokenIndex13
								if buffer[position] != rune('_') {
									goto l18
								}
								position++
								goto l13
							l18:
								position, tokenIndex = position13, tokenIndex13
								if buffer[position] != rune('.') {
									goto l12
								}
								position++
							}
						l13:
							goto l11
						l12:
							position, tokenIndex = position12, tokenIndex12
						}
						add(ruleIDENT, position8)
					}
					add(rulePegText, position7)
				}
				{
					add(ruleAction0, position)
				}
				{
					position20 := position
					if buffer[position] != rune('(') {
						goto l5
					}
					position++
					if !_rules[rulesp]() {
						goto l5
					}
					add(ruleopen, position20)
				}
				{
					position21 := position
					{
						position22, tokenIndex22 := position, tokenIndex
						if !_rules[ruleCall]() {
							goto l23
						}
					l24:
						{
							position25, tokenIndex25 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l25
							}
							if !_rules[ruleCall]() {
								goto l25
							}
							goto l24
						l25:
							position, tokenIndex = position25, tokenIndex25
						}
						{
							position26, tokenIndex26 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l26
							}
							if !_rules[ruleargs]() {
								goto l26
							}
							goto l27
						l26:
							position, tokenIndex = position26, tokenIndex26
						}
					l27:
						goto l22
					l23:
						position, tokenIndex = position22, tokenIndex22
						{
							position29, tokenIndex29 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l29
							}
							goto l30
						l29:
							position, tokenIndex = position29, tokenIndex29
						}
					l30:
						if !_rules[ruleargs]() {
							goto l28
						}
						goto l22
					l28:
						position, tokenIndex = position22, tokenIndex22
						if !_rules[rulesp]() {
							goto l5
						}
					}
				l22:
					add(ruleallargs, position21)
				}
				{
					position31, tokenIndex31 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l31
					}
					goto l32
				l31:
					position, tokenIndex = position31, tokenIndex31
				}
			l32:
				if !_rules[ruleclose]() {
					goto l5
				}
				if !_rules[rulewhitesp]() {
					goto l5
				}
				{
					add(ruleAction1, position)
				}
				add(ruleCall, position6)
			}
			return true
		l5:
			position, tokenIndex = position5, tokenIndex5
			return false
		},
		/* 2 allargs <- <((Call (comma Call)* (comma args)?) / (comma? args) / sp)> */
		nil,
		/* 3 args <- <(arg (comma args)? sp)> */
		func() bool {
			position35, tokenIndex35 := position, tokenIndex
			{
				position36 := position
				{
					position37 := position
					{
						position38, tokenIndex38 := position, tokenIndex
						if !_rules[rulefield]() {
							goto l39
						}
						if !_rules[rulesp]() {
							goto l39
						}
						if buffer[position] != rune('=') {
							goto l39
						}
						position++
						if !_rules[rulesp]() {
							goto l39
						}
						if !_rules[rulevalue]() {
							goto l39
						}
						goto l38
					l39:
						position, tokenIndex = position38, tokenIndex38
						if !_rules[rulefield]() {
							goto l35
						}
						if !_rules[rulesp]() {
							goto l35
						}
						{
							position40 := position
							{
								position41, tokenIndex41 := position, tokenIndex
								if buffer[position] != rune('>') {
									goto l42
								}
								position++
								if buffer[position] != rune('<') {
									goto l42
								}
								position++
								{
									add(ruleAction2, position)
								}
								goto l41
							l42:
								position, tokenIndex = position41, tokenIndex41
								if buffer[position] != rune('<') {
									goto l44
								}
								position++
								if buffer[position] != rune('=') {
									goto l44
								}
								position++
								{
									add(ruleAction3, position)
								}
								goto l41
							l44:
								position, tokenIndex = position41, tokenIndex41
								if buffer[position] != rune('>') {
									goto l46
								}
								position++
								if buffer[position] != rune('=') {
									goto l46
								}
								position++
								{
									add(ruleAction4, position)
								}
								goto l41
							l46:
								position, tokenIndex = position41, tokenIndex41
								if buffer[position] != rune('=') {
									goto l48
								}
								position++
								if buffer[position] != rune('=') {
									goto l48
								}
								position++
								{
									add(ruleAction5, position)
								}
								goto l41
							l48:
								position, tokenIndex = position41, tokenIndex41
								if buffer[position] != rune('!') {
									goto l50
								}
								position++
								if buffer[position] != rune('=') {
									goto l50
								}
								position++
								{
									add(ruleAction6, position)
								}
								goto l41
							l50:
								position, tokenIndex = position41, tokenIndex41
								if buffer[position] != rune('<') {
									goto l52
								}
								position++
								{
									add(ruleAction7, position)
								}
								goto l41
							l52:
								position, tokenIndex = position41, tokenIndex41
								if buffer[position] != rune('>') {
									goto l35
								}
								position++
								{
									add(ruleAction8, position)
								}
							}
						l41:
							add(ruleCOND, position40)
						}
						if !_rules[rulesp]() {
							goto l35
						}
						if !_rules[rulevalue]() {
							goto l35
						}
					}
				l38:
					add(rulearg, position37)
				}
				{
					position55, tokenIndex55 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l55
					}
					if !_rules[ruleargs]() {
						goto l55
					}
					goto l56
				l55:
					position, tokenIndex = position55, tokenIndex55
				}
			l56:
				if !_rules[rulesp]() {
					goto l35
				}
				add(ruleargs, position36)
			}
			return true
		l35:
			position, tokenIndex = position35, tokenIndex35
			return false
		},
		/* 4 arg <- <((field sp '=' sp value) / (field sp COND sp value))> */
		nil,
		/* 5 COND <- <(('>' '<' Action2) / ('<' '=' Action3) / ('>' '=' Action4) / ('=' '=' Action5) / ('!' '=' Action6) / ('<' Action7) / ('>' Action8))> */
		nil,
		/* 6 open <- <('(' sp)> */
		nil,
		/* 7 value <- <(item / (lbrack Action9 list rbrack Action10))> */
		func() bool {
			position60, tokenIndex60 := position, tokenIndex
			{
				position61 := position
				{
					position62, tokenIndex62 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l63
					}
					goto l62
				l63:
					position, tokenIndex = position62, tokenIndex62
					{
						position64 := position
						if buffer[position] != rune('[') {
							goto l60
						}
						position++
						if !_rules[rulesp]() {
							goto l60
						}
						add(rulelbrack, position64)
					}
					{
						add(ruleAction9, position)
					}
					if !_rules[rulelist]() {
						goto l60
					}
					{
						position66 := position
						if !_rules[rulesp]() {
							goto l60
						}
						if buffer[position] != rune(']') {
							goto l60
						}
						position++
						if !_rules[rulesp]() {
							goto l60
						}
						add(rulerbrack, position66)
					}
					{
						add(ruleAction10, position)
					}
				}
			l62:
				add(rulevalue, position61)
			}
			return true
		l60:
			position, tokenIndex = position60, tokenIndex60
			return false
		},
		/* 8 list <- <(item (comma list)?)> */
		func() bool {
			position68, tokenIndex68 := position, tokenIndex
			{
				position69 := position
				if !_rules[ruleitem]() {
					goto l68
				}
				{
					position70, tokenIndex70 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l70
					}
					if !_rules[rulelist]() {
						goto l70
					}
					goto l71
				l70:
					position, tokenIndex = position70, tokenIndex70
				}
			l71:
				add(rulelist, position69)
			}
			return true
		l68:
			position, tokenIndex = position68, tokenIndex68
			return false
		},
		/* 9 item <- <(('n' 'u' 'l' 'l' &(comma / (sp close)) Action11) / ('t' 'r' 'u' 'e' &(comma / (sp close)) Action12) / ('f' 'a' 'l' 's' 'e' &(comma / (sp close)) Action13) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action14) / (<('-'? '.' [0-9]+)> Action15) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action16) / ('"' <doublequotedstring> '"' Action17) / ('\'' <singlequotedstring> '\'' Action18))> */
		func() bool {
			position72, tokenIndex72 := position, tokenIndex
			{
				position73 := position
				{
					position74, tokenIndex74 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l75
					}
					position++
					if buffer[position] != rune('u') {
						goto l75
					}
					position++
					if buffer[position] != rune('l') {
						goto l75
					}
					position++
					if buffer[position] != rune('l') {
						goto l75
					}
					position++
					{
						position76, tokenIndex76 := position, tokenIndex
						{
							position77, tokenIndex77 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l78
							}
							goto l77
						l78:
							position, tokenIndex = position77, tokenIndex77
							if !_rules[rulesp]() {
								goto l75
							}
							if !_rules[ruleclose]() {
								goto l75
							}
						}
					l77:
						position, tokenIndex = position76, tokenIndex76
					}
					{
						add(ruleAction11, position)
					}
					goto l74
				l75:
					position, tokenIndex = position74, tokenIndex74
					if buffer[position] != rune('t') {
						goto l80
					}
					position++
					if buffer[position] != rune('r') {
						goto l80
					}
					position++
					if buffer[position] != rune('u') {
						goto l80
					}
					position++
					if buffer[position] != rune('e') {
						goto l80
					}
					position++
					{
						position81, tokenIndex81 := position, tokenIndex
						{
							position82, tokenIndex82 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l83
							}
							goto l82
						l83:
							position, tokenIndex = position82, tokenIndex82
							if !_rules[rulesp]() {
								goto l80
							}
							if !_rules[ruleclose]() {
								goto l80
							}
						}
					l82:
						position, tokenIndex = position81, tokenIndex81
					}
					{
						add(ruleAction12, position)
					}
					goto l74
				l80:
					position, tokenIndex = position74, tokenIndex74
					if buffer[position] != rune('f') {
						goto l85
					}
					position++
					if buffer[position] != rune('a') {
						goto l85
					}
					position++
					if buffer[position] != rune('l') {
						goto l85
					}
					position++
					if buffer[position] != rune('s') {
						goto l85
					}
					position++
					if buffer[position] != rune('e') {
						goto l85
					}
					position++
					{
						position86, tokenIndex86 := position, tokenIndex
						{
							position87, tokenIndex87 := position, tokenIndex
							if !_rules[rulecomma]() {
								goto l88
							}
							goto l87
						l88:
							position, tokenIndex = position87, tokenIndex87
							if !_rules[rulesp]() {
								goto l85
							}
							if !_rules[ruleclose]() {
								goto l85
							}
						}
					l87:
						position, tokenIndex = position86, tokenIndex86
					}
					{
						add(ruleAction13, position)
					}
					goto l74
				l85:
					position, tokenIndex = position74, tokenIndex74
					{
						position91 := position
						{
							position92, tokenIndex92 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l92
							}
							position++
							goto l93
						l92:
							position, tokenIndex = position92, tokenIndex92
						}
					l93:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l90
						}
						position++
					l94:
						{
							position95, tokenIndex95 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l95
							}
							position++
							goto l94
						l95:
							position, tokenIndex = position95, tokenIndex95
						}
						{
							position96, tokenIndex96 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l96
							}
							position++
						l98:
							{
								position99, tokenIndex99 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l99
								}
								position++
								goto l98
							l99:
								position, tokenIndex = position99, tokenIndex99
							}
							goto l97
						l96:
							position, tokenIndex = position96, tokenIndex96
						}
					l97:
						add(rulePegText, position91)
					}
					{
						add(ruleAction14, position)
					}
					goto l74
				l90:
					position, tokenIndex = position74, tokenIndex74
					{
						position102 := position
						{
							position103, tokenIndex103 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l103
							}
							position++
							goto l104
						l103:
							position, tokenIndex = position103, tokenIndex103
						}
					l104:
						if buffer[position] != rune('.') {
							goto l101
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l101
						}
						position++
					l105:
						{
							position106, tokenIndex106 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l106
							}
							position++
							goto l105
						l106:
							position, tokenIndex = position106, tokenIndex106
						}
						add(rulePegText, position102)
					}
					{
						add(ruleAction15, position)
					}
					goto l74
				l101:
					position, tokenIndex = position74, tokenIndex74
					{
						position109 := position
						{
							position112, tokenIndex112 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l113
							}
							position++
							goto l112
						l113:
							position, tokenIndex = position112, tokenIndex112
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l114
							}
							position++
							goto l112
						l114:
							position, tokenIndex = position112, tokenIndex112
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l115
							}
							position++
							goto l112
						l115:
							position, tokenIndex = position112, tokenIndex112
							if buffer[position] != rune('-') {
								goto l116
							}
							position++
							goto l112
						l116:
							position, tokenIndex = position112, tokenIndex112
							if buffer[position] != rune('_') {
								goto l117
							}
							position++
							goto l112
						l117:
							position, tokenIndex = position112, tokenIndex112
							if buffer[position] != rune(':') {
								goto l108
							}
							position++
						}
					l112:
					l110:
						{
							position111, tokenIndex111 := position, tokenIndex
							{
								position118, tokenIndex118 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l119
								}
								position++
								goto l118
							l119:
								position, tokenIndex = position118, tokenIndex118
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l120
								}
								position++
								goto l118
							l120:
								position, tokenIndex = position118, tokenIndex118
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l121
								}
								position++
								goto l118
							l121:
								position, tokenIndex = position118, tokenIndex118
								if buffer[position] != rune('-') {
									goto l122
								}
								position++
								goto l118
							l122:
								position, tokenIndex = position118, tokenIndex118
								if buffer[position] != rune('_') {
									goto l123
								}
								position++
								goto l118
							l123:
								position, tokenIndex = position118, tokenIndex118
								if buffer[position] != rune(':') {
									goto l111
								}
								position++
							}
						l118:
							goto l110
						l111:
							position, tokenIndex = position111, tokenIndex111
						}
						add(rulePegText, position109)
					}
					{
						add(ruleAction16, position)
					}
					goto l74
				l108:
					position, tokenIndex = position74, tokenIndex74
					if buffer[position] != rune('"') {
						goto l125
					}
					position++
					{
						position126 := position
						{
							position127 := position
						l128:
							{
								position129, tokenIndex129 := position, tokenIndex
								{
									position130, tokenIndex130 := position, tokenIndex
									{
										position132, tokenIndex132 := position, tokenIndex
										{
											position133, tokenIndex133 := position, tokenIndex
											if buffer[position] != rune('"') {
												goto l134
											}
											position++
											goto l133
										l134:
											position, tokenIndex = position133, tokenIndex133
											if buffer[position] != rune('\\') {
												goto l135
											}
											position++
											goto l133
										l135:
											position, tokenIndex = position133, tokenIndex133
											if buffer[position] != rune('\n') {
												goto l132
											}
											position++
										}
									l133:
										goto l131
									l132:
										position, tokenIndex = position132, tokenIndex132
									}
									if !matchDot() {
										goto l131
									}
									goto l130
								l131:
									position, tokenIndex = position130, tokenIndex130
									if buffer[position] != rune('\\') {
										goto l136
									}
									position++
									if buffer[position] != rune('n') {
										goto l136
									}
									position++
									goto l130
								l136:
									position, tokenIndex = position130, tokenIndex130
									if buffer[position] != rune('\\') {
										goto l137
									}
									position++
									if buffer[position] != rune('"') {
										goto l137
									}
									position++
									goto l130
								l137:
									position, tokenIndex = position130, tokenIndex130
									if buffer[position] != rune('\\') {
										goto l138
									}
									position++
									if buffer[position] != rune('\'') {
										goto l138
									}
									position++
									goto l130
								l138:
									position, tokenIndex = position130, tokenIndex130
									if buffer[position] != rune('\\') {
										goto l129
									}
									position++
									if buffer[position] != rune('\\') {
										goto l129
									}
									position++
								}
							l130:
								goto l128
							l129:
								position, tokenIndex = position129, tokenIndex129
							}
							add(ruledoublequotedstring, position127)
						}
						add(rulePegText, position126)
					}
					if buffer[position] != rune('"') {
						goto l125
					}
					position++
					{
						add(ruleAction17, position)
					}
					goto l74
				l125:
					position, tokenIndex = position74, tokenIndex74
					if buffer[position] != rune('\'') {
						goto l72
					}
					position++
					{
						position140 := position
						{
							position141 := position
						l142:
							{
								position143, tokenIndex143 := position, tokenIndex
								{
									position144, tokenIndex144 := position, tokenIndex
									{
										position146, tokenIndex146 := position, tokenIndex
										{
											position147, tokenIndex147 := position, tokenIndex
											if buffer[position] != rune('\'') {
												goto l148
											}
											position++
											goto l147
										l148:
											position, tokenIndex = position147, tokenIndex147
											if buffer[position] != rune('\\') {
												goto l149
											}
											position++
											goto l147
										l149:
											position, tokenIndex = position147, tokenIndex147
											if buffer[position] != rune('\n') {
												goto l146
											}
											position++
										}
									l147:
										goto l145
									l146:
										position, tokenIndex = position146, tokenIndex146
									}
									if !matchDot() {
										goto l145
									}
									goto l144
								l145:
									position, tokenIndex = position144, tokenIndex144
									if buffer[position] != rune('\\') {
										goto l150
									}
									position++
									if buffer[position] != rune('n') {
										goto l150
									}
									position++
									goto l144
								l150:
									position, tokenIndex = position144, tokenIndex144
									if buffer[position] != rune('\\') {
										goto l151
									}
									position++
									if buffer[position] != rune('"') {
										goto l151
									}
									position++
									goto l144
								l151:
									position, tokenIndex = position144, tokenIndex144
									if buffer[position] != rune('\\') {
										goto l152
									}
									position++
									if buffer[position] != rune('\'') {
										goto l152
									}
									position++
									goto l144
								l152:
									position, tokenIndex = position144, tokenIndex144
									if buffer[position] != rune('\\') {
										goto l143
									}
									position++
									if buffer[position] != rune('\\') {
										goto l143
									}
									position++
								}
							l144:
								goto l142
							l143:
								position, tokenIndex = position143, tokenIndex143
							}
							add(rulesinglequotedstring, position141)
						}
						add(rulePegText, position140)
					}
					if buffer[position] != rune('\'') {
						goto l72
					}
					position++
					{
						add(ruleAction18, position)
					}
				}
			l74:
				add(ruleitem, position73)
			}
			return true
		l72:
			position, tokenIndex = position72, tokenIndex72
			return false
		},
		/* 10 doublequotedstring <- <((!('"' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		nil,
		/* 11 singlequotedstring <- <((!('\'' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		nil,
		/* 12 field <- <(<(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_')*)> Action19)> */
		func() bool {
			position156, tokenIndex156 := position, tokenIndex
			{
				position157 := position
				{
					position158 := position
					{
						position159, tokenIndex159 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l160
						}
						position++
						goto l159
					l160:
						position, tokenIndex = position159, tokenIndex159
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l156
						}
						position++
					}
				l159:
				l161:
					{
						position162, tokenIndex162 := position, tokenIndex
						{
							position163, tokenIndex163 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l164
							}
							position++
							goto l163
						l164:
							position, tokenIndex = position163, tokenIndex163
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l165
							}
							position++
							goto l163
						l165:
							position, tokenIndex = position163, tokenIndex163
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l166
							}
							position++
							goto l163
						l166:
							position, tokenIndex = position163, tokenIndex163
							if buffer[position] != rune('_') {
								goto l162
							}
							position++
						}
					l163:
						goto l161
					l162:
						position, tokenIndex = position162, tokenIndex162
					}
					add(rulePegText, position158)
				}
				{
					add(ruleAction19, position)
				}
				add(rulefield, position157)
			}
			return true
		l156:
			position, tokenIndex = position156, tokenIndex156
			return false
		},
		/* 13 close <- <(')' sp)> */
		func() bool {
			position168, tokenIndex168 := position, tokenIndex
			{
				position169 := position
				if buffer[position] != rune(')') {
					goto l168
				}
				position++
				if !_rules[rulesp]() {
					goto l168
				}
				add(ruleclose, position169)
			}
			return true
		l168:
			position, tokenIndex = position168, tokenIndex168
			return false
		},
		/* 14 sp <- <(' ' / '\t')*> */
		func() bool {
			{
				position171 := position
			l172:
				{
					position173, tokenIndex173 := position, tokenIndex
					{
						position174, tokenIndex174 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l175
						}
						position++
						goto l174
					l175:
						position, tokenIndex = position174, tokenIndex174
						if buffer[position] != rune('\t') {
							goto l173
						}
						position++
					}
				l174:
					goto l172
				l173:
					position, tokenIndex = position173, tokenIndex173
				}
				add(rulesp, position171)
			}
			return true
		},
		/* 15 comma <- <(sp ',' sp)> */
		func() bool {
			position176, tokenIndex176 := position, tokenIndex
			{
				position177 := position
				if !_rules[rulesp]() {
					goto l176
				}
				if buffer[position] != rune(',') {
					goto l176
				}
				position++
				if !_rules[rulesp]() {
					goto l176
				}
				add(rulecomma, position177)
			}
			return true
		l176:
			position, tokenIndex = position176, tokenIndex176
			return false
		},
		/* 16 lbrack <- <('[' sp)> */
		nil,
		/* 17 rbrack <- <(sp ']' sp)> */
		nil,
		/* 18 whitesp <- <(' ' / '\t' / '\n')*> */
		func() bool {
			{
				position181 := position
			l182:
				{
					position183, tokenIndex183 := position, tokenIndex
					{
						position184, tokenIndex184 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l185
						}
						position++
						goto l184
					l185:
						position, tokenIndex = position184, tokenIndex184
						if buffer[position] != rune('\t') {
							goto l186
						}
						position++
						goto l184
					l186:
						position, tokenIndex = position184, tokenIndex184
						if buffer[position] != rune('\n') {
							goto l183
						}
						position++
					}
				l184:
					goto l182
				l183:
					position, tokenIndex = position183, tokenIndex183
				}
				add(rulewhitesp, position181)
			}
			return true
		},
		/* 19 IDENT <- <(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '-' / '_' / '.')*)> */
		nil,
		nil,
		/* 22 Action0 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 23 Action1 <- <{ p.endCall() }> */
		nil,
		/* 24 Action2 <- <{ p.addBTWN() }> */
		nil,
		/* 25 Action3 <- <{ p.addLTE() }> */
		nil,
		/* 26 Action4 <- <{ p.addGTE() }> */
		nil,
		/* 27 Action5 <- <{ p.addEQ() }> */
		nil,
		/* 28 Action6 <- <{ p.addNEQ() }> */
		nil,
		/* 29 Action7 <- <{ p.addLT() }> */
		nil,
		/* 30 Action8 <- <{ p.addGT() }> */
		nil,
		/* 31 Action9 <- <{ p.startList() }> */
		nil,
		/* 32 Action10 <- <{ p.endList() }> */
		nil,
		/* 33 Action11 <- <{ p.addVal(nil) }> */
		nil,
		/* 34 Action12 <- <{ p.addVal(true) }> */
		nil,
		/* 35 Action13 <- <{ p.addVal(false) }> */
		nil,
		/* 36 Action14 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 37 Action15 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 38 Action16 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 39 Action17 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 40 Action18 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 41 Action19 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
	}
	p.rules = _rules
}
