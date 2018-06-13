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
	rulenewline
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
	"newline",
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
	rules  [40]func() bool
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
		/* 1 Call <- <(newline* <([a-z] / [A-Z])+> Action0 open args close newline* Action1)> */
		func() bool {
			position5, tokenIndex5 := position, tokenIndex
			{
				position6 := position
			l7:
				{
					position8, tokenIndex8 := position, tokenIndex
					if !_rules[rulenewline]() {
						goto l8
					}
					goto l7
				l8:
					position, tokenIndex = position8, tokenIndex8
				}
				{
					position9 := position
					{
						position12, tokenIndex12 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l13
						}
						position++
						goto l12
					l13:
						position, tokenIndex = position12, tokenIndex12
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l5
						}
						position++
					}
				l12:
				l10:
					{
						position11, tokenIndex11 := position, tokenIndex
						{
							position14, tokenIndex14 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l15
							}
							position++
							goto l14
						l15:
							position, tokenIndex = position14, tokenIndex14
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l11
							}
							position++
						}
					l14:
						goto l10
					l11:
						position, tokenIndex = position11, tokenIndex11
					}
					add(rulePegText, position9)
				}
				{
					add(ruleAction0, position)
				}
				{
					position17 := position
					if buffer[position] != rune('(') {
						goto l5
					}
					position++
					if !_rules[rulesp]() {
						goto l5
					}
					add(ruleopen, position17)
				}
				if !_rules[ruleargs]() {
					goto l5
				}
				{
					position18 := position
					if buffer[position] != rune(')') {
						goto l5
					}
					position++
					if !_rules[rulesp]() {
						goto l5
					}
					add(ruleclose, position18)
				}
			l19:
				{
					position20, tokenIndex20 := position, tokenIndex
					if !_rules[rulenewline]() {
						goto l20
					}
					goto l19
				l20:
					position, tokenIndex = position20, tokenIndex20
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
		/* 2 args <- <((arg (comma args)? sp) / sp)> */
		func() bool {
			position22, tokenIndex22 := position, tokenIndex
			{
				position23 := position
				{
					position24, tokenIndex24 := position, tokenIndex
					{
						position26 := position
						{
							position27, tokenIndex27 := position, tokenIndex
							if !_rules[ruleCall]() {
								goto l28
							}
							goto l27
						l28:
							position, tokenIndex = position27, tokenIndex27
							if !_rules[rulefield]() {
								goto l29
							}
							if !_rules[rulesp]() {
								goto l29
							}
							if buffer[position] != rune('=') {
								goto l29
							}
							position++
							if !_rules[rulesp]() {
								goto l29
							}
							if !_rules[rulevalue]() {
								goto l29
							}
							goto l27
						l29:
							position, tokenIndex = position27, tokenIndex27
							if !_rules[rulefield]() {
								goto l25
							}
							if !_rules[rulesp]() {
								goto l25
							}
							{
								position30 := position
								{
									position31, tokenIndex31 := position, tokenIndex
									if buffer[position] != rune('>') {
										goto l32
									}
									position++
									if buffer[position] != rune('<') {
										goto l32
									}
									position++
									{
										add(ruleAction2, position)
									}
									goto l31
								l32:
									position, tokenIndex = position31, tokenIndex31
									if buffer[position] != rune('<') {
										goto l34
									}
									position++
									if buffer[position] != rune('=') {
										goto l34
									}
									position++
									{
										add(ruleAction3, position)
									}
									goto l31
								l34:
									position, tokenIndex = position31, tokenIndex31
									if buffer[position] != rune('>') {
										goto l36
									}
									position++
									if buffer[position] != rune('=') {
										goto l36
									}
									position++
									{
										add(ruleAction4, position)
									}
									goto l31
								l36:
									position, tokenIndex = position31, tokenIndex31
									if buffer[position] != rune('=') {
										goto l38
									}
									position++
									if buffer[position] != rune('=') {
										goto l38
									}
									position++
									{
										add(ruleAction5, position)
									}
									goto l31
								l38:
									position, tokenIndex = position31, tokenIndex31
									if buffer[position] != rune('!') {
										goto l40
									}
									position++
									if buffer[position] != rune('=') {
										goto l40
									}
									position++
									{
										add(ruleAction6, position)
									}
									goto l31
								l40:
									position, tokenIndex = position31, tokenIndex31
									if buffer[position] != rune('<') {
										goto l42
									}
									position++
									{
										add(ruleAction7, position)
									}
									goto l31
								l42:
									position, tokenIndex = position31, tokenIndex31
									if buffer[position] != rune('>') {
										goto l25
									}
									position++
									{
										add(ruleAction8, position)
									}
								}
							l31:
								add(ruleCOND, position30)
							}
							if !_rules[rulesp]() {
								goto l25
							}
							if !_rules[rulevalue]() {
								goto l25
							}
						}
					l27:
						add(rulearg, position26)
					}
					{
						position45, tokenIndex45 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l45
						}
						if !_rules[ruleargs]() {
							goto l45
						}
						goto l46
					l45:
						position, tokenIndex = position45, tokenIndex45
					}
				l46:
					if !_rules[rulesp]() {
						goto l25
					}
					goto l24
				l25:
					position, tokenIndex = position24, tokenIndex24
					if !_rules[rulesp]() {
						goto l22
					}
				}
			l24:
				add(ruleargs, position23)
			}
			return true
		l22:
			position, tokenIndex = position22, tokenIndex22
			return false
		},
		/* 3 arg <- <(Call / (field sp '=' sp value) / (field sp COND sp value))> */
		nil,
		/* 4 COND <- <(('>' '<' Action2) / ('<' '=' Action3) / ('>' '=' Action4) / ('=' '=' Action5) / ('!' '=' Action6) / ('<' Action7) / ('>' Action8))> */
		nil,
		/* 5 open <- <('(' sp)> */
		nil,
		/* 6 value <- <(item / (lbrack Action9 list rbrack Action10))> */
		func() bool {
			position50, tokenIndex50 := position, tokenIndex
			{
				position51 := position
				{
					position52, tokenIndex52 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l53
					}
					goto l52
				l53:
					position, tokenIndex = position52, tokenIndex52
					{
						position54 := position
						if buffer[position] != rune('[') {
							goto l50
						}
						position++
						if !_rules[rulesp]() {
							goto l50
						}
						add(rulelbrack, position54)
					}
					{
						add(ruleAction9, position)
					}
					if !_rules[rulelist]() {
						goto l50
					}
					{
						position56 := position
						if !_rules[rulesp]() {
							goto l50
						}
						if buffer[position] != rune(']') {
							goto l50
						}
						position++
						if !_rules[rulesp]() {
							goto l50
						}
						add(rulerbrack, position56)
					}
					{
						add(ruleAction10, position)
					}
				}
			l52:
				add(rulevalue, position51)
			}
			return true
		l50:
			position, tokenIndex = position50, tokenIndex50
			return false
		},
		/* 7 list <- <(item (comma list)?)> */
		func() bool {
			position58, tokenIndex58 := position, tokenIndex
			{
				position59 := position
				if !_rules[ruleitem]() {
					goto l58
				}
				{
					position60, tokenIndex60 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l60
					}
					if !_rules[rulelist]() {
						goto l60
					}
					goto l61
				l60:
					position, tokenIndex = position60, tokenIndex60
				}
			l61:
				add(rulelist, position59)
			}
			return true
		l58:
			position, tokenIndex = position58, tokenIndex58
			return false
		},
		/* 8 item <- <(('n' 'u' 'l' 'l' Action11) / ('t' 'r' 'u' 'e' Action12) / ('f' 'a' 'l' 's' 'e' Action13) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action14) / (<('-'? '.' [0-9]+)> Action15) / (<([a-z] / [A-Z] / [0-9] / '-' / '_' / ':')+> Action16) / ('"' <doublequotedstring> '"' Action17) / ('\'' <singlequotedstring> '\'' Action18))> */
		func() bool {
			position62, tokenIndex62 := position, tokenIndex
			{
				position63 := position
				{
					position64, tokenIndex64 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l65
					}
					position++
					if buffer[position] != rune('u') {
						goto l65
					}
					position++
					if buffer[position] != rune('l') {
						goto l65
					}
					position++
					if buffer[position] != rune('l') {
						goto l65
					}
					position++
					{
						add(ruleAction11, position)
					}
					goto l64
				l65:
					position, tokenIndex = position64, tokenIndex64
					if buffer[position] != rune('t') {
						goto l67
					}
					position++
					if buffer[position] != rune('r') {
						goto l67
					}
					position++
					if buffer[position] != rune('u') {
						goto l67
					}
					position++
					if buffer[position] != rune('e') {
						goto l67
					}
					position++
					{
						add(ruleAction12, position)
					}
					goto l64
				l67:
					position, tokenIndex = position64, tokenIndex64
					if buffer[position] != rune('f') {
						goto l69
					}
					position++
					if buffer[position] != rune('a') {
						goto l69
					}
					position++
					if buffer[position] != rune('l') {
						goto l69
					}
					position++
					if buffer[position] != rune('s') {
						goto l69
					}
					position++
					if buffer[position] != rune('e') {
						goto l69
					}
					position++
					{
						add(ruleAction13, position)
					}
					goto l64
				l69:
					position, tokenIndex = position64, tokenIndex64
					{
						position72 := position
						{
							position73, tokenIndex73 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l73
							}
							position++
							goto l74
						l73:
							position, tokenIndex = position73, tokenIndex73
						}
					l74:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l71
						}
						position++
					l75:
						{
							position76, tokenIndex76 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l76
							}
							position++
							goto l75
						l76:
							position, tokenIndex = position76, tokenIndex76
						}
						{
							position77, tokenIndex77 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l77
							}
							position++
						l79:
							{
								position80, tokenIndex80 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l80
								}
								position++
								goto l79
							l80:
								position, tokenIndex = position80, tokenIndex80
							}
							goto l78
						l77:
							position, tokenIndex = position77, tokenIndex77
						}
					l78:
						add(rulePegText, position72)
					}
					{
						add(ruleAction14, position)
					}
					goto l64
				l71:
					position, tokenIndex = position64, tokenIndex64
					{
						position83 := position
						{
							position84, tokenIndex84 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l84
							}
							position++
							goto l85
						l84:
							position, tokenIndex = position84, tokenIndex84
						}
					l85:
						if buffer[position] != rune('.') {
							goto l82
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l82
						}
						position++
					l86:
						{
							position87, tokenIndex87 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l87
							}
							position++
							goto l86
						l87:
							position, tokenIndex = position87, tokenIndex87
						}
						add(rulePegText, position83)
					}
					{
						add(ruleAction15, position)
					}
					goto l64
				l82:
					position, tokenIndex = position64, tokenIndex64
					{
						position90 := position
						{
							position93, tokenIndex93 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l94
							}
							position++
							goto l93
						l94:
							position, tokenIndex = position93, tokenIndex93
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l95
							}
							position++
							goto l93
						l95:
							position, tokenIndex = position93, tokenIndex93
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l96
							}
							position++
							goto l93
						l96:
							position, tokenIndex = position93, tokenIndex93
							if buffer[position] != rune('-') {
								goto l97
							}
							position++
							goto l93
						l97:
							position, tokenIndex = position93, tokenIndex93
							if buffer[position] != rune('_') {
								goto l98
							}
							position++
							goto l93
						l98:
							position, tokenIndex = position93, tokenIndex93
							if buffer[position] != rune(':') {
								goto l89
							}
							position++
						}
					l93:
					l91:
						{
							position92, tokenIndex92 := position, tokenIndex
							{
								position99, tokenIndex99 := position, tokenIndex
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l100
								}
								position++
								goto l99
							l100:
								position, tokenIndex = position99, tokenIndex99
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l101
								}
								position++
								goto l99
							l101:
								position, tokenIndex = position99, tokenIndex99
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l102
								}
								position++
								goto l99
							l102:
								position, tokenIndex = position99, tokenIndex99
								if buffer[position] != rune('-') {
									goto l103
								}
								position++
								goto l99
							l103:
								position, tokenIndex = position99, tokenIndex99
								if buffer[position] != rune('_') {
									goto l104
								}
								position++
								goto l99
							l104:
								position, tokenIndex = position99, tokenIndex99
								if buffer[position] != rune(':') {
									goto l92
								}
								position++
							}
						l99:
							goto l91
						l92:
							position, tokenIndex = position92, tokenIndex92
						}
						add(rulePegText, position90)
					}
					{
						add(ruleAction16, position)
					}
					goto l64
				l89:
					position, tokenIndex = position64, tokenIndex64
					if buffer[position] != rune('"') {
						goto l106
					}
					position++
					{
						position107 := position
						{
							position108 := position
						l109:
							{
								position110, tokenIndex110 := position, tokenIndex
								{
									position111, tokenIndex111 := position, tokenIndex
									{
										position113, tokenIndex113 := position, tokenIndex
										{
											position114, tokenIndex114 := position, tokenIndex
											if buffer[position] != rune('"') {
												goto l115
											}
											position++
											goto l114
										l115:
											position, tokenIndex = position114, tokenIndex114
											if buffer[position] != rune('\\') {
												goto l116
											}
											position++
											goto l114
										l116:
											position, tokenIndex = position114, tokenIndex114
											if buffer[position] != rune('\n') {
												goto l113
											}
											position++
										}
									l114:
										goto l112
									l113:
										position, tokenIndex = position113, tokenIndex113
									}
									if !matchDot() {
										goto l112
									}
									goto l111
								l112:
									position, tokenIndex = position111, tokenIndex111
									if buffer[position] != rune('\\') {
										goto l117
									}
									position++
									if buffer[position] != rune('n') {
										goto l117
									}
									position++
									goto l111
								l117:
									position, tokenIndex = position111, tokenIndex111
									if buffer[position] != rune('\\') {
										goto l118
									}
									position++
									if buffer[position] != rune('"') {
										goto l118
									}
									position++
									goto l111
								l118:
									position, tokenIndex = position111, tokenIndex111
									if buffer[position] != rune('\\') {
										goto l119
									}
									position++
									if buffer[position] != rune('\'') {
										goto l119
									}
									position++
									goto l111
								l119:
									position, tokenIndex = position111, tokenIndex111
									if buffer[position] != rune('\\') {
										goto l110
									}
									position++
									if buffer[position] != rune('\\') {
										goto l110
									}
									position++
								}
							l111:
								goto l109
							l110:
								position, tokenIndex = position110, tokenIndex110
							}
							add(ruledoublequotedstring, position108)
						}
						add(rulePegText, position107)
					}
					if buffer[position] != rune('"') {
						goto l106
					}
					position++
					{
						add(ruleAction17, position)
					}
					goto l64
				l106:
					position, tokenIndex = position64, tokenIndex64
					if buffer[position] != rune('\'') {
						goto l62
					}
					position++
					{
						position121 := position
						{
							position122 := position
						l123:
							{
								position124, tokenIndex124 := position, tokenIndex
								{
									position125, tokenIndex125 := position, tokenIndex
									{
										position127, tokenIndex127 := position, tokenIndex
										{
											position128, tokenIndex128 := position, tokenIndex
											if buffer[position] != rune('\'') {
												goto l129
											}
											position++
											goto l128
										l129:
											position, tokenIndex = position128, tokenIndex128
											if buffer[position] != rune('\\') {
												goto l130
											}
											position++
											goto l128
										l130:
											position, tokenIndex = position128, tokenIndex128
											if buffer[position] != rune('\n') {
												goto l127
											}
											position++
										}
									l128:
										goto l126
									l127:
										position, tokenIndex = position127, tokenIndex127
									}
									if !matchDot() {
										goto l126
									}
									goto l125
								l126:
									position, tokenIndex = position125, tokenIndex125
									if buffer[position] != rune('\\') {
										goto l131
									}
									position++
									if buffer[position] != rune('n') {
										goto l131
									}
									position++
									goto l125
								l131:
									position, tokenIndex = position125, tokenIndex125
									if buffer[position] != rune('\\') {
										goto l132
									}
									position++
									if buffer[position] != rune('"') {
										goto l132
									}
									position++
									goto l125
								l132:
									position, tokenIndex = position125, tokenIndex125
									if buffer[position] != rune('\\') {
										goto l133
									}
									position++
									if buffer[position] != rune('\'') {
										goto l133
									}
									position++
									goto l125
								l133:
									position, tokenIndex = position125, tokenIndex125
									if buffer[position] != rune('\\') {
										goto l124
									}
									position++
									if buffer[position] != rune('\\') {
										goto l124
									}
									position++
								}
							l125:
								goto l123
							l124:
								position, tokenIndex = position124, tokenIndex124
							}
							add(rulesinglequotedstring, position122)
						}
						add(rulePegText, position121)
					}
					if buffer[position] != rune('\'') {
						goto l62
					}
					position++
					{
						add(ruleAction18, position)
					}
				}
			l64:
				add(ruleitem, position63)
			}
			return true
		l62:
			position, tokenIndex = position62, tokenIndex62
			return false
		},
		/* 9 doublequotedstring <- <((!('"' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		nil,
		/* 10 singlequotedstring <- <((!('\'' / '\\' / '\n') .) / ('\\' 'n') / ('\\' '"') / ('\\' '\'') / ('\\' '\\'))*> */
		nil,
		/* 11 field <- <(<(([a-z] / [A-Z]) ([a-z] / [A-Z] / [0-9] / '_')*)> Action19)> */
		func() bool {
			position137, tokenIndex137 := position, tokenIndex
			{
				position138 := position
				{
					position139 := position
					{
						position140, tokenIndex140 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l141
						}
						position++
						goto l140
					l141:
						position, tokenIndex = position140, tokenIndex140
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l137
						}
						position++
					}
				l140:
				l142:
					{
						position143, tokenIndex143 := position, tokenIndex
						{
							position144, tokenIndex144 := position, tokenIndex
							if c := buffer[position]; c < rune('a') || c > rune('z') {
								goto l145
							}
							position++
							goto l144
						l145:
							position, tokenIndex = position144, tokenIndex144
							if c := buffer[position]; c < rune('A') || c > rune('Z') {
								goto l146
							}
							position++
							goto l144
						l146:
							position, tokenIndex = position144, tokenIndex144
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l147
							}
							position++
							goto l144
						l147:
							position, tokenIndex = position144, tokenIndex144
							if buffer[position] != rune('_') {
								goto l143
							}
							position++
						}
					l144:
						goto l142
					l143:
						position, tokenIndex = position143, tokenIndex143
					}
					add(rulePegText, position139)
				}
				{
					add(ruleAction19, position)
				}
				add(rulefield, position138)
			}
			return true
		l137:
			position, tokenIndex = position137, tokenIndex137
			return false
		},
		/* 12 close <- <(')' sp)> */
		nil,
		/* 13 sp <- <(' ' / '\t')*> */
		func() bool {
			{
				position151 := position
			l152:
				{
					position153, tokenIndex153 := position, tokenIndex
					{
						position154, tokenIndex154 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l155
						}
						position++
						goto l154
					l155:
						position, tokenIndex = position154, tokenIndex154
						if buffer[position] != rune('\t') {
							goto l153
						}
						position++
					}
				l154:
					goto l152
				l153:
					position, tokenIndex = position153, tokenIndex153
				}
				add(rulesp, position151)
			}
			return true
		},
		/* 14 comma <- <(sp ',' sp)> */
		func() bool {
			position156, tokenIndex156 := position, tokenIndex
			{
				position157 := position
				if !_rules[rulesp]() {
					goto l156
				}
				if buffer[position] != rune(',') {
					goto l156
				}
				position++
				if !_rules[rulesp]() {
					goto l156
				}
				add(rulecomma, position157)
			}
			return true
		l156:
			position, tokenIndex = position156, tokenIndex156
			return false
		},
		/* 15 lbrack <- <('[' sp)> */
		nil,
		/* 16 rbrack <- <(sp ']' sp)> */
		nil,
		/* 17 newline <- <(sp '\n' sp)> */
		func() bool {
			position160, tokenIndex160 := position, tokenIndex
			{
				position161 := position
				if !_rules[rulesp]() {
					goto l160
				}
				if buffer[position] != rune('\n') {
					goto l160
				}
				position++
				if !_rules[rulesp]() {
					goto l160
				}
				add(rulenewline, position161)
			}
			return true
		l160:
			position, tokenIndex = position160, tokenIndex160
			return false
		},
		nil,
		/* 20 Action0 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 21 Action1 <- <{ p.endCall() }> */
		nil,
		/* 22 Action2 <- <{ p.addBTWN() }> */
		nil,
		/* 23 Action3 <- <{ p.addLTE() }> */
		nil,
		/* 24 Action4 <- <{ p.addGTE() }> */
		nil,
		/* 25 Action5 <- <{ p.addEQ() }> */
		nil,
		/* 26 Action6 <- <{ p.addNEQ() }> */
		nil,
		/* 27 Action7 <- <{ p.addLT() }> */
		nil,
		/* 28 Action8 <- <{ p.addGT() }> */
		nil,
		/* 29 Action9 <- <{ p.startList() }> */
		nil,
		/* 30 Action10 <- <{ p.endList() }> */
		nil,
		/* 31 Action11 <- <{ p.addVal(nil) }> */
		nil,
		/* 32 Action12 <- <{ p.addVal(true) }> */
		nil,
		/* 33 Action13 <- <{ p.addVal(false) }> */
		nil,
		/* 34 Action14 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 35 Action15 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 36 Action16 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 37 Action17 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 38 Action18 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 39 Action19 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
	}
	p.rules = _rules
}
