package pql

//go:generate peg -inline -switch pql.peg

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
	rules  [38]func() bool
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
									{
										switch buffer[position] {
										case '>':
											if buffer[position] != rune('>') {
												goto l25
											}
											position++
											{
												add(ruleAction8, position)
											}
											break
										case '<':
											if buffer[position] != rune('<') {
												goto l25
											}
											position++
											{
												add(ruleAction7, position)
											}
											break
										case '!':
											if buffer[position] != rune('!') {
												goto l25
											}
											position++
											if buffer[position] != rune('=') {
												goto l25
											}
											position++
											{
												add(ruleAction6, position)
											}
											break
										default:
											if buffer[position] != rune('=') {
												goto l25
											}
											position++
											if buffer[position] != rune('=') {
												goto l25
											}
											position++
											{
												add(ruleAction5, position)
											}
											break
										}
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
						position43, tokenIndex43 := position, tokenIndex
						if !_rules[rulecomma]() {
							goto l43
						}
						if !_rules[ruleargs]() {
							goto l43
						}
						goto l44
					l43:
						position, tokenIndex = position43, tokenIndex43
					}
				l44:
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
		/* 4 COND <- <(('>' '<' Action2) / ('<' '=' Action3) / ('>' '=' Action4) / ((&('>') ('>' Action8)) | (&('<') ('<' Action7)) | (&('!') ('!' '=' Action6)) | (&('=') ('=' '=' Action5))))> */
		nil,
		/* 5 open <- <('(' sp)> */
		nil,
		/* 6 value <- <(item / (lbrack Action9 list rbrack Action10))> */
		func() bool {
			position48, tokenIndex48 := position, tokenIndex
			{
				position49 := position
				{
					position50, tokenIndex50 := position, tokenIndex
					if !_rules[ruleitem]() {
						goto l51
					}
					goto l50
				l51:
					position, tokenIndex = position50, tokenIndex50
					{
						position52 := position
						if buffer[position] != rune('[') {
							goto l48
						}
						position++
						if !_rules[rulesp]() {
							goto l48
						}
						add(rulelbrack, position52)
					}
					{
						add(ruleAction9, position)
					}
					if !_rules[rulelist]() {
						goto l48
					}
					{
						position54 := position
						if !_rules[rulesp]() {
							goto l48
						}
						if buffer[position] != rune(']') {
							goto l48
						}
						position++
						if !_rules[rulesp]() {
							goto l48
						}
						add(rulerbrack, position54)
					}
					{
						add(ruleAction10, position)
					}
				}
			l50:
				add(rulevalue, position49)
			}
			return true
		l48:
			position, tokenIndex = position48, tokenIndex48
			return false
		},
		/* 7 list <- <(item (comma list)?)> */
		func() bool {
			position56, tokenIndex56 := position, tokenIndex
			{
				position57 := position
				if !_rules[ruleitem]() {
					goto l56
				}
				{
					position58, tokenIndex58 := position, tokenIndex
					if !_rules[rulecomma]() {
						goto l58
					}
					if !_rules[rulelist]() {
						goto l58
					}
					goto l59
				l58:
					position, tokenIndex = position58, tokenIndex58
				}
			l59:
				add(rulelist, position57)
			}
			return true
		l56:
			position, tokenIndex = position56, tokenIndex56
			return false
		},
		/* 8 item <- <(('n' 'u' 'l' 'l' Action11) / ('t' 'r' 'u' 'e' Action12) / ('f' 'a' 'l' 's' 'e' Action13) / (<('-'? [0-9]+ ('.' [0-9]*)?)> Action14) / (<('-'? '.' [0-9]+)> Action15) / ((&('\'') ('\'' <((&(':') ':') | (&('_') '_') | (&('-') '-') | (&('0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9') [0-9]) | (&('A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z') [A-Z]) | (&('a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z') [a-z]))+> '\'' Action18)) | (&('"') ('"' <((&(':') ':') | (&('_') '_') | (&('-') '-') | (&('0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9') [0-9]) | (&('A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z') [A-Z]) | (&('a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z') [a-z]))+> '"' Action17)) | (&('-' | '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' | ':' | 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | '_' | 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z') (<((&(':') ':') | (&('_') '_') | (&('-') '-') | (&('0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9') [0-9]) | (&('A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z') [A-Z]) | (&('a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z') [a-z]))+> Action16))))> */
		func() bool {
			position60, tokenIndex60 := position, tokenIndex
			{
				position61 := position
				{
					position62, tokenIndex62 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l63
					}
					position++
					if buffer[position] != rune('u') {
						goto l63
					}
					position++
					if buffer[position] != rune('l') {
						goto l63
					}
					position++
					if buffer[position] != rune('l') {
						goto l63
					}
					position++
					{
						add(ruleAction11, position)
					}
					goto l62
				l63:
					position, tokenIndex = position62, tokenIndex62
					if buffer[position] != rune('t') {
						goto l65
					}
					position++
					if buffer[position] != rune('r') {
						goto l65
					}
					position++
					if buffer[position] != rune('u') {
						goto l65
					}
					position++
					if buffer[position] != rune('e') {
						goto l65
					}
					position++
					{
						add(ruleAction12, position)
					}
					goto l62
				l65:
					position, tokenIndex = position62, tokenIndex62
					if buffer[position] != rune('f') {
						goto l67
					}
					position++
					if buffer[position] != rune('a') {
						goto l67
					}
					position++
					if buffer[position] != rune('l') {
						goto l67
					}
					position++
					if buffer[position] != rune('s') {
						goto l67
					}
					position++
					if buffer[position] != rune('e') {
						goto l67
					}
					position++
					{
						add(ruleAction13, position)
					}
					goto l62
				l67:
					position, tokenIndex = position62, tokenIndex62
					{
						position70 := position
						{
							position71, tokenIndex71 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l71
							}
							position++
							goto l72
						l71:
							position, tokenIndex = position71, tokenIndex71
						}
					l72:
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l69
						}
						position++
					l73:
						{
							position74, tokenIndex74 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l74
							}
							position++
							goto l73
						l74:
							position, tokenIndex = position74, tokenIndex74
						}
						{
							position75, tokenIndex75 := position, tokenIndex
							if buffer[position] != rune('.') {
								goto l75
							}
							position++
						l77:
							{
								position78, tokenIndex78 := position, tokenIndex
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l78
								}
								position++
								goto l77
							l78:
								position, tokenIndex = position78, tokenIndex78
							}
							goto l76
						l75:
							position, tokenIndex = position75, tokenIndex75
						}
					l76:
						add(rulePegText, position70)
					}
					{
						add(ruleAction14, position)
					}
					goto l62
				l69:
					position, tokenIndex = position62, tokenIndex62
					{
						position81 := position
						{
							position82, tokenIndex82 := position, tokenIndex
							if buffer[position] != rune('-') {
								goto l82
							}
							position++
							goto l83
						l82:
							position, tokenIndex = position82, tokenIndex82
						}
					l83:
						if buffer[position] != rune('.') {
							goto l80
						}
						position++
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l80
						}
						position++
					l84:
						{
							position85, tokenIndex85 := position, tokenIndex
							if c := buffer[position]; c < rune('0') || c > rune('9') {
								goto l85
							}
							position++
							goto l84
						l85:
							position, tokenIndex = position85, tokenIndex85
						}
						add(rulePegText, position81)
					}
					{
						add(ruleAction15, position)
					}
					goto l62
				l80:
					position, tokenIndex = position62, tokenIndex62
					{
						switch buffer[position] {
						case '\'':
							if buffer[position] != rune('\'') {
								goto l60
							}
							position++
							{
								position88 := position
								{
									switch buffer[position] {
									case ':':
										if buffer[position] != rune(':') {
											goto l60
										}
										position++
										break
									case '_':
										if buffer[position] != rune('_') {
											goto l60
										}
										position++
										break
									case '-':
										if buffer[position] != rune('-') {
											goto l60
										}
										position++
										break
									case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
										if c := buffer[position]; c < rune('0') || c > rune('9') {
											goto l60
										}
										position++
										break
									case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
										if c := buffer[position]; c < rune('A') || c > rune('Z') {
											goto l60
										}
										position++
										break
									default:
										if c := buffer[position]; c < rune('a') || c > rune('z') {
											goto l60
										}
										position++
										break
									}
								}

							l89:
								{
									position90, tokenIndex90 := position, tokenIndex
									{
										switch buffer[position] {
										case ':':
											if buffer[position] != rune(':') {
												goto l90
											}
											position++
											break
										case '_':
											if buffer[position] != rune('_') {
												goto l90
											}
											position++
											break
										case '-':
											if buffer[position] != rune('-') {
												goto l90
											}
											position++
											break
										case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
											if c := buffer[position]; c < rune('0') || c > rune('9') {
												goto l90
											}
											position++
											break
										case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
											if c := buffer[position]; c < rune('A') || c > rune('Z') {
												goto l90
											}
											position++
											break
										default:
											if c := buffer[position]; c < rune('a') || c > rune('z') {
												goto l90
											}
											position++
											break
										}
									}

									goto l89
								l90:
									position, tokenIndex = position90, tokenIndex90
								}
								add(rulePegText, position88)
							}
							if buffer[position] != rune('\'') {
								goto l60
							}
							position++
							{
								add(ruleAction18, position)
							}
							break
						case '"':
							if buffer[position] != rune('"') {
								goto l60
							}
							position++
							{
								position94 := position
								{
									switch buffer[position] {
									case ':':
										if buffer[position] != rune(':') {
											goto l60
										}
										position++
										break
									case '_':
										if buffer[position] != rune('_') {
											goto l60
										}
										position++
										break
									case '-':
										if buffer[position] != rune('-') {
											goto l60
										}
										position++
										break
									case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
										if c := buffer[position]; c < rune('0') || c > rune('9') {
											goto l60
										}
										position++
										break
									case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
										if c := buffer[position]; c < rune('A') || c > rune('Z') {
											goto l60
										}
										position++
										break
									default:
										if c := buffer[position]; c < rune('a') || c > rune('z') {
											goto l60
										}
										position++
										break
									}
								}

							l95:
								{
									position96, tokenIndex96 := position, tokenIndex
									{
										switch buffer[position] {
										case ':':
											if buffer[position] != rune(':') {
												goto l96
											}
											position++
											break
										case '_':
											if buffer[position] != rune('_') {
												goto l96
											}
											position++
											break
										case '-':
											if buffer[position] != rune('-') {
												goto l96
											}
											position++
											break
										case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
											if c := buffer[position]; c < rune('0') || c > rune('9') {
												goto l96
											}
											position++
											break
										case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
											if c := buffer[position]; c < rune('A') || c > rune('Z') {
												goto l96
											}
											position++
											break
										default:
											if c := buffer[position]; c < rune('a') || c > rune('z') {
												goto l96
											}
											position++
											break
										}
									}

									goto l95
								l96:
									position, tokenIndex = position96, tokenIndex96
								}
								add(rulePegText, position94)
							}
							if buffer[position] != rune('"') {
								goto l60
							}
							position++
							{
								add(ruleAction17, position)
							}
							break
						default:
							{
								position100 := position
								{
									switch buffer[position] {
									case ':':
										if buffer[position] != rune(':') {
											goto l60
										}
										position++
										break
									case '_':
										if buffer[position] != rune('_') {
											goto l60
										}
										position++
										break
									case '-':
										if buffer[position] != rune('-') {
											goto l60
										}
										position++
										break
									case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
										if c := buffer[position]; c < rune('0') || c > rune('9') {
											goto l60
										}
										position++
										break
									case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
										if c := buffer[position]; c < rune('A') || c > rune('Z') {
											goto l60
										}
										position++
										break
									default:
										if c := buffer[position]; c < rune('a') || c > rune('z') {
											goto l60
										}
										position++
										break
									}
								}

							l101:
								{
									position102, tokenIndex102 := position, tokenIndex
									{
										switch buffer[position] {
										case ':':
											if buffer[position] != rune(':') {
												goto l102
											}
											position++
											break
										case '_':
											if buffer[position] != rune('_') {
												goto l102
											}
											position++
											break
										case '-':
											if buffer[position] != rune('-') {
												goto l102
											}
											position++
											break
										case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
											if c := buffer[position]; c < rune('0') || c > rune('9') {
												goto l102
											}
											position++
											break
										case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
											if c := buffer[position]; c < rune('A') || c > rune('Z') {
												goto l102
											}
											position++
											break
										default:
											if c := buffer[position]; c < rune('a') || c > rune('z') {
												goto l102
											}
											position++
											break
										}
									}

									goto l101
								l102:
									position, tokenIndex = position102, tokenIndex102
								}
								add(rulePegText, position100)
							}
							{
								add(ruleAction16, position)
							}
							break
						}
					}

				}
			l62:
				add(ruleitem, position61)
			}
			return true
		l60:
			position, tokenIndex = position60, tokenIndex60
			return false
		},
		/* 9 field <- <(<(([a-z] / [A-Z]) ((&('_') '_') | (&('0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9') [0-9]) | (&('A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z') [A-Z]) | (&('a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z') [a-z]))*)> Action19)> */
		func() bool {
			position106, tokenIndex106 := position, tokenIndex
			{
				position107 := position
				{
					position108 := position
					{
						position109, tokenIndex109 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l110
						}
						position++
						goto l109
					l110:
						position, tokenIndex = position109, tokenIndex109
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l106
						}
						position++
					}
				l109:
				l111:
					{
						position112, tokenIndex112 := position, tokenIndex
						{
							switch buffer[position] {
							case '_':
								if buffer[position] != rune('_') {
									goto l112
								}
								position++
								break
							case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
								if c := buffer[position]; c < rune('0') || c > rune('9') {
									goto l112
								}
								position++
								break
							case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
								if c := buffer[position]; c < rune('A') || c > rune('Z') {
									goto l112
								}
								position++
								break
							default:
								if c := buffer[position]; c < rune('a') || c > rune('z') {
									goto l112
								}
								position++
								break
							}
						}

						goto l111
					l112:
						position, tokenIndex = position112, tokenIndex112
					}
					add(rulePegText, position108)
				}
				{
					add(ruleAction19, position)
				}
				add(rulefield, position107)
			}
			return true
		l106:
			position, tokenIndex = position106, tokenIndex106
			return false
		},
		/* 10 close <- <(')' sp)> */
		nil,
		/* 11 sp <- <(' ' / '\t')*> */
		func() bool {
			{
				position117 := position
			l118:
				{
					position119, tokenIndex119 := position, tokenIndex
					{
						position120, tokenIndex120 := position, tokenIndex
						if buffer[position] != rune(' ') {
							goto l121
						}
						position++
						goto l120
					l121:
						position, tokenIndex = position120, tokenIndex120
						if buffer[position] != rune('\t') {
							goto l119
						}
						position++
					}
				l120:
					goto l118
				l119:
					position, tokenIndex = position119, tokenIndex119
				}
				add(rulesp, position117)
			}
			return true
		},
		/* 12 comma <- <(sp ',' sp)> */
		func() bool {
			position122, tokenIndex122 := position, tokenIndex
			{
				position123 := position
				if !_rules[rulesp]() {
					goto l122
				}
				if buffer[position] != rune(',') {
					goto l122
				}
				position++
				if !_rules[rulesp]() {
					goto l122
				}
				add(rulecomma, position123)
			}
			return true
		l122:
			position, tokenIndex = position122, tokenIndex122
			return false
		},
		/* 13 lbrack <- <('[' sp)> */
		nil,
		/* 14 rbrack <- <(sp ']' sp)> */
		nil,
		/* 15 newline <- <(sp '\n' sp)> */
		func() bool {
			position126, tokenIndex126 := position, tokenIndex
			{
				position127 := position
				if !_rules[rulesp]() {
					goto l126
				}
				if buffer[position] != rune('\n') {
					goto l126
				}
				position++
				if !_rules[rulesp]() {
					goto l126
				}
				add(rulenewline, position127)
			}
			return true
		l126:
			position, tokenIndex = position126, tokenIndex126
			return false
		},
		nil,
		/* 18 Action0 <- <{ p.startCall(buffer[begin:end] ) }> */
		nil,
		/* 19 Action1 <- <{ p.endCall() }> */
		nil,
		/* 20 Action2 <- <{ p.addBTWN() }> */
		nil,
		/* 21 Action3 <- <{ p.addLTE() }> */
		nil,
		/* 22 Action4 <- <{ p.addGTE() }> */
		nil,
		/* 23 Action5 <- <{ p.addEQ() }> */
		nil,
		/* 24 Action6 <- <{ p.addNEQ() }> */
		nil,
		/* 25 Action7 <- <{ p.addLT() }> */
		nil,
		/* 26 Action8 <- <{ p.addGT() }> */
		nil,
		/* 27 Action9 <- <{ p.startList() }> */
		nil,
		/* 28 Action10 <- <{ p.endList() }> */
		nil,
		/* 29 Action11 <- <{ p.addVal(nil) }> */
		nil,
		/* 30 Action12 <- <{ p.addVal(true) }> */
		nil,
		/* 31 Action13 <- <{ p.addVal(false) }> */
		nil,
		/* 32 Action14 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 33 Action15 <- <{ p.addNumVal(buffer[begin:end]) }> */
		nil,
		/* 34 Action16 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 35 Action17 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 36 Action18 <- <{ p.addVal(buffer[begin:end]) }> */
		nil,
		/* 37 Action19 <- <{ p.addField(buffer[begin:end]) }> */
		nil,
	}
	p.rules = _rules
}
