package pilosa

import (
	"strings"
	"unicode/utf8"
)

func tokenizeLike(like string) []string {
	var tokens []string
	for like != "" {
		var token string
		i := strings.IndexAny(like, "%_")
		switch {
		case i == 0:
			j := 1
			for j < len(like) && (like[j] == '%' || like[j] == '_') {
				j++
			}
			token, like = like[:j], like[j:]
		case i < 0:
			token, like = like, ""
		default:
			token, like = like[:i], like[i:]
		}
		tokens = append(tokens, token)
	}
	return tokens
}

type filterStepKind uint8

const (
	filterStepPrefix filterStepKind = iota
	filterStepSkipN
	filterStepSkipThrough
	filterStepMinLength
)

type filterStep struct {
	kind filterStepKind
	str  string
	n    int
}

func planLike(like string) []filterStep {
	tokens := tokenizeLike(like)

	steps := make([]filterStep, 0, len(tokens))
	var merged bool
	for i, t := range tokens {
		if merged {
			merged = false
			continue
		}

		var step filterStep
		hasPercent := strings.ContainsRune(t, '%')
		underscores := strings.Count(t, "_")
		switch {
		case hasPercent && i+1 < len(tokens):
			step = filterStep{
				kind: filterStepSkipThrough,
				str:  tokens[i+1],
				n:    underscores,
			}
			merged = true
		case hasPercent:
			step = filterStep{
				kind: filterStepMinLength,
				n:    underscores,
			}
		case underscores > 0:
			step = filterStep{
				kind: filterStepSkipN,
				n:    underscores,
			}
		default:
			step = filterStep{
				kind: filterStepPrefix,
				str:  t,
			}
		}
		steps = append(steps, step)
	}

	return steps
}

func matchLike(key string, like ...filterStep) bool {
	for i, step := range like {
		switch step.kind {
		case filterStepPrefix:
			if !strings.HasPrefix(key, step.str) {
				return false
			}
			key = key[len(step.str):]
		case filterStepSkipN:
			n := step.n
			for j := 0; j < n; j++ {
				_, len := utf8.DecodeRuneInString(key)
				if len == 0 {
					return false
				}
				key = key[len:]
			}
		case filterStepSkipThrough:
			var skipped int
			for skipped < step.n {
				j := strings.Index(key, step.str)
				switch j {
				case -1:
					return false
				case 0:
					_, len := utf8.DecodeRuneInString(key)
					if len == 0 {
						return false
					}
					key = key[len:]
					skipped += len
				default:
					k := -1
					for k = range key[:j] {
					}
					skipped += k + 1

					key = key[j:]
				}
			}

			remaining := like[i+1:]
			for {
				j := strings.Index(key, step.str)
				switch {
				case j == -1:
					return false
				case j > 0:
					key = key[j:]
				}

				if matchLike(key[len(step.str):], remaining...) {
					return true
				}
				key = key[1:]
			}
		case filterStepMinLength:
			if len(key) < step.n {
				return false
			}

			j := -1
			for j = range key {
			}
			return j+1 >= step.n
		default:
			panic("invalid step")
		}
	}
	return key == ""
}
