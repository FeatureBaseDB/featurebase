// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"bytes"
	"strings"
	"unicode/utf8"
)

// tokenizeLike turns a "like" pattern into a list of tokens.
// Every token is either a string to exactly match or a combination of % and _ placeholders.
func tokenizeLike(like string) []string {
	var tokens []string
	for like != "" {
		var token string
		i := strings.IndexAny(like, "%_")
		switch i {
		case 0:
			// Generate a token of placeholders.

			// Iterate bytewise over the string to find the end of the token.
			// The % and _ characters are ASCII, so we do not have to worry about Unicode right here.
			j := 1
			for j < len(like) && (like[j] == '%' || like[j] == '_') {
				j++
			}
			token, like = like[:j], like[j:]
		case -1:
			// There are no more placeholders - generate the last token.
			token, like = like, ""
		default:
			// Generate an exact match token.
			token, like = like[:i], like[i:]
		}
		tokens = append(tokens, token)
	}
	return tokens
}

// filterStepKind is a kind of step in a like filter.
type filterStepKind uint8

const (
	filterStepPrefix      filterStepKind = iota // x...
	filterStepSkipN                             // __...
	filterStepSkipThrough                       // %x...
	filterStepSuffix                            // %x
	filterStepMinLength                         // _%
)

// filterStep is a step in a like filter.
type filterStep struct {
	// kind is the step kind.
	kind filterStepKind

	// str is the substring for a prefix/skipthrough/suffix step.
	str []byte

	// n is the number of underscores in the step (if relevant).
	n int
}

// planLike generates a filtering plan for a like pattern.
func planLike(like string) []filterStep {
	// Tokenize the like pattern.
	tokens := tokenizeLike(like)

	steps := make([]filterStep, 0, len(tokens))
	var merged bool
	for i, t := range tokens {
		if merged {
			// The token was already merged into the previous step.
			merged = false
			continue
		}

		// Convert the token to a step.
		var step filterStep
		hasPercent := strings.ContainsRune(t, '%')
		underscores := strings.Count(t, "_")
		switch {
		case hasPercent && i+1 < len(tokens):
			// Generate a step to skip through the next token.
			step = filterStep{
				kind: filterStepSkipThrough,
				str:  []byte(tokens[i+1]),
				n:    underscores,
			}
			merged = true
		case hasPercent:
			// Generate a terminating step to absorb the remainder of the string.
			step = filterStep{
				kind: filterStepMinLength,
				n:    underscores,
			}
		case underscores > 0:
			// Generate a step to absorb _ placeholders.
			step = filterStep{
				kind: filterStepSkipN,
				n:    underscores,
			}
		default:
			// Generate a step to process an exact match of the beginning of a string.
			step = filterStep{
				kind: filterStepPrefix,
				str:  []byte(t),
			}
		}
		steps = append(steps, step)
	}

	// Optimize suffix matching.
	if len(steps) > 0 && steps[len(steps)-1].kind == filterStepSkipThrough {
		steps[len(steps)-1].kind = filterStepSuffix
	}

	return steps
}

// matchLike matches a string using a like plan.
func matchLike(key []byte, like ...filterStep) bool {
	for i, step := range like {
		switch step.kind {
		case filterStepPrefix:
			// Match a prefix.
			if !bytes.HasPrefix(key, step.str) {
				return false
			}
			key = key[len(step.str):]
		case filterStepSkipN:
			// Skip some placeholders.
			n := step.n
			for j := 0; j < n; j++ {
				_, len := utf8.DecodeRune(key)
				if len == 0 {
					return false
				}
				key = key[len:]
			}
		case filterStepSkipThrough:
			// Skip through a string.

			// Skip through placeholders.
			var skipped int
			for skipped < step.n {
				j := bytes.Index(key, step.str)
				switch j {
				case -1:
					// There are no more matches.
					return false
				case 0:
					// Skip a single rune to ensure forward progress.
					// This is somewhat inefficient since we have to search the string again next time.
					// This will hopefully not have to be used very frequently.
					_, len := utf8.DecodeRune(key)
					key = key[len:]
					skipped += len
				default:
					// Skip until the substring and count the skipped runes.
					k := -1
					for k = range key[:j] {
					}
					skipped += k + 1

					key = key[j:]
				}
			}

			// Iterate through the substring matches until the rest of the pattern matches.
			remaining := like[i+1:]
			for {
				// Find the next substring match.
				j := bytes.Index(key, step.str)
				switch {
				case j == -1:
					// There are no more matches.
					return false
				case j > 0:
					// Skip the data before the substring.
					key = key[j:]
				}

				// Apply the rest of the filter.
				if matchLike(key[len(step.str):], remaining...) {
					// This instance matches, no need to search any more.
					return true
				}

				// Skip the first rune of the substring so we do not rescan this substring match.
				_, len := utf8.DecodeRune(key)
				key = key[len:]
			}
		case filterStepSuffix:
			// Match a suffix.
			if !bytes.HasSuffix(key, step.str) {
				// Suffix not present.
				return false
			}
			if step.n <= 0 {
				// No skip length check necessary.
				return true
			}

			// Check length of the substring before the suffix.
			key = key[:len(key)-len(step.str)]
			fallthrough
		case filterStepMinLength:
			if len(key) < step.n {
				// The string is definitely too short.
				return false
			}

			// Check if the string is long enough.
			return utf8.RuneCount(key) >= step.n
		default:
			panic("invalid step")
		}
	}

	// If there is any unmatched data left, this is not a match.
	return len(key) == 0
}
