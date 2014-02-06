package query

import (
	"errors"
	"pilosa/db"
	"strconv"

	"tux21b.org/v1/gocql/uuid"
)

var InvalidQueryError = errors.New("Invalid query format.")

type QueryParser struct{}

func (qp *QueryParser) walkInputs(tokens []Token) ([]QueryInput, uint64, int) {
	// BITMAP
	if tokens[0].Type == TYPE_ID {
		// TODO: look for frame type in the tokens list
		b, err := strconv.ParseUint(tokens[0].Text, 10, 64)
		bitmap_id := uint64(b)
		if err != nil {
			panic(err)
		}
		// if the next 2 tokens are comma-frame, then we have a frame, else set to a default
		frame_type := "general"
		profile_id := uint64(0)

		if len(tokens) > 4 && tokens[2].Type == TYPE_FRAME && tokens[4].Type == TYPE_PROFILE {
			frame_type = tokens[2].Text
			profile_id, err = strconv.ParseUint(tokens[4].Text, 10, 64)
			if err != nil {
				panic(err)
			}
		} else if len(tokens) > 2 && tokens[2].Type == TYPE_FRAME {
			frame_type = tokens[2].Text
		} else if len(tokens) > 2 && tokens[2].Type == TYPE_PROFILE {
			profile_id, err = strconv.ParseUint(tokens[2].Text, 10, 64)
			if err != nil {
				panic(err)
			}
		}
		bm := db.Bitmap{bitmap_id, frame_type}
		return []QueryInput{&bm}, uint64(profile_id), 0
	}

	// LIST OF QUERIES
	n := int(10) // default LIMIT to 10
	qi := []QueryInput{}
	open_parens := -1 // >=0 means i'm inside the search for end paren
	start := 0
	for i := 0; i < len(tokens); i++ {
		if tokens[i].Type == TYPE_FUNC && open_parens == -1 {
			start = i
		} else if tokens[i].Type == TYPE_LP {
			open_parens++
		} else if tokens[i].Type == TYPE_RP {
			if open_parens == 0 {
				q, err := qp.walk(tokens[start : i+1])
				if err != nil {
					panic(err)
				}
				qi = append(qi, q)
				open_parens = -1
			} else {
				open_parens--
			}
		} else if tokens[i].Type == TYPE_LIMIT {
			x, _ := strconv.ParseInt(tokens[i].Text, 10, 32)
			n = int(x)
		}
	}
	return qi, 0, n
}

func (qp *QueryParser) walk(tokens []Token) (*Query, error) {

	if tokens[0].Type != TYPE_FUNC {
		panic("BAD!")
	}
	if tokens[1].Type != TYPE_LP {
		panic("BAD!")
	}

	q := new(Query)
	id := uuid.RandomUUID()
	q.Id = &id
	q.Operation = tokens[0].Text

	// scan from open to close paren
	open_parens := 0
	for i := 2; i < len(tokens); i++ {
		// 1 must be "("
		if tokens[i].Type == TYPE_LP {
			open_parens++
		} else if tokens[i].Type == TYPE_RP {
			if open_parens == 0 {
				if i == len(tokens)-1 {
					q.Inputs, q.ProfileId, q.N = qp.walkInputs(tokens[2:i])
				}
			} else {
				open_parens--
			}
		}
	}
	return q, nil
}

func (qp *QueryParser) Parse(tokens []Token) (*Query, error) {
	return qp.walk(tokens)
}
