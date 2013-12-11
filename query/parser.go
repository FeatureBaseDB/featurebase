package query

import (
    "encoding/json"
	"errors"
	"pilosa/db"
	//"github.com/davecgh/go-spew/spew"
)


var InvalidQueryError = errors.New("Invalid query format.")

type QueryParser struct {
	QueryString string
}

func (q *QueryParser) Walk(data interface{}) (*Query, error) {
	query := new(Query)

	slice, ok := data.([]interface{})
	if !ok {
		return nil, InvalidQueryError
	}
	operation, ok := slice[0].(string)

	if !ok {
		return nil, InvalidQueryError
	}
	if operation == "union" || operation == "intersect" {
		query.Operation = operation
		inputs := slice[1:]
		query.Inputs = make([]QueryInput, len(inputs))
		for idx, input := range inputs {
			subquery, err := q.Walk(input)
			if err != nil {
				return nil, err
			}
			query.Inputs[idx] = subquery
		}
	} else if operation == "bitmap" {
		query.Operation = "get"
		frame, ok := slice[1].(string)
		if !ok {
			return nil, InvalidQueryError
		}
		id, ok := slice[2].(float64)
		if !ok {
			return nil, InvalidQueryError
		}
		id_int := int(id)
		query.Inputs = []QueryInput{db.Bitmap{id_int, frame}}
	}

	return query, nil
}

func (q *QueryParser) Parse() (*Query, error) {
	var data interface{}
	if err := json.Unmarshal([]byte(q.QueryString), &data); err != nil {
		return nil, err
	}
	return q.Walk(data)
}
