// +build gofuzz

package pql

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/pilosa/pilosa/pql/internal/oldpql"
	"github.com/pkg/errors"
)

func Fuzz(data []byte) int {
	p1 := NewParser(bytes.NewReader(data))
	q1, err1 := p1.Parse()
	p2 := oldpql.NewParser(bytes.NewReader(data))
	q2, err2 := p2.Parse()
	if err1 != nil && err2 != nil {
		return 0 // both error - this is fine
	}
	if err1 != nil || err2 != nil {
		// error in one but not both - need to know this
		panic(fmt.Sprintf("Query: '%s' errored one but not both.\n%v\n%v\n", data, err1, err2))
	}

	// if parsers got different results
	if err := queriesEqual(q1, q2); err != nil {
		panic(fmt.Sprintf(`Query: '%s' parsed, but got different results:
Result New (string)
%s
Result New (hashv)
%#v
Result Old (string)
%s
Result Old (hashv)
%#v
err:
%v
`, data, q1, q1, q2, q2, err))
	}

	// both queries parsed succesfully and got equivalent results
	return 1
}

func queriesEqual(q1 *Query, q2 *oldpql.Query) (err error) {
	if q1.String() != q2.String() {
		defer func() {
			// golang black magic
			if err == nil {
				err = errors.New("string reps unequal")
			} else {
				err = errors.Wrap(err, "string reps unequal")
			}
		}()
	}
	if len(q1.Calls) != len(q2.Calls) {
		return errors.Errorf("call lengths unequal: %d and %d", len(q1.Calls), len(q2.Calls))
	}
	for i, c1 := range q1.Calls {
		c2 := q2.Calls[i]
		if err := callsEqual(c1, c2); err != nil {
			return errors.Wrapf(err, "calls at %d not equal", i)
		}
	}
	return nil
}

func callsEqual(c1 *Call, c2 *oldpql.Call) error {
	if err := argsEqual(c1.Args, c2.Args); err != nil {
		return errors.Wrap(err, "args unequal")
	}
	if c1.Name != c2.Name {
		return errors.Errorf("names unequal '%s' != '%s'", c1.Name, c2.Name)
	}
	if len(c1.Children) != len(c2.Children) {
		return errors.Errorf("different child lengths %d and %d", len(c1.Children), len(c2.Children))
	}

	for i, child1 := range c1.Children {
		child2 := c2.Children[i]
		if err := callsEqual(child1, child2); err != nil {
			return errors.Wrapf(err, "children at %d not equal", i)
		}
	}

	return nil
}

func argsEqual(a1 map[string]interface{}, a2 map[string]interface{}) error {
	if len(a1) != len(a2) {
		return errors.Errorf("lengths unequal %d and %d", len(a1), len(a2))
	}

	for k, v1 := range a1 {
		v2 := a1[k]
		if c1, ok := v1.(Condition); ok {
			if c2, ok := v2.(oldpql.Condition); ok {
				if int(c1.Op) != int(c2.Op) {
					return errors.Errorf("condition ops unequal %d %d", c1, c2)
				}
				if !reflect.DeepEqual(c1.Value, c2.Value) {
					return errors.Errorf("condition values unequal '%v' '%v'", c1.Value, c2.Value)
				}
				continue
			}
			return errors.Errorf("values at %s unequal '%v' '%v'", k, v1, v2)
		}
		if !reflect.DeepEqual(v1, v2) {
			return errors.Errorf("values at %s unequal '%v' '%v'", k, v1, v2)
		}
	}
	return nil
}
