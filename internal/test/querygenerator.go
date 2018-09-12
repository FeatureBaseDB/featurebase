// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pilosa/pilosa/pql"
)

type Args map[string]interface{}

type Calls []*pql.Call

func PQL(calls ...*pql.Call) *pql.Query {
	return &pql.Query{Calls: calls}
}

func Row(frame string, row int) *pql.Call {
	return &pql.Call{
		Name: "Row",
		Args: Args{
			"frame": frame,
			"row":   row,
		},
	}
}

func mutationArgs(args ...interface{}) Args {
	rargs := make(Args)
	for _, arg := range args {
		switch v := arg.(type) {
		case int:
			rargs["column"] = v
		case string:
			if strings.Contains(v, "=") {
				parts := strings.Split(v, "=")
				rargs["frame"] = parts[0]
				i, _ := strconv.ParseInt(parts[1], 10, 64)
				rargs["value"] = i
			} else {
				rargs["timestamp"] = v
			}
		default:
			fmt.Printf("wat %T!\n", v)
		}
	}

	return rargs
}

func Set(args ...interface{}) *pql.Call {
	return &pql.Call{Name: "Set", Args: mutationArgs(args...)}
}

func Clear(args ...interface{}) *pql.Call {
	return &pql.Call{Name: "Clear", Args: mutationArgs(args...)}
}

func magic(args ...interface{}) (Args, Calls) {
	var (
		rargs Args
		calls Calls
	)

	for _, arg := range args {
		switch v := arg.(type) {
		case Args:
			rargs = v
		case []*pql.Call:
			calls = append(calls, v...)
		default:
			fmt.Printf("wat %T!\n", v)
		}
	}

	return rargs, calls
}
func Count(args ...*pql.Call) *pql.Call {
	kvargs, children := magic(args)
	return &pql.Call{Name: "Count", Args: kvargs, Children: children}
}

func Union(args ...*pql.Call) *pql.Call {
	kvargs, children := magic(args)
	return &pql.Call{Name: "Union", Args: kvargs, Children: children}
}

func Intersect(args ...*pql.Call) *pql.Call {
	kvargs, children := magic(args)
	return &pql.Call{Name: "Intersect", Args: kvargs, Children: children}
}

func Difference(args ...*pql.Call) *pql.Call {
	kvargs, children := magic(args)
	return &pql.Call{Name: "Difference", Args: kvargs, Children: children}
}

func Xor(args ...*pql.Call) *pql.Call {
	kvargs, children := magic(args)
	return &pql.Call{Name: "Xor", Args: kvargs, Children: children}
}

func Not(args ...*pql.Call) *pql.Call {
	kvargs, children := magic(args)
	return &pql.Call{Name: "Not", Args: kvargs, Children: children}
}

func Between(_ string, min, max int) *pql.Call {
	return &pql.Call{
		Name: "Range",
		Args: Args{
			"Op":    pql.BETWEEN,
			"Value": []int{min, max},
		},
	}
}
func Lt(_ string, column int) *pql.Call {
	return &pql.Call{
		Name: "Range",
		Args: Args{
			"Op":    pql.LT,
			"Value": column,
		},
	}
}
func Lte(_ string, column int) *pql.Call {
	return &pql.Call{
		Name: "Range",
		Args: Args{
			"Op":    pql.LTE,
			"Value": column,
		},
	}
}
func Gt(_ string, column int) *pql.Call {
	return &pql.Call{
		Name: "Range",
		Args: Args{
			"Op":    pql.GT,
			"Value": column,
		},
	}
}

func Gte(_ string, column int) *pql.Call {
	return &pql.Call{
		Name: "Range",
		Args: Args{
			"Op":    pql.GTE,
			"Value": column,
		},
	}
}
func CompareCall(a, b *pql.Call) bool {
	if a.Name != b.Name {
		return false
	}
	for k, i := range a.Args {
		switch v := i.(type) {
		case []int:
			bside := b.Args[k]
			for j := range v {
				if v[j] != bside.([]int)[j] {
					return false
				}

			}
		default:
			if b.Args[k] != i {
				return false
			}
		}
	}

	if len(a.Children) == len(b.Children) {
		for i := range a.Children {
			if !CompareCall(a.Children[i], b.Children[i]) {
				return false
			}
		}
	} else {
		return false
	}
	return true
}

func Compare(a, b *pql.Query) bool {
	for i := range a.Calls {
		if !CompareCall(a.Calls[i], b.Calls[i]) {
			return false
		}

	}
	return true
}
