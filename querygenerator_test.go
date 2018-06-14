package pilosa_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

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

func Between(frame string, min, max int) *pql.Call {
	return &pql.Call{
		Name: "Range",
		Args: Args{
			"Op":    pql.BETWEEN,
			"Value": []int{min, max},
		},
	}
}
func Lt(frame string, column int) *pql.Call {
	return &pql.Call{
		Name: "Range",
		Args: Args{
			"Op":    pql.LT,
			"Value": column,
		},
	}
}
func Lte(frame string, column int) *pql.Call {
	return &pql.Call{
		Name: "Range",
		Args: Args{
			"Op":    pql.LTE,
			"Value": column,
		},
	}
}
func Gt(frame string, column int) *pql.Call {
	return &pql.Call{
		Name: "Range",
		Args: Args{
			"Op":    pql.GT,
			"Value": column,
		},
	}
}

func Gte(frame string, column int) *pql.Call {
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

func TestPQL_Generator(t *testing.T) {
	t.Run("pql.Query generator", func(t *testing.T) {
		for _, u := range []struct {
			pql  string
			calc *pql.Query
			exp  *pql.Query
		}{
			{
				pql:  "Union(Row(aaa=10),Row(bbb=9))",
				calc: PQL(Union(Row("aaa", 10), Row("bbb", 9))),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Union",
							Args: map[string]interface{}{},
							Children: []*pql.Call{
								{
									Name: "Row",
									Args: map[string]interface{}{"frame": "aaa", "row": 10},
								},
								{
									Name: "Row",
									Args: map[string]interface{}{"frame": "bbb", "row": 9},
								},
							},
						},
					},
				},
			},
			{
				pql:  "Intersect(Row(aaa=10),Row(bbb=9))",
				calc: PQL(Intersect(Row("aaa", 10), Row("bbb", 9))),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Intersect",
							Args: map[string]interface{}{},
							Children: []*pql.Call{
								{
									Name: "Row",
									Args: map[string]interface{}{"frame": "aaa", "row": 10},
								},
								{
									Name: "Row",
									Args: map[string]interface{}{"frame": "bbb", "row": 9},
								},
							},
						},
					},
				},
			},
			{
				pql:  "Difference(Row(aaa=10),Row(bbb=9))",
				calc: PQL(Difference(Row("aaa", 10), Row("bbb", 9))),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Difference",
							Args: map[string]interface{}{},
							Children: []*pql.Call{
								{
									Name: "Row",
									Args: map[string]interface{}{"frame": "aaa", "row": 10},
								},
								{
									Name: "Row",
									Args: map[string]interface{}{"frame": "bbb", "row": 9},
								},
							},
						},
					},
				},
			},
			{
				pql:  "Range(bbb > 20)",
				calc: PQL(Gt("bbb", 20)),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Range",
							Args: map[string]interface{}{
								"Op":    pql.GT,
								"Value": 20,
							},
						},
					},
				},
			},
			{
				pql:  "Range(10 < bbb < 20)",
				calc: PQL(Between("bbb", 10, 20)),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Range",
							Args: map[string]interface{}{
								"Op":    pql.BETWEEN,
								"Value": []int{10, 20},
							},
						},
					},
				},
			},
			{
				pql:  "Set(10, aaa=9)",
				calc: PQL(Set(10, "aaa=9")),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Set",
							Args: map[string]interface{}{
								"frame":  "aaa",
								"value":  int64(9),
								"column": 10,
							},
						},
					},
				},
			},
			{
				pql:  "Clear(10, aaa=10)",
				calc: PQL(Clear(10, "aaa=9")),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Clear",
							Args: map[string]interface{}{
								"frame":  "aaa",
								"value":  int64(9),
								"column": 10,
							},
						},
					},
				},
			},
			{
				pql:  `Set(10, aaa=10, "2017-03-02T03:00")`,
				calc: PQL(Set(10, "aaa=9", "2017-03-02T03:00")),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Set",
							Args: map[string]interface{}{
								"frame":     "aaa",
								"value":     int64(9),
								"column":    10,
								"timestamp": "2017-03-02T03:00",
							},
						},
					},
				},
			},
			{
				pql:  `Count(Row(aaa=10))`,
				calc: PQL(Count(Row("aaa", 10))),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Count",
							Args: map[string]interface{}{},
							Children: []*pql.Call{
								{
									Name: "Row",
									Args: map[string]interface{}{"frame": "aaa", "row": 10},
								},
							},
						},
					},
				},
			},
			{
				pql:  "Intersect(Union(Row(aaa=10),Row(bbb=9)), Row(aaa=12))",
				calc: PQL(Intersect(Union(Row("aaa", 10), Row("bbb", 9)), Row("aaa", 12))),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Intersect",
							Args: map[string]interface{}{},
							Children: []*pql.Call{
								{
									Name: "Union",
									Args: map[string]interface{}{},
									Children: []*pql.Call{
										{
											Name: "Row",
											Args: map[string]interface{}{"frame": "aaa", "row": 10},
										},
										{
											Name: "Row",
											Args: map[string]interface{}{"frame": "bbb", "row": 9},
										},
									},
								},
								{
									Name: "Row",
									Args: map[string]interface{}{"frame": "aaa", "row": 12},
								},
							},
						},
					},
				},
			},
		} {

			if !Compare(u.calc, u.exp) {
				t.Fatalf("Not Equal. expected: %v, got %v for %s", u.exp, u.calc, u.pql)
			}
		}
	})

}
