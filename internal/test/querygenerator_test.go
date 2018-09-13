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
	"testing"

	"github.com/pilosa/pilosa/pql"
)

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
			{
				pql:  `Not(Row(aaa=10))`,
				calc: PQL(Not(Row("aaa", 10))),
				exp: &pql.Query{
					Calls: []*pql.Call{
						{
							Name: "Not",
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
		} {

			if !Compare(u.calc, u.exp) {
				t.Fatalf("Not Equal. expected: %v, got %v for %s", u.exp, u.calc, u.pql)
			}
		}
	})

}
