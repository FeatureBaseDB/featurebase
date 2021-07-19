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

package pql_test

import (
	"testing"

	"github.com/molecula/featurebase/v2/pql"
)

// Ensure call can be converted into a string.
func TestCall_String(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		c := &pql.Call{Name: "Bitmap"}
		if s := c.String(); s != `Bitmap()` {
			t.Fatalf("unexpected string: %s", s)
		}
	})
	t.Run("With Args", func(t *testing.T) {
		c := &pql.Call{
			Name: "Range",
			Args: map[string]interface{}{
				"other":  "f",
				"field0": &pql.Condition{Op: pql.GTE, Value: 10},
			},
		}
		if s := c.String(); s != `Range(field0>=10, other="f")` {
			t.Fatalf("unexpected string: %s", s)
		}
	})
}

// Ensure condition string with subject is correct.
func TestCondition_StringWithSubj(t *testing.T) {
	subj := "subj"
	for _, tt := range []struct {
		op  pql.Token
		val interface{}
		exp string
	}{
		{pql.BETWEEN, []interface{}{int64(4), int64(8)}, "4<=subj<=8"},
		{pql.BETWEEN, []interface{}{uint64(5), uint64(9)}, "5<=subj<=9"},
		{pql.BETWEEN, []interface{}{pql.Decimal{Value: -401, Scale: 2}, pql.Decimal{Value: 802, Scale: 1}}, "-4.01<=subj<=80.2"},
		{pql.EQ, nil, "subj==null"},
		{pql.NEQ, nil, "subj!=null"},
	} {
		c := &pql.Condition{
			Op:    tt.op,
			Value: tt.val,
		}
		if sws := c.StringWithSubj(subj); sws != tt.exp {
			t.Fatalf("invalid between string. expected: %s, got %s", tt.exp, sws)
		}
	}
}
