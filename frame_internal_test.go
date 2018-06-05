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

package pilosa

import (
	"reflect"
	"testing"

	"github.com/pilosa/pilosa/pql"
)

// Ensure a bsiGroup can adjust to its baseValue.
func TestBSIGroup_BaseValue(t *testing.T) {
	b0 := &bsiGroup{
		Name: "b0",
		Type: bsiGroupTypeInt,
		Min:  -100,
		Max:  900,
	}
	b1 := &bsiGroup{
		Name: "b1",
		Type: bsiGroupTypeInt,
		Min:  0,
		Max:  1000,
	}

	b2 := &bsiGroup{
		Name: "b2",
		Type: bsiGroupTypeInt,
		Min:  100,
		Max:  1100,
	}

	t.Run("Normal Condition", func(t *testing.T) {

		for _, tt := range []struct {
			f             *bsiGroup
			op            pql.Token
			val           int64
			expBaseValue  uint64
			expOutOfRange bool
		}{
			// LT
			{b0, pql.LT, 5, 105, false},
			{b0, pql.LT, -8, 92, false},
			{b0, pql.LT, -108, 0, true},
			{b0, pql.LT, 1005, 1000, false},
			{b0, pql.LT, 0, 100, false},

			{b1, pql.LT, 5, 5, false},
			{b1, pql.LT, -8, 0, true},
			{b1, pql.LT, 1005, 1000, false},
			{b1, pql.LT, 0, 0, false},

			{b2, pql.LT, 5, 0, true},
			{b2, pql.LT, -8, 0, true},
			{b2, pql.LT, 105, 5, false},
			{b2, pql.LT, 1105, 1000, false},

			// GT
			{b0, pql.GT, -105, 0, false},
			{b0, pql.GT, 5, 105, false},
			{b0, pql.GT, 905, 0, true},
			{b0, pql.GT, 0, 100, false},

			{b1, pql.GT, 5, 5, false},
			{b1, pql.GT, -8, 0, false},
			{b1, pql.GT, 1005, 0, true},
			{b1, pql.GT, 0, 0, false},

			{b2, pql.GT, 5, 0, false},
			{b2, pql.GT, -8, 0, false},
			{b2, pql.GT, 105, 5, false},
			{b2, pql.GT, 1105, 0, true},

			// EQ
			{b0, pql.EQ, -105, 0, true},
			{b0, pql.EQ, 5, 105, false},
			{b0, pql.EQ, 905, 0, true},
			{b0, pql.EQ, 0, 100, false},

			{b1, pql.EQ, 5, 5, false},
			{b1, pql.EQ, -8, 0, true},
			{b1, pql.EQ, 1005, 0, true},
			{b1, pql.EQ, 0, 0, false},

			{b2, pql.EQ, 5, 0, true},
			{b2, pql.EQ, -8, 0, true},
			{b2, pql.EQ, 105, 5, false},
			{b2, pql.EQ, 1105, 0, true},
		} {
			bv, oor := tt.f.baseValue(tt.op, tt.val)
			if oor != tt.expOutOfRange {
				t.Fatalf("baseValue calculation on %s op %s, expected outOfRange %v, got %v", tt.f.Name, tt.op, tt.expOutOfRange, oor)
			} else if !reflect.DeepEqual(bv, tt.expBaseValue) {
				t.Fatalf("baseValue calculation on %s, expected value %v, got %v", tt.f.Name, tt.expBaseValue, bv)
			}
		}
	})

	t.Run("Betwween Condition", func(t *testing.T) {
		for _, tt := range []struct {
			f               *bsiGroup
			predMin         int64
			predMax         int64
			expBaseValueMin uint64
			expBaseValueMax uint64
			expOutOfRange   bool
		}{

			{b0, -205, -105, 0, 0, true},
			{b0, -105, 80, 0, 180, false},
			{b0, 5, 20, 105, 120, false},
			{b0, 20, 1005, 120, 1000, false},
			{b0, 1005, 2000, 0, 0, true},

			{b1, -105, -5, 0, 0, true},
			{b1, -5, 20, 0, 20, false},
			{b1, 5, 20, 5, 20, false},
			{b1, 20, 1005, 20, 1000, false},
			{b1, 1005, 2000, 0, 0, true},

			{b2, 5, 95, 0, 0, true},
			{b2, 95, 120, 0, 20, false},
			{b2, 105, 120, 5, 20, false},
			{b2, 120, 1105, 20, 1000, false},
			{b2, 1105, 2000, 0, 0, true},
		} {
			min, max, oor := tt.f.baseValueBetween(tt.predMin, tt.predMax)
			if oor != tt.expOutOfRange {
				t.Fatalf("baseValueBetween calculation on %s, expected outOfRange %v, got %v", tt.f.Name, tt.expOutOfRange, oor)
			} else if !reflect.DeepEqual(min, tt.expBaseValueMin) || !reflect.DeepEqual(max, tt.expBaseValueMax) {
				t.Fatalf("baseValueBetween calculation on %s, expected min/max %v/%v, got %v/%v", tt.f.Name, tt.expBaseValueMin, tt.expBaseValueMax, min, max)
			}
		}
	})
}
