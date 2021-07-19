// Copyright 2020 Pilosa Corp.
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

package sql

import (
	"fmt"
	"reflect"
	"testing"

	pproto "github.com/molecula/featurebase/v2/proto"
	"github.com/pkg/errors"
)

func TestHeaderAssignment(t *testing.T) {
	abcdHdrs := []*pproto.ColumnInfo{
		{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"},
	}
	tests := []struct {
		cols         []Column
		hdrs         []*pproto.ColumnInfo
		expPlacement []uint
		expLabels    []string
		expErr       error
	}{
		{
			cols: []Column{
				NewBasicColumn("a", "namea", ""),
			},
			hdrs:         abcdHdrs,
			expPlacement: []uint{0},
			expLabels:    []string{"namea"},
		},
		{
			cols: []Column{
				NewBasicColumn("", "a", ""),
			},
			hdrs:         abcdHdrs,
			expPlacement: []uint{0},
			expLabels:    []string{"a"},
		},
		{
			cols: []Column{
				NewBasicColumn("", "a", "aliasa"),
			},
			hdrs:         abcdHdrs,
			expPlacement: []uint{0},
			expLabels:    []string{"aliasa"},
		},
		{
			cols: []Column{
				NewBasicColumn("", "a", "aliasa"),
				NewBasicColumn("c", "namec", ""),
			},
			hdrs:         abcdHdrs,
			expPlacement: []uint{0, 2},
			expLabels:    []string{"aliasa", "namec"},
		},
		{
			cols: []Column{
				NewBasicColumn("d", "c", "aliasd"),
				NewBasicColumn("b", "nameb", ""),
			},
			hdrs:         abcdHdrs,
			expPlacement: []uint{3, 1},
			expLabels:    []string{"aliasd", "nameb"},
		},
		// Errors
		{
			cols: []Column{
				NewBasicColumn("", "x", ""),
			},
			hdrs:   abcdHdrs,
			expErr: ErrFieldNotInHeaders,
		},
		{
			cols: []Column{
				NewBasicColumn("", "a", ""),
				NewBasicColumn("", "b", ""),
				NewBasicColumn("", "c", ""),
				NewBasicColumn("", "d", ""),
				NewBasicColumn("", "e", ""),
			},
			hdrs:   abcdHdrs,
			expErr: ErrIncompleteHeaders,
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			placement, labels, err := headerAssignment(test.cols, test.hdrs)

			if test.expErr == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if test.expErr != errors.Cause(err) {
					t.Fatalf("expected error: %v, but got: %v", test.expErr, err)
				}
			}

			if !reflect.DeepEqual(placement, test.expPlacement) {
				t.Fatalf("expected placement: %v, but got: %v", test.expPlacement, placement)
			} else if !reflect.DeepEqual(labels, test.expLabels) {
				t.Fatalf("expected labels: %v, but got: %v", test.expLabels, labels)
			}
		})
	}
}
