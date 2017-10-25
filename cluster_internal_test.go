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
)

// Ensure that fragCombos creates the correct fragment mapping.
func TestFragCombos(t *testing.T) {

	c := NewCluster()
	uri0, err := NewURIFromAddress("host0")
	if err != nil {
		t.Fatal(err)
	}
	uri1, err := NewURIFromAddress("host1")
	if err != nil {
		t.Fatal(err)
	}
	c.AddNode(*uri0)
	c.AddNode(*uri1)

	tests := []struct {
		idx        string
		maxSlice   uint64
		frameViews viewsByFrame
		expected   fragsByHost
	}{
		{
			idx:        "i",
			maxSlice:   uint64(2),
			frameViews: viewsByFrame{"f": []string{"v1", "v2"}},
			expected: fragsByHost{
				URI{"http", "host0", 10101}: []frag{{"f", "v1", uint64(0)}, {"f", "v2", uint64(0)}},
				URI{"http", "host1", 10101}: []frag{{"f", "v1", uint64(1)}, {"f", "v2", uint64(1)}, {"f", "v1", uint64(2)}, {"f", "v2", uint64(2)}},
			},
		},
		{
			idx:        "foo",
			maxSlice:   uint64(3),
			frameViews: viewsByFrame{"f": []string{"v0"}},
			expected: fragsByHost{
				URI{"http", "host0", 10101}: []frag{{"f", "v0", uint64(1)}, {"f", "v0", uint64(2)}},
				URI{"http", "host1", 10101}: []frag{{"f", "v0", uint64(0)}, {"f", "v0", uint64(3)}},
			},
		},
	}
	for _, test := range tests {

		actual := c.fragCombos(test.idx, test.maxSlice, test.frameViews)
		if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("expected: %v, but got: %v", test.expected, actual)
		}

	}
}
