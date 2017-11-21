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
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa/internal"
)

// Ensure that fragCombos creates the correct fragment mapping.
func TestFragCombos(t *testing.T) {

	uri0, err := NewURIFromAddress("host0")
	if err != nil {
		t.Fatal(err)
	}
	uri1, err := NewURIFromAddress("host1")
	if err != nil {
		t.Fatal(err)
	}

	c := NewCluster()
	c.addNodeBasicSorted(*uri0)
	c.addNodeBasicSorted(*uri1)

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

// newIndexWithTempPath returns a new instance of Index.
func newIndexWithTempPath(name string) *Index {
	path, err := ioutil.TempDir("", "pilosa-index-")
	if err != nil {
		panic(err)
	}
	index, err := NewIndex(path, name)
	if err != nil {
		panic(err)
	}
	return index
}

// Ensure that fragSources creates the correct fragment mapping.
func TestFragSources(t *testing.T) {

	uri0, err := NewURIFromAddress("host0")
	if err != nil {
		t.Fatal(err)
	}
	uri1, err := NewURIFromAddress("host1")
	if err != nil {
		t.Fatal(err)
	}
	uri2, err := NewURIFromAddress("host2")
	if err != nil {
		t.Fatal(err)
	}
	uri3, err := NewURIFromAddress("host3")
	if err != nil {
		t.Fatal(err)
	}

	c1 := NewCluster()
	c1.ReplicaN = 1
	c1.addNodeBasicSorted(*uri0)
	c1.addNodeBasicSorted(*uri1)

	c2 := NewCluster()
	c2.ReplicaN = 1
	c2.addNodeBasicSorted(*uri0)
	c2.addNodeBasicSorted(*uri1)
	c2.addNodeBasicSorted(*uri2)

	c3 := NewCluster()
	c3.ReplicaN = 2
	c3.addNodeBasicSorted(*uri0)
	c3.addNodeBasicSorted(*uri1)

	c4 := NewCluster()
	c4.ReplicaN = 2
	c4.addNodeBasicSorted(*uri0)
	c4.addNodeBasicSorted(*uri1)
	c4.addNodeBasicSorted(*uri2)

	c5 := NewCluster()
	c5.ReplicaN = 2
	c5.addNodeBasicSorted(*uri0)
	c5.addNodeBasicSorted(*uri1)
	c5.addNodeBasicSorted(*uri2)
	c5.addNodeBasicSorted(*uri3)

	idx := newIndexWithTempPath("i")
	frame, err := idx.CreateFrameIfNotExists("f", FrameOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = frame.SetBit("standard", 1, 101, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = frame.SetBit("standard", 1, 1300000, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = frame.SetBit("standard", 1, 2600000, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = frame.SetBit("standard", 1, 3900000, nil)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		from     *Cluster
		to       *Cluster
		idx      *Index
		expected map[URI][]*internal.ResizeSource
		err      string
	}{
		{
			from: c1,
			to:   c2,
			idx:  idx,
			expected: map[URI][]*internal.ResizeSource{
				URI{"http", "host0", 10101}: []*internal.ResizeSource{},
				URI{"http", "host1", 10101}: []*internal.ResizeSource{},
				URI{"http", "host2", 10101}: []*internal.ResizeSource{
					{&internal.URI{"http", "host0", 10101}, "i", "f", "standard", uint64(0)},
					{&internal.URI{"http", "host1", 10101}, "i", "f", "standard", uint64(2)},
				},
			},
			err: "",
		},
		{
			from: c4,
			to:   c3,
			idx:  idx,
			expected: map[URI][]*internal.ResizeSource{
				URI{"http", "host0", 10101}: []*internal.ResizeSource{
					{&internal.URI{"http", "host1", 10101}, "i", "f", "standard", uint64(1)},
				},
				URI{"http", "host1", 10101}: []*internal.ResizeSource{
					{&internal.URI{"http", "host0", 10101}, "i", "f", "standard", uint64(0)},
					{&internal.URI{"http", "host0", 10101}, "i", "f", "standard", uint64(2)},
				},
			},
			err: "",
		},
		{
			from: c5,
			to:   c4,
			idx:  idx,
			expected: map[URI][]*internal.ResizeSource{
				URI{"http", "host0", 10101}: []*internal.ResizeSource{
					{&internal.URI{"http", "host2", 10101}, "i", "f", "standard", uint64(0)},
					{&internal.URI{"http", "host2", 10101}, "i", "f", "standard", uint64(2)},
				},
				URI{"http", "host1", 10101}: []*internal.ResizeSource{
					{&internal.URI{"http", "host0", 10101}, "i", "f", "standard", uint64(3)},
				},
				URI{"http", "host2", 10101}: []*internal.ResizeSource{},
			},
			err: "",
		},
		{
			from:     c2,
			to:       c4,
			idx:      idx,
			expected: nil,
			err:      "clusters are the same size",
		},
		{
			from:     c1,
			to:       c5,
			idx:      idx,
			expected: nil,
			err:      "adding more than one node at a time is not supported",
		},
		{
			from:     c5,
			to:       c1,
			idx:      idx,
			expected: nil,
			err:      "removing more than one node at a time is not supported",
		},
	}
	for _, test := range tests {

		actual, err := (test.from).fragSources(test.to, test.idx)
		if test.err != "" {
			if err.Error() != test.err {
				t.Fatalf("expected error: %s", test.err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, test.expected) {
				t.Errorf("expected: %v, but got: %v", test.expected, actual)
			}
		}
	}
}

// Ensure that fragSources creates the correct fragment mapping.
func TestResizeJob(t *testing.T) {

	uri0, err := NewURIFromAddress("host0")
	if err != nil {
		t.Fatal(err)
	}
	uri1, err := NewURIFromAddress("host1")
	if err != nil {
		t.Fatal(err)
	}
	uri2, err := NewURIFromAddress("host2")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		existingURIs []URI
		uri          URI
		action       string
		expectedURIs map[URI]bool
	}{
		{
			existingURIs: []URI{*uri0, *uri1},
			uri:          *uri2,
			action:       ResizeJobActionAdd,
			expectedURIs: map[URI]bool{*uri0: false, *uri1: false, *uri2: false},
		},
		{
			existingURIs: []URI{*uri0, *uri1, *uri2},
			uri:          *uri2,
			action:       ResizeJobActionRemove,
			expectedURIs: map[URI]bool{*uri0: false, *uri1: false},
		},
	}
	for _, test := range tests {

		actual := NewResizeJob(test.existingURIs, test.uri, test.action)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual.URIs, test.expectedURIs) {
			t.Errorf("expected: %v, but got: %v", test.expectedURIs, actual.URIs)
		}
	}
}
