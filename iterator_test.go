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

package pilosa_test

import (
	"reflect"
	"testing"

	"github.com/pilosa/pilosa"
)

// Ensure slice iterator and iterate over a set of pairs.
func TestSliceIterator(t *testing.T) {
	// Initialize iterator.
	itr := pilosa.NewSliceIterator(
		[]uint64{0, 0, 2, 4},
		[]uint64{0, 1, 0, 10},
	)

	// Iterate over all pairs.
	var pairs [][2]uint64
	for pid, bid, eof := itr.Next(); !eof; pid, bid, eof = itr.Next() {
		pairs = append(pairs, [2]uint64{pid, bid})
	}

	// Verify pairs output correctly.
	if !reflect.DeepEqual(pairs, [][2]uint64{
		{0, 0},
		{0, 1},
		{2, 0},
		{4, 10},
	}) {
		t.Fatalf("unexpected pairs: %+v", pairs)
	}
}

// Ensure buffered iterator can unread values on to the buffer.
func TestBufIterator(t *testing.T) {
	itr := pilosa.NewBufIterator(pilosa.NewSliceIterator(
		[]uint64{0, 0, 1, 2},
		[]uint64{1, 3, 0, 100},
	))
	itr.Seek(0, 2)
	if pid, bid, eof := itr.Next(); pid != 0 || bid != 3 || eof {
		t.Fatalf("unexpected seek: (%d, %d, %v)", pid, bid, eof)
	} else if pid, bid, eof := itr.Next(); pid != 1 || bid != 0 || eof {
		t.Fatalf("unexpected next: (%d, %d, %v)", pid, bid, eof)
	}

	itr.Unread()
	if pid, bid, eof := itr.Next(); pid != 1 || bid != 0 || eof {
		t.Fatalf("unexpected next(buffered): (%d, %d, %v)", pid, bid, eof)
	}

	if pid, bid, eof := itr.Next(); pid != 2 || bid != 100 || eof {
		t.Fatalf("unexpected next: (%d, %d, %v)", pid, bid, eof)
	} else if _, _, eof := itr.Next(); !eof {
		t.Fatal("expected eof")
	}
}

// Ensure buffered iterator will panic if unreading onto a full buffer.
func TestBufIterator_DoubleFillPanic(t *testing.T) {
	var v interface{}
	func() {
		defer func() { v = recover() }()

		itr := pilosa.NewBufIterator(pilosa.NewSliceIterator(nil, nil))
		itr.Unread()
		itr.Unread()
	}()

	if !reflect.DeepEqual(v, "pilosa.BufIterator: buffer full") {
		t.Fatalf("unexpected panic value: %#v", v)
	}
}
