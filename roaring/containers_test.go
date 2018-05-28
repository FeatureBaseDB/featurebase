// Copyright (C) 2017-2018 Pilosa Corp. All rights reserved.
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

package roaring

import (
	"testing"
)

func TestContainersIterator(t *testing.T) {
	slc := NewFileBitmap().Containers
	testContainersIterator(slc, t)
}

func testContainersIterator(cs Containers, t *testing.T) {
	itr, found := cs.Iterator(0)
	if found {
		t.Fatalf("shouldn't have found 0 in empty btc")
	}
	if itr.Next() {
		t.Fatal("Next() should be false for empty btc")
	}

	cs.Put(1, &Container{n: 1})
	cs.Put(2, &Container{n: 2})

	itr, found = cs.Iterator(0)
	if found {
		t.Fatalf("shouldn't have found 0")
	}

	if !itr.Next() {
		t.Fatalf("one should be next, but got false")
	}
	if key, val := itr.Value(); key != 1 || val.n != 1 {
		t.Fatalf("Wrong k/v, exp: 1,1 got: %v,%v", key, val.n)
	}
	if !itr.Next() {
		t.Fatalf("two should be next, but got false")
	}
	if key, val := itr.Value(); key != 2 || val.n != 2 {
		t.Fatalf("Wrong k/v, exp: 2,2 got: %v,%v", key, val.n)
	}

	if itr.Next() {
		t.Fatalf("itr should be done, but got true")
	}

	cs.Put(3, &Container{n: 3})
	cs.Put(5, &Container{n: 5})
	cs.Put(6, &Container{n: 6})

	itr, found = cs.Iterator(3)
	if !itr.Next() {
		t.Fatalf("3 should be next, but got false")
	}
	if !found {
		t.Fatalf("should have found 3")
	}
	if key, val := itr.Value(); key != 3 || val.n != 3 {
		t.Fatalf("Wrong k/v, exp: 3,3 got: %v,%v", key, val.n)
	}
	if !itr.Next() {
		t.Fatalf("5 should be next, but got false")
	}
	if key, val := itr.Value(); key != 5 || val.n != 5 {
		t.Fatalf("Wrong k/v, exp: 5,5 got: %v,%v", key, val.n)
	}

	itr, found = cs.Iterator(4)
	if found {
		t.Fatalf("shouldn't have found 4")
	}
	if !itr.Next() {
		t.Fatalf("5 should be next, but got false")
	}
	if key, val := itr.Value(); key != 5 || val.n != 5 {
		t.Fatalf("Wrong k/v, exp: 5,5 got: %v,%v", key, val.n)
	}
	if !itr.Next() {
		t.Fatalf("6 should be next, but got false")
	}
	if key, val := itr.Value(); key != 6 || val.n != 6 {
		t.Fatalf("Wrong k/v, exp: 6,6 got: %v,%v", key, val.n)
	}

	if itr.Next() {
		t.Fatalf("itr should be done, but got true")
	}

}
