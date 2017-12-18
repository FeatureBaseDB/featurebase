package roaring

import (
	"testing"
)

func TestContainersIterator(t *testing.T) {
	slc := NewSkipListContainers()
	itr, found := slc.Iterator(0)
	if found {
		t.Fatalf("shouldn't have found 0 in empty slc")
	}
	if itr.Next() {
		t.Fatal("Next() should be false for empty slc")
	}

	slc.Put(1, &container{n: 1})
	slc.Put(2, &container{n: 2})

	itr, found = slc.Iterator(0)
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

	slc.Put(3, &container{n: 3})
	slc.Put(5, &container{n: 5})
	slc.Put(6, &container{n: 6})

	itr, found = slc.Iterator(3)
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

	itr, found = slc.Iterator(4)
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
