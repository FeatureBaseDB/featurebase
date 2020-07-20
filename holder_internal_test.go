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

package pilosa

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
)

type testHolderOperator struct {
	indexSeen, indexProcessed       int
	fieldSeen, fieldProcessed       int
	viewSeen, viewProcessed         int
	fragmentSeen, fragmentProcessed int
	waitHere                        chan struct{}
}

func (t *testHolderOperator) CheckIndex(string) (bool, bool) {
	t.indexSeen++
	return true, true
}

func (t *testHolderOperator) CheckField(string, string) (bool, bool) {
	t.fieldSeen++
	return true, true
}

func (t *testHolderOperator) CheckView(string, string, string) (bool, bool) {
	t.viewSeen++
	return true, true
}

func (t *testHolderOperator) CheckFragment(string, string, string, uint64) bool {
	t.fragmentSeen++
	return true
}

func (t *testHolderOperator) ProcessIndex(*Index) error {
	t.indexProcessed++
	return nil
}

func (t *testHolderOperator) ProcessField(*Field) error {
	t.fieldProcessed++
	return nil
}

func (t *testHolderOperator) ProcessView(*view) error {
	t.viewProcessed++
	return nil
}

func (t *testHolderOperator) ProcessFragment(*fragment) error {
	if t.waitHere != nil {
		<-t.waitHere
	}
	t.fragmentProcessed++
	return nil
}

func makeHolder() (*Holder, string, error) {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		return nil, "", err
	}
	h := NewHolder(DefaultPartitionN)
	h.Path = path
	return h, path, nil
}

func testSetBit(t *testing.T, h *Holder, index, field string, rowID, columnID uint64) {

	idx, err := h.CreateIndexIfNotExists(index, IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}

	tx, err := h.BeginTx(writable, idx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	f, err := idx.CreateFieldIfNotExists(field, OptFieldTypeDefault())
	if err != nil {
		t.Fatalf("setting bit: %v", err)
	}
	_, err = f.SetBit(tx, rowID, columnID, nil)
	if err != nil {
		t.Fatalf("setting bit: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestHolderOperatorProcess(t *testing.T) {
	h, path, err := makeHolder()
	if err != nil {
		t.Fatalf("creating holder: %v", err)
	}
	defer os.RemoveAll(path)
	defer h.Close()

	// Write bits to separate indexes.
	testSetBit(t, h, "i0", "f", 100, 200)
	testSetBit(t, h, "i1", "f", 100, 200)
	testSetBit(t, h, "i1", "f", 100, 12345678)

	testOp := testHolderOperator{}
	ctx := context.Background()
	err = h.Process(ctx, &testOp)
	if err != nil {
		t.Fatalf("processing holder: %v", err)
	}
	expected := testHolderOperator{
		indexSeen: 2, indexProcessed: 2,
		fieldSeen: 2, fieldProcessed: 2,
		viewSeen: 2, viewProcessed: 2,
		fragmentSeen: 3, fragmentProcessed: 3,
	}
	if testOp != expected {
		t.Fatalf("holder processor did not process as expected. expected %#v, got %#v", expected, testOp)
	}
}

func TestHolderOperatorCancel(t *testing.T) {
	h, path, err := makeHolder()
	if err != nil {
		t.Fatalf("creating holder: %v", err)
	}
	defer os.RemoveAll(path)
	defer h.Close()

	// Write bits to separate indexes.
	testSetBit(t, h, "i0", "f", 100, 200)
	testSetBit(t, h, "i1", "f", 100, 200)
	testSetBit(t, h, "i1", "f", 100, 12345678)

	// Here, we want to ensure that the operation gets cancelled
	// successfully. In practice we expect it to process one fragment, then
	// end up blocked on the waitHere, then get cancelled... But the
	// waitHere blockage isn't really something holder.Process can do
	// anything about, so we close the channel, so two fragments are
	// processed. But in theory you could end up with only one fragment
	// processed if this goroutine managed to cancel before the processor
	// gets to the next fragment. Point is, it shouldn't hit all three,
	// because the checks against the cancellation should fire before it
	// gets there.
	testOp := testHolderOperator{waitHere: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		err = h.Process(ctx, &testOp)
		close(done)
	}()
	testOp.waitHere <- struct{}{}
	cancel()
	close(testOp.waitHere)
	<-done
	if err != context.Canceled {
		t.Fatalf("processing holder: expected context.Canceled, got %v", err)
	}
	testOp.waitHere = nil
	expected := testHolderOperator{
		indexSeen: 2, indexProcessed: 2,
		fieldSeen: 2, fieldProcessed: 2,
		viewSeen: 2, viewProcessed: 2,
		fragmentSeen: 3, fragmentProcessed: 3,
	}
	if testOp == expected {
		t.Fatalf("holder processor did not cancel. expected something other than %#v", expected)
	}
}
