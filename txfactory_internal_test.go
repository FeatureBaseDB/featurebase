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
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
)

func Test_TxFactory_Qcx_query_context(t *testing.T) {
	src := CurrentBackend()
	if src == "rbf" || src == "bolt" {
		// ok
	} else {
		t.Skip("this test only for rbf and bolt")
	}

	shard := uint64(0)
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, shard, "")
	defer f.Clean(t)
	tx.Rollback()

	barrier := NewBarrier()
	defer barrier.Close()

	done := make(chan bool)

	setter := func(k int) {
		for i := 0; ; i++ {
			barrier.WaitAtGate(0)
			select {
			case <-done:
				return
			default:
			}
			// add to the group txn on the txf.
			qcx := idx.holder.txf.NewQcx()

			tx, finisher, err := qcx.GetTx(Txo{Write: true, Index: idx, Shard: f.shard})
			PanicOn(err)

			// Set bits on the fragment.
			if _, err := f.setBit(tx, 120, 1); err != nil {
				panic(err)
			} else if _, err := f.setBit(tx, 120, 6); err != nil {
				panic(err)
			} else if _, err := f.setBit(tx, 121, 0); err != nil {
				panic(err)
			}
			// should have two containers set in the fragment.

			// Verify counts on rows.
			if n := f.mustRow(tx, 120).Count(); n != 2 {
				panic(fmt.Sprintf("unexpected count: %d", n))
			} else if n := f.mustRow(tx, 121).Count(); n != 1 {
				panic(fmt.Sprintf("unexpected count: %d", n))
			}
			finisher(nil) // hit the write tx.Commit path
			// commit the change, and verify it is still there
			PanicOn(qcx.Finish())
			qcx.Reset()

			tx, finread, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
			PanicOn(err)
			if n := f.mustRow(tx, 120).Count(); n != 2 {
				panic(fmt.Sprintf("unexpected count (reopen): %d", n))
			} else if n := f.mustRow(tx, 121).Count(); n != 1 {
				panic(fmt.Sprintf("unexpected count (reopen): %d", n))
			}
			finread(nil) // no-op on reads that are in a group, so must qcx.Abort() to stop them.
			qcx.Abort()
			qcx.Reset()
		}
	}
	N := 1000
	for i := 0; i < N; i++ {
		go setter(i)
	}
	time.Sleep(time.Second * 1)
	close(done)

	// allow all goro to finish before Closing the lmdb.env, otherwise
	// we will crash as the goroutines making Tx will try to use the env
	// after it is closed. It can take quite a while.
	// one writer might be blocking the other... so ask for only N-2 at first
	// to avoid deadlock.
	barrier.BlockUntil(N - 2)
	barrier.UnblockReaders()
	time.Sleep(1 * time.Second)
}

// test TxFactory.green2blue
//
// blue_green starting with an empty or full blue database
//  should copy all of green (if blue is empty); or if blue is ull,
//  verify that blue has all the same bits as green.
//
// Benefits: a) we start with known identical state so our testing/comparisons can be valid;
//       and b) we have an easy migration mechanism, to go from one storage format to another.
//
func Test_TxFactory_UpdateBlueFromGreen_OnStartup(t *testing.T) {
	checked := []string{"roaring", "rbf"}

	expectError := false
	for _, blue := range checked {
		for _, green := range checked {
			if blue == green {
				continue
			}
			if blue == "roaring" {
				// not supported
				expectError = true
			} else {
				expectError = false
			}
			blue_green := blue + "_" + green
			//vv("setting blue_green to '%v'", blue_green)

			// =============================
			// Begin setup.
			//
			// Setup happens with green only.

			h, path, err := makeHolder(t, green)
			if err != nil {
				t.Fatalf("creating holder: %v", err)
			}
			defer os.RemoveAll(path)
			//vv("path = %v", path)

			// we will manually h.Close() below

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

			// verify data is there
			rowID := uint64(100)
			colID := uint64(200)
			_, _ = rowID, colID
			testMustHaveBit(t, h, "i0", "f", rowID, colID)
			testMustHaveBit(t, h, "i1", "f", 100, 200)
			testMustHaveBit(t, h, "i1", "f", 100, 12345678)

			//vv("about to reopen; blue_green = '%v' but PILOSA_STORAGE_BACKEND='%v'", blue_green, os.Getenv("PILOSA_STORAGE_BACKEND"))
			//h.DumpAllShards()

			//vv("after dump, about to close")
			h.Close()

			//vv("after close, about to re-open")

			// can we re.Open the same holder h? hopefully without a problem.
			PanicOn(h.Open())

			//vv("h.Open() re-open worked; blue_green = '%v'; dump; with PILOSA_STORAGE_BACKEND='%v'", blue_green, os.Getenv("PILOSA_STORAGE_BACKEND"))
			//h.DumpAllShards()

			testMustHaveBit(t, h, "i0", "f", rowID, colID) // panic here, colID 200 bit was cold.
			testMustHaveBit(t, h, "i1", "f", 100, 200)
			testMustHaveBit(t, h, "i1", "f", 100, 12345678)
			h.Close()

			//vv("successful re-open and then Close again of h.")

			// check that we can open a NewHolder on green, on same path, and still see our bits.
			// Because the NewHolder is the code that creates and configures TxFactory as blue_green.
			cfg := mustHolderConfig()
			cfg.StorageConfig.Backend = green
			h2 := NewHolder(path, cfg)
			PanicOn(h2.Open())

			testMustHaveBit(t, h2, "i0", "f", rowID, colID)
			testMustHaveBit(t, h2, "i1", "f", 100, 200)
			testMustHaveBit(t, h2, "i1", "f", 100, 12345678)
			h2.Close()

			// verify that blue does not have it.
			// open a new holder on path, just looking at blue.
			cfg = mustHolderConfig()
			cfg.StorageConfig.Backend = blue
			h3 := NewHolder(path, cfg)
			PanicOn(h3.Open())

			testMustNotHaveBit(t, h3, "i0", "f", rowID, colID)
			testMustNotHaveBit(t, h3, "i1", "f", 100, 200)
			testMustNotHaveBit(t, h3, "i1", "f", 100, 12345678)

			h3.Close()

			// =============================
			// Setup done. On to actual test.

			// Opening in blue_green mode means that once Holder.Open()
			// returns without error, the blue and green databases are
			// identical.
			// Since blue is empty, the blue database will get synched up
			// with the green during Holder.Open().

			// open a holder with path again, now looking at both blue and green.
			// The Holder.Open should do the migration from green, populating blue.
			cfg = mustHolderConfig()
			cfg.StorageConfig.Backend = blue_green
			h4 := NewHolder(path, cfg)

			//vv("about to h4.Open we should populate blue from green")
			err = h4.Open()
			if expectError {
				if err == nil {
					panic("expected error since migration to roaring not supported")
				}
			} else {
				PanicOn(err)
			}

			testMustHaveBit(t, h4, "i0", "f", rowID, colID)
			testMustHaveBit(t, h4, "i1", "f", 100, 200)
			testMustHaveBit(t, h4, "i1", "f", 100, 12345678)

			//vv("successfully verified populatingBlueFromGreen with blue_green = '%v'", blue_green)
			h4.Close()
			os.RemoveAll(path)
		}
	}
}

// test the situation where we startup blue_green with existing data and
// go to verify it but blue has more data than green.
// That will also cause query divergence.
func Test_TxFactory_verifyBlueEqualsGreen(t *testing.T) {
	checked := []string{"roaring", "bolt", "rbf"}

	for _, blue := range checked {
		for _, green := range checked {
			if blue == green {
				continue
			}
			if blue == "roaring" {
				// not supported
				continue
			}
			blue_green := blue + "_" + green

			// =============================
			// Begin setup.
			//
			// Setup happens with green only.

			h, path, err := makeHolder(t, green)
			if err != nil {
				t.Fatalf("creating holder: %v", err)
			}
			defer os.RemoveAll(path)

			//vv("on green, which is '%v'", green)
			// we will manually h.Close() below

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

			// verify data is there
			rowID := uint64(100)
			colID := uint64(200)
			_, _ = rowID, colID
			testMustHaveBit(t, h, "i0", "f", rowID, colID)
			testMustHaveBit(t, h, "i1", "f", 100, 200)
			testMustHaveBit(t, h, "i1", "f", 100, 12345678)

			h.Close()

			// verify that blue does not have it.
			// open a new holder on path, just looking at blue.

			//vv("on blue, which is '%v'", blue)

			cfg := mustHolderConfig()
			cfg.StorageConfig.Backend = blue
			h3 := NewHolder(path, cfg)
			PanicOn(h3.Open())

			testMustNotHaveBit(t, h3, "i0", "f", rowID, colID)
			testMustNotHaveBit(t, h3, "i1", "f", 100, 200)
			testMustNotHaveBit(t, h3, "i1", "f", 100, 12345678)

			h3.Close()

			// =============================
			// Setup done. On to actual test.

			// Opening in blue_green mode means that once Holder.Open()
			// returns without error, the blue and green databases are
			// identical.
			// Since blue is empty, the blue database will get synched up
			// with the green during Holder.Open().

			//vv("on blue_green, which is '%v'", blue_green)

			// open a holder with path again, now looking at both blue and green.
			// The Holder.Open should do the migration from green, populating blue.
			cfg = mustHolderConfig()
			cfg.StorageConfig.Backend = blue_green
			h4 := NewHolder(path, cfg)
			PanicOn(h4.Open())

			testMustHaveBit(t, h4, "i0", "f", rowID, colID)
			testMustHaveBit(t, h4, "i1", "f", 100, 200)
			testMustHaveBit(t, h4, "i1", "f", 100, 12345678)
			h4.Close()

			// now open just blue, and add a bit to a new index, i2.
			//vv("on blue, which is '%v'", blue)
			cfg = mustHolderConfig()
			cfg.StorageConfig.Backend = blue
			h5 := NewHolder(path, cfg)
			PanicOn(h5.Open())
			testSetBit(t, h5, "i2", "f", 500, 777)

			//vv("after adding a bit to blue, we have:")
			//h5.DumpAllShards()

			h5.Close()

			// now open blue_green. should get a verification failure
			// due to the extra bit in blue.

			// BEGIN verficiation that should ERROR out b/c blue has more data.

			// open a holder with path again, now looking at both blue and green.
			// The Holder.Open should verify blue against green and notice the extra bit.
			cfg = mustHolderConfig()
			cfg.StorageConfig.Backend = blue_green
			h6 := NewHolder(path, cfg)
			err = h6.Open()
			//h6.DumpAllShards()

			if err == nil {
				h6.Close()
				t.Fatalf("should have had blue-green verification fail on Holder.Open")
			}

			h6.Close()
		}
	}
}

func Test_TxFactory_verifyStringConstantsMatch(t *testing.T) {
	// txtype.String() method MUST return strings that match
	// our const definitions at the top of txfactory.go, or
	// else blue-green transactions cannot determine when
	// the second transaction is being released in dbshard.go.
	check := []txtype{roaringTxn, rbfTxn, boltTxn}
	expect := []string{RoaringTxn, RBFTxn, BoltTxn}
	for i, chk := range check {
		obs := chk.String()
		if obs != expect[i] {
			t.Fatalf("expected '%v' but got '%v'", expect[i], obs)
		}
	}
}
