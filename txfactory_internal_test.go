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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/glycerine/lmdb-go/lmdb"
)

func Test_TxFactory_Qcx_query_context(t *testing.T) {
	src := os.Getenv("PILOSA_TXSRC")
	if src == "rbf" || src == "lmdb" {
		// ok
	} else {
		t.Skip("this test only for lmdb and rbf")
	}

	shard := uint64(0)
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, shard, "")
	defer f.Clean(t)
	tx.Rollback()

	barrier := lmdb.NewBarrier()
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
			qcx := idx.Txf.NewQcx()

			tx, finisher := qcx.GetTx(Txo{Write: true, Index: idx, Shard: f.shard})

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
			panicOn(qcx.Finish())

			tx, finread := qcx.GetTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
			if n := f.mustRow(tx, 120).Count(); n != 2 {
				panic(fmt.Sprintf("unexpected count (reopen): %d", n))
			} else if n := f.mustRow(tx, 121).Count(); n != 1 {
				panic(fmt.Sprintf("unexpected count (reopen): %d", n))
			}
			finread(nil) // no-op on reads that are in a group, so must qcx.Abort() to stop them.
			qcx.Abort()
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
	// one writer might be blocking the other... so ask for only N-1 at first.
	//barrier.BlockUntil(N - 1)
	barrier.BlockUntil(N)
	//barrier.UnblockReaders()
	//time.Sleep(1 * time.Second)
}
