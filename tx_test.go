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

package pilosa_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/test"
)

func queryIRABit(m0api *pilosa.API, acctOwnerID uint64, iraField string, iraRowID uint64, index string) (bit bool) {
	query := fmt.Sprintf("Row(%v=%v)", iraField, iraRowID) // acctOwnerID)
	res, err := m0api.Query(context.Background(), &pilosa.QueryRequest{Index: index, Query: query})
	panicOn(err)
	cols := res.Results[0].(*pilosa.Row).Columns()
	for i := range cols {
		if cols[i] == acctOwnerID {
			return true
		}
	}
	return false
}

func mustQueryAcct(m0api *pilosa.API, acctOwnerID uint64, fieldAcct0, index string) (acctBal int64) {
	query := fmt.Sprintf("FieldValue(field=%v, column=%v)", fieldAcct0, acctOwnerID)
	res, err := m0api.Query(context.Background(), &pilosa.QueryRequest{Index: index, Query: query})
	panicOn(err)

	if len(res.Results) == 0 {
		return 0
	}
	valCount := res.Results[0].(pilosa.ValCount)
	return valCount.Val
}

func queryBalances(m0api *pilosa.API, acctOwnerID uint64, fldAcct0, fldAcct1, index string) (acct0bal, acct1bal int64) {

	acct0bal = mustQueryAcct(m0api, acctOwnerID, fldAcct0, index)
	acct1bal = mustQueryAcct(m0api, acctOwnerID, fldAcct1, index)
	return
}

func skipForRoaring(t *testing.T) {
	src := os.Getenv("PILOSA_TXSRC")
	// once txfactory.go DefaultTxsrc != RoaringTxn, this
	// will break, of course. Take out the src == "" below.
	if (src == "" && pilosa.DefaultTxsrc == pilosa.RoaringTxn) || strings.Contains(src, "roaring") {
		t.Skip("skip if roaring pseudo-txn involved -- won't show transactional rollback")
	}
}

func TestAPI_ImportAtomicRecord(t *testing.T) {
	skipForRoaring(t)
	c := test.MustRunCluster(t, 1,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
	)
	defer c.Close()

	m0 := c.GetNode(0)
	m0api := m0.API

	ctx := context.Background()
	index := "i"

	fieldAcct0 := "acct0"
	fieldAcct1 := "acct1"

	transferUSD := int64(100)
	_ = transferUSD
	opts := pilosa.OptFieldTypeInt(-1000, 1000)

	_, err := m0api.CreateIndex(ctx, index, pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = m0api.CreateField(ctx, index, fieldAcct0, opts)
	if err != nil {
		t.Fatalf("creating fieldAcct0: %v", err)
	}
	_, err = m0api.CreateField(ctx, index, fieldAcct1, opts)
	if err != nil {
		t.Fatalf("creating fieldAcct1: %v", err)
	}

	iraField := "ira" // set field.
	iraRowID := uint64(3)
	_, err = m0api.CreateField(ctx, index, iraField)
	if err != nil {
		t.Fatalf("creating fieldIRA: %v", err)
	}

	acctOwnerID := uint64(78) // ColumnID
	shard := acctOwnerID / ShardWidth

	// setup 500 USD in acct1 and 700 USD in acct2.
	// transfer 100 USD.
	// should see 400 USD in acct, and 800 USD in acct2.
	//

	// setup initial balances

	createAIRUpdate := func(acct0bal, acct1bal int64) (air *pilosa.AtomicRecord) {
		ivr0 := &pilosa.ImportValueRequest{
			Index:     index,
			Field:     fieldAcct0,
			Shard:     shard,
			ColumnIDs: []uint64{acctOwnerID},
			Values:    []int64{acct0bal},
		}
		ivr1 := &pilosa.ImportValueRequest{
			Index:     index,
			Field:     fieldAcct1,
			Shard:     shard,
			ColumnIDs: []uint64{acctOwnerID},
			Values:    []int64{acct1bal},
		}

		ir0 := &pilosa.ImportRequest{
			Index:     index,
			Field:     iraField,
			Shard:     shard,
			ColumnIDs: []uint64{acctOwnerID},
			RowIDs:    []uint64{iraRowID},
		}

		air = &pilosa.AtomicRecord{
			Index: index,
			Shard: shard,
			Ivr: []*pilosa.ImportValueRequest{
				ivr0, ivr1,
			},
			Ir: []*pilosa.ImportRequest{ir0},
		}
		return
	}

	expectedBalStartingAcct0 := int64(500)
	expectedBalStartingAcct1 := int64(700)

	air := createAIRUpdate(expectedBalStartingAcct0, expectedBalStartingAcct1)

	//vv("BEFORE the first ImportAtomicRecord!")

	if err := m0api.ImportAtomicRecord(ctx, nil, air); err != nil {
		t.Fatal(err)
	}

	//vv("AFTER the first ImportAtomicRecord!")

	iraBit := queryIRABit(m0api, acctOwnerID, iraField, iraRowID, index)
	if !iraBit {
		panic("IRA bit should have been set")
	}

	startingBalanceAcct0, startingBalanceAcct1 := queryBalances(m0api, acctOwnerID, fieldAcct0, fieldAcct1, index)
	//vv("starting balance: acct0=%v,  acct1=%v", startingBalanceAcct0, startingBalanceAcct1)

	if startingBalanceAcct0 != expectedBalStartingAcct0 {
		panic(fmt.Sprintf("expected %v, observed %v starting acct0 balance", expectedBalStartingAcct0, startingBalanceAcct0))
	}
	if startingBalanceAcct1 != expectedBalStartingAcct1 {
		panic(fmt.Sprintf("expected %v, observed %v starting acct1 balance", expectedBalStartingAcct1, startingBalanceAcct1))
	}

	//vv("sad path: transferUSD %v from %v -> %v, with power loss half-way through", transferUSD, fieldAcct0, fieldAcct1)

	opt := func(o *pilosa.ImportOptions) error {
		o.SimPowerLossAfter = 1
		return nil
	}
	expectedBalEndingAcct0 := expectedBalStartingAcct0 - 100
	expectedBalEndingAcct1 := expectedBalStartingAcct1 + 100

	air = createAIRUpdate(expectedBalEndingAcct0, expectedBalEndingAcct1)

	qcx := m0api.Txf().NewQcx()
	//vv("just before the SECOND ImportAtomicRecord, qcx is %p, should NOT BE NIL", qcx)
	err = m0api.ImportAtomicRecord(ctx, qcx, air, opt)
	//err = m0api.ImportAtomicRecord(ctx, nil, air, opt)
	if err != pilosa.ErrAborted {
		panic(fmt.Sprintf("expected ErrTxnAborted but got err='%#v'", err))
	}
	// sad path, cleanup
	qcx.Abort()
	qcx = nil

	b0, b1 := queryBalances(m0api, acctOwnerID, fieldAcct0, fieldAcct1, index)
	//vv("after power failure tx, balance: acct0=%v,  acct1=%v", b0, b1)

	if b0 != expectedBalStartingAcct0 {
		panic(fmt.Sprintf("expected %v, observed %v starting acct0 balance", expectedBalStartingAcct0, b0))
	}
	if b1 != expectedBalStartingAcct1 {
		panic(fmt.Sprintf("expected %v, observed %v starting acct1 balance", expectedBalStartingAcct1, b1))
	}
	//vv("good: with power loss half-way, no change in account balances; acct0=%v; acct1=%v", b0, b1)

	// next part of the test, just make sure we do the update.
	//vv("happy path: transferUSD %v from %v -> %v, with no interruption.", transferUSD, fieldAcct0, fieldAcct1)

	// happy path with no power failure half-way through.

	err = m0api.ImportAtomicRecord(ctx, nil, air)
	panicOn(err)

	eb0, eb1 := queryBalances(m0api, acctOwnerID, fieldAcct0, fieldAcct1, index)

	// should have been applied this time.
	if eb0 != expectedBalEndingAcct0 ||
		eb1 != expectedBalEndingAcct1 {
		panic(fmt.Sprintf("problem: transaction did not get committed/applied. transferUSD=%v, but we see: startingBalanceAcct0=%v -> endingBalanceAcct0=%v; startingBalanceAcct1=%v -> endingBalanceAcct1=%v", transferUSD, startingBalanceAcct0, eb0, startingBalanceAcct1, eb1))
	}
	//vv("ending balance: acct0=%v,  acct1=%v", eb0, eb1)

	// clear all the bits
	air.Ivr[0].Clear = true
	air.Ivr[1].Clear = true
	air.Ir[0].Clear = true

	err = m0api.ImportAtomicRecord(ctx, nil, air)
	panicOn(err)

	eb0, eb1 = queryBalances(m0api, acctOwnerID, fieldAcct0, fieldAcct1, index)
	if eb0 != 0 ||
		eb1 != 0 {
		panic("problem: bits did not clear")
	}
	//vv("cleared balances: acct0=%v,  acct1=%v", eb0, eb1)

	iraBit = queryIRABit(m0api, acctOwnerID, iraField, iraRowID, index)
	if iraBit {
		panic("IRA bit should have been cleared")
	}

}
