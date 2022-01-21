// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/test"
)

// TestTransactionManager currently uses an in memory transaction
// store, but tests a variety of timeouts, and therefore could be
// sensitive to slowness in the implementation. Especially if a store
// were used that actually wrote things to disk.
func TestTransactionManager(t *testing.T) {
	store := pilosa.NewInMemTransactionStore()

	tm := pilosa.NewTransactionManager(store)
	tm.Log = logger.NewBufferLogger()
	ctx := context.Background()

	// can add a non-exclusive transaction
	trns1 := mustStart(t, tm, "a", time.Microsecond, false)
	test.CompareTransactions(t, &pilosa.Transaction{ID: "a", Active: true, Timeout: time.Microsecond, Deadline: time.Now()}, trns1)

	// can have two non exclusive transactions
	trns2 := mustStart(t, tm, "b", time.Microsecond, false)
	test.CompareTransactions(t, &pilosa.Transaction{ID: "b", Active: true, Timeout: time.Microsecond, Deadline: time.Now()}, trns2)

	// trying to start a transaction with same name errors and returns previous transaction
	t3, err := tm.Start(ctx, "a", time.Second, true)
	if err != pilosa.ErrTransactionExists {
		t.Errorf("expected transaction exists, but got: '%v'", err)
	}
	test.CompareTransactions(t, trns1, t3)

	// can get an existing transaction
	trns2_2 := mustGet(t, tm, "b")
	test.CompareTransactions(t, trns2, trns2_2)

	// can list all transactions
	trnsMap := mustList(t, tm)
	if len(trnsMap) != 2 {
		t.Errorf("unexpected number of transactions in map: %d", len(trnsMap))
	}
	test.CompareTransactions(t, trnsMap["a"], trns1)
	test.CompareTransactions(t, trnsMap["b"], trns2)

	// can submit an exclusive transaction
	trnsE := mustStart(t, tm, "ce", 100*time.Millisecond, true)
	test.CompareTransactions(t, &pilosa.Transaction{ID: "ce", Active: false, Exclusive: true, Timeout: 100 * time.Millisecond, Deadline: time.Now().Add(100 * time.Millisecond)}, trnsE)

	// can't start new transactions while an exclusive transaction is pending
	if _, err := tm.Start(ctx, "d", time.Millisecond, false); err != pilosa.ErrTransactionExclusive {
		t.Errorf("unexpected error starting transaction while an exclusive transaction exists: %v", err)
	}

	// can't start new exclusive transactions while an exclusive transaction is pending
	if _, err := tm.Start(ctx, "ee", time.Millisecond, true); err != pilosa.ErrTransactionExclusive {
		t.Errorf("unexpected error starting transaction while an exclusive transaction exists: %v", err)
	}

	// exclusive transaction becomes active after deadlines expire
	for i := 0; true; i++ {
		time.Sleep(time.Millisecond)
		trnsE, err := tm.Get(ctx, "ce")
		if err != nil {
			t.Errorf("error retrieving exclusive transaction: %v", err)
		}
		if trnsE.Active {
			break
		}
		if i > 10000 {
			t.Fatalf("exclusive transaction never became active: %+v", trnsE)
		}
	}

	// can't start new transactions while an exclusive transaction is active
	if _, err := tm.Start(ctx, "f", time.Millisecond, false); err != pilosa.ErrTransactionExclusive {
		t.Errorf("unexpected error starting transaction while an exclusive transaction exists: %v", err)
	}

	// can't start new exclusive transactions while an exclusive transaction is active
	if _, err := tm.Start(ctx, "ge", time.Millisecond, true); err != pilosa.ErrTransactionExclusive {
		t.Errorf("unexpected error starting transaction while an exclusive transaction exists: %v", err)
	}

	// exclusive transaction gets expired after other transactions have attempted to start
	for i := 0; true; i++ {
		time.Sleep(time.Millisecond * 20)
		trnsE, err := tm.Get(ctx, "ce")
		if err == nil {
			if i > 10 {
				t.Fatalf("exclusive transaction didn't expire: %+v", trnsE)
			}
		} else if err != pilosa.ErrTransactionNotFound {
			t.Errorf("unexpected error fetching transaction while waiting for expiration: %v", err)
		} else {
			break // transaction was not found, therefore it expired and we can happily continue
		}
	}

	// can start a new exclusive transaction and it's immediately active
	trnsHE := mustStart(t, tm, "he", time.Hour, true)
	test.CompareTransactions(t, &pilosa.Transaction{ID: "he", Active: true, Exclusive: true, Timeout: time.Hour, Deadline: time.Now().Add(time.Hour)}, trnsHE)

	// can't start new transactions while an exclusive transaction is active
	if _, err := tm.Start(ctx, "i", time.Millisecond, false); err != pilosa.ErrTransactionExclusive {
		t.Errorf("unexpected error starting transaction while an exclusive transaction exists: %v", err)
	}

	// can finish an active exclusive transaction
	trnsHE_finish := mustFinish(t, tm, "he")
	test.CompareTransactions(t, trnsHE, trnsHE_finish)

	// can start normal transaction after finishing exclusive transaction
	trnsJ := mustStart(t, tm, "j", time.Hour, false)
	test.CompareTransactions(t, &pilosa.Transaction{ID: "j", Active: true, Timeout: time.Hour, Deadline: time.Now().Add(time.Hour)}, trnsJ)

	// can finish normal transaction
	trnsJ_finish := mustFinish(t, tm, "j")
	test.CompareTransactions(t, trnsJ, trnsJ_finish)

	// can start normal transaction after finishing normal transaction
	trnsK := mustStart(t, tm, "k", time.Hour, false)
	test.CompareTransactions(t, &pilosa.Transaction{ID: "k", Active: true, Timeout: time.Hour, Deadline: time.Now().Add(time.Hour)}, trnsK)

	// can start new exclusive transaction, but not immediately active
	trnsLE := mustStart(t, tm, "le", time.Hour, true)
	test.CompareTransactions(t, &pilosa.Transaction{ID: "le", Exclusive: true, Timeout: time.Hour, Deadline: time.Now().Add(time.Hour)}, trnsLE)

	// finishing k should activate le
	trnsK_finish := mustFinish(t, tm, "k")
	test.CompareTransactions(t, trnsK, trnsK_finish)
	trnsLE_active := mustGet(t, tm, "le")
	trnsLE.Active = true
	test.CompareTransactions(t, trnsLE, trnsLE_active)

	mustFinish(t, tm, "le")

	// can start normal transaction to test deadline reset
	trnsM := mustStart(t, tm, "m", time.Millisecond*400, false)
	test.CompareTransactions(t, &pilosa.Transaction{ID: "m", Active: true, Timeout: time.Millisecond * 400, Deadline: time.Now().Add(time.Millisecond * 400)}, trnsM)

	// start new exclusive transaction to trigger deadline check
	trnsNE := mustStart(t, tm, "ne", time.Hour, true)
	test.CompareTransactions(t, &pilosa.Transaction{ID: "ne", Exclusive: true, Timeout: time.Hour, Deadline: time.Now().Add(time.Hour)}, trnsNE)

	// sleep for most of the deadline
	time.Sleep(time.Millisecond * 300)

	// reset deadline
	trnsM_reset, err := tm.ResetDeadline(ctx, "m")
	if err != nil {
		t.Errorf("resetting deadline: %v", err)
	}
	trnsM.Deadline = time.Now().Add(time.Millisecond * 400)
	test.CompareTransactions(t, trnsM, trnsM_reset)

	// sleep until past the original deadline
	time.Sleep(time.Millisecond * 200)

	// verify that trnsM still exists
	trnsM_again := mustGet(t, tm, "m")
	test.CompareTransactions(t, trnsM, trnsM_again)

}

func mustStart(t *testing.T, tm *pilosa.TransactionManager, id string, timeout time.Duration, exclusive bool) *pilosa.Transaction {
	t.Helper()
	trns, err := tm.Start(context.Background(), id, timeout, exclusive)
	if err != nil {
		t.Errorf("starting transaction: %v", err)
	}
	return trns
}

func mustFinish(t *testing.T, tm *pilosa.TransactionManager, id string) *pilosa.Transaction {
	t.Helper()
	trns, err := tm.Finish(context.Background(), id)
	if err != nil {
		t.Errorf("finishing transaction: %v", err)
	}
	return trns
}

func mustGet(t *testing.T, tm *pilosa.TransactionManager, id string) *pilosa.Transaction {
	t.Helper()
	trns, err := tm.Get(context.Background(), id)
	if err != nil {
		t.Errorf("getting transaction %s: %v", id, err)
	}
	return trns
}

func mustList(t *testing.T, tm *pilosa.TransactionManager) map[string]*pilosa.Transaction {
	t.Helper()
	trnsMap, err := tm.List(context.Background())
	if err != nil {
		t.Errorf("getting transaction list: %v", err)
	}
	return trnsMap
}

func TestInMemTransactionStore(t *testing.T) {
	ims := pilosa.NewInMemTransactionStore()

	err := ims.Put(&pilosa.Transaction{ID: "blah", Timeout: time.Second})
	if err != nil {
		t.Fatalf("adding blah: %v", err)
	}

	trns, err := ims.Get("blah")
	if err != nil {
		t.Fatalf("getting blah: %v", err)
	}
	if trns.ID != "blah" || trns.Timeout != time.Second {
		t.Fatalf("unexpected transaction for blah: %+v", t)
	}

	_, err = ims.Get("nope")
	if err != pilosa.ErrTransactionNotFound {
		t.Fatalf("unexpected error: %v", err)
	}

	l, err := ims.List()
	if err != nil {
		t.Fatalf("listing transactions: %v", err)
	}
	if len(l) != 1 {
		t.Errorf("unexpected number of transactions: %d", len(l))
	}
	if l["blah"].ID != "blah" || l["blah"].Timeout != time.Second {
		t.Errorf("unexpected transaction at blah: %+v", l["blah"])
	}

}

func TestMarshalUnmarshalTransaction(t *testing.T) {
	tests := []struct {
		name        string
		transaction *pilosa.Transaction
	}{
		{
			name:        "empty",
			transaction: &pilosa.Transaction{},
		},
		{
			name: "basic",
			transaction: &pilosa.Transaction{
				ID:        "blah",
				Active:    true,
				Exclusive: true,
				Timeout:   time.Minute,
				Deadline:  time.Now(),
			},
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			bytes, err := json.Marshal(tst.transaction)
			if err != nil {
				t.Errorf("marshalling: %v", err)
			}

			nt := &pilosa.Transaction{}
			err = json.Unmarshal(bytes, nt)
			if err != nil {
				t.Fatalf("unmarshalling: %v", err)
			}

			test.CompareTransactions(t, tst.transaction, nt)
		})
	}
}

func TestUnmarshalTransaction(t *testing.T) {
	tests := []struct {
		name            string
		transactionJSON string
		exp             *pilosa.Transaction
	}{
		{
			name:            "empty",
			transactionJSON: `{}`,
			exp:             &pilosa.Transaction{},
		},
		{
			name:            "basicPost",
			transactionJSON: `{"id": "blah", "exclusive": false, "timeout": "1m"}`,
			exp:             &pilosa.Transaction{ID: "blah", Timeout: time.Minute},
		},
		{
			name:            "basicPostFloatTimeout",
			transactionJSON: `{"id": "blah", "exclusive": false, "timeout": 10.5}`,
			exp:             &pilosa.Transaction{ID: "blah", Timeout: time.Second*10 + time.Second/2},
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			nt := &pilosa.Transaction{}
			err := json.Unmarshal([]byte(tst.transactionJSON), nt)
			if err != nil {
				t.Fatalf("unmarshalling: %v", err)
			}

			test.CompareTransactions(t, tst.exp, nt)
		})
	}
}
