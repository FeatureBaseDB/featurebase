// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package querycontext

import (
	"context"
	"os"
	"testing"
)

func TestDot(t *testing.T) {
	// txs, err := newRbfTxStore("foo", &flexibleKeySplitter{splitIndexes: map[string]struct{}{"i": {}}})
	txs, err := newRbfTxStore("/data/foo", nil)
	if err != nil {
		t.Fatalf("creating tx store: %v", err)
	}
	file, err := os.Create("test.dot")
	if err != nil {
		t.Fatalf("creating file: %v", err)
	}
	defer file.Close()
	scope := txs.Scope()
	scope.AddIndex("i")
	q, err := txs.NewWriteQueryContext(context.Background(), scope)
	if err != nil {
		t.Fatalf("creating query context: %v", err)
	}
	defer q.Release()
	_, err = q.NewWrite("i", "f", "v", 0)
	if err != nil {
		t.Fatalf("creating write: %v", err)
	}
	_, err = q.NewWrite("i", "g", "v", 0)
	if err != nil {
		t.Fatalf("creating write: %v", err)
	}
	_, err = q.NewWrite("i", "f", "v", 1)
	if err != nil {
		t.Fatalf("creating write: %v", err)
	}
	// read, but it's in a writable thing, so still creates rbfQueryWrite
	_, err = q.NewRead("i", "f", "v", 2)
	if err != nil {
		t.Fatalf("creating read: %v", err)
	}
	_, err = q.NewRead("j", "f", "v", 0)
	if err != nil {
		t.Fatalf("creating read: %v", err)
	}
	q2, err := txs.NewQueryContext(context.Background())
	if err != nil {
		t.Fatalf("creating query context: %v", err)
	}
	_, err = q2.NewRead("i", "f", "v", 0)
	if err != nil {
		t.Fatalf("creating write: %v", err)
	}
	_, err = q2.NewRead("i", "g", "v", 0)
	if err != nil {
		t.Fatalf("creating write: %v", err)
	}
	var dg dotGraph
	dg.enqueue(txs)
	dg.build(5)
	err = dg.Write(file)
	if err != nil {
		t.Fatalf("writing dot: %v", err)
	}
}
