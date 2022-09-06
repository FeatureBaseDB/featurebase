// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client_test

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/client"
)

func TestColumnShard(t *testing.T) {
	a := client.Column{RowID: 15, ColumnID: 55, Timestamp: 100101}
	target := uint64(0)
	if a.Shard(100) != target {
		t.Fatalf("shard %d != %d", target, a.Shard(100))
	}
	target = 5
	if a.Shard(10) != target {
		t.Fatalf("shard %d != %d", target, a.Shard(10))
	}
}

func TestColumnLess(t *testing.T) {
	a := client.Column{RowID: 10, ColumnID: 200}
	a2 := client.Column{RowID: 10, ColumnID: 1000}
	b := client.Column{RowID: 200, ColumnID: 10}
	c := client.FieldValue{ColumnID: 1}
	if !a.Less(a2) {
		t.Fatalf("%v should be less than %v", a, a2)
	}
	if !a.Less(b) {
		t.Fatalf("%v should be less than %v", a, b)
	}
	if b.Less(a) {
		t.Fatalf("%v should not be less than %v", b, a)
	}
	if c.Less(a) {
		t.Fatalf("%v should not be less than %v", c, a)
	}
}

func TestFieldValueShard(t *testing.T) {
	a := client.FieldValue{ColumnID: 55, Value: 125}
	target := uint64(0)
	if a.Shard(100) != target {
		t.Fatalf("shard %d != %d", target, a.Shard(100))
	}
	target = 5
	if a.Shard(10) != target {
		t.Fatalf("shard %d != %d", target, a.Shard(10))
	}

}

func TestFieldValueLess(t *testing.T) {
	a := client.FieldValue{ColumnID: 55, Value: 125}
	b := client.FieldValue{ColumnID: 100, Value: 125}
	c := client.Column{ColumnID: 1, RowID: 2}
	if !a.Less(b) {
		t.Fatalf("%v should be less than %v", a, b)
	}
	if b.Less(a) {
		t.Fatalf("%v should not be less than %v", b, a)
	}
	if c.Less(a) {
		t.Fatalf("%v should not be less than %v", c, a)
	}
}
