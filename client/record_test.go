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

// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client_test

import (
	"testing"

	"github.com/molecula/featurebase/v2/client"
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
