// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package client_test

import (
	"testing"

	"github.com/pilosa/pilosa/v2/client"
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
