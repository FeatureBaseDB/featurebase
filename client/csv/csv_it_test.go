// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
//go:build integration
// +build integration

package csv_test

import (
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/client"
	"github.com/molecula/featurebase/v3/client/csv"
)

func TestCSVIterate(t *testing.T) {
	text := `10,7
		10,5
		2,3
		7,1`
	iterator := csv.NewColumnIterator(csv.RowIDColumnID, strings.NewReader(text))
	recs := consumeIterator(t, iterator)
	target := []client.Record{
		client.Column{RowID: 10, ColumnID: 7},
		client.Column{RowID: 10, ColumnID: 5},
		client.Column{RowID: 2, ColumnID: 3},
		client.Column{RowID: 7, ColumnID: 1},
	}
	if !reflect.DeepEqual(target, recs) {
		t.Fatalf("%v != %v", target, recs)
	}
}

func consumeIterator(t *testing.T, it *csv.Iterator) []client.Record {
	recs := []client.Record{}
	for {
		r, err := it.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, r)
	}
	return recs
}
