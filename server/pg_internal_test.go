// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package server

import (
	"testing"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/pg"
	"github.com/stretchr/testify/assert"
)

// pg_internal_test.go tests unexported methods from server/pg.go

// TestQueryResultWriter implements the QueryResultWriter interface for testing
type TestQueryResultWriter struct {
	Header  []pg.ColumnInfo
	RowText []string
	TagTag  string
}

func (t *TestQueryResultWriter) WriteHeader(headers ...pg.ColumnInfo) error {
	t.Header = append(t.Header, headers...)
	return nil
}

func (t *TestQueryResultWriter) WriteRowText(rowTexts ...string) error {
	t.RowText = append(t.RowText, rowTexts...)
	return nil
}

func (t *TestQueryResultWriter) Tag(tag string) {
	t.TagTag = tag
}

func TestPgWriteDistinctTimestamp(t *testing.T) {
	w := TestQueryResultWriter{}
	expected := pilosa.DistinctTimestamp{Name: "test", Values: []string{"date1", "date2", "date3"}}
	err := pgWriteDistinctTimestamp(&w, expected)
	assert.Nil(t, err)

	if w.Header[0].Name != expected.Name {
		t.Fatalf("Header Name is wrong. got %v, want %v", w.Header[0], expected.Name)
	}
	if w.Header[0].Type != pg.TypeCharoid {
		t.Fatalf("Header Type is wrong. got %v, want %v", w.Header[0].Type, pg.TypeCharoid)
	}
	for i, value := range w.RowText {
		if value != expected.Values[i] {
			t.Fatalf("Value not written properly. got %v, want %v", value, expected.Values[i])
		}
	}

}
