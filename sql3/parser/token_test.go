// Copyright 2021 Molecula Corp. All rights reserved.
package parser_test

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

func TestPos_String(t *testing.T) {
	if got, want := (parser.Pos{}).String(), `-`; got != want {
		t.Fatalf("String()=%q, want %q", got, want)
	}
}
