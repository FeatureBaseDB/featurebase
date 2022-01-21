// Copyright 2021 Molecula Corp. All rights reserved.
package sql2_test

import (
	"testing"

	sql "github.com/molecula/featurebase/v3/sql2"
)

func TestPos_String(t *testing.T) {
	if got, want := (sql.Pos{}).String(), `-`; got != want {
		t.Fatalf("String()=%q, want %q", got, want)
	}
}
