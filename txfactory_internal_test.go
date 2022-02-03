// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"testing"
)

func Test_TxFactory_verifyStringConstantsMatch(t *testing.T) {
	// txtype.String() method MUST return strings that match
	// our const definitions at the top of txfactory.go.
	check := []txtype{rbfTxn}
	expect := []string{RBFTxn}
	for i, chk := range check {
		obs := chk.String()
		if obs != expect[i] {
			t.Fatalf("expected '%v' but got '%v'", expect[i], obs)
		}
	}
}
