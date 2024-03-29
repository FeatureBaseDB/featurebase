// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package test

import (
	"testing"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
)

const deadlineSkew = time.Second

// CompareTransactions errors describing how the
// transactions differ (if at all). The deadlines need only be close
// (within deadlineSkew).
func CompareTransactions(t *testing.T, trns1, trns2 *pilosa.Transaction) {
	t.Helper()
	if err := pilosa.CompareTransactions(trns1, trns2); err != nil {
		t.Errorf("%v", err)
	}
	if trns1 == nil || trns2 == nil {
		return
	}

	diff := trns1.Deadline.Sub(trns2.Deadline)

	if diff > deadlineSkew || diff < -deadlineSkew {
		t.Errorf("Deadlines differ by %v:\n%+v\n%+v", diff, trns1, trns2)
	}
	if trns1.Stats != trns2.Stats {
		t.Errorf("Stats differ:\n%+v\n%+v", trns1, trns2)
	}
}
