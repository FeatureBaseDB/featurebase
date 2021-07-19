// Copyright 2020 Pilosa Corp.
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

package test

import (
	"testing"
	"time"

	"github.com/molecula/featurebase/v2"
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
