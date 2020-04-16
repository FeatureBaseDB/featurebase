package test

import (
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
)

// CompareTransactions errors describing how the
// transactions differ (if at all). The deadlines need only be close
// (within 3ms).
func CompareTransactions(t *testing.T, trns1, trns2 pilosa.Transaction) {
	t.Helper()
	if trns1.ID != trns2.ID {
		t.Errorf("IDs differ:\n%+v\n%+v", trns1, trns2)
	}
	if trns1.Active != trns2.Active {
		t.Errorf("Actives differ:\n%+v\n%+v", trns1, trns2)
	}
	if trns1.Exclusive != trns2.Exclusive {
		t.Errorf("Exclusives differ:\n%+v\n%+v", trns1, trns2)
	}
	if trns1.Timeout != trns2.Timeout {
		t.Errorf("Timeouts differ:\n%+v\n%+v", trns1, trns2)
	}

	diff := trns1.Deadline.Sub(trns2.Deadline)

	if diff > time.Millisecond*3 || diff < time.Millisecond*-3 {
		t.Errorf("Deadlines differ by %v:\n%+v\n%+v", diff, trns1, trns2)
	}
	if trns1.Stats != trns2.Stats {
		t.Errorf("Stats differ:\n%+v\n%+v", trns1, trns2)
	}
}
