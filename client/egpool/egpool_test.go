// Copyright 2021 Molecula Corp. All rights reserved.
package egpool_test

import (
	"errors"
	"testing"

	"github.com/molecula/featurebase/v3/client/egpool"
)

func TestEGPool(t *testing.T) {
	eg := egpool.Group{}

	a := make([]int, 10)

	for i := 0; i < 10; i++ {
		i := i
		eg.Go(func() error {
			a[i] = i
			if i == 7 {
				return errors.New("blah")
			}
			return nil
		})
	}

	err := eg.Wait()
	if err == nil || err.Error() != "blah" {
		t.Errorf("expected err blah, got: %v", err)
	}

	for i := 0; i < 10; i++ {
		if a[i] != i {
			t.Errorf("expected a[%d] to be %d, but is %d", i, i, a[i])
		}
	}
}
