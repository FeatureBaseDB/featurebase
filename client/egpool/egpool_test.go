// Copyright 2017 Pilosa Corp.
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

package egpool_test

import (
	"errors"
	"testing"

	"github.com/molecula/featurebase/v2/client/egpool"
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
