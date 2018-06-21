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

package pilosa_test

import (
	"context"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

// TestMonitorAntiEntropy is a regression test which which caught a bug where
// pilosa.Server was not having its remoteClient field set by an option and so
// it was using a nil client in monitorAntiEntropy.
func TestMonitorAntiEntropy(t *testing.T) {
	cluster := test.MustRunMainWithCluster(t, 3, test.OptAntiEntropyInterval(time.Millisecond*20))
	client := cluster[1].Client()
	err := client.CreateIndex(context.Background(), "balh", pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	err = client.CreateField(context.Background(), "balh", "fralh", pilosa.FieldOptions{})
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}

	time.Sleep(time.Millisecond * 40)
	for _, m := range cluster {
		err := m.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

}
