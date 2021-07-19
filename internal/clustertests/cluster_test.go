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

package clustertest

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/disco"
	picli "github.com/molecula/featurebase/v2/http"
)

func TestClusterStuff(t *testing.T) {
	if os.Getenv("ENABLE_PILOSA_CLUSTER_TESTS") != "1" {
		t.Skip("pilosa cluster tests are not enabled")
	}
	cli1, err := picli.NewInternalClient("pilosa1:10101", picli.GetHTTPClient(nil))
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	cli2, err := picli.NewInternalClient("pilosa2:10101", picli.GetHTTPClient(nil))
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	cli3, err := picli.NewInternalClient("pilosa3:10101", picli.GetHTTPClient(nil))
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}

	t.Run("long pause", func(t *testing.T) {
		err := cli1.CreateIndex(context.Background(), "testidx", pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		err = cli1.CreateFieldWithOptions(context.Background(), "testidx", "testf", pilosa.FieldOptions{CacheType: pilosa.CacheTypeRanked, CacheSize: 100})
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		data := make([]pilosa.Bit, 10)
		for i := 0; i < 1000; i++ {
			data[i%10].RowID = 0
			data[i%10].ColumnID = uint64((i/10)*pilosa.ShardWidth + i%10)
			shard := uint64(i / 10)
			if i%10 == 9 {
				err = cli1.Import(context.Background(), "testidx", "testf", shard, data)
				if err != nil {
					t.Fatalf("importing: %v", err)
				}
			}
		}

		// Check query results from each node.
		for i, cli := range []*picli.InternalClient{cli1, cli2, cli3} {
			r, err := cli.Query(context.Background(), "testidx", &pilosa.QueryRequest{Index: "testidx", Query: "Count(Row(testf=0))"})
			if err != nil {
				t.Fatalf("count querying pilosa%d: %v", i, err)
			}
			if r.Results[0].(uint64) != 1000 {
				t.Fatalf("count on pilosa%d after import is %d", i, r.Results[0].(uint64))
			}
		}

		pcmd := exec.Command("/pumba", "pause", "clustertests_pilosa3_1", "--duration", "10s")
		pcmd.Stdout = os.Stdout
		pcmd.Stderr = os.Stderr
		t.Log("pausing pilosa3 for 10s")
		err = pcmd.Start()
		if err != nil {
			t.Fatalf("starting pumba command: %v", err)
		}
		err = pcmd.Wait()
		if err != nil {
			t.Fatalf("waiting on pumba pause cmd: %v", err)
		}

		t.Log("done with pause, waiting for stability")
		waitForStatus(t, cli1.Status, string(disco.ClusterStateNormal), 30, time.Second)
		t.Log("done waiting for stability")

		// Check query results from each node.
		for i, cli := range []*picli.InternalClient{cli1, cli2, cli3} {
			r, err := cli.Query(context.Background(), "testidx", &pilosa.QueryRequest{Index: "testidx", Query: "Count(Row(testf=0))"})
			if err != nil {
				t.Fatalf("count querying pilosa%d: %v", i, err)
			}
			if r.Results[0].(uint64) != 1000 {
				t.Fatalf("count on pilosa%d after import is %d", i, r.Results[0].(uint64))
			}
		}
	})
}

func waitForStatus(t *testing.T, stator func(context.Context) (string, error), status string, n int, sleep time.Duration) {
	t.Helper()

	for i := 0; i < n; i++ {
		s, err := stator(context.TODO())
		if err != nil {
			t.Logf("Status (try %d/%d): %v (retrying in %s)", i, n, err, sleep.String())
		} else {
			t.Logf("Status (try %d/%d): %s (retrying in %s)", i, n, s, sleep.String())
		}
		if s == status {
			return
		}
		time.Sleep(sleep)
	}

	s, err := stator(context.TODO())
	if err != nil {
		t.Fatalf("querying status: %v", err)
	}
	if status != s {
		waited := time.Duration(n) * sleep
		t.Fatalf("waited %s for status: %s, got: %s", waited.String(), status, s)
	}
}
