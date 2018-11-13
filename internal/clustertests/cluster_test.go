package clustertest

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
	picli "github.com/pilosa/pilosa/http"
)

func TestClusterStuff(t *testing.T) {
	if os.Getenv("ENABLE_PILOSA_CLUSTER_TESTS") != "1" {
		t.Skip()
	}
	cli, err := picli.NewInternalClient("pilosa1:10101", picli.GetHTTPClient(nil))
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}

	t.Run("long pause", func(t *testing.T) {
		err := cli.CreateIndex(context.Background(), "testidx", pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		err = cli.CreateFieldWithOptions(context.Background(), "testidx", "testf", pilosa.FieldOptions{CacheType: pilosa.CacheTypeRanked, CacheSize: 100})
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		data := make([]pilosa.Bit, 10)
		for i := 0; i < 1000; i++ {
			data[i%10].RowID = 0
			data[i%10].ColumnID = uint64((i/10)*pilosa.ShardWidth + i%10)
			shard := uint64(i / 10)
			if i%10 == 9 {
				err = cli.Import(context.Background(), "testidx", "testf", shard, data)
				if err != nil {
					t.Fatalf("importing: %v", err)
				}
			}
		}

		r, err := cli.Query(context.Background(), "testidx", &pilosa.QueryRequest{Index: "testidx", Query: "Count(Row(testf=0))"})
		if err != nil {
			t.Fatalf("count querying: %v", err)
		}
		if r.Results[0].(uint64) != 1000 {
			t.Fatalf("count after import is %d", r.Results[0].(uint64))
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

		// TODO change the sleep to wait for status to return to NORMAL - need support in internal client for getting status
		t.Log("done with pause, waiting for stability")
		time.Sleep(time.Second * 3)
		t.Log("done waiting for stability")

		r, err = cli.Query(context.Background(), "testidx", &pilosa.QueryRequest{Index: "testidx", Query: "Count(Row(testf=0))"})
		if err != nil {
			t.Fatalf("count querying: %v", err)
		}
		if r.Results[0].(uint64) != 1000 {
			t.Fatalf("count after import is %d", r.Results[0].(uint64))
		}
	})

}
