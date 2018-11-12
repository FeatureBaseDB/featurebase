package clustertest

import (
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/pilosa/go-pilosa"
	pi "github.com/pilosa/pilosa"
)

func TestClusterStuff(t *testing.T) {
	if os.Getenv("ENABLE_PILOSA_CLUSTER_TESTS") != "1" {
		t.Skip()
	}
	cli := getPilosaClient(t)

	t.Run("long pause", func(t *testing.T) {

		idx := pilosa.NewIndex("testidx")
		err := cli.CreateIndex(idx)
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		f := idx.Field("testf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 10))
		err = cli.CreateField(f)
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		data := make([]pilosa.Column, 1000)
		for i := range data {
			data[i].RowID = 0
			data[i].ColumnID = uint64((i/10)*pi.ShardWidth + i%10)
		}

		err = cli.ImportField(f, &colIterator{cols: data}, pilosa.OptImportBatchSize(1000))
		if err != nil {
			t.Fatalf("importing: %v", err)
		}

		r, err := cli.Query(idx.Count(f.Row(0)))
		if err != nil {
			t.Fatalf("count querying: %v", err)
		}
		if r.Result().Count() != 1000 {
			t.Fatalf("count after import is %d", r.Result().Count())
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
		// TODO change the sleep to wait for status to return to NORMAL or timeout once we have Status.State support in go-pilosa
		t.Log("done with pause, waiting for stability")
		time.Sleep(time.Second * 3)
		t.Log("done waiting for stability")

		r, err = cli.Query(idx.Count(f.Row(0)))
		if err != nil {
			t.Fatalf("count querying: %v", err)
		} else if r.Result().Count() != 1000 {
			t.Fatalf("count after import is %d", r.Result().Count())
		}

	})

	down := exec.Command("/pumba", "stop", "clustertests_pilosa3_1", "clustertests_pilosa2_1", "clustertests_pilosa1_1")
	down.Stdout = os.Stdout
	down.Stderr = os.Stderr
	err := down.Run()
	if err != nil {
		t.Logf("stopping Pilosa: %v", err)
	}
}

// Utils

func getPilosaClient(t *testing.T) *pilosa.Client {
	cli, err := pilosa.NewClient("pilosa1:10101")
	if err != nil {
		time.Sleep(time.Millisecond * 40)
	}
	time.Sleep(time.Second * 2)
	// TODO uncomment the following once we get the version of go-pilosa that has the State field on Status.
	// start := time.Now()
	// for i := 0; true; i++ {
	// 	s, err := cli.Status()
	// 	if i > 800 {
	// 		t.Fatalf("couldn't connect to cluster after %d attempts and %v: state: %s, err: %v", i, time.Since(start), s.State, err)
	// 	}
	// 	if err != nil {
	// 		time.Sleep(time.Millisecond * 40)
	// 		continue
	// 	}
	// 	if s.State == "NORMAL" {
	// 		break
	// 	} else {
	// 		time.Sleep(time.Millisecond * 40)
	// 	}
	// }

	return cli
}

type colIterator struct {
	cols []pilosa.Column
	i    uint
}

func (c *colIterator) NextRecord() (pilosa.Record, error) {
	if int(c.i) >= len(c.cols) {
		return nil, io.EOF
	}
	c.i++
	return c.cols[c.i-1], nil
}

func TestColIterator(t *testing.T) {
	data := make([]pilosa.Column, 3)
	for i := range data {
		data[i].RowID = 0
		data[i].ColumnID = uint64((i/10)*pi.ShardWidth + i%10)
	}

	ci := colIterator{cols: data}
	col := pilosa.Column{}
	if rec, err := ci.NextRecord(); rec != col {
		t.Fatalf("first record wrong: %v, err: %v", rec, err)
	}
	col.ColumnID = 1
	if rec, err := ci.NextRecord(); rec != col || err != nil {
		t.Fatalf("second record wrong: %v, err: %v", rec, err)
	}
	col.ColumnID = 2
	if rec, err := ci.NextRecord(); rec != col || err != nil {
		t.Fatalf("third record wrong: %v, err: %v", rec, err)
	}
	if rec, err := ci.NextRecord(); err != io.EOF {
		t.Fatalf("should be EOF, but got %v, err: %v", rec, err)
	}
}
