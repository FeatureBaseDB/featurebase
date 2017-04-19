package pilosa_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

func createCluster(c *pilosa.Cluster) ([]*Server, []*Index) {
	numNodes := len(c.Nodes)
	idx := make([]*Index, numNodes)
	server := make([]*Server, numNodes)
	for i := 0; i < numNodes; i++ {
		idx[i] = MustOpenIndex()
		server[i] = NewServer()
		server[i].Handler.Host = server[i].Host()
		server[i].Handler.Cluster = c
		server[i].Handler.Cluster.Nodes[i].Host = server[i].Host()
		server[i].Handler.Index = idx[i].Index
	}
	return server, idx
}

// Test distributed TopN Bitmap count across 3 nodes.
func TestClient_MultiNode(t *testing.T) {
	cluster := NewCluster(3)
	s, idx := createCluster(cluster)

	for i := 0; i < len(cluster.Nodes); i++ {
		defer idx[i].Close()
		defer s[i].Close()
	}

	s[0].Handler.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		e := pilosa.NewExecutor()
		e.Index = idx[0].Index
		e.Host = cluster.Nodes[0].Host
		e.Cluster = cluster
		return e.Execute(ctx, db, query, slices, opt)
	}
	s[1].Handler.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		e := pilosa.NewExecutor()
		e.Index = idx[1].Index
		e.Host = cluster.Nodes[1].Host
		e.Cluster = cluster
		return e.Execute(ctx, db, query, slices, opt)
	}
	s[2].Handler.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		e := pilosa.NewExecutor()
		e.Index = idx[2].Index
		e.Host = cluster.Nodes[2].Host
		e.Cluster = cluster
		return e.Execute(ctx, db, query, slices, opt)
	}

	// Create a dispersed set of bitmaps across 3 nodes such that each individual node and slice width increment would reveal a different TopN.
	idx[0].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 0).MustSetBits(99, 1, 2, 3, 4)
	idx[0].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 0).MustSetBits(100, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	idx[0].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 0).MustSetBits(98, 1, 2, 3, 4, 5, 6)
	idx[0].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 0).MustSetBits(1, 4)
	idx[0].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 0).MustSetBits(22, 1, 2, 3, 4, 5)

	idx[1].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 10).MustSetBits(100, (SliceWidth*10)+10)
	idx[1].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 10).MustSetBits(4, (SliceWidth*10)+10, (SliceWidth*10)+11, (SliceWidth*10)+12)
	idx[1].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 10).MustSetBits(4, (SliceWidth*10)+10, (SliceWidth*10)+11, (SliceWidth*10)+12, (SliceWidth*10)+13, (SliceWidth*10)+14, (SliceWidth*10)+15)
	idx[1].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 10).MustSetBits(2, (SliceWidth*10)+1, (SliceWidth*10)+2, (SliceWidth*10)+3, (SliceWidth*10)+4)
	idx[1].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 10).MustSetBits(3, (SliceWidth*10)+1, (SliceWidth*10)+2, (SliceWidth*10)+3, (SliceWidth*10)+4, (SliceWidth*10)+5)
	idx[1].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 10).MustSetBits(22, (SliceWidth*10)+1, (SliceWidth*10)+2, (SliceWidth*10)+10)

	idx[2].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 6).MustSetBits(24, (SliceWidth*6)+10, (SliceWidth*6)+11, (SliceWidth*6)+12, (SliceWidth*6)+13, (SliceWidth*6)+14)
	idx[2].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 6).MustSetBits(20, (SliceWidth*6)+10, (SliceWidth*6)+11, (SliceWidth*6)+12, (SliceWidth*6)+13)
	idx[2].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 6).MustSetBits(21, (SliceWidth*6)+10)
	idx[2].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 6).MustSetBits(100, (SliceWidth*6)+10)
	idx[2].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 6).MustSetBits(99, (SliceWidth*6)+10, (SliceWidth*6)+11, (SliceWidth*6)+12)
	idx[2].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 6).MustSetBits(98, (SliceWidth*6)+10, (SliceWidth*6)+11)
	idx[2].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 6).MustSetBits(22, (SliceWidth*6)+10, (SliceWidth*6)+11, (SliceWidth*6)+12)

	// Rebuild the RankCache.
	// We have to do this to avoid the 10-second cache invalidation delay
	// built into cache.Invalidate()
	idx[0].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 0).RecalculateCache()
	idx[1].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 10).RecalculateCache()
	idx[2].MustCreateFragmentIfNotExists("d", "f.n", pilosa.ViewStandard, 6).RecalculateCache()

	// Connect to each node to compare results.
	client := make([]*Client, 3)
	client[0] = MustNewClient(s[0].Host())
	client[1] = MustNewClient(s[0].Host())
	client[2] = MustNewClient(s[0].Host())

	topN := 4
	q := fmt.Sprintf(`TopN(frame="%s", n=%d)`, "f.n", topN)

	result, err := client[0].ExecuteQuery(context.Background(), "d", q, true)
	if err != nil {
		t.Fatal(err)
	}

	// Check the results before every node has the correct max slice value.
	pairs := result.(internal.QueryResponse).Results[0].Pairs
	for _, pair := range pairs {
		if pair.Key == 22 && pair.Count != 5 {
			t.Fatalf("Invalid Cluster wide MaxSlice prevents accurate calculation of %s", pair)
		}
	}

	// Set max slice to correct value.
	idx[0].DB("d").SetRemoteMaxSlice(10)
	idx[1].DB("d").SetRemoteMaxSlice(10)
	idx[2].DB("d").SetRemoteMaxSlice(10)

	result, err = client[0].ExecuteQuery(context.Background(), "d", q, true)
	if err != nil {
		t.Fatal(err)
	}

	// Test must return exactly N results.
	if len(result.(internal.QueryResponse).Results[0].Pairs) != topN {
		t.Fatalf("unexpected number of TopN results: %s", spew.Sdump(result))
	}
	p := []*internal.Pair{
		{Key: 100, Count: 12},
		{Key: 22, Count: 11},
		{Key: 98, Count: 8},
		{Key: 99, Count: 7}}

	// Valdidate the Top 4 result counts.
	if !reflect.DeepEqual(result.(internal.QueryResponse).Results[0].Pairs, p) {
		t.Fatalf("Invalid TopN result set: %s", spew.Sdump(result))
	}

	result1, err := client[1].ExecuteQuery(context.Background(), "d", q, true)
	if err != nil {
		t.Fatal(err)
	}
	result2, err := client[2].ExecuteQuery(context.Background(), "d", q, true)
	if err != nil {
		t.Fatal(err)
	}

	// Compare TopN results across all nodes in the cluster.
	if !reflect.DeepEqual(result, result1) {
		t.Fatalf("TopN result should be the same on node0 and node1: %s", spew.Sdump(result1))
	}

	if !reflect.DeepEqual(result, result2) {
		t.Fatalf("TopN result should be the same on node0 and node2: %s", spew.Sdump(result2))
	}
}

// Ensure client can bulk import data.
func TestClient_Import(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	// Load bitmap into cache to ensure cache gets updated.
	f := idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0)
	f.Bitmap(0)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Index = idx.Index

	// Send import request.
	c := MustNewClient(s.Host())
	if err := c.Import(context.Background(), "d", "f", 0, []pilosa.Bit{
		{BitmapID: 0, ProfileID: 1},
		{BitmapID: 0, ProfileID: 5},
		{BitmapID: 200, ProfileID: 6},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := f.Bitmap(0).Bits(); !reflect.DeepEqual(a, []uint64{1, 5}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
	if a := f.Bitmap(200).Bits(); !reflect.DeepEqual(a, []uint64{6}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
}

// Ensure client can bulk import data to an inverse frame.
func TestClient_ImportInverseEnabled(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	d := idx.MustCreateDBIfNotExists("d", pilosa.DBOptions{})
	frameOpts := pilosa.FrameOptions{
		InverseEnabled: true,
	}
	frame, err := d.CreateFrameIfNotExists("f", frameOpts)
	if err != nil {
		panic(err)
	}
	v, err := frame.CreateViewIfNotExists(pilosa.ViewInverse)
	if err != nil {
		panic(err)
	}
	f, err := v.CreateFragmentIfNotExists(0)
	if err != nil {
		panic(err)
	}

	// Load bitmap into cache to ensure cache gets updated.
	f.Bitmap(0)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Index = idx.Index

	// Send import request.
	c := MustNewClient(s.Host())
	if err := c.Import(context.Background(), "d", "f", 0, []pilosa.Bit{
		{BitmapID: 0, ProfileID: 1},
		{BitmapID: 0, ProfileID: 5},
		{BitmapID: 200, ProfileID: 5},
		{BitmapID: 200, ProfileID: 6},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := f.Bitmap(1).Bits(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
	if a := f.Bitmap(5).Bits(); !reflect.DeepEqual(a, []uint64{0, 200}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
	if a := f.Bitmap(6).Bits(); !reflect.DeepEqual(a, []uint64{200}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
}

// Ensure client backup and restore a frame.
func TestClient_BackupRestore(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0).MustSetBits(100, 1, 2, 3, SliceWidth-1)
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 1).MustSetBits(100, SliceWidth, SliceWidth+2)
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 5).MustSetBits(100, (5*SliceWidth)+1)
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0).MustSetBits(200, 20000)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Index = idx.Index

	c := MustNewClient(s.Host())

	// Backup from frame.
	var buf bytes.Buffer
	if err := c.BackupTo(context.Background(), &buf, "d", "f", pilosa.ViewStandard); err != nil {
		t.Fatal(err)
	}

	// Restore to a different frame.
	if _, err := idx.MustCreateDBIfNotExists("x", pilosa.DBOptions{}).CreateFrameIfNotExists("y", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreFrom(context.Background(), &buf, "x", "y", pilosa.ViewStandard); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := idx.Fragment("x", "y", pilosa.ViewStandard, 0).Bitmap(100).Bits(); !reflect.DeepEqual(a, []uint64{1, 2, 3, SliceWidth - 1}) {
		t.Fatalf("unexpected bits(0): %+v", a)
	}
	if a := idx.Fragment("x", "y", pilosa.ViewStandard, 1).Bitmap(100).Bits(); !reflect.DeepEqual(a, []uint64{SliceWidth, SliceWidth + 2}) {
		t.Fatalf("unexpected bits(0): %+v", a)
	}
	if a := idx.Fragment("x", "y", pilosa.ViewStandard, 5).Bitmap(100).Bits(); !reflect.DeepEqual(a, []uint64{(5 * SliceWidth) + 1}) {
		t.Fatalf("unexpected bits(0): %+v", a)
	}
	if a := idx.Fragment("x", "y", pilosa.ViewStandard, 0).Bitmap(200).Bits(); !reflect.DeepEqual(a, []uint64{20000}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
}

// Ensure client can retrieve a list of all checksums for blocks in a fragment.
func TestClient_FragmentBlocks(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	// Set two bits on blocks 0 & 3.
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0).SetBit(pilosa.HashBlockSize*3, 100)

	// Set a bit on a different slice.
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 1).SetBit(0, 1)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Index = idx.Index

	// Retrieve blocks.
	c := MustNewClient(s.Host())
	blocks, err := c.FragmentBlocks(context.Background(), "d", "f", pilosa.ViewStandard, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(blocks) != 2 {
		t.Fatalf("unexpected blocks: %s", spew.Sdump(blocks))
	} else if blocks[0].ID != 0 {
		t.Fatalf("unexpected block id(0): %d", blocks[0].ID)
	} else if blocks[1].ID != 3 {
		t.Fatalf("unexpected block id(1): %d", blocks[1].ID)
	}

	// Verify data matches local blocks.
	if a := idx.Fragment("d", "f", pilosa.ViewStandard, 0).Blocks(); !reflect.DeepEqual(a, blocks) {
		t.Fatalf("blocks mismatch:\n\nexp=%s\n\ngot=%s\n\n", spew.Sdump(a), spew.Sdump(blocks))
	}
}

// Client represents a test wrapper for pilosa.Client.
type Client struct {
	*pilosa.Client
}

// MustNewClient returns a new instance of Client. Panic on error.
func MustNewClient(host string) *Client {
	c, err := pilosa.NewClient(host)
	if err != nil {
		panic(err)
	}
	return &Client{Client: c}
}
