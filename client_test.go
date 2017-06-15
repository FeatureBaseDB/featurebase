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

func createCluster(c *pilosa.Cluster) ([]*Server, []*Holder) {
	numNodes := len(c.Nodes)
	hldr := make([]*Holder, numNodes)
	server := make([]*Server, numNodes)
	for i := 0; i < numNodes; i++ {
		hldr[i] = MustOpenHolder()
		server[i] = NewServer()
		server[i].Handler.Host = server[i].Host()
		server[i].Handler.Cluster = c
		server[i].Handler.Cluster.Nodes[i].Host = server[i].Host()
		server[i].Handler.Holder = hldr[i].Holder
	}
	return server, hldr
}

// Test distributed TopN Row count across 3 nodes.
func TestClient_MultiNode(t *testing.T) {
	cluster := NewCluster(3)
	s, hldr := createCluster(cluster)

	for i := 0; i < len(cluster.Nodes); i++ {
		defer hldr[i].Close()
		defer s[i].Close()
	}

	s[0].Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		e := pilosa.NewExecutor()
		e.Holder = hldr[0].Holder
		e.Host = cluster.Nodes[0].Host
		e.Cluster = cluster
		return e.Execute(ctx, index, query, slices, opt)
	}
	s[1].Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		e := pilosa.NewExecutor()
		e.Holder = hldr[1].Holder
		e.Host = cluster.Nodes[1].Host
		e.Cluster = cluster
		return e.Execute(ctx, index, query, slices, opt)
	}
	s[2].Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		e := pilosa.NewExecutor()
		e.Holder = hldr[2].Holder
		e.Host = cluster.Nodes[2].Host
		e.Cluster = cluster
		return e.Execute(ctx, index, query, slices, opt)
	}

	// Create a dispersed set of bitmaps across 3 nodes such that each individual node and slice width increment would reveal a different TopN.
	sliceNums := []uint64{1, 2, 6}
	for i, num := range sliceNums {
		owns := s[i].Handler.Handler.Cluster.OwnsSlices("i", 20, s[i].Host())
		ownsNum := false
		for _, ownNum := range owns {
			if ownNum == num {
				ownsNum = true
				break
			}
		}
		if !ownsNum {
			t.Fatalf("Trying to use slice %d on host %s, but it doesn't own that slice. It owns %s", num, s[i].Host(), owns)
		}
	}

	baseBit0 := SliceWidth * sliceNums[0]
	baseBit1 := SliceWidth * sliceNums[1]
	baseBit2 := SliceWidth * sliceNums[2]

	maxSlice := uint64(0)
	for _, x := range sliceNums {
		if x > maxSlice {
			maxSlice = x
		}
	}

	hldr[0].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[0]).MustSetBits(100, baseBit0+10)
	hldr[0].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[0]).MustSetBits(4, baseBit0+10, baseBit0+11, baseBit0+12)
	hldr[0].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[0]).MustSetBits(4, baseBit0+10, baseBit0+11, baseBit0+12, baseBit0+13, baseBit0+14, baseBit0+15)
	hldr[0].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[0]).MustSetBits(2, baseBit0+1, baseBit0+2, baseBit0+3, baseBit0+4)
	hldr[0].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[0]).MustSetBits(3, baseBit0+1, baseBit0+2, baseBit0+3, baseBit0+4, baseBit0+5)
	hldr[0].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[0]).MustSetBits(22, baseBit0+1, baseBit0+2, baseBit0+10)

	hldr[1].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[1]).MustSetBits(99, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4)
	hldr[1].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[1]).MustSetBits(100, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4, baseBit1+5, baseBit1+6, baseBit1+7, baseBit1+8, baseBit1+9, baseBit1+10)
	hldr[1].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[1]).MustSetBits(98, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4, baseBit1+5, baseBit1+6)
	hldr[1].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[1]).MustSetBits(1, baseBit1+4)
	hldr[1].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[1]).MustSetBits(22, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4, baseBit1+5)

	hldr[2].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[2]).MustSetBits(24, baseBit2+10, baseBit2+11, baseBit2+12, baseBit2+13, baseBit2+14)
	hldr[2].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[2]).MustSetBits(20, baseBit2+10, baseBit2+11, baseBit2+12, baseBit2+13)
	hldr[2].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[2]).MustSetBits(21, baseBit2+10)
	hldr[2].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[2]).MustSetBits(100, baseBit2+10)
	hldr[2].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[2]).MustSetBits(99, baseBit2+10, baseBit2+11, baseBit2+12)
	hldr[2].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[2]).MustSetBits(98, baseBit2+10, baseBit2+11)
	hldr[2].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[2]).MustSetBits(22, baseBit2+10, baseBit2+11, baseBit2+12)

	// Rebuild the RankCache.
	// We have to do this to avoid the 10-second cache invalidation delay
	// built into cache.Invalidate()
	hldr[0].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[0]).RecalculateCache()
	hldr[1].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[1]).RecalculateCache()
	hldr[2].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[2]).RecalculateCache()

	// Connect to each node to compare results.
	client := make([]*Client, 3)
	client[0] = MustNewClient(s[0].Host())
	client[1] = MustNewClient(s[1].Host())
	client[2] = MustNewClient(s[2].Host())

	topN := 4
	q := fmt.Sprintf(`TopN(frame="%s", n=%d)`, "f", topN)

	result, err := client[0].ExecuteQuery(context.Background(), "i", q, true)
	if err != nil {
		t.Fatal(err)
	}

	// Check the results before every node has the correct max slice value.
	pairs := result.(internal.QueryResponse).Results[0].Pairs
	for _, pair := range pairs {
		if pair.Key == 22 && pair.Count != 3 {
			t.Fatalf("Invalid Cluster wide MaxSlice prevents accurate calculation of %s", pair)
		}
	}

	// Set max slice to correct value.
	hldr[0].Index("i").SetRemoteMaxSlice(maxSlice)
	hldr[1].Index("i").SetRemoteMaxSlice(maxSlice)
	hldr[2].Index("i").SetRemoteMaxSlice(maxSlice)

	result, err = client[0].ExecuteQuery(context.Background(), "i", q, true)
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

	result1, err := client[1].ExecuteQuery(context.Background(), "i", q, true)
	if err != nil {
		t.Fatal(err)
	}
	result2, err := client[2].ExecuteQuery(context.Background(), "i", q, true)
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
	hldr := MustOpenHolder()
	defer hldr.Close()

	// Load bitmap into cache to ensure cache gets updated.
	f := hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0)
	f.Row(0)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Holder = hldr.Holder

	// Send import request.
	c := MustNewClient(s.Host())
	if err := c.Import(context.Background(), "i", "f", 0, []pilosa.Bit{
		{RowID: 0, ColumnID: 1},
		{RowID: 0, ColumnID: 5},
		{RowID: 200, ColumnID: 6},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := f.Row(0).Bits(); !reflect.DeepEqual(a, []uint64{1, 5}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
	if a := f.Row(200).Bits(); !reflect.DeepEqual(a, []uint64{6}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
}

// Ensure client can bulk import data to an inverse frame.
func TestClient_ImportInverseEnabled(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	frameOpts := pilosa.FrameOptions{
		InverseEnabled: true,
	}
	frame, err := idx.CreateFrameIfNotExists("f", frameOpts)
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
	f.Row(0)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Holder = hldr.Holder

	// Send import request.
	c := MustNewClient(s.Host())
	if err := c.Import(context.Background(), "i", "f", 0, []pilosa.Bit{
		{RowID: 0, ColumnID: 1},
		{RowID: 0, ColumnID: 5},
		{RowID: 200, ColumnID: 5},
		{RowID: 200, ColumnID: 6},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := f.Row(1).Bits(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
	if a := f.Row(5).Bits(); !reflect.DeepEqual(a, []uint64{0, 200}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
	if a := f.Row(6).Bits(); !reflect.DeepEqual(a, []uint64{200}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
}

// Ensure client backup and restore a frame.
func TestClient_BackupRestore(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).MustSetBits(100, 1, 2, 3, SliceWidth-1)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).MustSetBits(100, SliceWidth, SliceWidth+2)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 5).MustSetBits(100, (5*SliceWidth)+1)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).MustSetBits(200, 20000)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Holder = hldr.Holder

	c := MustNewClient(s.Host())

	// Backup from frame.
	var buf bytes.Buffer
	if err := c.BackupTo(context.Background(), &buf, "i", "f", pilosa.ViewStandard); err != nil {
		t.Fatal(err)
	}

	// Restore to a different frame.
	if _, err := hldr.MustCreateIndexIfNotExists("x", pilosa.IndexOptions{}).CreateFrameIfNotExists("y", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreFrom(context.Background(), &buf, "x", "y", pilosa.ViewStandard); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := hldr.Fragment("x", "y", pilosa.ViewStandard, 0).Row(100).Bits(); !reflect.DeepEqual(a, []uint64{1, 2, 3, SliceWidth - 1}) {
		t.Fatalf("unexpected bits(0): %+v", a)
	}
	if a := hldr.Fragment("x", "y", pilosa.ViewStandard, 1).Row(100).Bits(); !reflect.DeepEqual(a, []uint64{SliceWidth, SliceWidth + 2}) {
		t.Fatalf("unexpected bits(0): %+v", a)
	}
	if a := hldr.Fragment("x", "y", pilosa.ViewStandard, 5).Row(100).Bits(); !reflect.DeepEqual(a, []uint64{(5 * SliceWidth) + 1}) {
		t.Fatalf("unexpected bits(0): %+v", a)
	}
	if a := hldr.Fragment("x", "y", pilosa.ViewStandard, 0).Row(200).Bits(); !reflect.DeepEqual(a, []uint64{20000}) {
		t.Fatalf("unexpected bits: %+v", a)
	}
}

// Ensure client backup and restore a frame with inverse view.
func TestClient_BackupInverseView(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	frameOpts := pilosa.FrameOptions{
		InverseEnabled: true,
	}
	frame, err := idx.CreateFrameIfNotExists("f", frameOpts)
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

	f.SetBit(100, 1)
	f.SetBit(100, 2)
	f.SetBit(100, 3)
	f.SetBit(100, SliceWidth-1)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Holder = hldr.Holder

	c := MustNewClient(s.Host())

	// Backup from frame.
	var buf bytes.Buffer
	if err := c.BackupTo(context.Background(), &buf, "i", "f", pilosa.ViewInverse); err != nil {
		t.Fatal(err)
	}

	// Restore to a different frame.
	if _, err := hldr.MustCreateIndexIfNotExists("x", pilosa.IndexOptions{}).CreateFrameIfNotExists("y", pilosa.FrameOptions{InverseEnabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreFrom(context.Background(), &buf, "x", "y", pilosa.ViewInverse); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := hldr.Fragment("x", "y", pilosa.ViewInverse, 0).Row(100).Bits(); !reflect.DeepEqual(a, []uint64{1, 2, 3, SliceWidth - 1}) {
		t.Fatalf("unexpected bits(0): %+v", a)
	}

}

// backup returns error with invalid view
func TestClient_BackupInvalidView(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).MustSetBits(100, 1, 2, 3, SliceWidth-1)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Holder = hldr.Holder

	c := MustNewClient(s.Host())

	// Backup from frame.
	var buf bytes.Buffer
	err := c.BackupTo(context.Background(), &buf, "i", "f", "invalid_view")
	if err != pilosa.ErrInvalidView {
		t.Fatal(err)
	}
}

// Ensure client can retrieve a list of all checksums for blocks in a fragment.
func TestClient_FragmentBlocks(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	// Set two bits on blocks 0 & 3.
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(pilosa.HashBlockSize*3, 100)

	// Set a bit on a different slice.
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(0, 1)

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Holder = hldr.Holder

	// Retrieve blocks.
	c := MustNewClient(s.Host())
	blocks, err := c.FragmentBlocks(context.Background(), "i", "f", pilosa.ViewStandard, 0)
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
	if a := hldr.Fragment("i", "f", pilosa.ViewStandard, 0).Blocks(); !reflect.DeepEqual(a, blocks) {
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
