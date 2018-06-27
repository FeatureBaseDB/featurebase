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

package http_test

import (
	"context"
	"fmt"
	gohttp "net/http"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/http"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/test"
)

func createCluster(c *pilosa.Cluster) ([]*test.Server, []*test.Holder) {
	numNodes := len(c.Nodes)
	hldr := make([]*test.Holder, numNodes)
	server := make([]*test.Server, numNodes)
	for i := 0; i < numNodes; i++ {
		hldr[i] = test.MustOpenHolder()
		server[i] = test.NewServer()
		server[i].Handler.API.Cluster = c
		server[i].Handler.API.Cluster.Nodes[i].URI = server[i].HostURI()
		server[i].Handler.API.Holder = hldr[i].Holder
	}
	return server, hldr
}

var defaultClient *gohttp.Client

func init() {
	defaultClient = http.GetHTTPClient(nil)

}

// Test distributed TopN Row count across 3 nodes.
func TestClient_MultiNode(t *testing.T) {
	t.Skip() // Until test.NewServer() works

	cluster := test.NewCluster(3)
	s, hldr := createCluster(cluster)

	for i := 0; i < len(cluster.Nodes); i++ {
		defer hldr[i].Close()
		defer s[i].Close()
	}

	s[0].Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		httpClient := http.NewInternalClientFromURI(&cluster.Nodes[0].URI, defaultClient)
		e := pilosa.NewExecutor(pilosa.OptExecutorInternalQueryClient(httpClient))
		e.Holder = hldr[0].Holder
		e.Node = cluster.Nodes[0]
		e.Cluster = cluster
		return e.Execute(ctx, index, query, slices, opt)
	}
	s[1].Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		httpClient := http.NewInternalClientFromURI(&cluster.Nodes[0].URI, defaultClient)
		e := pilosa.NewExecutor(pilosa.OptExecutorInternalQueryClient(httpClient))
		e.Holder = hldr[1].Holder
		e.Node = cluster.Nodes[1]
		e.Cluster = cluster
		return e.Execute(ctx, index, query, slices, opt)
	}
	s[2].Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		httpClient := http.NewInternalClientFromURI(&cluster.Nodes[0].URI, defaultClient)
		e := pilosa.NewExecutor(pilosa.OptExecutorInternalQueryClient(httpClient))
		e.Holder = hldr[2].Holder
		e.Node = cluster.Nodes[2]
		e.Cluster = cluster
		return e.Execute(ctx, index, query, slices, opt)
	}

	// Create a dispersed set of bitmaps across 3 nodes such that each individual node and slice width increment would reveal a different TopN.
	sliceNums := []uint64{1, 2, 6}

	// This was generated with: `owns := s[i].Handler.Handler.API.Cluster.OwnsSlices("i", 20, s[i].HostURI())`
	owns := [][]uint64{
		{1, 3, 4, 8, 10, 13, 17, 19},
		{2, 5, 7, 11, 12, 14, 18},
		{0, 6, 9, 15, 16, 20},
	}

	for i, num := range sliceNums {
		ownsNum := false
		for _, ownNum := range owns[i] {
			if ownNum == num {
				ownsNum = true
				break
			}
		}
		if !ownsNum {
			t.Fatalf("Trying to use slice %d on host %s, but it doesn't own that slice. It owns %v", num, s[i].Host(), owns)
		}
	}

	baseBit0 := pilosa.SliceWidth * sliceNums[0]
	baseBit1 := pilosa.SliceWidth * sliceNums[1]
	baseBit2 := pilosa.SliceWidth * sliceNums[2]

	maxSlice := uint64(0)
	for _, x := range sliceNums {
		if x > maxSlice {
			maxSlice = x
		}
	}

	hldr[0].MustSetBits("i", "f", 100, baseBit0+10)
	hldr[0].MustSetBits("i", "f", 4, baseBit0+10, baseBit0+11, baseBit0+12)
	hldr[0].MustSetBits("i", "f", 4, baseBit0+10, baseBit0+11, baseBit0+12, baseBit0+13, baseBit0+14, baseBit0+15)
	hldr[0].MustSetBits("i", "f", 2, baseBit0+1, baseBit0+2, baseBit0+3, baseBit0+4)
	hldr[0].MustSetBits("i", "f", 3, baseBit0+1, baseBit0+2, baseBit0+3, baseBit0+4, baseBit0+5)
	hldr[0].MustSetBits("i", "f", 22, baseBit0+1, baseBit0+2, baseBit0+10)

	hldr[1].MustSetBits("i", "f", 99, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4)
	hldr[1].MustSetBits("i", "f", 100, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4, baseBit1+5, baseBit1+6, baseBit1+7, baseBit1+8, baseBit1+9, baseBit1+10)
	hldr[1].MustSetBits("i", "f", 98, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4, baseBit1+5, baseBit1+6)
	hldr[1].MustSetBits("i", "f", 1, baseBit1+4)
	hldr[1].MustSetBits("i", "f", 22, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4, baseBit1+5)

	hldr[2].MustSetBits("i", "f", 24, baseBit2+10, baseBit2+11, baseBit2+12, baseBit2+13, baseBit2+14)
	hldr[2].MustSetBits("i", "f", 20, baseBit2+10, baseBit2+11, baseBit2+12, baseBit2+13)
	hldr[2].MustSetBits("i", "f", 21, baseBit2+10)
	hldr[2].MustSetBits("i", "f", 100, baseBit2+10)
	hldr[2].MustSetBits("i", "f", 99, baseBit2+10, baseBit2+11, baseBit2+12)
	hldr[2].MustSetBits("i", "f", 98, baseBit2+10, baseBit2+11)
	hldr[2].MustSetBits("i", "f", 22, baseBit2+10, baseBit2+11, baseBit2+12)

	// Rebuild the RankCache.
	// We have to do this to avoid the 10-second cache invalidation delay
	// built into cache.Invalidate()
	hldr[0].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[0]).RecalculateCache()
	hldr[1].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[1]).RecalculateCache()
	hldr[2].MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, sliceNums[2]).RecalculateCache()

	// Connect to each node to compare results.
	client := make([]*Client, 3)
	client[0] = MustNewClient(s[0].Host(), defaultClient)
	client[1] = MustNewClient(s[1].Host(), defaultClient)
	client[2] = MustNewClient(s[2].Host(), defaultClient)

	topN := 4
	queryRequest := &internal.QueryRequest{
		Query:  fmt.Sprintf(`TopN(f, n=%d)`, topN),
		Remote: false,
	}
	result, err := client[0].Query(context.Background(), "i", queryRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Check the results before every node has the correct max slice value.
	pairs := result.Results[0].Pairs
	for _, pair := range pairs {
		if pair.ID == 22 && pair.Count != 3 {
			t.Fatalf("Invalid Cluster wide MaxSlice prevents accurate calculation of %s", pair)
		}
	}

	// Set max slice to correct value.
	hldr[0].Index("i").SetRemoteMaxSlice(maxSlice)
	hldr[1].Index("i").SetRemoteMaxSlice(maxSlice)
	hldr[2].Index("i").SetRemoteMaxSlice(maxSlice)

	result, err = client[0].Query(context.Background(), "i", queryRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Test must return exactly N results.
	if len(result.Results[0].Pairs) != topN {
		t.Fatalf("unexpected number of TopN results: %s", spew.Sdump(result))
	}
	p := []*internal.Pair{
		{ID: 100, Count: 12},
		{ID: 22, Count: 11},
		{ID: 98, Count: 8},
		{ID: 99, Count: 7}}

	// Valdidate the Top 4 result counts.
	if !reflect.DeepEqual(result.Results[0].Pairs, p) {
		t.Fatalf("Invalid TopN result set: %s", spew.Sdump(result))
	}

	result1, err := client[1].Query(context.Background(), "i", queryRequest)
	if err != nil {
		t.Fatal(err)
	}
	result2, err := client[2].Query(context.Background(), "i", queryRequest)
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
	cmd := test.MustRunMainWithCluster(t, 1)[0]
	host := cmd.URL()
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	// Load bitmap into cache to ensure cache gets updated.
	hldr.SetBit("i", "f", 1, 0) // set a bit so the view gets created.
	hldr.Row("i", "f", 0)

	// Send import request.
	c := MustNewClient(host, defaultClient)
	if err := c.Import(context.Background(), "i", "f", 0, []pilosa.Bit{
		{RowID: 0, ColumnID: 1},
		{RowID: 0, ColumnID: 5},
		{RowID: 200, ColumnID: 6},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := hldr.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 5}) {
		t.Fatalf("unexpected columns: %+v", a)
	}
	if a := hldr.Row("i", "f", 200).Columns(); !reflect.DeepEqual(a, []uint64{6}) {
		t.Fatalf("unexpected columns: %+v", a)
	}
}

// Ensure client can bulk import value data.
func TestClient_ImportValue(t *testing.T) {
	cmd := test.MustRunMainWithCluster(t, 1)[0]
	host := cmd.URL()
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	fldName := "f"
	fo := pilosa.FieldOptions{
		Type: pilosa.FieldTypeInt,
		Min:  -100,
		Max:  100,
	}

	// Load bitmap into cache to ensure cache gets updated.
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	field, err := index.CreateFieldIfNotExists(fldName, fo)
	if err != nil {
		t.Fatal(err)
	}

	// Send import request.
	c := MustNewClient(host, defaultClient)
	if err := c.ImportValue(context.Background(), "i", "f", 0, []pilosa.FieldValue{
		{ColumnID: 1, Value: -10},
		{ColumnID: 2, Value: 20},
		{ColumnID: 3, Value: 40},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify Sum.
	sum, cnt, err := field.Sum(nil, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if sum != 50 || cnt != 3 {
		t.Fatalf("unexpected values: got sum=%v, count=%v; expected sum=50, cnt=3", sum, cnt)
	}

	// Verify Min.
	min, cnt, err := field.Min(nil, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if min != -10 || cnt != 1 {
		t.Fatalf("unexpected values: got min=%v, count=%v; expected min=-10, cnt=1", min, cnt)
	}

	// Verify Min with Filter.
	filter, err := field.Range(fldName, pql.GT, 40)
	if err != nil {
		t.Fatal(err)
	}
	min, cnt, err = field.Min(filter, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if min != -100 || cnt != 0 {
		t.Fatalf("unexpected values: got min=%v, count=%v; expected min=-100, cnt=0", min, cnt)
	}

	// Verify Max.
	max, cnt, err := field.Max(nil, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if max != 40 || cnt != 1 {
		t.Fatalf("unexpected values: got max=%v, count=%v; expected max=40, cnt=1", max, cnt)
	}
}

// Ensure client can retrieve a list of all checksums for blocks in a fragment.
func TestClient_FragmentBlocks(t *testing.T) {
	cmd := test.MustRunMainWithCluster(t, 1)[0]
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", pilosa.HashBlockSize*3, 100)

	// Set a bit on a different slice.
	hldr.SetBit("i", "f", 0, 1)
	c := MustNewClient(cmd.URL(), defaultClient)
	blocks, err := c.FragmentBlocks(context.Background(), nil, "i", "f", 0)
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
	*http.InternalClient
}

// MustNewClient returns a new instance of Client. Panic on error.
func MustNewClient(host string, h *gohttp.Client) *Client {
	c, err := http.NewInternalClient(host, h)
	if err != nil {
		panic(err)
	}
	return &Client{InternalClient: c}
}
