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

package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	nethttp "net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/test"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var runStress bool

func init() { // nolint: gochecknoinits
	flag.BoolVar(&runStress, "stress", false, "Enable stress tests (time consuming)")
}

// Ensure program can process queries and maintain consistency.
func TestMain_Set_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}

	for i := 0; i < 100; i++ {
		//for i := 0; i < 10; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()

			rand := rand.New(rand.NewSource(int64(i)))
			cmds := GenerateSetCommands(1000, rand)

			m := test.RunCommand(t)
			defer m.Close()

			// Create client.
			client, err := http.NewInternalClient(m.API.Node().URI.HostPort(), http.GetHTTPClient(nil))
			if err != nil {
				t.Fatal(err)
			}

			// Execute Set() commands.
			for _, cmd := range cmds {
				if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
					t.Fatal(err)
				}
				if err := client.CreateField(context.Background(), "i", cmd.Field); err != nil && err != pilosa.ErrFieldExists {
					t.Fatal(err)
				}
				if _, err := m.Query(t, "i", "", fmt.Sprintf(`Set(%d, %s=%d)`, cmd.ColumnID, cmd.Field, cmd.ID)); err != nil {
					t.Fatal(err)
				}
			}

			// Validate data.
			for field, fieldSet := range SetCommands(cmds).Fields() {
				for id, columnIDs := range fieldSet {
					exp := MustMarshalJSON(map[string]interface{}{
						"results": []interface{}{
							map[string]interface{}{
								"columns": columnIDs,
								"attrs":   map[string]interface{}{},
							},
						},
					}) + "\n"
					if res, err := m.Query(t, "i", "", fmt.Sprintf(`Row(%s=%d)`, field, id)); err != nil {
						t.Fatal(err)
					} else if res != exp {
						t.Fatalf("unexpected result:\n\ngot=%s\n\nexp=%s\n\n", res, exp)
					}
				}
			}

			if err := m.Reopen(); err != nil {
				t.Fatal(err)
			}

			// Validate data after reopening.
			for field, fieldSet := range SetCommands(cmds).Fields() {
				for id, columnIDs := range fieldSet {
					exp := MustMarshalJSON(map[string]interface{}{
						"results": []interface{}{
							map[string]interface{}{
								"columns": columnIDs,
								"attrs":   map[string]interface{}{},
							},
						},
					}) + "\n"
					if res, err := m.Query(t, "i", "", fmt.Sprintf(`Row(%s=%d)`, field, id)); err != nil {
						t.Fatal(err)
					} else if res != exp {
						t.Fatalf("unexpected result (reopen):\n\ngot=%s\n\nexp=%s\n\n", res, exp)
					}
				}
			}
		})
	}
}

// Ensure program can set row attributes and retrieve them.
func TestMain_SetRowAttrs(t *testing.T) {
	m := test.RunCommand(t)
	defer m.Close()

	// Create fields.
	client := m.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client.CreateField(context.Background(), "i", "x"); err != nil {
		t.Fatal(err)
	} else if err := client.CreateField(context.Background(), "i", "z"); err != nil {
		t.Fatal(err)
	} else if err := client.CreateField(context.Background(), "i", "neg"); err != nil {
		t.Fatal(err)
	}

	// Set columns on different rows in different fields.
	if _, err := m.Query(t, "i", "", `Set(100, x=1)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query(t, "i", "", `Set(100, x=2)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query(t, "i", "", `Set(100, x=2)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query(t, "i", "", `Set(100, neg=3)`); err != nil {
		t.Fatal(err)
	}

	// Set row attributes.
	if _, err := m.Query(t, "i", "", `SetRowAttrs(x, 1, x=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query(t, "i", "", `SetRowAttrs(x, 2, x=-200)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query(t, "i", "", `SetRowAttrs(z, 2, x=300)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query(t, "i", "", `SetRowAttrs(neg, 3, x=-0.44)`); err != nil {
		t.Fatal(err)
	}

	// Query row x/1.
	if res, err := m.Query(t, "i", "", `Row(x=1)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"columns":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	// Query row x/2.
	if res, err := m.Query(t, "i", "", `Row(x=2)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-200},"columns":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query rows after reopening.
	if res, err := m.Query(t, "i", "columnAttrs=true", `Row(x=1)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"columns":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}

	if res, err := m.Query(t, "i", "columnAttrs=true", `Row(neg=3)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-0.44},"columns":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}
	// Query row x/2.
	if res, err := m.Query(t, "i", "", `Row(x=2)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-200},"columns":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}
}

// Ensure program can set column attributes and retrieve them.
func TestMain_SetColumnAttrs(t *testing.T) {
	m := test.RunCommand(t)
	defer m.Close()

	// Create fields.
	client := m.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client.CreateField(context.Background(), "i", "x"); err != nil {
		t.Fatal(err)
	}

	// Set columns on row.
	if _, err := m.Query(t, "i", "", `Set(100, x=1)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query(t, "i", "", `Set(101, x=1)`); err != nil {
		t.Fatal(err)
	}

	// Set column attributes.
	if _, err := m.Query(t, "i", "", `SetColumnAttrs(100, foo="bar")`); err != nil {
		t.Fatal(err)
	}

	// Query row.
	if res, err := m.Query(t, "i", "columnAttrs=true", `Row(x=1)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"columns":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query row after reopening.
	if res, err := m.Query(t, "i", "columnAttrs=true", `Row(x=1)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"columns":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}
}

func TestMain_GroupBy(t *testing.T) {
	m := test.RunCommand(t)
	defer m.Close()

	// Create fields.
	client := m.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	}
	if err := client.CreateFieldWithOptions(context.Background(), "i", "generalk", pilosa.FieldOptions{Keys: true}); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateFieldWithOptions(context.Background(), "i", "subk", pilosa.FieldOptions{Keys: true}); err != nil {
		t.Fatal(err)
	}

	query := `
		Set(0, generalk="ten")
		Set(1, generalk="ten")
		Set(1001, generalk="ten")
		Set(2, generalk="eleven")
		Set(1002, generalk="eleven")
		Set(2, generalk="twelve")
		Set(1002, generalk="twelve")

		Set(0, subk="one-hundred")
		Set(1, subk="one-hundred")
		Set(3, subk="one-hundred")
		Set(1001, subk="one-hundred")
		Set(2, subk="one-hundred-ten")
		Set(0, subk="one-hundred-ten")
	`

	// Set columns on row.
	if _, err := m.Query(t, "i", "", query); err != nil {
		t.Fatal(err)
	}

	expected := []pilosa.GroupCount{
		{Group: []pilosa.FieldRow{{Field: "generalk", RowKey: "ten"}, {Field: "subk", RowKey: "one-hundred"}}, Count: 3},
		{Group: []pilosa.FieldRow{{Field: "generalk", RowKey: "ten"}, {Field: "subk", RowKey: "one-hundred-ten"}}, Count: 1},
		{Group: []pilosa.FieldRow{{Field: "generalk", RowKey: "eleven"}, {Field: "subk", RowKey: "one-hundred-ten"}}, Count: 1},
		{Group: []pilosa.FieldRow{{Field: "generalk", RowKey: "twelve"}, {Field: "subk", RowKey: "one-hundred-ten"}}, Count: 1},
	}

	// Query row.
	if res, err := m.QueryProtobuf("i", `GroupBy(Rows(generalk), Rows(subk))`); err != nil {
		t.Fatal(err)
	} else {
		test.CheckGroupBy(t, expected, res.Results[0].([]pilosa.GroupCount))
	}
}

func TestMain_MinMaxFloat(t *testing.T) {
	m := test.RunCommand(t)
	defer m.Close()

	// Create fields.
	client := m.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	}
	if err := client.CreateFieldWithOptions(context.Background(), "i", "dec", pilosa.FieldOptions{Type: pilosa.FieldTypeDecimal, Scale: 3, Max: pql.NewDecimal(100000, 0)}); err != nil {
		t.Fatal(err)
	}

	query := `
		Set(0, dec=1.32)
		Set(1, dec=4.44)
	`

	// Set columns on row.
	if _, err := m.Query(t, "i", "", query); err != nil {
		t.Fatal(err)
	}

	// Query row.
	exp0 := pilosa.ValCount{DecimalVal: &pql.Decimal{Value: 4440, Scale: 3}, Count: 1}
	exp1 := pilosa.ValCount{DecimalVal: &pql.Decimal{Value: 1320, Scale: 3}, Count: 1}
	if res, err := m.QueryProtobuf("i", `Max(field=dec) Min(field=dec)`); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(res.Results[0], exp0) || !reflect.DeepEqual(res.Results[1], exp1) {
		t.Fatalf("unexpected results: %+v", res.Results)
	}
}

// Ensure the host can be parsed.
func TestConfig_Parse_Host(t *testing.T) {
	if c, err := server.ParseConfig(`bind = "local"`); err != nil {
		t.Fatal(err)
	} else if c.Bind != "local" {
		t.Fatalf("unexpected host: %s", c.Bind)
	}
}

// Ensure the data directory can be parsed.
func TestConfig_Parse_DataDir(t *testing.T) {
	if c, err := server.ParseConfig(`data-dir = "/tmp/foo"`); err != nil {
		t.Fatal(err)
	} else if c.DataDir != "/tmp/foo" {
		t.Fatalf("unexpected data dir: %s", c.DataDir)
	}
}

func TestConcurrentFieldCreation(t *testing.T) {
	cluster := test.MustRunCluster(t, 3)
	defer cluster.Close()

	api0 := cluster.GetNode(0).API
	if _, err := api0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil {
		t.Fatalf("creating index: %v", err)
	}
	eg := errgroup.Group{}
	for i := 0; i < 100; i++ {
		i := i
		eg.Go(func() error {
			if _, err := api0.CreateField(context.Background(), "i", fmt.Sprintf("f%d", i)); err != nil {
				return err
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		t.Fatalf("creating concurrent field: %v", err)
	}
}

func TestTransactionsAPI(t *testing.T) {
	cluster := test.MustRunCluster(t, 3)
	defer cluster.Close()

	api0 := cluster.GetNode(0).API
	api1 := cluster.GetNode(1).API
	ctx := context.Background()
	//api2 := cluster.GetNode(2).API

	// can fetch empty transactions
	if trnsMap, err := api0.Transactions(ctx); err != nil {
		t.Fatalf("getting transactions: %v", err)
	} else if len(trnsMap) != 0 {
		t.Fatalf("unexpectedly has transactions: %v", trnsMap)
	}

	// can't fetch transactions from non-coordinator
	if _, err := api1.Transactions(ctx); err != pilosa.ErrNodeNotCoordinator {
		t.Errorf("api1 should return ErrNodeNotCoordinator when asked for transactions but got: %v", err)
	}

	// can start transaction
	if trns, err := api0.StartTransaction(ctx, "a", time.Minute, false, false); err != nil {
		t.Errorf("couldn't start transaction: %v", err)
	} else {
		test.CompareTransactions(t, &pilosa.Transaction{ID: "a", Active: true, Timeout: time.Minute, Deadline: time.Now().Add(time.Minute)}, trns)
	}

	// can retrieve transaction from other nodes with remote=true
	if trns, err := api1.GetTransaction(ctx, "a", true); err != nil {
		t.Errorf("couldn't fetch transaction from other node with remote=true: %v", err)
	} else {
		test.CompareTransactions(t, &pilosa.Transaction{ID: "a", Active: true, Timeout: time.Minute, Deadline: time.Now().Add(time.Minute)}, trns)
	}

	// can start transaction with blank id and get uuid back
	id := ""
	if trns, err := api0.StartTransaction(ctx, id, time.Minute, false, false); err != nil {
		t.Errorf("couldn't start transaction: %v", err)
	} else {
		id = trns.ID
		if len(id) != 36 { // UUID
			t.Errorf("unexpected generated ID: %s", id)
		}
		test.CompareTransactions(t, &pilosa.Transaction{ID: id, Active: true, Timeout: time.Minute, Deadline: time.Now().Add(time.Minute)}, trns)
	}

	// can't finish transaction on non-coordinator
	if _, err := api1.FinishTransaction(ctx, id, false); err != pilosa.ErrNodeNotCoordinator {
		t.Errorf("unexpected error is not ErrNodeNotCoordinator: %v", err)
	}

	// can finish transaction
	if _, err := api0.FinishTransaction(ctx, id, false); err != nil {
		t.Errorf("couldn't finish transaction: %v", err)
	}

	// can finish previous transaction
	if _, err := api0.FinishTransaction(ctx, "a", false); err != nil {
		t.Errorf("couldn't finish transaction a: %v", err)
	}

	// can start exclusive transaction
	if te, err := api0.StartTransaction(ctx, "exc", time.Minute, true, false); err != nil {
		t.Errorf("couldn't start exclusive transaction: %v", err)
	} else if !te.Active {
		t.Errorf("expected exclusive transaction to be active: %+v", te)
	}

	// can finish exclusive transaction
	if _, err := api0.FinishTransaction(ctx, "exc", false); err != nil {
		t.Errorf("couldn't finish exclusive transaction: %v", err)
	}

	// can start transaction (with same name as previous finished transaction)
	if trns, err := api0.StartTransaction(ctx, "a", time.Minute, false, false); err != nil {
		t.Errorf("couldn't start transaction: %v", err)
	} else {
		test.CompareTransactions(t, &pilosa.Transaction{ID: "a", Active: true, Timeout: time.Minute, Deadline: time.Now().Add(time.Minute)}, trns)
	}

	// can start exclusive transaction and is not immediately active
	if te, err := api0.StartTransaction(ctx, "exc", time.Minute, true, false); err != nil {
		t.Errorf("couldn't start exclusive transaction: %v", err)
	} else if te.Active {
		t.Errorf("expected exclusive transaction to be inactive: %+v", te)
	}

	// can finish non-exclusive transaction
	if _, err := api0.FinishTransaction(ctx, "a", false); err != nil {
		t.Errorf("couldn't finish transaction a: %v", err)
	}

	// can poll exclusive transaction and is active
	var excTrns *pilosa.Transaction
	if trns, err := api0.GetTransaction(ctx, "exc", false); err != nil {
		t.Errorf("couldn't poll exclusive transaction: %v", err)
	} else {
		excTrns = &pilosa.Transaction{ID: "exc", Active: true, Exclusive: true, Timeout: time.Minute, Deadline: time.Now().Add(time.Minute)}
		test.CompareTransactions(t, excTrns, trns)
	}

	// can't start another exclusive transaction
	if trns, err := api0.StartTransaction(ctx, "exc2", time.Minute, true, false); errors.Cause(err) != pilosa.ErrTransactionExclusive {
		t.Errorf("unexpected error: %v", err)
	} else {
		// returned transaction should be the exclusive one which is blocking this one
		test.CompareTransactions(t, excTrns, trns)
	}

	// can't keep the second exclusive name but make it nonexclusive and start a transaction
	if trns, err := api0.StartTransaction(ctx, "exc2", time.Minute, false, false); errors.Cause(err) != pilosa.ErrTransactionExclusive {
		t.Errorf("unexpected error: %v", err)
	} else {
		test.CompareTransactions(t, excTrns, trns)
	}

	// transaction is active on other nodes with remote=true
	if trns, err := api1.GetTransaction(ctx, "exc", true); err != nil {
		t.Errorf("couldn't poll exclusive transaction: %v", err)
	} else {
		test.CompareTransactions(t, &pilosa.Transaction{ID: "exc", Active: true, Exclusive: true, Timeout: time.Minute, Deadline: time.Now().Add(time.Minute)}, trns)
	}

	// LATER, test deadline extension on non-coordinator blocks active, exclusive transaction being returned
}

func TestMain_RecalculateHashes(t *testing.T) {
	const clusterSize = 5
	cluster := test.MustRunCluster(t, clusterSize)
	defer cluster.Close()

	// Create the schema.
	client0 := cluster.GetNode(0).Client()
	if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal("create index:", err)
	}
	if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
		t.Fatal("create field:", err)
	}

	// Set some columns
	data := []string{}
	for rowID := 1; rowID < 10; rowID++ {
		for columnID := 1; columnID < 100; columnID++ {
			data = append(data, fmt.Sprintf(`Set(%d, f=%d)`, columnID, rowID))
		}
	}
	if _, err := cluster.GetNode(0).Query(t, "i", "", strings.Join(data, "")); err != nil {
		t.Fatal("setting columns:", err)
	}

	// Calculate caches on the first node
	err := cluster.GetNode(0).RecalculateCaches(t)
	if err != nil {
		t.Fatalf("recalculating caches: %v", err)
	}

	target := `{"results":[[{"id":7,"key":"","count":99},{"id":1,"key":"","count":99},{"id":9,"key":"","count":99},{"id":5,"key":"","count":99},{"id":4,"key":"","count":99},{"id":8,"key":"","count":99},{"id":2,"key":"","count":99},{"id":6,"key":"","count":99},{"id":3,"key":"","count":99}]]}`

	// Run a TopN query on all nodes. The result should be the same as the target.
	for _, m := range cluster.Nodes {
		res, err := m.Query(t, "i", "", `TopN(f)`)
		if err != nil {
			t.Fatal(err)
		}
		res = strings.TrimSpace(res)
		if sortedString(target) != sortedString(res) {
			t.Fatalf("%v != %v", target, res)
		}
	}
}

// SetCommand represents a command to set a column.
type SetCommand struct {
	ID       uint64
	Field    string
	ColumnID uint64
}

type SetCommands []SetCommand

// Fields returns the set of column ids for each field/row.
func (a SetCommands) Fields() map[string]map[uint64][]uint64 {
	// Create a set of unique commands.
	m := make(map[SetCommand]struct{})
	for _, cmd := range a {
		m[cmd] = struct{}{}
	}

	// Build unique ids for each field & row.
	fields := make(map[string]map[uint64][]uint64)
	for cmd := range m {
		if fields[cmd.Field] == nil {
			fields[cmd.Field] = make(map[uint64][]uint64)
		}
		fields[cmd.Field][cmd.ID] = append(fields[cmd.Field][cmd.ID], cmd.ColumnID)
	}

	// Sort each set of column ids.
	for _, field := range fields {
		for id := range field {
			sort.Sort(uint64Slice(field[id]))
		}
	}

	return fields
}

// GenerateSetCommands generates random SetCommand objects.
func GenerateSetCommands(n int, rand *rand.Rand) []SetCommand {
	cmds := make([]SetCommand, rand.Intn(n))
	for i := range cmds {
		cmds[i] = SetCommand{
			ID:       uint64(rand.Intn(1000)),
			Field:    "x",
			ColumnID: uint64(rand.Intn(10)),
		}
	}
	return cmds
}

// MustMarshalJSON marshals v into a string. Panic on error.
func MustMarshalJSON(v interface{}) string {
	buf, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(buf)
}

func sortedString(s string) string {
	arr := strings.Split(s, "")
	sort.Strings(arr)
	return strings.Join(arr, "")
}

// uint64Slice represents a sortable slice of uint64 numbers.
type uint64Slice []uint64

func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }

func TestClusteringNodesReplica1(t *testing.T) {
	cluster := test.MustRunCluster(t, 3)
	defer cluster.Close()

	err := cluster.AwaitState(pilosa.ClusterStateNormal, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}

	if err := cluster.GetNode(2).Command.Close(); err != nil {
		t.Fatalf("closing third node: %v", err)
	}

	// confirm that cluster stops accepting queries after one node closes
	if _, err := cluster.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{}); !strings.Contains(err.Error(), "not allowed in state STARTING") {
		t.Fatalf("got unexpected error querying an incomplete cluster: %v", err)
	}

	// Create new main with the same config.
	config := cluster.GetNode(2).Command.Config
	config.Translation.MapSize = 100000

	// this isn't necessary, but makes the test run way faster
	config.Gossip.Port = strconv.Itoa(int(cluster.GetNode(2).Command.GossipTransport().URI.Port))

	cluster.GetNode(2).Command = server.NewCommand(cluster.GetNode(2).Stdin, cluster.GetNode(2).Stdout, cluster.GetNode(2).Stderr, server.OptCommandServerOptions(pilosa.OptServerOpenTranslateStore(pilosa.OpenInMemTranslateStore)))
	cluster.GetNode(2).Command.Config = config

	// Run new program.
	if err := cluster.GetNode(2).Start(); err != nil {
		t.Fatalf("restarting node 2: %v", err)
	}

	err = cluster.AwaitState(pilosa.ClusterStateNormal, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("resuming normal operations: %v", err)
	}
}

func TestClusteringNodesReplica2(t *testing.T) {
	cluster := test.MustNewCluster(t, 3)
	for _, c := range cluster.Nodes {
		c.Config.Cluster.ReplicaN = 2
	}
	err := cluster.Start()
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	defer cluster.Close()

	err = cluster.AwaitState(pilosa.ClusterStateNormal, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}

	if err := cluster.GetNode(2).Command.Close(); err != nil {
		t.Fatalf("closing third node: %v", err)
	}

	err = cluster.AwaitCoordinatorState(pilosa.ClusterStateDegraded, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("after closing first server: %v", err)
	}

	// confirm that cluster keeps accepting queries if replication > 1
	if _, err := cluster.GetNode(0).API.CreateIndex(context.Background(), "anewindex", pilosa.IndexOptions{}); err != nil {
		t.Fatalf("got unexpected error creating index: %v", err)
	}

	// confirm that cluster stops accepting queries if 2 nodes fail and replication == 2
	if err := cluster.GetNode(1).Command.Close(); err != nil {
		t.Fatalf("closing 2nd node: %v", err)
	}

	err = cluster.AwaitCoordinatorState(pilosa.ClusterStateStarting, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("after closing second server: %v", err)
	}

	if _, err := cluster.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{}); !strings.Contains(err.Error(), "not allowed in state STARTING") {
		t.Fatalf("got unexpected error querying an incomplete cluster: %v", err)
	}

	// Create new main with the same config.
	config := cluster.GetNode(2).Command.Config
	config.Translation.MapSize = 100000
	// config.Bind = cluster.GetNode(2).API.Node().URI.HostPort()

	// this isn't necessary, but makes the test run way faster
	config.Gossip.Port = strconv.Itoa(int(cluster.GetNode(2).Command.GossipTransport().URI.Port))

	cluster.GetNode(2).Command = server.NewCommand(cluster.GetNode(2).Stdin, cluster.GetNode(2).Stdout, cluster.GetNode(2).Stderr, server.OptCommandServerOptions(pilosa.OptServerOpenTranslateStore(pilosa.OpenInMemTranslateStore)))
	cluster.GetNode(2).Command.Config = config

	// Run new program.
	if err := cluster.GetNode(2).Start(); err != nil {
		t.Fatalf("restarting node 2: %v", err)
	}

	err = cluster.AwaitCoordinatorState(pilosa.ClusterStateDegraded, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("after restarting first server: %v", err)
	}

	// Create new main with the same config.
	config = cluster.GetNode(1).Command.Config
	// config.Bind = cluster.GetNode(1).API.Node().URI.HostPort()
	config.Translation.MapSize = 100000

	// this isn't necessary, but makes the test run way faster
	config.Gossip.Port = strconv.Itoa(int(cluster.GetNode(1).Command.GossipTransport().URI.Port))

	cluster.GetNode(1).Command = server.NewCommand(cluster.GetNode(1).Stdin, cluster.GetNode(1).Stdout, cluster.GetNode(1).Stderr, server.OptCommandServerOptions(pilosa.OptServerOpenTranslateStore(pilosa.OpenInMemTranslateStore)))
	cluster.GetNode(1).Command.Config = config

	// Run new program.
	if err := cluster.GetNode(1).Start(); err != nil {
		t.Fatalf("restarting node 1: %v", err)
	}

	err = cluster.AwaitState(pilosa.ClusterStateNormal, 200*time.Microsecond)
	if err != nil {
		t.Fatalf("resuming normal operations: %v", err)
	}
}

func TestRemoveNodeAfterItDies(t *testing.T) {
	cluster := test.MustNewCluster(t, 3)
	for _, c := range cluster.Nodes {
		c.Config.Cluster.ReplicaN = 2
	}
	err := cluster.Start()
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	// The anonymous function is necessary so that the slice
	// passed to Close() as a receiver is the modified value
	// of cluster, because we're removing the last entry from it
	// below.
	defer func() {
		cluster.Close()
	}()

	err = cluster.AwaitState(pilosa.ClusterStateNormal, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}

	// prevent double-closing cluster.GetNode(2) from the deferred Close above
	disabled := cluster.GetNode(2)
	if err := cluster.CloseAndRemove(2); err != nil {
		t.Fatalf("closing third node: %v", err)
	}

	err = cluster.AwaitCoordinatorState(pilosa.ClusterStateDegraded, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}

	if _, err := cluster.GetNode(0).API.RemoveNode(disabled.API.Node().ID); err != nil {
		t.Fatalf("removing failed node: %v", err)
	}

	err = cluster.AwaitCoordinatorState(pilosa.ClusterStateNormal, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("removing disabled node: %v", err)
	}

	hosts := cluster.GetNode(0).API.Hosts(context.Background())
	if len(hosts) != 2 {
		t.Fatalf("unexpected hosts: %v", hosts)
	}
}

func TestRemoveConcurrentIndexCreation(t *testing.T) {
	cluster := test.MustNewCluster(t, 3)
	for _, c := range cluster.Nodes {
		c.Config.Cluster.ReplicaN = 2
	}
	err := cluster.Start()
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	defer cluster.Close()
	err = cluster.AwaitState(pilosa.ClusterStateNormal, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}

	errc := make(chan error)
	go func() {
		_, err := cluster.GetNode(0).API.CreateIndex(context.Background(), "blah", pilosa.IndexOptions{})
		errc <- err
	}()

	if _, err := cluster.GetNode(0).API.RemoveNode(cluster.GetNode(2).API.Node().ID); err != nil {
		t.Fatalf("removing node: %v", err)
	}

	err = cluster.AwaitCoordinatorState(pilosa.ClusterStateNormal, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}

	hosts := cluster.GetNode(0).API.Hosts(context.Background())
	if len(hosts) != 2 {
		t.Fatalf("unexpected hosts: %v", hosts)
	}
	if err := <-errc; err != nil {
		t.Fatalf("error from index creation: %v", err)
	}
}

// Ensure program imports timestamps as UTC.
func TestMain_ImportTimestamp(t *testing.T) {
	m := test.RunCommand(t)
	defer m.Close()

	indexName := "i"
	fieldName := "f"

	// Create index.
	if _, err := m.API.CreateIndex(context.Background(), indexName, pilosa.IndexOptions{}); err != nil {
		t.Fatal(err)
	}

	// Create field.
	if _, err := m.API.CreateField(context.Background(), indexName, fieldName, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD"))); err != nil {
		t.Fatal(err)
	}

	data := pilosa.ImportRequest{
		Index:      indexName,
		Field:      fieldName,
		Shard:      0,
		RowIDs:     []uint64{1, 2},
		ColumnIDs:  []uint64{1, 2},
		Timestamps: []int64{1514764800000000000, 1577833200000000000}, // 2018-01-01T00:00, 2019-12-31T23:00
	}

	// Import data.
	qcx := m.API.Txf().NewQcx()
	if err := m.API.Import(context.Background(), qcx, &data); err != nil { /// first write i/0 here. 2nd write here.
		t.Fatal(err)
	}
	if err := qcx.Finish(); err != nil {
		t.Fatal(err)
	}
	// Ensure the correct views were created.
	dir := fmt.Sprintf("%s/%s/%s/views", m.Config.DataDir, indexName, fieldName)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	exp := []string{
		"standard", "standard_2018", "standard_201801", "standard_20180101",
		"standard_2019", "standard_201912", "standard_20191231",
	}
	got := []string{}
	for _, f := range files {
		got = append(got, f.Name())
	}

	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("expected %v, but got %v", exp, got)
	}
}

func TestMain_ImportTimestampNoStandardView(t *testing.T) {
	m := test.RunCommand(t)
	defer m.Close()

	indexName := "i"
	fieldName := "f-no-standard"

	// Create index.
	if _, err := m.API.CreateIndex(context.Background(), indexName, pilosa.IndexOptions{}); err != nil {
		t.Fatal(err)
	}

	// Create field.
	if _, err := m.API.CreateField(context.Background(), indexName, fieldName, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD"), true)); err != nil {
		t.Fatal(err)
	}

	data := pilosa.ImportRequest{
		Index:      indexName,
		Field:      fieldName,
		Shard:      0,
		RowIDs:     []uint64{1, 2},
		ColumnIDs:  []uint64{1, 2},
		Timestamps: []int64{1514764800000000000, 1577833200000000000}, // 2018-01-01T00:00, 2019-12-31T23:00
	}

	// Import data.
	qcx := m.API.Txf().NewQcx()
	if err := m.API.Import(context.Background(), qcx, &data); err != nil {
		t.Fatal(err)
	}
	if err := qcx.Finish(); err != nil {
		t.Fatal(err)
	}

	// Ensure the correct views were created.
	dir := fmt.Sprintf("%s/%s/%s/views", m.Config.DataDir, indexName, fieldName)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	exp := []string{
		"standard_2018", "standard_201801", "standard_20180101",
		"standard_2019", "standard_201912", "standard_20191231",
	}
	got := []string{}
	for _, f := range files {
		got = append(got, f.Name())
	}

	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("expected %v, but got %v", exp, got)
	}
}

func TestClusterQueriesAfterRestart(t *testing.T) {
	cluster := test.MustRunCluster(t, 3)
	defer cluster.Close()
	cmd1 := cluster.GetNode(1)

	for _, com := range cluster.Nodes {
		nodes := com.API.Hosts(context.Background())
		for _, n := range nodes {
			if n.State != "READY" {
				t.Fatalf("unexpected node state after upping cluster: %v", nodes)
			}
		}
	}

	cmd1.MustCreateIndex(t, "testidx", pilosa.IndexOptions{})
	cmd1.MustCreateField(t, "testidx", "testfield", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 10))

	// build a query to set the first bit in 100 shards
	query := strings.Builder{}
	for i := 0; i < 100; i++ {
		query.WriteString(fmt.Sprintf("Set(%d, testfield=0)", i*pilosa.ShardWidth))
	}
	_, err := cmd1.API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "testidx",
		Query: query.String(),
	})
	if err != nil {
		t.Fatalf("setting 100 bits in 100 shards: %v", err)
	}

	results, err := cmd1.API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "testidx",
		Query: "Count(Row(testfield=0))",
	})
	if err != nil {
		t.Fatalf("counting row: %v", err)
	}
	if results.Results[0].(uint64) != 100 {
		t.Fatalf("Count should be 100, but got %v of type %[1]T", results.Results[0])
	}

	err = cmd1.Command.Close()
	if err != nil {
		t.Fatalf("closing node0: %v", err)
	}

	// confirm that cluster stops accepting queries after one node closes
	if _, err := cluster.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{}); !strings.Contains(err.Error(), "not allowed in state STARTING") {
		t.Fatalf("got unexpected error querying an incomplete cluster: %v", err)
	}

	// Create new main with the same config.
	config := cmd1.Command.Config
	config.Bind = cmd1.API.Node().URI.HostPort()

	// this isn't necessary, but makes the test run way faster
	config.Gossip.Port = strconv.Itoa(int(cmd1.Command.GossipTransport().URI.Port))
	cmd1.Command = server.NewCommand(cmd1.Stdin, cmd1.Stdout, cmd1.Stderr, server.OptCommandServerOptions(pilosa.OptServerOpenTranslateStore(pilosa.OpenInMemTranslateStore)))
	cmd1.Command.Config = config
	err = cmd1.Start()
	if err != nil {
		t.Fatalf("reopening node 0: %v", err)
	}

	for cmd1.API.State() != pilosa.ClusterStateNormal {
		time.Sleep(time.Millisecond)
	}

	results, err = cmd1.API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "testidx",
		Query: "Count(Row(testfield=0))",
	})
	if err != nil {
		t.Fatalf("counting row: %v", err)
	}
	if results.Results[0].(uint64) != 100 {
		t.Fatalf("Count should be 100, but got %v of type %[1]T", results.Results[0])
	}

}

// TODO: confirm that things keep working if a node is hard-closed (no nodeLeave event) and immediately restarted with a different address.

func TestClusterExhaustingConnections(t *testing.T) {
	if !runStress {
		t.Skip("stress")
	}
	cluster := test.MustRunCluster(t, 5)
	defer cluster.Close()
	cmd1 := cluster.GetNode(1)

	for _, com := range cluster.Nodes {
		nodes := com.API.Hosts(context.Background())
		for _, n := range nodes {
			if n.State != "READY" {
				t.Fatalf("unexpected node state after upping cluster: %v", nodes)
			}
		}
	}

	cmd1.MustCreateIndex(t, "testidx", pilosa.IndexOptions{})
	cmd1.MustCreateField(t, "testidx", "testfield", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 10))

	eg := errgroup.Group{}
	for i := 0; i < 20; i++ {
		i := i
		eg.Go(func() error {
			for j := i; j < 10000; j += 20 {
				_, err := cluster.GetNode(i%5).API.Query(context.Background(), &pilosa.QueryRequest{
					Index: "testidx",
					Query: fmt.Sprintf("Set(%d, testfield=0)", j*pilosa.ShardWidth),
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		t.Fatalf("setting lots of shards: %v", err)
	}
}

func TestQueryingWithQuotesAndStuff(t *testing.T) {
	m := test.RunCommand(t)
	defer m.Close()

	client, err := http.NewInternalClient(m.API.Node().URI.HostPort(), http.GetHTTPClient(nil))
	if err != nil {
		t.Fatal(err)
	}

	// Execute Set() commands.
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{Keys: true}); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateFieldWithOptions(context.Background(), "i", "fld", pilosa.FieldOptions{Keys: true}); err != nil {
		t.Fatal(err)
	}

	// Test escaped single quote gets set properly
	if res, err := m.Query(t, "i", "", `Set('bl\'ah', fld=ha)`); err != nil {
		t.Fatal(err)
	} else if !strings.Contains(res, "[true]") {
		t.Errorf("setting escaped single quote result: %s", res)
	}
	if res, err := m.Query(t, "i", "", `Row(fld=ha)`); err != nil {
		t.Fatal(err)
	} else if !strings.Contains(res, `bl'ah`) {
		t.Errorf("value with escaped single quote set improperly: %s", res)
	}

	// Test escaped double quote gets set properly
	if res, err := m.Query(t, "i", "", `Set("d\"ah", fld=dq)`); err != nil {
		t.Fatal(err)
	} else if !strings.Contains(res, "[true]") {
		t.Errorf("value with escaped double quote set improperly: %s", res)
	}
	if res, err := m.Query(t, "i", "", `Row(fld=dq)`); err != nil {
		t.Fatal(err)
	} else if !strings.Contains(res, `d\"ah`) {
		// the backslash is there because JSON needs to escape the
		// double quote since it uses double quotes
		t.Errorf("value with escaped double quote set improperly: %s", res)
	}
}

func TestClusterExhaustingConnectionsImport(t *testing.T) {
	if !runStress {
		t.Skip("stress")
	}
	cluster := test.MustRunCluster(t, 5)
	defer cluster.Close()
	cmd1 := cluster.GetNode(1)

	for _, com := range cluster.Nodes {
		nodes := com.API.Hosts(context.Background())
		for _, n := range nodes {
			if n.State != "READY" {
				t.Fatalf("unexpected node state after upping cluster: %v", nodes)
			}
		}
	}

	cmd1.MustCreateIndex(t, "testidx", pilosa.IndexOptions{})
	cmd1.MustCreateField(t, "testidx", "testfield", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 10))

	bm := roaring.NewBitmap()
	bm.DirectAdd(0)
	buf := &bytes.Buffer{}
	_, err := bm.WriteTo(buf)
	if err != nil {
		t.Fatalf("writing to buffer: %v", err)
	}
	data := buf.Bytes()

	eg := errgroup.Group{}
	for i := uint64(0); i < 20; i++ {
		i := i
		eg.Go(func() error {
			for j := i; j < 10000; j += 20 {
				if (j-i)%1000 == 0 {
					fmt.Printf("%d is %.2f%% done.\n", i, float64(j-i)*100/100000)
				}
				err := cluster.GetNode(int(i%5)).API.ImportRoaring(context.Background(), "testidx", "testfield", j, false, &pilosa.ImportRoaringRequest{
					Views: map[string][]byte{
						"": data,
					},
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	err = eg.Wait()
	if err != nil {
		t.Fatalf("setting lots of shards: %v", err)
	}
}

func TestClusterMinMaxSumDecimal(t *testing.T) {
	cluster := test.MustRunCluster(t, 3)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	cmd.MustCreateIndex(t, "testdec", pilosa.IndexOptions{Keys: true, TrackExistence: true})
	cmd.MustCreateField(t, "testdec", "adec", pilosa.OptFieldTypeDecimal(2))

	test.Do(t, "POST", cluster.GetNode(0).URL()+"/index/testdec/query", `
Set("a", adec=42.2)
Set("b", adec=11.12)
Set("c", adec=13.41)
Set("d", adec=99.87)
Set("e", adec=11.13)
Set("f", adec=12.12)
Set("g", adec=15.52)
Set("h", adec=100.22)
`)

	result := test.Do(t, "POST", cluster.GetNode(0).URL()+"/index/testdec/query", "Sum(field=adec)")
	if !strings.Contains(result.Body, `"decimalValue":305.59`) {
		t.Fatalf("expected decimal sum of 305.59, but got: '%s'", result.Body)
	} else if !strings.Contains(result.Body, `"count":8`) {
		t.Fatalf("expected count 8, but got: '%s'", result.Body)
	}

	result = test.Do(t, "POST", cluster.GetNode(0).URL()+"/index/testdec/query", "Max(field=adec)")
	if !strings.Contains(result.Body, `"decimalValue":100.22`) {
		t.Fatalf("expected decimal max of 100.22, but got: '%s'", result.Body)
	} else if !strings.Contains(result.Body, `"count":1`) {
		t.Fatalf("expected count 1, but got: '%s'", result.Body)
	}

	result = test.Do(t, "POST", cluster.GetNode(0).URL()+"/index/testdec/query", "Min(field=adec)")
	if !strings.Contains(result.Body, `"decimalValue":11.12`) {
		t.Fatalf("expected decimal min of 11.12, but got: '%s'", result.Body)
	} else if !strings.Contains(result.Body, `"count":1`) {
		t.Fatalf("expected count 1, but got: '%s'", result.Body)
	}

}

func TestMain(m *testing.M) {
	port := pilosa.GetAvailPort()
	fmt.Printf("server/ TestMain: online stack-traces: curl http://localhost:%v/debug/pprof/goroutine?debug=2\n", port)
	go func() {
		_ = nethttp.ListenAndServe(fmt.Sprintf("127.0.0.1:%v", port), nil)
	}()
	os.Exit(m.Run())
}

// TestClusterCreatedAtRace is a regression test for an issue where
// creating the same field concurrently across the cluster could cause
// the createdAt value to be disagreed upon by various nodes in the
// cluster (which would cause ingest to fail).
func TestClusterCreatedAtRace(t *testing.T) {
	iterations := 1
	if runStress {
		iterations = 10
	}
	for k := 0; k < iterations; k++ {
		t.Run(fmt.Sprintf("run-%d", k), func(t *testing.T) {
			cluster := test.MustRunCluster(t, 4)
			defer cluster.Close()

			for _, com := range cluster.Nodes {
				nodes := com.API.Hosts(context.Background())
				for _, n := range nodes {
					if n.State != "READY" {
						t.Fatalf("unexpected node state after upping cluster: %v", nodes)
					}
				}
			}
			_, err := cluster.Nodes[0].API.CreateIndex(context.Background(), "anindex", pilosa.IndexOptions{})
			if err != nil && errors.Cause(err).Error() != pilosa.ErrIndexExists.Error() {
				t.Fatal(err)
			}

			eg := errgroup.Group{}
			for i := 0; i < 4; i++ {
				for _, cmd := range cluster.Nodes {
					cmd := cmd
					eg.Go(func() error {
						_, err := cmd.API.CreateField(context.Background(), "anindex", "afield")
						if err != nil && errors.Cause(err).Error() != pilosa.ErrFieldExists.Error() {
							return errors.Wrap(err, "creating field")
						}
						return nil
					})
				}
			}

			err = eg.Wait()
			if err != nil {
				t.Fatalf("creating indices and fields concurrently: %v", err)
			}

			schemas := make([]*pilosa.IndexInfo, len(cluster.Nodes))
			for i, cmd := range cluster.Nodes {
				schemas[i] = cmd.API.Schema(context.Background())[0]
			}

			createdAtField := schemas[0].Fields[0].CreatedAt
			for i, schema := range schemas[1:] {
				if schema.Fields[0].CreatedAt != createdAtField {
					t.Fatalf("node %d doesn't match node 0 for field. 0: %d, %d: %d", i, createdAtField, i, schema.Fields[0].CreatedAt)
				}
			}

		})
	}
}
