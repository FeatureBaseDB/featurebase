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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/quick"

	"github.com/pelletier/go-toml"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/http"
	"github.com/pilosa/pilosa/server"
	"github.com/pilosa/pilosa/test"
)

// Ensure program can process queries and maintain consistency.
func TestMain_Set_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}

	if err := quick.Check(func(cmds []SetCommand) bool {
		m := test.MustRunCommand()
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
			if _, err := m.Query("i", "", fmt.Sprintf(`Set(%d, %s=%d)`, cmd.ColumnID, cmd.Field, cmd.ID)); err != nil {
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
				if res, err := m.Query("i", "", fmt.Sprintf(`Row(%s=%d)`, field, id)); err != nil {
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
				if res, err := m.Query("i", "", fmt.Sprintf(`Row(%s=%d)`, field, id)); err != nil {
					t.Fatal(err)
				} else if res != exp {
					t.Fatalf("unexpected result (reopen):\n\ngot=%s\n\nexp=%s\n\n", res, exp)
				}
			}
		}

		return true
	}, &quick.Config{
		Values: func(values []reflect.Value, rand *rand.Rand) {
			values[0] = reflect.ValueOf(GenerateSetCommands(1000, rand))
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure program can set row attributes and retrieve them.
func TestMain_SetRowAttrs(t *testing.T) {
	m := test.MustRunCommand()
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
	if _, err := m.Query("i", "", `Set(100, x=1)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `Set(100, x=2)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `Set(100, x=2)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `Set(100, neg=3)`); err != nil {
		t.Fatal(err)
	}

	// Set row attributes.
	if _, err := m.Query("i", "", `SetRowAttrs(x, 1, x=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(x, 2, x=-200)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(z, 2, x=300)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(neg, 3, x=-0.44)`); err != nil {
		t.Fatal(err)
	}

	// Query row x/1.
	if res, err := m.Query("i", "", `Row(x=1)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"columns":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	// Query row x/2.
	if res, err := m.Query("i", "", `Row(x=2)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-200},"columns":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query rows after reopening.
	if res, err := m.Query("i", "columnAttrs=true", `Row(x=1)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"columns":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}

	if res, err := m.Query("i", "columnAttrs=true", `Row(neg=3)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-0.44},"columns":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}
	// Query row x/2.
	if res, err := m.Query("i", "", `Row(x=2)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-200},"columns":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}
}

// Ensure program can set column attributes and retrieve them.
func TestMain_SetColumnAttrs(t *testing.T) {
	m := test.MustRunCommand()
	defer m.Close()

	// Create fields.
	client := m.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client.CreateField(context.Background(), "i", "x"); err != nil {
		t.Fatal(err)
	}

	// Set columns on row.
	if _, err := m.Query("i", "", `Set(100, x=1)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `Set(101, x=1)`); err != nil {
		t.Fatal(err)
	}

	// Set column attributes.
	if _, err := m.Query("i", "", `SetColumnAttrs(100, foo="bar")`); err != nil {
		t.Fatal(err)
	}

	// Query row.
	if res, err := m.Query("i", "columnAttrs=true", `Row(x=1)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"columns":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query row after reopening.
	if res, err := m.Query("i", "columnAttrs=true", `Row(x=1)`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"columns":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}
}

// Ensure the host can be parsed.
func TestConfig_Parse_Host(t *testing.T) {
	if c, err := ParseConfig(`bind = "local"`); err != nil {
		t.Fatal(err)
	} else if c.Bind != "local" {
		t.Fatalf("unexpected host: %s", c.Bind)
	}
}

// Ensure the data directory can be parsed.
func TestConfig_Parse_DataDir(t *testing.T) {
	if c, err := ParseConfig(`data-dir = "/tmp/foo"`); err != nil {
		t.Fatal(err)
	} else if c.DataDir != "/tmp/foo" {
		t.Fatalf("unexpected data dir: %s", c.DataDir)
	}
}

// tempMkdir makes a temporary directory
func tempMkdir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "pilosatemp")
	if err != nil {
		t.Fatalf("failed to create test directory: %s", err)
	}
	return dir
}

func TestMain_RecalculateHashes(t *testing.T) {
	const clusterSize = 5
	cluster := test.MustRunCluster(t, clusterSize)

	// Create the schema.
	client0 := cluster[0].Client()
	if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal("create index:", err)
	}
	if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
		t.Fatal("create field:", err)
	}
	return

	// Set some columns
	data := []string{}
	for rowID := 1; rowID < 10; rowID++ {
		for columnID := 1; columnID < 100; columnID++ {
			data = append(data, fmt.Sprintf(`Set(%d, f=%d)`, columnID, rowID))
		}
	}
	if _, err := cluster[0].Query("i", "", strings.Join(data, "")); err != nil {
		t.Fatal("setting columns:", err)
	}

	// Calculate caches on the first node
	err := cluster[0].RecalculateCaches()
	if err != nil {
		t.Fatalf("recalculating caches: %v", err)
	}

	target := `{"results":[[{"id":7,"count":99},{"id":1,"count":99},{"id":9,"count":99},{"id":5,"count":99},{"id":4,"count":99},{"id":8,"count":99},{"id":2,"count":99},{"id":6,"count":99},{"id":3,"count":99}]]}`

	// Run a TopN query on all nodes. The result should be the same as the target.
	for _, m := range cluster {
		res, err := m.Query("i", "", `TopN(f)`)
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

// ParseConfig parses s into a Config.
func ParseConfig(s string) (server.Config, error) {
	var c server.Config
	err := toml.Unmarshal([]byte(s), &c)
	return c, err
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
