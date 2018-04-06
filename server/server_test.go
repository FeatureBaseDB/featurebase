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
	"runtime"
	"sort"
	"strings"
	"testing"
	"testing/quick"

	"github.com/BurntSushi/toml"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

// Ensure program can process queries and maintain consistency.
func TestMain_Set_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}

	if err := quick.Check(func(cmds []SetCommand) bool {
		m := test.MustRunMain()
		defer m.Close()

		// Create client.
		client, err := pilosa.NewInternalHTTPClient(m.Server.URI.HostPort(), pilosa.GetHTTPClient(nil))
		if err != nil {
			t.Fatal(err)
		}

		// Execute SetBit() commands.
		for _, cmd := range cmds {
			if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
				t.Fatal(err)
			}
			if err := client.CreateFrame(context.Background(), "i", cmd.Frame, pilosa.FrameOptions{}); err != nil && err != pilosa.ErrFrameExists {
				t.Fatal(err)
			}
			if _, err := m.Query("i", "", fmt.Sprintf(`SetBit(row=%d, frame=%q, col=%d)`, cmd.ID, cmd.Frame, cmd.ColumnID)); err != nil {
				t.Fatal(err)
			}
		}

		// Validate data.
		for frame, frameSet := range SetCommands(cmds).Frames() {
			for id, columnIDs := range frameSet {
				exp := MustMarshalJSON(map[string]interface{}{
					"results": []interface{}{
						map[string]interface{}{
							"bits":  columnIDs,
							"attrs": map[string]interface{}{},
						},
					},
				}) + "\n"
				if res, err := m.Query("i", "", fmt.Sprintf(`Bitmap(row=%d, frame=%q)`, id, frame)); err != nil {
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
		for frame, frameSet := range SetCommands(cmds).Frames() {
			for id, columnIDs := range frameSet {
				exp := MustMarshalJSON(map[string]interface{}{
					"results": []interface{}{
						map[string]interface{}{
							"bits":  columnIDs,
							"attrs": map[string]interface{}{},
						},
					},
				}) + "\n"
				if res, err := m.Query("i", "", fmt.Sprintf(`Bitmap(row=%d, frame=%q)`, id, frame)); err != nil {
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
	m := test.MustRunMain()
	defer m.Close()

	// Create frames.
	client := m.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client.CreateFrame(context.Background(), "i", "x", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if err := client.CreateFrame(context.Background(), "i", "z", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if err := client.CreateFrame(context.Background(), "i", "neg", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Set bits on different rows in different frames.
	if _, err := m.Query("i", "", `SetBit(row=1, frame="x", col=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(row=2, frame="x", col=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(row=2, frame="z", col=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(row=3, frame="neg", col=100)`); err != nil {
		t.Fatal(err)
	}

	// Set row attributes.
	if _, err := m.Query("i", "", `SetRowAttrs(row=1, frame="x", x=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(row=2, frame="x", x=-200)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(row=2, frame="z", x=300)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(row=3, frame="neg", x=-0.44)`); err != nil {
		t.Fatal(err)
	}

	// Query row x/1.
	if res, err := m.Query("i", "", `Bitmap(row=1, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	// Query row x/2.
	if res, err := m.Query("i", "", `Bitmap(row=2, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-200},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query rows after reopening.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(row=1, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}

	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(row=3, frame="neg")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-0.44},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}
	// Query row x/2.
	if res, err := m.Query("i", "", `Bitmap(row=2, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-200},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}
}

// Ensure program can set column attributes and retrieve them.
func TestMain_SetColumnAttrs(t *testing.T) {
	m := test.MustRunMain()
	defer m.Close()

	// Create frames.
	client := m.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client.CreateFrame(context.Background(), "i", "x", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Set bits on row.
	if _, err := m.Query("i", "", `SetBit(row=1, frame="x", col=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(row=1, frame="x", col=101)`); err != nil {
		t.Fatal(err)
	}

	// Set column attributes.
	if _, err := m.Query("i", "", `SetColumnAttrs(col=100, foo="bar")`); err != nil {
		t.Fatal(err)
	}

	// Query row.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(row=1, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query row after reopening.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(row=1, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}
}

// Ensure program can set bits on one cluster and then restore to a second cluster.
func TestMain_FrameRestore(t *testing.T) {
	mains1 := test.MustRunMainWithCluster(t, 2)
	m10 := mains1[0]
	m11 := mains1[1]

	// Create frames.
	client := m10.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal("create index:", err)
	}
	if err := client.CreateFrame(context.Background(), "i", "f", pilosa.FrameOptions{}); err != nil {
		t.Fatal("create frame:", err)
	}

	// Write data on first cluster.
	if _, err := m10.Query("i", "", `
		SetBit(row=1, frame="f", col=100)
		SetBit(row=1, frame="f", col=1000)
		SetBit(row=1, frame="f", col=100000)
		SetBit(row=1, frame="f", col=200000)
		SetBit(row=1, frame="f", col=400000)
		SetBit(row=1, frame="f", col=600000)
		SetBit(row=1, frame="f", col=800000)
	`); err != nil {
		t.Fatal("setting bits:", err)
	}

	// Query row on first cluster.
	if res, err := m10.Query("i", "", `Bitmap(row=1, frame="f")`); err != nil {
		t.Fatal("bitmap query:", err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,1000,100000,200000,400000,600000,800000]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	// Start second cluster.
	mains2 := test.MustRunMainWithCluster(t, 2)
	m20 := mains2[0]
	defer m20.Close()
	m21 := mains2[1]
	defer m21.Close()

	// Import from first cluster.
	client20, err := pilosa.NewInternalHTTPClient(m20.Server.URI.HostPort(), pilosa.GetHTTPClient(nil))
	if err != nil {
		t.Fatal("new client:", err)
	}
	client21, err := pilosa.NewInternalHTTPClient(m21.Server.URI.HostPort(), pilosa.GetHTTPClient(nil))
	if err != nil {
		t.Fatal("new client:", err)
	}

	if err := m20.Client().CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal("create new index:", err)
	}
	if err := m20.Client().CreateFrame(context.Background(), "i", "f", pilosa.FrameOptions{}); err != nil {
		t.Fatal("create new frame:", err)
	}

	if err := client20.RestoreFrame(context.Background(), m10.Server.URI.HostPort(), "i", "f"); err != nil {
		t.Fatal("restore frame:", err)
	}
	if err := client21.RestoreFrame(context.Background(), m11.Server.URI.HostPort(), "i", "f"); err != nil {
		t.Fatal("restore frame:", err)
	}

	// Query row on second cluster.
	if res, err := m20.Query("i", "", `Bitmap(row=1, frame="f")`); err != nil {
		t.Fatal("another bitmap query:", err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,1000,100000,200000,400000,600000,800000]}]}`+"\n" {
		t.Fatalf("2unexpected result: %s", res)
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

// Ensure the file handle count is working
func TestCountOpenFiles(t *testing.T) {
	// Windows is not supported yet
	if runtime.GOOS == "windows" {
		t.Skip("Skipping unsupported CountOpenFiles test on Windows.")
	}
	count, err := pilosa.CountOpenFiles()
	if err != nil {
		t.Errorf("CountOpenFiles failed: %s", err)
	}
	if count == 0 {
		t.Error("CountOpenFiles returned invalid value 0.")
	}
}

func TestMain_RecalculateHashes(t *testing.T) {
	const clusterSize = 5
	cluster := test.MustRunMainWithCluster(t, clusterSize)

	// Create the schema.
	client0 := cluster[0].Client()
	if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal("create index:", err)
	}
	if err := client0.CreateFrame(context.Background(), "i", "f", pilosa.FrameOptions{CacheType: "ranked"}); err != nil {
		t.Fatal("create frame:", err)
	}

	// Set some bits
	data := []string{}
	for rowID := 1; rowID < 10; rowID++ {
		for columnID := 1; columnID < 100; columnID++ {
			data = append(data, fmt.Sprintf(`SetBit(row=%d, frame="f", col=%d)`, rowID, columnID))
		}
	}
	if _, err := cluster[0].Query("i", "", strings.Join(data, "")); err != nil {
		t.Fatal("setting bits:", err)
	}

	// Calculate caches on the first node
	cluster[0].RecalculateCaches()
	target := `{"results":[[{"id":7,"count":99},{"id":1,"count":99},{"id":9,"count":99},{"id":5,"count":99},{"id":4,"count":99},{"id":8,"count":99},{"id":2,"count":99},{"id":6,"count":99},{"id":3,"count":99}]]}`

	// Run a TopN query on all nodes. The result should be the same as the target.
	for _, m := range cluster {
		res, err := m.Query("i", "", `TopN(frame="f")`)
		if err != nil {
			t.Fatal(err)
		}
		res = strings.TrimSpace(res)
		if sortedString(target) != sortedString(res) {
			t.Fatalf("%v != %v", target, res)
		}
	}
}

// SetCommand represents a command to set a bit.
type SetCommand struct {
	ID       uint64
	Frame    string
	ColumnID uint64
}

type SetCommands []SetCommand

// Frames returns the set of column ids for each frame/row.
func (a SetCommands) Frames() map[string]map[uint64][]uint64 {
	// Create a set of unique commands.
	m := make(map[SetCommand]struct{})
	for _, cmd := range a {
		m[cmd] = struct{}{}
	}

	// Build unique ids for each frame & row.
	frames := make(map[string]map[uint64][]uint64)
	for cmd := range m {
		if frames[cmd.Frame] == nil {
			frames[cmd.Frame] = make(map[uint64][]uint64)
		}
		frames[cmd.Frame][cmd.ID] = append(frames[cmd.Frame][cmd.ID], cmd.ColumnID)
	}

	// Sort each set of column ids.
	for _, frame := range frames {
		for id := range frame {
			sort.Sort(uint64Slice(frame[id]))
		}
	}

	return frames
}

// GenerateSetCommands generates random SetCommand objects.
func GenerateSetCommands(n int, rand *rand.Rand) []SetCommand {
	cmds := make([]SetCommand, rand.Intn(n))
	for i := range cmds {
		cmds[i] = SetCommand{
			ID:       uint64(rand.Intn(1000)),
			Frame:    "x",
			ColumnID: uint64(rand.Intn(10)),
		}
	}
	return cmds
}

// ParseConfig parses s into a Config.
func ParseConfig(s string) (pilosa.Config, error) {
	var c pilosa.Config
	_, err := toml.Decode(s, &c)
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
