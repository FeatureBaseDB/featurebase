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
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/gossip"
	"github.com/pilosa/pilosa/server"
	"github.com/pilosa/pilosa/test"
)

// Ensure program can process queries and maintain consistency.
func TestMain_Set_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}

	if err := quick.Check(func(cmds []SetCommand) bool {
		m := MustRunMain()
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
			if _, err := m.Query("i", "", fmt.Sprintf(`SetBit(rowID=%d, frame=%q, columnID=%d)`, cmd.ID, cmd.Frame, cmd.ColumnID)); err != nil {
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
				if res, err := m.Query("i", "", fmt.Sprintf(`Bitmap(rowID=%d, frame=%q)`, id, frame)); err != nil {
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
				if res, err := m.Query("i", "", fmt.Sprintf(`Bitmap(rowID=%d, frame=%q)`, id, frame)); err != nil {
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
	m := MustRunMain()
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
	if _, err := m.Query("i", "", `SetBit(rowID=1, frame="x", columnID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(rowID=2, frame="x", columnID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(rowID=2, frame="z", columnID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(rowID=3, frame="neg", columnID=100)`); err != nil {
		t.Fatal(err)
	}

	// Set row attributes.
	if _, err := m.Query("i", "", `SetRowAttrs(rowID=1, frame="x", x=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(rowID=2, frame="x", x=-200)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(rowID=2, frame="z", x=300)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(rowID=3, frame="neg", x=-0.44)`); err != nil {
		t.Fatal(err)
	}

	// Query row x/1.
	if res, err := m.Query("i", "", `Bitmap(rowID=1, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	// Query row x/2.
	if res, err := m.Query("i", "", `Bitmap(rowID=2, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-200},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query rows after reopening.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(rowID=1, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}

	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(rowID=3, frame="neg")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-0.44},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}
	// Query row x/2.
	if res, err := m.Query("i", "", `Bitmap(rowID=2, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-200},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}
}

// Ensure program can set column attributes and retrieve them.
func TestMain_SetColumnAttrs(t *testing.T) {
	m := MustRunMain()
	defer m.Close()

	// Create frames.
	client := m.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client.CreateFrame(context.Background(), "i", "x", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Set bits on row.
	if _, err := m.Query("i", "", `SetBit(rowID=1, frame="x", columnID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(rowID=1, frame="x", columnID=101)`); err != nil {
		t.Fatal(err)
	}

	// Set column attributes.
	if _, err := m.Query("i", "", `SetColumnAttrs(id=100, foo="bar")`); err != nil {
		t.Fatal(err)
	}

	// Query row.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(rowID=1, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query row after reopening.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(rowID=1, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}
}

// Ensure program can set column attributes with columnLabel option.
func TestMain_SetColumnAttrsWithColumnOption(t *testing.T) {
	m := MustRunMain()
	defer m.Close()

	// Create frames.
	client := m.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{ColumnLabel: "col"}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client.CreateFrame(context.Background(), "i", "x", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Set bits on row.
	if _, err := m.Query("i", "", `SetBit(rowID=1, frame="x", col=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(rowID=1, frame="x", col=101)`); err != nil {
		t.Fatal(err)
	}

	// Set column attributes.
	if _, err := m.Query("i", "", `SetColumnAttrs(col=100, foo="bar")`); err != nil {
		t.Fatal(err)
	}

	// Query row.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(rowID=1, frame="x")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

}

// Ensure program can set bits on one cluster and then restore to a second cluster.
func TestMain_FrameRestore(t *testing.T) {
	m0 := MustRunMain()
	// TODO: this test used to start a two node cluster, but there was a race
	// condition with anti-entropy. We need some general code for starting up
	// arbitrarily sized Pilosa clusters for testing, and then we should
	// re-instate the multi-node nature of this test.

	// Create frames.
	client := m0.Client()
	if err := client.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal("create index:", err)
	} else if err := client.CreateFrame(context.Background(), "i", "f", pilosa.FrameOptions{}); err != nil {
		t.Fatal("create frame:", err)
	}

	// Write data on first cluster.
	if _, err := m0.Query("i", "", `
		SetBit(rowID=1, frame="f", columnID=100)
		SetBit(rowID=1, frame="f", columnID=1000)
		SetBit(rowID=1, frame="f", columnID=100000)
		SetBit(rowID=1, frame="f", columnID=200000)
		SetBit(rowID=1, frame="f", columnID=400000)
		SetBit(rowID=1, frame="f", columnID=600000)
		SetBit(rowID=1, frame="f", columnID=800000)
	`); err != nil {
		t.Fatal("setting bits:", err)
	}

	// Query row on first cluster.
	if res, err := m0.Query("i", "", `Bitmap(rowID=1, frame="f")`); err != nil {
		t.Fatal("bitmap query:", err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,1000,100000,200000,400000,600000,800000]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	// Start second cluster.
	m2 := MustRunMain()
	defer m2.Close()

	// Import from first cluster.
	client, err := pilosa.NewInternalHTTPClient(m2.Server.URI.HostPort(), pilosa.GetHTTPClient(nil))
	if err != nil {
		t.Fatal("new client:", err)
	} else if err := m2.Client().CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal("create new index:", err)
	} else if err := m2.Client().CreateFrame(context.Background(), "i", "f", pilosa.FrameOptions{}); err != nil {
		t.Fatal("create new frame:", err)
	} else if err := client.RestoreFrame(context.Background(), m0.Server.URI.HostPort(), "i", "f"); err != nil {
		t.Fatal("restore frame:", err)
	}

	// Query row on second cluster.
	if res, err := m2.Query("i", "", `Bitmap(rowID=1, frame="f")`); err != nil {
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

// Ensure program can send/receive broadcast messages.
func TestMain_SendReceiveMessage(t *testing.T) {
	t.SkipNow()
	// TODO re-enable this test when we have a better way of
	// creating a multi-node cluster that doesn't have data races.
	m0 := MustRunMain()
	defer m0.Close()

	m1 := MustRunMain()
	defer m1.Close()

	// Update cluster config
	m0.Server.Cluster.Nodes = []*pilosa.Node{
		{Host: m0.Server.URI.HostPort()},
		{Host: m1.Server.URI.HostPort()},
	}
	m1.Server.Cluster.Nodes = m0.Server.Cluster.Nodes

	// Configure node0

	// get the host portion of addr to use for binding
	gossipHost := m0.Server.URI.Host()
	gossipPort := 0
	gossipSeed := ""

	gossipNodeSet0, err := gossip.NewGossipNodeSet(m0.Server.URI.HostPort(), gossipHost, gossipPort, gossipSeed, m0.Server, nil)
	if err != nil {
		t.Fatal(err)
	}
	m0.Server.Cluster.NodeSet = gossipNodeSet0
	m0.Server.Broadcaster = m0.Server
	m0.Server.Gossiper = gossipNodeSet0
	m0.Server.Handler.Broadcaster = m0.Server.Broadcaster
	m0.Server.Holder.Broadcaster = m0.Server.Broadcaster
	m0.Server.BroadcastReceiver = gossipNodeSet0

	if err := m0.Server.BroadcastReceiver.Start(m0.Server); err != nil {
		t.Fatal(err)
	}
	// Open NodeSet communication
	if err := m0.Server.Cluster.NodeSet.Open(); err != nil {
		t.Fatal(err)
	}

	// Configure node1

	// get the host portion of addr to use for binding
	gossipHost = m1.Server.URI.Host()
	gossipPort = 0
	gossipSeed = gossipNodeSet0.Seed()

	gossipNodeSet1, err := gossip.NewGossipNodeSet(m1.Server.URI.HostPort(), gossipHost, gossipPort, gossipSeed, m1.Server, nil)
	if err != nil {
		t.Fatal(err)
	}
	m1.Server.Cluster.NodeSet = gossipNodeSet1
	m1.Server.Broadcaster = m1.Server
	m1.Server.Gossiper = gossipNodeSet1
	m1.Server.Handler.Broadcaster = m1.Server.Broadcaster
	m1.Server.Holder.Broadcaster = m1.Server.Broadcaster
	m1.Server.BroadcastReceiver = gossipNodeSet1

	if err := m1.Server.BroadcastReceiver.Start(m1.Server); err != nil {
		t.Fatal(err)
	}
	// Open NodeSet communication
	if err := m1.Server.Cluster.NodeSet.Open(); err != nil {
		t.Fatal(err)
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// Expected indexes and Frames
	expected := map[string][]string{
		"i": []string{"f"},
	}

	// Create a client for each node.
	client0 := m0.Client()
	client1 := m1.Client()

	// Create indexes and frames on one node.
	if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client0.CreateFrame(context.Background(), "i", "f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Make sure node0 knows about the index and frame created.
	schema0, err := client0.Schema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	received0 := map[string][]string{}
	for _, idx := range schema0 {
		received0[idx.Name] = []string{}
		for _, frame := range idx.Frames {
			received0[idx.Name] = append(received0[idx.Name], frame.Name)
		}
	}
	if !reflect.DeepEqual(received0, expected) {
		t.Fatalf("unexpected schema on node0: %s", received0)
	}

	// Make sure node1 knows about the index and frame created.
	schema1, err := client1.Schema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	received1 := map[string][]string{}
	for _, idx := range schema1 {
		received1[idx.Name] = []string{}
		for _, frame := range idx.Frames {
			received1[idx.Name] = append(received1[idx.Name], frame.Name)
		}
	}
	if !reflect.DeepEqual(received1, expected) {
		t.Fatalf("unexpected schema on node1: %s", received1)
	}

	// Write data on first node.
	if _, err := m0.Query("i", "", `
            SetBit(rowID=1, frame="f", columnID=1)
            SetBit(rowID=1, frame="f", columnID=2400000)
        `); err != nil {
		t.Fatal(err)
	}

	// We have to wait for the broadcast message to be sent before checking state.
	time.Sleep(1 * time.Second)

	// Make sure node0 knows about the latest MaxSlice.
	maxSlices0, err := client0.MaxSliceByIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if maxSlices0["i"] != 2 {
		t.Fatalf("unexpected maxSlice on node0: %d", maxSlices0["i"])
	}

	// Make sure node1 knows about the latest MaxSlice.
	maxSlices1, err := client1.MaxSliceByIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if maxSlices1["i"] != 2 {
		t.Fatalf("unexpected maxSlice on node1: %d", maxSlices1["i"])
	}

	// Write input definition to the first node.
	if _, err := m0.CreateDefinition("i", "test", `{
            "frames": [{"name": "event-time",
                        "options": {
                            "cacheType": "ranked",
                            "timeQuantum": "YMD"
                        }}],
            "fields": [{"name": "columnID",
                        "primaryKey": true
                        }]}
        `); err != nil {
		t.Fatal(err)
	}

	// We have to wait for the broadcast message to be sent before checking state.
	time.Sleep(1 * time.Second)

	frame0 := m0.Server.Holder.Frame("i", "event-time")
	if frame0 == nil {
		t.Fatal("frame not found")
	}
	frame1 := m1.Server.Holder.Frame("i", "event-time")
	if frame1 == nil {
		t.Fatal("frame not found")
	}
}

// Main represents a test wrapper for main.Main.
type Main struct {
	*server.Command

	Stdin  bytes.Buffer
	Stdout bytes.Buffer
	Stderr bytes.Buffer
}

// NewMain returns a new instance of Main with a temporary data directory and random port.
func NewMain() *Main {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	m := &Main{Command: server.NewCommand(os.Stdin, os.Stdout, os.Stderr)}
	m.Server.Network = *test.Network
	m.Config.DataDir = path
	m.Config.Bind = "localhost:0"
	m.Config.Cluster.Type = "static"
	m.Command.Stdin = &m.Stdin
	m.Command.Stdout = &m.Stdout
	m.Command.Stderr = &m.Stderr

	if testing.Verbose() {
		m.Command.Stdout = io.MultiWriter(os.Stdout, m.Command.Stdout)
		m.Command.Stderr = io.MultiWriter(os.Stderr, m.Command.Stderr)
	}

	return m
}

// MustRunMain returns a new, running Main. Panic on error.
func MustRunMain() *Main {
	m := NewMain()
	m.Config.Metric.Diagnostics = false // Disable diagnostics.
	if err := m.Run(); err != nil {
		panic(err)
	}
	return m
}

// Close closes the program and removes the underlying data directory.
func (m *Main) Close() error {
	defer os.RemoveAll(m.Config.DataDir)
	return m.Command.Close()
}

// Reopen closes the program and reopens it.
func (m *Main) Reopen() error {
	if err := m.Command.Close(); err != nil {
		return err
	}

	// Create new main with the same config.
	config := m.Config
	m.Command = server.NewCommand(os.Stdin, os.Stdout, os.Stderr)
	m.Server.Network = *test.Network
	m.Config = config

	// Run new program.
	if err := m.Run(); err != nil {
		return err
	}
	return nil
}

// URL returns the base URL string for accessing the running program.
func (m *Main) URL() string { return "http://" + m.Server.Addr().String() }

// Client returns a client to connect to the program.
func (m *Main) Client() *pilosa.InternalHTTPClient {
	client, err := pilosa.NewInternalHTTPClient(m.Server.URI.HostPort(), pilosa.GetHTTPClient(nil))
	if err != nil {
		panic(err)
	}
	return client
}

// Query executes a query against the program through the HTTP API.
func (m *Main) Query(index, rawQuery, query string) (string, error) {
	resp := MustDo("POST", m.URL()+fmt.Sprintf("/index/%s/query?", index)+rawQuery, query)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return resp.Body, nil
}

// CreateDefinition.
func (m *Main) CreateDefinition(index, def, query string) (string, error) {
	resp := MustDo("POST", m.URL()+fmt.Sprintf("/index/%s/input-definition/%s", index, def), query)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return resp.Body, nil
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

// MustDo executes http.Do() with an http.NewRequest(). Panic on error.
func MustDo(method, urlStr string, body string) *httpResponse {
	req, err := http.NewRequest(method, urlStr, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	return &httpResponse{Response: resp, Body: string(buf)}
}

// httpResponse is a wrapper for http.Response that holds the Body as a string.
type httpResponse struct {
	*http.Response
	Body string
}

// MustMarshalJSON marshals v into a string. Panic on error.
func MustMarshalJSON(v interface{}) string {
	buf, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(buf)
}

// uint64Slice represents a sortable slice of uint64 numbers.
type uint64Slice []uint64

func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
