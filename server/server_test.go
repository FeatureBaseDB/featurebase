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
	"sort"
	"strings"
	"testing"
	"testing/quick"

	"github.com/BurntSushi/toml"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/server"
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
		client, err := pilosa.NewClient(m.Server.Host)
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
			if _, err := m.Query("i", "", fmt.Sprintf(`SetBit(id=%d, frame=%q, columnID=%d)`, cmd.ID, cmd.Frame, cmd.ColumnID)); err != nil {
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
				if res, err := m.Query("i", "", fmt.Sprintf(`Bitmap(id=%d, frame=%q)`, id, frame)); err != nil {
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
				if res, err := m.Query("i", "", fmt.Sprintf(`Bitmap(id=%d, frame=%q)`, id, frame)); err != nil {
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
	} else if err := client.CreateFrame(context.Background(), "i", "x.n", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if err := client.CreateFrame(context.Background(), "i", "z", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if err := client.CreateFrame(context.Background(), "i", "neg", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Set bits on different rows in different frames.
	if _, err := m.Query("i", "", `SetBit(id=1, frame="x.n", columnID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(id=2, frame="x.n", columnID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(id=2, frame="z", columnID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(id=3, frame="neg", columnID=100)`); err != nil {
		t.Fatal(err)
	}

	// Set row attributes.
	if _, err := m.Query("i", "", `SetRowAttrs(id=1, frame="x.n", x=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(id=2, frame="x.n", x=-200)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(id=2, frame="z", x=300)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetRowAttrs(id=3, frame="neg", x=-0.44)`); err != nil {
		t.Fatal(err)
	}

	// Query row x.n/1.
	if res, err := m.Query("i", "", `Bitmap(id=1, frame="x.n")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	// Query row x.n/2.
	if res, err := m.Query("i", "", `Bitmap(id=2, frame="x.n")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-200},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query rows after reopening.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(id=1, frame="x.n")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}

	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(id=3, frame="neg")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":-0.44},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}
	// Query row x.n/2.
	if res, err := m.Query("i", "", `Bitmap(id=2, frame="x.n")`); err != nil {
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
	} else if err := client.CreateFrame(context.Background(), "i", "x.n", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Set bits on row.
	if _, err := m.Query("i", "", `SetBit(id=1, frame="x.n", columnID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(id=1, frame="x.n", columnID=101)`); err != nil {
		t.Fatal(err)
	}

	// Set column attributes.
	if _, err := m.Query("i", "", `SetColumnAttrs(id=100, foo="bar")`); err != nil {
		t.Fatal(err)
	}

	// Query row.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(id=1, frame="x.n")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query row after reopening.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(id=1, frame="x.n")`); err != nil {
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
	} else if err := client.CreateFrame(context.Background(), "i", "x.n", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Set bits on row.
	if _, err := m.Query("i", "", `SetBit(id=1, frame="x.n", col=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("i", "", `SetBit(id=1, frame="x.n", col=101)`); err != nil {
		t.Fatal(err)
	}

	// Set column attributes.
	if _, err := m.Query("i", "", `SetColumnAttrs(col=100, foo="bar")`); err != nil {
		t.Fatal(err)
	}

	// Query row.
	if res, err := m.Query("i", "columnAttrs=true", `Bitmap(id=1, frame="x.n")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,101]}],"columnAttrs":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

}

// Ensure program can set bits on one cluster and then restore to a second cluster.
func TestMain_FrameRestore(t *testing.T) {
	m0 := MustRunMain()
	defer m0.Close()

	m1 := MustRunMain()
	defer m1.Close()

	// Update cluster config.
	m0.Server.Cluster.Nodes = []*pilosa.Node{
		{Host: m0.Server.Host},
		{Host: m1.Server.Host},
	}
	m1.Server.Cluster.Nodes = m0.Server.Cluster.Nodes

	// Create frames.
	client := m0.Client()
	if err := client.CreateIndex(context.Background(), "x", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client.CreateFrame(context.Background(), "x", "f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Write data on first cluster.
	if _, err := m0.Query("x", "", `
		SetBit(id=1, frame="f", columnID=100)
		SetBit(id=1, frame="f", columnID=1000)
		SetBit(id=1, frame="f", columnID=100000)
		SetBit(id=1, frame="f", columnID=200000)
		SetBit(id=1, frame="f", columnID=400000)
		SetBit(id=1, frame="f", columnID=600000)
		SetBit(id=1, frame="f", columnID=800000)
	`); err != nil {
		t.Fatal(err)
	}

	// Query row on first cluster.
	if res, err := m0.Query("x", "", `Bitmap(id=1, frame="f")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,1000,100000,200000,400000,600000,800000]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	// Start second cluster.
	m2 := MustRunMain()
	defer m2.Close()

	// Import from first cluster.
	client, err := pilosa.NewClient(m2.Server.Host)
	if err != nil {
		t.Fatal(err)
	} else if err := m2.Client().CreateIndex(context.Background(), "x", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := m2.Client().CreateFrame(context.Background(), "x", "f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if err := client.RestoreFrame(context.Background(), m0.Server.Host, "x", "f"); err != nil {
		t.Fatal(err)
	}

	// Query row on second cluster.
	if res, err := m2.Query("x", "", `Bitmap(id=1, frame="f")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,1000,100000,200000,400000,600000,800000]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}
}

// Ensure the host can be parsed.
func TestConfig_Parse_Host(t *testing.T) {
	if c, err := ParseConfig(`host = "local"`); err != nil {
		t.Fatal(err)
	} else if c.Host != "local" {
		t.Fatalf("unexpected host: %s", c.Host)
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

// Ensure the "plugins" config can be parsed.
func TestConfig_Parse_Plugins(t *testing.T) {
	if c, err := ParseConfig(`
[plugins]
path = "/path/to/plugins"
`); err != nil {
		t.Fatal(err)
	} else if c.Plugins.Path != "/path/to/plugins" {
		t.Fatalf("unexpected path: %s", c.Plugins.Path)
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
	m.Config.DataDir = path
	m.Config.Host = "localhost:0"
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
func (m *Main) Client() *pilosa.Client {
	client, err := pilosa.NewClient(m.Server.Host)
	if err != nil {
		panic(err)
	}
	return client
}

// Query executes a query against the program through the HTTP API.
func (m *Main) Query(index, rawQuery, query string) (string, error) {
	fmt.Println("Query:", index, query)
	resp := MustDo("POST", m.URL()+fmt.Sprintf("/index/%s/query?", index)+rawQuery, query)
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
			Frame:    "x.n",
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
