package main_test

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
	main "github.com/pilosa/pilosa/cmd/pilosa"
)

// Ensure program can process queries and maintain consistency.
func TestMain_Set_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}

	if err := quick.Check(func(cmds []SetCommand) bool {
		m := MustRunMain()
		defer m.Close()

		// Execute SetBit() commands.
		for _, cmd := range cmds {
			if _, err := m.Query("db=d", fmt.Sprintf(`SetBit(id=%d, frame=%q, profileID=%d)`, cmd.ID, cmd.Frame, cmd.ProfileID)); err != nil {
				t.Fatal(err)
			}
		}

		// Validate data.
		for frame, frameSet := range SetCommands(cmds).Frames() {
			for id, profileIDs := range frameSet {
				exp := MustMarshalJSON(map[string]interface{}{
					"results": []interface{}{
						map[string]interface{}{
							"bits":  profileIDs,
							"attrs": map[string]interface{}{},
						},
					},
				}) + "\n"
				if res, err := m.Query("db=d", fmt.Sprintf(`Bitmap(id=%d, frame=%q)`, id, frame)); err != nil {
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
			for id, profileIDs := range frameSet {
				exp := MustMarshalJSON(map[string]interface{}{
					"results": []interface{}{
						map[string]interface{}{
							"bits":  profileIDs,
							"attrs": map[string]interface{}{},
						},
					},
				}) + "\n"
				if res, err := m.Query("db=d", fmt.Sprintf(`Bitmap(id=%d, frame=%q)`, id, frame)); err != nil {
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

// Ensure program can set bitmap attributes and retrieve them.
func TestMain_SetBitmapAttrs(t *testing.T) {
	m := MustRunMain()
	defer m.Close()

	// Set bits on different bitmaps in different frames.
	if _, err := m.Query("db=d", `SetBit(id=1, frame="x.n", profileID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("db=d", `SetBit(id=2, frame="x.n", profileID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("db=d", `SetBit(id=2, frame="z", profileID=100)`); err != nil {
		t.Fatal(err)
	}

	// Set bitmap attributes.
	if _, err := m.Query("db=d", `SetBitmapAttrs(id=1, frame="x.n", x=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("db=d", `SetBitmapAttrs(id=2, frame="x.n", x=200)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("db=d", `SetBitmapAttrs(id=2, frame="z", x=300)`); err != nil {
		t.Fatal(err)
	}

	// Query bitmap x.n/1.
	if res, err := m.Query("db=d", `Bitmap(id=1, frame="x.n")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	// Query bitmap x.n/2.
	if res, err := m.Query("db=d", `Bitmap(id=2, frame="x.n")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":200},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query bitmap after reopening.
	if res, err := m.Query("db=d&profiles=true", `Bitmap(id=1, frame="x.n")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{"x":100},"bits":[100]}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
	}
}

// Ensure program can set profile attributes and retrieve them.
func TestMain_SetProfileAttrs(t *testing.T) {
	m := MustRunMain()
	defer m.Close()

	// Set bits on bitmap.
	if _, err := m.Query("db=d", `SetBit(id=1, frame="x.n", profileID=100)`); err != nil {
		t.Fatal(err)
	} else if _, err := m.Query("db=d", `SetBit(id=1, frame="x.n", profileID=101)`); err != nil {
		t.Fatal(err)
	}

	// Set profile attributes.
	if _, err := m.Query("db=d", `SetProfileAttrs(id=100, foo="bar")`); err != nil {
		t.Fatal(err)
	}

	// Query bitmap.
	if res, err := m.Query("db=d&profiles=true", `Bitmap(id=1, frame="x.n")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,101]}],"profiles":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result: %s", res)
	}

	if err := m.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Query bitmap after reopening.
	if res, err := m.Query("db=d&profiles=true", `Bitmap(id=1, frame="x.n")`); err != nil {
		t.Fatal(err)
	} else if res != `{"results":[{"attrs":{},"bits":[100,101]}],"profiles":[{"id":100,"attrs":{"foo":"bar"}}]}`+"\n" {
		t.Fatalf("unexpected result(reopen): %s", res)
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

	// Write data on first cluster.
	if _, err := m0.Query("db=d", `
		SetBit(id=1, frame="f", profileID=100)
		SetBit(id=1, frame="f", profileID=1000)
		SetBit(id=1, frame="f", profileID=100000)
		SetBit(id=1, frame="f", profileID=200000)
		SetBit(id=1, frame="f", profileID=400000)
		SetBit(id=1, frame="f", profileID=600000)
		SetBit(id=1, frame="f", profileID=800000)
	`); err != nil {
		t.Fatal(err)
	}

	// Query bitmap on first cluster.
	if res, err := m0.Query("db=d", `Bitmap(id=1, frame="f")`); err != nil {
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
	} else if err := client.RestoreFrame(context.Background(), m0.Server.Host, "d", "f"); err != nil {
		t.Fatal(err)
	}

	// Query bitmap on second cluster.
	if res, err := m2.Query("db=d", `Bitmap(id=1, frame="f")`); err != nil {
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
	*main.Main

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

	m := &Main{Main: main.NewMain()}
	m.Config.DataDir = path
	m.Config.Host = "localhost:0"
	m.Main.Stdin = &m.Stdin
	m.Main.Stdout = &m.Stdout
	m.Main.Stderr = &m.Stderr

	if testing.Verbose() {
		m.Main.Stdout = io.MultiWriter(os.Stdout, m.Main.Stdout)
		m.Main.Stderr = io.MultiWriter(os.Stderr, m.Main.Stderr)
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
	return m.Main.Close()
}

// Reopen closes the program and reopens it.
func (m *Main) Reopen() error {
	if err := m.Main.Close(); err != nil {
		return err
	}

	// Create new main with the same config.
	config := m.Config
	m.Main = main.NewMain()
	m.Config = config

	// Run new program.
	if err := m.Run(); err != nil {
		return err
	}
	return nil
}

// URL returns the base URL string for accessing the running program.
func (m *Main) URL() string { return "http://" + m.Server.Addr().String() }

// Query executes a query against the program through the HTTP API.
func (m *Main) Query(rawQuery, query string) (string, error) {
	resp := MustDo("POST", m.URL()+"/query?"+rawQuery, query)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return resp.Body, nil
}

// SetCommand represents a command to set a bit.
type SetCommand struct {
	ID        uint64
	Frame     string
	ProfileID uint64
}

type SetCommands []SetCommand

// Frames returns the set of profile ids for each frame/bitmap.
func (a SetCommands) Frames() map[string]map[uint64][]uint64 {
	// Create a set of unique commands.
	m := make(map[SetCommand]struct{})
	for _, cmd := range a {
		m[cmd] = struct{}{}
	}

	// Build unique ids for each frame & bitmap.
	frames := make(map[string]map[uint64][]uint64)
	for cmd := range m {
		if frames[cmd.Frame] == nil {
			frames[cmd.Frame] = make(map[uint64][]uint64)
		}
		frames[cmd.Frame][cmd.ID] = append(frames[cmd.Frame][cmd.ID], cmd.ProfileID)
	}

	// Sort each set of profile ids.
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
			ID:        uint64(rand.Intn(1000)),
			Frame:     "x.n",
			ProfileID: uint64(rand.Intn(10)),
		}
	}
	return cmds
}

// ParseConfig parses s into a Config.
func ParseConfig(s string) (main.Config, error) {
	var c main.Config
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
