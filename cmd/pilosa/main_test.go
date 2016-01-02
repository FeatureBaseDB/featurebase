package main_test

import (
	"bytes"
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
	main "github.com/umbel/pilosa/cmd/pilosa"
)

// Ensure program can process queries and maintain consistency.
func TestMain_Set_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}

	if err := quick.Check(func(cmds []SetCommand) bool {
		m := MustRunMain()
		defer m.Close()

		// Execute set() commands.
		for _, cmd := range cmds {
			if res, err := m.Query("d", fmt.Sprintf(`set(id=%d, frame=%q, profile_id=%d)`, cmd.ID, cmd.Frame, cmd.ProfileID)); err != nil {
				t.Fatal(err)
			} else if res != `{}`+"\n" {
				t.Fatalf("unexpected result: %s", res)
			}
		}

		// Validate data.
		for frame, frameSet := range SetCommands(cmds).Frames() {
			for id, profileIDs := range frameSet {
				exp := MustMarshalJSON(map[string]interface{}{"result": profileIDs}) + "\n"
				if res, err := m.Query("d", fmt.Sprintf(`get(id=%d, frame=%q)`, id, frame)); err != nil {
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
				exp := MustMarshalJSON(map[string]interface{}{"result": profileIDs}) + "\n"
				if res, err := m.Query("d", fmt.Sprintf(`get(id=%d, frame=%q)`, id, frame)); err != nil {
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
func (m *Main) URL() string { return "http://" + m.Addr().String() }

// Query executes a query against the program through the HTTP API.
func (m *Main) Query(db, query string) (string, error) {
	resp := MustDo("POST", m.URL()+"/query?db="+db, query)
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
