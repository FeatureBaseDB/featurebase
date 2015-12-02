package main_test

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/umbel/pilosa/cmd/pilosa"
)

// Ensure the host can be parsed.
func TestConfig_Parse_Host(t *testing.T) {
	if c, err := ParseConfig(`host = "local"`); err != nil {
		t.Fatal(err)
	} else if c.Host != "local" {
		t.Fatalf("unexpected host: %s", c.Host)
	}
}

// Ensure the addr can be parsed.
func TestConfig_Parse_Addr(t *testing.T) {
	if c, err := ParseConfig(`addr = ":80"`); err != nil {
		t.Fatal(err)
	} else if c.Addr != ":80" {
		t.Fatalf("unexpected addr: %s", c.Addr)
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

// ParseConfig parses s into a config.
func ParseConfig(s string) (main.Config, error) {
	var c main.Config
	_, err := toml.Decode(s, &c)
	return c, err
}
