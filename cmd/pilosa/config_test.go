package main_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/umbel/pilosa/cmd/pilosa"
)

// Ensure the ID can be parsed as a GUID.
func TestConfig_Parse_ID(t *testing.T) {
	if c, err := ParseConfig(`
id = "00000000-0000-0000-0000-000000000001"
`); err != nil {
		t.Fatal(err)
	} else if c.ID == nil || c.ID.String() != `00000000-0000-0000-0000-000000000001` {
		t.Fatalf("unexpected id: %s", c.ID)
	}
}

// Ensure that parsing an invalid GUID returns an error.
func TestConfig_Parse_ID_ErrInvalid(t *testing.T) {
	if _, err := ParseConfig(`
id = "x"
`); err == nil || err.Error() != `Type mismatch for 'main.Config.id': invalid GUID "x"` {
		t.Fatal(err)
	}
}

// Ensure the host can be parsed.
func TestConfig_Parse_Host(t *testing.T) {
	if c, err := ParseConfig(`host = "localhost"`); err != nil {
		t.Fatal(err)
	} else if c.Host != "localhost" {
		t.Fatalf("unexpected host: %s", c.Host)
	}
}

// Ensure the "tcp" config can be parsed.
func TestConfig_Parse_TCP(t *testing.T) {
	if c, err := ParseConfig(`
[tcp]
port = 123
`); err != nil {
		t.Fatal(err)
	} else if c.TCP.Port != 123 {
		t.Fatalf("unexpected port: %s", c.TCP.Port)
	}
}

// Ensure the "http" config can be parsed.
func TestConfig_Parse_HTTP(t *testing.T) {
	if c, err := ParseConfig(`
[http]
port = 123
default-db = "xyz"
request-log-path = "/path/to/log"
set-bit-log-enabled = true
`); err != nil {
		t.Fatal(err)
	} else if c.HTTP.Port != 123 {
		t.Fatalf("unexpected port: %s", c.HTTP.Port)
	} else if c.HTTP.DefaultDB != "xyz" {
		t.Fatalf("unexpected default db: %s", c.HTTP.DefaultDB)
	} else if c.HTTP.RequestLogPath != "/path/to/log" {
		t.Fatalf("unexpected request log path: %s", c.HTTP.RequestLogPath)
	} else if c.HTTP.SetBitLogEnabled != true {
		t.Fatalf("unexpected set bit log enabled: %v", c.HTTP.SetBitLogEnabled)
	}
}

// Ensure the "log" config can be parsed.
func TestConfig_Parse_Log(t *testing.T) {
	if c, err := ParseConfig(`
[log]
path = "/path/to/log"
level = "debug"
`); err != nil {
		t.Fatal(err)
	} else if c.Log.Path != "/path/to/log" {
		t.Fatalf("unexpected path: %s", c.Log.Path)
	} else if c.Log.Level != "debug" {
		t.Fatalf("unexpected level: %s", c.Log.Level)
	}
}

// Ensure the "storage" config can be parsed.
func TestConfig_Parse_Storage(t *testing.T) {
	if c, err := ParseConfig(`
[storage]
backend = "cassandra"
hosts = ["server0", "server1"]
keyspace = "pilosa"
fragment-base = "/path/to/base"
supported-frames = ["a", "b", "c"]
cassandra-time-window = "5s"
cassandra-max-size-batch = 50
`); err != nil {
		t.Fatal(err)
	} else if c.Storage.Backend != "cassandra" {
		t.Fatalf("unexpected backend: %s", c.Storage.Backend)
	} else if !reflect.DeepEqual(c.Storage.Hosts, []string{"server0", "server1"}) {
		t.Fatalf("unexpected hosts: %+v", c.Storage.Hosts)
	} else if c.Storage.Keyspace != "pilosa" {
		t.Fatalf("unexpected keyspace: %s", c.Storage.Keyspace)
	} else if c.Storage.FragmentBase != "/path/to/base" {
		t.Fatalf("unexpected fragment base: %s", c.Storage.FragmentBase)
	} else if !reflect.DeepEqual(c.Storage.SupportedFrames, []string{"a", "b", "c"}) {
		t.Fatalf("unexpected supported frames: %s", c.Storage.SupportedFrames)
	} else if time.Duration(c.Storage.CassandraTimeWindow) != 5*time.Second {
		t.Fatalf("unexpected cassandra time window: %s", time.Duration(c.Storage.CassandraTimeWindow))
	} else if c.Storage.CassandraMaxSizeBatch != 50 {
		t.Fatalf("unexpected cassandra max size batch: %s", c.Storage.CassandraMaxSizeBatch)
	}
}

// Ensure the "aws" config can be parsed.
func TestConfig_Parse_AWS(t *testing.T) {
	if c, err := ParseConfig(`
[aws]
access-key-id = "abc"
secret-access-key = "def"
`); err != nil {
		t.Fatal(err)
	} else if c.AWS.AccessKeyID != "abc" {
		t.Fatalf("unexpected access key id: %s", c.AWS.AccessKeyID)
	} else if c.AWS.SecretAccessKey != "def" {
		t.Fatalf("unexpected secret access key: %s", c.AWS.SecretAccessKey)
	}
}

// Ensure the "leveldb" config can be parsed.
func TestConfig_Parse_LevelDB(t *testing.T) {
	if c, err := ParseConfig(`
[leveldb]
path = "/path/to/db"
`); err != nil {
		t.Fatal(err)
	} else if c.LevelDB.Path != "/path/to/db" {
		t.Fatalf("unexpected path: %s", c.LevelDB.Path)
	}
}

// Ensure the "statsd" config can be parsed.
func TestConfig_Parse_Statsd(t *testing.T) {
	if c, err := ParseConfig(`
[statsd]
host = "localhost"
`); err != nil {
		t.Fatal(err)
	} else if c.Statsd.Host != "localhost" {
		t.Fatalf("unexpected host: %s", c.Statsd.Host)
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

// Ensure the "etcd" config can be parsed.
func TestConfig_Parse_ETCD(t *testing.T) {
	if c, err := ParseConfig(`
[etcd]
hosts = ["127.0.0.1"]
fragment-alloc-lock-ttl = "5m"
`); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(c.ETCD.Hosts, []string{"127.0.0.1"}) {
		t.Fatalf("unexpected hosts: %+v", c.ETCD.Hosts)
	} else if time.Duration(c.ETCD.FragmentAllocLockTTL) != 5*time.Minute {
		t.Fatalf("unexpected fragment alloc lock ttl: %v", c.ETCD.FragmentAllocLockTTL)
	}
}

// ParseConfig parses s into a config.
func ParseConfig(s string) (main.Config, error) {
	var c main.Config
	_, err := toml.Decode(s, &c)
	return c, err
}
