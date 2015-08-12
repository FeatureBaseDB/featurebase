package main

import (
	"time"

	"github.com/umbel/pilosa/index"
	"github.com/umbel/pilosa/transport"
	"github.com/umbel/pilosa/util"
)

const (
	// DefaultLogPath is the default file path where the log will be written.
	DefaultLogPath = "/tmps"

	// DefaultLogLevel is the default logging level used by seelog.
	DefaultLogLevel = "info"

	// DefaultFragmentBase is the default path where fragments are stored.
	DefaultFragmentBase = "/tmp/single"
)

var (
	// DefaultSupportedFrames are the frames supported by default.
	DefaultSupportedFrames = [...]string{"b.n", "t.t", "l.n", "d", "p.n"}

	// DefaultETCDHosts are the default hosts to connect to.
	DefaultETCDHosts = [...]string{"http://127.0.0.1:4001"}
)

// Config represents the configuration for the command.
type Config struct {
	ID   *util.GUID `toml:"id"`
	Host string     `toml:"host"`

	TCP struct {
		Port int `toml:"port"`
	} `toml:"tcp"`

	HTTP struct {
		Port             int    `toml:"port"`
		DefaultDB        string `toml:"default-db"`
		RequestLogPath   string `toml:"request-log-path"`
		SetBitLogEnabled bool   `toml:"set-bit-log-enabled"`
	} `toml:"http"`

	Log struct {
		Path  string `toml:"path"`
		Level string `toml:"level"`
	}

	Storage struct {
		Backend  string   `toml:"backend"`
		Hosts    []string `toml:"hosts"`
		Keyspace string   `toml:"keyspace"`

		FragmentBase    string   `toml:"fragment-base"`
		SupportedFrames []string `toml:"supported-frames"`

		CassandraTimeWindow   Duration `toml:"cassandra-time-window"`
		CassandraMaxSizeBatch int      `toml:"cassandra-max-size-batch"`
	} `toml:"storage"`

	AWS struct {
		AccessKeyID     string `toml:"access-key-id"`
		SecretAccessKey string `toml:"secret-access-key"`
	} `toml:"aws"`

	LevelDB struct {
		Path string `toml:"path"`
	} `toml:"leveldb"`

	Statsd struct {
		Host string `toml:"host"`
	} `toml:"statsd"`

	Plugins struct {
		Path string `toml:"path"`
	} `toml:"plugins"`

	ETCD struct {
		Hosts                []string `toml:"hosts"`
		FragmentAllocLockTTL Duration `toml:"fragment-alloc-lock-ttl"`
	} `toml:"etcd"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() Config {
	var c Config
	c.Host = "localhost"
	c.TCP.Port = transport.DefaultTCPPort
	c.HTTP.Port = transport.DefaultHTTPPort
	c.Log.Path = DefaultLogPath
	c.Storage.Backend = index.DefaultBackend
	c.Storage.Hosts = index.DefaultStorageHosts[:]
	c.Storage.Keyspace = index.DefaultStorageKeyspace
	c.Storage.FragmentBase = DefaultFragmentBase
	c.Storage.SupportedFrames = DefaultSupportedFrames[:]
	c.Storage.CassandraTimeWindow = Duration(index.DefaultCassandraTimeWindow)
	c.Storage.CassandraMaxSizeBatch = index.DefaultCassandraMaxSizeBatch
	c.Statsd.Host = util.DefaultStatsdHost
	c.ETCD.Hosts = DefaultETCDHosts[:]
	return c
}

// Duration is a TOML wrapper type for time.Duration.
type Duration time.Duration

// String returns the string representation of the duration.
func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalText parses a TOML value into a duration value.
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}

	*d = Duration(v)
	return nil
}
