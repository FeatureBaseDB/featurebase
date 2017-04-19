package pilosa

import "time"

const (
	// DefaultHost is the default hostname and port to use.
	DefaultHost         = "localhost"
	DefaultPort         = "10101"
	DefaultClusterType  = "static"
	DefaultInternalPort = "14000"
)

// Config represents the configuration for the command.
type Config struct {
	DataDir string `toml:"data-dir"`
	Host    string `toml:"host"`

	Cluster struct {
		ReplicaN        int      `toml:"replicas"`
		Type            string   `toml:"type"`
		Nodes           []string `toml:"hosts"`
		InternalNodes   []string `toml:"internal-hosts"`
		PollingInterval Duration `toml:"polling-interval"`
		InternalPort    string   `toml:"internal-port"`
		GossipSeed      string   `toml:"gossip-seed"`
	} `toml:"cluster"`

	Plugins struct {
		Path string `toml:"path"`
	} `toml:"plugins"`

	AntiEntropy struct {
		Interval Duration `toml:"interval"`
	} `toml:"anti-entropy"`

	LogPath string `toml:"log-path"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		Host: DefaultHost + ":" + DefaultPort,
	}
	c.Cluster.ReplicaN = DefaultReplicaN
	c.Cluster.Type = DefaultClusterType
	c.Cluster.PollingInterval = Duration(DefaultPollingInterval)
	c.Cluster.Nodes = []string{}
	c.Cluster.InternalNodes = []string{}
	c.AntiEntropy.Interval = Duration(DefaultAntiEntropyInterval)
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

func (d Duration) MarshalText() (text []byte, err error) {
	return []byte(d.String()), nil
}
