package pilosa

import "time"

const (
	// DefaultHost is the default hostname and port to use.
	DefaultHost       = "localhost:15000"
	DefaultGossipPort = 14000
)

// Config represents the configuration for the command.
type Config struct {
	DataDir string `toml:"data-dir"`
	Host    string `toml:"host"`

	Cluster struct {
		ReplicaN        int           `toml:"replicas"`
		Nodes           []*ConfigNode `toml:"node"`
		PollingInterval Duration      `toml:"polling-interval"`
		Gossip          *ConfigGossip `toml:"gossip"`
	} `toml:"cluster"`

	Plugins struct {
		Path string `toml:"path"`
	} `toml:"plugins"`

	AntiEntropy struct {
		Interval Duration `toml:"interval"`
	} `toml:"anti-entropy"`
}

type ConfigNode struct {
	Host string `toml:"host"`
}

type ConfigGossip struct {
	Port int    `toml:"port"`
	Seed string `toml:"seed"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		Host: DefaultHost,
	}
	c.Cluster.ReplicaN = DefaultReplicaN
	c.Cluster.PollingInterval = Duration(DefaultPollingInterval)
	c.AntiEntropy.Interval = Duration(DefaultAntiEntropyInterval)
	return c
}

func NewConfigForHosts(hosts []string) *Config {
	conf := NewConfig()
	for _, hostport := range hosts {
		conf.Cluster.Nodes = append(conf.Cluster.Nodes, &ConfigNode{Host: hostport})
	}
	return conf
}

// PilosaCluster returns a new instance of Cluster based on the config.
func (c *Config) PilosaCluster() *Cluster {
	cluster := NewCluster()
	cluster.ReplicaN = c.Cluster.ReplicaN

	for _, n := range c.Cluster.Nodes {
		cluster.Nodes = append(cluster.Nodes, &Node{Host: n.Host})
	}

	// setup gossip if specified
	if c.Cluster.Gossip != nil {
		gossipPort := DefaultGossipPort
		gossipSeed := DefaultHost
		if c.Cluster.Gossip.Port != 0 {
			gossipPort = c.Cluster.Gossip.Port
		}
		if c.Cluster.Gossip.Seed != "" {
			gossipSeed = c.Cluster.Gossip.Seed
		}
		cluster.NodeSet = NewGossipNodeSet(c.Host, gossipPort, gossipSeed)
	} else {
		cluster.NodeSet = NewStaticNodeSet()
	}

	return cluster
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
