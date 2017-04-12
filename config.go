package pilosa

import (
	"net"
	"strconv"
	"time"
)

const (
	// DefaultHost is the default hostname and port to use.
	DefaultHost          = "localhost"
	DefaultPort          = "10101"
	DefaultMessengerType = "static"
	DefaultGossipPort    = "14000"
)

// Config represents the configuration for the command.
type Config struct {
	DataDir string `toml:"data-dir"`
	Host    string `toml:"host"`

	Cluster struct {
		ReplicaN        int          `toml:"replicas"`
		MessengerType   string       `toml:"messenger-type"`
		Nodes           []string     `toml:"hosts"`
		PollingInterval Duration     `toml:"polling-interval"`
		Gossip          ConfigGossip `toml:"gossip"`
	} `toml:"cluster"`

	Plugins struct {
		Path string `toml:"path"`
	} `toml:"plugins"`

	AntiEntropy struct {
		Interval Duration `toml:"interval"`
	} `toml:"anti-entropy"`

	LogPath string `toml:"log-path"`
}

type ConfigGossip struct {
	Port int    `toml:"port"`
	Seed string `toml:"seed"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		Host: DefaultHost + ":" + DefaultPort,
	}
	c.Cluster.ReplicaN = DefaultReplicaN
	c.Cluster.MessengerType = DefaultMessengerType
	c.Cluster.PollingInterval = Duration(DefaultPollingInterval)
	c.Cluster.Nodes = []string{}
	c.AntiEntropy.Interval = Duration(DefaultAntiEntropyInterval)
	return c
}

// NewConfigForHosts returns a Config object with Config.Cluster.Nodes already
// set up.
func NewConfigForHosts(hosts []string) *Config {
	conf := NewConfig()
	for _, hostport := range hosts {
		conf.Cluster.Nodes = append(conf.Cluster.Nodes, hostport)
	}
	return conf
}

// PilosaMessenger returns a new instance of Messenger based on the config.
func (c *Config) PilosaMessenger() *Messenger {
	messenger := NewMessenger()
	switch c.Cluster.MessengerType {
	case "broadcast":
		n := NewHTTPMessageBroker()
		n.messenger = messenger
		messenger.Broker = n
	case "gossip":
		n := NewGossipMessageBroker()
		n.messenger = messenger
		messenger.Broker = n
	case "static":
		// nop
	}
	return messenger
}

// PilosaCluster returns a new instance of Cluster based on the config.
func (c *Config) PilosaCluster() *Cluster {
	cluster := NewCluster()
	cluster.ReplicaN = c.Cluster.ReplicaN

	for _, hostport := range c.Cluster.Nodes {
		cluster.Nodes = append(cluster.Nodes, &Node{Host: hostport})
	}

	// Setup a Broadcast (over HTTP) or Gossip NodeSet based on config.
	switch c.Cluster.MessengerType {
	case "broadcast":
		cluster.NodeSet = NewHTTPNodeSet()
		cluster.NodeSet.(*HTTPNodeSet).Join(cluster.Nodes)
	case "gossip":
		gport, err := strconv.Atoi(DefaultGossipPort)
		if err != nil {
			// what?
		}
		gossipPort := gport
		gossipSeed := DefaultHost
		if c.Cluster.Gossip.Port != 0 {
			gossipPort = c.Cluster.Gossip.Port
		}
		if c.Cluster.Gossip.Seed != "" {
			gossipSeed = c.Cluster.Gossip.Seed
		}
		// get the host portion of addr to use for binding
		gossipHost, _, err := net.SplitHostPort(c.Host)
		if err != nil {
			gossipHost = c.Host
		}
		cluster.NodeSet = NewGossipNodeSet(c.Host, gossipHost, gossipPort, gossipSeed)
	case "static":
		cluster.NodeSet = NewStaticNodeSet()
	default:
		cluster.NodeSet = NewStaticNodeSet()
	}

	return cluster
}

// AssociateMessageBroker allows an implementation to associate objects to the MessageBroker
// after cluster configuration.
func (c *Config) AssociateMessageBroker(s *Server) {
	switch c.Cluster.MessengerType {
	case "broadcast":
		// nop
	case "gossip":
		s.Cluster.NodeSet.(*GossipNodeSet).config.memberlistConfig.Delegate = s.Messenger.Broker.(*GossipMessageBroker)
	case "static":
		// nop
	}
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
