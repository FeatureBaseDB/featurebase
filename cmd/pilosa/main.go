package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"time"

	"github.com/BurntSushi/toml"
	log "github.com/cihub/seelog"
	"github.com/coreos/go-etcd/etcd"
	"github.com/kr/s3/s3util"
	"github.com/mitchellh/panicwrap"
	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/core"
	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/dispatch"
	"github.com/umbel/pilosa/executor"
	"github.com/umbel/pilosa/hold"
	"github.com/umbel/pilosa/transport"
	"github.com/umbel/pilosa/util"
)

// Build holds the build information passed in at compile time.
var Build string

func main() {
	m := NewMain()
	if err := m.Run(os.Args[1:]...); err != nil {
		fmt.Fprintln(m.Stderr, err.Error())
		os.Exit(-1)
	}
}

// Main represents the main program execution.
type Main struct {
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run executes the main program execution.
func (m *Main) Run(args ...string) error {
	defer log.Flush()

	// Handle panics with a log entry and process exit.
	if code, err := panicwrap.BasicWrap(func(output string) {
		log.Critical("The child panicked:\n\n", output)
		os.Exit(1)
	}); err != nil {
		panic(err)
	} else if code >= 0 {
		os.Exit(code)
	}

	// Parse command line arguments.
	opt, err := m.ParseFlags(args)
	if err != nil {
		return err
	}

	// Parse configuration.
	config := NewConfig()
	if opt.ConfigPath != "" {
		if _, err := toml.DecodeFile(opt.ConfigPath, &config); err != nil {
			return err
		}
	}

	// Generate an ID if one is not specified in the config.
	id := config.ID
	if id == nil {
		*id = util.RandomUUID()
	}

	// Set up profiling.
	if opt.CPUProfile != "" {
		f, err := os.Create(opt.CPUProfile)
		if err != nil {
			return err
		}

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Pass configuration to packages.
	// NOTE: This is temporary. These config options should be encapsulated in the types.
	db.SupportedFrames = config.Storage.SupportedFrames
	pilosa.FragmentBase = config.Storage.FragmentBase
	pilosa.Backend = config.Storage.Backend
	pilosa.LevelDBPath = config.LevelDB.Path

	// Initialize AWS storage.
	s3util.DefaultConfig.AccessKey = config.AWS.AccessKeyID
	s3util.DefaultConfig.SecretKey = config.AWS.SecretAccessKey

	// Initialize Statsd.
	util.StatsdHost = config.Statsd.Host
	util.SetupStatsd()

	// Initialize logging.
	logger, _ := log.LoggerFromConfigAsBytes([]byte(SeelogProductionConfig(config.Log.Path, *id, config.Log.Level)))
	log.ReplaceLogger(logger)

	// Initialize etcd client.
	etcdClient := etcd.NewClient(config.ETCD.Hosts)

	// Initialize the cluster.
	cluster := db.NewCluster()

	// Create index.
	idx := pilosa.NewFragmentContainer()

	// Initialize the holder.
	hold := hold.NewHolder()

	// Initialize process map.
	processMap := core.NewProcessMap()

	// Start process mapper.
	processMapper := core.NewProcessMapper("/pilosa/0")
	processMapper.ID = *id
	processMapper.TCPPort = config.TCP.Port
	processMapper.HTTPPort = config.HTTP.Port
	processMapper.Host = config.Host
	processMapper.ProcessMap = processMap
	processMapper.EtcdClient = etcdClient

	// Start topology mapper.
	topologyMapper := core.NewTopologyMapper("/pilosa/0")
	topologyMapper.Cluster = cluster
	topologyMapper.ProcessMap = processMap
	topologyMapper.EtcdClient = etcdClient
	topologyMapper.Index = idx
	topologyMapper.SupportedFrames = config.Storage.SupportedFrames
	topologyMapper.FragmentAllocLockTTL = time.Duration(config.ETCD.FragmentAllocLockTTL)

	// Start the transport.
	transport := transport.NewTcpTransport(*id)
	transport.Port = config.TCP.Port
	transport.ProcessMap = processMap
	go transport.Run()

	// Create the pinger.
	pinger := core.NewPinger(*id)
	pinger.Hold = hold
	pinger.Transport = transport

	// Create the batcher.
	batcher := core.NewBatcher(*id)
	batcher.Cluster = cluster
	batcher.Hold = hold
	batcher.Transport = transport

	// Start the web service.
	core.RequestLogPath = config.HTTP.RequestLogPath
	ws := core.NewWebService()
	ws.ID = *id
	ws.Port = config.HTTP.Port
	ws.Version = Build
	ws.DefaultDB = config.HTTP.DefaultDB
	ws.SetBitLogEnabled = config.HTTP.SetBitLogEnabled
	ws.Cluster = cluster
	ws.TopologyMapper = topologyMapper
	ws.Pinger = pinger
	ws.Batcher = batcher

	// Start the executor.
	ex := executor.NewExecutor(*id)
	ex.ProcessMap = processMap
	ex.PluginsPath = config.Plugins.Path
	ex.Hold = hold
	ex.Index = idx

	// Start the dispatcher.
	dispatch := dispatch.NewDispatch()
	dispatch.Executor = ex
	dispatch.Hold = hold
	dispatch.Index = idx
	dispatch.Transport = transport
	go dispatch.Run()

	fmt.Printf("Pilosa %s\n", Build)

	log.Warn("STOP")

	return nil
}

// ParseFlags parses command line flags from args.
func (m *Main) ParseFlags(args []string) (Options, error) {
	var opt Options
	fs := flag.NewFlagSet("pilosa", flag.ContinueOnError)
	fs.SetOutput(m.Stderr)
	fs.StringVar(&opt.ConfigPath, "config", "", "config path")
	fs.StringVar(&opt.CPUProfile, "cpuprofile", "", "write cpu profile to file")

	if err := fs.Parse(args); err != nil {
		return opt, err
	}

	return opt, nil
}

// Options represents the command line options.
type Options struct {
	CPUProfile string
	ConfigPath string
}
