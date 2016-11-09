package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/umbel/pilosa/bench"
)

var Benchmarks = map[string]bench.Benchmarker{
	"SetContiguousBits": &bench.SetBitBenchmark{Iterations: 100000, NumProfiles: 1000, DB: "setcontiguousbench"},
}

func main() {
	m := NewMain()

	// Parse command line arguments.
	if err := m.ParseFlags(os.Args[1:]); err == flag.ErrHelp {
		fmt.Fprintln(m.Stderr, m.Usage())
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}

	// Execute the program.
	if err := m.Run(); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}
}

type Main struct {
	Benchmarks []bench.Benchmarker
	AgentNum   int
	Hosts      []string

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

func NewMain() *Main {
	return &Main{
		Benchmarks: []bench.Benchmarker{},
		Hosts:      []string{},
		AgentNum:   0,

		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

func (m *Main) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosa-bench-agent", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)

	var benchmarks string
	var pilosaHosts string

	fs.StringVar(&benchmarks, "benchmarks", "", "Comma separated list of benchmarks to run")
	fs.StringVar(&pilosaHosts, "hosts", "localhost:15000", "Comma separated list of host:port")
	fs.IntVar(&m.AgentNum, "agentNum", 0, "An integer differentiating this agent from other in the fleet.")

	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, bmName := range strings.Split(benchmarks, ",") {
		if bm, ok := Benchmarks[bmName]; ok {
			m.Benchmarks = append(m.Benchmarks, bm)
		} else {
			return fmt.Errorf("%v is not a configured benchmark", bmName)
		}
	}
	m.Hosts = strings.Split(pilosaHosts, ",")

	return nil
}

func (m *Main) Usage() string {
	return `
pilosa-benchmark-agent is a tool for running a set of benchmarks against a pilosa cluster.

Usage:

	pilosa-benchmark-agent [arguments]

The following arguments are available:

	-benchmarks
      Comma separated list of benchmarks.

  -hosts
      Comma separated list of host:port describing all hosts in the cluster.

	-agentNum N
      An integer differentiating this agent from others in the fleet.
`[1:]
}

func (m *Main) Run() error {
	sbm := bench.Serial(m.Benchmarks...)
	err := sbm.Init(m.Hosts)
	if err != nil {
		return fmt.Errorf("in m.Run initialization: %v", err)
	}
	res := sbm.Run(m.AgentNum)
	fmt.Fprintln(m.Stdout, res)
	return nil
}
