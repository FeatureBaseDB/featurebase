package bench

import (
	"fmt"
	"sync"

	"strconv"

	"flag"
	"io/ioutil"

	"github.com/umbel/pilosa"
)

// Benchmark is an interface to guide the creation of new pilosa benchmarks or
// benchmark components. It defines 2 methods, Init, and Run. These are separate
// methods so that benchmark running code can time only the running of the
// benchmark, and not any setup.
type Benchmark interface {
	// Init takes a list of hosts and is generally expected to set up a
	// connection to pilosa using whatever client it chooses.
	Init(hosts []string) error

	// Run runs the benchmark. It takes an agentNum which should be used to
	// parameterize the benchmark if it is being run simultaneously on multiple
	// "agents". E.G. the agentNum might be used to make a random seed different
	// for each agent, or have each agent set a different set of bits. The return
	// value of Run is kept generic so that any relevant statistics or metrics
	// that may be specific to the benchmark in question can be reported.
	Run(agentNum int) map[string]interface{}
}

type Command interface {
	Benchmark
	ConsumeFlags(args []string) ([]string, error)
	Usage() string
}

// DiagonalSetBits sets bits with increasing profile id and bitmap id.
type DiagonalSetBits struct {
	cli *pilosa.Client
	// bits being set will all be greater than BaseBitmapID.
	BaseBitmapID int
	// profile ids used will all be greater than BaseProfileID.
	BaseProfileID int
	// Iterations is the number of bits that will be set by this Benchmark.
	Iterations int
	// DB to use in pilosa.
	DB string
}

func (b *DiagonalSetBits) Usage() string {
	return `
DiagonalSetBits sets bits with increasing profile id and bitmap id.

Usage: DiagonalSetBits [arguments]

The following arguments are available:

	-BaseBitmapID int
		bitmap id to start from

	-BaseProfileID int
		profile id num to start from

	-Iterations int
		number of bits to set

	-DB string
		pilosa db to use
`[1:]
}

func (b *DiagonalSetBits) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("DiagonalSetBits", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&b.BaseBitmapID, "BaseBitmapID", 0, "bits being set will all be greater than BaseBitmapID")
	fs.IntVar(&b.BaseProfileID, "BaseProfileID", 0, "profile ids used will all be greater than BaseProfileID")
	fs.IntVar(&b.Iterations, "Iterations", 100, "Iterations is the number of bits that will be set by this Benchmark")
	fs.StringVar(&b.DB, "DB", "benchdb", "pilosa DB to use")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Init connects to pilosa and sets the client on b.
func (b *DiagonalSetBits) Init(hosts []string) (err error) {
	b.cli, err = pilosa.NewClient(hosts[0])
	if err != nil {
		return err
	}
	if b.DB == "" {
		b.DB = "DiagonalSetBits"
	}
	return nil
}

// Run runs the DiagonalSetBits benchmark
func (b *DiagonalSetBits) Run(agentNum int) map[string]interface{} {
	results := make(map[string]interface{})
	if b.cli == nil {
		results["error"] = fmt.Errorf("No client set for DiagonalSetBits agent: %v", agentNum)
		return results
	}
	for n := 0; n < b.Iterations; n++ {
		iterID := agentizeNum(n, b.Iterations, agentNum)
		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", b.BaseBitmapID+iterID, b.BaseProfileID+iterID)
		b.cli.ExecuteQuery(b.DB, query, true)
	}
	return results
}

// agentizeNum is a helper which combines the loop iteration (n) with the total
// number of iterations and the agentNum in order to produce a globally unique
// number across all loop iterations on all agents.
func agentizeNum(n, iterations, agentNum int) int {
	return n + (agentNum * iterations)
}

type parallelBenchmark struct {
	benchmarkers []Benchmark
}

// Init calls Init for each benchmark. If there are any errors, it will return a
// non-nil error value.
func (pb *parallelBenchmark) Init(hosts []string) error {
	errors := make([]error, len(pb.benchmarkers))
	hadErr := false
	wg := sync.WaitGroup{}
	for i, b := range pb.benchmarkers {
		wg.Add(1)
		go func(i int, b Benchmark) {
			defer wg.Done()
			errors[i] = b.Init(hosts)
			if errors[i] != nil {
				hadErr = true
			}
		}(i, b)
	}
	wg.Wait()
	if hadErr {
		return fmt.Errorf("Had errs in parallelBenchmark.Init: %v", errors)
	}
	return nil
}

// Run runs the parallel benchmark and returns it's results in a nested map - the
// top level keys are the indices of each benchmark in the list of benchmarks,
// and the values are the results of each benchmark's Run method.
func (pb *parallelBenchmark) Run(agentNum int) map[string]interface{} {
	wg := sync.WaitGroup{}
	results := make(map[string]interface{}, len(pb.benchmarkers))
	resultsLock := sync.Mutex{}
	for i, b := range pb.benchmarkers {
		wg.Add(1)
		go func(i int, b Benchmark) {
			defer wg.Done()
			ret := b.Run(agentNum)
			resultsLock.Lock()
			results[strconv.Itoa(i)] = ret
			resultsLock.Unlock()
		}(i, b)
	}
	wg.Wait()
	return results
}

// Parallel takes a variable number of Benchmarks and returns a Benchmark
// which combines them and will run them in parallel.
func Parallel(bs ...Benchmark) Benchmark {
	return &parallelBenchmark{
		benchmarkers: bs,
	}
}

type serialBenchmark struct {
	benchmarkers []Benchmark
}

// Init calls Init for each benchmark. If there are any errors, it will return a
// non-nil error value.
func (sb *serialBenchmark) Init(hosts []string) error {
	errors := make([]error, len(sb.benchmarkers))
	hadErr := false
	for i, b := range sb.benchmarkers {
		errors[i] = b.Init(hosts)
		if errors[i] != nil {
			hadErr = true
		}
	}
	if hadErr {
		return fmt.Errorf("Had errs in serialBenchmark.Init: %v", errors)
	}
	return nil
}

// Run runs the serial benchmark and returns it's results in a nested map - the
// top level keys are the indices of each benchmark in the list of benchmarks,
// and the values are the results of each benchmark's Run method.
func (sb *serialBenchmark) Run(agentNum int) map[string]interface{} {
	results := make(map[string]interface{}, len(sb.benchmarkers))
	for i, b := range sb.benchmarkers {
		ret := b.Run(agentNum)
		results[strconv.Itoa(i)] = ret
	}
	return results
}

// Serial takes a variable number of Benchmarks and returns a Benchmark
// which combines then and will run each serially.
func Serial(bs ...Benchmark) Benchmark {
	return &serialBenchmark{
		benchmarkers: bs,
	}
}
