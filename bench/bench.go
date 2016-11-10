package bench

import (
	"fmt"
	"sync"

	"strconv"

	"flag"
	"io/ioutil"

	"github.com/umbel/pilosa"
)

// Benchmarker is an interface to guide the creation of new pilosa benchmarks or
// benchmark components. It defines 2 methods, Init, and Run. These are separate
// methods so that benchmark running code can time only the running of the
// benchmark, and not any setup.
type Benchmarker interface {
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

type BenchmarkCmd interface {
	Benchmarker
	ConsumeFlags(args []string) ([]string, error)
	Usage() string
}

// SetBitBenchmark sets a bunch of bits in pilosa, and can be configured in a
// number of ways.
type SetBitBenchmark struct {
	cli *pilosa.Client
	// bits being set will all be greater than BaseBitmapID.
	BaseBitmapID int
	// profile ids used will all be greater than BaseProfileID.
	BaseProfileID int
	// Iterations is the number of bits that will be set by this Benchmark.
	Iterations int
	// Number of profiles to iterate through - in this way multiple bits may be
	// set on the same profile.
	NumProfiles int
	// DB to use in pilosa.
	DB string
}

func (b *SetBitBenchmark) Usage() string {
	return `
SetBitBenchmark sets a bunch of bits.

Usage: SetBitBenchmark [arguments]

The following arguments are available:

	-BaseBitmapID int
		bit num to start from

	-BaseProfileID int
		profile id num to start from

	-Iterations int
		number of bits to set

	-NumProfiles int
		number of profiles to loop through

	-DB string
		pilosa db to use
`[1:]
}

func (b *SetBitBenchmark) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("SetBitBenchmark", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&b.BaseBitmapID, "BaseBitmapID", 0, "bits being set will all be greater than BaseBitmapID")
	fs.IntVar(&b.BaseProfileID, "BaseProfileID", 0, "profile ids used will all be greater than BaseProfileID")
	fs.IntVar(&b.Iterations, "Iterations", 100, "Iterations is the number of bits that will be set by this Benchmark")
	fs.IntVar(&b.NumProfiles, "NumProfiles", 100, "number of profiles to iterate through")
	fs.StringVar(&b.DB, "DB", "benchdb", "pilosa DB to use")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Init connects to pilosa and sets the client on b.
func (b *SetBitBenchmark) Init(hosts []string) (err error) {
	b.cli, err = pilosa.NewClient(hosts[0])
	if err != nil {
		return err
	}
	if b.Iterations == 0 {
		b.Iterations = 100
	}
	if b.DB == "" {
		b.DB = "SetBitBenchmark"
	}
	return nil
}

// Run runs the SetBitBenchmark
func (b *SetBitBenchmark) Run(agentNum int) map[string]interface{} {
	results := make(map[string]interface{})
	if b.cli == nil {
		results["error"] = fmt.Errorf("No client set for SetBitBenchmark agent: %v", agentNum)
		return results
	}
	for n := 0; n < b.Iterations; n++ {
		iterID := agentizeNum(n, b.Iterations, agentNum)
		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", b.BaseBitmapID+iterID, b.BaseProfileID+iterID%b.NumProfiles)
		b.cli.ExecuteQuery("2", query, true)
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
	benchmarkers []Benchmarker
}

// Init calls Init for each benchmark. If there are any errors, it will return a
// non-nil error value.
func (pb *parallelBenchmark) Init(hosts []string) error {
	errors := make([]error, len(pb.benchmarkers))
	hadErr := false
	wg := sync.WaitGroup{}
	for i, b := range pb.benchmarkers {
		wg.Add(1)
		go func(i int, b Benchmarker) {
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
		go func(i int, b Benchmarker) {
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

// Parallel takes a variable number of Benchmarkers and returns a Benchmarker
// which combines them and will run them in parallel.
func Parallel(bs ...Benchmarker) Benchmarker {
	return &parallelBenchmark{
		benchmarkers: bs,
	}
}

type serialBenchmark struct {
	benchmarkers []Benchmarker
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

// Serial takes a variable number of Benchmarkers and returns a Benchmarker
// which combines then and will run each serially.
func Serial(bs ...Benchmarker) Benchmarker {
	return &serialBenchmark{
		benchmarkers: bs,
	}
}

var Benchmarks = map[string]Benchmarker{
	"SetContiguousBits": &SetBitBenchmark{Iterations: 100000, NumProfiles: 1000, DB: "setcontiguousbench"},
}
