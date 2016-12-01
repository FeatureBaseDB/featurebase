package bench

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Benchmark is an interface to guide the creation of new pilosa benchmarks or
// benchmark components. It defines 2 methods, Init, and Run. These are separate
// methods so that benchmark running code can time only the running of the
// benchmark, and not any setup.
type Benchmark interface {
	// Init takes a list of hosts and is generally expected to set up a
	// connection to pilosa using whatever client it chooses.
	Init(hosts []string, agentNum int) error

	// Run runs the benchmark. It takes an agentNum which should be used to
	// parameterize the benchmark if it is being run simultaneously on multiple
	// "agents". E.G. the agentNum might be used to make a random seed different
	// for each agent, or have each agent set a different set of bits. The return
	// value of Run is kept generic so that any relevant statistics or metrics
	// that may be specific to the benchmark in question can be reported.
	Run(agentNum int) map[string]interface{}
}

// Command extends Benchmark by adding methods for configuring via command line flags and returning usage information.
type Command interface {
	Benchmark

	// ConsumeFlags sets and parses flags, and then returns flagSet.Args()
	ConsumeFlags(args []string) ([]string, error)

	// Usage returns information on how to use this benchmark
	Usage() string
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
func (pb *parallelBenchmark) Init(hosts []string, agentNum int) error {
	var g ErrGroup
	for i, _ := range pb.benchmarkers {
		b := pb.benchmarkers[i]
		g.Go(func() error {
			return b.Init(hosts, agentNum)
		})
	}
	return g.Wait()
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
func (sb *serialBenchmark) Init(hosts []string, agentNum int) error {
	errors := make([]error, len(sb.benchmarkers))
	hadErr := false
	for i, b := range sb.benchmarkers {
		errors[i] = b.Init(hosts, agentNum)
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
	runtimes := make(map[string]time.Duration)
	total_start := time.Now()
	for i, b := range sb.benchmarkers {
		start := time.Now()
		ret := b.Run(agentNum)
		end := time.Now()
		results[strconv.Itoa(i)] = ret
		runtimes[strconv.Itoa(i)] = end.Sub(start)
	}
	runtimes["total"] = time.Now().Sub(total_start)
	results["runtimes"] = runtimes
	return results
}

// Serial takes a variable number of Benchmarks and returns a Benchmark
// which combines then and will run each serially.
func Serial(bs ...Benchmark) Benchmark {
	return &serialBenchmark{
		benchmarkers: bs,
	}
}
