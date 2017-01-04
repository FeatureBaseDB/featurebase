package bench

import "context"

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
	Run(ctx context.Context, agentNum int) map[string]interface{}
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
