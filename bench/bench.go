package bench

import (
	"fmt"

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
