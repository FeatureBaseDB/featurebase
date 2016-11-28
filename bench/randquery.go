package bench

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"
)

// RandomQuery sets bits randomly and deterministically based on a seed.
type RandomQuery struct {
	HasClient
	MaxDepth      int
	MaxArgs       int
	MaxN          int
	BaseBitmapID  int64
	BitmapIDRange int64
	Iterations    int // number of queries
	Seed          int64
	DBs           []string // DBs to query.

}

func (b *RandomQuery) Usage() string {
	return `
random-query constructs random queries

Usage: random-query [arguments]

The following arguments are available:

	-max-depth int
		Maximum nesting depth of queries

	-max-args int
		Maximum number of args for Union/Intersect/Difference Queries

	-max-n int
		Maximum N value for TopN queries.

	-base-bitmap-id int
		bitmap id to start from

	-bitmap-id-range int
		number of possible bitmap ids that can be set

	-iterations int
		number of bits to set

	-seed int
		Seed for RNG

	-dbs string
		Comma separated list of DBs to query against

	-client-type string
		Can be 'single' (all agents hitting one host) or 'round_robin'
`[1:]
}

func (b *RandomQuery) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("RandomQuery", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&b.MaxDepth, "max-depth", 4, "")
	fs.IntVar(&b.MaxArgs, "max-args", 4, "")
	fs.IntVar(&b.MaxN, "max-n", 4, "")
	fs.Int64Var(&b.BaseBitmapID, "base-bitmap-id", 0, "")
	fs.Int64Var(&b.BitmapIDRange, "bitmap-id-range", 100000, "")
	fs.Int64Var(&b.Seed, "seed", 1, "")
	fs.IntVar(&b.Iterations, "iterations", 100, "")
	var dbs string
	fs.StringVar(&dbs, "dbs", "benchdb", "")
	fs.StringVar(&b.ClientType, "client-type", "single", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	b.DBs = strings.Split(dbs, ",")
	return fs.Args(), nil
}

// Run runs the RandomQuery benchmark
func (b *RandomQuery) Run(agentNum int) map[string]interface{} {
	seed := b.Seed + int64(agentNum)
	results := make(map[string]interface{})
	if b.cli == nil {
		results["error"] = fmt.Errorf("No client set for RandomQuery agent: %v", agentNum)
		return results
	}
	qm := NewQueryGenerator(seed)
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		call := qm.Random(b.MaxN, b.MaxDepth, b.MaxArgs, uint64(b.BaseBitmapID), uint64(b.BitmapIDRange))
		start = time.Now()
		b.cli.ExecuteQuery(context.TODO(), b.DBs[n%len(b.DBs)], call.String(), true)
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
