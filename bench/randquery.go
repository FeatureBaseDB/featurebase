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
RandomQuery constructs random queries

Usage: RandomQuery [arguments]

The following arguments are available:

	-MaxDepth int
		Maximum nesting depth of queries

	-MaxArgs int
		Maximum number of args for Union/Intersect/Difference Queries

	-MaxN int
		Maximum N value for TopN queries.

	-BaseBitmapID int
		bitmap id to start from

	-BitmapIDRange int
		number of possible bitmap ids that can be set

	-Iterations int
		number of bits to set

	-Seed int
		Seed for RNG

	-DBs string
		Comma separated list of DBs to query against

	-ClientType string
		Can be 'single' (all agents hitting one host) or 'round_robin'
`[1:]
}

func (b *RandomQuery) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("RandomQuery", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&b.MaxDepth, "MaxDepth", 4, "")
	fs.IntVar(&b.MaxArgs, "MaxArgs", 4, "")
	fs.IntVar(&b.MaxN, "MaxN", 4, "")
	fs.Int64Var(&b.BaseBitmapID, "BaseBitmapID", 0, "")
	fs.Int64Var(&b.BitmapIDRange, "BitmapIDRange", 100000, "")
	fs.Int64Var(&b.Seed, "Seed", 1, "")
	fs.IntVar(&b.Iterations, "Iterations", 100, "")
	var dbs string
	fs.StringVar(&dbs, "DBs", "benchdb", "")
	fs.StringVar(&b.ClientType, "ClientType", "single", "")

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
	qm := NewQueryMaker(seed)
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
