package bench

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"
)

// RandomQuery queries randomly and deterministically based on a seed.
type RandomPql struct {
	HasClient
	Name          string   `json:"name"`
	MaxDepth      int      `json:"max-depth"`
	MaxArgs       int      `json:"max-args"`
	MaxN          int      `json:"max-n"`
	BaseBitmapID  int64    `json:"base-bitmap-id"`
	BitmapIDRange int64    `json:"bitmap-id-range"`
	Iterations    int      `json:"iterations"`
	Seed          int64    `json:"seed"`
	DBs           []string `json:"dbs"`
}

// Init adds the agent num to the random seed and initializes the client.
func (b *RandomPql) Init(hosts []string, agentNum int) error {
	b.Name = "random-pql"
	b.Seed = b.Seed + int64(agentNum)
	return b.HasClient.Init(hosts, agentNum)
}

// Usage returns the usage message to be printed.
func (b *RandomPql) Usage() string {
	return `
random-pql compare random queries between protobuf and pql

Agent number modifies the random seed.

Usage: random-pql[arguments]

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

// ConsumeFlags parses all flags up to the next non flag argument (argument does
// not start with "-" and isn't the value of a flag). It returns the remaining
// args.
func (b *RandomPql) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("RandomPql", flag.ContinueOnError)
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

// Run runs the RandomPQL benchmark
func (b *RandomPql) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	if b.client == nil {
		results["error"] = fmt.Errorf("No client set")
		return results
	}
	qm := NewQueryGenerator(b.Seed)
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		call := qm.Random(b.MaxN, b.MaxDepth, b.MaxArgs, uint64(b.BaseBitmapID), uint64(b.BitmapIDRange))
		start = time.Now()
		queryString := call.String()
		b.client.ExecutePql(ctx, b.DBs[n%len(b.DBs)], queryString)
		s.Add(time.Now().Sub(start))

	}
	AddToResults(s, results)
	return results
}
