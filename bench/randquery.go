package bench

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"
	"errors"
)

// RandomQuery queries randomly and deterministically based on a seed.
type RandomQuery struct {
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
func (b *RandomQuery) Init(hosts []string, agentNum int) error {
	b.Name = "random-query"
	b.Seed = b.Seed + int64(agentNum)
	return b.HasClient.Init(hosts, agentNum)
}

// Usage returns the usage message to be printed.
func (b *RandomQuery) Usage() string {
	return `
random-query constructs random queries

Agent number modifies the random seed.

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

	-content-type string
		protobuf or pql
`[1:]
}

// ConsumeFlags parses all flags up to the next non flag argument (argument does
// not start with "-" and isn't the value of a flag). It returns the remaining
// args.
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
	fs.StringVar(&b.ContentType, "content-type", "protobuf", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	b.DBs = strings.Split(dbs, ",")
	return fs.Args(), nil
}

// Run runs the RandomQuery benchmark
func (b *RandomQuery) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	if b.client == nil {
		results["error"] = fmt.Errorf("No client set for RandomQuery")
		return results
	}
	qm := NewQueryGenerator(b.Seed)
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		call := qm.Random(b.MaxN, b.MaxDepth, b.MaxArgs, uint64(b.BaseBitmapID), uint64(b.BitmapIDRange))
		start = time.Now()
		b.ExecuteQuery(b.ContentType, b.DBs[n % len(b.DBs)], call.String(), ctx)
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}

func (b *RandomQuery) ExecuteQuery(contentType, db, query string, ctx context.Context, ) (interface{}, error) {
	if contentType == "protobuf" {
		return b.client.ExecuteQuery(ctx, db, query, true)
	} else if contentType == "pql" {
		return b.client.ExecutePQL(ctx, db, query)
	} else {
		return nil, errors.New("unsupport content type")
	}
}
