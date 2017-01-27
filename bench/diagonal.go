package bench

import (
	"fmt"

	"flag"
	"io/ioutil"

	"context"
	"time"
)

// DiagonalSetBits sets bits with increasing profile id and bitmap id.
type DiagonalSetBits struct {
	HasClient
	Name          string `json:"name"`
	BaseBitmapID  int    `json:"base-bitmap-id"`
	BaseProfileID int    `json:"base-profile-id"`
	Iterations    int    `json:"iterations"`
	DB            string `json:"db"`
}

// Init sets up the pilosa client and modifies the configured values based on
// the agent num.
func (b *DiagonalSetBits) Init(hosts []string, agentNum int) error {
	b.Name = "diagonal-set-bits"
	b.BaseBitmapID = b.BaseBitmapID + (agentNum * b.Iterations)
	b.BaseProfileID = b.BaseProfileID + (agentNum * b.Iterations)
	return b.HasClient.Init(hosts, agentNum)
}

// Usage returns the usage message to be printed.
func (b *DiagonalSetBits) Usage() string {
	return `
diagonal-set-bits sets bits with increasing profile id and bitmap id.

Agent num offsets both the base profile id and base bitmap id by the number of
iterations, so that only bits on the main diagonal are set, and agents don't
overlap at all.

Usage: diagonal-set-bits [arguments]

The following arguments are available:

	-base-bitmap-id int
		bits being set will all be greater than BaseBitmapID

	-base-profile-id int
		profile id num to start from

	-iterations int
		number of bits to set

	-db string
		pilosa db to use

	-client-type string
		Can be 'single' (all agents hitting one host) or 'round_robin'

	-content-type string
		protobuf or pql
`[1:]
}

// ConsumeFlags parses all flags up to the next non flag argument (argument does
// not start with "-" and isn't the value of a flag). It returns the remaining
// args.
func (b *DiagonalSetBits) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("DiagonalSetBits", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&b.BaseBitmapID, "base-bitmap-id", 0, "")
	fs.IntVar(&b.BaseProfileID, "base-profile-id", 0, "")
	fs.IntVar(&b.Iterations, "iterations", 100, "")
	fs.StringVar(&b.DB, "db", "benchdb", "")
	fs.StringVar(&b.ClientType, "client-type", "single", "")
	fs.StringVar(&b.ContentType, "content-type", "protobuf", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Run runs the DiagonalSetBits benchmark
func (b *DiagonalSetBits) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	if b.client == nil {
		results["error"] = fmt.Errorf("No client set for DiagonalSetBits")
		return results
	}
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", b.BaseBitmapID+n, b.BaseProfileID+n)
		start = time.Now()
		_, err := b.client.ExecuteQuery(ctx, b.DB, query, true)
		if err != nil {
			results["error"] = err
			return results
		}
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
