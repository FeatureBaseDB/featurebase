package bench

import (
	"fmt"

	"flag"
	"io/ioutil"

	"context"
	"math/rand"
	"time"
)

// RandomSetBits sets bits randomly and deterministically based on a seed.
type RandomSetBits struct {
	HasClient
	Name           string `json:"name"`
	BaseBitmapID   int64  `json:"base-bitmap-id"`
	BaseProfileID  int64  `json:"base-profile-id"`
	BitmapIDRange  int64  `json:"bitmap-id-range"`
	ProfileIDRange int64  `json:"profile-id-range"`
	Iterations     int    `json:"iterations"`
	Seed           int64  `json:"seed"`
	DB             string `json:"db"`
}

func (b *RandomSetBits) Init(hosts []string, agentNum int) error {
	b.Name = "RandomSetBits"
	return b.HasClient.Init(hosts, agentNum)
}

func (b *RandomSetBits) Usage() string {
	return `
random-set-bits sets random bits

Usage: random-set-bits [arguments]

The following arguments are available:

	-base-bitmap-id int
		bits being set will all be greater than BaseBitmapID

	-bitmap-id-range int
		number of possible bitmap ids that can be set

	-base-profile-id int
		profile id num to start from

	-profile-id-range int
		number of possible profile ids that can be set

	-iterations int
		number of bits to set

	-seed int
		Seed for RNG

	-db string
		pilosa db to use

	-client-type string
		Can be 'single' (all agents hitting one host) or 'round_robin'
`[1:]
}

func (b *RandomSetBits) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("RandomSetBits", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.Int64Var(&b.BaseBitmapID, "base-bitmap-id", 0, "")
	fs.Int64Var(&b.BitmapIDRange, "bitmap-id-range", 100000, "")
	fs.Int64Var(&b.BaseProfileID, "base-profile-id", 0, "")
	fs.Int64Var(&b.ProfileIDRange, "profile-id-range", 100000, "")
	fs.Int64Var(&b.Seed, "seed", 1, "")
	fs.IntVar(&b.Iterations, "iterations", 100, "")
	fs.StringVar(&b.DB, "db", "benchdb", "")
	fs.StringVar(&b.ClientType, "client-type", "single", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Run runs the RandomSetBits benchmark
func (b *RandomSetBits) Run(ctx context.Context, agentNum int) map[string]interface{} {
	src := rand.NewSource(b.Seed + int64(agentNum))
	rng := rand.New(src)
	results := make(map[string]interface{})
	if b.cli == nil {
		results["error"] = fmt.Errorf("No client set for RandomSetBits agent: %v", agentNum)
		return results
	}
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		bitmapID := rng.Int63n(b.BitmapIDRange)
		profID := rng.Int63n(b.ProfileIDRange)
		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", b.BaseBitmapID+bitmapID, b.BaseProfileID+profID)
		start = time.Now()
		b.cli.ExecuteQuery(ctx, b.DB, query, true)
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
