package bench

import (
	"fmt"

	"flag"
	"io/ioutil"

	"context"
	"math/rand"
)

// RandomSetBits sets bits randomly and deterministically based on a seed.
type RandomSetBits struct {
	HasClient
	BaseBitmapID   int64
	BaseProfileID  int64
	BitmapIDRange  int64
	ProfileIDRange int64
	Iterations     int // number of bits that will be set
	Seed           int64
	DB             string // DB to use in pilosa.

}

func (b *RandomSetBits) Usage() string {
	return `
RandomSetBits sets random bits

Usage: RandomSetBits [arguments]

The following arguments are available:

	-BaseBitmapID int
		bitmap id to start from

	-BitmapIDRange int
		number of possible bitmap ids that can be set

	-BaseProfileID int
		profile id num to start from

	-ProfileIDRange int
		number of possible profile ids that can be set

	-Iterations int
		number of bits to set

	-Seed int
		Seed for RNG

	-DB string
		pilosa db to use

	-ClientType string
		Can be 'single' (all agents hitting one host) or 'round_robin'
`[1:]
}

func (b *RandomSetBits) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("RandomSetBits", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.Int64Var(&b.BaseBitmapID, "BaseBitmapID", 0, "")
	fs.Int64Var(&b.BitmapIDRange, "BitmapIDRange", 100000, "")
	fs.Int64Var(&b.BaseProfileID, "BaseProfileID", 0, "")
	fs.Int64Var(&b.ProfileIDRange, "ProfileIDRange", 100000, "")
	fs.Int64Var(&b.Seed, "Seed", 1, "")
	fs.IntVar(&b.Iterations, "Iterations", 100, "")
	fs.StringVar(&b.DB, "DB", "benchdb", "")
	fs.StringVar(&b.ClientType, "ClientType", "single", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Run runs the RandomSetBits benchmark
func (b *RandomSetBits) Run(agentNum int) map[string]interface{} {
	src := rand.NewSource(b.Seed + int64(agentNum))
	rng := rand.New(src)
	results := make(map[string]interface{})
	if b.cli == nil {
		results["error"] = fmt.Errorf("No client set for RandomSetBits agent: %v", agentNum)
		return results
	}
	for n := 0; n < b.Iterations; n++ {
		bitmapID := rng.Int63n(b.BitmapIDRange)
		profID := rng.Int63n(b.ProfileIDRange)
		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", b.BaseBitmapID+bitmapID, b.BaseProfileID+profID)
		b.cli.ExecuteQuery(context.TODO(), b.DB, query, true)
	}
	return results
}
