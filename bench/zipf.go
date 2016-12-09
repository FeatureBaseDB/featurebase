package bench

import (
	"fmt"

	"flag"
	"io/ioutil"

	"context"
	"math/rand"
	"time"
)

// ZipfSetBits sets bits randomly and deterministically based on a seed, according to the Zipf distribution
type ZipfSetBits struct {
	HasClient
	BaseBitmapID    int64
	BaseProfileID   int64
	BitmapIDRange   int64
	ProfileIDRange  int64
	Iterations      int // number of bits that will be set
	Seed            int64
	BitmapExponent  float64
	BitmapOffset    float64
	ProfileExponent float64
	ProfileOffset   float64
	DB              string // DB to use in pilosa.

}

func (b *ZipfSetBits) Usage() string {
	return `
zipf-set-bits sets random bits according to Zipf distribution

Usage: zipf-set-bits [arguments]

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

	BitmapExponent  float64
	BitmapOffset    float64
	ProfileExponent float64
	ProfileOffset   float64

	-client-type string
		Can be 'single' (all agents hitting one host) or 'round_robin'
`[1:]
}

func (b *ZipfSetBits) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("ZipfSetBits", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.Int64Var(&b.BaseBitmapID, "base-bitmap-id", 0, "")
	fs.Int64Var(&b.BitmapIDRange, "bitmap-id-range", 100000, "")
	fs.Int64Var(&b.BaseProfileID, "base-profile-id", 0, "")
	fs.Int64Var(&b.ProfileIDRange, "profile-id-range", 100000, "")
	fs.Int64Var(&b.Seed, "seed", 1, "")
	fs.IntVar(&b.Iterations, "iterations", 100, "")
	fs.StringVar(&b.DB, "db", "benchdb", "")
	fs.Float64Var(&b.BitmapExponent, "bitmap-exponent", 1.01, "")
	fs.Float64Var(&b.BitmapOffset, "bitmap-offset", 1, "")
	fs.Float64Var(&b.ProfileExponent, "profile-exponent", 1.01, "")
	fs.Float64Var(&b.ProfileOffset, "profile-offset", 1, "")
	fs.StringVar(&b.ClientType, "client-type", "single", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Run runs the ZipfSetBits benchmark
func (b *ZipfSetBits) Run(ctx context.Context, agentNum int) map[string]interface{} {
	rnd := rand.New(rand.NewSource(b.Seed + int64(agentNum)))
	bitmapRng := rand.NewZipf(rnd, b.BitmapExponent, b.BitmapOffset, uint64(b.BitmapIDRange))
	profileRng := rand.NewZipf(rnd, b.ProfileExponent, b.ProfileOffset, uint64(b.ProfileIDRange))
	bitmapPerm := NewPermutationGenerator(b.BitmapIDRange, b.Seed)
	profilePerm := NewPermutationGenerator(b.ProfileIDRange, b.Seed)

	results := make(map[string]interface{})
	if b.cli == nil {
		results["error"] = fmt.Errorf("No client set for ZipfSetBits agent: %v", agentNum)
		return results
	}
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		// generate IDs from Zipf distribution
		bitmapIDOriginal := bitmapRng.Uint64()
		profIDOriginal := profileRng.Uint64()
		// permute IDs randomly, but repeatably
		bitmapID := bitmapPerm.Next(int64(bitmapIDOriginal))
		profID := profilePerm.Next(int64(profIDOriginal))

		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", b.BaseBitmapID+int64(bitmapID), b.BaseProfileID+int64(profID))
		start = time.Now()
		b.cli.ExecuteQuery(ctx, b.DB, query, true)
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
