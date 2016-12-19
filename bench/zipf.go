package bench

import (
	"fmt"

	"flag"
	"io/ioutil"

	"context"
	"math"
	"math/rand"
	"time"
)

// ZipfSetBits sets random bits according to the Zipf-Mandelbrot distribution.
// This distribution accepts two parameters, Exponent and Ratio, for both bitmaps and profiles.
// It also uses PermutationGenerator to permute IDs randomly.
type ZipfSetBits struct {
	HasClient
	Name           string
	BaseBitmapID   int64
	BaseProfileID  int64
	BitmapIDRange  int64
	ProfileIDRange int64
	Iterations     int // number of bits that will be set
	Seed           int64
	BitmapRng      *rand.Zipf
	ProfileRng     *rand.Zipf
	BitmapPerm     *PermutationGenerator
	ProfilePerm    *PermutationGenerator
	DB             string // DB to use in pilosa.

	// TODO remove these - but theyre needed in ConsumeFlags
	BitmapExponent  float64
	BitmapRatio     float64
	ProfileExponent float64
	ProfileRatio    float64
}

func (b *ZipfSetBits) Usage() string {
	return `
zipf-set-bits sets random bits according to the Zipf distribution.
This is a power-law distribution controlled by two parameters.
Exponent, in the range (1, inf), with a default value of 1.001, controls
the "sharpness" of the distribution, with higher exponent being sharper.
Ratio, in the range (0, 1), with a default value of 0.25, controls the
maximum variation of the distribution, with higher ratio being more uniform.

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

	-bitmap-exponent float64
		zipf exponent parameter for bitmap IDs

	-bitmap-ratio float64
		zipf probability ratio parameter for bitmap IDs

	-profile-exponent float64
		zipf exponent parameter for profile IDs

	-profile-ratio float64
		zipf probability ratio parameter for profile IDs

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
	fs.Float64Var(&b.BitmapRatio, "bitmap-ratio", 0.25, "")
	fs.Float64Var(&b.ProfileExponent, "profile-exponent", 1.01, "")
	fs.Float64Var(&b.ProfileRatio, "profile-ratio", 0.25, "")
	fs.StringVar(&b.ClientType, "client-type", "single", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Offset is the true parameter used by the Zipf distribution, but the ratio,
// as defined here, is a simpler, readable way to define the distribution.
// Offset is in [1, inf), and its meaning depends on N (a pain for updating benchmark configs)
// ratio is in (0, 1), and its meaning does not depend on N.
// it is the ratio of the lowest probability in the distribution to the highest.
// ratio=0.01 corresponds to a very small offset - the most skewed distribution for a given pair (N, exp)
// ratio=0.99 corresponds to a very large offset - the most nearly uniform distribution for a given (N, exp)
func getZipfOffset(N int64, exp, ratio float64) float64 {
	z := math.Pow(ratio, 1/exp)
	return z * float64(N-1) / (1 - z)
}

func (b *ZipfSetBits) Init(hosts []string, agentNum int) error {
	b.Name = "ZipfSetBits"
	rnd := rand.New(rand.NewSource(b.Seed + int64(agentNum)))
	bitmapOffset := getZipfOffset(b.BitmapIDRange, b.BitmapExponent, b.BitmapRatio)
	b.BitmapRng = rand.NewZipf(rnd, b.BitmapExponent, bitmapOffset, uint64(b.BitmapIDRange-1))
	profileOffset := getZipfOffset(b.ProfileIDRange, b.ProfileExponent, b.ProfileRatio)
	b.ProfileRng = rand.NewZipf(rnd, b.ProfileExponent, profileOffset, uint64(b.ProfileIDRange-1))

	b.BitmapPerm = NewPermutationGenerator(b.BitmapIDRange, b.Seed)
	b.ProfilePerm = NewPermutationGenerator(b.ProfileIDRange, b.Seed+1)

	return b.HasClient.Init(hosts, agentNum)
}

// Run runs the ZipfSetBits benchmark
func (b *ZipfSetBits) Run(ctx context.Context, agentNum int) map[string]interface{} {
	results := make(map[string]interface{})
	if b.cli == nil {
		results["error"] = fmt.Errorf("No client set for ZipfSetBits agent: %v", agentNum)
		return results
	}
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		// generate IDs from Zipf distribution
		bitmapIDOriginal := b.BitmapRng.Uint64()
		profIDOriginal := b.ProfileRng.Uint64()
		// permute IDs randomly, but repeatably
		bitmapID := b.BitmapPerm.Next(int64(bitmapIDOriginal))
		profID := b.ProfilePerm.Next(int64(profIDOriginal))

		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", b.BaseBitmapID+int64(bitmapID), b.BaseProfileID+int64(profID))
		start = time.Now()
		b.cli.ExecuteQuery(ctx, b.DB, query, true)
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
