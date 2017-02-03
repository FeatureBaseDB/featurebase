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

// Zipf sets random bits according to the Zipf-Mandelbrot distribution.
// This distribution accepts two parameters, Exponent and Ratio, for both bitmaps and profiles.
// It also uses PermutationGenerator to permute IDs randomly.
type Zipf struct {
	HasClient
	Name            string  `json:"name"`
	BaseBitmapID    int64   `json:"base-bitmap-id"`
	BaseProfileID   int64   `json:"base-profile-id"`
	BitmapIDRange   int64   `json:"bitmap-id-range"`
	ProfileIDRange  int64   `json:"profile-id-range"`
	Iterations      int     `json:"iterations"`
	Seed            int64   `json:"seed"`
	DB              string  `json:"db"`
	BitmapExponent  float64 `json:"bitmap-exponent"`
	BitmapRatio     float64 `json:"bitmap-ratio"`
	ProfileExponent float64 `json:"profile-exponent"`
	ProfileRatio    float64 `json:"profile-ratio"`
	Operation       string  `json:"operation"`
	bitmapRng       *rand.Zipf
	profileRng      *rand.Zipf
	bitmapPerm      *PermutationGenerator
	profilePerm     *PermutationGenerator
}

// Usage returns the usage message to be printed.
func (b *Zipf) Usage() string {
	return `
zipf sets random bits according to the Zipf distribution.
This is a power-law distribution controlled by two parameters.
Exponent, in the range (1, inf), with a default value of 1.001, controls
the "sharpness" of the distribution, with higher exponent being sharper.
Ratio, in the range (0, 1), with a default value of 0.25, controls the
maximum variation of the distribution, with higher ratio being more uniform.

Agent number modifies random seed.

Usage: zipf [arguments]

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

	-operation string
		Can be 'set' or 'clear'

	-content-type string
		protobuf or pql
`[1:]
}

// ConsumeFlags parses all flags up to the next non flag argument (argument does
// not start with "-" and isn't the value of a flag). It returns the remaining
// args.
func (b *Zipf) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("Zipf", flag.ContinueOnError)
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
	fs.StringVar(&b.Operation, "operation", "set", "")
	fs.StringVar(&b.ContentType, "content-type", "protobuf", "")

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

// Init sets up the benchmark based on the agent number and initializes the
// client.
func (b *Zipf) Init(hosts []string, agentNum int) error {
	b.Name = "zipf"
	b.Seed = b.Seed + int64(agentNum)
	rnd := rand.New(rand.NewSource(b.Seed))
	bitmapOffset := getZipfOffset(b.BitmapIDRange, b.BitmapExponent, b.BitmapRatio)
	b.bitmapRng = rand.NewZipf(rnd, b.BitmapExponent, bitmapOffset, uint64(b.BitmapIDRange-1))
	profileOffset := getZipfOffset(b.ProfileIDRange, b.ProfileExponent, b.ProfileRatio)
	b.profileRng = rand.NewZipf(rnd, b.ProfileExponent, profileOffset, uint64(b.ProfileIDRange-1))

	b.bitmapPerm = NewPermutationGenerator(b.BitmapIDRange, b.Seed)
	b.profilePerm = NewPermutationGenerator(b.ProfileIDRange, b.Seed+1)

	if b.Operation != "set" && b.Operation != "clear" {
		return fmt.Errorf("Unsupported operation: \"%s\" (must be \"set\" or \"clear\")", b.Operation)
	}

	return b.HasClient.Init(hosts, agentNum)
}

// Run runs the Zipf benchmark
func (b *Zipf) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	if b.client == nil {
		results["error"] = fmt.Errorf("No client set for Zipf")
		return results
	}
	operation := "SetBit"
	if b.Operation == "clear" {
		operation = "ClearBit"
	}
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		// generate IDs from Zipf distribution
		bitmapIDOriginal := b.bitmapRng.Uint64()
		profIDOriginal := b.profileRng.Uint64()
		// permute IDs randomly, but repeatably
		bitmapID := b.bitmapPerm.Next(int64(bitmapIDOriginal))
		profID := b.profilePerm.Next(int64(profIDOriginal))

		query := fmt.Sprintf("%s(%d, 'frame.n', %d)", operation, b.BaseBitmapID+int64(bitmapID), b.BaseProfileID+int64(profID))
		start = time.Now()
		_, err := b.client.ExecuteQuery(ctx, b.DB, query, true)
		if err != nil {
			results["error"] = fmt.Sprintf("Error executing query in zipf: %v", err)
			return results
		}
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
