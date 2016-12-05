package bench

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"

	"github.com/pilosa/pilosa/pilosactl"
)

// Import sets bits with increasing profile id and bitmap id.
type Import struct {
	BaseBitmapID      int64
	MaxBitmapID       int64
	BaseProfileID     int64
	MaxProfileID      int64
	RandomBitmapOrder bool
	MinBitsPerMap     int64
	MaxBitsPerMap     int64
	AgentControls     string
	Seed              int64

	pilosactl.ImportCommand
}

func (b *Import) Usage() string {
	return `
import generates an import file and imports using pilosa's bulk import interface

Usage: import [arguments]

The following arguments are available:

	-base-bitmap-id int
		bits being set will all be greater than this

	-maximum-bitmap-id int
		bits being set will all be less than this

	-base-profile-id int
		profile id num to start from

	-max-profile-id int
		maximum profile id to generate

	-random-bitmap-order
		if this option is set, the import file will not be sorted by bitmap id

	-min-bits-per-map int
		minimum number of bits set per bitmap

	-max-bits-per-map int
		maximum number of bits set per bitmap

	-agent-controls string
		can be 'height', 'width', or empty (TODO or square?)- increasing
		number of agents modulates bitmap id range, profile id range,
		or just sets more bits in the same range.

	-seed int
		seed for RNG

	-db string
		pilosa db to use

	-frame string
		frame to import into
`[1:]
}

func (b *Import) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("Import", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.Int64Var(&b.BaseBitmapID, "base-bitmap-id", 0, "")
	fs.Int64Var(&b.MaxBitmapID, "max-bitmap-id", 1000, "")
	fs.Int64Var(&b.BaseProfileID, "base-profile-id", 0, "")
	fs.Int64Var(&b.MaxProfileID, "max-profile-id", 1000, "")
	fs.BoolVar(&b.RandomBitmapOrder, "random-bitmap-order", false, "")
	fs.Int64Var(&b.MinBitsPerMap, "min-bits-per-map", 0, "")
	fs.Int64Var(&b.MaxBitsPerMap, "max-bits-per-map", 10, "")
	fs.StringVar(&b.AgentControls, "agent-controls", "", "")
	fs.Int64Var(&b.Seed, "seed", 0, "")
	fs.StringVar(&b.Database, "db", "benchdb", "")
	fs.StringVar(&b.Frame, "frame", "testframe", "")
	fs.IntVar(&b.BufferSize, "buffer-size", 10000000, "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

func (b *Import) Init(hosts []string, agentNum int) error {
	var err error
	b.Client, err = firstHostClient(hosts)
	if err != nil {
		return err
	}
	// generate csv data
	baseBitmapID, maxBitmapID, baseProfileID, maxProfileID := b.BaseBitmapID, b.MaxBitmapID, b.BaseProfileID, b.MaxProfileID
	if b.AgentControls == "height" {
		numBitmapIDs := (b.MaxBitmapID - b.BaseBitmapID)
		baseBitmapID = b.BaseBitmapID + (numBitmapIDs * int64(agentNum))
		maxBitmapID = baseBitmapID + numBitmapIDs
	}
	if b.AgentControls == "height" {
		numProfileIDs := (b.MaxProfileID - b.BaseProfileID)
		baseProfileID = b.BaseProfileID + (numProfileIDs * int64(agentNum))
		maxProfileID = baseProfileID + numProfileIDs
	}
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}
	GenerateImportCSV(f, baseBitmapID, maxBitmapID, baseProfileID, maxProfileID,
		b.MinBitsPerMap, b.MaxBitsPerMap, b.Seed+int64(agentNum), b.RandomBitmapOrder)
	// set b.Paths
	b.Paths = []string{f.Name()}
	return nil
}

// Run runs the Import benchmark
func (b *Import) Run(agentNum int) map[string]interface{} {
	results := make(map[string]interface{})
	b.ImportCommand.Run(context.TODO())
	return results
}

func GenerateImportCSV(w io.Writer, baseBitmapID, maxBitmapID, baseProfileID, maxProfileID, minBitsPerMap, maxBitsPerMap, seed int64, randomOrder bool) {
	src := rand.NewSource(seed)
	rng := rand.New(src)

	var bitmapIDs []int
	if randomOrder {
		bitmapIDs = rng.Perm(int(maxBitmapID - baseBitmapID))
	}
	for i := baseBitmapID; i < maxBitmapID; i++ {
		var bitmapID int64
		if randomOrder {
			bitmapID = int64(bitmapIDs[i-baseBitmapID])
		} else {
			bitmapID = int64(i)
		}

		numBitsToSet := rng.Int63n(maxBitsPerMap-minBitsPerMap) + minBitsPerMap
		for j := int64(0); j < numBitsToSet; j++ {
			profileID := rng.Int63n(maxProfileID-baseProfileID) + baseProfileID
			fmt.Fprintf(w, "%d,%d\n", bitmapID, profileID)
		}
	}
}
