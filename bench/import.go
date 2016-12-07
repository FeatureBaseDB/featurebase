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

func NewImport(stdin io.Reader, stdout, stderr io.Writer) *Import {
	return &Import{
		ImportCommand: pilosactl.NewImportCommand(stdin, stdout, stderr),
	}
}

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
	numbits           int

	*pilosactl.ImportCommand
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
	if len(hosts) == 0 {
		return fmt.Errorf("Need at least one host")
	}
	b.Host = hosts[0]
	// generate csv data
	baseBitmapID, maxBitmapID, baseProfileID, maxProfileID := b.BaseBitmapID, b.MaxBitmapID, b.BaseProfileID, b.MaxProfileID
	switch b.AgentControls {
	case "height":
		numBitmapIDs := (b.MaxBitmapID - b.BaseBitmapID)
		baseBitmapID = b.BaseBitmapID + (numBitmapIDs * int64(agentNum))
		maxBitmapID = baseBitmapID + numBitmapIDs
	case "width":
		numProfileIDs := (b.MaxProfileID - b.BaseProfileID)
		baseProfileID = b.BaseProfileID + (numProfileIDs * int64(agentNum))
		maxProfileID = baseProfileID + numProfileIDs
	case "":
		break
	default:
		return fmt.Errorf("agent-controls: '%v' is not supported", b.AgentControls)
	}
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}
	// set b.Paths)
	num := GenerateImportCSV(f, baseBitmapID, maxBitmapID, baseProfileID, maxProfileID,
		b.MinBitsPerMap, b.MaxBitsPerMap, b.Seed+int64(agentNum), b.RandomBitmapOrder)
	b.numbits = num
	// set b.Paths
	f.Close()
	b.Paths = []string{f.Name()}
	return nil
}

// Run runs the Import benchmark
func (b *Import) Run(agentNum int) map[string]interface{} {
	results := make(map[string]interface{})
	err := b.ImportCommand.Run(context.TODO())
	if err != nil {
		results["error"] = err.Error()
	}
	results["numbits"] = b.numbits
	results["config"] = *b
	return results
}

func GenerateImportCSV(w io.Writer, baseBitmapID, maxBitmapID, baseProfileID, maxProfileID, minBitsPerMap, maxBitsPerMap, seed int64, randomOrder bool) int {
	src := rand.NewSource(seed)
	rng := rand.New(src)

	var bitmapIDs []int
	if randomOrder {
		bitmapIDs = rng.Perm(int(maxBitmapID - baseBitmapID))
	}
	numrows := 0
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
			numrows += 1
		}
	}
	return numrows
}
