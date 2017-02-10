package bench

import (
	"context"
	"flag"
	"io"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

// NewSliceHeight creates a new slice height benchmark with stdin/out/err
// initialized.
func NewSliceHeight(stdin io.Reader, stdout, stderr io.Writer) *SliceHeight {
	return &SliceHeight{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// SliceHeight benchmark tests the effect of an increasing number of bitmaps in
// a single slice on query time.
type SliceHeight struct {
	MaxTime time.Duration `json:"max-time"`
	hosts   []string

	Name          string `json:"name"`
	MinBitsPerMap int64  `json:"min-bits-per-map"`
	MaxBitsPerMap int64  `json:"max-bits-per-map"`
	Seed          int64  `json:"seed"`
	Database      string `json:"database"`
	Frame         string `json:"frame"`

	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// Usage returns the usage message to be printed.
func (b *SliceHeight) Usage() string {
	return `
slice-height repeatedly imports more bitmaps into a single slice and tests query times in between.

Agent number has no effect on this benchmark.

Usage: slice-height [arguments]

The following arguments are available:

	-max-time int
		stop benchmark after this many seconds

	-min-bits-per-map int
		minimum number of bits set per bitmap

	-max-bits-per-map int
		maximum number of bits set per bitmap

	-seed int
		seed for RNG

	-db string
		pilosa db to use

	-frame string
		frame to import into
`[1:]
}

// ConsumeFlags parses all flags up to the next non flag argument (argument does
// not start with "-" and isn't the value of a flag). It returns the remaining
// args.
func (b *SliceHeight) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("SliceHeight", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)

	maxTime := fs.Int("max-time", 30, "")
	fs.Int64Var(&b.MinBitsPerMap, "min-bits-per-map", 0, "")
	fs.Int64Var(&b.MaxBitsPerMap, "max-bits-per-map", 10, "")
	fs.Int64Var(&b.Seed, "seed", 0, "")
	fs.StringVar(&b.Database, "db", "benchdb", "")
	fs.StringVar(&b.Frame, "frame", "testframe", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	b.MaxTime = time.Duration(*maxTime) * time.Second
	return fs.Args(), nil
}

// Init sets up the slice height benchmark.
func (b *SliceHeight) Init(hosts []string, agentNum int) error {
	b.Name = "slice-height"
	b.hosts = hosts
	return nil
}

// Run runs the SliceHeight benchmark
func (b *SliceHeight) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})

	imp := NewImport(b.Stdin, b.Stdout, b.Stderr)
	imp.MaxBitmapID = 100
	imp.MaxProfileID = pilosa.SliceWidth
	imp.MinBitsPerMap = b.MinBitsPerMap
	imp.MaxBitsPerMap = b.MaxBitsPerMap
	imp.Database = b.Database
	imp.Frame = b.Frame

	start := time.Now()

	for i := 0; i > -1; i++ {
		iresults := make(map[string]interface{})
		results["iteration"+strconv.Itoa(i)] = iresults

		genstart := time.Now()
		imp.Init(b.hosts, 0)
		gendur := time.Now().Sub(genstart)
		iresults["csvgen"] = gendur

		iresults["import"] = imp.Run(ctx)

		qstart := time.Now()
		q := &pql.TopN{Frame: b.Frame, N: 50}
		_, err := imp.Client.ExecuteQuery(ctx, b.Database, q.String(), true)
		if err != nil {
			iresults["query_error"] = err.Error()
		} else {
			qdur := time.Now().Sub(qstart)
			iresults["query"] = qdur
		}
		imp.BaseBitmapID = imp.MaxBitmapID
		imp.MaxBitmapID = imp.MaxBitmapID * 10

		if time.Now().Sub(start) > b.MaxTime {
			break
		}
	}

	return results
}
