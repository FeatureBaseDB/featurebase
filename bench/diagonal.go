package bench

import (
	"fmt"

	"flag"
	"io/ioutil"

	"context"
	"github.com/umbel/pilosa"
)

// DiagonalSetBits sets bits with increasing profile id and bitmap id.
type DiagonalSetBits struct {
	cli           *pilosa.Client
	BaseBitmapID  int
	BaseProfileID int
	Iterations    int
	DB            string
}

func (b *DiagonalSetBits) Usage() string {
	return `
DiagonalSetBits sets bits with increasing profile id and bitmap id.

Usage: DiagonalSetBits [arguments]

The following arguments are available:

	-BaseBitmapID int
		bits being set will all be greater than BaseBitmapID

	-BaseProfileID int
		profile id num to start from

	-Iterations int
		number of bits to set

	-DB string
		pilosa db to use
`[1:]
}

func (b *DiagonalSetBits) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("DiagonalSetBits", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&b.BaseBitmapID, "BaseBitmapID", 0, "")
	fs.IntVar(&b.BaseProfileID, "BaseProfileID", 0, "")
	fs.IntVar(&b.Iterations, "Iterations", 100, "")
	fs.StringVar(&b.DB, "DB", "benchdb", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Init connects to pilosa and sets the client on b.
func (b *DiagonalSetBits) Init(hosts []string, agentNum int) (err error) {
	b.cli, err = pilosa.NewClient(hosts[0])
	if err != nil {
		return err
	}
	if b.DB == "" {
		b.DB = "DiagonalSetBits"
	}
	return nil
}

// Run runs the DiagonalSetBits benchmark
func (b *DiagonalSetBits) Run(agentNum int) map[string]interface{} {
	results := make(map[string]interface{})
	if b.cli == nil {
		results["error"] = fmt.Errorf("No client set for DiagonalSetBits agent: %v", agentNum)
		return results
	}
	for n := 0; n < b.Iterations; n++ {
		iterID := agentizeNum(n, b.Iterations, agentNum)
		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", b.BaseBitmapID+iterID, b.BaseProfileID+iterID)
		b.cli.ExecuteQuery(context.TODO(), b.DB, query, true)
	}
	return results
}
