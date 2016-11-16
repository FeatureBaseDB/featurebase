package bench

import (
	"fmt"
	"strconv"

	"flag"
	"io/ioutil"

	"context"
	"github.com/umbel/pilosa"
)

// MultiDBSetBits sets bits with increasing profile id and bitmap id.
type MultiDBSetBits struct {
	cli           *pilosa.Client
	BaseBitmapID  int
	BaseProfileID int
	Iterations    int
}

func (b *MultiDBSetBits) Usage() string {
	return `
MultiDBSetBits sets bits with increasing profile id and bitmap id using a different DB for each agent.

Usage: MultiDBSetBits [arguments]

The following arguments are available:

	-BaseBitmapID int
		bits being set will all be greater than BaseBitmapID

	-BaseProfileID int
		profile id num to start from

	-Iterations int
		number of bits to set

`[1:]
}

func (b *MultiDBSetBits) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("MultiDBSetBits", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&b.BaseBitmapID, "BaseBitmapID", 0, "")
	fs.IntVar(&b.BaseProfileID, "BaseProfileID", 0, "")
	fs.IntVar(&b.Iterations, "Iterations", 100, "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Init connects to pilosa and sets the client on b.
func (b *MultiDBSetBits) Init(hosts []string, agentNum int) (err error) {
	b.cli, err = pilosa.NewClient(hosts[0])
	if err != nil {
		return err
	}
	return nil
}

// Run runs the MultiDBSetBits benchmark
func (b *MultiDBSetBits) Run(agentNum int) map[string]interface{} {
	results := make(map[string]interface{})
	if b.cli == nil {
		results["error"] = fmt.Errorf("No client set for MultiDBSetBits agent: %v", agentNum)
		return results
	}
	for n := 0; n < b.Iterations; n++ {
		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", b.BaseBitmapID+n, b.BaseProfileID+n)
		b.cli.ExecuteQuery(context.TODO(), "multidb"+strconv.Itoa(agentNum), query, true)
	}
	return results
}
