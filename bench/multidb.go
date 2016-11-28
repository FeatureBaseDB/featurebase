package bench

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"
)

// MultiDBSetBits sets bits with increasing profile id and bitmap id.
type MultiDBSetBits struct {
	HasClient
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

	-ClientType string
		Can be 'single' (all agents hitting one host) or 'round_robin'

`[1:]
}

func (b *MultiDBSetBits) ConsumeFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("MultiDBSetBits", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&b.BaseBitmapID, "BaseBitmapID", 0, "")
	fs.IntVar(&b.BaseProfileID, "BaseProfileID", 0, "")
	fs.IntVar(&b.Iterations, "Iterations", 100, "")
	fs.StringVar(&b.ClientType, "ClientType", "single", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

// Run runs the MultiDBSetBits benchmark
func (b *MultiDBSetBits) Run(agentNum int) map[string]interface{} {
	results := make(map[string]interface{})
	if b.cli == nil {
		results["error"] = fmt.Errorf("No client set for MultiDBSetBits agent: %v", agentNum)
		return results
	}
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", b.BaseBitmapID+n, b.BaseProfileID+n)
		start = time.Now()
		b.cli.ExecuteQuery(context.TODO(), "multidb"+strconv.Itoa(agentNum), query, true)
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
