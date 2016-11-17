package bench_test

import (
	"github.com/umbel/pilosa/bench"
	"testing"
)

func TestRandomBitmapCall(t *testing.T) {
	qm := bench.NewQueryMaker(5)
	bmc := qm.RandomBitmapCall(4, 3, 0, 1000)
	t.Log(bmc.String())
}

func TestRandom(t *testing.T) {
	for i := 99; i < 120; i++ {
		qm := bench.NewQueryMaker(int64(i))
		call := qm.Random(10, 4, 3, 0, 1000)
		t.Log(call)
	}
}
