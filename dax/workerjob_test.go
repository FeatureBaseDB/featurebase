package dax

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWorkerDiffAdd(t *testing.T) {
	tests := []struct {
		w1  WorkerDiff
		w2  WorkerDiff
		exp WorkerDiff
	}{
		{
			w1: WorkerDiff{
				AddedJobs:   []Job{},
				RemovedJobs: []Job{},
			},
			w2: WorkerDiff{
				AddedJobs:   []Job{},
				RemovedJobs: []Job{},
			},
			exp: WorkerDiff{
				AddedJobs:   []Job{},
				RemovedJobs: []Job{},
			},
		},
		{
			w1: WorkerDiff{
				AddedJobs:   []Job{"a", "b"},
				RemovedJobs: []Job{"c", "d"},
			},
			w2: WorkerDiff{
				AddedJobs:   []Job{"c"},
				RemovedJobs: []Job{"a", "z"},
			},
			exp: WorkerDiff{
				AddedJobs:   []Job{"b"},
				RemovedJobs: []Job{"d", "z"},
			},
		},
		{
			w1: WorkerDiff{
				AddedJobs:   []Job{"a", "b"},
				RemovedJobs: []Job{},
			},
			w2: WorkerDiff{
				AddedJobs:   []Job{"c", "d"},
				RemovedJobs: []Job{},
			},
			exp: WorkerDiff{
				AddedJobs:   []Job{"a", "b", "c", "d"},
				RemovedJobs: []Job{},
			},
		},
		{
			w1: WorkerDiff{
				AddedJobs:   []Job{},
				RemovedJobs: []Job{"a", "b"},
			},
			w2: WorkerDiff{
				AddedJobs:   []Job{},
				RemovedJobs: []Job{"c", "d"},
			},
			exp: WorkerDiff{
				AddedJobs:   []Job{},
				RemovedJobs: []Job{"a", "b", "c", "d"},
			},
		},
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("workerdiff: %d", i), func(t *testing.T) {
			tst.w1.Add(tst.w2)
			assert.ElementsMatch(t, tst.exp.AddedJobs, tst.w1.AddedJobs)
			assert.ElementsMatch(t, tst.exp.RemovedJobs, tst.w1.RemovedJobs)
		})
	}
}

func TestWorkerDiffsApply(t *testing.T) {

	a := []WorkerDiff{
		{
			Address:     "addr2",
			AddedJobs:   []Job{"j10", "j11"},
			RemovedJobs: []Job{"j99", "j100"},
		},
		{
			Address:     "addr1",
			AddedJobs:   []Job{"j1", "j2"},
			RemovedJobs: []Job{"j86"},
		},
	}

	b := []WorkerDiff{
		{
			Address:     "addr2",
			AddedJobs:   []Job{"j3", "j99"},
			RemovedJobs: []Job{"j2", "j10"},
		},
		{
			Address:     "addr3",
			AddedJobs:   []Job{"j777"},
			RemovedJobs: []Job{},
		},
	}

	out := WorkerDiffs(a).Apply(b)

	exp := []WorkerDiff{
		{
			Address:     "addr1",
			AddedJobs:   []Job{"j1", "j2"},
			RemovedJobs: []Job{"j86"},
		},
		{
			Address:     "addr2",
			AddedJobs:   []Job{"j11", "j3", "j99"},
			RemovedJobs: []Job{"j10", "j100", "j2"},
		},
		{
			Address:     "addr3",
			AddedJobs:   []Job{"j777"},
			RemovedJobs: []Job{},
		},
	}

	assert.ElementsMatch(t, exp, out)
}
