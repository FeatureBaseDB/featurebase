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
