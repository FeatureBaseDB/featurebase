package naive_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/molecula/featurebase/v3/dax"
	daxbolt "github.com/molecula/featurebase/v3/dax/boltdb"
	"github.com/molecula/featurebase/v3/dax/mds/controller/naive/boltdb"
	testbolt "github.com/molecula/featurebase/v3/dax/test/boltdb"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

func newBoltBalancer(t *testing.T) (*daxbolt.DB, func()) {
	db := testbolt.MustOpenDB(t)
	assert.NoError(t, db.InitializeBuckets(boltdb.NaiveBalancerBuckets...))

	return db, func() {
		testbolt.MustCloseDB(t, db)
		testbolt.CleanupDB(t, db.Path())
	}
}

func TestBalancer(t *testing.T) {
	ctx := context.Background()
	t.Run("SingleWorker", func(t *testing.T) {
		db, cleanup := newBoltBalancer(t)
		defer cleanup()
		bal := boltdb.NewBalancer("test", db, logger.NewStandardLogger(os.Stderr))
		tests := []struct {
			fn       func(context.Context, fmt.Stringer) ([]dax.WorkerDiff, error)
			input    string
			expDiff  []dax.WorkerDiff
			expState []dax.WorkerInfo
		}{
			{
				// Add job.
				fn:       bal.AddJob,
				input:    "p2",
				expDiff:  []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{},
			},
			{
				// Add worker.
				fn:    bal.AddWorker,
				input: "n1",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n1",
						AddedJobs:   []dax.Job{"p2"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n1",
						Jobs: []dax.Job{"p2"},
					},
				},
			},
			{
				// Add another job out of order.
				fn:    bal.AddJob,
				input: "p1",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n1",
						AddedJobs:   []dax.Job{"p1"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2"},
					},
				},
			},
			{
				// Add another job.
				fn:    bal.AddJob,
				input: "p3",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n1",
						AddedJobs:   []dax.Job{"p3"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2", "p3"},
					},
				},
			},
			{
				// Add a duplicate job.
				fn:      bal.AddJob,
				input:   "p2",
				expDiff: []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2", "p3"},
					},
				},
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				diff, err := test.fn(ctx, newStringWrapper(test.input))
				assert.NoError(t, err)
				assert.Equal(t, test.expDiff, diff)

				cs, err := bal.CurrentState(ctx)
				assert.NoError(t, err)
				assert.Equal(t, test.expState, cs)
			})
		}
	})

	t.Run("MultipleWorkers", func(t *testing.T) {
		db, cleanup := newBoltBalancer(t)
		defer cleanup()
		bal := boltdb.NewBalancer("test", db, logger.NewStandardLogger(os.Stderr))
		tests := []struct {
			fn       func(context.Context, fmt.Stringer) ([]dax.WorkerDiff, error)
			input    string
			balance  bool
			expDiff  []dax.WorkerDiff
			expState []dax.WorkerInfo
		}{
			{
				// Balance when empty.
				balance:  true,
				expDiff:  []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{},
			},
			{
				// Add worker.
				fn:      bal.AddWorker,
				input:   "n2",
				expDiff: []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{
					{
						ID:   "n2",
						Jobs: []dax.Job{},
					},
				},
			},
			{
				// Add worker again.
				fn:      bal.AddWorker,
				input:   "n2",
				expDiff: []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{
					{
						ID:   "n2",
						Jobs: []dax.Job{},
					},
				},
			},
			{
				// Add a second worker.
				fn:      bal.AddWorker,
				input:   "n1",
				expDiff: []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{
					{
						ID:   "n1",
						Jobs: []dax.Job{},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{},
					},
				},
			},
			{
				// Add job.
				fn:    bal.AddJob,
				input: "p2",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n1",
						AddedJobs:   []dax.Job{"p2"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n1",
						Jobs: []dax.Job{"p2"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{},
					},
				},
			},
			{
				// Add job.
				fn:    bal.AddJob,
				input: "p3",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n2",
						AddedJobs:   []dax.Job{"p3"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n1",
						Jobs: []dax.Job{"p2"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p3"},
					},
				},
			},
			{
				// Add job.
				fn:    bal.AddJob,
				input: "p1",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n1",
						AddedJobs:   []dax.Job{"p1"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p3"},
					},
				},
			},
			{
				// Add a third worker.
				fn:      bal.AddWorker,
				input:   "n0",
				expDiff: []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{},
					},
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p3"},
					},
				},
			},
			{
				// Add job.
				fn:    bal.AddJob,
				input: "p4",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n0",
						AddedJobs:   []dax.Job{"p4"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{"p4"},
					},
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p3"},
					},
				},
			},
			{
				// Add job.
				fn:    bal.AddJob,
				input: "p5",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n0",
						AddedJobs:   []dax.Job{"p5"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{"p4", "p5"},
					},
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p3"},
					},
				},
			},
			{
				// Add job.
				fn:    bal.AddJob,
				input: "p0",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n2",
						AddedJobs:   []dax.Job{"p0"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{"p4", "p5"},
					},
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p0", "p3"},
					},
				},
			},
			{
				// Add job.
				fn:    bal.AddJob,
				input: "p6",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n0",
						AddedJobs:   []dax.Job{"p6"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{"p4", "p5", "p6"},
					},
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p0", "p3"},
					},
				},
			},
			{
				// Add job.
				fn:    bal.AddJob,
				input: "p7",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n1",
						AddedJobs:   []dax.Job{"p7"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{"p4", "p5", "p6"},
					},
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2", "p7"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p0", "p3"},
					},
				},
			},

			//////////////////// Remove /////////////////////////

			{
				// Remove nonexistent worker.
				fn:      bal.RemoveWorker,
				input:   "nonexistent",
				expDiff: []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{"p4", "p5", "p6"},
					},
					{
						ID:   "n1",
						Jobs: []dax.Job{"p1", "p2", "p7"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p0", "p3"},
					},
				},
			},
			{
				// Remove worker.
				fn:    bal.RemoveWorker,
				input: "n1",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n1",
						AddedJobs:   []dax.Job{},
						RemovedJobs: []dax.Job{"p1", "p2", "p7"},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{"p4", "p5", "p6"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p0", "p3"},
					},
				},
			},

			{
				// Remove job (from free list).
				fn:      bal.RemoveJob,
				input:   "p2",
				expDiff: []dax.WorkerDiff{},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{"p4", "p5", "p6"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p0", "p3"},
					},
				},
			},

			{
				// Balance after remove.
				balance: true,
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n0",
						AddedJobs:   []dax.Job{"p7"},
						RemovedJobs: []dax.Job{},
					},
					{
						WorkerID:    "n2",
						AddedJobs:   []dax.Job{"p1"},
						RemovedJobs: []dax.Job{},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{"p4", "p5", "p6", "p7"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p0", "p1", "p3"},
					},
				},
			},

			{
				// Remove job.
				fn:    bal.RemoveJob,
				input: "p1",
				expDiff: []dax.WorkerDiff{
					{
						WorkerID:    "n2",
						AddedJobs:   []dax.Job{},
						RemovedJobs: []dax.Job{"p1"},
					},
				},
				expState: []dax.WorkerInfo{
					{
						ID:   "n0",
						Jobs: []dax.Job{"p4", "p5", "p6", "p7"},
					},
					{
						ID:   "n2",
						Jobs: []dax.Job{"p0", "p3"},
					},
				},
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				var diff []dax.WorkerDiff
				var err error
				if test.balance {
					diff, err = bal.Balance(ctx)
				} else {
					diff, err = test.fn(ctx, newStringWrapper(test.input))
				}
				assert.NoError(t, err)
				assert.Equal(t, test.expDiff, diff)

				cs, err := bal.CurrentState(ctx)
				assert.NoError(t, err)
				assert.Equal(t, test.expState, cs)
			})
		}
	})

	t.Run("WorkerState", func(t *testing.T) {
		db, cleanup := newBoltBalancer(t)
		defer cleanup()
		bal := boltdb.NewBalancer("test", db, logger.NewStandardLogger(os.Stderr))

		_, err := bal.AddWorker(ctx, newStringWrapper("n1"))
		assert.NoError(t, err)
		_, err = bal.AddJob(ctx, newStringWrapper("p1"))
		assert.NoError(t, err)

		exp := dax.WorkerInfo{
			ID:   "n1",
			Jobs: []dax.Job{"p1"},
		}
		ws, err := bal.WorkerState(ctx, "n1")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		// Worker doesn't exist.
		exp = dax.WorkerInfo{
			ID: "x1",
		}
		ws, err = bal.WorkerState(ctx, "x1")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)
	})

	t.Run("WorkersForJobs", func(t *testing.T) {
		db, cleanup := newBoltBalancer(t)
		defer cleanup()
		bal := boltdb.NewBalancer("test", db, logger.NewStandardLogger(os.Stderr))

		_, err := bal.AddWorker(ctx, newStringWrapper("n1"))
		assert.NoError(t, err)
		_, err = bal.AddWorker(ctx, newStringWrapper("n2"))
		assert.NoError(t, err)
		for i := 0; i < 12; i++ {
			_, err = bal.AddJob(ctx, newStringWrapper(fmt.Sprintf("p%d", i)))
			assert.NoError(t, err)
		}

		exp := dax.WorkerInfo{
			ID:   "n1",
			Jobs: []dax.Job{"p0", "p10", "p2", "p4", "p6", "p8"},
		}
		ws, err := bal.WorkerState(ctx, "n1")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		exp = dax.WorkerInfo{
			ID:   "n2",
			Jobs: []dax.Job{"p1", "p11", "p3", "p5", "p7", "p9"},
		}
		ws, err = bal.WorkerState(ctx, "n2")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		tests := []struct {
			jobs []dax.Job
			exp  []dax.WorkerInfo
		}{
			{
				jobs: []dax.Job{"p0"},
				exp: []dax.WorkerInfo{
					{ID: "n1", Jobs: []dax.Job{"p0"}},
				},
			},
			{
				jobs: []dax.Job{"p0", "p4"},
				exp: []dax.WorkerInfo{
					{ID: "n1", Jobs: []dax.Job{"p0", "p4"}},
				},
			},
			{
				jobs: []dax.Job{"p0", "p4", "p999"},
				exp: []dax.WorkerInfo{
					{ID: "n1", Jobs: []dax.Job{"p0", "p4"}},
				},
			},
			{
				jobs: []dax.Job{"p0", "p1"},
				exp: []dax.WorkerInfo{
					{ID: "n1", Jobs: []dax.Job{"p0"}},
					{ID: "n2", Jobs: []dax.Job{"p1"}},
				},
			},
			{
				jobs: []dax.Job{"p5", "p0", "p1", "p8"},
				exp: []dax.WorkerInfo{
					{ID: "n1", Jobs: []dax.Job{"p0", "p8"}},
					{ID: "n2", Jobs: []dax.Job{"p1", "p5"}},
				},
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				workers, err := bal.WorkersForJobs(ctx, test.jobs)
				assert.NoError(t, err)
				assert.Equal(t, test.exp, workers)
			})
		}

		// Some tests for WorkersForJobPrefix
		workers, err := bal.WorkersForJobPrefix(ctx, "p1")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []dax.WorkerInfo{
			{ID: "n1", Jobs: []dax.Job{"p10"}},
			{ID: "n2", Jobs: []dax.Job{"p1", "p11"}},
		}, workers)

		workers, err = bal.WorkersForJobPrefix(ctx, "p2")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []dax.WorkerInfo{
			{ID: "n1", Jobs: []dax.Job{"p2"}},
		}, workers)

		workers, err = bal.WorkersForJobPrefix(ctx, "pp")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []dax.WorkerInfo{}, workers)

	})

	t.Run("Balance", func(t *testing.T) {
		db, cleanup := newBoltBalancer(t)
		defer cleanup()
		bal := boltdb.NewBalancer("test", db, logger.NewStandardLogger(os.Stderr))

		// Add two workers with some jobs evenly spread across them.
		_, err := bal.AddWorker(ctx, newStringWrapper("n1"))
		assert.NoError(t, err)
		_, err = bal.AddWorker(ctx, newStringWrapper("n2"))
		assert.NoError(t, err)
		for i := 0; i < 13; i++ {
			_, err = bal.AddJob(ctx, newStringWrapper(fmt.Sprintf("p%d", i)))
			assert.NoError(t, err)
		}

		exp := dax.WorkerInfo{
			ID:   "n1",
			Jobs: []dax.Job{"p0", "p10", "p12", "p2", "p4", "p6", "p8"},
		}
		ws, err := bal.WorkerState(ctx, "n1")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		exp = dax.WorkerInfo{
			ID:   "n2",
			Jobs: []dax.Job{"p1", "p11", "p3", "p5", "p7", "p9"},
		}
		ws, err = bal.WorkerState(ctx, "n2")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		// Now, add a worker and confirm that it currently has no jobs assigned
		// to it.
		_, err = bal.AddWorker(ctx, newStringWrapper("n3"))
		assert.NoError(t, err)
		exp = dax.WorkerInfo{
			ID:   "n3",
			Jobs: []dax.Job{},
		}
		ws, err = bal.WorkerState(ctx, "n3")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		// Finally, call Balance() and confirm that the appropriate jobs got
		// reassigned.
		_, err = bal.Balance(ctx)
		assert.NoError(t, err)

		exp = dax.WorkerInfo{
			ID:   "n1",
			Jobs: []dax.Job{"p0", "p10", "p12", "p2", "p4"},
		}
		ws, err = bal.WorkerState(ctx, "n1")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		exp = dax.WorkerInfo{
			ID:   "n2",
			Jobs: []dax.Job{"p1", "p11", "p3", "p5"},
		}
		ws, err = bal.WorkerState(ctx, "n2")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)

		exp = dax.WorkerInfo{
			ID:   "n3",
			Jobs: []dax.Job{"p6", "p7", "p8", "p9"},
		}
		ws, err = bal.WorkerState(ctx, "n3")
		assert.NoError(t, err)
		assert.Equal(t, exp, ws)
	})
}

type stringWrapper struct {
	s string
}

func newStringWrapper(s string) *stringWrapper {
	return &stringWrapper{
		s: s,
	}
}

func (s *stringWrapper) String() string {
	return s.s
}
