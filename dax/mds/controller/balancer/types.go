package balancer

import (
	"sort"

	"github.com/featurebasedb/featurebase/v3/dax"
)

// jobSetDiffs is used internally to capture the diffs as they're happening. We
// call output() to generate the final result.
type jobSetDiffs struct {
	added   dax.Set[dax.Job]
	removed dax.Set[dax.Job]
}

func newJobSetDiffs() jobSetDiffs {
	return jobSetDiffs{
		added:   dax.NewSet[dax.Job](),
		removed: dax.NewSet[dax.Job](),
	}
}

type InternalDiffs map[dax.Address]jobSetDiffs

func NewInternalDiffs() InternalDiffs {
	return make(InternalDiffs)
}

func (d InternalDiffs) Added(address dax.Address, job dax.Job) {
	if _, ok := d[address]; !ok {
		d[address] = newJobSetDiffs()
	}

	// Before adding the job, make sure we haven't indicated that it has been
	// removed prior to this. If it has, we need to invalidate that "remove"
	// instruction.
	d[address].removed.Remove(job)

	d[address].added.Add(job)
}

func (d InternalDiffs) Removed(address dax.Address, job dax.Job) {
	if _, ok := d[address]; !ok {
		d[address] = newJobSetDiffs()
	}

	// Before removing the job, make sure we haven't indicated that it has been
	// added prior to this. If it has, we need to invalidate that "add"
	// instruction.
	d[address].added.Remove(job)

	d[address].removed.Add(job)
}

func (d InternalDiffs) Merge(d2 InternalDiffs) {
	for k, v := range d2 {
		if _, ok := d[k]; !ok {
			d[k] = newJobSetDiffs()
		}
		d[k].added.Merge(v.added)
		d[k].removed.Merge(v.removed)
	}
}

// Output converts internalDiff to []controller.WorkerDiff for external
// consumption.
func (d InternalDiffs) Output() []dax.WorkerDiff {
	out := make([]dax.WorkerDiff, len(d))

	i := 0
	for k, v := range d {
		out[i].Address = k
		out[i].AddedJobs = v.added.Sorted()
		out[i].RemovedJobs = v.removed.Sorted()
		i++
	}

	sort.Sort(dax.WorkerDiffs(out))

	return out
}
