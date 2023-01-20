package dax

import (
	"sort"
	"strings"
)

// Job is a generic identifier used to represent a specific role assigned to a
// worker.
type Job string

// Job allows a Job to implement the Jobber interface.
func (j Job) Job() Job {
	return j
}

// Jobs is a slice of Job.
type Jobs []Job

type Jobber interface {
	Job() Job
}

// WorkerInfo represents a Worker and the Jobs to which it has been assigned.
type WorkerInfo struct {
	Address Address
	Jobs    []Job
}

// WorkerInfos is a sortable slice of WorkerInfo.
type WorkerInfos []WorkerInfo

func (w WorkerInfos) Len() int           { return len(w) }
func (w WorkerInfos) Less(i, j int) bool { return w[i].Address < w[j].Address }
func (w WorkerInfos) Swap(i, j int)      { w[i], w[j] = w[j], w[i] }

// WorkerDiff represents the changes made to a Worker following the latest
// event.
type WorkerDiff struct {
	Address     Address
	AddedJobs   []Job
	RemovedJobs []Job
}

// Add adds w2 to w. It panics of w and w2 don't have teh same worker
// ID. Any job that is added and then removed or removed and then
// added cancels out and won't be present after add is called.
func (w *WorkerDiff) Add(w2 WorkerDiff) {
	if w.Address != w2.Address {
		panic("can't add worker diffs from different workers")
	}
	a1 := NewSet(w.AddedJobs...)
	a2 := NewSet(w2.AddedJobs...)
	r1 := NewSet(w.RemovedJobs...)
	r2 := NewSet(w2.RemovedJobs...)

	// final Added is (a1 - r2) + (a2 - r1)
	// this is because anything that is removed and then added, or added and then removed cancels out
	added := a1.Minus(r2).Plus(a2.Minus(r1))

	// final removed is (r1 - a2) + (r2 - a1)
	removed := r1.Minus(a2).Plus(r2.Minus(a1))

	w.AddedJobs = added.Slice()
	w.RemovedJobs = removed.Slice()
}

// WorkerDiffs is a sortable slice of WorkerDiff.
type WorkerDiffs []WorkerDiff

func (w WorkerDiffs) Len() int           { return len(w) }
func (w WorkerDiffs) Less(i, j int) bool { return w[i].Address < w[j].Address }
func (w WorkerDiffs) Swap(i, j int)      { w[i], w[j] = w[j], w[i] }

// Set is a set of stringy items.
type Set[K ~string] map[K]struct{}

func NewSet[K ~string](stuff ...K) Set[K] {
	s := make(map[K]struct{})
	for _, thing := range stuff {
		s[thing] = struct{}{}
	}
	return Set[K](s)
}

// Count returns the number of items in the set.
func (s Set[K]) Count() int {
	return len(s)
}

// Contains returns true if k is in the set.
func (s Set[K]) Contains(k K) bool {
	_, ok := s[k]
	return ok
}

// Add adds k to the set.
func (s Set[K]) Add(k K) {
	s[k] = struct{}{}
}

// Remove removes k from the set.
func (s Set[K]) Remove(k K) {
	delete(s, k)
}

// RemoveByPrefix removes all items from Set that have the given prefix.
func (s Set[K]) RemoveByPrefix(prefix string) []K {
	ret := make([]K, 0)
	for k := range s {
		if strings.HasPrefix(string(k), prefix) {
			ret = append(ret, k)
			delete(s, k)
		}
	}
	return ret
}

// Slice returns a slice containing each member of the set in an undefined order.
func (s Set[K]) Slice() []K {
	ret := make([]K, 0, len(s))
	for k := range s {
		ret = append(ret, k)
	}
	return ret
}

// Copy creates a copy of the set.
func (s Set[K]) Copy() Set[K] {
	ret := make(map[K]struct{})
	for k, v := range s {
		ret[k] = v
	}
	return ret
}

// Minus returns the a copy of s without any members which are also in s2.
func (s Set[K]) Minus(s2 Set[K]) Set[K] {
	ret := make(map[K]struct{})
	for k := range s {
		if _, ok := s2[k]; !ok {
			ret[k] = struct{}{}
		}
	}
	return ret
}

// Plus returns a copy of s that also contains all members of s2.
func (s Set[K]) Plus(s2 Set[K]) Set[K] {
	ret := s.Copy()
	for k := range s2 {
		ret[k] = struct{}{}
	}
	return ret
}

// Merge adds the members of s2 to s.
func (s Set[K]) Merge(s2 Set[K]) {
	for k, v := range s2 {
		s[k] = v
	}
}

// Sorted returns Set[K] as a sorted slice of K.
func (s Set[K]) Sorted() []K {
	js := make([]K, 0, len(s))
	for j := range s {
		js = append(js, j)
	}
	sort.Slice(js, func(i, j int) bool {
		return js[i] < js[j]
	})

	return js
}
