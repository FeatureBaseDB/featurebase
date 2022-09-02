package idk

import "sync/atomic"

// ProgressTracker tracks the progress of record sourcing.
type ProgressTracker struct {
	progress uint64
}

// proceed is called after a record is sourced.
func (t *ProgressTracker) proceed() {
	atomic.AddUint64(&t.progress, 1)
}

// Check the number of records that have been sourced so far.
func (t *ProgressTracker) Check() uint64 {
	return atomic.LoadUint64(&t.progress)
}

type trackedSource struct {
	Source
	t *ProgressTracker
}

func (ts *trackedSource) Record() (Record, error) {
	r, err := ts.Source.Record()
	if err == nil {
		ts.t.proceed()
	}
	return r, err
}

// Track record generation progress on a source.
// Wraps the source with a progress-tracking mechanism.
func (t *ProgressTracker) Track(src Source) Source {
	return &trackedSource{src, t}
}
