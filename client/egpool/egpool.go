package egpool

import (
	"errors"
	"fmt"
	"sync"
)

type Group struct {
	PoolSize int

	jobs chan func() error

	sema     chan struct{}
	errMu    sync.Mutex
	firstErr error
	errs     []error
}

func (eg *Group) Go(f func() error) {
	if eg.PoolSize <= 0 {
		eg.PoolSize = 1
	}

	if eg.jobs == nil {
		eg.jobs = make(chan func() error)
		eg.sema = make(chan struct{}, eg.PoolSize)
	}

	// Start the job in an idle worker if possible.
	select {
	case eg.jobs <- f:
		return
	default:
	}

	// Start a new worker if necessary.
	select {
	case eg.jobs <- f:
		// A worker finished its previous job and took this one over.
		return
	case eg.sema <- struct{}{}:
		// Start a new worker.
		go eg.processJobs()
		eg.jobs <- f
	}
}

func (eg *Group) err(err error) {
	eg.errMu.Lock()
	defer eg.errMu.Unlock()

	if eg.firstErr == nil {
		eg.firstErr = err
	}
	eg.errs = append(eg.errs, err)
}

type ErrPanic struct {
	Value interface{}
}

func (p ErrPanic) Error() string {
	return fmt.Sprintf("panic: %v", p.Value)
}

var ErrGoexit = errors.New("runtime.Goexit used in job function")

func (eg *Group) processJobs() {
	// Notify pool of shutdown.
	defer func() { <-eg.sema }()

	// Handle panic and Goexit.
	var finished bool
	defer func() {
		if !finished {
			if p := recover(); p != nil {
				eg.err(ErrPanic{p})
			} else {
				eg.err(ErrGoexit)
			}
		}
	}()

	// Run jobs from queue.
	for jobFn := range eg.jobs {
		err := jobFn()
		if err != nil {
			eg.err(err)
		}
	}

	finished = true
}

func (eg *Group) Wait() error {
	if eg.jobs == nil {
		return nil
	}
	close(eg.jobs)
	for i := 0; i < eg.PoolSize; i++ {
		eg.sema <- struct{}{}
	}
	return eg.firstErr
}

func (eg *Group) Errors() []error {
	return eg.errs
}
