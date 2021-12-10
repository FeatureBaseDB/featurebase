// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	// "runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/syswrap"
	"github.com/pkg/errors"
)

// generation represents one "generation" of opening a data file.
// This is what determines when it's safe to unmap a data file, if it
// got mapped, and handles closing/reopening files if we need to
// manage file handle availability. It's an interface because this
// lets us write simpler code for specific cases, rather than handling
// the whole matrix of mapped/unmapped, staying open/being reopened,
// etcetera.
//
// You create a generation by calling newGeneration with a file
// path. If it succeeds in opening that path, it calls a provided
// setup function with the data from the generation, and a flag
// indicating whether the data is mmapped. If the setup function
// fails, newGeneration cleans things up and closes. Otherwise,
// it returns a generation.
//
// The generation itself uses runtime.SetFinalizer to clean up when
// the last reference to it goes away. You should store a pointer
// to the generation in any object which is reliant on the generation.
//
// When you anticipate a generation should be done (for instance,
// opening a new generation), the old one gets marked done, which
// stashes a timestamp in it. Later operations can check whether
// the timestamp is a while back, and if so, complain that something
// might be wrong.
//
// In some cases, we don't have enough open file limit to keep every
// file actually open. To address this, use the `Transaction` function,
// which ensures that the file is open, stores a reference to it in
// a provided `*io.Writer`, and then restores the previous value of
// the io.Writer when it's done. For instance, for a bitmap, this might
// be used with `&b.OpWriter`.
//
// newGeneration takes an optional previous generation; it calls
// that generation's Done function after running the provided setup,
// and bumps the generation count.
type generation interface {
	// Transaction runs the given transaction with the generation's
	// file open. If the **os.File parameter is
	// non-nil, the generation's file will be open, and stored
	// into that pointer, during the execution of func, after
	// which the previous contents are restored. Otherwise
	// the file may or may not be open during the operation.
	Transaction(*io.Writer, func() error) error
	// Done() should be called exactly once, to indicate that a
	// generation is expected not to be in use for long -- for instance,
	// when a new generation replaces it.
	Done()
	// Generation count.
	Generation() int64
	// ID indicates the source -- path and generation number -- that
	// this generation represents.
	ID() string
	// Dead indicates whether this generation is Done.
	Dead() bool
	// Bytes reports the storage associated with this generation, if any.
	// DO NOT USE THIS. Except if you're debugging mmap segfaults.
	Bytes() []byte
}

type mmapGeneration struct {
	mu         sync.Mutex // mutex guards modifiers of generation, not of data
	transMu    sync.Mutex // guards transactions, specifically
	path       string
	id         string
	file       *os.File
	data       []byte
	generation int64     // generation counter
	dead       bool      // we think this generation is dead
	deadSince  time.Time // when this generation was marked dead
	retries    int       // for cases where we're retrying
	logger     logger.Logger
}

func (m *mmapGeneration) Dead() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dead
}

func (m *mmapGeneration) ID() string {
	return m.id
}

func (m *mmapGeneration) Generation() int64 {
	return m.generation
}

// Transaction runs an exclusive call, ensuring that the file is open if
// the *io.Writer parameter is present.
func (m *mmapGeneration) Transaction(fileP *io.Writer, fn func() error) (transactionErr error) {
	m.transMu.Lock()
	defer m.transMu.Unlock()
	// HEY LOOK CAREFULLY AT THIS BIT:
	// We can't just defer this unlock. We specifically want to be
	// sure to unlock the regular mutex *before* this function is over,
	// and if we error out trying to open the file, we want to do it
	// even sooner. If we deferred this, the transaction would block
	// *everything*, including things like sanity checks against the
	// generation being Dead(), but also including the deferred
	// re-close-the-file.
	m.mu.Lock()
	// if we've been asked for a file pointer, we need to ensure that
	// our file is open, and that the file pointer to it is stored in
	// the requested location, then revert that when we're done.
	// if we aren't asked for a file pointer, nothing needs the file
	// open.
	if m.dead {
		elapsed := time.Since(m.deadSince)
		m.logger.Warnf("transaction against %s, which has been dead for %v\n", m.id, elapsed)
	}
	if fileP != nil {
		if m.file == nil {
			// we ignore the shouldClose response here; if this
			// fragment was previously not being kept open, we're
			// going to stick with that.
			_, err := m.openFile()
			if err != nil {
				m.mu.Unlock()
				return err
			}
			defer func() {
				// report a close error if we have no other error to report
				m.mu.Lock()
				defer m.mu.Unlock()
				err := m.closeFile()
				if transactionErr == nil {
					transactionErr = err
				}
			}()
		}
		var fileStash io.Writer
		fileStash, *fileP = *fileP, m.file
		defer func() {
			*fileP = fileStash
		}()
	}
	// We are done locking the generation itself for now.
	m.mu.Unlock()
	// wouldPanic := debug.SetPanicOnFault(true)
	//	defer func() {
	//		debug.SetPanicOnFault(wouldPanic)
	//		if r := recover(); r != nil {
	//			if err, ok := r.(error); ok {
	//				// special case: if we caught a page fault, we diagnose that directly. sadly,
	//				// we can't see the actual values that were used to generate this, probably.
	//				if err.Error() == "runtime error: invalid memory address or nil pointer dereference" {
	//					if transactionErr == nil {
	//						transactionErr = errors.New("invalid memory access during transaction")
	//					} else {
	//						transactionErr = fmt.Errorf("invalid memory access during transaction, previous error %v", transactionErr)
	//					}
	//					return
	//				}
	//			}
	//			if transactionErr == nil {
	//				transactionErr = fmt.Errorf("panic during transaction: %v", r)
	//			} else {
	//				transactionErr = fmt.Errorf("panic during erroring transaction: panic %v, previous error %v", r, transactionErr)
	//			}
	//		}
	//	}()
	return fn()
}

func (m *mmapGeneration) Bytes() []byte {
	return m.data
}

// Done marks the generation done, and closes its file, but may not unmap it.
// It's still conceptually possible to end up doing a Transaction against a
// done generation, but it's a red flag.
func (m *mmapGeneration) Done() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dead {
		oops := fmt.Sprintf("generation %s, marked done again at %v, previously marked dead at %v",
			m.id, time.Now(), m.deadSince)
		panic(oops)
	}
	m.dead = true
	m.deadSince = time.Now()
	err := m.closeFile()
	if err != nil {
		m.logger.Errorf("error closing generation %s: %v", m.id, err)
	}
	// If we're not debugging, the finalizer won't have been enabled
	// previously. Finalizers have non-zero cost, so having them not be
	// created until they're needed seems rewarding?
	if !generationDebug {
		runtime.SetFinalizer(m, generationFinalizer)
	}
	endGeneration(m.id)
	// note, Done() doesn't close the file; only the finalizer actually
	// does the shutdown.
}

// Try to close the file if it's currently open.
func (m *mmapGeneration) closeFile() error {
	var lastErr error
	// report the most serious error encountered, but still close
	// file even if something else failed.
	if m.file != nil {
		if err := m.file.Sync(); err != nil {
			lastErr = fmt.Errorf("sync: %s", err)
		}
		if err := syscall.Flock(int(m.file.Fd()), syscall.LOCK_UN); err != nil {
			lastErr = fmt.Errorf("unlock: %s", err)
		}
		if err := syswrap.CloseFile(m.file); err != nil {
			lastErr = fmt.Errorf("close file: %s", err)
		}
		m.file = nil
	}
	return lastErr
}

// openFile ensures the file is open and locked, or fails. If it does
// open the file, it will also report the "you need to close this file
// when you're done" flag from syswrap.
func (m *mmapGeneration) openFile() (shouldClose bool, err error) {
	if m.file != nil {
		return false, nil
	}
	m.file, shouldClose, err = syswrap.OpenFile(m.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return false, err
	}

	// do we actually want this in every openFile? I don't know.
	if err := syscall.Flock(int(m.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = syswrap.CloseFile(m.file)
		m.file = nil
		return false, fmt.Errorf("flock: %s", err)
	}
	return shouldClose, nil
}

func generationFinalizer(m *mmapGeneration) {
	m.mu.Lock()
	if !m.dead {
		m.logger.Infof("finalizing generation %s which isn't dead yet\n",
			m.id)
	}
	m.mu.Unlock()
	err := m.closeFile()
	if err != nil {
		m.logger.Errorf("finalizing generation, closing file: %v\n", err)
	}
	if m.data != nil {
		err := syswrap.Munmap(m.data)
		if err != nil {
			m.logger.Errorf("finalizing generation, munmap: %v\n", err)
		}
		m.data = nil
	}
	finalizeGeneration(m.id)
}

// Cancel closes a generation out entirely. It cancels any finalizer,
// unmaps any data, ends generation tracking, and closes any files.
// It does each of these separately whether or not the others need to be done,
// or succeed. It's used to handle failures from newGeneration; it makes sure
// the generation isn't holding any resources and doesn't need to be cleaned
// up otherwise.
//
// Mostly a helper function because there's several cases where newGeneration
// might fail.
func (m *mmapGeneration) Cancel() {
	if m.data != nil {
		_ = syswrap.Munmap(m.data)
		m.data = nil
	}
	err := m.closeFile()
	if err != nil {
		m.logger.Errorf("error cancelling generation %s: %v", m.id, err)
	}
	runtime.SetFinalizer(m, nil)
	m.dead = true
	m.deadSince = time.Now()
	cancelGeneration(m.id)
}

// newGeneration creates a new generation using the given file path. It
// then calls the provided setup function with the allocated storage, a
// file handle, the new generation, and a flag indicatting whether the storage
// is memory-mapped. If the setup function returns a non-nil error, the
// generation is cleaned up, and newGeneration fails. The setup function
// also returns a boolean indicating whether it used the mapping; if it
// didn't, newGeneration discards the mapping and returns a nil generation.
//
// If generationDebug is enabled, we track the generation even if no mapping
// is actually in use, so we can verify that the tracking is working.
//
// On failure, newGeneration returns nil values for generation and func,
// and an error. On success, the func returned is the close func to use
// when the generation is no longer needed by the caller.
func newGeneration(existing generation, path string, readData bool, setup func([]byte, *os.File, generation, bool) (bool, error), logger logger.Logger) (generation, error) {
	m := mmapGeneration{path: path, logger: logger}
	if existing != nil {
		m.generation = existing.Generation() + 1
		m.retries = existing.(*mmapGeneration).retries
		// we might keep a previous generation around just for its generation count.
		if !existing.Dead() {
			defer existing.Done()
		}
	}
	shouldClose, err := m.openFile()
	if err != nil {
		return nil, err
	}
	m.id = fmt.Sprintf("%s:%d", m.path, m.generation)
	// possibly assign new generation ID if this one's been used, which can
	// happen with reopens, especially during testing.
	m.id = registerGeneration(m.id)
	// if debugging, we always want the finalizer on so we notice if a
	// generation is finalized without being closed. for non-debugging
	// use, we only need it when the generation is closed.
	if generationDebug {
		runtime.SetFinalizer(&m, generationFinalizer)
	}
	// Mmap the underlying file so it can be zero copied.
	var mapped bool
	var data []byte
	fi, err := m.file.Stat()
	if err == nil && fi.Size() > 0 {
		data, err = syswrap.Mmap(int(m.file.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
		if err == syswrap.ErrMaxMapCountReached {
			// I have no idea where/how to display this message.
			m.logger.Warnf("maximum number of maps reached, reading file '%s' instead", m.path)
		} else if err != nil {
			m.Cancel()
			return nil, errors.Wrap(err, "mmap failed")
		} else {
			mapped = true
		}
	}
	if data == nil && readData {
		data, err = ioutil.ReadAll(m.file)
		if err != nil {
			m.Cancel()
			return nil, errors.Wrap(err, "failure file readall")
		}
	}
	// if we got here, data's the expected data, so let's try to use it
	mappedAny, err := setup(data, m.file, &m, mapped)

	// if the setup failed, we unmap data if we previously mapped it,
	// and exit. Note that having no data, or having only trivial
	// data (like a zero-container Roaring file) isn't "failed".
	if err != nil {
		m.Cancel()
		// Unless, that is, we think the file probably ought to
		// be truncated: For instance, if a bitmap has a corrupted
		// ops log, we could truncate that part of it and retry.
		if err, ok := err.(roaring.FileShouldBeTruncatedError); ok && m.retries < 1 {
			m.logger.Infof("file %s read partially, but should-be-truncated at %d bytes\n", m.path, err.SuggestedLength())
			// close this generation, then try again. once.
			m.retries++
			err := os.Truncate(m.path, err.SuggestedLength())
			if err != nil {
				m.logger.Errorf("truncating file failed [but retrying anyway]: %v\n", err)
			}
			return newGeneration(&m, path, readData, setup, logger)
		}
		return nil, err
	}

	if mapped {
		// when generationDebug is on, we want to track this even
		// if it's not being used.
		if generationDebug || mappedAny {
			// Advise the kernel that the mmap is accessed randomly.
			// We don't care much about errors with this.
			_ = madvise(data, syscall.MADV_RANDOM)
			// store the data, so we can unmap it when this generation
			// gets finalized.
			m.data = data
		} else {
			// unmap the data and don't stash the pointer in this
			// generation. It's not being used. This generation
			// doesn't need to exist, yay.
			unmapErr := syswrap.Munmap(data)
			if unmapErr != nil {
				m.logger.Errorf("error unmapping (probably harmless): %v", unmapErr)
			}
		}
	}
	// shouldClose comes from underlying syswrap.OpenFile, which checks
	// a count of open files to hint at us when we need to start closing
	// files to preserve open file descriptor limit.
	if shouldClose {
		err := m.closeFile()
		if err != nil {
			m.logger.Errorf("closing file to preserve open files failed: %v\n", err)
		}
	}
	// It's possible that the generation has no actual data to track,
	// because nothing's mapped, in which case there won't be any bitmap
	// sources following this, just the fragment source. (Bitmaps won't
	// be attached to the source unless they're actually mapped to it,
	// or generationDebug is true). That's okay. We pay a tiny cost
	// for the finalizer, but we also get higher confidence that it really
	// does get cleaned up.
	return &m, nil
}

// NopGeneration is used in fragment.openStorage() to short-circuit
// generation stuff that only applies to RoaringTx; doesn't apply to RBFTx/BadgerTx/etc.
type NopGeneration struct {
}

func (g *NopGeneration) Transaction(w *io.Writer, f func() error) error {
	return f()
}
func (g *NopGeneration) Done() {}
func (g *NopGeneration) Generation() int64 {
	return 0
}
func (g *NopGeneration) ID() string {
	return "NOP"
}
func (g *NopGeneration) Dead() bool {
	return true
}
func (g *NopGeneration) Bytes() (ret []byte) {
	return
}
