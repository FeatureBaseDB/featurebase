// Copyright 2022 Molecula Corp. All rights reserved.

// Package task provides an interface for indicating when an operation has
// been blocked, so that a worker pool which wants to be doing N things at
// a time can start trying new things when some things are blocked.
//
// To understand this, you have to start with the original context: We have
// a worker pool, which can handle up to N tasks at once. Tasks come in
// in batches, asynchronously. At most one write task can be active on a given
// database at a time, but many read tasks can be active on the database,
// with or without a write task. Each read task completes only when its entire
// containing operation completes. Write tasks can *partially* complete
// immediately, but in some cases, must wait for read tasks to finish before
// they can do crucial bookkeeping work.
//
// Regardless of the workload, we always have tasks which can progress
// available, and if we do them, eventually everything will complete. However,
// for some workloads, it is possible to pick N tasks *all of which are
// blocked*. In this case, the worker pool becomes useless. Furthermore,
// even if we don't hit that state, we can hit a state where nearly all worker
// pool tasks are blocked.
//
// To address this, we need a way for a worker pool to recognize that a worker
// has become blocked, and *start another worker*. This can result in running
// more than N workers at once. However, it rarely results in running *many*
// more. The typical case would be that we have a worker pool of N, and M of
// them are blocked waiting for write access to a given database. If one of them
// becomes unblocked, we may end up with N+1 active workers, but the other M-1
// waiting on that database are still blocked.
//
// It might seem like the simplest thing to do is use a buffered channel as a
// semaphore, this being a standard Go idiom for pools. It's a great idiom, but
// in our case, it runs into a problem. When each worker starts, it writes into
// a buffered channel. When it becomes blocked, it reads from the channel to
// free up a slot. When it becomes unblocked, then, it has to write to the
// channel to indicate that it's taking up a slot again. But writes to the
// channel are contested, and usually only become possible when something else
// either blocks or exits... Meaning that, precisely at the moment that we have
// gained a highly contested lock and are able to proceed, we block for an
// indeterminate period of time *while holding that lock*. This is the opposite
// of what we want.
package task
