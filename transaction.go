// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"
	"encoding/json"
	"regexp"
	"sync"
	"time"

	"github.com/molecula/featurebase/v2/logger"
	"github.com/pkg/errors"
)

var txIDRegexp = regexp.MustCompile("^[A-Za-z0-9_-]*$")

// Transaction contains information related to a block of work that
// needs to be tracked and spans multiple API calls.
type Transaction struct {
	// ID is an arbitrary string identifier. All transactions must have a unique ID.
	ID string `json:"id"`

	// Active notes whether an exclusive transaction is active, or
	// still pending (if other active transactions exist). All
	// non-exclusive transactions are always active.
	Active bool `json:"active"`

	// Exclusive is set to true for transactions which can only become active when no other
	// transactions exist.
	Exclusive bool `json:"exclusive"`

	// Timeout is the minimum idle time for which this transaction should continue to exist.
	Timeout time.Duration `json:"timeout"`

	// CreatedAt is the timestamp at which the transaction was created. This supports
	// the case of listing transactions in a useful order.
	CreatedAt time.Time `json:"createdAt"`

	// Deadline is calculated from Timeout. TODO reset deadline each time there is activity
	// on the transaction. (we can't do this until there is some method of associating a
	// request/call with a transaction)
	Deadline time.Time `json:"deadline"`

	// Stats track statistics for the transaction. Not yet used.
	Stats TransactionStats `json:"stats"`
}

type TransactionStats struct{}

// TransactionManager enforces the rules for transactions on a single
// node. It is goroutine-safe. It should be created by a call to
// NewTransactionManager where it takes a TransactionStore. If logging
// is desired, Log should be set before an instance of
// TransactionManager is used.
type TransactionManager struct {
	mu sync.RWMutex

	Log logger.Logger

	store TransactionStore

	checkingDeadlines bool
}

// NewTransactionManager creates a new TransactionManager with the
// given store, and starts a deadline-checker in a goroutine.
func NewTransactionManager(store TransactionStore) *TransactionManager {
	tm := &TransactionManager{
		Log:               logger.NopLogger,
		store:             store,
		checkingDeadlines: true,
	}
	// start deadline checker in case we've just started up, but there is already state in the store.
	go tm.deadlineChecker()
	return tm
}

// Start starts a new transaction with the given parameters. If an
// exclusive transaction is pending or in progress,
// ErrTransactionExclusive is returned. If a transaction with the same
// id already exists, that transaction is returned along with
// ErrTransactionExists. If there is no error, the created transaction
// is returned—this is primarily so that the caller can discover if an
// exclusive transaction has been made immediately active or if they
// need to poll.
func (tm *TransactionManager) Start(ctx context.Context, id string, timeout time.Duration, exclusive bool) (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !txIDRegexp.Match([]byte(id)) {
		return nil, errors.New("invalid transaction ID, must match [A-Za-z0-9_-]")
	}

	trnsMap, err := tm.store.List()
	if err != nil {
		return nil, errors.Wrap(err, "listing transactions in Start")
	}

	// check for an exclusive transaction
	for _, trns := range trnsMap {
		if trns.Exclusive {
			// if someone wants a transaction, and we're not able to
			// give it to them, we want to be checking deadlines.
			// TODO: it would be nice if we could identify whether
			// this trns has expired, and if so, automatically remove
			// it and continue without returning ErrTransactionExclusive
			// on this iteration of the loop. One way we could do that is
			// to call tm.checkDeadlines() here (note that we'd have to
			// have an unprotectedCheckDeadlines()), and then after that
			// check if trns still exists in tm.store. If not, continue.
			tm.startDeadlineChecker()
			return trns, ErrTransactionExclusive
		}
	}
	if trns, ok := trnsMap[id]; ok {
		return trns, ErrTransactionExists
	}

	// set new transaction to active if it is not exclusive or if
	// there are no other transactions.
	active := !exclusive || (len(trnsMap) == 0)

	// set deadline according to timeout
	createdAt := time.Now()
	deadline := createdAt.Add(timeout)
	trns := &Transaction{
		ID:        id,
		Active:    active,
		Exclusive: exclusive,
		Timeout:   timeout,
		CreatedAt: createdAt,
		Deadline:  deadline,
	}
	if err = tm.store.Put(trns); err != nil {
		return nil, errors.Wrap(err, "adding to store")
	}

	// we won't check deadlines unless there's actually a
	// transaction pending
	if exclusive && !active {
		tm.startDeadlineChecker()
	}

	return trns, nil
}

// Finish completes and removes a transaction, returning the completed
// transaction (so that the caller can e.g. view the Stats)
func (tm *TransactionManager) Finish(ctx context.Context, id string) (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.finish(id)
}

// finish is the unprotected implementation of Finish
func (tm *TransactionManager) finish(id string) (*Transaction, error) {
	trns, err := tm.store.Remove(id)
	if err != nil {
		return nil, err
	}

	// After removing, check to see if we need to activate an exclusive transaction
	trnsMap, err := tm.store.List()
	if err != nil {
		tm.log().Errorf("error listing transactions in Finish: %v", err)
		return trns, nil
	}

	if len(trnsMap) == 1 {
		for _, etrans := range trnsMap {
			if etrans.Exclusive {
				if etrans.Active { // sanity check
					panic("we just removed a transaction, and the sole remaining exclusive transaction was already active")
				}
				etrans.Active = true
				etrans.Deadline = time.Now().Add(etrans.Timeout)
				if err := tm.store.Put(etrans); err != nil {
					tm.log().Errorf("activating exclusive transaction after finishing last transaction: %v", err)
					return trns, nil
				}
			}
		}
	}
	return trns, nil
}

// Get retrieves the transaction with the given ID. Returns ErrTransactionNotFound
// if there isn't one.
func (tm *TransactionManager) Get(ctx context.Context, id string) (*Transaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.store.Get(id)
}

// List returns map of all transactions by their ID. It is a copy and
// so may be retained and modified by the caller.
func (tm *TransactionManager) List(ctx context.Context) (map[string]*Transaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.store.List()
}

// ResetDeadline updates the deadline for the transaction with the
// given ID to be equal to the current time plus the transaction's
// timeout.
func (tm *TransactionManager) ResetDeadline(ctx context.Context, id string) (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	trns, err := tm.store.Get(id)
	if err != nil {
		return nil, errors.Wrap(err, "getting transaction")
	}

	trns.Deadline = time.Now().Add(trns.Timeout)

	err = tm.store.Put(trns)
	return trns, errors.Wrap(err, "storing transaction with new timeout")
}

// startDeadlineChecker may only be called while tm.mu is held.
func (tm *TransactionManager) startDeadlineChecker() {
	if !tm.checkingDeadlines {
		tm.checkingDeadlines = true
		go tm.deadlineChecker()
	}
}

// deadlineChecker loops continuously checking for expired
// deadlines. It stops when there are no upcoming deadlines.
func (tm *TransactionManager) deadlineChecker() {
	interval := tm.checkDeadlines()
	for interval != 0 {
		time.Sleep(interval)
		interval = tm.checkDeadlines()
	}
	tm.mu.Lock()
	tm.checkingDeadlines = false
	tm.mu.Unlock()
}

// checkDeadlines finishes transactions which are past their
// deadlines. It returns the duration until the next deadline. If
// there are no exclusive transactions, it does nothing and returns 0
// as a signal to stop checking.
func (tm *TransactionManager) checkDeadlines() time.Duration {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	trnsMap, err := tm.store.List()
	if err != nil {
		tm.log().Errorf("transaction deadline checker couldn't list transactions: %v", err)
		return 0
	}

	hasExclusive := false
	for _, trns := range trnsMap {
		if trns.Exclusive {
			hasExclusive = true
			break
		}
	}
	if !hasExclusive {
		return 0 // no need to expire things if nothing is waiting
	}

	now := time.Now()
	// track the time interval to next deadline
	nextInterval := time.Duration(0)
	for id, trns := range trnsMap {
		if !trns.Active {
			continue
		}
		if !now.Before(trns.Deadline) {
			trnsF, err := tm.finish(id)
			if err != nil {
				tm.log().Errorf("error finishing expired transaction '%s': %+v: %v", id, trnsF, err)
			} else {
				tm.log().Infof("cleared expired transaction: %+v", trnsF)
			}
		} else {
			interval := trns.Deadline.Sub(now)
			if nextInterval == 0 || interval < nextInterval {
				nextInterval = interval
			}
		}
	}
	return nextInterval
}

func (tm *TransactionManager) log() logger.Logger {
	if tm.Log != nil {
		return tm.Log
	}
	return logger.NopLogger
}

// TransactionStore declares the functionality which a store for
// Pilosa transactions must implement.
type TransactionStore interface {
	// Put stores a new transaction or replaces an existing transaction with the given one.
	Put(trns *Transaction) error
	// Get retrieves the transaction at id or returns ErrTransactionNotFound if there isn't one.
	Get(id string) (*Transaction, error)
	// List returns a map of all transactions by ID. The map must be safe to modify by the caller.
	List() (map[string]*Transaction, error)
	// Remove deletes the transaction from the store. It must return ErrTransactionNotFound if there isn't one.
	Remove(id string) (*Transaction, error)
}

type OpenTransactionStoreFunc func(path string) (TransactionStore, error)

func OpenInMemTransactionStore(path string) (TransactionStore, error) {
	return NewInMemTransactionStore(), nil
}

// InMemTransactionStore does not persist transaction data and is only
// useful for testing.
type InMemTransactionStore struct {
	mu   sync.RWMutex
	tmap map[string]*Transaction
}

func NewInMemTransactionStore() *InMemTransactionStore {
	return &InMemTransactionStore{
		tmap: make(map[string]*Transaction),
	}
}

func (s *InMemTransactionStore) Put(trns *Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tmap[trns.ID] = trns.Copy()
	return nil
}

func (s *InMemTransactionStore) Get(id string) (*Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if trns, ok := s.tmap[id]; ok {
		return trns.Copy(), nil
	}
	return nil, ErrTransactionNotFound
}

func (s *InMemTransactionStore) List() (map[string]*Transaction, error) {
	cp := make(map[string]*Transaction)
	for id, trns := range s.tmap {
		cp[id] = trns.Copy()
	}
	return cp, nil
}

func (s *InMemTransactionStore) Remove(id string) (*Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if trns, ok := s.tmap[id]; ok {
		delete(s.tmap, id)
		return trns.Copy(), nil
	}
	return nil, ErrTransactionNotFound
}

type Error string

func (e Error) Error() string { return string(e) }

const ErrTransactionNotFound = Error("transaction not found")
const ErrTransactionExclusive = Error("there is an exclusive transaction, try later")
const ErrTransactionExists = Error("transaction with the given id already exists")

func CompareTransactions(t1, t2 *Transaction) error {
	if t1 == nil && t2 == nil {
		return nil
	}
	if t1 == nil || t2 == nil {
		return errors.Errorf("transactions are not equal: %+v %+v", t1, t2)
	}
	if t1.ID != t2.ID {
		return errors.Errorf("transaction IDs not equal: %+v %+v", t1, t2)
	}
	if t1.Active != t2.Active {
		return errors.Errorf("transaction Actives not equal: %+v %+v", t1, t2)
	}
	if t1.Exclusive != t2.Exclusive {
		return errors.Errorf("transaction Exclusives not equal: %+v %+v", t1, t2)
	}
	if t1.Timeout != t2.Timeout {
		return errors.Errorf("transaction Timeouts not equal: %+v %+v", t1, t2)
	}
	// don't care about Deadline or Stats
	return nil
}

func (trns *Transaction) UnmarshalJSON(b []byte) error {
	tmp := &struct {
		ID        string      `json:"id"`
		Active    bool        `json:"active"`
		Exclusive bool        `json:"exclusive"`
		Timeout   interface{} `json:"timeout"`
		Deadline  string      `json:"deadline"`
	}{}
	err := json.Unmarshal(b, tmp)
	if err != nil {
		return err
	}
	trns.ID = tmp.ID
	trns.Active = tmp.Active
	trns.Exclusive = tmp.Exclusive
	switch tm := tmp.Timeout.(type) {
	case string:
		dur, err := time.ParseDuration(tm)
		if err != nil {
			return errors.Wrapf(err, "timeout as string must be a valid duration got: '%s'", tm)
		}
		trns.Timeout = dur
	case float64:
		// interpret as number of seconds
		seconds := int64(tm)
		nsec := (tm - float64(seconds)) * 1e9
		trns.Timeout = time.Duration(seconds*1e9 + int64(nsec))
	case nil:
		break
	default:
		return errors.New("timeout must be float64 or string")
	}

	if tmp.Deadline != "" {
		trns.Deadline, err = time.Parse(time.RFC3339Nano, tmp.Deadline)
	}
	return errors.Wrap(err, "parsing deadline")
}

func (trns *Transaction) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ID        string `json:"id"`
		Active    bool   `json:"active"`
		Exclusive bool   `json:"exclusive"`
		Timeout   string `json:"timeout"`
		CreatedAt string `json:"createdAt"`
		Deadline  string `json:"deadline"`
	}{
		ID:        trns.ID,
		Active:    trns.Active,
		Exclusive: trns.Exclusive,
		Timeout:   trns.Timeout.String(),
		CreatedAt: trns.CreatedAt.In(time.UTC).Format(time.RFC3339Nano),
		Deadline:  trns.Deadline.In(time.UTC).Format(time.RFC3339Nano),
	})
}

func (trns *Transaction) Copy() *Transaction {
	return &Transaction{
		ID:        trns.ID,
		Active:    trns.Active,
		Exclusive: trns.Exclusive,
		Timeout:   trns.Timeout,
		CreatedAt: trns.CreatedAt,
		Deadline:  trns.Deadline,
		Stats:     trns.Stats,
	}
}
