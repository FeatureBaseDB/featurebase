// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/molecula/featurebase/v2/disco"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/clientv3util"
)

// leasedKV is an etcd key and value attached to a lease. It can be used to detect if a node went down.
// It will try to renew the lease at any cost after losing it.
// It will recreate the previous existing value for the key again.
type leasedKV struct {
	e       *Etcd
	cancel  context.CancelFunc
	done    <-chan struct{}
	leaseID clientv3.LeaseID

	key        string
	ttlSeconds int64

	mu      sync.Mutex
	value   string // protected by mu
	stopped bool   // protected by mu
}

func newLeasedKV(e *Etcd, key string, ttlSeconds int64) *leasedKV {
	return &leasedKV{
		e:          e,
		key:        key,
		ttlSeconds: ttlSeconds,
	}
}

// Start creates the key and attaches it to a lease.
// If the lease cannot be renewed in time, it will try to renew it ad finitum.
func (l *leasedKV) Start(initValue string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	kaChann, err := l.create(initValue)
	if err != nil {
		return err
	}

	go l.consumeLease(kaChann)
	return nil
}

func (l *leasedKV) create(initValue string) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ctx, cancel := context.WithCancel(context.Background())

	if l.cancel != nil {
		l.cancel()
	}
	l.cancel = cancel
	l.done = ctx.Done()
	var leaseResp *clientv3.LeaseGrantResponse

	err := l.e.retryClient(func(cli *clientv3.Client) (err error) {
		leaseResp, err = cli.Grant(ctx, l.ttlSeconds)
		return err
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating a lease")
	}
	l.leaseID = leaseResp.ID

	err = l.e.retryClient(func(cli *clientv3.Client) (err error) {
		_, err = cli.Txn(ctx).
			Then(clientv3.OpPut(l.key, initValue, clientv3.WithLease(l.leaseID))).
			Commit()
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "creating key %s with value [%s]", l.key, initValue)
	}

	var kaChan <-chan *clientv3.LeaseKeepAliveResponse
	err = l.e.retryClient(func(cli *clientv3.Client) (err error) {
		kaChan, err = cli.KeepAlive(ctx, l.leaseID)
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "keeping alive the lease for the key %s with value %s", l.key, l.value)
	}

	l.value = initValue

	return kaChan, nil
}

func (l *leasedKV) consumeLease(ch <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		select {
		case _, ok := <-ch:
			if ok {
				continue
			}
			l.mu.Lock()

			if l.stopped {
				l.mu.Unlock()
				return
			}

			if e := retry("consumeLease", 1*time.Second, func() error {
				kaChann, err := l.create(l.value)
				if err != nil {
					return err
				}

				go l.consumeLease(kaChann)
				return nil
			}); e != nil {
				log.Printf("lease %q cannot be recreated: %v", l.key, e)
				l.mu.Unlock()
				return
			}

			log.Printf("lease %q recreated after a problem", l.key)
			l.mu.Unlock()
			return
		case <-l.done:
			// don't recreate lease.
			return
		}
	}
}

// Stop will cancel the lease renewal.
// After calling Stop, this object should be discarded and not used anymore.
func (l *leasedKV) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped = true
	if l.cancel != nil {
		l.cancel()
	}
	// low-effort attempt to cancel existing lease. if the cluster is
	// shutting down, we don't want this to take long.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := l.e.retryClient(func(cli *clientv3.Client) (err error) {
		_, err = cli.Revoke(ctx, l.leaseID)
		return err
	})
	// if retryClient succeeds, just to be thorough, we'll cancel that
	// context.
	cancel()
	if err != nil {
		// It turns out that this will almost always report a
		// failure because since we're shutting things down,
		// the cluster as a whole may not be able to process responses.
		// So this is a low-interest message usually.
		l.e.logger.Debugf("revoking lease during shutdown: %v", err)
	}

}

// Set will change the specific value for this key.
func (l *leasedKV) Set(ctx context.Context, value string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.e.retryClient(func(cli *clientv3.Client) (err error) {
		_, err = cli.Txn(ctx).
			Then(clientv3.OpPut(l.key, value, clientv3.WithIgnoreLease())).
			Commit()
		return err
	})
	// l.e.logger.Printf("set key %q on %q value %q: err %v", l.key, l.e.options.Name, value, err)

	if err != nil {
		return errors.Wrapf(err, "creating key %s with value [%s]", l.key, l.value)
	}

	l.value = value

	return nil
}

// Get will obtain the actual value for the key.
func (l *leasedKV) Get(ctx context.Context) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var getResp *clientv3.TxnResponse
	err := l.e.retryClient(func(cli *clientv3.Client) (err error) {
		getResp, err = cli.Txn(ctx).
			If(clientv3util.KeyExists(l.key)).
			Then(clientv3.OpGet(l.key, clientv3.WithIgnoreLease())).
			Commit()
		return err
	})
	if err != nil {
		return "", errors.Wrapf(err, "getting key %s", l.key)
	}

	if !getResp.Succeeded || len(getResp.Responses) == 0 {
		return "", disco.ErrNoResults
	}

	l.value = string(getResp.Responses[0].GetResponseRange().Kvs[0].Value)

	return l.value, nil
}

func retry(desc string, sleep time.Duration, f func() error) (err error) {
	for {
		lastErr := f()
		if lastErr == nil {
			return lastErr
		}
		if errors.Is(lastErr, context.DeadlineExceeded) {
			if err != nil {
				return err
			} else {
				return lastErr
			}
		}
		log.Printf("%s: got error %v, retrying", desc, lastErr)
		err = lastErr
		time.Sleep(sleep)
	}
}
