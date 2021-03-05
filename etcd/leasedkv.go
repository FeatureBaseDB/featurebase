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

	"github.com/pilosa/pilosa/v2/disco"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/clientv3util"
)

// leasedKV is an etcd key and value attached to a lease. It can be used to detect if a node went down.
// It will try to renew the lease at any cost after losing it.
// It will recreate the previous existing value for the key again.
type leasedKV struct {
	cli    *clientv3.Client
	cancel context.CancelFunc

	key        string
	ttlSeconds int64

	mu      sync.Mutex
	value   string // protected by mu
	stopped bool   // protected by mu
}

func newLeasedKV(cli *clientv3.Client, key string, ttlSeconds int64) *leasedKV {
	return &leasedKV{
		cli:        cli,
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
	l.cancel = cancel

	leaseResp, err := l.cli.Grant(ctx, l.ttlSeconds)
	if err != nil {
		return nil, errors.Wrap(err, "creating a lease")
	}

	if _, err := l.cli.Txn(ctx).
		Then(clientv3.OpPut(l.key, initValue, clientv3.WithLease(leaseResp.ID))).
		Commit(); err != nil {
		return nil, errors.Wrapf(err, "creating key %s with value [%s]", l.key, initValue)
	}

	kaChann, err := l.cli.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return nil, errors.Wrapf(err, "keeping alive the lease for the key %s with value %s", l.key, l.value)
	}

	l.value = initValue

	return kaChann, nil
}

func (l *leasedKV) consumeLease(ch <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		_, ok := <-ch
		if !ok {
			l.mu.Lock()

			if l.stopped {
				l.mu.Unlock()
				return
			}

			if ok := retry(1*time.Second, func() error {
				kaChann, err := l.create(l.value)
				if err != nil {
					return err
				}

				go l.consumeLease(kaChann)
				return nil
			}); !ok {
				log.Println("lease cannot be recreated. Key:", l.key)
				l.mu.Unlock()
				return
			}

			log.Println("lease recreated after a problem. Key:", l.key)
			l.mu.Unlock()
			return
		}
	}
}

// Stop will cancel the lease renewal.
// After calling Stop, this object should be discarded and not used anymore.
func (l *leasedKV) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.cancel != nil {
		l.cancel()
	}

	l.stopped = true
}

// Set will change the specific value for this key.
func (l *leasedKV) Set(ctx context.Context, value string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.cli.Txn(ctx).
		Then(clientv3.OpPut(l.key, value, clientv3.WithIgnoreLease())).
		Commit(); err != nil {
		return errors.Wrapf(err, "creating key %s with value [%s]", l.key, l.value)
	}

	l.value = value

	return nil
}

// Get will obtain the actual value for the key.
func (l *leasedKV) Get(ctx context.Context) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	getResp, err := l.cli.Txn(ctx).
		If(clientv3util.KeyExists(l.key)).
		Then(clientv3.OpGet(l.key, clientv3.WithIgnoreLease())).
		Commit()
	if err != nil {
		return "", errors.Wrapf(err, "getting key %s", l.key)
	}

	if !getResp.Succeeded || len(getResp.Responses) == 0 {
		return "", disco.ErrNoResults
	}

	l.value = string(getResp.Responses[0].GetResponseRange().Kvs[0].Value)

	return l.value, nil
}

func retry(sleep time.Duration, f func() error) bool {
	for {
		err := f()
		if err == nil {
			return true
		}

		// sometimes the element in charge of stopping the lease renewal doesn't do it, causing context errors.
		if errors.Is(err, context.DeadlineExceeded) {
			return false
		}

		time.Sleep(sleep)

		log.Println("retrying after error:", err)
	}
}
