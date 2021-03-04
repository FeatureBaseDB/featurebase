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
	"sync"

	"github.com/pilosa/pilosa/v2/disco"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/clientv3util"
)

type leasedKV struct {
	cli    *clientv3.Client
	cancel context.CancelFunc

	mu sync.Mutex

	key, value string
	ttlSeconds int64
}

func newLeasedKV(cli *clientv3.Client, key string, ttlSeconds int64) *leasedKV {
	return &leasedKV{
		cli:        cli,
		key:        key,
		ttlSeconds: ttlSeconds,
	}
}

func (l *leasedKV) Start(ctx context.Context, initValue string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	l.cancel = cancel

	return l.create(ctx, initValue)
}

func (l *leasedKV) create(ctx context.Context, initValue string) error {
	leaseResp, err := l.cli.Grant(ctx, l.ttlSeconds)
	if err != nil {
		return errors.Wrap(err, "creating a lease")
	}

	if _, err := l.cli.Txn(ctx).
		Then(clientv3.OpPut(l.key, initValue, clientv3.WithLease(leaseResp.ID))).
		Commit(); err != nil {
		return errors.Wrapf(err, "creating key %s with value [%s]", l.key, initValue)
	}

	if _, err := l.cli.KeepAlive(ctx, leaseResp.ID); err != nil {
		return errors.Wrapf(err, "keeping alive the lease for the key %s with value %s", l.key, l.value)
	}

	l.value = initValue

	return nil
}

func (l *leasedKV) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.cancel != nil {
		l.cancel()
	}
}

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
