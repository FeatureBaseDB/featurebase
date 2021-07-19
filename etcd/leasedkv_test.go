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
	"errors"
	"os"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/testhook"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver/api/v3client"
)

const initVal = "test"
const newVal = "newValue"

func TestLeasedKv(t *testing.T) {
	cfg := embed.NewConfig()

	dir, err := testhook.TempDir(t, "leasedkv-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg.Dir = dir
	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cli := v3client.New(etcd.Server)
	defer func() {
		etcd.Close()
		<-etcd.Server.StopNotify()
		cli.Close()
	}()
	wrapper := &Etcd{e: etcd, cli: cli, logger: logger.NewLogfLogger(t)}

	lkv := newLeasedKV(wrapper, "/test", 1)

	ctx := context.Background()

	err = lkv.Start(initVal)
	if err != nil {
		t.Fatal(err)
	}

	v, err := lkv.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if v != initVal {
		t.Fatal("obtained value is not the same as expected. Obtained:", v, "Expected:", initVal)
	}

	err = lkv.Set(ctx, "otherValue")
	if err != nil {
		t.Fatal(err)
	}
	err = lkv.Set(ctx, "3")
	if err != nil {
		t.Fatal(err)
	}
	err = lkv.Set(ctx, newVal)
	if err != nil {
		t.Fatal(err)
	}

	v, err = lkv.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if v != newVal {
		t.Fatal("obtained value is not the same as expected. Obtained:", v, "Expected:", newVal)
	}

	time.Sleep(5 * time.Second)

	lkv.Stop()

	// we need to wait to force the lease expiration
	time.Sleep(5 * time.Second)

	_, err = lkv.Get(ctx)
	if err == nil || !errors.Is(err, disco.ErrNoResults) {
		t.Fatal("expected error:", disco.ErrNoResults, "obtained:", err)
	}
}
