// Copyright 2020 Pilosa Corp.
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

package pg

import (
	"crypto/rand"
	"testing"
)

func TestCancel(t *testing.T) {
	mgr := NewLocalCancellationManager(rand.Reader)
	notify, cancel, token, err := mgr.Token()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-notify:
		t.Fatal("unexpected cancellation")
	default:
	}

	err = mgr.Cancel(token)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-notify:
	default:
		t.Fatal("cancellation not propogated")
	}

	select {
	case <-notify:
		t.Fatal("unexpected cancellation")
	default:
	}

	err = mgr.Cancel(CancellationToken{PID: -1, Key: -1})
	if err == nil {
		t.Fatal("invalid cancellation completed")
	}

	select {
	case <-notify:
		t.Fatal("unexpected cancellation")
	default:
	}

	cancel()

	select {
	case _, ok := <-notify:
		if ok {
			t.Fatal("unexpected cancellation")
		}
	default:
		t.Fatal("expected cancellation channel to be closed")
	}

	err = mgr.Cancel(token)
	if err == nil {
		t.Fatal("invalid cancellation completed")
	}
}
