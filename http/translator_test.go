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

package http_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/test"
)

func TestTranslateStore_EntryReader(t *testing.T) {
	// Ensure client can connect and stream the translate store data.
	t.Run("OK", func(t *testing.T) {
		t.Run("ServerDisconnect", func(t *testing.T) {
			// This test is currently flawed, breaking intermittently with message:
			// "translator_test.go:65: unexpected EOF"
			t.Skip()

			cluster := test.MustRunCluster(t, 1)
			defer cluster.Close()
			primary := cluster.GetNode(0)

			hldr := test.Holder{Holder: primary.Server.Holder()}
			index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{Keys: true})
			_, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
			if err != nil {
				t.Fatal(err)
			}

			// Set data on the primary node.
			if _, err := primary.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `` +
				fmt.Sprintf("Set(%s, f=%d)\n", `"foo"`, 10) +
				fmt.Sprintf("Set(%s, f=%d)\n", `"bar"`, 10) +
				fmt.Sprintf("Set(%s, f=%d)\n", `"baz"`, 10),
			}); err != nil {
				t.Fatal(err)
			}

			// Connect to server and stream all available data.
			r := http.NewTranslateEntryReader(context.Background(), nil)
			r.URL = primary.URL()

			// Wait to ensure writes make it to translate store
			time.Sleep(500 * time.Millisecond)

			// Close the primary to disconnect reader.
			primary.Close()

			var entry pilosa.TranslateEntry
			if err := r.ReadEntry(&entry); err != nil {
				t.Fatal(err)
			} else if got, want := entry.ID, uint64(1); got != want {
				t.Fatalf("entry.ID=%v, want %v", got, want)
			} else if got, want := entry.Key, "-"; got != want {
				t.Fatalf("entry.Key=%v, want %v", got, want)
			}

			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
		})

		/*
			// Ensure server closes store reader if client disconnects.
			t.Run("ClientDisconnect", func(t *testing.T) {
				t.Skip() // can't mock server from http package
				// Setup mock so that Read() hangs.
				done := make(chan struct{})

				var mrc mock.ReadCloser
				mrc.ReadFunc = func(p []byte) (int, error) {
					<-done
					return 0, io.EOF
				}

				closeInvoked := make(chan struct{})

				mrc.CloseFunc = func() error {
					close(closeInvoked)
					return nil
				}

				var translateStore mock.TranslateStore

				translateStore.ReaderFunc = func(ctx context.Context, off int64) (io.ReadCloser, error) {
					return &mrc, nil
				}

				opts := server.OptCommandServerOptions(pilosa.OptServerPrimaryTranslateStore(translateStore))
				cluster := test.MustRunCluster(t, 1, []server.CommandOption{opts})
				defer cluster.Close()
				primary := cluster[0]

				defer close(done)

				// Connect to server and begin streaming.
				ctx, cancel := context.WithCancel(context.Background())
				store := http.NewTranslateStore(primary.URL())
				if _, err := store.Reader(ctx, 0); err != nil {
					t.Fatal(err)
				}

				// Cancel the context and check if server is closed.
				cancel()
				select {
				case <-time.NewTimer(time.Millisecond * 100).C:
					t.Fatal("expected server close")
				case <-closeInvoked:
					return
				}
			})
		*/
	})

	/*
		// Ensure client is notified if the server doesn't support streaming replication.
		t.Run("ErrNotImplemented", func(t *testing.T) {
			t.Skip() // can't mock server from http package
			var translateStore mock.TranslateStore
			translateStore.ReaderFunc = func(ctx context.Context, off int64) (io.ReadCloser, error) {
				return nil, pilosa.ErrNotImplemented
			}

			opts := server.OptCommandServerOptions(pilosa.OptServerPrimaryTranslateStore(translateStore))
			cluster := test.MustRunCluster(t, 1, []server.CommandOption{opts})
			defer cluster.Close()
			primary := cluster[0]

			ts := http.NewTranslateStore(primary.URL())
			_, err := ts.Reader(context.Background(), 0)
			if err != pilosa.ErrNotImplemented {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	*/
}

func benchmarkSetup(b *testing.B, ctx context.Context, key string, nkeys int) (string, pilosa.TranslateOffsetMap, func()) {
	b.Helper()

	cluster := test.MustRunCluster(b, 1)
	primary := cluster.GetNode(0)

	idx := primary.MustCreateIndex(b, "i", pilosa.IndexOptions{})
	fld := primary.MustCreateField(b, idx.Name(), "f", pilosa.OptFieldKeys())
	offset := make(pilosa.TranslateOffsetMap)
	offset.SetIndexPartitionOffset(idx.Name(), 0, 1)
	offset.SetFieldOffset(idx.Name(), fld.Name(), 1)

	// Set data on the primary node.
	for k := 0; k < nkeys; k++ {
		if _, err := primary.API.Query(ctx, &pilosa.QueryRequest{
			Index: idx.Name(),
			Query: fmt.Sprintf(`Set(%d, %s="%s%[1]d")`, k, fld.Name(), key),
		}); err != nil {
			b.Fatalf("quering api: %+v", err)
		}
	}

	return primary.URL(), offset, func() {
		b.Helper()

		if err := primary.API.DeleteIndex(ctx, idx.Name()); err != nil {
			panic(err)
		}
		if err := cluster.Close(); err != nil {
			panic(err)
		}
	}
}

func benchmarkReadEntry(b *testing.B, r pilosa.TranslateEntryReader, key string, nkeys int) {
	var entry pilosa.TranslateEntry
	for k := 0; k < nkeys; k++ {
		if err := r.ReadEntry(&entry); err != nil {
			b.Fatalf("reading entry: %+v", err)
		}
		if entry.Key != fmt.Sprintf("%s%d", key, k) {
			b.Fatalf("got: %s, expected: %s%d", entry.Key, key, k)
		}
	}
}

const (
	key   = "foo"
	nkeys = 1000
)

func BenchmarkReadEntryNoMutex(b *testing.B) {
	ctx := context.Background()
	url, offset, teardown := benchmarkSetup(b, ctx, key, nkeys)
	defer teardown()

	for n := 0; n < b.N; n++ {
		r, err := http.GetOpenTranslateReaderFunc(nil)(ctx, url, offset)
		if err != nil {
			b.Fatalf("opening translate reader: %+v", err)
		}
		benchmarkReadEntry(b, r, key, nkeys)
		r.Close()
	}
}

func BenchmarkReadEntryWithMutex(b *testing.B) {
	ctx := context.Background()
	url, offset, teardown := benchmarkSetup(b, ctx, key, nkeys)
	defer teardown()

	for n := 0; n < b.N; n++ {
		r, err := http.GetOpenTranslateReaderWithLockerFunc(nil, &sync.Mutex{})(ctx, url, offset)
		if err != nil {
			b.Fatalf("opening translate reader: %+v", err)
		}
		benchmarkReadEntry(b, r, key, nkeys)
		r.Close()
	}
}
