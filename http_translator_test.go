// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/test"
)

func TestTranslateStore_EntryReader(t *testing.T) {
	// Ensure client can connect and stream the translate store data.
	t.Run("OK", func(t *testing.T) {
		t.Run("ServerDisconnect", func(t *testing.T) {
			// This test is currently flawed
			t.Skip("failing with error: invalid memory address or nil pointer dereference when reading entry")

			cluster := test.MustRunCluster(t, 1)
			defer cluster.Close()
			primary := cluster.GetNode(0)

			hldr := test.Holder{Holder: primary.Server.Holder()}
			index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{Keys: true})
			_, err := index.CreateField("f", "")
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
			r := pilosa.NewTranslateEntryReader(context.Background(), nil)
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
	})
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
		r, err := pilosa.GetOpenTranslateReaderFunc(nil)(ctx, url, offset)
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
		r, err := pilosa.GetOpenTranslateReaderWithLockerFunc(nil, &sync.Mutex{})(ctx, url, offset)
		if err != nil {
			b.Fatalf("opening translate reader: %+v", err)
		}
		benchmarkReadEntry(b, r, key, nkeys)
		r.Close()
	}
}
