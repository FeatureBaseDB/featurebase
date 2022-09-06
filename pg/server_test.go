// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pg_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/pg"
	"github.com/featurebasedb/featurebase/v3/pg/pgtest"
)

// TestStartupTimeout tests that an incoming connection that does nothing times out and gets closed.
func TestStartupTimeout(t *testing.T) {
	t.Parallel()

	connect, shutdown, err := pgtest.ServeMem(&pg.Server{
		StartupTimeout: time.Millisecond,
		Logger:         logger.NopLogger,
	})
	if err != nil {
		t.Fatalf("starting in-memory postgres server: %v", err)
	}
	defer shutdown.Finish(t, "in-memory postgres server")

	conn, err := connect()
	if err != nil {
		t.Fatalf("failed to acquire connection: %v", err)
	}
	defer conn.Close()

	// The server isn't sending anything, so this should block until the connection dies.
	conn.Read(make([]byte, 1024)) //nolint:errcheck
}

// TestStartupInvalidLength tests that sending an HTTP GET request does not cause the server to allocate 1.2 GiB of memory.
func TestStartupInvalidLength(t *testing.T) {
	t.Parallel()

	res := testing.Benchmark(func(b *testing.B) {
		connect, shutdown, err := pgtest.ServeMem(&pg.Server{
			MaxStartupSize: 1024,
			Logger:         logger.NopLogger,
		})
		if err != nil {
			t.Fatalf("starting in-memory postgres server: %v", err)
		}
		defer shutdown.Finish(t, "in-memory postgres server")

		b.ReportAllocs()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			conn, err := connect()
			if err != nil {
				t.Fatalf("failed to acquire connection: %v", err)
			}

			_, err = conn.Write([]byte("GET "))
			if err != nil {
				t.Fatalf("failed to write invalid length: %v", err)
			}

			// The server isn't sending anything, so this should block until the connection dies.
			conn.Read(make([]byte, 1024)) //nolint:errcheck

			err = conn.Close()
			if err != nil {
				t.Fatalf("failed to close connection: %v", err)
			}
		}
	})
	bpo := res.AllocedBytesPerOp()
	t.Logf("allocated %d bytes per op", bpo)
	if bpo > 1024*1024 {
		t.Errorf("allocated too much memory: %d bytes/connection", bpo)
	}
}

// TestPQConnect tests connecting the Go SQL driver `pq` to this postgres server.
func TestPQConnect(t *testing.T) {
	t.Parallel()

	server := &pg.Server{
		StartupTimeout: time.Second,
		Logger:         logger.NopLogger,
	}

	addr, shutdown, err := pgtest.ServeTCP("localhost:0", server)
	if err != nil {
		t.Fatalf("starting postgres server: %v", err)
	}
	defer shutdown.Finish(t, "postgres server")

	tcpAddr := addr.(*net.TCPAddr)

	connector, err := pq.NewConnector(fmt.Sprintf("user=molecula dbname=pilosa sslmode=disable host=%s port=%d", tcpAddr.IP, tcpAddr.Port))
	if err != nil {
		t.Fatalf("failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pgtest.ShutdownFunc(conn.Close).Finish(t, "postgres TLS conn")
}

// TestPQConnectSSL tests connecting the Go SQL driver `pq` to this postgres server, with SSL enabled.
func TestPQConnectSSL(t *testing.T) {
	t.Parallel()

	server := &pg.Server{
		StartupTimeout: time.Second,
		Logger:         logger.NopLogger,
	}

	addr, shutdown, err := pgtest.ServeTLS("localhost:0", server)
	if err != nil {
		t.Fatalf("starting postgres server: %v", err)
	}
	defer shutdown.Finish(t, "postgres TLS server")

	tcpAddr := addr.(*net.TCPAddr)

	connector, err := pq.NewConnector(fmt.Sprintf("user=molecula dbname=pilosa sslmode=require host=%s port=%d", tcpAddr.IP, tcpAddr.Port))
	if err != nil {
		t.Fatalf("failed to create connector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pgtest.ShutdownFunc(conn.Close).Finish(t, "postgres TLS conn")
}

// TestPSQLQuery tests sending a query from the `psql` command line tool.
func TestPSQLQuery(t *testing.T) {
	// Check if psql is present.
	// Skip this test if it is not.
	_, err := exec.LookPath("psql")
	if err != nil {
		if err, ok := err.(*exec.Error); ok {
			if err.Err == exec.ErrNotFound {
				t.Skip("psql is not available")
			}
		}
		t.Fatalf("searching for psql: %v", err)
	}

	t.Run("Query", func(t *testing.T) {
		server := &pg.Server{
			QueryHandler: pgtest.HandlerFunc(func(ctx context.Context, w pg.QueryResultWriter, q pg.Query) error {
				err := w.WriteHeader(pg.ColumnInfo{
					Name: "field",
					Type: pg.TypeCharoid,
				})
				if err != nil {
					return err
				}

				err = w.WriteRowText("h")
				if err != nil {
					return err
				}

				err = w.WriteRowText("xyzzy")
				if err != nil {
					return err
				}

				return nil
			}),
			TypeEngine:     pg.PrimitiveTypeEngine{},
			StartupTimeout: time.Second,
			Logger:         logger.NopLogger,
		}

		addr, shutdown, err := pgtest.ServeTCP("localhost:0", server)
		if err != nil {
			t.Fatalf("starting postgres server: %v", err)
		}
		defer shutdown.Finish(t, "postgres server")

		tcpAddr := addr.(*net.TCPAddr)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, "psql", "-h", tcpAddr.IP.String(), "-p", strconv.Itoa(tcpAddr.Port), "-c", "test query")
		data, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("psql failed: %v", string(data))
		}
	})

	t.Run("Cancel", func(t *testing.T) {
		var term func() error
		var qerr error
		var qdone bool
		defer func() {
			if qerr != nil {
				t.Fatal(qerr)
			}
			if !qdone {
				t.Fatal("query not done")
			}
		}()

		var mutex sync.Mutex
		mutex.Lock()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		server := &pg.Server{
			QueryHandler: pgtest.HandlerFunc(func(qctx context.Context, w pg.QueryResultWriter, q pg.Query) error {
				defer func() { qdone = true }()

				mutex.Lock()
				defer mutex.Unlock()

				qerr = term()
				if qerr != nil {
					return qerr
				}
				select {
				case <-qctx.Done():
				case <-ctx.Done():
					qerr = ctx.Err()
					return qerr
				}
				return nil
			}),
			TypeEngine:          pg.PrimitiveTypeEngine{},
			StartupTimeout:      time.Second,
			Logger:              logger.NopLogger,
			CancellationManager: pg.NewLocalCancellationManager(rand.Reader),
		}

		addr, shutdown, err := pgtest.ServeTCP("localhost:0", server)
		if err != nil {
			t.Fatalf("starting postgres server: %v", err)
		}
		defer shutdown.Finish(t, "postgres server")

		tcpAddr := addr.(*net.TCPAddr)

		cmd := exec.CommandContext(ctx, "psql", "-h", tcpAddr.IP.String(), "-p", strconv.Itoa(tcpAddr.Port), "-c", "test query")
		term = func() error { return cmd.Process.Signal(syscall.SIGINT) }
		var buf bytes.Buffer
		cmd.Stderr = &buf
		cmd.Stdout = &buf
		err = cmd.Start()
		if err != nil {
			t.Fatalf("starting postgres client: %v", err)
		}
		mutex.Unlock()
		err = cmd.Wait()
		if err != nil {
			t.Fatalf("psql failed (%v): %s", err, buf.String())
		}
	})
}
