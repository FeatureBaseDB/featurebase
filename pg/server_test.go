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

package pg_test

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/pilosa/pilosa/v2/logger"
	"github.com/pilosa/pilosa/v2/pg"
	"github.com/pilosa/pilosa/v2/pg/pgtest"
)

// TestStartupTimeout tests that an incoming connection that does nothing times out and gets closed.
func TestStartupTimeout(t *testing.T) {
	t.Parallel()

	connect, shutdown, err := pgtest.ServeMem(&pg.Server{
		StartupTimeout: time.Millisecond,
		Logger:         logger.NewLogfLogger(t),
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
			Logger:         logger.NewLogfLogger(t),
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
		Logger:         logger.NewLogfLogger(t),
	}
	addr, shutdown, err := pgtest.ServeTCP(":0", server)
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
		Logger:         logger.NewLogfLogger(t),
	}
	addr, shutdown, err := pgtest.ServeTLS(":0", server)
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
		Logger:         logger.NewLogfLogger(t),
	}
	addr, shutdown, err := pgtest.ServeTCP(":0", server)
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
}
