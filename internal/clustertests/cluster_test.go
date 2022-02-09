// Copyright 2021 Molecula Corp. All rights reserved.
package clustertest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/authn"
	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/encoding/proto"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"
)

// container turns a docker-compose service name into a container ID
// by calling "docker-compose ps"
func container(t *testing.T, svc string) string {
	project := "clustertests"
	if p := os.Getenv("PROJECT"); p != "" {
		project = p
	}
	stdout, stderr, err := runCmd("docker-compose", "-p", project, "ps", "-q", svc)
	if err != nil {
		t.Fatalf("couldn't construct container name, err: %v, stderr:\n%s\nstdout:\n%s", err, stderr, stdout)
	}
	name := strings.Trim(stdout, "\n")
	return name
}

func GetAuthToken(t *testing.T) string {
	t.Helper()

	var (
		ClientID         = "e9088663-eb08-41d7-8f65-efb5f54bbb71"
		ClientSecret     = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		AuthorizeURL     = "fakeidp:10101/authorize"
		TokenURL         = "fakeidp:10101/token"
		GroupEndpointURL = "fakeidp:10101/groups"
		LogoutURL        = "fakeidp:10101/logout"
		Scopes           = []string{"https://graph.microsoft.com/.default", "offline_access"}
		Key              = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
	)

	a, err := authn.NewAuth(
		logger.NewStandardLogger(os.Stdout),
		"http://localhost:10101/",
		Scopes,
		AuthorizeURL,
		TokenURL,
		GroupEndpointURL,
		LogoutURL,
		ClientID,
		ClientSecret,
		Key,
	)
	if err != nil {
		t.Fatalf("NewAuth: %v", err)
	}

	// make a valid token
	tkn := jwt.New(jwt.SigningMethodHS256)
	claims := tkn.Claims.(jwt.MapClaims)
	claims["oid"] = "42"
	claims["name"] = "valid"
	token, err := tkn.SignedString([]byte(a.SecretKey()))
	if err != nil {
		t.Fatal(err)
	}

	return token
}
func TestClusterStuff(t *testing.T) {
	if os.Getenv("ENABLE_PILOSA_CLUSTER_TESTS") != "1" {
		t.Skip("pilosa cluster tests are not enabled")
	}

	auth := false
	if os.Getenv("ENABLE_AUTH") == "1" {
		auth = true
	}

	cli1, err := pilosa.NewInternalClient("pilosa1:10101", pilosa.GetHTTPClient(nil), pilosa.WithSerializer(proto.Serializer{}))
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	cli2, err := pilosa.NewInternalClient("pilosa2:10101", pilosa.GetHTTPClient(nil), pilosa.WithSerializer(proto.Serializer{}))
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	cli3, err := pilosa.NewInternalClient("pilosa3:10101", pilosa.GetHTTPClient(nil), pilosa.WithSerializer(proto.Serializer{}))
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	ctx := context.Background()
	token := ""
	// generate auth token and add to context
	if auth {
		token = GetAuthToken(t)
		ctx = context.WithValue(ctx, "token", "Bearer "+token)
	}

	if err := cli1.CreateIndex(ctx, "testidx", pilosa.IndexOptions{}); err != nil {
		t.Fatalf("creating index: %v", err)
	}
	if err := cli1.CreateFieldWithOptions(ctx, "testidx", "testf", pilosa.FieldOptions{CacheType: pilosa.CacheTypeRanked, CacheSize: 100}); err != nil {
		t.Fatalf("creating field: %v", err)
	}

	req := &pilosa.ImportRequest{
		Index: "testidx",
		Field: "testf",
	}
	req.ColumnIDs = make([]uint64, 10)
	req.RowIDs = make([]uint64, 10)

	for i := 0; i < 1000; i++ {
		req.RowIDs[i%10] = 0
		req.ColumnIDs[i%10] = uint64((i/10)*pilosa.ShardWidth + i%10)
		req.Shard = uint64(i / 10)
		if i%10 == 9 {
			err = cli1.Import(ctx, nil, req, &pilosa.ImportOptions{})
			if err != nil {
				t.Fatalf("importing: %v", err)
			}
		}
	}

	// Check query results from each node.
	for i, cli := range []*pilosa.InternalClient{cli1, cli2, cli3} {
		r, err := cli.Query(ctx, "testidx", &pilosa.QueryRequest{Index: "testidx", Query: "Count(Row(testf=0))"})
		if err != nil {
			t.Fatalf("count querying pilosa%d: %v", i, err)
		}
		if r.Results[0].(uint64) != 1000 {
			t.Fatalf("count on pilosa%d after import is %d", i, r.Results[0].(uint64))
		}
	}
	t.Run("long pause", func(t *testing.T) {
		if err := sendCmd("docker", "pause", container(t, "pilosa3")); err != nil {
			t.Fatalf("sending pause: %v", err)
		}
		t.Log("pausing pilosa3 for 10s")
		time.Sleep(time.Second * 10)
		if err := sendCmd("docker", "unpause", container(t, "pilosa3")); err != nil {
			t.Fatalf("sending unpause: %v", err)
		}
		t.Log("done with pause, waiting for stability")
		waitForStatus(t, cli1.Status, string(disco.ClusterStateNormal), 30, time.Second, ctx)
		t.Log("done waiting for stability")

		// Check query results from each node.
		for i, cli := range []*pilosa.InternalClient{cli1, cli2, cli3} {
			r, err := cli.Query(ctx, "testidx", &pilosa.QueryRequest{Index: "testidx", Query: "Count(Row(testf=0))"})
			if err != nil {
				t.Fatalf("count querying pilosa%d: %v", i, err)
			}
			if r.Results[0].(uint64) != 1000 {
				t.Fatalf("count on pilosa%d after import is %d", i, r.Results[0].(uint64))
			}
		}
	})

	t.Run("backup", func(t *testing.T) {
		// do backup with node 1 down, but restart it after a few seconds
		if err := sendCmd("docker", "stop", container(t, "pilosa1")); err != nil {
			t.Fatalf("sending stop command: %v", err)
		}
		var backupCmd *exec.Cmd
		tmpdir := t.TempDir()

		if auth {
			if backupCmd, err = startCmd(
				"featurebase", "backup", "--host=pilosa1:10101", fmt.Sprintf("--output=%s", tmpdir+"/backuptest"), "--auth-token", token); err != nil {
				t.Fatalf("sending backup command: %v", err)
			}
		} else {
			if backupCmd, err = startCmd(
				"featurebase", "backup", "--host=pilosa1:10101", fmt.Sprintf("--output=%s", tmpdir+"/backuptest")); err != nil {
				t.Fatalf("sending backup command: %v", err)
			}
		}
		time.Sleep(time.Second * 5)
		if err = sendCmd("docker", "start", container(t, "pilosa1")); err != nil {
			t.Fatalf("sending start command: %v", err)
		}

		if err = backupCmd.Wait(); err != nil {
			t.Fatalf("waiting on backup to finish: %v", err)
		}

		client := http.Client{}
		req, err := http.NewRequest(http.MethodDelete, "http://pilosa1:10101/index/testidx", nil)
		if auth {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if err != nil {
			t.Fatalf("getting req: %v", err)
		} else if resp, err := client.Do(req); err != nil {
			t.Fatalf("doing request: %v", err)
		} else if resp.StatusCode >= 400 {
			bod, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				t.Logf("reading error body: %v", readErr)
			}
			t.Fatalf("deleting index: code=%d, body=%s", resp.StatusCode, bod)
		}

		var restoreCmd *exec.Cmd
		if auth {
			if restoreCmd, err = startCmd("featurebase", "restore", "-s", tmpdir+"/backuptest", "--host", "pilosa1:10101", "--auth-token", token); err != nil {
				t.Fatalf("starting restore: %v", err)
			}
		} else {
			if restoreCmd, err = startCmd("featurebase", "restore", "-s", tmpdir+"/backuptest", "--host", "pilosa1:10101"); err != nil {
				t.Fatalf("starting restore: %v", err)
			}
		}
		time.Sleep(time.Millisecond * 50)
		if err = sendCmd("docker", "stop", container(t, "pilosa2")); err != nil {
			t.Fatalf("sending stop command: %v", err)
		}

		time.Sleep(time.Second * 10)
		if err = sendCmd("docker", "start", container(t, "pilosa2")); err != nil {
			t.Fatalf("sending stop command: %v", err)
		}
		if err := restoreCmd.Wait(); err != nil {
			t.Fatalf("restore failed: %v", err)
		}

		if err = sendCmd("docker", "pause", container(t, "pilosa1")); err != nil {
			t.Fatalf("sending pause command: %v", err)
		}
		if err = sendCmd("docker", "pause", container(t, "pilosa2")); err != nil {
			t.Fatalf("sending pause command: %v", err)
		}
		if err = sendCmd("docker", "pause", container(t, "pilosa3")); err != nil {
			t.Fatalf("sending pause command: %v", err)
		}
		// now do backup with all nodes down and too short a timeout
		// so it fails. Has be to be all 3 because the cluster has
		// replicas=3 and the backup command will retry on replicas.
		if auth {
			if backupCmd, err = startCmd(
				"featurebase", "backup", "--host=pilosa1:10101", fmt.Sprintf("--output=%s", tmpdir+"/backuptest2"), "--retry-period=200ms", "--auth-token", token); err != nil {
				t.Fatalf("sending second backup command: %v", err)
			}
		} else {
			if backupCmd, err = startCmd(
				"featurebase", "backup", "--host=pilosa1:10101", fmt.Sprintf("--output=%s", tmpdir+"/backuptest2"), "--retry-period=200ms"); err != nil {
				t.Fatalf("sending second backup command: %v", err)
			}
		}

		t.Logf("sleeping 8s")
		time.Sleep(time.Second * 8)
		t.Logf("restarting FB nodes")

		if err = sendCmd("docker", "unpause", container(t, "pilosa1")); err != nil {
			t.Fatalf("sending unpause command: %v", err)
		}
		if err = sendCmd("docker", "unpause", container(t, "pilosa2")); err != nil {
			t.Fatalf("sending unpause command: %v", err)
		}
		if err = sendCmd("docker", "unpause", container(t, "pilosa3")); err != nil {
			t.Fatalf("sending unpause command: %v", err)
		}
		if err = backupCmd.Wait(); err == nil {
			t.Fatal("backup command should have errored but didn't")
		}

	})
}

func waitForStatus(t *testing.T, stator func(context.Context) (string, error), status string, n int, sleep time.Duration, ctx context.Context) {
	t.Helper()

	for i := 0; i < n; i++ {
		s, err := stator(ctx)
		if err != nil {
			t.Logf("Status (try %d/%d): %v (retrying in %s)", i, n, err, sleep.String())
		} else {
			t.Logf("Status (try %d/%d): curr: %s, expect: %s (retrying in %s)", i, n, s, status, sleep.String())
		}
		if s == status {
			return
		}
		time.Sleep(sleep)
	}

	s, err := stator(ctx)
	if err != nil {
		t.Fatalf("querying status: %v", err)
	}
	if status != s {
		waited := time.Duration(n) * sleep
		t.Fatalf("waited %s for status: %s, got: %s", waited.String(), status, s)
	}
}

// runCmd is a helper which uses os.Exec to run a command and returns
// stdout and stderr as separate strings, and any error returned from
// Command.Run
func runCmd(name string, args ...string) (sout, serr string, err error) {
	cmd := exec.Command(name, args...)
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err = cmd.Run()
	return stdout.String(), stderr.String(), errors.Wrap(err, "running command")
}
