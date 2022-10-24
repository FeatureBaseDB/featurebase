// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/authn"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/featurebasedb/featurebase/v3/testhook"
	"github.com/golang-jwt/jwt"
)

func TestImportCommand_Validation(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	err := cm.Run(context.Background())
	if err != pilosa.ErrIndexRequired {
		t.Fatalf("Command not working, expect: %s, actual: '%s'", pilosa.ErrIndexRequired, err)
	}

	cm.Index = "i"
	err = cm.Run(context.Background())
	if err != pilosa.ErrFieldRequired {
		t.Fatalf("Command not working, expect: %s, actual: '%s'", pilosa.ErrFieldRequired, err)
	}

	cm.Field = "f"
	err = cm.Run(context.Background())
	if err.Error() != "path required" {
		t.Fatalf("Command not working, expect: %s, actual: '%s'", "path required", err)
	}
}

func TestImportCommand_Basic(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		buf := bytes.Buffer{}
		stdin, stdout, stderr := GetIO(buf)
		cm := NewImportCommand(stdin, stdout, stderr)
		file, err := testhook.TempFile(t, "import.csv")
		if err != nil {
			t.Fatalf("creating tempfile: %v", err)
		}
		_, err = file.Write([]byte("1,2\n3,4\n5,6"))
		if err != nil {
			t.Fatalf("writing to tempfile: %v", err)
		}
		ctx := context.Background()
		if err != nil {
			t.Fatal(err)
		}

		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster.GetNode(0)
		cm.Host = cmd.API.Node().URI.HostPort()

		cm.Index = "i"
		cm.Field = "f"
		cm.CreateSchema = true
		cm.Paths = []string{file.Name()}
		err = cm.Run(ctx)
		if err != nil {
			t.Fatalf("Import Run doesn't work: %s", err)
		}
	})

	t.Run("clear", func(t *testing.T) {
		buf := bytes.Buffer{}
		stdin, stdout, stderr := GetIO(buf)
		cm := NewImportCommand(stdin, stdout, stderr)
		file, err := testhook.TempFile(t, "import.csv")
		if err != nil {
			t.Fatalf("creating tempfile: %v", err)
		}
		_, err = file.Write([]byte("1,2\n3,4\n5,6"))
		if err != nil {
			t.Fatalf("writing to tempfile: %v", err)
		}
		ctx := context.Background()

		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster.GetNode(0)
		cm.Host = cmd.API.Node().URI.HostPort()

		cm.Index = "i"
		cm.Field = "f"
		cm.CreateSchema = true
		cm.Clear = true
		cm.Paths = []string{file.Name()}
		err = cm.Run(ctx)
		if err != nil {
			t.Fatalf("Import Run clear doesn't work: %s", err)
		}
	})
}

// Ensure that the ImportValue path runs.
func TestImportCommand_RunValue(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		buf := bytes.Buffer{}
		stdin, stdout, stderr := GetIO(buf)
		cm := NewImportCommand(stdin, stdout, stderr)
		file, err := testhook.TempFile(t, "import-value.csv")
		if err != nil {
			t.Fatalf("creating tempfile: %v", err)
		}
		_, err = file.Write([]byte("1,2\n3,4\n5,6"))
		if err != nil {
			t.Fatalf("writing to tempfile: %v", err)
		}
		ctx := context.Background()

		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster.GetNode(0)
		cm.Host = cmd.API.Node().URI.HostPort()

		resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader("")))
		if err != nil {
			t.Fatalf("http request: %v", err)
		}
		resp.Body.Close()
		resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i/field/f", strings.NewReader(`{"options":{"type": "int", "min": 0, "max": 100}}`)))
		if err != nil {
			t.Fatalf("http request: %v", err)
		}
		resp.Body.Close()

		cm.Index = "i"
		cm.Field = "f"
		cm.Paths = []string{file.Name()}
		err = cm.Run(ctx)
		if err != nil {
			t.Fatalf("Import Run with values doesn't work: %s", err)
		}
	})

	t.Run("clear", func(t *testing.T) {
		buf := bytes.Buffer{}
		stdin, stdout, stderr := GetIO(buf)
		cm := NewImportCommand(stdin, stdout, stderr)
		file, err := testhook.TempFile(t, "import-value.csv")
		if err != nil {
			t.Fatalf("creating tempfile: %v", err)
		}
		_, err = file.Write([]byte("1,2\n3,4\n5,6"))
		if err != nil {
			t.Fatalf("writing to tempfile: %v", err)
		}
		ctx := context.Background()
		if err != nil {
			t.Fatal(err)
		}

		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster.GetNode(0)
		cm.Host = cmd.API.Node().URI.HostPort()

		resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader("")))
		if err != nil {
			t.Fatalf("posting request: %v", err)
		}
		resp.Body.Close()
		resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i/field/f", strings.NewReader(`{"options":{"type": "int", "min": 0, "max": 100}}`)))
		if err != nil {
			t.Fatalf("posting request: %v", err)
		}
		resp.Body.Close()

		cm.Index = "i"
		cm.Field = "f"
		cm.Paths = []string{file.Name()}
		cm.Clear = true
		err = cm.Run(ctx)
		if err != nil {
			t.Fatalf("Import Run with values doesn't work: %s", err)
		}
	})
}

// Ensure that import with keys runs.
func TestImportCommand_RunKeys(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	file, err := testhook.TempFile(t, "import-key.csv")
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.Write([]byte("foo1,bar2\nfoo3,bar4\nfoo5,bar6"))
	if err != nil {
		t.Fatalf("writing to tempfile: %v", err)
	}
	ctx := context.Background()

	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	cm.Host = cmd.API.Node().URI.HostPort()

	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i", cm.Host, cluster), strings.NewReader(`{"options":{"keys": true}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()
	resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i/field/f", cm.Host, cluster), strings.NewReader(`{"options":{"keys": true}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()

	cm.Index = cluster.Idx("i")
	cm.Field = "f"
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with keys doesn't work: %s", err)
	}
}

// Ensure that import with keys runs with key replication.
func TestImportCommand_KeyReplication(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	file, err := testhook.TempFile(t, "import-key.csv")
	if err != nil {
		t.Fatal(err)
	}
	keyBytes := []byte{}
	for row := 0; row < 50; row++ {
		for col := 0; col < 50; col++ {
			x := fmt.Sprintf("foo%d,bar%d\n", row, col)
			keyBytes = append(keyBytes, x...)
		}
	}
	x := "fooEND,barEND"
	keyBytes = append(keyBytes, x...)

	_, err = file.Write(keyBytes)
	if err != nil {
		t.Fatalf("writing to tempfile: %v", err)
	}
	ctx := context.Background()

	c := test.MustRunCluster(t, 3)
	defer c.Close()
	cmd0 := c.GetNode(0)
	cmd1 := c.GetNode(1)

	host0 := cmd0.API.Node().URI.HostPort()
	host1 := cmd1.API.Node().URI.HostPort()

	cm.Host = host0

	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i", cm.Host, c), strings.NewReader(`{"options":{"keys": true}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()
	resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i/field/f", cm.Host, c), strings.NewReader(`{"options":{"keys": true}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()

	cm.Index = c.Idx("i")
	cm.Field = "f"
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with key replication doesn't work: %s", err)
	}

	// Verify that the data is available on both nodes.
	for _, host := range []string{host0, host1} {
		if err := test.RetryUntil(2*time.Second, func() error {
			qry := "Count(Row(f=foo0))"
			resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i/query", host, c), strings.NewReader(qry)))
			if err != nil {
				return fmt.Errorf("Querying data for validation: %s", err)
			}

			// Read body and unmarshal response.
			exp := `{"results":[50]}` + "\n"
			if body, err := io.ReadAll(resp.Body); err != nil {
				return fmt.Errorf("reading: %s", err)
			} else if !reflect.DeepEqual(body, []byte(exp)) {
				return fmt.Errorf("expected: %s, but got: %s", exp, body)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

}

// Ensure that integer import with keys runs.
func TestImportCommand_RunValueKeys(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	file, err := testhook.TempFile(t, "import-key.csv")
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.Write([]byte("foo1,2\nfoo3,4\nfoo5,6"))
	if err != nil {
		t.Fatalf("writing to tempfile: %v", err)
	}
	ctx := context.Background()

	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	cm.Host = cmd.API.Node().URI.HostPort()

	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i", cm.Host, cluster), strings.NewReader(`{"options":{"keys": true}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()
	resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i/field/f", cm.Host, cluster), strings.NewReader(`{"options":{"type": "int", "min": 0, "max": 100}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()

	cm.Index = cluster.Idx("i")
	cm.Field = "f"
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with keys doesn't work: %s", err)
	}
}

func TestImportCommand_InvalidFile(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	cm.Host = cmd.API.Node().URI.HostPort()
	cm.Index = "i"
	cm.Field = "f"
	file, err := testhook.TempFile(t, "import.csv")
	if err != nil {
		t.Fatalf("creating tempfile: %v", err)
	}
	_, err = file.Write([]byte("a,2\n3,5\n5,6"))
	if err != nil {
		t.Fatalf("writing to tempfile: %v", err)
	}
	cm.Paths = []string{file.Name()}
	err = cm.Run(context.Background())
	if !strings.Contains(err.Error(), "invalid row id on row") {
		t.Fatalf("expect error: invalid row id on row, actual: %s", err)
	}

	file, err = testhook.TempFile(t, "import1.csv")
	if err != nil {
		t.Fatalf("creating tempfile: %v", err)
	}
	_, err = file.Write([]byte("1,\n3,\n5,6"))
	if err != nil {
		t.Fatalf("writing to tempfile: %v", err)
	}
	cm.Paths = []string{file.Name()}
	err = cm.Run(context.Background())
	if !strings.Contains(err.Error(), "invalid column id on row") {
		t.Fatalf("expect error: invalid column id on row, actual: %s", err)
	}

	file, err = testhook.TempFile(t, "import1.csv")
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.Write([]byte("1,2,34343\n1,3,54565,\n5,6,565"))
	if err != nil {
		t.Fatal(err)
	}
	cm.Paths = []string{file.Name()}
	err = cm.Run(context.Background())
	if !strings.Contains(err.Error(), "invalid timestamp on row") {
		t.Fatalf("expect error: invalid timestamp on row, actual: %s", err)
	}

	file, err = testhook.TempFile(t, "import1.csv")
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.Write([]byte("1\n3\n5"))
	if err != nil {
		t.Fatal(err)
	}
	cm.Paths = []string{file.Name()}
	err = cm.Run(context.Background())
	if !strings.Contains(err.Error(), "bad column count on row") {
		t.Fatalf("expect error: bad column count on row, actual: %s", err)
	}

}

// MustNewHTTPRequest creates a new HTTP request. Panic on error.
func MustNewHTTPRequest(method, urlStr string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, urlStr, body)
	req.Header.Add("Accept", "application/json")
	if err != nil {
		panic(err)
	}
	return req
}

// declare stdin, stdout, stderr
func GetIO(buf bytes.Buffer) (io.Reader, io.Writer, io.Writer) {
	rder := []byte{}
	stdin := bytes.NewReader(rder)
	stdout := bufio.NewWriter(&buf)
	stderr := bufio.NewWriter(&buf)
	return stdin, stdout, stderr
}

func TestImportCommand_BugOverwriteValue(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	file, err := testhook.TempFile(t, "import-value.csv")
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.Write([]byte("0,17\n"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	cm.Host = cmd.API.Node().URI.HostPort()

	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i", cm.Host, cluster), strings.NewReader("")))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()
	resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i/field/f", cm.Host, cluster), strings.NewReader(`{"options":{"type": "int", "min": 0, "max":2147483648 }}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()

	cm.Index = cluster.Idx("i")
	cm.Field = "f"
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with values doesn't work: %s", err)
	}

	file.Close()
	file, err = testhook.TempFile(t, "import-value2.csv")
	if err != nil {
		t.Fatalf("Error creating tempfile: %s", err)
	}
	_, err = file.Write([]byte("0,16\n"))
	if err != nil {
		t.Fatalf("writing bytes to tempfile: %v", err)
	}
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with values doesn't work: %s", err)
	}

	file.Close()
	file, err = testhook.TempFile(t, "import-value3.csv")
	if err != nil {
		t.Fatalf("Error creating tempfile: %s", err)
	}
	_, err = file.Write([]byte("0,19\n"))
	if err != nil {
		t.Fatalf("writing bytes to tempfile: %v", err)
	}
	cm.Paths = []string{file.Name()}
	err = cm.Run(ctx)
	if err != nil {
		t.Fatalf("Import Run with values doesn't work: %s", err)
	}
}

// Ensure that import into bool field runs.
func TestImportCommand_RunBool(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewImportCommand(stdin, stdout, stderr)
	ctx := context.Background()

	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	cm.Host = cmd.API.Node().URI.HostPort()

	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i", cm.Host, cluster), strings.NewReader("")))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()
	resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", fmt.Sprintf("http://%s/index/%i/field/f", cm.Host, cluster), strings.NewReader(`{"options":{"type": "bool"}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()

	cm.Index = cluster.Idx("i")
	cm.Field = "f"

	t.Run("Valid", func(t *testing.T) {
		file, err := testhook.TempFile(t, "import-bool.csv")
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write([]byte("0,1\n1,2\n1,3"))
		if err != nil {
			t.Fatalf("writing bytes to tempfile: %v", err)
		}

		cm.Paths = []string{file.Name()}
		err = cm.Run(ctx)
		if err != nil {
			t.Fatalf("Import Run to bool field doesn't work: %s", err)
		}
	})

	// Ensure that invalid bool values return an error.
	t.Run("Invalid", func(t *testing.T) {
		file, err := testhook.TempFile(t, "import-invalid-bool.csv")
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write([]byte("0,1\n1,2\n1,3\n2,4"))
		if err != nil {
			t.Fatalf("writing bytes to tempfile: %v", err)
		}
		cm.Paths = []string{file.Name()}
		err = cm.Run(ctx)
		if !strings.Contains(err.Error(), "bool field imports only support values 0 and 1") {
			t.Fatalf("expect error: bool field imports only support values 0 and 1, actual: %s", err)
		}
	})
}

func TestImport_AuthOn(t *testing.T) {
	clusterSize := 1

	logFilename := "./testdata/query.log"
	_, err := os.Create(logFilename)
	if err != nil {
		t.Fatalf("Failed to create query log file: %s", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := json.Marshal(
			authn.Groups{
				Groups: []authn.Group{
					{
						GroupID:   "group-id-test",
						GroupName: "group-id-test",
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("unexpected error marshalling groups response: %v", err)
		}
		fmt.Fprintf(w, "%s", body)
	}))

	auth := server.Auth{
		Enable:           true,
		ClientId:         "e9088663-eb08-41d7-8f65-efb5f54bbb71",
		ClientSecret:     "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
		AuthorizeURL:     "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize",
		TokenURL:         "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/token",
		GroupEndpointURL: srv.URL,
		LogoutURL:        "https://login.microsoftonline.com/common/oauth2/v2.0/logout",
		Scopes:           []string{"https://graph.microsoft.com/.default", "offline_access"},
		SecretKey:        "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
		RedirectBaseURL:  "https://localhost:0",
		QueryLogPath:     logFilename,
		PermissionsFile:  "./testdata/permissions.yaml",
	}

	commandOpts := make([][]server.CommandOption, clusterSize)
	configs := make([]*server.Config, clusterSize)
	for i := range configs {
		conf := server.NewConfig()
		configs[i] = conf
		conf.Bind = "https://localhost:0"
		conf.Auth = auth
		conf.TLS.CertificatePath = "./testdata/certs/localhost.crt"
		conf.TLS.CertificateKeyPath = "./testdata/certs/localhost.key"
		conf.TLS.CACertPath = "./testdata/certs/pilosa-ca.crt"
		conf.TLS.EnableClientVerification = false
		conf.TLS.SkipVerify = true
		commandOpts[i] = append(commandOpts[i], server.OptCommandConfig(conf))
	}
	a, err := authn.NewAuth(
		logger.NewStandardLogger(os.Stdout),
		"http://localhost:0/",
		auth.Scopes,
		auth.AuthorizeURL,
		auth.TokenURL,
		srv.URL,
		auth.LogoutURL,
		auth.ClientId,
		auth.ClientSecret,
		auth.SecretKey,
		[]string{},
	)
	if err != nil {
		t.Fatal(err)
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
	validToken := "Bearer " + token
	invalidToken := "Bearer " + string(tkn.Raw)

	tests := []struct {
		Index        string
		Field        string
		CreateSchema bool
		Token        string
		Err          error
	}{
		{
			Index:        "test",
			Field:        "field1",
			CreateSchema: true,
			Token:        validToken,
			Err:          nil,
		},
		{
			Index:        "test",
			Field:        "field1",
			CreateSchema: false,
			Token:        validToken,
			Err:          nil,
		},
		{
			Index:        "test",
			Field:        "field1",
			CreateSchema: false,
			Token:        invalidToken,
			Err:          fmt.Errorf("auth token is empty"),
		},
		{
			Index:        "test",
			Field:        "field1",
			CreateSchema: true,
			Token:        invalidToken,
			Err:          fmt.Errorf("auth token is empty"),
		},
	}

	t.Run("set", func(t *testing.T) {
		buf := bytes.Buffer{}
		stdin, stdout, stderr := GetIO(buf)
		cm := NewImportCommand(stdin, stdout, stderr)
		file, err := testhook.TempFile(t, "import.csv")
		if err != nil {
			t.Fatalf("creating tempfile: %v", err)
		}
		_, err = file.Write([]byte("1,2\n3,4\n5,6"))
		if err != nil {
			t.Fatalf("writing to tempfile: %v", err)
		}

		if err != nil {
			t.Fatal(err)
		}

		// note: cluster isn't shared because of custom opts
		cluster := test.MustRunCluster(t, clusterSize, commandOpts...)
		defer cluster.Close()
		cmd := cluster.GetNode(0)
		cm.Host = cmd.API.Node().URI.HostPort()

		for i, test := range tests {
			cm.Index = test.Index
			cm.Field = test.Field
			cm.CreateSchema = test.CreateSchema
			cm.Paths = []string{file.Name()}
			ctx := authn.WithAccessToken(context.Background(), test.Token)
			err = cm.Run(ctx)
			if test.Err != nil {
				if !strings.Contains(err.Error(), test.Err.Error()) {
					t.Fatalf("Test: %d, Import Run doesn't work: got %s, expected: %s", i, err, test.Err)
				}
			} else {
				if err != test.Err {
					t.Fatalf("Test: %d, Import Run doesn't work: got %s, expected: %s", i, err, test.Err)
				}
			}

		}
		err = os.Remove(logFilename)
		if err != nil {
			t.Fatalf("Failed to delete query log file: %s", err)
		}
	})
}
