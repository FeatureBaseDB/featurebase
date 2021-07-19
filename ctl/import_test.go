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

package ctl

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/test"
	"github.com/molecula/featurebase/v2/testhook"
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

	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader(`{"options":{"keys": true}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()
	resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i/field/f", strings.NewReader(`{"options":{"keys": true}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()

	cm.Index = "i"
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
	// create a large import file in order to test the
	// translateStoreBufferSize growth logic.
	keyBytes := []byte{}
	for row := 0; row < 100; row++ {
		for col := 0; col < 100; col++ {
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

	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader(`{"options":{"keys": true}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()
	resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i/field/f", strings.NewReader(`{"options":{"keys": true}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()

	cm.Index = "i"
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
			resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+host+"/index/i/query", strings.NewReader(qry)))
			if err != nil {
				return fmt.Errorf("Querying data for validation: %s", err)
			}

			// Read body and unmarshal response.
			exp := `{"results":[100]}` + "\n"
			if body, err := ioutil.ReadAll(resp.Body); err != nil {
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

	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader(`{"options":{"keys": true}}`)))
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

	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader("")))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()
	resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i/field/f", strings.NewReader(`{"options":{"type": "int", "min": 0, "max":2147483648 }}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()

	cm.Index = "i"
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

	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i", strings.NewReader("")))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()
	resp, err = http.DefaultClient.Do(MustNewHTTPRequest("POST", "http://"+cm.Host+"/index/i/field/f", strings.NewReader(`{"options":{"type": "bool"}}`)))
	if err != nil {
		t.Fatalf("posting request: %v", err)
	}
	resp.Body.Close()

	cm.Index = "i"
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
