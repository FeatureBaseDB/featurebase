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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

func TestBackupCommand_FileRequired(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)

	cm := NewBackupCommand(stdin, stdout, stderr)
	err := cm.Run(context.Background())
	if err.Error() != "output file required" {
		t.Fatalf("expect error: output file required, actual: %s", err)
	}

}

func TestBackupCommand_Run(t *testing.T) {

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)

	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	defer s.Close()
	uri, err := pilosa.NewURIFromAddress(s.Host())
	if err != nil {
		t.Fatal(err)
	}
	s.Handler.URI = uri
	s.Handler.Cluster = test.NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Holder = hldr.Holder
	cm := NewBackupCommand(stdin, stdout, stderr)
	file, err := ioutil.TempFile("", "import.csv")

	cm.Host = s.Host()
	cm.Path = file.Name()
	err = cm.Run(context.Background())
	if err != nil {
		t.Fatalf("Command not working, error: '%s'", err)
	}
}

func TestBackupRestore_Run(t *testing.T) {
	s := MustNewRunningServer(t)
	host := "http://" + s.Server.Addr().String()
	//create index
	resp, err := http.Post(host+"/index/i", "application/json", nil)
	if err != nil {
		t.Fatalf("getting status: %v", err)
	}
	//create normal frame
	resp, err = http.Post(host+"/index/i/frame/f", "application/json", nil)
	if err != nil {
		t.Fatalf("getting status: %v", err)
	}
	// set a bit in two different slices
	resp, err = http.Post(host+"/index/i/query", "application/json", strings.NewReader("SetBit(rowID=1,frame=f,columnID=0)SetBit(rowID=1,frame=f,columnID=1048577)"))
	if err != nil {
		t.Fatalf("getting status: %v", err)
	}
	// set an bitmap attribute
	resp, err = http.Post(host+"/index/i/query", "application/json", strings.NewReader("SetRowAttrs(frame=f, rowID=1, hasAttribute=true)"))
	if err != nil {
		t.Fatalf("getting status: %v", err)
	}
	// create bsi frame
	fieldOptions := `{"options":{"rangeEnabled": true, "fields": [{"name": "field0", "type": "int", "min": 0, "max": 1000},{"name": "field1", "type": "int", "min": 0, "max": 1000}]}}`
	resp, err = http.Post(host+"/index/i/frame/bsi", "application/json", strings.NewReader(fieldOptions))
	if err != nil {
		t.Fatalf("getting status: %v", err)
	}
	// set a value
	resp, err = http.Post(host+"/index/i/query", "application/json", strings.NewReader("SetFieldValue(columnID=1, frame=bsi, field0=400)SetFieldValue(columnID=1048577, frame=bsi, field0=20)"))
	if err != nil {
		t.Fatalf("getting status: %v", err)
	}
	fmt.Println(resp)
	// run backup
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	file, err := ioutil.TempFile("", "backtest")
	backup := NewBackupCommand(stdin, stdout, stderr)

	backup.Host = s.Server.Addr().String()
	backup.Path = file.Name()
	err = backup.Run(context.Background())
	if err != nil {
		t.Fatalf("Backup Command not working, error: '%s'", err)
	}
	err = file.Close()
	if err != nil {
		t.Fatalf("Closing Backup File, error: '%s'", err)
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("Closing Server, error: '%s'", err)
	}
	//zap the data
	s = MustNewRunningServer(t)
	host = "http://" + s.Server.Addr().String()
	// run restore
	restore := NewRestoreCommand(stdin, stdout, stderr)
	restore.Path = backup.Path + ".pak"
	restore.Host = s.Server.Addr().String()
	err = restore.Run(context.Background())
	if err != nil {
		t.Fatalf("Restore Command not working, error: '%s'", err)
	}

	// check bits in normal frame
	resp, err = http.Post(host+"/index/i/query", "application/json", strings.NewReader("Bitmap(rowID=1,frame=f)"))

	if err != nil {
		t.Fatalf("getting status: %v", err)
	}
	res, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	expected := `{"results":[{"attrs":{"hasAttribute":true},"bits":[0,1048577]}]}`
	if strings.Compare(string(res), expected) == 0 {

		t.Fatalf("Unexpected Result %s vs %s", string(res), expected)
	}
	// check bitmap attribute
	// check bsi value
	resp, err = http.Post(host+"/index/i/query", "application/json", strings.NewReader("Sum(frame=bsi,field=field0)"))

	if err != nil {
		t.Fatalf("getting status: %v", err)
	}
	res, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	expected = `{"results":[{"sum":420,"count":2}]}`
	if strings.Compare(string(res), expected) == 0 {

		t.Fatalf("Unexpected Result %s vs %s", string(res), expected)
	}

	s.Close()

}
