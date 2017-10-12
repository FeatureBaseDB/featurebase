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
	"net/http"
	"strings"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

func TestExportCommand_Validation(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)

	cm := NewExportCommand(stdin, stdout, stderr)

	err := cm.Run(context.Background())
	if err != pilosa.ErrIndexRequired {
		t.Fatalf("Command not working, expect: %s, actual: '%s'", pilosa.ErrIndexRequired, err)
	}

	cm.Index = "i"
	err = cm.Run(context.Background())
	if err != pilosa.ErrFrameRequired {
		t.Fatalf("Command not working, expect: %s, actual: '%s'", pilosa.ErrFrameRequired, err)
	}

	cm.Frame = "f"
	cm.View = "test"
	err = cm.Run(context.Background())
	if err != pilosa.ErrInvalidView {
		t.Fatalf("Command not working, expect: %s, actual: '%s'", pilosa.ErrInvalidView, err)
	}
}

func TestExportCommand_Run(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)
	cm := NewExportCommand(stdin, stdout, stderr)

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
	cm.Host = s.Host()

	http.DefaultClient.Do(test.MustNewHTTPRequest("POST", s.URL+"/index/i", strings.NewReader("")))
	http.DefaultClient.Do(test.MustNewHTTPRequest("POST", s.URL+"/index/i/frame/f", strings.NewReader("")))

	cm.Index = "i"
	cm.Frame = "f"
	cm.View = pilosa.ViewStandard
	err = cm.Run(context.Background())
	if err != nil {
		t.Fatalf("Export Run doesn't work: %s", err)
	}
}
