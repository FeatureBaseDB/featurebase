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
	"io"
	"os"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/server"
)

func TestConfigCommand_Run(t *testing.T) {
	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()
	cm := NewConfigCommand(stdin, w, os.Stderr)
	cm.Config = server.NewConfig()

	err := cm.Run(context.Background())
	if err != nil {
		t.Fatalf("Config Run doesn't work: %s", err)
	}
	w.Close()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(buf.String(), ":10101") {
		t.Fatalf("Unexpected config: \n%s", buf.String())
	}
}
