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
	"io"
	"os"
	"testing"

	"github.com/pilosa/pilosa"
)

func TestBenchCommand_InvalidOption(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)

	cm := NewBenchCommand(stdin, stdout, stderr)
	err := cm.Run(context.Background())
	if err != pilosa.ErrHostRequired {
		t.Fatalf("Expect err: %s, actual err: %s", pilosa.ErrHostRequired, err)
	}

	cm.Host = "localhost:10101"
	err = cm.Run(context.Background())
	if err.Error() != "op required" {
		t.Fatalf("Expect err: %s, actual err: %s", "op required", err)
	}

	cm.Op = "test"
	err = cm.Run(context.Background())
	if err.Error() != "unknown bench op: \"test\"" {
		t.Fatalf("Expect err: %s, actual err: %s", "unknown bench op: test", err)
	}

}

func TestBenchCommand_Run(t *testing.T) {
	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()

	cm := NewBenchCommand(stdin, w, w)
	cm.Op = "set-bit"
	cm.Host = "localhost:10101"

	err := cm.Run(context.Background())
	if err.Error() != "operation count required" {
		t.Fatalf("Expect error: %s, actual err: %s", "operation count required", err)
	}

	cm.N = 1
	err = cm.Run(context.Background())
	if err != pilosa.ErrIndexRequired {
		t.Fatalf("Expect error: %s, actual err: %s", pilosa.ErrIndexRequired, err)
	}

	cm.Index = "i"
	err = cm.Run(context.Background())
	if err != pilosa.ErrFrameRequired {
		t.Fatalf("Expect error: %s, actual err: %s", pilosa.ErrFrameRequired, err)
	}

	cm.Frame = "f"
	err = cm.Run(context.Background())
	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	fmt.Println(buf.String())
	if err != nil {
		fmt.Println(buf.String())
	}
}
