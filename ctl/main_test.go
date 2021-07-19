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

package ctl_test

import (
	"fmt"
	"net"
	"net/http"
	"testing"

	_ "net/http/pprof"

	"github.com/molecula/featurebase/v2/testhook"
)

func TestMain(m *testing.M) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	fmt.Printf("pilosa/ctl TestMain: online stack-traces: curl http://localhost:%v/debug/pprof/goroutine?debug=2\n", port)
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			panic(err)
		}
	}()
	testhook.RunTestsWithHooks(m)

}
