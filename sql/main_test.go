// Copyright 2021 Molecula Corp. All rights reserved.
package sql_test

import (
	"fmt"
	"net"
	"testing"

	"net/http"
	_ "net/http/pprof"

	"github.com/featurebasedb/featurebase/v3/testhook"
)

func TestMain(m *testing.M) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	fmt.Printf("sql/ TestMain: online stack-traces: curl http://localhost:%v/debug/pprof/goroutine?debug=2\n", port)
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			panic(err)
		}
	}()
	testhook.RunTestsWithHooks(m)

}
