package ctl

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/felixge/fgprof"
	"github.com/pkg/errors"
)

type ctlUsageError struct{}

func (c ctlUsageError) Error() string {
	return "usage error"
}

var UsageError ctlUsageError

// startProfilingServer starts a server which handles /debug/pprof and
// /debug/fgprof for use in utilities we might want to profile but
// wouldn't otherwise be running an http server. Caller should call
// the returned close function before exiting to release resources.
func startProfilingServer(addr string, logger logger.Logger) (close func() error, err error) {
	if addr == "" {
		return func() error { return nil }, nil
	}

	sm := http.NewServeMux()
	sm.Handle("/debug/fgprof", fgprof.Handler())
	sm.HandleFunc("/debug/pprof/", pprof.Index)
	sm.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	sm.HandleFunc("/debug/pprof/profile", pprof.Profile)
	sm.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	sm.HandleFunc("/debug/pprof/trace", pprof.Trace)
	s := &http.Server{
		Addr:    addr,
		Handler: sm,
	}
	runtime.SetBlockProfileRate(10000000) // 1 sample per 10 ms
	runtime.SetMutexProfileFraction(100)  // 1% sampling
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		logger.Printf("Listening for /debug/pprof/ and /debug/fgprof on '%s'", ln.Addr().String())
		logger.Printf("%v", s.Serve(ln))
	}()

	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		err := s.Shutdown(ctx)
		if err != nil {
			return errors.Wrap(err, "shutting down profiling server")
		}
		return s.Close()
	}, nil
}
