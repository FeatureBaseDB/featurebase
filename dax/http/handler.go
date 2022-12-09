package http

import (
	"context"
	"net"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax/mds"
	"github.com/featurebasedb/featurebase/v3/dax/queryer"
	"github.com/featurebasedb/featurebase/v3/dax/snapshotter"
	"github.com/featurebasedb/featurebase/v3/dax/writelogger"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Handler represents an HTTP handler.
type Handler struct {
	Handler http.Handler

	bind string

	ln net.Listener
	// url is used to hold the advertise bind address for printing a log during startup.
	url string

	closeTimeout time.Duration

	server *http.Server

	mds         *mds.MDS
	writeLogger *writelogger.WriteLogger
	snapshotter *snapshotter.Snapshotter
	queryer     *queryer.Queryer

	computer http.Handler

	logger logger.Logger
}

// HandlerOption is a functional option type for Handler
type HandlerOption func(s *Handler) error

func OptHandlerBind(b string) HandlerOption {
	return func(h *Handler) error {
		h.bind = b
		return nil
	}
}

func OptHandlerMDS(m *mds.MDS) HandlerOption {
	return func(h *Handler) error {
		h.mds = m
		return nil
	}
}

func OptHandlerWriteLogger(w *writelogger.WriteLogger) HandlerOption {
	return func(h *Handler) error {
		h.writeLogger = w
		return nil
	}
}

func OptHandlerSnapshotter(s *snapshotter.Snapshotter) HandlerOption {
	return func(h *Handler) error {
		h.snapshotter = s
		return nil
	}
}

func OptHandlerQueryer(q *queryer.Queryer) HandlerOption {
	return func(h *Handler) error {
		h.queryer = q
		return nil
	}
}

func OptHandlerLogger(l logger.Logger) HandlerOption {
	return func(h *Handler) error {
		h.logger = l
		return nil
	}
}

// OptHandlerCloseTimeout controls how long to wait for the http Server to
// shutdown cleanly before forcibly destroying it. Default is 30 seconds.
func OptHandlerCloseTimeout(d time.Duration) HandlerOption {
	return func(h *Handler) error {
		h.closeTimeout = d
		return nil
	}
}

// OptHandlerListener set the listener that will be used by the HTTP server.
// Url must be the advertised URL. It will be used to show a log to the user
// about where the Web UI is. This option is mandatory.
func OptHandlerListener(ln net.Listener, url string) HandlerOption {
	return func(h *Handler) error {
		h.ln = ln
		h.url = url
		return nil
	}
}

func OptHandlerComputer(handler http.Handler) HandlerOption {
	return func(h *Handler) error {
		h.computer = handler
		return nil
	}
}

// NewHandler returns a new instance of Handler with a default logger.
func NewHandler(router http.Handler, opts ...HandlerOption) (*Handler, error) {
	handler := &Handler{
		logger:       logger.NopLogger,
		closeTimeout: time.Second * 30,
	}

	for _, opt := range opts {
		err := opt(handler)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}

	handler.Handler = router

	handler.server = &http.Server{Handler: handler}

	return handler, nil
}

func (h *Handler) Serve() error {
	err := h.server.Serve(h.ln)
	if err != nil && err.Error() != "http: Server closed" {
		h.logger.Errorf("HTTP handler terminated with error: %s\n", err)
		return errors.Wrap(err, "serve http")
	}
	return nil
}

// Close tries to cleanly shutdown the HTTP server, and failing that, after a
// timeout, calls Server.Close.
func (h *Handler) Close() error {
	deadlineCtx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(h.closeTimeout))
	defer cancelFunc()
	err := h.server.Shutdown(deadlineCtx)
	if err != nil {
		err = h.server.Close()
	}
	return errors.Wrap(err, "shutdown/close http server")
}

// ServeHTTP handles an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			stack := debug.Stack()
			h.logger.Printf("PANIC: %s\n%s", err, stack)
		}
	}()

	h.Handler.ServeHTTP(w, r)
}

// GET /health
func (h *Handler) handleGetHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
