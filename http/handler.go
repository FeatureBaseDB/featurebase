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

package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	_ "net/http/pprof" // Imported for its side-effect of registering pprof endpoints with the server.
	"net/url"
	"os"
	"reflect"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/encoding/proto"
	"github.com/pilosa/pilosa/v2/logger"
	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zeebo/blake3"
)

// Handler represents an HTTP handler.
type Handler struct {
	Handler http.Handler

	logger logger.Logger

	// Keeps the query argument validators for each handler
	validators map[string]*queryValidationSpec

	api *pilosa.API

	ln net.Listener

	closeTimeout time.Duration

	server *http.Server
}

// externalPrefixFlag denotes endpoints that are intended to be exposed to clients.
// This is used for stats tagging.
var externalPrefixFlag = map[string]bool{
	"schema":  true,
	"query":   true,
	"import":  true,
	"export":  true,
	"index":   true,
	"field":   true,
	"nodes":   true,
	"version": true,
}

type errorResponse struct {
	Error string `json:"error"`
}

// handlerOption is a functional option type for pilosa.Handler
type handlerOption func(s *Handler) error

func OptHandlerAllowedOrigins(origins []string) handlerOption {
	return func(h *Handler) error {
		h.Handler = handlers.CORS(
			handlers.AllowedOrigins(origins),
			handlers.AllowedHeaders([]string{"Content-Type"}),
		)(h.Handler)
		return nil
	}
}

func OptHandlerAPI(api *pilosa.API) handlerOption {
	return func(h *Handler) error {
		h.api = api
		return nil
	}
}

func OptHandlerLogger(logger logger.Logger) handlerOption {
	return func(h *Handler) error {
		h.logger = logger
		return nil
	}
}

func OptHandlerListener(ln net.Listener) handlerOption {
	return func(h *Handler) error {
		h.ln = ln
		return nil
	}
}

// OptHandlerCloseTimeout controls how long to wait for the http Server to
// shutdown cleanly before forcibly destroying it. Default is 30 seconds.
func OptHandlerCloseTimeout(d time.Duration) handlerOption {
	return func(h *Handler) error {
		h.closeTimeout = d
		return nil
	}
}

var makeImportOk sync.Once
var importOk []byte

// NewHandler returns a new instance of Handler with a default logger.
func NewHandler(opts ...handlerOption) (*Handler, error) {
	makeImportOk.Do(func() {
		var err error
		importOk, err = proto.DefaultSerializer.Marshal(&pilosa.ImportResponse{Err: ""})
		if err != nil {
			panic(fmt.Sprintf("trying to cache import-OK response: %v", err))
		}
	})
	handler := &Handler{
		logger:       logger.NopLogger,
		closeTimeout: time.Second * 30,
	}
	handler.Handler = newRouter(handler)
	handler.populateValidators()

	for _, opt := range opts {
		err := opt(handler)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}

	if handler.api == nil {
		return nil, errors.New("must pass OptHandlerAPI")
	}

	if handler.ln == nil {
		return nil, errors.New("must pass OptHandlerListener")
	}

	handler.server = &http.Server{Handler: handler}

	return handler, nil
}

func (h *Handler) Serve() error {
	err := h.server.Serve(h.ln)
	if err != nil && err.Error() != "http: Server closed" {
		h.logger.Printf("HTTP handler terminated with error: %s\n", err)
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

func (h *Handler) populateValidators() {
	h.validators = map[string]*queryValidationSpec{}
	h.validators["Home"] = queryValidationSpecRequired()
	h.validators["PostClusterResizeAbort"] = queryValidationSpecRequired()
	h.validators["PostClusterResizeRemoveNode"] = queryValidationSpecRequired()
	h.validators["PostClusterResizeSetCoordinator"] = queryValidationSpecRequired()
	h.validators["GetExport"] = queryValidationSpecRequired("index", "field", "shard")
	h.validators["GetIndexes"] = queryValidationSpecRequired()
	h.validators["GetIndex"] = queryValidationSpecRequired()
	h.validators["PostIndex"] = queryValidationSpecRequired()
	h.validators["DeleteIndex"] = queryValidationSpecRequired()
	h.validators["GetTranslateData"] = queryValidationSpecRequired("index", "partition")
	h.validators["PostTranslateKeys"] = queryValidationSpecRequired()
	h.validators["PostField"] = queryValidationSpecRequired()
	h.validators["DeleteField"] = queryValidationSpecRequired()
	h.validators["PostImport"] = queryValidationSpecRequired().Optional("clear", "ignoreKeyCheck")
	h.validators["PostImportRoaring"] = queryValidationSpecRequired().Optional("remote", "clear")
	h.validators["PostQuery"] = queryValidationSpecRequired().Optional("shards", "columnAttrs", "excludeRowAttrs", "excludeColumns", "profile")
	h.validators["GetInfo"] = queryValidationSpecRequired()
	h.validators["RecalculateCaches"] = queryValidationSpecRequired()
	h.validators["GetSchema"] = queryValidationSpecRequired()
	h.validators["PostSchema"] = queryValidationSpecRequired().Optional("remote")
	h.validators["GetStatus"] = queryValidationSpecRequired()
	h.validators["GetVersion"] = queryValidationSpecRequired()
	h.validators["PostClusterMessage"] = queryValidationSpecRequired()
	h.validators["GetFragmentBlockData"] = queryValidationSpecRequired()
	h.validators["GetFragmentBlocks"] = queryValidationSpecRequired("index", "field", "view", "shard")
	h.validators["GetFragmentData"] = queryValidationSpecRequired("index", "field", "view", "shard")
	h.validators["GetFragmentNodes"] = queryValidationSpecRequired("shard", "index")
	h.validators["PostIndexAttrDiff"] = queryValidationSpecRequired()
	h.validators["PostFieldAttrDiff"] = queryValidationSpecRequired()
	h.validators["GetNodes"] = queryValidationSpecRequired()
	h.validators["GetShardMax"] = queryValidationSpecRequired()
	h.validators["GetTransactions"] = queryValidationSpecRequired()
	h.validators["GetTransaction"] = queryValidationSpecRequired()
	h.validators["PostTransaction"] = queryValidationSpecRequired()
	h.validators["PostFinishTransaction"] = queryValidationSpecRequired()
	h.validators["Inspect"] = queryValidationSpecRequired().Optional("indexes", "fields", "views", "shards", "checksum", "containers")

}

type contextKeyQuery int

const (
	contextKeyQueryRequest contextKeyQuery = iota
	contextKeyQueryError
)

// addQueryContext puts the results of handler.readQueryRequest into the Context for use by
// both other middleware and any handlers.
func (h *Handler) addQueryContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pathParts := strings.Split(r.URL.Path, "/")
		if len(pathParts) > 3 && pathParts[3] == "query" {
			req, err := h.readQueryRequest(r)
			ctx := context.WithValue(r.Context(), contextKeyQueryRequest, req)
			ctx = context.WithValue(ctx, contextKeyQueryError, err)
			next.ServeHTTP(w, r.WithContext(ctx))
		} else {
			next.ServeHTTP(w, r)
		}
	})
}

func (h *Handler) queryArgValidator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := mux.CurrentRoute(r).GetName()

		if validator, ok := h.validators[key]; ok {
			if err := validator.validate(r.URL.Query()); err != nil {
				errText := err.Error()
				if validHeaderAcceptJSON(r.Header) {
					response := errorResponse{Error: errText}
					data, err := json.Marshal(response)
					if err != nil {
						h.logger.Printf("failed to encode error %q as JSON: %v", errText, err)
					} else {
						errText = string(data)
					}
				}
				http.Error(w, errText, http.StatusBadRequest)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) extractTracing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span, ctx := tracing.GlobalTracer.ExtractHTTPHeaders(r)
		defer span.Finish()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *Handler) collectStats(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t := time.Now()
		next.ServeHTTP(w, r)
		dur := time.Since(t)

		statsTags := make([]string, 0, 5)

		longQueryTime := h.api.LongQueryTime()
		if longQueryTime > 0 && dur > longQueryTime {
			queryRequest := r.Context().Value(contextKeyQueryRequest)

			var queryString string
			if req, ok := queryRequest.(*pilosa.QueryRequest); ok {
				queryString = req.Query
			}

			h.logger.Printf("%s %s %v %s", r.Method, r.URL.String(), dur, queryString)
			statsTags = append(statsTags, "slow:true")
		} else {
			statsTags = append(statsTags, "slow:false")
		}

		pathParts := strings.Split(r.URL.Path, "/")
		if externalPrefixFlag[pathParts[1]] {
			statsTags = append(statsTags, "where:external")
		} else {
			statsTags = append(statsTags, "where:internal")
		}

		statsTags = append(statsTags, "useragent:"+r.UserAgent())

		path, err := mux.CurrentRoute(r).GetPathTemplate()
		if err == nil {
			statsTags = append(statsTags, "path:"+path)
		}

		statsTags = append(statsTags, "method:"+r.Method)

		stats := h.api.StatsWithTags(statsTags)
		if stats != nil {
			stats.Timing(pilosa.MetricHTTPRequest, dur, 0.1)
		}
	})
}

// newRouter creates a new mux http router.
func newRouter(handler *Handler) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/", handler.handleHome).Methods("GET").Name("Home")
	router.HandleFunc("/cluster/resize/abort", handler.handlePostClusterResizeAbort).Methods("POST").Name("PostClusterResizeAbort")
	router.HandleFunc("/cluster/resize/remove-node", handler.handlePostClusterResizeRemoveNode).Methods("POST").Name("PostClusterResizeRemoveNode")
	router.HandleFunc("/cluster/resize/set-coordinator", handler.handlePostClusterResizeSetCoordinator).Methods("POST").Name("PostClusterResizeSetCoordinator")
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux).Methods("GET")
	router.Handle("/debug/vars", expvar.Handler()).Methods("GET")
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/export", handler.handleGetExport).Methods("GET").Name("GetExport")
	router.HandleFunc("/index", handler.handleGetIndexes).Methods("GET").Name("GetIndexes")
	router.HandleFunc("/index", handler.handlePostIndex).Methods("POST").Name("PostIndex")
	router.HandleFunc("/index/", handler.handlePostIndex).Methods("POST").Name("PostIndex")
	router.HandleFunc("/index/{index}", handler.handleGetIndex).Methods("GET").Name("GetIndex")
	router.HandleFunc("/index/{index}", handler.handlePostIndex).Methods("POST").Name("PostIndex")
	router.HandleFunc("/index/{index}", handler.handleDeleteIndex).Methods("DELETE").Name("DeleteIndex")
	//router.HandleFunc("/index/{index}/field", handler.handleGetFields).Methods("GET") // Not implemented.
	router.HandleFunc("/index/{index}/import-column-attrs", handler.handlePostImportColumnAttrs).Methods("POST").Name("PostImportColumnAttrs")
	router.HandleFunc("/index/{index}/field", handler.handlePostField).Methods("POST").Name("PostField")
	router.HandleFunc("/index/{index}/field/", handler.handlePostField).Methods("POST").Name("PostField")
	router.HandleFunc("/index/{index}/field/{field}", handler.handlePostField).Methods("POST").Name("PostField")
	router.HandleFunc("/index/{index}/field/{field}", handler.handleDeleteField).Methods("DELETE").Name("DeleteField")
	router.HandleFunc("/index/{index}/field/{field}/import", handler.handlePostImport).Methods("POST").Name("PostImport")
	router.HandleFunc("/index/{index}/field/{field}/import-roaring/{shard}", handler.handlePostImportRoaring).Methods("POST").Name("PostImportRoaring")
	router.HandleFunc("/index/{index}/query", handler.handlePostQuery).Methods("POST").Name("PostQuery")
	router.HandleFunc("/info", handler.handleGetInfo).Methods("GET").Name("GetInfo")
	router.HandleFunc("/inspect", handler.handleInspect).Methods("GET").Name("Inspect")
	router.HandleFunc("/recalculate-caches", handler.handleRecalculateCaches).Methods("POST").Name("RecalculateCaches")
	router.HandleFunc("/schema", handler.handleGetSchema).Methods("GET").Name("GetSchema")
	router.HandleFunc("/schema", handler.handlePostSchema).Methods("POST").Name("PostSchema")
	router.HandleFunc("/status", handler.handleGetStatus).Methods("GET").Name("GetStatus")
	router.HandleFunc("/transaction", handler.handlePostTransaction).Methods("POST").Name("PostTransaction")
	router.HandleFunc("/transaction/", handler.handlePostTransaction).Methods("POST").Name("PostTransaction")
	router.HandleFunc("/transaction/{id}", handler.handleGetTransaction).Methods("GET").Name("GetTransaction")
	router.HandleFunc("/transaction/{id}", handler.handlePostTransaction).Methods("POST").Name("PostTransaction")
	router.HandleFunc("/transaction/{id}/finish", handler.handlePostFinishTransaction).Methods("POST").Name("PostFinishTransaction")
	router.HandleFunc("/transactions", handler.handleGetTransactions).Methods("GET").Name("GetTransactions")
	router.HandleFunc("/queries", handler.handleGetActiveQueries).Methods("GET").Name("GetActiveQueries")
	router.HandleFunc("/version", handler.handleGetVersion).Methods("GET").Name("GetVersion")

	// /internal endpoints are for internal use only; they may change at any time.
	// DO NOT rely on these for external applications!
	router.HandleFunc("/internal/cluster/message", handler.handlePostClusterMessage).Methods("POST").Name("PostClusterMessage")
	router.HandleFunc("/internal/fragment/block/data", handler.handleGetFragmentBlockData).Methods("GET").Name("GetFragmentBlockData")
	router.HandleFunc("/internal/fragment/blocks", handler.handleGetFragmentBlocks).Methods("GET").Name("GetFragmentBlocks")
	router.HandleFunc("/internal/fragment/data", handler.handleGetFragmentData).Methods("GET").Name("GetFragmentData")
	router.HandleFunc("/internal/fragment/nodes", handler.handleGetFragmentNodes).Methods("GET").Name("GetFragmentNodes")
	router.HandleFunc("/internal/index/{index}/attr/diff", handler.handlePostIndexAttrDiff).Methods("POST").Name("PostIndexAttrDiff")
	router.HandleFunc("/internal/translate/data", handler.handleGetTranslateData).Methods("GET").Name("GetTranslateData")
	router.HandleFunc("/internal/translate/data", handler.handlePostTranslateData).Methods("POST").Name("PostTranslateData")
	router.HandleFunc("/internal/translate/keys", handler.handlePostTranslateKeys).Methods("POST").Name("PostTranslateKeys")
	router.HandleFunc("/internal/translate/ids", handler.handlePostTranslateIDs).Methods("POST").Name("PostTranslateIDs")
	router.HandleFunc("/internal/index/{index}/field/{field}/attr/diff", handler.handlePostFieldAttrDiff).Methods("POST").Name("PostFieldAttrDiff")
	router.HandleFunc("/internal/index/{index}/field/{field}/remote-available-shards/{shardID}", handler.handleDeleteRemoteAvailableShard).Methods("DELETE")
	router.HandleFunc("/internal/nodes", handler.handleGetNodes).Methods("GET").Name("GetNodes")
	router.HandleFunc("/internal/shards/max", handler.handleGetShardsMax).Methods("GET").Name("GetShardsMax") // TODO: deprecate, but it's being used by the client

	router.HandleFunc("/internal/translate/index/{index}/{partition}", handler.handlePostTranslateIndexDB).Methods("POST").Name("PostTranslateIndexDB")
	router.HandleFunc("/internal/translate/field/{index}/{field}", handler.handlePostTranslateFieldDB).Methods("POST").Name("PostTranslateFieldDB")

	router.Use(handler.queryArgValidator)
	router.Use(handler.addQueryContext)
	router.Use(handler.extractTracing)
	router.Use(handler.collectStats)
	return router
}

// ServeHTTP handles an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			stack := debug.Stack()
			msg := "PANIC: %s\n%s"
			h.logger.Printf(msg, err, stack)
			fmt.Fprintf(w, msg, err, stack)
		}
	}()

	h.Handler.ServeHTTP(w, r)
}

// successResponse is a general success/error struct for http responses.
type successResponse struct {
	h         *Handler
	Success   bool   `json:"success"`
	Name      string `json:"name,omitempty"`
	CreatedAt int64  `json:"createdAt,omitempty"`
	Error     *Error `json:"error,omitempty"`
}

// check determines success or failure based on the error.
// It also returns the corresponding http status code.
func (r *successResponse) check(err error) (statusCode int) {
	if err == nil {
		r.Success = true
		return 0
	}

	cause := errors.Cause(err)

	// Determine HTTP status code based on the error type.
	switch cause.(type) {
	case pilosa.BadRequestError:
		statusCode = http.StatusBadRequest
	case pilosa.ConflictError:
		statusCode = http.StatusConflict
	case pilosa.NotFoundError:
		statusCode = http.StatusNotFound
	default:
		statusCode = http.StatusInternalServerError
	}

	r.Success = false
	r.Error = &Error{Message: err.Error()}

	return statusCode
}

// write sends a response to the http.ResponseWriter based on the success
// status and the error.
func (r *successResponse) write(w http.ResponseWriter, err error) {
	// Apply the error and get the status code.
	statusCode := r.check(err)

	// Marshal the json response.
	msg, err := json.Marshal(r)
	if err != nil {
		http.Error(w, string(msg), http.StatusInternalServerError)
		return
	}

	// Write the response.
	if statusCode == 0 {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write(msg)
		if err != nil {
			r.h.logger.Printf("error writing response: %v", err)
			return
		}
		_, err = w.Write([]byte("\n"))
		if err != nil {
			r.h.logger.Printf("error writing newline after response: %v", err)
			return
		}
	} else {
		http.Error(w, string(msg), statusCode)
	}
}

func (h *Handler) handleHome(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "Welcome. Pilosa is running. Visit https://www.pilosa.com/docs/ for more information.", http.StatusNotFound)
}

// validHeaderAcceptJSON returns false if one or more Accept
// headers are present, but none of them are "application/json"
// (or any matching wildcard). Otherwise returns true.
func validHeaderAcceptJSON(header http.Header) bool {
	return validHeaderAcceptType(header, "application", "json")
}

func validHeaderAcceptType(header http.Header, typ, subtyp string) bool {
	if v, found := header["Accept"]; found {
		for _, v := range v {
			t, _, err := mime.ParseMediaType(v)
			if err != nil {
				switch err {
				case mime.ErrInvalidMediaParameter:
					// This is an optional feature, so we can keep going anyway.
				default:
					continue
				}
			}
			spl := strings.SplitN(t, "/", 2)
			if len(spl) < 2 {
				continue
			}
			switch {
			case spl[0] == typ && spl[1] == subtyp:
				return true
			case spl[0] == "*" && spl[1] == subtyp:
				return true
			case spl[0] == typ && spl[1] == "*":
				return true
			case spl[0] == "*" && spl[1] == "*":
				return true
			}
		}
		return false
	}
	return true
}

// headerAcceptRoaringRow tells us that the request should accept roaring
// rows in response.
func headerAcceptRoaringRow(header http.Header) bool {
	for _, v := range header["X-Pilosa-Row"] {
		if v == "roaring" {
			return true
		}
	}
	return false
}

// handleGetSchema handles GET /schema requests.
func (h *Handler) handleGetSchema(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	schema := h.api.Schema(r.Context())
	if err := json.NewEncoder(w).Encode(pilosa.Schema{Indexes: schema}); err != nil {
		h.logger.Printf("write schema response error: %s", err)
	}
}

func (h *Handler) handlePostSchema(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	remoteStr := q.Get("remote")
	var remote bool
	if remoteStr == "true" {
		remote = true
	}

	schema := &pilosa.Schema{}
	if err := json.NewDecoder(r.Body).Decode(schema); err != nil {
		http.Error(w, fmt.Sprintf("decoding request as JSON Pilosa schema: %v", err), http.StatusBadRequest)
		return
	}

	if err := h.api.ApplySchema(r.Context(), schema, remote); err != nil {
		http.Error(w, fmt.Sprintf("apply schema to Pilosa: %v", err), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleGetStatus handles GET /status requests.
func (h *Handler) handleGetStatus(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	status := getStatusResponse{
		State:   h.api.State(),
		Nodes:   h.api.Hosts(r.Context()),
		LocalID: h.api.Node().ID,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		h.logger.Printf("write status response error: %s", err)
	}
}

func (h *Handler) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	info := h.api.Info()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(info); err != nil {
		h.logger.Printf("write info response error: %s", err)
	}
}

func (h *Handler) handleInspect(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	q := r.URL.Query()
	_, checksum := q["checksum"]
	_, containers := q["containers"]
	req := pilosa.InspectRequest{
		HolderFilterParams: pilosa.HolderFilterParams{
			Indexes: q.Get("indexes"),
			Fields:  q.Get("fields"),
			Views:   q.Get("views"),
			Shards:  q.Get("shards"),
		},
		InspectRequestParams: pilosa.InspectRequestParams{
			Checksum:   checksum,
			Containers: containers,
		},
	}
	info, err := h.api.Inspect(r.Context(), &req)
	if err != nil {
		http.Error(w, fmt.Sprintf("inspect request: %v", err), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(info); err != nil {
		h.logger.Printf("write inspect response error: %s", err)
	}
}

type getSchemaResponse struct {
	Indexes []*pilosa.IndexInfo `json:"indexes"`
}

type getStatusResponse struct {
	State   string         `json:"state"`
	Nodes   []*pilosa.Node `json:"nodes"`
	LocalID string         `json:"localID"`
}

func hash(s string) string {

	hasher := blake3.New()
	_, _ = hasher.Write([]byte(s))
	var buf [16]byte
	_, _ = hasher.Digest().Read(buf[0:])

	return fmt.Sprintf("%x", buf)
}

var DoPerQueryProfiling = false

// handlePostQuery handles /query requests.
func (h *Handler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	// Read previouly parsed request from context
	qreq := r.Context().Value(contextKeyQueryRequest)
	qerr := r.Context().Value(contextKeyQueryError)
	req, ok := qreq.(*pilosa.QueryRequest)

	if DoPerQueryProfiling {

		txsrc := os.Getenv("PILOSA_TXSRC")
		reqHash := hash(req.Query)

		qlen := len(req.Query)
		if qlen > 100 {
			qlen = 100
		}
		name := "_query." + reqHash + "." + txsrc + "." + time.Now().Format("20060102150405") + "." + req.Query[:qlen]
		f, err := os.Create(name)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		_ = pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()

	} // end DoPerQueryProfiling
	/*
		er = trace.Start(f)
		if er != nil {
			panic(er)
		}
		defer trace.Stop()
	*/

	var err error
	err, _ = qerr.(error)

	if err != nil || !ok {
		w.WriteHeader(http.StatusBadRequest)
		e := h.writeQueryResponse(w, r, &pilosa.QueryResponse{Err: err})
		if e != nil {
			h.logger.Printf("write query response error: %v (while trying to write another error: %v)", e, err)
		}
		return
	}
	// TODO: Remove
	req.Index = mux.Vars(r)["index"]

	resp, err := h.api.Query(r.Context(), req)
	if err != nil {
		switch errors.Cause(err) {
		case pilosa.ErrTooManyWrites:
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		case pilosa.ErrTranslateStoreReadOnly:
			u := h.api.PrimaryReplicaNodeURL()
			u.Path, u.RawQuery = r.URL.Path, r.URL.RawQuery
			http.Redirect(w, r, u.String(), http.StatusFound)
			return
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
		e := h.writeQueryResponse(w, r, &pilosa.QueryResponse{Err: err})
		if e != nil {
			h.logger.Printf("write query response error: %v (while trying to write another error: %v)", e, err)
		}
		return
	}

	// Set appropriate status code, if there is an error. It doesn't appear that
	// resp.Err could ever be set in API.Query, so this code block is probably
	// doing nothing right now.
	if resp.Err != nil {
		switch errors.Cause(resp.Err) {
		case pilosa.ErrTooManyWrites:
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}

	// Write response back to client.
	if err := h.writeQueryResponse(w, r, &resp); err != nil {
		h.logger.Printf("write query response error: %s", err)
	}
}

// handleGetShardsMax handles GET /internal/shards/max requests.
func (h *Handler) handleGetShardsMax(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(getShardsMaxResponse{
		Standard: h.api.MaxShards(r.Context()),
	}); err != nil {
		h.logger.Printf("write shards-max response error: %s", err)
	}
}

type getShardsMaxResponse struct {
	Standard map[string]uint64 `json:"standard"`
}

// handleGetIndexes handles GET /index request.
func (h *Handler) handleGetIndexes(w http.ResponseWriter, r *http.Request) {
	h.handleGetSchema(w, r)
}

// handleGetIndex handles GET /index/<indexname> requests.
func (h *Handler) handleGetIndex(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	indexName := mux.Vars(r)["index"]
	for _, idx := range h.api.Schema(r.Context()) {
		if idx.Name == indexName {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(idx); err != nil {
				h.logger.Printf("write response error: %s", err)
			}
			return
		}
	}
	http.Error(w, fmt.Sprintf("Index %s Not Found", indexName), http.StatusNotFound)
}

type postIndexRequest struct {
	Options pilosa.IndexOptions `json:"options"`
}

//_postIndexRequest is necessary to avoid recursion while decoding.
type _postIndexRequest postIndexRequest

// Custom Unmarshal JSON to validate request body when creating a new index.
func (p *postIndexRequest) UnmarshalJSON(b []byte) error {

	// m is an overflow map used to capture additional, unexpected keys.
	m := make(map[string]interface{})
	if err := json.Unmarshal(b, &m); err != nil {
		return errors.Wrap(err, "unmarshalling unexpected values")
	}

	validIndexOptions := getValidOptions(pilosa.IndexOptions{})
	err := validateOptions(m, validIndexOptions)
	if err != nil {
		return err
	}
	// Unmarshal expected values.
	_p := _postIndexRequest{
		Options: pilosa.IndexOptions{
			Keys:           false,
			TrackExistence: true,
		},
	}
	if err := json.Unmarshal(b, &_p); err != nil {
		return errors.Wrap(err, "unmarshalling expected values")
	}

	p.Options = _p.Options

	return nil
}

func getValidOptions(option interface{}) []string {
	validOptions := []string{}
	val := reflect.ValueOf(option)
	for i := 0; i < val.Type().NumField(); i++ {
		jsonTag := val.Type().Field(i).Tag.Get("json")
		s := strings.Split(jsonTag, ",")
		validOptions = append(validOptions, s[0])
	}
	return validOptions
}

// Raise errors for any unknown key
func validateOptions(data map[string]interface{}, validIndexOptions []string) error {
	for k, v := range data {
		switch k {
		case "options":
			options, ok := v.(map[string]interface{})
			if !ok {
				return errors.New("options is not map[string]interface{}")
			}
			for kk, vv := range options {
				if !foundItem(validIndexOptions, kk) {
					return fmt.Errorf("unknown key: %v:%v", kk, vv)
				}
			}
		default:
			return fmt.Errorf("unknown key: %v:%v", k, v)
		}
	}
	return nil
}

func foundItem(items []string, item string) bool {
	for _, i := range items {
		if item == i {
			return true
		}
	}
	return false
}

// handleDeleteIndex handles DELETE /index request.
func (h *Handler) handleDeleteIndex(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	indexName := mux.Vars(r)["index"]

	resp := successResponse{h: h}
	err := h.api.DeleteIndex(r.Context(), indexName)
	resp.write(w, err)
}

// handlePostIndex handles POST /index request.
func (h *Handler) handlePostIndex(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}

	resp := successResponse{h: h, Name: indexName}

	// Decode request.
	req := postIndexRequest{
		Options: pilosa.IndexOptions{
			Keys:           false,
			TrackExistence: true,
		},
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil && err != io.EOF {
		resp.write(w, err)
		return
	}
	index, err := h.api.CreateIndex(r.Context(), indexName, req.Options)

	if index != nil {
		resp.CreatedAt = index.CreatedAt()
	} else if _, ok = errors.Cause(err).(pilosa.ConflictError); ok {
		if index, _ = h.api.Index(r.Context(), indexName); index != nil {
			resp.CreatedAt = index.CreatedAt()
		}
	}
	resp.write(w, err)
}

// handlePostIndexAttrDiff handles POST /internal/index/attr/diff requests.
func (h *Handler) handlePostIndexAttrDiff(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	indexName := mux.Vars(r)["index"]

	// Decode request.
	var req postIndexAttrDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	attrs, err := h.api.IndexAttrDiff(r.Context(), indexName, req.Blocks)
	if err != nil {
		if errors.Cause(err) == pilosa.ErrIndexNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(postIndexAttrDiffResponse{
		Attrs: attrs,
	}); err != nil {
		h.logger.Printf("response encoding error: %s", err)
	}
}

func (h *Handler) handleGetActiveQueries(w http.ResponseWriter, r *http.Request) {
	var rtype string
	switch {
	case validHeaderAcceptType(r.Header, "text", "plain"):
		rtype = "text/plain"
	case validHeaderAcceptJSON(r.Header):
		rtype = "application/json"
	default:
		http.Error(w, "no acceptable response type selected", http.StatusNotAcceptable)
		return
	}
	queries, err := h.api.ActiveQueries(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", rtype)
	switch rtype {
	case "text/plain":
		durations := make([]string, len(queries))
		for i, q := range queries {
			durations[i] = q.Age.String()
		}
		var maxlen int
		for _, l := range durations {
			if len(l) > maxlen {
				maxlen = len(l)
			}
		}
		for i, q := range queries {
			_, err := fmt.Fprintf(w, "%*s%q\n", -(maxlen + 2), durations[i], q.Query)
			if err != nil {
				h.logger.Printf("sending GetActiveQueries response: %s", err)
				return
			}
		}
		if _, err := w.Write([]byte{'\n'}); err != nil {
			h.logger.Printf("sending GetActiveQueries response: %s", err)
		}
	case "application/json":
		if err := json.NewEncoder(w).Encode(queries); err != nil {
			h.logger.Printf("encoding GetActiveQueries response: %s", err)
		}
	}
}

type postIndexAttrDiffRequest struct {
	Blocks []pilosa.AttrBlock `json:"blocks"`
}

type postIndexAttrDiffResponse struct {
	Attrs map[uint64]map[string]interface{} `json:"attrs"`
}

// handlePostField handles POST /field request.
func (h *Handler) handlePostField(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}

	fieldName, ok := mux.Vars(r)["field"]
	if !ok {
		http.Error(w, "field name is required", http.StatusBadRequest)
		return
	}

	resp := successResponse{h: h, Name: fieldName}

	// Decode request.
	var req postFieldRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(&req)
	if err != nil && err != io.EOF {
		resp.write(w, err)
		return
	}

	// Validate field options.
	if err := req.Options.validate(); err != nil {
		resp.write(w, err)
		return
	}

	// Convert json options into functional options.
	var fos []pilosa.FieldOption
	switch req.Options.Type {
	case pilosa.FieldTypeSet:
		fos = append(fos, pilosa.OptFieldTypeSet(*req.Options.CacheType, *req.Options.CacheSize))
	case pilosa.FieldTypeInt:
		if req.Options.Min == nil {
			min := pql.NewDecimal(int64(math.MinInt64), 0)
			req.Options.Min = &min
		}
		if req.Options.Max == nil {
			max := pql.NewDecimal(int64(math.MaxInt64), 0)
			req.Options.Max = &max
		}
		fos = append(fos, pilosa.OptFieldTypeInt(req.Options.Min.ToInt64(0), req.Options.Max.ToInt64(0)))
	case pilosa.FieldTypeDecimal:
		scale := int64(0)
		if req.Options.Scale != nil {
			scale = *req.Options.Scale
		}
		if req.Options.Min == nil {
			min := pql.NewDecimal(int64(math.MinInt64), scale)
			req.Options.Min = &min
		}
		if req.Options.Max == nil {
			max := pql.NewDecimal(int64(math.MaxInt64), scale)
			req.Options.Max = &max
		}
		var minmax []pql.Decimal
		if req.Options.Min != nil {
			minmax = []pql.Decimal{
				*req.Options.Min,
			}
			if req.Options.Max != nil {
				minmax = append(minmax, *req.Options.Max)
			}
		}
		fos = append(fos, pilosa.OptFieldTypeDecimal(scale, minmax...))
	case pilosa.FieldTypeTime:
		fos = append(fos, pilosa.OptFieldTypeTime(*req.Options.TimeQuantum, req.Options.NoStandardView))
	case pilosa.FieldTypeMutex:
		fos = append(fos, pilosa.OptFieldTypeMutex(*req.Options.CacheType, *req.Options.CacheSize))
	case pilosa.FieldTypeBool:
		fos = append(fos, pilosa.OptFieldTypeBool())
	}
	if req.Options.Keys != nil {
		if *req.Options.Keys {
			fos = append(fos, pilosa.OptFieldKeys())
		}
	}
	if req.Options.ForeignIndex != nil {
		fos = append(fos, pilosa.OptFieldForeignIndex(*req.Options.ForeignIndex))
	}

	field, err := h.api.CreateField(r.Context(), indexName, fieldName, fos...)
	if _, ok := err.(pilosa.BadRequestError); ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if field != nil {
		resp.CreatedAt = field.CreatedAt()
	} else if _, ok = errors.Cause(err).(pilosa.ConflictError); ok {
		if field, _ = h.api.Field(r.Context(), indexName, fieldName); field != nil {
			resp.CreatedAt = field.CreatedAt()
		}
	}
	resp.write(w, err)
}

type postFieldRequest struct {
	Options fieldOptions `json:"options"`
}

// fieldOptions tracks pilosa.FieldOptions. It is made up of pointers to values,
// and used for input validation.
type fieldOptions struct {
	Type           string              `json:"type,omitempty"`
	CacheType      *string             `json:"cacheType,omitempty"`
	CacheSize      *uint32             `json:"cacheSize,omitempty"`
	Min            *pql.Decimal        `json:"min,omitempty"`
	Max            *pql.Decimal        `json:"max,omitempty"`
	Scale          *int64              `json:"scale,omitempty"`
	TimeQuantum    *pilosa.TimeQuantum `json:"timeQuantum,omitempty"`
	Keys           *bool               `json:"keys,omitempty"`
	NoStandardView bool                `json:"noStandardView,omitempty"`
	ForeignIndex   *string             `json:"foreignIndex,omitempty"`
}

func (o *fieldOptions) validate() error {
	// Pointers to default values.
	defaultCacheType := pilosa.DefaultCacheType
	defaultCacheSize := uint32(pilosa.DefaultCacheSize)

	switch o.Type {
	case pilosa.FieldTypeSet, "":
		// Because FieldTypeSet is the default, its arguments are
		// not required. Instead, the defaults are applied whenever
		// a value does not exist.
		if o.Type == "" {
			o.Type = pilosa.FieldTypeSet
		}
		if o.CacheType == nil {
			o.CacheType = &defaultCacheType
		}
		if o.CacheSize == nil {
			o.CacheSize = &defaultCacheSize
		}
		if o.Min != nil {
			return pilosa.NewBadRequestError(errors.New("min does not apply to field type set"))
		} else if o.Max != nil {
			return pilosa.NewBadRequestError(errors.New("max does not apply to field type set"))
		} else if o.TimeQuantum != nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum does not apply to field type set"))
		} else if o.ForeignIndex != nil {
			return pilosa.NewBadRequestError(errors.New("set field cannot be a foreign key"))
		}
	case pilosa.FieldTypeInt:
		if o.CacheType != nil {
			return pilosa.NewBadRequestError(errors.New("cacheType does not apply to field type int"))
		} else if o.CacheSize != nil {
			return pilosa.NewBadRequestError(errors.New("cacheSize does not apply to field type int"))
		} else if o.TimeQuantum != nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum does not apply to field type int"))
		} else if o.ForeignIndex != nil && o.Type == pilosa.FieldTypeDecimal {
			return pilosa.NewBadRequestError(errors.New("decimal field cannot be a foreign key"))
		}
	case pilosa.FieldTypeDecimal:
		if o.Scale == nil {
			return pilosa.NewBadRequestError(errors.New("decimal field requires a scale argument"))
		} else if o.CacheType != nil {
			return pilosa.NewBadRequestError(errors.New("cacheType does not apply to field type int"))
		} else if o.CacheSize != nil {
			return pilosa.NewBadRequestError(errors.New("cacheSize does not apply to field type int"))
		} else if o.TimeQuantum != nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum does not apply to field type int"))
		} else if o.ForeignIndex != nil && o.Type == pilosa.FieldTypeDecimal {
			return pilosa.NewBadRequestError(errors.New("decimal field cannot be a foreign key"))
		}
	case pilosa.FieldTypeTime:
		if o.CacheType != nil {
			return pilosa.NewBadRequestError(errors.New("cacheType does not apply to field type time"))
		} else if o.CacheSize != nil {
			return pilosa.NewBadRequestError(errors.New("cacheSize does not apply to field type time"))
		} else if o.Min != nil {
			return pilosa.NewBadRequestError(errors.New("min does not apply to field type time"))
		} else if o.Max != nil {
			return pilosa.NewBadRequestError(errors.New("max does not apply to field type time"))
		} else if o.TimeQuantum == nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum is required for field type time"))
		} else if o.ForeignIndex != nil {
			return pilosa.NewBadRequestError(errors.New("time field cannot be a foreign key"))
		}
	case pilosa.FieldTypeMutex:
		if o.CacheType == nil {
			o.CacheType = &defaultCacheType
		}
		if o.CacheSize == nil {
			o.CacheSize = &defaultCacheSize
		}
		if o.Min != nil {
			return pilosa.NewBadRequestError(errors.New("min does not apply to field type mutex"))
		} else if o.Max != nil {
			return pilosa.NewBadRequestError(errors.New("max does not apply to field type mutex"))
		} else if o.TimeQuantum != nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum does not apply to field type mutex"))
		} else if o.ForeignIndex != nil {
			return pilosa.NewBadRequestError(errors.New("mutex field cannot be a foreign key"))
		}
	case pilosa.FieldTypeBool:
		if o.CacheType != nil {
			return pilosa.NewBadRequestError(errors.New("cacheType does not apply to field type bool"))
		} else if o.CacheSize != nil {
			return pilosa.NewBadRequestError(errors.New("cacheSize does not apply to field type bool"))
		} else if o.Min != nil {
			return pilosa.NewBadRequestError(errors.New("min does not apply to field type bool"))
		} else if o.Max != nil {
			return pilosa.NewBadRequestError(errors.New("max does not apply to field type bool"))
		} else if o.TimeQuantum != nil {
			return pilosa.NewBadRequestError(errors.New("timeQuantum does not apply to field type bool"))
		} else if o.Keys != nil {
			return pilosa.NewBadRequestError(errors.New("keys does not apply to field type bool"))
		} else if o.ForeignIndex != nil {
			return pilosa.NewBadRequestError(errors.New("bool field cannot be a foreign key"))
		}
	default:
		return errors.Errorf("invalid field type: %s", o.Type)
	}
	return nil
}

// handleDeleteField handles DELETE /field request.
func (h *Handler) handleDeleteField(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	indexName := mux.Vars(r)["index"]
	fieldName := mux.Vars(r)["field"]

	resp := successResponse{h: h}
	err := h.api.DeleteField(r.Context(), indexName, fieldName)
	resp.write(w, err)
}

func (h *Handler) handleGetTransactions(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	trnsMap, err := h.api.Transactions(r.Context())
	if err != nil {
		switch errors.Cause(err) {
		case pilosa.ErrNodeNotCoordinator:
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, "problem getting transactions: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(trnsMap); err != nil {
		h.logger.Printf("encoding GetTransactions response: %s", err)
	}
}

type TransactionResponse struct {
	Transaction *pilosa.Transaction `json:"transaction,omitempty"`
	Error       string              `json:"error,omitempty"`
}

func (h *Handler) doTransactionResponse(w http.ResponseWriter, err error, trns *pilosa.Transaction) {
	if err != nil {
		switch errors.Cause(err) {
		case pilosa.ErrNodeNotCoordinator, pilosa.ErrTransactionExists:
			w.WriteHeader(http.StatusBadRequest)
		case pilosa.ErrTransactionExclusive:
			w.WriteHeader(http.StatusConflict)
		case pilosa.ErrTransactionNotFound:
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}

	var errString string
	if err != nil {
		errString = err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(
		TransactionResponse{Error: errString, Transaction: trns})
	if err != nil {
		h.logger.Printf("encoding transaction response: %v", err)
	}

}

func (h *Handler) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	id := mux.Vars(r)["id"]
	trns, err := h.api.GetTransaction(r.Context(), id, false)
	h.doTransactionResponse(w, err, trns)
}

func (h *Handler) handlePostTransaction(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	reqTrns := &pilosa.Transaction{}
	if err := json.NewDecoder(r.Body).Decode(reqTrns); err != nil || reqTrns.Timeout == 0 {
		if err == nil {
			http.Error(w, "timeout is required and cannot be 0", http.StatusBadRequest)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	id, ok := mux.Vars(r)["id"]
	if !ok {
		id = reqTrns.ID
	}
	trns, err := h.api.StartTransaction(r.Context(), id, reqTrns.Timeout, reqTrns.Exclusive, false)

	h.doTransactionResponse(w, err, trns)
}

func (h *Handler) handlePostFinishTransaction(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	id := mux.Vars(r)["id"]
	trns, err := h.api.FinishTransaction(r.Context(), id, false)
	h.doTransactionResponse(w, err, trns)
}

// handleDeleteRemoteAvailableShard handles DELETE /field/{field}/available-shards/{shardID} request.
func (h *Handler) handleDeleteRemoteAvailableShard(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	indexName := mux.Vars(r)["index"]
	fieldName := mux.Vars(r)["field"]
	shardID, _ := strconv.ParseUint(mux.Vars(r)["shardID"], 10, 64)

	resp := successResponse{h: h}
	err := h.api.DeleteAvailableShard(r.Context(), indexName, fieldName, shardID)
	resp.write(w, err)
}

// handlePostFieldAttrDiff handles POST /internal/field/attr/diff requests.
func (h *Handler) handlePostFieldAttrDiff(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	indexName := mux.Vars(r)["index"]
	fieldName := mux.Vars(r)["field"]

	// Decode request.
	var req postFieldAttrDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	attrs, err := h.api.FieldAttrDiff(r.Context(), indexName, fieldName, req.Blocks)
	if err != nil {
		switch errors.Cause(err) {
		case pilosa.ErrFragmentNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(postFieldAttrDiffResponse{
		Attrs: attrs,
	}); err != nil {
		h.logger.Printf("response encoding error: %s", err)
	}
}

type postFieldAttrDiffRequest struct {
	Blocks []pilosa.AttrBlock `json:"blocks"`
}

type postFieldAttrDiffResponse struct {
	Attrs map[uint64]map[string]interface{} `json:"attrs"`
}

// readQueryRequest parses an query parameters from r.
func (h *Handler) readQueryRequest(r *http.Request) (*pilosa.QueryRequest, error) {
	switch r.Header.Get("Content-Type") {
	case "application/x-protobuf":
		return h.readProtobufQueryRequest(r)
	default:
		return h.readURLQueryRequest(r)
	}
}

// readProtobufQueryRequest parses query parameters in protobuf from r.
func (h *Handler) readProtobufQueryRequest(r *http.Request) (*pilosa.QueryRequest, error) {
	// Slurp the body.
	body, err := readBody(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	qreq := &pilosa.QueryRequest{}
	err = proto.DefaultSerializer.Unmarshal(body, qreq)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling query request")
	}
	return qreq, nil
}

// readURLQueryRequest parses query parameters from URL parameters from r.
func (h *Handler) readURLQueryRequest(r *http.Request) (*pilosa.QueryRequest, error) {
	q := r.URL.Query()

	// Parse query string.
	buf, err := readBody(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}
	query := string(buf)

	// Parse list of shards.
	shards, err := parseUint64Slice(q.Get("shards"))
	if err != nil {
		return nil, errors.New("invalid shard argument")
	}

	// Optional profiling
	profile := false
	profileString := q.Get("profile")
	if profileString != "" {
		profile, err = strconv.ParseBool(q.Get("profile"))
		if err != nil {
			return nil, fmt.Errorf("invalid profile argument: '%s' (should be true/false)", profileString)
		}
	}

	return &pilosa.QueryRequest{
		Query:           query,
		Shards:          shards,
		Profile:         profile,
		ColumnAttrs:     q.Get("columnAttrs") == "true",
		ExcludeRowAttrs: q.Get("excludeRowAttrs") == "true",
		ExcludeColumns:  q.Get("excludeColumns") == "true",
	}, nil
}

// writeQueryResponse writes the response from the executor to w.
func (h *Handler) writeQueryResponse(w http.ResponseWriter, r *http.Request, resp *pilosa.QueryResponse) error {
	if !validHeaderAcceptJSON(r.Header) {
		w.Header().Set("Content-Type", "application/protobuf")
		return h.writeProtobufQueryResponse(w, resp, headerAcceptRoaringRow(r.Header))
	}
	w.Header().Set("Content-Type", "application/json")
	return h.writeJSONQueryResponse(w, resp)
}

// writeProtobufQueryResponse writes the response from the executor to w as protobuf.
func (h *Handler) writeProtobufQueryResponse(w io.Writer, resp *pilosa.QueryResponse, writeRoaring bool) error {
	serializer := proto.DefaultSerializer
	if writeRoaring {
		serializer = proto.RoaringSerializer
	}
	if buf, err := serializer.Marshal(resp); err != nil {
		return errors.Wrap(err, "marshalling")
	} else if _, err := w.Write(buf); err != nil {
		return errors.Wrap(err, "writing")
	}
	return nil
}

// writeJSONQueryResponse writes the response from the executor to w as JSON.
func (h *Handler) writeJSONQueryResponse(w io.Writer, resp *pilosa.QueryResponse) error {
	return json.NewEncoder(w).Encode(resp)
}

func validateProtobufHeader(r *http.Request) (error string, code int) {
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		return "Unsupported media type", http.StatusUnsupportedMediaType
	}
	if r.Header.Get("Accept") != "application/x-protobuf" {
		return "Not acceptable", http.StatusNotAcceptable
	}
	return
}

// handleGetExport handles /export requests.
func (h *Handler) handleGetExport(w http.ResponseWriter, r *http.Request) {
	switch r.Header.Get("Accept") {
	case "text/csv":
		h.handleGetExportCSV(w, r)
	default:
		http.Error(w, "Not acceptable", http.StatusNotAcceptable)
	}
}

func (h *Handler) handleGetExportCSV(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters.
	q := r.URL.Query()
	index, field := q.Get("index"), q.Get("field")

	shard, err := strconv.ParseUint(q.Get("shard"), 10, 64)
	if err != nil {
		http.Error(w, "invalid shard", http.StatusBadRequest)
		return
	}

	if err = h.api.ExportCSV(r.Context(), index, field, shard, w); err != nil {
		switch errors.Cause(err) {
		case pilosa.ErrFragmentNotFound:
			break
		case pilosa.ErrClusterDoesNotOwnShard:
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
}

// handleGetFragmentNodes handles /internal/fragment/nodes requests.
func (h *Handler) handleGetFragmentNodes(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	q := r.URL.Query()
	index := q.Get("index")

	// Read shard parameter.
	shard, err := strconv.ParseUint(q.Get("shard"), 10, 64)
	if err != nil {
		http.Error(w, "shard should be an unsigned integer", http.StatusBadRequest)
		return
	}

	// Retrieve fragment owner nodes.
	nodes, err := h.api.ShardNodes(r.Context(), index, shard)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Write to response.
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(nodes); err != nil {
		h.logger.Printf("json write error: %s", err)
	}
}

// handleGetNodes handles /internal/nodes requests.
func (h *Handler) handleGetNodes(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	// Retrieve all nodes.
	nodes := h.api.Hosts(r.Context())

	// Write to response.
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(nodes); err != nil {
		h.logger.Printf("json write error: %s", err)
	}
}

// handleGetFragmentBlockData handles GET /internal/fragment/block/data requests.
func (h *Handler) handleGetFragmentBlockData(w http.ResponseWriter, r *http.Request) {
	buf, err := h.api.FragmentBlockData(r.Context(), r.Body)
	if err != nil {
		if _, ok := err.(pilosa.BadRequestError); ok {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if errors.Cause(err) == pilosa.ErrFragmentNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Write response.
	w.Header().Set("Content-Type", "application/protobuf")
	w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	_, err = w.Write(buf)
	if err != nil {
		h.logger.Printf("writing fragment/block/data response: %v", err)
	}
}

// handleGetFragmentBlocks handles GET /internal/fragment/blocks requests.
func (h *Handler) handleGetFragmentBlocks(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	// Read shard parameter.
	q := r.URL.Query()
	shard, err := strconv.ParseUint(q.Get("shard"), 10, 64)
	if err != nil {
		http.Error(w, "shard required", http.StatusBadRequest)
		return
	}

	blocks, err := h.api.FragmentBlocks(r.Context(), q.Get("index"), q.Get("field"), q.Get("view"), shard)
	if err != nil {
		if errors.Cause(err) == pilosa.ErrFragmentNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(getFragmentBlocksResponse{
		Blocks: blocks,
	}); err != nil {
		h.logger.Printf("block response encoding error: %s", err)
	}
}

type getFragmentBlocksResponse struct {
	Blocks []pilosa.FragmentBlock `json:"blocks"`
}

// handleGetFragmentData handles GET /internal/fragment/data requests.
func (h *Handler) handleGetFragmentData(w http.ResponseWriter, r *http.Request) {
	// Read shard parameter.
	q := r.URL.Query()
	shard, err := strconv.ParseUint(q.Get("shard"), 10, 64)
	if err != nil {
		http.Error(w, "shard required", http.StatusBadRequest)
		return
	}
	// Retrieve fragment data from holder.
	f, err := h.api.FragmentData(r.Context(), q.Get("index"), q.Get("field"), q.Get("view"), shard)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	// Stream fragment to response body.
	if _, err := f.WriteTo(w); err != nil {
		h.logger.Printf("error streaming fragment data: %s", err)
	}
}

// handleGetTranslateData handles GET /internal/translate/data requests.
func (h *Handler) handleGetTranslateData(w http.ResponseWriter, r *http.Request) {
	// Read partition parameter.
	q := r.URL.Query()
	partition, err := strconv.ParseUint(q.Get("partition"), 10, 32)
	if err != nil {
		http.Error(w, "partition required", http.StatusBadRequest)
		return
	}
	// Retrieve partition data from holder.
	p, err := h.api.TranslateData(r.Context(), q.Get("index"), int(partition))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	// Stream translate partition to response body.
	if _, err := p.WriteTo(w); err != nil {
		h.logger.Printf("error streaming translation data: %s", err)
	}
}

// handleGetVersion handles /version requests.
func (h *Handler) handleGetVersion(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(struct {
		Version string `json:"version"`
	}{
		Version: h.api.Version(),
	})
	if err != nil {
		h.logger.Printf("write version response error: %s", err)
	}
}

// QueryResult types.
const (
	QueryResultTypeRow uint32 = iota
	QueryResultTypePairs
	QueryResultTypeUint64
)

// parseUint64Slice returns a slice of uint64s from a comma-delimited string.
func parseUint64Slice(s string) ([]uint64, error) {
	var a []uint64
	for _, str := range strings.Split(s, ",") {
		// Ignore blanks.
		if str == "" {
			continue
		}

		// Parse number.
		num, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "parsing int")
		}
		a = append(a, num)
	}
	return a, nil
}

func (h *Handler) handlePostClusterResizeSetCoordinator(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	// Decode request.
	var req setCoordinatorRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "decoding request "+err.Error(), http.StatusBadRequest)
		return
	}

	oldNode, newNode, err := h.api.SetCoordinator(r.Context(), req.ID)
	if err != nil {
		if errors.Cause(err) == pilosa.ErrNodeIDNotExists {
			http.Error(w, "setting new coordinator: "+err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, "setting new coordinator: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	// Encode response.
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(setCoordinatorResponse{
		Old: oldNode,
		New: newNode,
	}); err != nil {
		h.logger.Printf("response encoding error: %s", err)
	}
}

type setCoordinatorRequest struct {
	ID string `json:"id"`
}

type setCoordinatorResponse struct {
	Old *pilosa.Node `json:"old"`
	New *pilosa.Node `json:"new"`
}

// handlePostClusterResizeRemoveNode handles POST /cluster/resize/remove-node request.
func (h *Handler) handlePostClusterResizeRemoveNode(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	// Decode request.
	var req removeNodeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	removeNode, err := h.api.RemoveNode(req.ID)
	if err != nil {
		if errors.Cause(err) == pilosa.ErrNodeIDNotExists {
			http.Error(w, "removing node: "+err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, "removing node: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(removeNodeResponse{
		Remove: removeNode,
	}); err != nil {
		h.logger.Printf("response encoding error: %s", err)
	}
}

type removeNodeRequest struct {
	ID string `json:"id"`
}

type removeNodeResponse struct {
	Remove *pilosa.Node `json:"remove"`
}

// handlePostClusterResizeAbort handles POST /cluster/resize/abort request.
func (h *Handler) handlePostClusterResizeAbort(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	err := h.api.ResizeAbort()
	var msg string
	if err != nil {
		switch errors.Cause(err) {
		case pilosa.ErrNodeNotCoordinator:
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		case pilosa.ErrResizeNotRunning:
			msg = err.Error()
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	// Encode response.
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(clusterResizeAbortResponse{
		Info: msg,
	}); err != nil {
		h.logger.Printf("response encoding error: %s", err)
	}
}

type clusterResizeAbortResponse struct {
	Info string `json:"info"`
}

func (h *Handler) handleRecalculateCaches(w http.ResponseWriter, r *http.Request) {
	err := h.api.RecalculateCaches(r.Context())
	if err != nil {
		http.Error(w, "recalculating caches: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handlePostClusterMessage(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	// Verify that request is only communicating over protobufs.
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	err := h.api.ClusterMessage(r.Context(), r.Body)
	if err != nil {
		switch err := err.(type) {
		case pilosa.MessageProcessingError:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(defaultClusterMessageResponse{}); err != nil {
		h.logger.Printf("response encoding error: %s", err)
	}
}

type defaultClusterMessageResponse struct{}

func (h *Handler) handlePostTranslateData(w http.ResponseWriter, r *http.Request) {
	// Parse offsets for all indexes and fields from POST body.
	offsets := make(pilosa.TranslateOffsetMap)
	if err := json.NewDecoder(r.Body).Decode(&offsets); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Stream all translation data.
	rd, err := h.api.GetTranslateEntryReader(r.Context(), offsets)
	if errors.Cause(err) == pilosa.ErrNotImplemented {
		http.Error(w, err.Error(), http.StatusNotImplemented)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rd.Close()

	// Flush header so client can continue.
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	// Copy from reader to client until store or client disconnect.
	enc := json.NewEncoder(w)
	for {
		// Read from store.
		var entry pilosa.TranslateEntry
		if err := rd.ReadEntry(&entry); err == io.EOF {
			return
		} else if err != nil {
			h.logger.Printf("http: translate store read error: %s", err)
			return
		}

		// Write to response & flush.
		if err := enc.Encode(&entry); err != nil {
			return
		}
		w.(http.Flusher).Flush()
	}
}

type queryValidationSpec struct {
	required []string
	args     map[string]struct{}
}

func queryValidationSpecRequired(requiredArgs ...string) *queryValidationSpec {
	args := map[string]struct{}{}
	for _, arg := range requiredArgs {
		args[arg] = struct{}{}
	}

	return &queryValidationSpec{
		required: requiredArgs,
		args:     args,
	}
}

func (s *queryValidationSpec) Optional(args ...string) *queryValidationSpec {
	for _, arg := range args {
		s.args[arg] = struct{}{}
	}
	return s
}

func (s queryValidationSpec) validate(query url.Values) error {
	for _, req := range s.required {
		if query.Get(req) == "" {
			return errors.Errorf("%s is required", req)
		}
	}
	for k := range query {
		if _, ok := s.args[k]; !ok {
			return errors.Errorf("%s is not a valid argument", k)
		}
	}
	return nil
}

func GetHTTPClient(t *tls.Config) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   200,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if t != nil {
		transport.TLSClientConfig = t
	}
	return &http.Client{Transport: transport}
}

// handlePostImport handles /import requests.
func (h *Handler) handlePostImport(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if error, code := validateProtobufHeader(r); error != "" {
		http.Error(w, error, code)
		return
	}

	// Get index and field type to determine how to handle the
	// import data.
	indexName := mux.Vars(r)["index"]
	index, err := h.api.Index(r.Context(), indexName)
	if err != nil {
		if errors.Cause(err) == pilosa.ErrIndexNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	fieldName := mux.Vars(r)["field"]
	field := index.Field(fieldName)
	if field == nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// If the clear flag is true, treat the import as clear bits.
	q := r.URL.Query()
	doClear := q.Get("clear") == "true"
	doIgnoreKeyCheck := q.Get("ignoreKeyCheck") == "true"

	opts := []pilosa.ImportOption{
		pilosa.OptImportOptionsClear(doClear),
		pilosa.OptImportOptionsIgnoreKeyCheck(doIgnoreKeyCheck),
	}

	// Read entire body.
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Unmarshal request based on field type.
	if field.Type() == pilosa.FieldTypeInt || field.Type() == pilosa.FieldTypeDecimal {
		// Field type: Int
		// Marshal into request object.
		req := &pilosa.ImportValueRequest{}
		if err := proto.DefaultSerializer.Unmarshal(body, req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := h.api.ImportValue(r.Context(), req, opts...); err != nil {
			switch errors.Cause(err) {
			case pilosa.ErrClusterDoesNotOwnShard, pilosa.ErrPreconditionFailed:
				http.Error(w, err.Error(), http.StatusPreconditionFailed)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
	} else {
		// Field type: set, time, mutex
		// Marshal into request object.
		req := &pilosa.ImportRequest{}
		if err := proto.DefaultSerializer.Unmarshal(body, req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := h.api.Import(r.Context(), req, opts...); err != nil {
			switch errors.Cause(err) {
			case pilosa.ErrClusterDoesNotOwnShard, pilosa.ErrPreconditionFailed:
				http.Error(w, err.Error(), http.StatusPreconditionFailed)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
	}

	// Write response.
	_, err = w.Write(importOk)
	if err != nil {
		h.logger.Printf("writing import response: %v", err)
	}
}

// handlePostImportColumnAttrs
func (h *Handler) handlePostImportColumnAttrs(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	} else if r.Header.Get("Accept") != "application/x-protobuf" {
		http.Error(w, "Not acceptable", http.StatusNotAcceptable)
		return
	}

	opts := []pilosa.ImportOption{}

	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	req := &pilosa.ImportColumnAttrsRequest{}
	if err := proto.DefaultSerializer.Unmarshal(body, req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.api.ImportColumnAttrs(r.Context(), req, opts...); err != nil {
		switch errors.Cause(err) {
		case pilosa.ErrClusterDoesNotOwnShard, pilosa.ErrPreconditionFailed:
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Write response.
	_, err = w.Write(importOk)
	if err != nil {
		h.logger.Printf("writing import-column-attrs response: %v", err)
	}
}

// handlePostImportRoaring
func (h *Handler) handlePostImportRoaring(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if error, code := validateProtobufHeader(r); error != "" {
		http.Error(w, error, code)
		return
	}

	// Get index and field type to determine how to handle the
	// import data.
	indexName := mux.Vars(r)["index"]
	fieldName := mux.Vars(r)["field"]

	q := r.URL.Query()
	remoteStr := q.Get("remote")
	var remote bool
	if remoteStr == "true" {
		remote = true
	}

	ctx := r.Context()

	// Read entire body.
	span, _ := tracing.StartSpanFromContext(ctx, "ioutil.ReadAll-Body")
	body, err := readBody(r)
	span.LogKV("bodySize", len(body))
	span.Finish()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	req := &pilosa.ImportRoaringRequest{}
	span, _ = tracing.StartSpanFromContext(ctx, "Unmarshal")
	err = proto.DefaultSerializer.Unmarshal(body, req)
	span.Finish()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	urlVars := mux.Vars(r)
	shard, err := strconv.ParseUint(urlVars["shard"], 10, 64)
	if err != nil {
		http.Error(w, "shard should be an unsigned integer", http.StatusBadRequest)
		return
	}
	resp := &pilosa.ImportResponse{}
	// TODO give meaningful stats for import
	err = h.api.ImportRoaring(ctx, indexName, fieldName, shard, remote, req)
	if err != nil {
		resp.Err = err.Error()
		if _, ok := err.(pilosa.BadRequestError); ok {
			w.WriteHeader(http.StatusBadRequest)
		} else if _, ok := err.(pilosa.NotFoundError); ok {
			w.WriteHeader(http.StatusNotFound)
		} else if _, ok := err.(pilosa.PreconditionFailedError); ok {
			w.WriteHeader(http.StatusPreconditionFailed)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}

	// Marshal response object.
	buf, err := proto.DefaultSerializer.Marshal(resp)
	if err != nil {
		http.Error(w, fmt.Sprintf("marshal import-roaring response: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response.
	_, err = w.Write(buf)
	if err != nil {
		h.logger.Printf("writing import-roaring response: %v", err)
		return
	}
}

func (h *Handler) handlePostTranslateKeys(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	} else if r.Header.Get("Accept") != "application/x-protobuf" {
		http.Error(w, "Not acceptable", http.StatusNotAcceptable)
		return
	}

	buf, err := h.api.TranslateKeys(r.Context(), r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("translate keys: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response.
	_, err = w.Write(buf)
	if err != nil {
		h.logger.Printf("writing translate keys response: %v", err)
		return
	}
}

func (h *Handler) handlePostTranslateIDs(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	} else if r.Header.Get("Accept") != "application/x-protobuf" {
		http.Error(w, "Not acceptable", http.StatusNotAcceptable)
		return
	}
	buf, err := h.api.TranslateIDs(r.Context(), r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("translate ids: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response.
	_, err = w.Write(buf)
	if err != nil {
		h.logger.Printf("writing translate keys response: %v", err)
	}
}

// Read entire request body.
func readBody(r *http.Request) ([]byte, error) {
	var contentLength int64 = bytes.MinRead
	if r.ContentLength > 0 {
		contentLength = r.ContentLength
	}

	buf := bytes.NewBuffer(make([]byte, 0, 1+contentLength))
	if _, err := buf.ReadFrom(r.Body); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (h *Handler) handlePostTranslateFieldDB(w http.ResponseWriter, r *http.Request) {
	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}

	fieldName, ok := mux.Vars(r)["field"]
	if !ok {
		http.Error(w, "field name is required", http.StatusBadRequest)
		return
	}
	bd, err := readBody(r)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	br := bytes.NewReader(bd)

	err = h.api.TranslateFieldDB(r.Context(), indexName, fieldName, br)
	resp := successResponse{h: h, Name: fieldName}
	resp.check(err)
	resp.write(w, err)
}

func (h *Handler) handlePostTranslateIndexDB(w http.ResponseWriter, r *http.Request) {
	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}

	partitionArg, ok := mux.Vars(r)["partition"]
	if !ok {
		http.Error(w, "partition is required", http.StatusBadRequest)
		return
	}
	partition, err := strconv.ParseUint(partitionArg, 10, 64)
	if err != nil {
		http.Error(w, "bad partition", http.StatusBadRequest)
		return
	}

	bd, err := readBody(r)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	br := bytes.NewReader(bd)
	err = h.api.TranslateIndexDB(r.Context(), indexName, int(partition), br)
	resp := successResponse{h: h, Name: indexName}
	resp.check(err)
	resp.write(w, err)
}
