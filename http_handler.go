// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"encoding/hex"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/felixge/fgprof"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/featurebasedb/featurebase/v3/authn"
	"github.com/featurebasedb/featurebase/v3/authz"
	fbcontext "github.com/featurebasedb/featurebase/v3/context"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/monitor"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/rbf"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/storage"
	"github.com/featurebasedb/featurebase/v3/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prom2json"
	uuid "github.com/satori/go.uuid"
	"github.com/zeebo/blake3"
)

const (
	// HeaderRequestUserID is request userid header
	HeaderRequestUserID = "X-Request-Userid"
)

// Handler represents an HTTP handler.
type Handler struct {
	Handler http.Handler

	fileSystem FileSystem

	logger logger.Logger

	queryLogger logger.Logger

	// Keeps the query argument validators for each handler
	validators map[string]*queryValidationSpec

	api *API

	ln net.Listener
	// url is used to hold the advertise bind address for printing a log during startup.
	url string

	closeTimeout time.Duration

	serializer        Serializer
	roaringSerializer Serializer

	server *http.Server

	middleware []func(http.Handler) http.Handler

	pprofCPUProfileBuffer *bytes.Buffer

	auth *authn.Auth

	permissions *authz.GroupPermissions

	// sqlEnabled is serving as a feature flag for turning on/off the /sql
	// endpoint.
	sqlEnabled bool
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

const (
	// OriginalIPHeader is the original IP for client
	// It is used mainly for authenticating on remote nodes
	// ForwardedIPHeader gets updated to the node's IP
	// when requests are forward to other nodes in the cluster
	OriginalIPHeader = "X-Molecula-Original-IP"

	// ForwardedIPHeader is part of the standard header
	// it is used to identify the originating IP of a client
	ForwardedIPHeader = "X-Forwarded-For"

	// AllowedNetworksGroupName is used for the admin group authorization
	// when authentication is completed through checking the client IP
	// against the allowed networks
	AllowedNetworksGroupName = "allowed-networks"
)

type errorResponse struct {
	Error string `json:"error"`
}

// handlerOption is a functional option type for Handler
type handlerOption func(s *Handler) error

func OptHandlerMiddleware(middleware func(http.Handler) http.Handler) handlerOption {
	return func(h *Handler) error {
		h.middleware = append(h.middleware, middleware)
		return nil
	}
}

func OptHandlerAllowedOrigins(origins []string) handlerOption {
	return func(h *Handler) error {
		h.middleware = append(h.middleware, handlers.CORS(
			handlers.AllowedOrigins(origins),
			handlers.AllowedHeaders([]string{"Content-Type"}),
		))
		return nil
	}
}

func OptHandlerAPI(api *API) handlerOption {
	return func(h *Handler) error {
		h.api = api
		return nil
	}
}

func OptHandlerAuthN(authn *authn.Auth) handlerOption {
	return func(h *Handler) error {
		h.auth = authn
		return nil
	}
}

func OptHandlerAuthZ(gp *authz.GroupPermissions) handlerOption {
	return func(h *Handler) error {
		h.permissions = gp
		return nil
	}
}

func OptHandlerFileSystem(fs FileSystem) handlerOption {
	return func(h *Handler) error {
		h.fileSystem = fs
		return nil
	}
}

func OptHandlerLogger(logger logger.Logger) handlerOption {
	return func(h *Handler) error {
		h.logger = logger
		return nil
	}
}

func OptHandlerQueryLogger(logger logger.Logger) handlerOption {
	return func(h *Handler) error {
		h.queryLogger = logger
		return nil
	}
}

func OptHandlerSerializer(s Serializer) handlerOption {
	return func(h *Handler) error {
		h.serializer = s
		return nil
	}
}

func OptHandlerRoaringSerializer(s Serializer) handlerOption {
	return func(h *Handler) error {
		h.roaringSerializer = s
		return nil
	}
}

// OptHandlerListener set the listener that will be used by the HTTP server.
// Url must be the advertised URL. It will be used to show a log to the user
// about where the Web UI is. This option is mandatory.
func OptHandlerListener(ln net.Listener, url string) handlerOption {
	return func(h *Handler) error {
		h.ln = ln
		h.url = url
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

// OptHandlerSQLEnabled enables the /sql endpoint.
func OptHandlerSQLEnabled(v bool) handlerOption {
	return func(h *Handler) error {
		h.sqlEnabled = v
		return nil
	}
}

var (
	makeImportOk sync.Once
	importOk     []byte
)

// NewHandler returns a new instance of Handler with a default logger.
func NewHandler(opts ...handlerOption) (*Handler, error) {
	handler := &Handler{
		fileSystem:   NopFileSystem,
		logger:       logger.NopLogger,
		closeTimeout: time.Second * 30,
	}

	for _, opt := range opts {
		err := opt(handler)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}
	if handler.serializer == nil || handler.roaringSerializer == nil {
		return nil, errors.New("must use serializer options when creating handler")
	}
	makeImportOk.Do(func() {
		var err error
		importOk, err = handler.serializer.Marshal(&ImportResponse{Err: ""})
		if err != nil {
			panic(fmt.Sprintf("trying to cache import-OK response: %v", err))
		}
	})

	// if OptHandlerFileSystem is used, it must be before newRouter is called
	handler.Handler = newRouter(handler)
	handler.populateValidators()

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

func (h *Handler) populateValidators() {
	h.validators = map[string]*queryValidationSpec{}
	h.validators["GetExport"] = queryValidationSpecRequired("index", "field", "shard")
	h.validators["GetIndexes"] = queryValidationSpecRequired()
	h.validators["GetIndex"] = queryValidationSpecRequired()
	h.validators["PostIndex"] = queryValidationSpecRequired()
	h.validators["DeleteIndex"] = queryValidationSpecRequired()
	h.validators["GetTranslateData"] = queryValidationSpecRequired("index").Optional("partition", "field")
	h.validators["PostTranslateKeys"] = queryValidationSpecRequired()
	h.validators["PostField"] = queryValidationSpecRequired()
	h.validators["DeleteField"] = queryValidationSpecRequired()
	h.validators["PostImport"] = queryValidationSpecRequired().Optional("clear", "ignoreKeyCheck")
	h.validators["PostImportAtomicRecord"] = queryValidationSpecRequired().Optional("simPowerLossAfter")
	h.validators["PostImportRoaring"] = queryValidationSpecRequired().Optional("remote", "clear")
	h.validators["PostQuery"] = queryValidationSpecRequired().Optional("shards", "excludeColumns", "profile", "remote")
	h.validators["GetInfo"] = queryValidationSpecRequired()
	h.validators["RecalculateCaches"] = queryValidationSpecRequired()
	h.validators["GetSchema"] = queryValidationSpecRequired().Optional("views")
	h.validators["PostSchema"] = queryValidationSpecRequired().Optional("remote")
	h.validators["GetStatus"] = queryValidationSpecRequired()
	h.validators["GetVersion"] = queryValidationSpecRequired()
	h.validators["PostClusterMessage"] = queryValidationSpecRequired()
	h.validators["GetFragmentBlockData"] = queryValidationSpecRequired()
	h.validators["GetFragmentBlocks"] = queryValidationSpecRequired("index", "field", "view", "shard")
	h.validators["GetFragmentData"] = queryValidationSpecRequired("index", "field", "view", "shard")
	h.validators["GetFragmentNodes"] = queryValidationSpecRequired("shard", "index")
	h.validators["GetPartitionNodes"] = queryValidationSpecRequired("partition")
	h.validators["GetNodes"] = queryValidationSpecRequired()
	h.validators["GetShardMax"] = queryValidationSpecRequired()
	h.validators["GetTransactionList"] = queryValidationSpecRequired()
	h.validators["GetTransactions"] = queryValidationSpecRequired()
	h.validators["GetTransaction"] = queryValidationSpecRequired()
	h.validators["PostTransaction"] = queryValidationSpecRequired()
	h.validators["PostFinishTransaction"] = queryValidationSpecRequired()
	h.validators["DeleteDataframe"] = queryValidationSpecRequired()
}

type contextKeyQuery int

const (
	contextKeyQueryRequest contextKeyQuery = iota
	contextKeyQueryError
	contextKeyGroupMembership
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
						h.logger.Errorf("failed to encode error %q as JSON: %v", errText, err)
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
			if req, ok := queryRequest.(*QueryRequest); ok {
				queryString = req.Query
			}

			h.logger.Printf("HTTP query duration %v exceeds %v: %s %s %s", dur, longQueryTime, r.Method, r.URL.String(), queryString)
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
			stats.Timing(MetricHTTPRequest, dur, 0.1)
		}
	})
}

func (h *Handler) monitorPerformance(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !monitor.IsOn() {
			next.ServeHTTP(w, r)
			return
		}
		// %% begin sonarcloud ignore %%
		prefixes := make(map[string]struct{})
		prefixes["index"] = struct{}{}
		prefixes["info"] = struct{}{}
		prefixes["schema"] = struct{}{}
		prefixes["status"] = struct{}{}
		prefixes["queries"] = struct{}{}
		prefixes["query-history"] = struct{}{}

		pathParts := strings.Split(r.URL.Path, "/")
		// checks < 5 to exclude import endpoints for now from sentry transactions as
		// there could potentially be many of them. Pricing is per transaction.
		if len(pathParts) > 1 && len(pathParts) < 5 {
			if _, ok := prefixes[pathParts[1]]; ok {
				path := scrubPath(pathParts)
				span := monitor.StartSpan(r.Context(), "HTTP", path)
				qreq := r.Context().Value(contextKeyQueryRequest)
				req, ok := qreq.(*QueryRequest)
				if ok && req != nil {
					span.SetTag("PQL Query", req.Query)
					span.SetTag("SQL Query", req.SQLQuery)
					span.SetTag("Index", mux.Vars(r)["index"])
				}
				next.ServeHTTP(w, r)
				span.Finish()
				return
			}
		}
		next.ServeHTTP(w, r)
		// %% end sonarcloud ignore %%
	})
}

func scrubPath(pathParts []string) string {
	l := len(pathParts)
	if l > 1 && pathParts[1] == "index" {
		switch {
		case l > 4:
			pathParts[4] = "{field}"
			fallthrough
		case l > 2:
			pathParts[2] = "{index}"
		}
	}
	return strings.Join(pathParts, "/")
}

// latticeRoutes lists the frontend routes that do not directly correspond to
// backend routes, and require special handling.
var latticeRoutes = []string{"/tables", "/query", "/querybuilder", "/signin"} // TODO somehow pull this from some metadata in the lattice directory

// newRouter creates a new mux http router.
func newRouter(handler *Handler) http.Handler {
	router := mux.NewRouter()

	// TODO: figure out how to protect these if needed
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux).Methods("GET")
	router.PathPrefix("/debug/fgprof").Handler(fgprof.Handler()).Methods("GET")
	router.Handle("/debug/vars", expvar.Handler()).Methods("GET")
	router.Handle("/metrics", promhttp.Handler())

	router.HandleFunc("/metrics.json", handler.chkAuthZ(handler.handleGetMetricsJSON, authz.Admin)).Methods("GET").Name("GetMetricsJSON")
	router.HandleFunc("/export", handler.chkAuthZ(handler.handleGetExport, authz.Read)).Methods("GET").Name("GetExport")
	router.HandleFunc("/import-atomic-record", handler.chkAuthZ(handler.handlePostImportAtomicRecord, authz.Admin)).Methods("POST").Name("PostImportAtomicRecord")
	router.HandleFunc("/index", handler.chkAuthZ(handler.handleGetIndexes, authz.Read)).Methods("GET").Name("GetIndexes")
	router.HandleFunc("/index", handler.chkAuthZ(handler.handlePostIndex, authz.Admin)).Methods("POST").Name("PostIndex")
	router.HandleFunc("/index/", handler.chkAuthZ(handler.handlePostIndex, authz.Admin)).Methods("POST").Name("PostIndex")
	router.HandleFunc("/index/{index}", handler.chkAuthZ(handler.handleGetIndex, authz.Read)).Methods("GET").Name("GetIndex")
	router.HandleFunc("/index/{index}", handler.chkAuthZ(handler.handlePostIndex, authz.Admin)).Methods("POST").Name("PostIndex")
	router.HandleFunc("/index/{index}", handler.chkAuthZ(handler.handleDeleteIndex, authz.Admin)).Methods("DELETE").Name("DeleteIndex")
	router.HandleFunc("/index/{index}/dataframe/{shard}", handler.chkAuthZ(handler.handlePostDataframe, authz.Write)).Methods("POST").Name("PostDataframe")
	router.HandleFunc("/index/{index}/dataframe/{shard}", handler.chkAuthZ(handler.handleGetDataframe, authz.Read)).Methods("GET").Name("GetDataframe")
	router.HandleFunc("/index/{index}/dataframe", handler.chkAuthZ(handler.handleGetDataframeSchema, authz.Read)).Methods("GET").Name("GetDataframeSchema")
	router.HandleFunc("/index/{index}/dataframe", handler.chkAuthZ(handler.handleDeleteDataframe, authz.Write)).Methods("DELETE").Name("DeleteDataframe")
	router.HandleFunc("/index/{index}/field", handler.chkAuthZ(handler.handlePostField, authz.Write)).Methods("POST").Name("PostField")
	router.HandleFunc("/index/{index}/field/", handler.chkAuthZ(handler.handlePostField, authz.Write)).Methods("POST").Name("PostField")
	router.HandleFunc("/index/{index}/field/{field}/view", handler.chkAuthZ(handler.handleGetView, authz.Admin)).Methods("GET")
	router.HandleFunc("/index/{index}/field/{field}/view/{view}", handler.chkAuthZ(handler.handleDeleteView, authz.Admin)).Methods("DELETE").Name("DeleteView")
	router.HandleFunc("/index/{index}/field/{field}", handler.chkAuthZ(handler.handlePostField, authz.Write)).Methods("POST").Name("PostField")
	router.HandleFunc("/index/{index}/field/{field}", handler.chkAuthZ(handler.handlePatchField, authz.Write)).Methods("PATCH").Name("PatchField")
	router.HandleFunc("/index/{index}/field/{field}", handler.chkAuthZ(handler.handleDeleteField, authz.Write)).Methods("DELETE").Name("DeleteField")
	router.HandleFunc("/index/{index}/field/{field}/import", handler.chkAuthZ(handler.handlePostImport, authz.Write)).Methods("POST").Name("PostImport")
	router.HandleFunc("/index/{index}/field/{field}/mutex-check", handler.chkAuthZ(handler.handleGetMutexCheck, authz.Read)).Methods("GET").Name("GetMutexCheck")
	router.HandleFunc("/index/{index}/field/{field}/import-roaring/{shard}", handler.chkAuthZ(handler.handlePostImportRoaring, authz.Write)).Methods("POST").Name("PostImportRoaring")
	router.HandleFunc("/index/{index}/shard/{shard}/import-roaring", handler.chkAuthZ(handler.handlePostShardImportRoaring, authz.Write)).Methods("POST").Name("PostImportRoaring")
	router.HandleFunc("/index/{index}/query", handler.chkAuthZ(handler.handlePostQuery, authz.Read)).Methods("POST").Name("PostQuery")
	router.HandleFunc("/info", handler.chkAuthZ(handler.handleGetInfo, authz.Admin)).Methods("GET").Name("GetInfo")
	router.HandleFunc("/recalculate-caches", handler.chkAuthZ(handler.handleRecalculateCaches, authz.Admin)).Methods("POST").Name("RecalculateCaches")
	router.HandleFunc("/schema", handler.chkAuthZ(handler.handleGetSchema, authz.Read)).Methods("GET").Name("GetSchema")
	router.HandleFunc("/schema/details", handler.chkAuthZ(handler.handleGetSchemaDetails, authz.Read)).Methods("GET").Name("GetSchemaDetails")
	router.HandleFunc("/schema", handler.chkAuthZ(handler.handlePostSchema, authz.Admin)).Methods("POST").Name("PostSchema")
	router.HandleFunc("/status", handler.chkAuthZ(handler.handleGetStatus, authz.Read)).Methods("GET").Name("GetStatus")
	router.HandleFunc("/transaction", handler.chkAuthZ(handler.handlePostTransaction, authz.Read)).Methods("POST").Name("PostTransaction")
	router.HandleFunc("/transaction/", handler.chkAuthZ(handler.handlePostTransaction, authz.Read)).Methods("POST").Name("PostTransaction")
	router.HandleFunc("/transaction/{id}", handler.chkAuthZ(handler.handleGetTransaction, authz.Read)).Methods("GET").Name("GetTransaction")
	router.HandleFunc("/transaction/{id}", handler.chkAuthZ(handler.handlePostTransaction, authz.Read)).Methods("POST").Name("PostTransaction")
	router.HandleFunc("/transaction/{id}/finish", handler.chkAuthZ(handler.handlePostFinishTransaction, authz.Read)).Methods("POST").Name("PostFinishTransaction")
	router.HandleFunc("/transactions", handler.chkAuthZ(handler.handleGetTransactions, authz.Read)).Methods("GET").Name("GetTransactions")
	router.HandleFunc("/queries", handler.chkAuthZ(handler.handleGetActiveQueries, authz.Admin)).Methods("GET").Name("GetActiveQueries")

	// enable this endpoint based on config
	if handler.sqlEnabled {
		router.HandleFunc("/sql", handler.chkAuthZ(handler.handlePostSQL, authz.Admin)).Methods("POST").Name("PostSQL")
	}

	router.HandleFunc("/query-history", handler.chkAuthZ(handler.handleGetPastQueries, authz.Admin)).Methods("GET").Name("GetPastQueries")
	router.HandleFunc("/version", handler.handleGetVersion).Methods("GET").Name("GetVersion")

	// /ui endpoints are for UI use; they may change at any time.
	router.HandleFunc("/ui/transaction", handler.chkAuthZ(handler.handleGetTransactionList, authz.Read)).Methods("GET").Name("GetTransactionList")
	router.HandleFunc("/ui/transaction/", handler.chkAuthZ(handler.handleGetTransactionList, authz.Read)).Methods("GET").Name("GetTransactionList")
	router.HandleFunc("/ui/shard-distribution", handler.chkAuthZ(handler.handleGetShardDistribution, authz.Admin)).Methods("GET").Name("GetShardDistribution")

	// /internal endpoints are for internal use only; they may change at any time.
	// DO NOT rely on these for external applications!

	// Truly used internally by featurebase
	router.HandleFunc("/internal/cluster/message", handler.chkInternal(handler.handlePostClusterMessage)).Methods("POST").Name("PostClusterMessage")
	router.HandleFunc("/internal/translate/data", handler.chkAuthZ(handler.handleGetTranslateData, authz.Read)).Methods("GET").Name("GetTranslateData")
	router.HandleFunc("/internal/translate/data", handler.chkAuthZ(handler.handlePostTranslateData, authz.Write)).Methods("POST").Name("PostTranslateData")

	// other ones
	router.HandleFunc("/internal/mem-usage", handler.chkAuthZ(handler.handleGetMemUsage, authz.Read)).Methods("GET").Name("GetUsage")
	router.HandleFunc("/internal/disk-usage", handler.chkAuthZ(handler.handleGetDiskUsage, authz.Read)).Methods("GET").Name("GetUsage")
	router.HandleFunc("/internal/disk-usage/{index}", handler.chkAuthZ(handler.handleGetDiskUsage, authz.Read)).Methods("GET").Name("GetUsage")
	router.HandleFunc("/internal/fragment/block/data", handler.chkAuthN(handler.handleGetFragmentBlockData)).Methods("GET").Name("GetFragmentBlockData")
	router.HandleFunc("/internal/fragment/blocks", handler.chkAuthN(handler.handleGetFragmentBlocks)).Methods("GET").Name("GetFragmentBlocks")
	router.HandleFunc("/internal/fragment/data", handler.chkAuthN(handler.handleGetFragmentData)).Methods("GET").Name("GetFragmentData")
	router.HandleFunc("/internal/fragment/nodes", handler.chkAuthN(handler.handleGetFragmentNodes)).Methods("GET").Name("GetFragmentNodes")
	router.HandleFunc("/internal/partition/nodes", handler.chkAuthN(handler.handleGetPartitionNodes)).Methods("GET").Name("GetPartitionNodes")
	router.HandleFunc("/internal/translate/keys", handler.chkAuthN(handler.handlePostTranslateKeys)).Methods("POST").Name("PostTranslateKeys")
	router.HandleFunc("/internal/translate/ids", handler.chkAuthN(handler.handlePostTranslateIDs)).Methods("POST").Name("PostTranslateIDs")
	router.HandleFunc("/internal/index/{index}/field/{field}/mutex-check", handler.chkAuthZ(handler.handleInternalGetMutexCheck, authz.Read)).Methods("GET").Name("InternalGetMutexCheck")
	router.HandleFunc("/internal/index/{index}/field/{field}/remote-available-shards/{shardID}", handler.chkAuthZ(handler.handleDeleteRemoteAvailableShard, authz.Admin)).Methods("DELETE")
	router.HandleFunc("/internal/index/{index}/shard/{shard}/snapshot", handler.chkAuthZ(handler.handleGetIndexShardSnapshot, authz.Read)).Methods("GET").Name("GetIndexShardSnapshot")
	router.HandleFunc("/internal/index/{index}/shards", handler.chkAuthZ(handler.handleGetIndexAvailableShards, authz.Read)).Methods("GET").Name("GetIndexAvailableShards")
	router.HandleFunc("/internal/nodes", handler.chkAuthN(handler.handleGetNodes)).Methods("GET").Name("GetNodes")
	router.HandleFunc("/internal/shards/max", handler.chkAuthN(handler.handleGetShardsMax)).Methods("GET").Name("GetShardsMax") // TODO: deprecate, but it's being used by the client

	router.HandleFunc("/internal/translate/index/{index}/keys/find", handler.chkAuthZ(handler.handleFindIndexKeys, authz.Admin)).Methods("POST").Name("FindIndexKeys")
	router.HandleFunc("/internal/translate/index/{index}/keys/create", handler.chkAuthZ(handler.handleCreateIndexKeys, authz.Admin)).Methods("POST").Name("CreateIndexKeys")
	router.HandleFunc("/internal/translate/index/{index}/{partition}", handler.chkAuthZ(handler.handlePostTranslateIndexDB, authz.Admin)).Methods("POST").Name("PostTranslateIndexDB")
	router.HandleFunc("/internal/translate/field/{index}/{field}", handler.chkAuthZ(handler.handlePostTranslateFieldDB, authz.Admin)).Methods("POST").Name("PostTranslateFieldDB")
	router.HandleFunc("/internal/translate/field/{index}/{field}/keys/find", handler.chkAuthZ(handler.handleFindFieldKeys, authz.Admin)).Methods("POST").Name("FindFieldKeys")
	router.HandleFunc("/internal/translate/field/{index}/{field}/keys/create", handler.chkAuthZ(handler.handleCreateFieldKeys, authz.Admin)).Methods("POST").Name("CreateFieldKeys")
	router.HandleFunc("/internal/translate/field/{index}/{field}/keys/like", handler.chkAuthZ(handler.handleMatchField, authz.Read)).Methods("POST").Name("MatchFieldKeys")

	router.HandleFunc("/internal/idalloc/reserve", handler.chkAuthN(handler.handleReserveIDs)).Methods("POST").Name("ReserveIDs")
	router.HandleFunc("/internal/idalloc/commit", handler.chkAuthN(handler.handleCommitIDs)).Methods("POST").Name("CommitIDs")
	router.HandleFunc("/internal/idalloc/restore", handler.chkAuthN(handler.handleRestoreIDAlloc)).Methods("POST").Name("RestoreIDAllocData")
	router.HandleFunc("/internal/idalloc/reset/{index}", handler.chkAuthN(handler.handleResetIDAlloc)).Methods("POST").Name("ResetIDAlloc")
	router.HandleFunc("/internal/idalloc/data", handler.chkAuthN(handler.handleIDAllocData)).Methods("GET").Name("IDAllocData")

	router.HandleFunc("/internal/restore/{index}/{shardID}", handler.chkAuthZ(handler.handlePostRestore, authz.Admin)).Methods("POST").Name("Restore")
	router.HandleFunc("/internal/dataframe/restore/{index}/{shardID}", handler.chkAuthZ(handler.handlePostDataframeRestore, authz.Admin)).Methods("POST").Name("Restore")

	router.HandleFunc("/internal/debug/rbf", handler.chkAuthZ(handler.handleGetInternalDebugRBFJSON, authz.Admin)).Methods("GET").Name("GetInternalDebugRBFJSON")

	// endpoints for collecting cpu profiles from a chosen begin point to
	// when the client wants to stop. Used for profiling imports that
	// could be long or short.
	router.HandleFunc("/cpu-profile/start", handler.chkAuthZ(handler.handleCPUProfileStart, authz.Admin)).Methods("GET").Name("CPUProfileStart")
	router.HandleFunc("/cpu-profile/stop", handler.chkAuthZ(handler.handleCPUProfileStop, authz.Admin)).Methods("GET").Name("CPUProfileStop")

	router.HandleFunc("/login", handler.handleLogin).Methods("GET").Name("Login")
	router.HandleFunc("/logout", handler.handleLogout).Methods("GET").Name("Logout")
	router.HandleFunc("/redirect", handler.handleRedirect).Methods("GET").Name("Redirect")
	router.HandleFunc("/auth", handler.handleCheckAuthentication).Methods("GET").Name("CheckAuthentication")
	router.HandleFunc("/userinfo", handler.handleUserInfo).Methods("GET").Name("UserInfo")
	router.HandleFunc("/internal/oauth-config", handler.handleOAuthConfig).Methods("GET").Name("GetOAuthConfig")

	router.HandleFunc("/health", handler.handleGetHealth).Methods("GET").Name("GetHealth")
	router.HandleFunc("/directive", handler.handleGetDirective).Methods("GET").Name("GetDirective")
	router.HandleFunc("/directive", handler.handlePostDirective).Methods("POST").Name("PostDirective")
	router.HandleFunc("/snapshot/shard-data", handler.handlePostSnapshotShardData).Methods("POST").Name("PostShapshotShardData")
	router.HandleFunc("/snapshot/table-keys", handler.handlePostSnapshotTableKeys).Methods("POST").Name("PostShapshotTableKeys")
	router.HandleFunc("/snapshot/field-keys", handler.handlePostSnapshotFieldKeys).Methods("POST").Name("PostShapshotFieldKeys")

	// Endpoints to support lattice UI embedded via statik.
	// The messiness here reflects the fact that assets live in a nontrivial
	// directory structure that is controlled externally.
	latticeHandler := newStatikHandler(handler)
	router.PathPrefix("/static").Handler(latticeHandler)
	router.Path("/").Handler(latticeHandler)
	router.Path("/favicon.png").Handler(latticeHandler)
	router.Path("/favicon.svg").Handler(latticeHandler)
	router.Path("/manifest.json").Handler(latticeHandler)
	for _, route := range latticeRoutes {
		router.Path(route).Handler(latticeHandler)
	}

	router.Use(handler.queryArgValidator)
	router.Use(handler.addQueryContext)
	router.Use(handler.extractTracing)
	router.Use(handler.monitorPerformance)
	router.Use(handler.collectStats)
	var h http.Handler = router
	for _, middleware := range handler.middleware {
		// Ideally, we would use `router.Use` to inject middleware,
		// instead of wrapping the handler. The reason we can't is
		// because the router will only apply middleware to matched
		// handlers. In this case, it won't match handlers with the
		// OPTIONS method, needed by the CORS middleware. This issue
		// is described in detail here:
		// https://github.com/gorilla/handlers/issues/142
		h = middleware(h)
	}
	return h
}

// ServeHTTP handles an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			stack := debug.Stack()
			msg := "%s\n%s"
			h.logger.Panicf(msg, err, stack)
			fmt.Fprintf(w, msg, err, stack)
		}
	}()

	h.Handler.ServeHTTP(w, r)
}

func (h *Handler) chkInternal(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.auth != nil {
			secret, ok := r.Header["X-Feature-Key"]
			var secretString string
			if ok {
				secretString = secret[0]
			}
			decodedString, err := hex.DecodeString(secretString)
			if err != nil || !ok || !bytes.Equal(decodedString, h.auth.SecretKey()) {
				http.Error(w, "internal secret key validation failed", http.StatusUnauthorized)
				return
			}
		}
		handler.ServeHTTP(w, r)
	}
}

func (h *Handler) chkAllowedNetworks(r *http.Request) (bool, context.Context) {
	// for every request, get IP of the request and check against configured IPs
	reqIP := GetIP(r)
	if reqIP == "" {
		return false, r.Context()
	}

	// if client IP is in allowed networks
	// add it to the context for key X-Molecula-Original-IP
	if h.auth.CheckAllowedNetworks(reqIP) {
		ctx := fbcontext.WithOriginalIP(r.Context(), reqIP)
		return true, ctx
	}
	return false, r.Context()
}

func (h *Handler) chkAuthN(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// if the request is unauthenticated and we have the appropriate header get the userid from the header
		requestUserID := r.Header.Get(HeaderRequestUserID)
		ctx = fbcontext.WithUserID(ctx, requestUserID)

		if h.auth == nil {
			handler.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// if IP is in allowed networks, then serve the request
		allowedNetwork, ctx := h.chkAllowedNetworks(r)
		if allowedNetwork {
			handler.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		access, refresh := getTokens(r)
		uinfo, err := h.auth.Authenticate(access, refresh)
		if err != nil {
			http.Error(w, errors.Wrap(err, "authenticating").Error(), http.StatusUnauthorized)
			return
		}

		// prefer the user id from an authenticated request over one in a header
		ctx = fbcontext.WithUserID(ctx, uinfo.UserID)

		// just in case it got refreshed
		ctx = authn.WithAccessToken(ctx, "Bearer"+access)
		ctx = authn.WithRefreshToken(ctx, refresh)
		h.auth.SetCookie(w, uinfo.Token, uinfo.RefreshToken, uinfo.Expiry)

		handler.ServeHTTP(w, r.WithContext(ctx))
	}
}

func (h *Handler) chkAuthZ(handler http.HandlerFunc, perm authz.Permission) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// if the request is unauthenticated and we have the appropriate header get the userid from the header
		requestUserID := r.Header.Get(HeaderRequestUserID)
		ctx = fbcontext.WithUserID(ctx, requestUserID)

		// handle the case when auth is not turned on
		if h.auth == nil {
			handler.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// check if IP is in allowed networks, if yes give it admin permissions
		allowedNetwork, ctx := h.chkAllowedNetworks(r)
		if allowedNetwork {
			ctx = context.WithValue(ctx, contextKeyGroupMembership, []string{AllowedNetworksGroupName, h.permissions.Admin})
			handler.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// make a copy of the requested permissions
		lperm := perm

		// check if the user is authenticated
		access, refresh := getTokens(r)

		uinfo, err := h.auth.Authenticate(access, refresh)
		if err != nil {
			http.Error(w, errors.Wrap(err, "authenticating").Error(), http.StatusForbidden)
			return
		}

		// prefer the user id from an authenticated request over one in a header
		ctx = fbcontext.WithUserID(ctx, uinfo.UserID)

		ctx = authn.WithAccessToken(ctx, "Bearer "+access)
		ctx = authn.WithRefreshToken(ctx, refresh)

		// just in case it got refreshed
		h.auth.SetCookie(w, uinfo.Token, uinfo.RefreshToken, uinfo.Expiry)

		// put the user's authN/Z info in the context
		ctx = context.WithValue(ctx, contextKeyGroupMembership, uinfo.Groups)
		ctx = authn.WithAccessToken(ctx, "Bearer "+uinfo.Token)
		ctx = authn.WithRefreshToken(ctx, uinfo.RefreshToken)
		// unlikely h.permissions will be nil, but we'll check to be safe
		if h.permissions == nil {
			h.logger.Errorf("authentication is turned on without authorization permissions set")
			http.Error(w, "authorizing", http.StatusInternalServerError)
			return
		}

		// figure out what the user is querying for
		queryString := ""
		queryRequest := ctx.Value(contextKeyQueryRequest)
		if req, ok := queryRequest.(*QueryRequest); ok {
			queryString = req.Query

			q, err := pql.ParseString(queryString)
			if err != nil {
				http.Error(w, errors.Wrap(err, "parsing query string").Error(), http.StatusBadRequest)
				return
			}

			// if there are write calls, and the needed perms don't already
			// satisfy write permissions, then make them write permissions
			if q.WriteCallN() > 0 && !lperm.Satisfies(authz.Write) {
				lperm = authz.Write
			}
		}
		// make the query string pretty
		queryString = strings.Replace(queryString, "\n", "", -1)

		// figure out if we should log this query
		toLog := true
		for _, ep := range []string{"/status", "/metrics", "/info", "/internal"} {
			if strings.HasPrefix(r.URL.Path, ep) {
				toLog = false
				break
			}
		}
		if toLog {
			h.queryLogger.Infof("%v, %v, %v, %v, %v, %v", GetIP(r), r.UserAgent(), r.URL.Path, uinfo.UserID, uinfo.UserName, queryString)
		}

		// if they're an admin, they can do whatever they want
		if h.permissions.IsAdmin(uinfo.Groups) {
			handler.ServeHTTP(w, r.WithContext(ctx))
			return
		} else if lperm == authz.Admin {
			// if they're not an admin, and they need to be, we can just
			// error right here
			http.Error(w, "Insufficient permissions: user does not have admin permission", http.StatusForbidden)
			return
		}

		// try to get the index name
		indexName, ok := mux.Vars(r)["index"]
		if !ok {
			indexName = r.URL.Query().Get("index")
		}

		// if we have an index name, then we check the user permissions
		// against that index
		if indexName != "" {
			p, err := h.permissions.GetPermissions(uinfo, indexName)
			if err != nil {
				w.Header().Add("Content-Type", "text/plain")
				http.Error(w, errors.Wrap(err, "Insufficient Permissions").Error(), http.StatusForbidden)
				return
			}

			// if they're not permitted to access this index, error
			if !p.Satisfies(lperm) {
				w.Header().Add("Content-Type", "text/plain")
				http.Error(w, "Insufficient permissions", http.StatusForbidden)
				return
			}
		}
		handler.ServeHTTP(w, r.WithContext(ctx))
	}
}

func GetIP(r *http.Request) string {
	// check if original IP was set in the request
	og := r.Header.Get(OriginalIPHeader)
	if og != "" {
		return og
	}

	// check if original IP is in the context
	if ogIP, ok := fbcontext.OriginalIP(r.Context()); ok && ogIP != "" {
		return ogIP
	}

	// X-Forwarded-For can have multiple IPs
	// the first IP will always be the originating client IP
	// the remaining IPs will be for any proxies the request went through
	forwarded := r.Header.Get(ForwardedIPHeader)
	forwardedList := strings.Split(forwarded, ",")
	if forwardedList[0] != "" {
		return forwardedList[0]
	}

	return r.RemoteAddr
}

// statikHandler implements the http.Handler interface, and responds to
// requests for static assets with the appropriate file contents embedded
// in a statik filesystem.
type statikHandler struct {
	handler  *Handler
	statikFS http.FileSystem
}

// newStatikHandler returns a new instance of statikHandler
func newStatikHandler(h *Handler) statikHandler {
	fs, err := h.fileSystem.New()
	if err == nil {
		h.logger.Printf("enabled Web UI at %s", h.url)
	}

	return statikHandler{
		handler:  h,
		statikFS: fs,
	}
}

func (s statikHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.UserAgent(), "curl") {
		msg := "Welcome. FeatureBase v" + s.handler.api.Version() + " is running. Visit https://docs.featurebase.com for more information."
		if s.statikFS != nil {
			msg += " Try the Web UI by visiting this URL in your browser."
		}
		http.Error(w, msg, http.StatusNotFound)
		return
	}

	if s.statikFS == nil {
		msg := "Web UI is not available. Please run `make generate-statik` before building Pilosa with `make install`."
		s.handler.logger.Infof(msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	// Without this check, refreshing the UI at e.g. /query
	// will request a nonexistent resource and return 404.
	for _, route := range latticeRoutes {
		if r.URL.String() == route {
			url, _ := url.Parse("/")
			r.URL = url
		}
	}

	http.FileServer(s.statikFS).ServeHTTP(w, r)
}

// successResponse is a general success/error struct for http responses.
type successResponse struct {
	h         *Handler
	Success   bool       `json:"success"`
	Name      string     `json:"name,omitempty"`
	CreatedAt int64      `json:"createdAt,omitempty"`
	Error     *HTTPError `json:"error,omitempty"`
}

// HTTPError defines a standard application error.
type HTTPError struct {
	// Human-readable message.
	Message string `json:"message"`
}

// Error returns the string representation of the error message.
func (e *HTTPError) Error() string {
	return e.Message
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
	case BadRequestError:
		statusCode = http.StatusBadRequest
	case ConflictError:
		statusCode = http.StatusConflict
	case NotFoundError:
		// TODO I think any error matches NotFoundError because it's a `type NotFoundError error`
		statusCode = http.StatusNotFound
	default:
		statusCode = http.StatusInternalServerError
	}

	r.Success = false
	r.Error = &HTTPError{Message: err.Error()}

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
			r.h.logger.Errorf("error writing response: %v", err)
			return
		}
		_, err = w.Write([]byte("\n"))
		if err != nil {
			r.h.logger.Errorf("error writing newline after response: %v", err)
			return
		}
	} else {
		http.Error(w, string(msg), statusCode)
	}
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

func (h *Handler) filterSchema(schema []*IndexInfo, g []authn.Group) []*IndexInfo {
	if !h.permissions.IsAdmin(g) {
		var filtered []*IndexInfo
		allowed := h.permissions.GetAuthorizedIndexList(g, authz.Read)
		for _, s := range schema {
			for _, index := range allowed {
				if s.Name == index {
					filtered = append(filtered, s)
					break
				}
			}
		}
		schema = filtered
	}
	return schema
}

func (h *Handler) getGroupMembership(r *http.Request) (g []authn.Group) {
	// if IP is in allowed networks, then give admin group membership
	if h.auth.CheckAllowedNetworks(GetIP(r)) {
		g = []authn.Group{
			{
				GroupID:   h.permissions.Admin,
				GroupName: AllowedNetworksGroupName,
			},
		}
		return g
	}

	// check if group membership was already set in request when auth token was obtained
	g = r.Context().Value(contextKeyGroupMembership).([]authn.Group)
	return g
}

// handleGetSchema handles GET /schema requests.
func (h *Handler) handleGetSchema(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	q := r.URL.Query()
	withViews := q.Get("views") == "true"

	w.Header().Set("Content-Type", "application/json")
	schema, err := h.api.Schema(r.Context(), withViews)
	if err != nil {
		h.logger.Printf("getting schema error: %s", err)
	}

	// if auth is turned on, filter response to only include authorized indexes
	if h.auth != nil {
		g := h.getGroupMembership(r)
		if g == nil {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		schema = h.filterSchema(schema, g)
	}

	if err := json.NewEncoder(w).Encode(Schema{Indexes: schema}); err != nil {
		h.logger.Errorf("write schema response error: %s", err)
	}
}

// handleGetSchema handles GET /schema/details requests. This is essentially the
// same thing as a GET /schema request, except WithViews is turned on by default.
// Previously, /schema/details returned the cardinality of each field, but this was
// removed for performance reasons. If, at some point in the future, there is a more
// performant way to get the cardinality of a field, that information would be
// included here.
func (h *Handler) handleGetSchemaDetails(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	schema, err := h.api.Schema(r.Context(), true)
	if err != nil {
		h.logger.Printf("error getting detailed schema: %s", err)
		return
	}

	// if auth is turned on, filter response to only include authorized indexes
	if h.auth != nil {
		g := h.getGroupMembership(r)
		if g == nil {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		schema = h.filterSchema(schema, g)
	}

	if err := json.NewEncoder(w).Encode(Schema{Indexes: schema}); err != nil {
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

	schema := &Schema{}
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

// handleGetMemUsage handles GET /internal/mem-usage requests.
func (h *Handler) handleGetMemUsage(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	use, err := GetMemoryUsage()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(use); err != nil {
		h.logger.Errorf("write mem usage response error: %s", err)
	}
}

// handleGetDiskUsage handles GET /internal/disk-usage requests.
func (h *Handler) handleGetDiskUsage(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	u := h.api.server.dataDir
	indexName, ok := mux.Vars(r)["index"]
	if ok {
		u = fmt.Sprintf("%s/indexes/%s", u, indexName)
	}

	use, err := GetDiskUsage(u)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(use); err != nil {
		h.logger.Errorf("write disk usage response error: %s", err)
	}
}

// handleGetShardDistribution handles GET /ui/shard-distribution requests.
func (h *Handler) handleGetShardDistribution(w http.ResponseWriter, r *http.Request) {
	dist := h.api.ShardDistribution(r.Context())
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(dist); err != nil {
		h.logger.Errorf("write status response error: %s", err)
	}
}

// handleGetStatus handles GET /status requests.
func (h *Handler) handleGetStatus(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	state, err := h.api.State()
	if err != nil {
		http.Error(w, "getting cluster state error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	status := getStatusResponse{
		State:       string(state),
		Nodes:       h.api.Hosts(r.Context()),
		LocalID:     h.api.Node().ID,
		ClusterName: h.api.ClusterName(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		h.logger.Errorf("write status response error: %s", err)
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
		h.logger.Errorf("write info response error: %s", err)
	}
}

type getSchemaResponse struct {
	Indexes []*IndexInfo `json:"indexes"`
}

type getStatusResponse struct {
	State       string        `json:"state"`
	Nodes       []*disco.Node `json:"nodes"`
	LocalID     string        `json:"localID"`
	ClusterName string        `json:"clusterName"`
}

func httpHash(s string) string {
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
	req, ok := qreq.(*QueryRequest)

	if DoPerQueryProfiling {
		backend := storage.DefaultBackend
		reqHash := httpHash(req.Query)

		qlen := len(req.Query)
		if qlen > 100 {
			qlen = 100
		}
		name := "_query." + reqHash + "." + backend + "." + time.Now().Format("20060102150405") + "." + req.Query[:qlen]
		f, err := os.Create(name)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		_ = pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()

	} // end DoPerQueryProfiling

	var err error
	err, _ = qerr.(error)
	if err != nil || !ok {
		w.WriteHeader(http.StatusBadRequest)
		e := h.writeQueryResponse(w, r, &QueryResponse{Err: err})
		if e != nil {
			h.logger.Errorf("write query response error: %v (while trying to write another error: %v)", e, err)
		}
		return
	}
	// TODO: Remove
	req.Index = mux.Vars(r)["index"]

	resp, err := h.api.Query(r.Context(), req)
	if err != nil {
		switch errors.Cause(err) {
		case ErrTooManyWrites:
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		case ErrTranslateStoreReadOnly:
			u := h.api.PrimaryReplicaNodeURL()
			u.Path, u.RawQuery = r.URL.Path, r.URL.RawQuery
			http.Redirect(w, r, u.String(), http.StatusFound)
			return
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
		e := h.writeQueryResponse(w, r, &QueryResponse{Err: err})
		if e != nil {
			h.logger.Errorf("write query response error: %v (while trying to write another error: %v)", e, err)
		}
		return
	}

	// Set appropriate status code, if there is an error. It doesn't appear that
	// resp.Err could ever be set in API.Query, so this code block is probably
	// doing nothing right now.
	if resp.Err != nil {
		switch errors.Cause(resp.Err) {
		case ErrTooManyWrites:
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}

	// Write response back to client.
	if err := h.writeQueryResponse(w, r, &resp); err != nil {
		h.logger.Errorf("write query response error: %s", err)
	}
}

func (h *Handler) writeBadRequest(w http.ResponseWriter, r *http.Request, err error) {
	w.WriteHeader(http.StatusBadRequest)
	e := h.writeQueryResponse(w, r, &QueryResponse{Err: err})
	if e != nil {
		h.logger.Errorf("write query response error: %v (while trying to write another error: %v)", e, err)
	}
}

// handlePostSQL handles /sql requests.
func (h *Handler) handlePostSQL(w http.ResponseWriter, r *http.Request) {
	includePlan := false

	includePlanValue := r.URL.Query().Get("plan")
	if len(includePlanValue) > 0 {
		var err error
		includePlan, err = strconv.ParseBool(includePlanValue)
		if err != nil {
			h.writeBadRequest(w, r, err)
			return
		}
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeBadRequest(w, r, err)
		return
	}

	requestID, err := uuid.NewV4()
	if err != nil {
		h.writeBadRequest(w, r, err)
	}
	// put the requestId in the context
	ctx := fbcontext.WithRequestID(r.Context(), requestID.String())

	sql := string(b)
	rootOperator, err := h.api.CompilePlan(ctx, sql)
	if err != nil {
		h.writeBadRequest(w, r, err)
		return
	}

	// Write response back to client.
	w.Header().Set("Content-Type", "application/json")

	// the pandas data frame format in json

	// Opening bracket.
	w.Write([]byte("{"))

	// Write the closing bracket on any exit from this method.
	defer func() {
		var execTime int64 = 0
		// we are going to make best effort here - don't actually care about the error
		// if there was an error, the request.ElapsedTime will be zero
		request, _ := h.api.server.SystemLayer.ExecutionRequests().GetRequest(requestID.String())
		execTime = request.ElapsedTime.Microseconds()

		var value []byte
		value, err = json.Marshal(execTime)
		if err != nil {
			w.Write([]byte(`,"exec_time": 0`))
		} else {
			w.Write([]byte(`,"exec_time":`))
			w.Write(value)
		}
		w.Write([]byte("}"))
	}()

	// writeError is a helper function that can be called anywhere during the
	// output handling to insert an error into the json output.
	writeError := func(err error, withComma bool) {
		if err != nil {
			errMsg, err := json.Marshal(err.Error())
			if err != nil {
				errMsg = []byte(`"PROBLEM ENCODING ERROR MESSAGE"`)
			}
			if withComma {
				w.Write([]byte(`,"error":`))
			} else {
				w.Write([]byte(`"error":`))
			}
			w.Write(errMsg)
		}
	}

	// writeWarnings is a helper function that can be called anywhere during the
	// output handling to insert warnings into the json output.
	writeWarnings := func(warnings []string) {
		if len(warnings) > 0 {
			w.Write([]byte(`,"warnings": [`))
			for i, warn := range warnings {
				warnMsg, err := json.Marshal(warn)
				if err != nil {
					warnMsg = []byte(`"PROBLEM ENCODING WARNING"`)
				}
				w.Write(warnMsg)
				if i < len(warnings)-1 {
					w.Write([]byte(`,`))
				}
			}
			w.Write([]byte(`]`))
		}
	}

	writePlan := func(plan map[string]interface{}) {
		if plan != nil && includePlan {
			planBytes, err := json.Marshal(plan)
			if err != nil {
				planBytes = []byte(`"PROBLEM ENCODING QUERY PLAN"`)
			}
			w.Write([]byte(`,"queryPlan":`))
			w.Write(planBytes)
		}
	}

	// Get a query iterator.
	iter, err := rootOperator.Iterator(ctx, nil)
	if err != nil {
		writeError(err, false)
		writeWarnings(rootOperator.Warnings())
		return
	}

	// Read schema & write to response.
	columns := rootOperator.Schema()
	schema := WireQuerySchema{
		Fields: make([]*WireQueryField, len(columns)),
	}
	for i, col := range columns {
		btype, err := dax.BaseTypeFromString(col.Type.BaseTypeName())
		if err != nil {
			writeError(err, false)
			writeWarnings(rootOperator.Warnings())
			return
		}
		schema.Fields[i] = &WireQueryField{
			Name:     dax.FieldName(col.ColumnName),
			Type:     col.Type.TypeDescription(),
			BaseType: btype,
			TypeInfo: col.Type.TypeInfo(),
		}
	}
	w.Write([]byte(`"schema":`))
	jsonSchema, err := json.Marshal(schema)
	if err != nil {
		h.logger.Errorf("write schema response error: %s", err)
		// Provide an empty list as the schema value to maintain valid json.
		w.Write([]byte("[]"))
		writeError(err, false)
		writeWarnings(rootOperator.Warnings())
		return
	}
	w.Write(jsonSchema)

	// Write the data (rows).
	w.Write([]byte(`,"data":[`))

	var rowErr error
	var currentRow types.Row
	var nextErr error

	rowCounter := 1
	for currentRow, nextErr = iter.Next(ctx); nextErr == nil; currentRow, nextErr = iter.Next(ctx) {
		jsonRow, err := json.Marshal(currentRow)
		if err != nil {
			h.logger.Errorf("json encoding error: %s", err)
			rowErr = err
			break
		}

		if rowCounter > 1 {
			// Include a comma between data rows.
			w.Write([]byte(","))
		}
		w.Write(jsonRow)

		rowCounter++
	}
	if nextErr != nil && nextErr != types.ErrNoMoreRows {
		rowErr = nextErr
	}

	w.Write([]byte("]"))

	writeError(rowErr, true)
	writeWarnings(rootOperator.Warnings())
	writePlan(rootOperator.Plan())
}

func (h *Handler) handleCPUProfileStart(w http.ResponseWriter, r *http.Request) {
	if h.pprofCPUProfileBuffer == nil {
		h.pprofCPUProfileBuffer = bytes.NewBuffer(nil)
	} else {
		http.Error(w, "cpu profile already in progress", http.StatusBadRequest)
		return
	}
	err := pprof.StartCPUProfile(h.pprofCPUProfileBuffer)
	if err != nil {
		http.Error(w, fmt.Sprintf("%v", err), http.StatusBadRequest)
		h.pprofCPUProfileBuffer = nil
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleCPUProfileStop(w http.ResponseWriter, r *http.Request) {
	if h.pprofCPUProfileBuffer == nil {
		http.Error(w, "no cpu profile in progress", http.StatusBadRequest)
		return
	}
	pprof.StopCPUProfile()

	// match what pprof usually returns:
	// HTTP/1.1 200 OK
	// Content-Disposition: attachment; filename="profile"
	// Content-Type: application/octet-stream
	// X-Content-Type-Options: nosniff
	// Date: Tue, 03 Nov 2020 18:31:36 GMT
	// Content-Length: 939

	// Send the headers
	by := h.pprofCPUProfileBuffer.Bytes()
	w.Header().Set("Content-Disposition", "attachment; filename=\"profile\"")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(by)))
	w.Header().Set("X-Content-Type-Options", "nosniff")

	_, err := io.Copy(w, h.pprofCPUProfileBuffer)
	h.pprofCPUProfileBuffer = nil
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleGetIndexAvailableShards handles GET /internal/index/:index/shards requests.
func (h *Handler) handleGetIndexAvailableShards(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	indexName := mux.Vars(r)["index"]
	shards, err := h.api.AvailableShards(r.Context(), indexName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(getIndexAvailableShardsResponse{Shards: shards.Slice()}); err != nil {
		h.logger.Errorf("write shards-max response error: %s", err)
	}
}

type getIndexAvailableShardsResponse struct {
	Shards []uint64 `json:"shards"`
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
		h.logger.Errorf("write shards-max response error: %s", err)
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
	q := r.URL.Query()
	withViews := q.Get("views") == "true"

	indexName := mux.Vars(r)["index"]
	schema, err := h.api.Schema(r.Context(), withViews)
	if err != nil {
		h.logger.Printf("getting schema error: %s", err)
	}

	for _, idx := range schema {
		if idx.Name == indexName {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(idx); err != nil {
				h.logger.Errorf("write response error: %s", err)
			}
			return
		}
	}
	http.Error(w, fmt.Sprintf("Index %s Not Found", indexName), http.StatusNotFound)
}

// handleGetView handles GET /index/<indexname>/field/<fieldname>/view requests.
func (h *Handler) handleGetView(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	indexName := mux.Vars(r)["index"]
	fieldName := mux.Vars(r)["field"]
	index, err := h.api.Index(r.Context(), indexName)

	if err != nil {
		http.Error(w, fmt.Sprintf("Index %s Not Found", indexName), http.StatusNotFound)
		return
	} else {
		w.Header().Set("Content-Type", "application/json")
		var viewsList []viewReponse
		for _, field := range index.fields {
			if field.name == fieldName {
				for _, view := range field.views() {
					viewsList = append(viewsList, viewReponse{Name: view.name, Type: view.fieldType, Field: view.field, Index: view.index})
				}

				if err := json.NewEncoder(w).Encode(viewsList); err != nil {
					h.logger.Errorf("write response error: %s", err)
				}
				return
			}
		}
	}
	http.Error(w, fmt.Sprintf("Field %s Not Found", fieldName), http.StatusNotFound)
}

type viewReponse struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Field string `json:"field"`
	Index string `json:"index"`
}

// handleDeleteIndex handles DELETE /index/<indexname>/field/<fieldname>/view/<viewname> request.
func (h *Handler) handleDeleteView(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	indexName := mux.Vars(r)["index"]
	fieldName := mux.Vars(r)["field"]
	viewName := mux.Vars(r)["view"]

	resp := successResponse{h: h}
	err := h.api.DeleteView(r.Context(), indexName, fieldName, viewName)
	resp.write(w, err)
}

type postIndexRequest struct {
	Options IndexOptions `json:"options"`
}

// _postIndexRequest is necessary to avoid recursion while decoding.
type _postIndexRequest postIndexRequest

// Custom Unmarshal JSON to validate request body when creating a new index.
func (p *postIndexRequest) UnmarshalJSON(b []byte) error {
	// m is an overflow map used to capture additional, unexpected keys.
	m := make(map[string]interface{})
	if err := json.Unmarshal(b, &m); err != nil {
		return errors.Wrap(err, "unmarshalling unexpected values")
	}

	validIndexOptions := getValidOptions(IndexOptions{})
	err := validateOptions(m, validIndexOptions)
	if err != nil {
		return err
	}
	// Unmarshal expected values.
	_p := _postIndexRequest{
		Options: IndexOptions{
			Description:    "",
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
		Options: IndexOptions{
			Description:    "",
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
	} else if _, ok = errors.Cause(err).(ConflictError); ok {
		if index, _ = h.api.Index(r.Context(), indexName); index != nil {
			resp.CreatedAt = index.CreatedAt()
		}
	}
	resp.write(w, err)
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
			_, err := fmt.Fprintf(w, "%*s%q\n", -(maxlen + 2), durations[i], q.PQL)
			if err != nil {
				h.logger.Errorf("sending GetActiveQueries response: %s", err)
				return
			}
		}
		if _, err := w.Write([]byte{'\n'}); err != nil {
			h.logger.Errorf("sending GetActiveQueries response: %s", err)
		}
	case "application/json":
		if err := json.NewEncoder(w).Encode(queries); err != nil {
			h.logger.Errorf("encoding GetActiveQueries response: %s", err)
		}
	}
}

func (h *Handler) handleGetPastQueries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	remoteStr := q.Get("remote")
	var remote bool
	if remoteStr == "true" {
		remote = true
	}

	queries, err := h.api.PastQueries(r.Context(), remote)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(queries); err != nil {
		h.logger.Errorf("encoding GetActiveQueries response: %s", err)
	}
}

func fieldOptionsToFunctionalOpts(opt fieldOptions) []FieldOption {
	// Convert json options into functional options.
	var fos []FieldOption
	switch opt.Type {
	case FieldTypeSet:
		fos = append(fos, OptFieldTypeSet(*opt.CacheType, *opt.CacheSize))
	case FieldTypeInt:
		min, max := pql.MinMax(0)
		// ensure the provided bounds are valid
		if opt.Max != nil && max.LessThan(*opt.Max) {
			opt.Max = &max
		}
		if opt.Max == nil {
			opt.Max = &max
		}

		if opt.Min != nil && min.GreaterThan(*opt.Min) {
			opt.Min = &min
		}
		if opt.Min == nil {
			opt.Min = &min
		}
		fos = append(fos, OptFieldTypeInt(opt.Min.ToInt64(0), opt.Max.ToInt64(0)))
	case FieldTypeDecimal:
		scale := int64(0)
		if opt.Scale != nil {
			scale = *opt.Scale
			min, max := pql.MinMax(scale)
			// ensure the provided bounds are valid
			if opt.Max != nil && max.LessThan(*opt.Max) {
				opt.Max = &max
			}
			if opt.Max == nil {
				opt.Max = &max
			}

			if opt.Min != nil && min.GreaterThan(*opt.Min) {
				opt.Min = &min
			}
			if opt.Min == nil {
				opt.Min = &min
			}
		}

		if opt.Min == nil {
			min := pql.NewDecimal(int64(math.MinInt64), scale)
			opt.Min = &min
		}
		if opt.Max == nil {
			max := pql.NewDecimal(int64(math.MaxInt64), scale)
			opt.Max = &max
		}
		var minmax []pql.Decimal
		if opt.Min != nil {
			minmax = []pql.Decimal{
				*opt.Min,
			}
			if opt.Max != nil {
				minmax = append(minmax, *opt.Max)
			}
		}
		fos = append(fos, OptFieldTypeDecimal(scale, minmax...))
	case FieldTypeTimestamp:
		if opt.Epoch == nil {
			epoch := DefaultEpoch
			opt.Epoch = &epoch
		}
		fos = append(fos, OptFieldTypeTimestamp(opt.Epoch.UTC(), *opt.TimeUnit))
	case FieldTypeTime:
		if opt.TTL != nil {
			fos = append(fos, OptFieldTypeTime(*opt.TimeQuantum, *opt.TTL, opt.NoStandardView))
		} else {
			fos = append(fos, OptFieldTypeTime(*opt.TimeQuantum, "0", opt.NoStandardView))
		}
	case FieldTypeMutex:
		fos = append(fos, OptFieldTypeMutex(*opt.CacheType, *opt.CacheSize))
	case FieldTypeBool:
		fos = append(fos, OptFieldTypeBool())
	}
	if opt.Keys != nil {
		if *opt.Keys {
			fos = append(fos, OptFieldKeys())
		}
	}
	if opt.ForeignIndex != nil {
		fos = append(fos, OptFieldForeignIndex(*opt.ForeignIndex))
	}
	return fos
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

	fos := fieldOptionsToFunctionalOpts(req.Options)
	field, err := h.api.CreateField(r.Context(), indexName, fieldName, fos...)
	if _, ok = err.(BadRequestError); ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if field != nil {
		resp.CreatedAt = field.CreatedAt()
	} else if _, ok = errors.Cause(err).(ConflictError); ok {
		if field, _ = h.api.Field(r.Context(), indexName, fieldName); field != nil {
			resp.CreatedAt = field.CreatedAt()
		}
	}
	resp.write(w, err)
}

// handlePatchField handles updates to field schema at /index/{index}/field/{field}
func (h *Handler) handlePatchField(w http.ResponseWriter, r *http.Request) {
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
	var req FieldUpdate
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(&req)
	if err != nil && err != io.EOF {
		resp.write(w, err)
		return
	}

	err = h.api.UpdateField(r.Context(), indexName, fieldName, req)
	resp.write(w, err)
}

type postFieldRequest struct {
	Options fieldOptions `json:"options"`
}

// fieldOptions tracks FieldOptions. It is made up of pointers to values,
// and used for input validation.
type fieldOptions struct {
	Type           string       `json:"type,omitempty"`
	CacheType      *string      `json:"cacheType,omitempty"`
	CacheSize      *uint32      `json:"cacheSize,omitempty"`
	Min            *pql.Decimal `json:"min,omitempty"`
	Max            *pql.Decimal `json:"max,omitempty"`
	Scale          *int64       `json:"scale,omitempty"`
	Epoch          *time.Time   `json:"epoch,omitempty"`
	TimeUnit       *string      `json:"timeUnit,omitempty"`
	TimeQuantum    *TimeQuantum `json:"timeQuantum,omitempty"`
	Keys           *bool        `json:"keys,omitempty"`
	NoStandardView bool         `json:"noStandardView,omitempty"`
	ForeignIndex   *string      `json:"foreignIndex,omitempty"`
	TTL            *string      `json:"ttl,omitempty"`
	Base           *int64       `json:"base,omitempty"`
}

func (o *fieldOptions) validate() error {
	// Pointers to default values.
	defaultCacheType := DefaultCacheType
	defaultCacheSize := uint32(DefaultCacheSize)

	switch o.Type {
	case FieldTypeSet, "":
		// Because FieldTypeSet is the default, its arguments are
		// not required. Instead, the defaults are applied whenever
		// a value does not exist.
		if o.Type == "" {
			o.Type = FieldTypeSet
		}
		if o.CacheType == nil {
			o.CacheType = &defaultCacheType
		}
		if o.CacheSize == nil {
			o.CacheSize = &defaultCacheSize
		}
		if o.Min != nil {
			return NewBadRequestError(errors.New("min does not apply to field type set"))
		} else if o.Max != nil {
			return NewBadRequestError(errors.New("max does not apply to field type set"))
		} else if o.TimeQuantum != nil {
			return NewBadRequestError(errors.New("timeQuantum does not apply to field type set"))
		} else if o.TTL != nil {
			return NewBadRequestError(errors.New("ttl does not apply to field type set"))
		}
	case FieldTypeInt:
		if o.CacheType != nil {
			return NewBadRequestError(errors.New("cacheType does not apply to field type int"))
		} else if o.CacheSize != nil {
			return NewBadRequestError(errors.New("cacheSize does not apply to field type int"))
		} else if o.TimeQuantum != nil {
			return NewBadRequestError(errors.New("timeQuantum does not apply to field type int"))
		} else if o.TTL != nil {
			return NewBadRequestError(errors.New("ttl does not apply to field type int"))
		}
	case FieldTypeDecimal:
		if o.Scale == nil {
			return NewBadRequestError(errors.New("decimal field requires a scale argument"))
		} else if o.CacheType != nil {
			return NewBadRequestError(errors.New("cacheType does not apply to field type int"))
		} else if o.CacheSize != nil {
			return NewBadRequestError(errors.New("cacheSize does not apply to field type int"))
		} else if o.TimeQuantum != nil {
			return NewBadRequestError(errors.New("timeQuantum does not apply to field type int"))
		} else if o.TTL != nil {
			return NewBadRequestError(errors.New("ttl does not apply to field type int"))
		} else if o.ForeignIndex != nil && o.Type == FieldTypeDecimal {
			return NewBadRequestError(errors.New("decimal field cannot be a foreign key"))
		}
	case FieldTypeTimestamp:
		if o.TimeUnit == nil {
			return NewBadRequestError(errors.New("timestamp field requires a timeUnit argument"))
		} else if !IsValidTimeUnit(*o.TimeUnit) {
			return NewBadRequestError(errors.New("invalid timeUnit argument"))
		} else if o.CacheType != nil {
			return NewBadRequestError(errors.New("cacheType does not apply to field type timestamp"))
		} else if o.CacheSize != nil {
			return NewBadRequestError(errors.New("cacheSize does not apply to field type timestamp"))
		} else if o.TimeQuantum != nil {
			return NewBadRequestError(errors.New("timeQuantum does not apply to field type timestamp"))
		} else if o.TTL != nil {
			return NewBadRequestError(errors.New("ttl does not apply to field type timestamp"))
		} else if o.ForeignIndex != nil {
			return NewBadRequestError(errors.New("timestamp field cannot be a foreign key"))
		}
	case FieldTypeTime:
		if o.CacheType != nil {
			return NewBadRequestError(errors.New("cacheType does not apply to field type time"))
		} else if o.CacheSize != nil {
			return NewBadRequestError(errors.New("cacheSize does not apply to field type time"))
		} else if o.Min != nil {
			return NewBadRequestError(errors.New("min does not apply to field type time"))
		} else if o.Max != nil {
			return NewBadRequestError(errors.New("max does not apply to field type time"))
		} else if o.TimeQuantum == nil {
			return NewBadRequestError(errors.New("timeQuantum is required for field type time"))
		}
	case FieldTypeMutex:
		if o.CacheType == nil {
			o.CacheType = &defaultCacheType
		}
		if o.CacheSize == nil {
			o.CacheSize = &defaultCacheSize
		}
		if o.Min != nil {
			return NewBadRequestError(errors.New("min does not apply to field type mutex"))
		} else if o.Max != nil {
			return NewBadRequestError(errors.New("max does not apply to field type mutex"))
		} else if o.TimeQuantum != nil {
			return NewBadRequestError(errors.New("timeQuantum does not apply to field type mutex"))
		} else if o.TTL != nil {
			return NewBadRequestError(errors.New("ttl does not apply to field type mutex"))
		}
	case FieldTypeBool:
		if o.CacheType != nil {
			return NewBadRequestError(errors.New("cacheType does not apply to field type bool"))
		} else if o.CacheSize != nil {
			return NewBadRequestError(errors.New("cacheSize does not apply to field type bool"))
		} else if o.Min != nil {
			return NewBadRequestError(errors.New("min does not apply to field type bool"))
		} else if o.Max != nil {
			return NewBadRequestError(errors.New("max does not apply to field type bool"))
		} else if o.TimeQuantum != nil {
			return NewBadRequestError(errors.New("timeQuantum does not apply to field type bool"))
		} else if o.Keys != nil {
			return NewBadRequestError(errors.New("keys does not apply to field type bool"))
		} else if o.TTL != nil {
			return NewBadRequestError(errors.New("ttl does not apply to field type bool"))
		} else if o.ForeignIndex != nil {
			return NewBadRequestError(errors.New("bool field cannot be a foreign key"))
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

func (h *Handler) handleGetTransactionList(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	trnsMap, err := h.api.Transactions(r.Context())
	if err != nil {
		switch errors.Cause(err) {
		case ErrNodeNotPrimary:
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, "problem getting transactions: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Convert the map of transactions to a slice.
	trnsList := make([]*Transaction, len(trnsMap))
	var i int
	for _, v := range trnsMap {
		trnsList[i] = v
		i++
	}

	// Sort the slice by createdAt.
	sort.Slice(trnsList, func(i, j int) bool {
		return trnsList[i].CreatedAt.Before(trnsList[j].CreatedAt)
	})

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(trnsList); err != nil {
		h.logger.Errorf("encoding GetTransactionList response: %s", err)
	}
}

func (h *Handler) handleGetTransactions(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	trnsMap, err := h.api.Transactions(r.Context())
	if err != nil {
		switch errors.Cause(err) {
		case ErrNodeNotPrimary:
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, "problem getting transactions: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(trnsMap); err != nil {
		h.logger.Errorf("encoding GetTransactions response: %s", err)
	}
}

type TransactionResponse struct {
	Transaction *Transaction `json:"transaction,omitempty"`
	Error       string       `json:"error,omitempty"`
}

func (h *Handler) doTransactionResponse(w http.ResponseWriter, err error, trns *Transaction) {
	if err != nil {
		switch errors.Cause(err) {
		case ErrNodeNotPrimary, ErrTransactionExists:
			w.WriteHeader(http.StatusBadRequest)
		case ErrTransactionExclusive:
			w.WriteHeader(http.StatusConflict)
		case ErrTransactionNotFound:
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
		h.logger.Errorf("encoding transaction response: %v", err)
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
	reqTrns := &Transaction{}
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

	if primary := h.api.PrimaryNode(); h.api.NodeID() == primary.ID {
		trns, err := h.api.StartTransaction(r.Context(), id, reqTrns.Timeout, reqTrns.Exclusive, false)
		h.doTransactionResponse(w, err, trns)
		return
	} else {
		http.Redirect(w, r, primary.URI.Normalize()+"/transaction/"+id, http.StatusSeeOther)
		return
	}
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

// handleGetIndexShardSnapshot handles GET /internal/index/{index}/shard/{shard}/snapshot requests.
func (h *Handler) handleGetIndexShardSnapshot(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	shard, err := strconv.ParseUint(mux.Vars(r)["shard"], 10, 64)
	if err != nil {
		http.Error(w, "Invalid shard parameter", http.StatusBadRequest)
		return
	}

	rc, err := h.api.IndexShardSnapshot(r.Context(), indexName, shard, false)
	if err != nil {
		switch errors.Cause(err) {
		case ErrIndexNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer rc.Close()

	// Copy data to response body.
	if _, err := io.CopyBuffer(&passthroughWriter{w}, rc, make([]byte, rbf.PageSize)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// readQueryRequest parses an query parameters from r.
func (h *Handler) readQueryRequest(r *http.Request) (*QueryRequest, error) {
	switch r.Header.Get("Content-Type") {
	case "application/x-protobuf":
		return h.readProtobufQueryRequest(r)
	default:
		return h.readURLQueryRequest(r)
	}
}

// passthroughWriter is used to remove non-Writer interfaces from an io.Writer.
// For example, a writer that implements io.ReaderFrom can change io.Copy() behavior.
type passthroughWriter struct {
	w io.Writer
}

func (w *passthroughWriter) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

// readProtobufQueryRequest parses query parameters in protobuf from r.
func (h *Handler) readProtobufQueryRequest(r *http.Request) (*QueryRequest, error) {
	// Slurp the body.
	body, err := readBody(r)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	qreq := &QueryRequest{}
	err = h.serializer.Unmarshal(body, qreq)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling query request")
	}
	return qreq, nil
}

// readURLQueryRequest parses query parameters from URL parameters from r.
func (h *Handler) readURLQueryRequest(r *http.Request) (*QueryRequest, error) {
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

	remote := parseBool(q.Get("remote"))

	// Optional profiling
	profile := false
	profileString := q.Get("profile")
	if profileString != "" {
		profile, err = strconv.ParseBool(q.Get("profile"))
		if err != nil {
			return nil, fmt.Errorf("invalid profile argument: '%s' (should be true/false)", profileString)
		}
	}

	return &QueryRequest{
		Query:   query,
		Remote:  remote,
		Shards:  shards,
		Profile: profile,
	}, nil
}

func parseBool(a string) bool {
	return strings.ToLower(a) == "true"
}

// writeQueryResponse writes the response from the executor to w.
func (h *Handler) writeQueryResponse(w http.ResponseWriter, r *http.Request, resp *QueryResponse) error {
	if !validHeaderAcceptJSON(r.Header) {
		w.Header().Set("Content-Type", "application/protobuf")
		return h.writeProtobufQueryResponse(w, resp, headerAcceptRoaringRow(r.Header))
	}
	w.Header().Set("Content-Type", "application/json")
	return h.writeJSONQueryResponse(w, resp)
}

// writeProtobufQueryResponse writes the response from the executor to w as protobuf.
func (h *Handler) writeProtobufQueryResponse(w io.Writer, resp *QueryResponse, writeRoaring bool) error {
	serializer := h.serializer
	if writeRoaring {
		serializer = h.roaringSerializer
	}
	if buf, err := serializer.Marshal(resp); err != nil {
		return errors.Wrap(err, "marshalling")
	} else if _, err := w.Write(buf); err != nil {
		return errors.Wrap(err, "writing")
	}
	return nil
}

// writeJSONQueryResponse writes the response from the executor to w as JSON.
func (h *Handler) writeJSONQueryResponse(w io.Writer, resp *QueryResponse) error {
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

// handleGetInternalDebugRBFJSON handles /internal/debug/rbf requests.
func (h *Handler) handleGetInternalDebugRBFJSON(w http.ResponseWriter, r *http.Request) {
	buf, err := json.MarshalIndent(h.api.RBFDebugInfo(), "", "  ")
	if err != nil {
		http.Error(w, "marshal json: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}

// handleGetMetricsJSON handles /metrics.json requests, translating text metrics results to more consumable JSON.
func (h *Handler) handleGetMetricsJSON(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	metrics := make(map[string][]*prom2json.Family)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	for _, node := range h.api.Hosts(r.Context()) {
		metricsURI := node.URI.String() + "/metrics"

		// The buffer size of 60 is performance controlling, but we
		// haven't studied what the optimal setting is. It was
		// earlier set to this value to capture all output from
		// prom2json at once. The output got larger recently, so
		// now we handle unlimited size output using a goroutine.
		mfChan := make(chan *dto.MetricFamily, 60)
		errChan := make(chan error)
		go func() {
			err := prom2json.FetchMetricFamilies(metricsURI, mfChan, transport)
			errChan <- err
		}()

		nodeMetrics := []*prom2json.Family{}
		for mf := range mfChan {
			nodeMetrics = append(nodeMetrics, prom2json.NewFamily(mf))
		}
		err := <-errChan
		if err != nil {
			http.Error(w, "fetching metrics: "+err.Error(), http.StatusInternalServerError)
			return
		}
		metrics[node.ID] = nodeMetrics
	}

	err := json.NewEncoder(w).Encode(metrics)
	if err != nil {
		h.logger.Errorf("json write error: %s", err)
	}
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
		case ErrFragmentNotFound:
			break
		case ErrClusterDoesNotOwnShard:
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
		h.logger.Errorf("json write error: %s", err)
	}
}

// handleGetPartitionNodes handles /internal/partition/nodes requests.
func (h *Handler) handleGetPartitionNodes(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	q := r.URL.Query()

	// Read partition parameter.
	partitionID, err := strconv.ParseInt(q.Get("partition"), 10, 64)
	if err != nil {
		http.Error(w, "shard should be an unsigned integer", http.StatusBadRequest)
		return
	}

	// Retrieve fragment owner nodes.
	nodes, err := h.api.PartitionNodes(r.Context(), int(partitionID))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Write to response.
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(nodes); err != nil {
		h.logger.Errorf("json write error: %s", err)
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
		h.logger.Errorf("json write error: %s", err)
	}
}

// handleGetFragmentBlockData handles GET /internal/fragment/block/data requests.
func (h *Handler) handleGetFragmentBlockData(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "fragment blocks feature removed", http.StatusNotFound)
}

// handleGetFragmentBlocks handles GET /internal/fragment/blocks requests.
func (h *Handler) handleGetFragmentBlocks(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "fragment blocks feature removed", http.StatusNotFound)
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
		h.logger.Errorf("error streaming fragment data: %s", err)
	}
}

// handleGetTranslateData handles GET /internal/translate/data requests.
func (h *Handler) handleGetTranslateData(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Perform field translation copy, if field specified.
	if fieldName := q.Get("field"); fieldName != "" {
		// Retrieve field data from holder.
		p, err := h.api.FieldTranslateData(r.Context(), q.Get("index"), fieldName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		// Stream translate data to response body.
		if _, err := p.WriteTo(w); err != nil {
			h.logger.Errorf("error streaming translation data: %s", err)
		}
		return
	}

	// Otherwise read partition parameter for index translation copy.
	partition, err := strconv.ParseUint(q.Get("partition"), 10, 32)
	if err != nil {
		http.Error(w, "partition or field required", http.StatusBadRequest)
		return
	}

	// Retrieve partition data from holder.
	p, err := h.api.TranslateData(r.Context(), q.Get("index"), int(partition))
	if redir, ok := err.(RedirectError); ok {
		newURL := *r.URL
		newURL.Host = redir.HostPort
		http.Redirect(w, r, newURL.String(), http.StatusSeeOther)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	// Stream translate partition to response body.
	if _, err := p.WriteTo(w); err != nil {
		h.logger.Errorf("error streaming translation data: %s", err)
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
		h.logger.Errorf("write version response error: %s", err)
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
		case MessageProcessingError:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(defaultClusterMessageResponse{}); err != nil {
		h.logger.Errorf("response encoding error: %s", err)
	}
}

type defaultClusterMessageResponse struct{}

func (h *Handler) handlePostTranslateData(w http.ResponseWriter, r *http.Request) {
	// Parse offsets for all indexes and fields from POST body.
	offsets := make(TranslateOffsetMap)
	if err := json.NewDecoder(r.Body).Decode(&offsets); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Stream all translation data.
	rd, err := h.api.GetTranslateEntryReader(r.Context(), offsets)
	if errors.Cause(err) == ErrNotImplemented {
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
		var entry TranslateEntry
		if err := rd.ReadEntry(&entry); err == io.EOF {
			return
		} else if err != nil {
			h.logger.Errorf("http: translate store read error: %s", err)
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

type ClientOption func(client *http.Client, dialer *net.Dialer) *http.Client

func ClientResponseHeaderTimeoutOption(dur time.Duration) ClientOption {
	return func(client *http.Client, dialer *net.Dialer) *http.Client {
		client.Transport.(*http.Transport).ResponseHeaderTimeout = dur
		return client
	}
}

func ClientDialTimeoutOption(dur time.Duration) ClientOption {
	return func(client *http.Client, dialer *net.Dialer) *http.Client {
		dialer.Timeout = dur
		return client
	}
}

func GetHTTPClient(t *tls.Config, opts ...ClientOption) *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 15 * time.Second,
		DualStack: true,
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   200,
		IdleConnTimeout:       20 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if t != nil {
		transport.TLSClientConfig = t
	}

	client := &http.Client{Transport: transport}
	for _, opt := range opts {
		client = opt(client, dialer)
	}
	return client
}

// handlePostImportAtomicRecord handles /import-atomic-record requests
func (h *Handler) handlePostImportAtomicRecord(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if error, code := validateProtobufHeader(r); error != "" {
		http.Error(w, error, code)
		return
	}

	// Read entire body.
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Unmarshal request based on field type.

	q := r.URL.Query()
	sLoss := q.Get("simPowerLossAfter")
	loss := 0
	if sLoss != "" {
		l, err := strconv.ParseInt(sLoss, 10, 64)
		loss = int(l)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}
	opt := func(o *ImportOptions) error {
		o.SimPowerLossAfter = loss
		return nil
	}

	req := &AtomicRecord{}
	if err := h.serializer.Unmarshal(body, req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qcx := h.api.Txf().NewQcx()
	err = h.api.ImportAtomicRecord(r.Context(), qcx, req, opt)
	if err == nil {
		err = qcx.Finish()
	} else {
		qcx.Abort()
	}
	if err != nil {
		switch errors.Cause(err) {
		case ErrClusterDoesNotOwnShard, ErrPreconditionFailed:
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
		case ErrBSIGroupValueTooLow, ErrBSIGroupValueTooHigh, ErrDecimalOutOfRange:
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Write response.
	_, err = w.Write(importOk)
	if err != nil {
		h.logger.Errorf("writing import response: %v", err)
	}
}

// handlePostImport handles /import requests.
func (h *Handler) handlePostImport(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if err, code := validateProtobufHeader(r); err != "" {
		http.Error(w, err, code)
		return
	}

	// Get index and field type to determine how to handle the
	// import data.
	indexName := mux.Vars(r)["index"]
	index, err := h.api.Index(r.Context(), indexName)
	if err != nil {
		if errors.Cause(err) == ErrIndexNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	fieldName := mux.Vars(r)["field"]
	field := index.Field(fieldName)
	if field == nil {
		http.Error(w, ErrFieldNotFound.Error(), http.StatusNotFound)
		return
	}

	// If the clear flag is true, treat the import as clear bits.
	q := r.URL.Query()
	doClear := q.Get("clear") == "true"
	doIgnoreKeyCheck := q.Get("ignoreKeyCheck") == "true"

	opts := []ImportOption{
		OptImportOptionsClear(doClear),
		OptImportOptionsIgnoreKeyCheck(doIgnoreKeyCheck),
	}

	// Read entire body.
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Unmarshal request based on field type.
	if field.Type() == FieldTypeInt || field.Type() == FieldTypeDecimal || field.Type() == FieldTypeTimestamp {
		// Field type: Int
		// Marshal into request object.
		req := &ImportValueRequest{}
		if err := h.serializer.Unmarshal(body, req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		qcx := h.api.Txf().NewQcx()
		defer qcx.Abort()

		if err := h.api.ImportValue(r.Context(), qcx, req, opts...); err != nil {
			switch errors.Cause(err) {
			case ErrClusterDoesNotOwnShard, ErrPreconditionFailed:
				http.Error(w, err.Error(), http.StatusPreconditionFailed)
			case ErrBSIGroupValueTooLow, ErrBSIGroupValueTooHigh, ErrDecimalOutOfRange:
				http.Error(w, err.Error(), http.StatusBadRequest)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}

			return
		}
		err := qcx.Finish()
		if err != nil {
			http.Error(w, fmt.Sprintf("error in qcx.Finish(): '%v'", err.Error()), http.StatusInternalServerError)
			return
		}
	} else {
		// Field type: set, time, mutex
		// Marshal into request object.
		req := &ImportRequest{}
		if err := h.serializer.Unmarshal(body, req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		qcx := h.api.Txf().NewQcx()
		defer qcx.Abort()

		if err := h.api.Import(r.Context(), qcx, req, opts...); err != nil {
			switch errors.Cause(err) {
			case ErrClusterDoesNotOwnShard, ErrPreconditionFailed:
				http.Error(w, err.Error(), http.StatusPreconditionFailed)
			case ErrBSIGroupValueTooLow, ErrBSIGroupValueTooHigh, ErrDecimalOutOfRange:
				http.Error(w, err.Error(), http.StatusBadRequest)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		err := qcx.Finish()
		if err != nil {
			http.Error(w, fmt.Sprintf("error in qcx.Finish() on set,time,mutex: '%v'", err.Error()), http.StatusInternalServerError)
			return
		}
	}

	// Write response.
	_, err = w.Write(importOk)
	if err != nil {
		h.logger.Errorf("writing import response: %v", err)
	}
}

// handleGetMutexCheck handles /mutex-check requests.
func (h *Handler) handleGetMutexCheck(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	// Get index and field type to determine how to handle the
	// import data.
	indexName, fieldName := mux.Vars(r)["index"], mux.Vars(r)["field"]
	q := r.URL.Query()
	limit := 0
	details := q.Get("details") == "true"
	limitStr := q.Get("limit")
	if limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			http.Error(w, "limit must be numeric", http.StatusBadRequest)
		}
	}
	qcx := h.api.Txf().NewQcx()
	defer qcx.Abort()
	out, err := h.api.MutexCheck(r.Context(), qcx, indexName, fieldName, details, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	outBytes, err := json.Marshal(out)
	if err != nil {
		http.Error(w, fmt.Sprintf("marshalling response: %v", err), http.StatusInternalServerError)
	}
	_, err = w.Write(outBytes)
	if err != nil {
		h.logger.Errorf("writing mutex-check response: %v", err)
	}
}

// handleInternalGetMutexCheck handles internal (non-forwarding )/mutex-check requests.
func (h *Handler) handleInternalGetMutexCheck(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	// Get index and field type to determine how to handle the
	// import data.
	indexName, fieldName := mux.Vars(r)["index"], mux.Vars(r)["field"]
	q := r.URL.Query()
	limit := 0
	details := q.Get("details") == "true"
	limitStr := q.Get("limit")
	if limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			http.Error(w, "limit must be numeric", http.StatusBadRequest)
		}
	}
	qcx := h.api.Txf().NewQcx()
	defer qcx.Abort()
	out, err := h.api.MutexCheckNode(r.Context(), qcx, indexName, fieldName, details, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	outBytes, err := json.Marshal(out)
	if err != nil {
		http.Error(w, fmt.Sprintf("marshalling response: %v", err), http.StatusInternalServerError)
	}
	_, err = w.Write(outBytes)
	if err != nil {
		h.logger.Errorf("writing mutex-check response: %v", err)
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
	span, _ := tracing.StartSpanFromContext(ctx, "io.ReadAll-Body")
	body, err := readBody(r)
	span.LogKV("bodySize", len(body))
	span.Finish()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	req := &ImportRoaringRequest{}
	span, _ = tracing.StartSpanFromContext(ctx, "Unmarshal")
	err = h.serializer.Unmarshal(body, req)
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
	resp := &ImportResponse{}
	// TODO give meaningful stats for import
	err = h.api.ImportRoaring(ctx, indexName, fieldName, shard, remote, req)
	if err != nil {
		resp.Err = err.Error()
		if _, ok := err.(BadRequestError); ok {
			w.WriteHeader(http.StatusBadRequest)
		} else if _, ok := err.(NotFoundError); ok {
			w.WriteHeader(http.StatusNotFound)
		} else if _, ok := err.(PreconditionFailedError); ok {
			w.WriteHeader(http.StatusPreconditionFailed)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}

	// Marshal response object.
	buf, err := h.serializer.Marshal(resp)
	if err != nil {
		http.Error(w, fmt.Sprintf("marshal import-roaring response: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response.
	_, err = w.Write(buf)
	if err != nil {
		h.logger.Errorf("writing import-roaring response: %v", err)
		return
	}
}

// handlePostShardImportRoaring takes data for multiple fields for a
// particular shard and imports it all in a single transaction. It was
// developed in the post-RBF world and should probably ultimately
// replace most of the other import endpoints.
func (h *Handler) handlePostShardImportRoaring(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if error, code := validateProtobufHeader(r); error != "" {
		http.Error(w, error, code)
		return
	}

	// Get index and field type to determine how to handle the
	// import data.
	indexName := mux.Vars(r)["index"]

	ctx := r.Context()

	// Read entire body.
	span, _ := tracing.StartSpanFromContext(ctx, "io.ReadAll-Body")
	body, err := readBody(r)
	span.LogKV("bodySize", len(body))
	span.Finish()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	req := &ImportRoaringShardRequest{}
	span, _ = tracing.StartSpanFromContext(ctx, "Unmarshal")
	err = h.serializer.Unmarshal(body, req)
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
	resp := &ImportResponse{}
	// TODO give meaningful stats for import
	err = h.api.ImportRoaringShard(ctx, indexName, shard, req)
	if err != nil {
		resp.Err = err.Error()
		if errors.Is(err, ErrIndexNotFound) {
			w.WriteHeader(http.StatusNotFound)
		} else if errors.As(err, &BadRequestError{}) {
			w.WriteHeader(http.StatusBadRequest)
		} else if _, ok := errors.Cause(err).(NotFoundError); ok {
			w.WriteHeader(http.StatusNotFound)
		} else if errors.As(err, &PreconditionFailedError{}) {
			w.WriteHeader(http.StatusPreconditionFailed)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}

	// Marshal response object.
	buf, err := h.serializer.Marshal(resp)
	if err != nil {
		http.Error(w, fmt.Sprintf("marshal shard-import-roaring response: %v", err), http.StatusInternalServerError)
		return
	}

	// Write response.
	_, err = w.Write(buf)
	if err != nil {
		h.logger.Errorf("writing shard-import-roaring response: %v", err)
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
	switch errors.Cause(err) {
	case nil:
		// Write response.
		if _, err = w.Write(buf); err != nil {
			h.logger.Errorf("writing translate keys response: %v", err)
		}

	case ErrTranslatingKeyNotFound:
		http.Error(w, fmt.Sprintf("translate keys: %v", err), http.StatusNotFound)

	case ErrTranslateStoreReadOnly:
		http.Error(w, fmt.Sprintf("translate keys: %v", err), http.StatusPreconditionFailed)

	default:
		http.Error(w, fmt.Sprintf("translate keys: %v", err), http.StatusInternalServerError)
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
		h.logger.Errorf("writing translate keys response: %v", err)
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

func (h *Handler) handleFindOrCreateKeys(w http.ResponseWriter, r *http.Request, requireField bool, create bool) {
	// Verify input and output types
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "Not acceptable", http.StatusNotAcceptable)
		return
	}
	var indexName, fieldName string
	var keys []string
	err := func() error {
		var ok bool
		indexName, ok = mux.Vars(r)["index"]
		if !ok {
			return errors.New("index name is required")
		}

		if requireField {
			fieldName, ok = mux.Vars(r)["field"]
			if !ok {
				return errors.New("field name is required")
			}
		}

		bd, err := readBody(r)
		if err != nil {
			return fmt.Errorf("failed to read body: %v", err)
		}

		err = json.Unmarshal(bd, &keys)
		if err != nil {
			return fmt.Errorf("failed to decode request: %v", err)
		}
		return nil
	}()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	var translations map[string]uint64
	switch {
	case requireField && create:
		translations, err = h.api.CreateFieldKeys(r.Context(), indexName, fieldName, keys...)
	case requireField && !create:
		translations, err = h.api.FindFieldKeys(r.Context(), indexName, fieldName, keys...)
	case !requireField && create:
		translations, err = h.api.CreateIndexKeys(r.Context(), indexName, keys...)
	case !requireField && !create:
		translations, err = h.api.FindIndexKeys(r.Context(), indexName, keys...)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("translating keys: %v", err), http.StatusInternalServerError)
		return
	}
	data, err := json.Marshal(translations)
	if err != nil {
		http.Error(w, fmt.Sprintf("encoding response: %v", err), http.StatusInternalServerError)
	}
	_, err = w.Write(data)
	if err != nil {
		h.logger.Printf("writing CreateFieldKeys response: %v", err)
	}
}

func (h *Handler) handleFindIndexKeys(w http.ResponseWriter, r *http.Request) {
	h.handleFindOrCreateKeys(w, r, false, false)
}

func (h *Handler) handleFindFieldKeys(w http.ResponseWriter, r *http.Request) {
	h.handleFindOrCreateKeys(w, r, true, false)
}

func (h *Handler) handleCreateIndexKeys(w http.ResponseWriter, r *http.Request) {
	h.handleFindOrCreateKeys(w, r, false, true)
}

func (h *Handler) handleCreateFieldKeys(w http.ResponseWriter, r *http.Request) {
	h.handleFindOrCreateKeys(w, r, true, true)
}

func (h *Handler) handleMatchField(w http.ResponseWriter, r *http.Request) {
	// Verify output type.
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "Not acceptable", http.StatusNotAcceptable)
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

	bd, err := readBody(r)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	matches, err := h.api.MatchField(r.Context(), indexName, fieldName, string(bd))
	if err != nil {
		http.Error(w, "failed to match pattern", http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(w).Encode(matches)
	if err != nil {
		http.Error(w, "encoding result", http.StatusBadRequest)
		return
	}
}

func (h *Handler) handleReserveIDs(w http.ResponseWriter, r *http.Request) {
	// Verify input and output types
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "Not acceptable", http.StatusNotAcceptable)
		return
	}

	bd, err := readBody(r)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var req IDAllocReserveRequest
	req.Offset = ^uint64(0)
	err = json.Unmarshal(bd, &req)
	if err != nil {
		http.Error(w, "failed to decode request", http.StatusBadRequest)
		return
	}

	ids, err := h.api.ReserveIDs(req.Key, req.Session, req.Offset, req.Count)
	if err != nil {
		var esync ErrIDOffsetDesync
		if errors.As(err, &esync) {
			w.Header().Add("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			err = json.NewEncoder(w).Encode(struct {
				ErrIDOffsetDesync
				Err string `json:"error"`
			}{
				ErrIDOffsetDesync: esync,
				Err:               err.Error(),
			})
			if err != nil {
				h.logger.Debugf("failed to send desync error: %v", err)
			}
			return
		}
		http.Error(w, fmt.Sprintf("reserving IDs: %v", err.Error()), http.StatusBadRequest)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(ids)
	if err != nil {
		http.Error(w, "encoding result", http.StatusBadRequest)
		return
	}
}

func (h *Handler) handleCommitIDs(w http.ResponseWriter, r *http.Request) {
	// Verify input and output types
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	} else if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "Not acceptable", http.StatusNotAcceptable)
		return
	}

	bd, err := readBody(r)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var req IDAllocCommitRequest
	err = json.Unmarshal(bd, &req)
	if err != nil {
		http.Error(w, "failed to decode request", http.StatusBadRequest)
		return
	}

	err = h.api.CommitIDs(req.Key, req.Session, req.Count)
	if err != nil {
		http.Error(w, fmt.Sprintf("committing IDs: %v", err.Error()), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleResetIDAlloc(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptType(r.Header, "text", "plain") {
		http.Error(w, "text/plain is not an acceptable response type", http.StatusNotAcceptable)
		return
	}
	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}

	err := h.api.ResetIDAlloc(indexName)
	if err != nil {
		http.Error(w, fmt.Sprintf("resetting ID allocation: %v", err.Error()), http.StatusBadRequest)
		return
	}
	w.Header().Add("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK")) //nolint:errcheck
}

func (h *Handler) handleIDAllocData(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/octet-stream")
	if err := h.api.WriteIDAllocDataTo(w); err != nil {
		http.Error(w, fmt.Sprintf("writeing id allocation data: %v", err.Error()), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) handleRestoreIDAlloc(w http.ResponseWriter, r *http.Request) {
	if err := h.api.RestoreIDAlloc(r.Body); err != nil {
		http.Error(w, fmt.Sprintf("restoring id allocation: %v", err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK")) //nolint:errcheck
}

func (h *Handler) handlePostRestore(w http.ResponseWriter, r *http.Request) {
	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}
	shardID, ok := mux.Vars(r)["shardID"]
	if !ok {
		http.Error(w, "shardID is required", http.StatusBadRequest)
		return
	}
	shard, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse shard %v %v err:%v", indexName, shardID, err), http.StatusBadRequest)
		return
	}
	ctx := context.Background()
	// validate shard for this node
	err = h.api.RestoreShard(ctx, indexName, shard, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to restore shard %v %v err:%v", indexName, shard, err), http.StatusBadRequest)
		return
	}

	w.Header().Add("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK")) //nolint:errcheck
}

// handleDeleteDataframe handles DELETE /index/dataframe request.
func (h *Handler) handleDeleteDataframe(w http.ResponseWriter, r *http.Request) {
	if !h.api.server.dataframeEnabled {
		http.Error(w, "Dataframe is disabled", http.StatusBadRequest)
		return
	}
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	indexName := mux.Vars(r)["index"]

	resp := successResponse{h: h}
	err := h.api.DeleteDataframe(r.Context(), indexName)
	resp.write(w, err)
}

func (h *Handler) handlePostDataframeRestore(w http.ResponseWriter, r *http.Request) {
	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}
	shardID, ok := mux.Vars(r)["shardID"]
	if !ok {
		http.Error(w, "shardID is required", http.StatusBadRequest)
		return
	}
	shard, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse shard %v %v err:%v", indexName, shardID, err), http.StatusBadRequest)
		return
	}
	idx, err := h.api.Index(r.Context(), indexName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Index %s Not Found", indexName), http.StatusNotFound)
		return
	}
	filename := idx.GetDataFramePath(shard) + ".parquet"
	dest, err := os.Create(filename)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create restore dataframe shard %v %v err:%v", indexName, shard, err), http.StatusBadRequest)
		return
	}
	_, err = io.Copy(dest, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to copy restore dataframe shard %v %v err:%v", indexName, shard, err), http.StatusBadRequest)
		return
	}

	w.Header().Add("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK")) //nolint:errcheck
}

func (h *Handler) handleLogin(w http.ResponseWriter, r *http.Request) {
	if h.auth == nil {
		http.Error(w, "", http.StatusNoContent)
		return
	}

	h.auth.Login(w, r)
}

func (h *Handler) handleRedirect(w http.ResponseWriter, r *http.Request) {
	if h.auth == nil {
		http.Error(w, "", http.StatusNoContent)
		return
	}
	h.auth.Redirect(w, r)
}

// handleOAuthConfig handles requests for a cleaned version of our oAuthConfig. We
// use this endpoint /internal/oauth-config in the `featurebase auth-token`
// subcommand to create a RedirectURL on the fly.
func (h *Handler) handleOAuthConfig(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	if h.auth == nil {
		http.Error(w, "auth not enabled: no OAuthConfig", http.StatusNotFound)
		return
	}

	config := h.auth.CleanOAuthConfig()
	if err := json.NewEncoder(w).Encode(config); err != nil {
		h.logger.Errorf("writing oauth-config info: %s", err)
	}
}

func (h *Handler) handleCheckAuthentication(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	if h.auth == nil {
		http.Error(w, "", http.StatusNoContent)
		return
	}

	access, refresh := getTokens(r)
	uinfo, err := h.auth.Authenticate(access, refresh)
	if uinfo == nil || err != nil {
		w.Header().Add("Content-Type", "text/plain")
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	// just in case it got refreshed
	h.auth.SetCookie(w, uinfo.Token, uinfo.RefreshToken, uinfo.Expiry)

	w.Header().Add("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK")) //nolint:errcheck
}

func (h *Handler) handleUserInfo(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}
	if h.auth == nil {
		http.Error(w, "", http.StatusNoContent)
		return
	}

	access, refresh := getTokens(r)
	uinfo, err := h.auth.Authenticate(access, refresh)
	if err != nil {
		h.logger.Errorf("error authenticating: %v", err)
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	// just in case it got refreshed
	h.auth.SetCookie(w, uinfo.Token, uinfo.RefreshToken, uinfo.Expiry)

	if err := json.NewEncoder(w).Encode(uinfo); err != nil {
		h.logger.Errorf("writing user info: %s", err)
	}
}

func (h *Handler) handleLogout(w http.ResponseWriter, r *http.Request) {
	if h.auth == nil {
		http.Error(w, "", http.StatusNoContent)
		return
	}
	h.auth.Logout(w, r)
}

// getTokens gets the access and refresh tokens from the request,
// returning empty strings if they aren't in the request.
func getTokens(r *http.Request) (string, string) {
	var access, refresh string
	if token, ok := r.Header["Authorization"]; ok && len(token) > 0 {
		parts := strings.Split(token[0], "Bearer ")
		if len(parts) >= 2 {
			access = parts[1]
		}
	}

	if token, ok := r.Header[authn.RefreshHeaderName]; ok && len(token) > 0 {
		refresh = token[0]
	}

	if access == "" {
		accessCookie, err := r.Cookie(authn.AccessCookieName)
		if err != nil {
			return access, refresh
		}
		access = accessCookie.Value
	}

	if refresh == "" {
		refreshCookie, err := r.Cookie(authn.RefreshCookieName)
		if err != nil {
			return access, refresh
		}
		refresh = refreshCookie.Value
	}

	return access, refresh
}

func (h *Handler) handleGetDirective(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	applied, err := h.api.DirectiveApplied(r.Context())
	if err != nil {
		http.Error(w, "getting directive applied error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := struct {
		Applied bool `json:"applied"`
	}{
		Applied: applied,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.logger.Errorf("write status response error: %s", err)
	}
}

func (h *Handler) handlePostDirective(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	body := r.Body
	defer body.Close()

	d := &dax.Directive{}
	if err := json.NewDecoder(body).Decode(d); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.api.Directive(r.Context(), d); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// POST /snapshot/shard-data
func (h *Handler) handlePostSnapshotShardData(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	body := r.Body
	defer body.Close()

	req := &dax.SnapshotShardDataRequest{}
	if err := json.NewDecoder(body).Decode(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.api.SnapshotShardData(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// POST /snapshot/table-keys
func (h *Handler) handlePostSnapshotTableKeys(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	body := r.Body
	defer body.Close()

	req := &dax.SnapshotTableKeysRequest{}
	if err := json.NewDecoder(body).Decode(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.api.SnapshotTableKeys(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// POST /snapshot/field-keys
func (h *Handler) handlePostSnapshotFieldKeys(w http.ResponseWriter, r *http.Request) {
	if !validHeaderAcceptJSON(r.Header) {
		http.Error(w, "JSON only acceptable response", http.StatusNotAcceptable)
		return
	}

	body := r.Body
	defer body.Close()

	req := &dax.SnapshotFieldKeysRequest{}
	if err := json.NewDecoder(body).Decode(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.api.SnapshotFieldKeys(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// GET /health
func (h *Handler) handleGetHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func init() {
	gob.Register(arrow.PrimitiveTypes.Int64)
	gob.Register(arrow.PrimitiveTypes.Float64)
}

// EXPERIMENTAL API MAY CHANGE

func (h *Handler) handleGetDataframeSchema(w http.ResponseWriter, r *http.Request) {
	if !h.api.server.dataframeEnabled {
		http.Error(w, "Dataframe is disabled", http.StatusBadRequest)
		return
	}
	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}
	parts, err := h.api.GetDataframeSchema(r.Context(), indexName)
	if err != nil {
		http.Error(w, fmt.Sprintf("%v", err), http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(parts); err != nil {
		h.logger.Errorf("dataframe schema err: %s", err)
	}
}

func (h *Handler) handleGetDataframe(w http.ResponseWriter, r *http.Request) {
	if !h.api.server.dataframeEnabled {
		http.Error(w, "Dataframe is disabled", http.StatusBadRequest)
		return
	}
	indexName, ok := mux.Vars(r)["index"]

	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}
	shardString, ok := mux.Vars(r)["shard"]
	if !ok {
		http.Error(w, "shard is required", http.StatusBadRequest)
		return
	}
	shard, err := strconv.ParseUint(shardString, 10, 64)
	if err != nil {
		http.Error(w, "shard should be an unsigned integer", http.StatusBadRequest)
		return
	}
	idx, err := h.api.Index(r.Context(), indexName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Index %s Not Found", indexName), http.StatusNotFound)
		return
	}
	filename := idx.GetDataFramePath(shard) + ".parquet"
	http.ServeFile(w, r, filename)
}

func (h *Handler) handlePostDataframe(w http.ResponseWriter, r *http.Request) {
	if !h.api.server.dataframeEnabled {
		http.Error(w, "Dataframe is disabled", http.StatusBadRequest)
		return
	}
	// TODO(twg) 2022/09/29 validate the request
	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}
	shardString, ok := mux.Vars(r)["shard"]
	if !ok {
		http.Error(w, "shard is required", http.StatusBadRequest)
		return
	}
	shard, err := strconv.ParseUint(shardString, 10, 64)
	if err != nil {
		http.Error(w, "shard should be an unsigned integer", http.StatusBadRequest)
		return
	}
	body, err := readBody(r)
	if err != nil {
		http.Error(w, "reading body", http.StatusBadRequest)
		return
	}
	blob := bytes.NewReader(body)
	dec := gob.NewDecoder(blob)
	var changesetRequest ChangesetRequest
	err = dec.Decode(&changesetRequest)
	if err != nil {
		http.Error(w, "decoding request", http.StatusBadRequest)
		return
	}
	err = h.api.ApplyDataframeChangeset(r.Context(), indexName, &changesetRequest, shard)
	if err != nil {
		http.Error(w, fmt.Sprintf("err:%v", err), http.StatusBadRequest)
		return
	}
	msg := ""

	resp := struct {
		Index string
		Err   string
	}{
		Index: indexName,
		Err:   msg,
	}
	w.Header().Add("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}
