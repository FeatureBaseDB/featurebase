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

package pilosa

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	// Imported for its side-effect of registering pprof endpoints with the server.
	_ "net/http/pprof"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/pilosa/pilosa/internal"
	"github.com/pkg/errors"
)

// Handler represents an HTTP handler.
type Handler struct {
	Router *mux.Router

	FileSystem FileSystem

	Logger Logger

	// Keeps the query argument validators for each handler
	validators map[string]*queryValidationSpec

	API *API
}

// externalPrefixFlag denotes endpoints that are intended to be exposed to clients.
// This is used for stats tagging.
var externalPrefixFlag = map[string]bool{
	"schema":  true,
	"query":   true,
	"import":  true,
	"export":  true,
	"index":   true,
	"frame":   true,
	"nodes":   true,
	"version": true,
}

type errorResponse struct {
	Error string `json:"error"`
}

// NewHandler returns a new instance of Handler with a default logger.
func NewHandler() *Handler {
	handler := &Handler{
		FileSystem: NopFileSystem,
		Logger:     NopLogger,
	}
	handler.Router = NewRouter(handler)
	handler.populateValidators()
	return handler
}

func (h *Handler) populateValidators() {
	h.validators = map[string]*queryValidationSpec{}
	h.validators["GetFragmentNodes"] = queryValidationSpecRequired("slice", "index")
	h.validators["GetSliceMax"] = queryValidationSpecRequired().Optional("inverse")
	h.validators["PostQuery"] = queryValidationSpecRequired().Optional("slices", "columnAttrs", "excludeAttrs", "excludeBits")
	h.validators["GetExport"] = queryValidationSpecRequired("index", "frame", "view", "slice")
	h.validators["GetFragmentData"] = queryValidationSpecRequired("index", "frame", "view", "slice")
	h.validators["PostFragmentData"] = queryValidationSpecRequired("index", "frame", "view", "slice")
	h.validators["GetFragmentBlocks"] = queryValidationSpecRequired("index", "frame", "view", "slice")
	h.validators["PostFrameRestore"] = queryValidationSpecRequired("host")
}

func (h *Handler) queryArgValidator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := mux.CurrentRoute(r).GetName()
		if validator, ok := h.validators[key]; ok {
			if err := validator.validate(r.URL.Query()); err != nil {
				// TODO: Return the response depending on the Accept header
				response := errorResponse{Error: err.Error()}
				body, err := json.Marshal(response)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				http.Error(w, string(body), http.StatusBadRequest)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// NewRouter creates a new mux http router.
func NewRouter(handler *Handler) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/", handler.handleWebUI).Methods("GET")
	router.HandleFunc("/assets/{file}", handler.handleWebUI).Methods("GET")
	router.HandleFunc("/cluster/message", handler.handlePostClusterMessage).Methods("POST")
	router.HandleFunc("/cluster/resize/set-coordinator", handler.handlePostClusterResizeSetCoordinator).Methods("POST")
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux).Methods("GET")
	router.Handle("/debug/vars", expvar.Handler()).Methods("GET")
	router.HandleFunc("/fragment/data", handler.handleGetFragmentData).Methods("GET").Name("GetFragmentData")
	router.HandleFunc("/hosts", handler.handleGetHosts).Methods("GET")
	router.HandleFunc("/id", handler.handleGetID).Methods("GET")
	router.HandleFunc("/schema", handler.handleGetSchema).Methods("GET")
	router.HandleFunc("/slices/max", handler.handleGetSlicesMax).Methods("GET") // TODO: deprecate, but it's being used by the client (for backups)
	router.HandleFunc("/status", handler.handleGetStatus).Methods("GET")
	router.HandleFunc("/info", handler.handleGetInfo).Methods("GET")
	router.HandleFunc("/version", handler.handleGetVersion).Methods("GET")

	router.HandleFunc("/cluster/resize/abort", handler.handlePostClusterResizeAbort).Methods("POST")

	router.HandleFunc("/cluster/resize/remove-node", handler.handlePostClusterResizeRemoveNode).Methods("POST")
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux).Methods("GET")
	router.Handle("/debug/vars", expvar.Handler()).Methods("GET")
	router.HandleFunc("/export", handler.handleGetExport).Methods("GET").Name("GetExport")
	router.HandleFunc("/fragment/block/data", handler.handleGetFragmentBlockData).Methods("GET")
	router.HandleFunc("/fragment/blocks", handler.handleGetFragmentBlocks).Methods("GET").Name("GetFragmentBlocks")
	router.HandleFunc("/fragment/data", handler.handlePostFragmentData).Methods("POST").Name("PostFragmentData")
	router.HandleFunc("/fragment/nodes", handler.handleGetFragmentNodes).Methods("GET").Name("GetFragmentNodes")
	router.HandleFunc("/import", handler.handlePostImport).Methods("POST")
	router.HandleFunc("/import-value", handler.handlePostImportValue).Methods("POST")
	router.HandleFunc("/index", handler.handleGetIndexes).Methods("GET")
	router.HandleFunc("/index/{index}", handler.handleGetIndex).Methods("GET")
	router.HandleFunc("/index/{index}", handler.handlePostIndex).Methods("POST")
	router.HandleFunc("/index/{index}", handler.handleDeleteIndex).Methods("DELETE")
	router.HandleFunc("/index/{index}/attr/diff", handler.handlePostIndexAttrDiff).Methods("POST")
	//router.HandleFunc("/index/{index}/frame", handler.handleGetFrames).Methods("GET") // Not implemented.
	router.HandleFunc("/index/{index}/frame/{frame}", handler.handlePostFrame).Methods("POST")
	router.HandleFunc("/index/{index}/frame/{frame}", handler.handleDeleteFrame).Methods("DELETE")
	router.HandleFunc("/index/{index}/frame/{frame}/attr/diff", handler.handlePostFrameAttrDiff).Methods("POST")
	router.HandleFunc("/index/{index}/frame/{frame}/restore", handler.handlePostFrameRestore).Methods("POST").Name("PostFrameRestore")
	router.HandleFunc("/index/{index}/frame/{frame}/field/{field}", handler.handlePostFrameField).Methods("POST")
	router.HandleFunc("/index/{index}/frame/{frame}/fields", handler.handleGetFrameFields).Methods("GET")
	router.HandleFunc("/index/{index}/frame/{frame}/field/{field}", handler.handleDeleteFrameField).Methods("DELETE")
	router.HandleFunc("/index/{index}/frame/{frame}/views", handler.handleGetFrameViews).Methods("GET")
	router.HandleFunc("/index/{index}/frame/{frame}/view/{view}", handler.handleDeleteView).Methods("DELETE")
	router.HandleFunc("/index/{index}/input/{input-definition}", handler.handlePostInput).Methods("POST")
	router.HandleFunc("/index/{index}/input-definition/{input-definition}", handler.handleGetInputDefinition).Methods("GET")
	router.HandleFunc("/index/{index}/input-definition/{input-definition}", handler.handlePostInputDefinition).Methods("POST")
	router.HandleFunc("/index/{index}/input-definition/{input-definition}", handler.handleDeleteInputDefinition).Methods("DELETE")
	router.HandleFunc("/index/{index}/query", handler.handlePostQuery).Methods("POST").Name("PostQuery")
	router.HandleFunc("/recalculate-caches", handler.handleRecalculateCaches).Methods("POST")

	// TODO: Apply MethodNotAllowed statuses to all endpoints.
	// Ideally this would be automatic, as described in this (wontfix) ticket:
	// https://github.com/gorilla/mux/issues/6
	// For now we just do it for the most commonly used handler, /query
	router.HandleFunc("/index/{index}/query", handler.methodNotAllowedHandler).Methods("GET")

	router.Use(handler.queryArgValidator)
	return router
}

func (h *Handler) methodNotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

// ServeHTTP handles an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			stack := debug.Stack()
			msg := "PANIC: %s\n%s"
			h.Logger.Printf(msg, err, stack)
			fmt.Fprintf(w, msg, err, stack)
		}
	}()

	t := time.Now()
	h.Router.ServeHTTP(w, r)
	dif := time.Since(t)

	// Calculate per request StatsD metrics when the handler is fully configured.
	statsTags := make([]string, 0, 3)

	longQueryTime := h.API.LongQueryTime()
	if longQueryTime > 0 && dif > longQueryTime {
		h.Logger.Printf("%s %s %v", r.Method, r.URL.String(), dif)
		statsTags = append(statsTags, "slow_query")
	}

	pathParts := strings.Split(r.URL.Path, "/")
	endpointName := strings.Join(pathParts, "_")

	if externalPrefixFlag[pathParts[1]] {
		statsTags = append(statsTags, "external")
	}

	// useragent tag identifies internal/external endpoints
	statsTags = append(statsTags, "useragent:"+r.UserAgent())
	stats := h.API.StatsWithTags(statsTags)
	if stats != nil {
		stats.Histogram("http."+endpointName, float64(dif), 0.1)
	}
}

func (h *Handler) handleWebUI(w http.ResponseWriter, r *http.Request) {
	// If user is using curl, don't chuck HTML at them
	if strings.HasPrefix(r.UserAgent(), "curl") {
		http.Error(w, "Welcome. Pilosa is running. Visit https://www.pilosa.com/docs/ for more information or try the WebUI by visiting this URL in your browser.", http.StatusNotFound)
		return
	}
	filesystem, err := h.FileSystem.New()
	if err != nil {
		_ = h.writeQueryResponse(w, r, &QueryResponse{Err: err})
		h.Logger.Printf("Pilosa WebUI is not available. Please run `make generate-statik` before building Pilosa with `make install`.")
		return
	}
	http.FileServer(filesystem).ServeHTTP(w, r)
}

// handleGetSchema handles GET /schema requests.
func (h *Handler) handleGetSchema(w http.ResponseWriter, r *http.Request) {
	schema := h.API.Schema(r.Context())
	if err := json.NewEncoder(w).Encode(getSchemaResponse{
		Indexes: schema,
	}); err != nil {
		h.Logger.Printf("write schema response error: %s", err)
	}
}

// handleGetStatus handles GET /status requests.
func (h *Handler) handleGetStatus(w http.ResponseWriter, r *http.Request) {
	status := getStatusResponse{
		State: h.API.State(),
		Nodes: h.API.Hosts(r.Context()),
	}
	if err := json.NewEncoder(w).Encode(status); err != nil {
		h.Logger.Printf("write status response error: %s", err)
	}
}

func (h *Handler) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	info := h.API.Info()
	if err := json.NewEncoder(w).Encode(info); err != nil {
		h.Logger.Printf("write info response error: %s", err)
	}
}

type getSchemaResponse struct {
	Indexes []*IndexInfo `json:"indexes"`
}

type getStatusResponse struct {
	State string  `json:"state"`
	Nodes []*Node `json:"nodes"`
}

// handlePostQuery handles /query requests.
func (h *Handler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	// Parse incoming request.
	req, err := h.readQueryRequest(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		h.writeQueryResponse(w, r, &QueryResponse{Err: err})
		return
	}
	// TODO: Remove
	req.Index = mux.Vars(r)["index"]

	resp, err := h.API.Query(r.Context(), req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		h.writeQueryResponse(w, r, &QueryResponse{Err: err})
		return
	}

	// Set appropriate status code, if there is an error.
	if resp.Err != nil {
		switch resp.Err {
		case ErrTooManyWrites:
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}

	// Write response back to client.
	if err := h.writeQueryResponse(w, r, &resp); err != nil {
		h.Logger.Printf("write query response error: %s", err)
	}
}

// handleGetSlicesMax handles GET /schema requests.
func (h *Handler) handleGetSlicesMax(w http.ResponseWriter, r *http.Request) {
	if err := json.NewEncoder(w).Encode(getSlicesMaxResponse{
		Standard: h.API.MaxSlices(r.Context()),
		Inverse:  h.API.MaxInverseSlices(r.Context()),
	}); err != nil {
		h.Logger.Printf("write slices-max response error: %s", err)
	}
}

type getSlicesMaxResponse struct {
	Standard map[string]uint64 `json:"standard"`
	Inverse  map[string]uint64 `json:"inverse"`
}

// handleGetIndexes handles GET /index request.
func (h *Handler) handleGetIndexes(w http.ResponseWriter, r *http.Request) {
	h.handleGetSchema(w, r)
}

// handleGetIndex handles GET /index/<indexname> requests.
func (h *Handler) handleGetIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	index, err := h.API.Index(r.Context(), indexName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err := json.NewEncoder(w).Encode(getIndexResponse{
		map[string]string{"name": index.Name()},
	}); err != nil {
		h.Logger.Printf("write response error: %s", err)
	}
}

type getIndexResponse struct {
	Index map[string]string `json:"index"`
}

type postIndexRequest struct {
	Options IndexOptions `json:"options"`
}

//_postIndexRequest is necessary to avoid recursion while decoding.
type _postIndexRequest postIndexRequest

// Custom Unmarshal JSON to validate request body when creating a new index.
func (p *postIndexRequest) UnmarshalJSON(b []byte) error {

	// m is an overflow map used to capture additional, unexpected keys.
	m := make(map[string]interface{})
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	validIndexOptions := getValidOptions(IndexOptions{})
	err := validateOptions(m, validIndexOptions)
	if err != nil {
		return err
	}
	// Unmarshal expected values.
	var _p _postIndexRequest
	if err := json.Unmarshal(b, &_p); err != nil {
		return err
	}

	p.Options = _p.Options

	return nil
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
					return fmt.Errorf("Unknown key: %v:%v", kk, vv)
				}
			}
		default:
			return fmt.Errorf("Unknown key: %v:%v", k, v)
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

type postIndexResponse struct{}

// handleDeleteIndex handles DELETE /index request.
func (h *Handler) handleDeleteIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	err := h.API.DeleteIndex(r.Context(), indexName)
	if err != nil {
		h.Logger.Printf("problem deleting index: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(deleteIndexResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type deleteIndexResponse struct{}

// handlePostIndex handles POST /index request.
func (h *Handler) handlePostIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// Decode request.
	var req postIndexRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err == io.EOF {
		// If no data was provided (EOF), we still create the index
		// with default values.
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = h.API.CreateIndex(r.Context(), indexName, req.Options)
	if err == ErrIndexExists {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postIndexResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

// handlePostIndexAttrDiff handles POST /index/attr/diff requests.
func (h *Handler) handlePostIndexAttrDiff(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// Decode request.
	var req postIndexAttrDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	attrs, err := h.API.IndexAttrDiff(r.Context(), indexName, req.Blocks)
	if err != nil {
		if err == ErrIndexNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postIndexAttrDiffResponse{
		Attrs: attrs,
	}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type postIndexAttrDiffRequest struct {
	Blocks []AttrBlock `json:"blocks"`
}

type postIndexAttrDiffResponse struct {
	Attrs map[uint64]map[string]interface{} `json:"attrs"`
}

// handlePostFrame handles POST /frame request.
func (h *Handler) handlePostFrame(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]

	// Decode request.
	var req postFrameRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err == io.EOF {
		// If no data was provided (EOF), we still create the frame
		// with default values.
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_, err = h.API.CreateFrame(r.Context(), indexName, frameName, req.Options)
	if err != nil {
		switch err {
		case ErrIndexNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		case ErrFrameExists:
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	// Encode response.
	if err := json.NewEncoder(w).Encode(postFrameResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type _postFrameRequest postFrameRequest

// Custom Unmarshal JSON to validate request body when creating a new frame. If there's new FrameOptions,
// adding it to validFrameOptions to make sure the new option is validated, otherwise the request will be failed
func (p *postFrameRequest) UnmarshalJSON(b []byte) error {
	// m is an overflow map used to capture additional, unexpected keys.
	m := make(map[string]interface{})
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	validFrameOptions := getValidOptions(FrameOptions{})
	err := validateOptions(m, validFrameOptions)
	if err != nil {
		return err
	}

	// Unmarshal expected values.
	var _p _postFrameRequest
	if err := json.Unmarshal(b, &_p); err != nil {
		return err
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

type postFrameRequest struct {
	Options FrameOptions `json:"options"`
}

type postFrameResponse struct{}

// handleDeleteFrame handles DELETE /frame request.
func (h *Handler) handleDeleteFrame(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]

	err := h.API.DeleteFrame(r.Context(), indexName, frameName)
	if err != nil {
		if err == ErrIndexNotFound {
			if err := json.NewEncoder(w).Encode(deleteIndexResponse{}); err != nil {
				h.Logger.Printf("response encoding error: %s", err)
			}
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(deleteFrameResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type deleteFrameResponse struct{}

// handlePostFrameField handles POST /frame/field request.
func (h *Handler) handlePostFrameField(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]
	fieldName := mux.Vars(r)["field"]

	// Decode request.
	var req postFrameFieldRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	field := &Field{
		Name: fieldName,
		Type: req.Type,
		Min:  req.Min,
		Max:  req.Max,
	}

	if err := h.API.CreateField(r.Context(), indexName, frameName, field); err != nil {
		if err == ErrFrameNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postFrameFieldResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type postFrameFieldRequest struct {
	Type string `json:"type,omitempty"`
	Min  int64  `json:"min,omitempty"`
	Max  int64  `json:"max,omitempty"`
}

type postFrameFieldResponse struct{}

// handleDeleteFrameField handles DELETE /frame/field request.
func (h *Handler) handleDeleteFrameField(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]
	fieldName := mux.Vars(r)["field"]

	if err := h.API.DeleteField(r.Context(), indexName, frameName, fieldName); err != nil {
		if err == ErrFrameNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(deleteFrameFieldResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

func (h *Handler) handleGetFrameFields(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]

	fields, err := h.API.Fields(r.Context(), indexName, frameName)
	if err != nil {
		switch err {
		case ErrIndexNotFound:
			fallthrough
		case ErrFrameNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(getFrameFieldsResponse{Fields: fields}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type getFrameFieldsResponse struct {
	Fields []*Field `json:"fields,omitempty"`
}

type deleteFrameFieldRequest struct{}

type deleteFrameFieldResponse struct{}

// handleGetFrameViews handles GET /frame/views request.
func (h *Handler) handleGetFrameViews(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]

	views, err := h.API.Views(r.Context(), indexName, frameName)
	if err != nil {
		if err == ErrFrameNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	names := make([]string, len(views))
	for i := range views {
		names[i] = views[i].Name()
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(getFrameViewsResponse{Views: names}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

// handleDeleteView handles Delete /frame/view request.
func (h *Handler) handleDeleteView(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]
	viewName := mux.Vars(r)["view"]

	if err := h.API.DeleteView(r.Context(), indexName, frameName, viewName); err != nil {
		if err == ErrFrameNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(deleteViewResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type deleteViewResponse struct{}

type getFrameViewsResponse struct {
	Views []string `json:"views,omitempty"`
}

// handlePostFrameAttrDiff handles POST /frame/attr/diff requests.
func (h *Handler) handlePostFrameAttrDiff(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]

	// Decode request.
	var req postFrameAttrDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	attrs, err := h.API.FrameAttrDiff(r.Context(), indexName, frameName, req.Blocks)
	if err != nil {
		switch err {
		case ErrFragmentNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postFrameAttrDiffResponse{
		Attrs: attrs,
	}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type postFrameAttrDiffRequest struct {
	Blocks []AttrBlock `json:"blocks"`
}

type postFrameAttrDiffResponse struct {
	Attrs map[uint64]map[string]interface{} `json:"attrs"`
}

// readColumnAttrSets returns a list of column attribute objects by id.
func (h *Handler) readColumnAttrSets(index *Index, ids []uint64) ([]*ColumnAttrSet, error) {
	if index == nil {
		return nil, nil
	}

	a := make([]*ColumnAttrSet, 0, len(ids))
	for _, id := range ids {
		// Read attributes for column. Skip column if empty.
		attrs, err := index.ColumnAttrStore().Attrs(id)
		if err != nil {
			return nil, err
		} else if len(attrs) == 0 {
			continue
		}

		// Append column with attributes.
		a = append(a, &ColumnAttrSet{ID: id, Attrs: attrs})
	}

	return a, nil
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

// readProtobufQueryRequest parses query parameters in protobuf from r.
func (h *Handler) readProtobufQueryRequest(r *http.Request) (*QueryRequest, error) {
	// Slurp the body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	// Unmarshal into object.
	var req internal.QueryRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	return decodeQueryRequest(&req), nil
}

// readURLQueryRequest parses query parameters from URL parameters from r.
func (h *Handler) readURLQueryRequest(r *http.Request) (*QueryRequest, error) {
	q := r.URL.Query()

	// Parse query string.
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	query := string(buf)

	// Parse list of slices.
	slices, err := parseUint64Slice(q.Get("slices"))
	if err != nil {
		return nil, errors.New("invalid slice argument")
	}

	return &QueryRequest{
		Query:        query,
		Slices:       slices,
		ColumnAttrs:  q.Get("columnAttrs") == "true",
		ExcludeAttrs: q.Get("excludeAttrs") == "true",
		ExcludeBits:  q.Get("excludeBits") == "true",
	}, nil
}

// validOptions return all attributes of an interface with lower first character.
func validOptions(v interface{}) map[string]bool {
	validQuery := make(map[string]bool)
	argsType := reflect.ValueOf(v).Type()

	for i := 0; i < argsType.NumField(); i++ {
		fieldName := argsType.Field(i).Name
		chars := []rune(fieldName)
		chars[0] = unicode.ToLower(chars[0])
		fieldName = string(chars)
		validQuery[fieldName] = true
	}
	return validQuery
}

// writeQueryResponse writes the response from the executor to w.
func (h *Handler) writeQueryResponse(w http.ResponseWriter, r *http.Request, resp *QueryResponse) error {
	if strings.Contains(r.Header.Get("Accept"), "application/x-protobuf") {
		return h.writeProtobufQueryResponse(w, resp)
	}
	return h.writeJSONQueryResponse(w, resp)
}

// writeProtobufQueryResponse writes the response from the executor to w as protobuf.
func (h *Handler) writeProtobufQueryResponse(w http.ResponseWriter, resp *QueryResponse) error {
	if buf, err := proto.Marshal(encodeQueryResponse(resp)); err != nil {
		return err
	} else if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}

// writeJSONQueryResponse writes the response from the executor to w as JSON.
func (h *Handler) writeJSONQueryResponse(w http.ResponseWriter, resp *QueryResponse) error {
	return json.NewEncoder(w).Encode(resp)
}

// handlePostImport handles /import requests.
func (h *Handler) handlePostImport(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	} else if r.Header.Get("Accept") != "application/x-protobuf" {
		http.Error(w, "Not acceptable", http.StatusNotAcceptable)
		return
	}

	// Read entire body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Marshal into request object.
	var req internal.ImportRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.API.Import(r.Context(), req); err != nil {
		switch err {
		case ErrIndexNotFound:
			fallthrough
		case ErrFrameNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		case ErrClusterDoesNotOwnSlice:
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Marshal response object.
	buf, e := proto.Marshal(&internal.ImportResponse{Err: errorString(err)})
	if e != nil {
		http.Error(w, fmt.Sprintf("marshal import response: %s", err), http.StatusInternalServerError)
		return
	}

	// Write response.
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(buf)
}

// handlePostImportValue handles /import-value requests.
func (h *Handler) handlePostImportValue(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	} else if r.Header.Get("Accept") != "application/x-protobuf" {
		http.Error(w, "Not acceptable", http.StatusNotAcceptable)
		return
	}

	// Read entire body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Marshal into request object.
	var req internal.ImportValueRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err = h.API.ImportValue(r.Context(), req); err != nil {
		switch err {
		case ErrIndexNotFound:
			fallthrough
		case ErrFrameNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		case ErrClusterDoesNotOwnSlice:
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Marshal response object.
	buf, e := proto.Marshal(&internal.ImportResponse{Err: errorString(err)})
	if e != nil {
		http.Error(w, fmt.Sprintf("marshal import response: %s", err), http.StatusInternalServerError)
		return
	}

	// Write response.
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(buf)
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
	index, frame, view := q.Get("index"), q.Get("frame"), q.Get("view")

	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "invalid slice", http.StatusBadRequest)
		return
	}

	if err = h.API.ExportCSV(r.Context(), index, frame, view, slice, w); err != nil {
		switch err {
		case ErrFragmentNotFound:
			break
		case ErrClusterDoesNotOwnSlice:
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
}

// handleGetFragmentNodes handles /fragment/nodes requests.
func (h *Handler) handleGetFragmentNodes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	index := q.Get("index")

	// Read slice parameter.
	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "slice should be an unsigned integer", http.StatusBadRequest)
		return
	}

	// Retrieve fragment owner nodes.
	nodes, err := h.API.SliceNodes(r.Context(), index, slice)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Write to response.
	if err := json.NewEncoder(w).Encode(nodes); err != nil {
		h.Logger.Printf("json write error: %s", err)
	}
}

// handleGetFragmentData handles GET /fragment/data requests.
func (h *Handler) handleGetFragmentData(w http.ResponseWriter, r *http.Request) {
	// Read slice parameter.
	q := r.URL.Query()
	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "slice required", http.StatusBadRequest)
		return
	}

	// Retrieve fragment from holder.
	f, err := h.API.MarshalFragment(r.Context(), q.Get("index"), q.Get("frame"), q.Get("view"), slice)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Stream fragment to response body.
	if _, err := f.WriteTo(w); err != nil {
		h.Logger.Printf("fragment backup error: %s", err)
	}
}

// handlePostFragmentData handles POST /fragment/data requests.
func (h *Handler) handlePostFragmentData(w http.ResponseWriter, r *http.Request) {
	// Read slice parameter.
	q := r.URL.Query()
	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "slice required", http.StatusBadRequest)
		return
	}

	if err = h.API.UnmarshalFragment(r.Context(), q.Get("index"), q.Get("frame"), q.Get("view"), slice, r.Body); err != nil {
		if err == ErrFrameNotFound {
			http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// handleGetFragmentBlockData handles GET /fragment/block/data requests.
func (h *Handler) handleGetFragmentBlockData(w http.ResponseWriter, r *http.Request) {
	buf, err := h.API.FragmentBlockData(r.Context(), r.Body)
	if err != nil {
		if _, ok := err.(BadRequestError); ok {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if err == ErrFragmentNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Write response.
	w.Header().Set("Content-Type", "application/protobuf")
	w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	w.Write(buf)
}

// handleGetFragmentBlocks handles GET /fragment/blocks requests.
func (h *Handler) handleGetFragmentBlocks(w http.ResponseWriter, r *http.Request) {
	// Read slice parameter.
	q := r.URL.Query()
	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "slice required", http.StatusBadRequest)
		return
	}

	blocks, err := h.API.FragmentBlocks(r.Context(), q.Get("index"), q.Get("frame"), q.Get("view"), slice)
	if err != nil {
		if err == ErrFragmentNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(getFragmentBlocksResponse{
		Blocks: blocks,
	}); err != nil {
		h.Logger.Printf("block response encoding error: %s", err)
	}
}

type getFragmentBlocksResponse struct {
	Blocks []FragmentBlock `json:"blocks"`
}

// handlePostFrameRestore handles POST /frame/restore requests.
func (h *Handler) handlePostFrameRestore(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]

	q := r.URL.Query()
	hostStr := q.Get("host")

	// Validate query parameters.
	if hostStr == "" {
		http.Error(w, "host required", http.StatusBadRequest)
		return
	}

	host, err := NewURIFromAddress(hostStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	err = h.API.RestoreFrame(r.Context(), indexName, frameName, host)
	switch err {
	case nil:
		break
	case ErrFrameNotFound:
		fallthrough
	case ErrFragmentNotFound:
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleGetHosts handles /hosts requests.
func (h *Handler) handleGetHosts(w http.ResponseWriter, r *http.Request) {
	hosts := h.API.Hosts(r.Context())
	if err := json.NewEncoder(w).Encode(hosts); err != nil {
		h.Logger.Printf("write version response error: %s", err)
	}
}

// handleGetVersion handles /version requests.
func (h *Handler) handleGetVersion(w http.ResponseWriter, r *http.Request) {
	err := json.NewEncoder(w).Encode(struct {
		Version string `json:"version"`
	}{
		Version: h.API.Version(),
	})
	if err != nil {
		h.Logger.Printf("write version response error: %s", err)
	}
}

// QueryResult types.
const (
	QueryResultTypeNil uint32 = iota
	QueryResultTypeBitmap
	QueryResultTypePairs
	QueryResultTypeValCount
	QueryResultTypeUint64
	QueryResultTypeBool
)

// QueryRequest represent a request to process a query.
type QueryRequest struct {
	// Index to execute query against.
	Index string

	// The query string to parse and execute.
	Query string

	// The slices to include in the query execution.
	// If empty, all slices are included.
	Slices []uint64

	// Return column attributes, if true.
	ColumnAttrs bool

	// Do not return row attributes, if true.
	ExcludeAttrs bool

	// Do not return bits, if true.
	ExcludeBits bool

	// If true, indicates that query is part of a larger distributed query.
	// If false, this request is on the originating node.
	Remote bool
}

func decodeQueryRequest(pb *internal.QueryRequest) *QueryRequest {
	req := &QueryRequest{
		Query:        pb.Query,
		Slices:       pb.Slices,
		ColumnAttrs:  pb.ColumnAttrs,
		Remote:       pb.Remote,
		ExcludeAttrs: pb.ExcludeAttrs,
		ExcludeBits:  pb.ExcludeBits,
	}

	return req
}

// QueryResponse represent a response from a processed query.
type QueryResponse struct {
	// Result for each top-level query call.
	// Can be a Bitmap, Pairs, or uint64.
	Results []interface{}

	// Set of column attribute objects matching IDs returned in Result.
	ColumnAttrSets []*ColumnAttrSet

	// Error during parsing or execution.
	Err error
}

// MarshalJSON marshals QueryResponse into a JSON-encoded byte slice
func (resp *QueryResponse) MarshalJSON() ([]byte, error) {
	var output struct {
		Results        []interface{}    `json:"results,omitempty"`
		ColumnAttrSets []*ColumnAttrSet `json:"columnAttrs,omitempty"`
		Err            string           `json:"error,omitempty"`
	}
	output.Results = resp.Results
	output.ColumnAttrSets = resp.ColumnAttrSets

	if resp.Err != nil {
		output.Err = resp.Err.Error()
	}
	return json.Marshal(output)
}

func encodeQueryResponse(resp *QueryResponse) *internal.QueryResponse {
	pb := &internal.QueryResponse{
		Results:        make([]*internal.QueryResult, len(resp.Results)),
		ColumnAttrSets: encodeColumnAttrSets(resp.ColumnAttrSets),
	}

	for i := range resp.Results {
		pb.Results[i] = &internal.QueryResult{}

		switch result := resp.Results[i].(type) {
		case *Bitmap:
			pb.Results[i].Type = QueryResultTypeBitmap
			pb.Results[i].Bitmap = encodeBitmap(result)
		case []Pair:
			pb.Results[i].Type = QueryResultTypePairs
			pb.Results[i].Pairs = encodePairs(result)
		case ValCount:
			pb.Results[i].Type = QueryResultTypeValCount
			pb.Results[i].ValCount = encodeValCount(result)
		case uint64:
			pb.Results[i].Type = QueryResultTypeUint64
			pb.Results[i].N = result
		case bool:
			pb.Results[i].Type = QueryResultTypeBool
			pb.Results[i].Changed = result
		case nil:
			pb.Results[i].Type = QueryResultTypeNil
		}
	}

	if resp.Err != nil {
		pb.Err = resp.Err.Error()
	}

	return pb
}

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
			return nil, err
		}
		a = append(a, num)
	}
	return a, nil
}

// errorString returns the string representation of err.
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// handlePostInputDefinition handles POST /input-definition request.
func (h *Handler) handlePostInputDefinition(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	inputDefName := mux.Vars(r)["input-definition"]

	// Decode request.
	var req InputDefinitionInfo
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err = h.API.CreateInputDefinition(r.Context(), indexName, inputDefName, req); err != nil {
		switch err {
		case ErrIndexNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		case ErrInputDefinitionExists:
			http.Error(w, err.Error(), http.StatusConflict)
		case ErrInputDefinitionAttrsRequired:
			fallthrough
		case ErrInputDefinitionNameRequired:
			fallthrough
		case ErrInputDefinitionActionRequired:
			fallthrough
		case ErrInputDefinitionHasPrimaryKey:
			fallthrough
		case ErrInputDefinitionDupePrimaryKey:
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	if err := json.NewEncoder(w).Encode(defaultInputDefinitionResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

// handleGetInputDefinition handles GET /input-definition request.
func (h *Handler) handleGetInputDefinition(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	inputDefName := mux.Vars(r)["input-definition"]

	inputDef, err := h.API.InputDefinition(r.Context(), indexName, inputDefName)
	if err != nil {
		switch err {
		case nil:
			break
		case ErrIndexNotFound:
			fallthrough
		case ErrInputDefinitionNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	if err = json.NewEncoder(w).Encode(InputDefinitionInfo{
		Frames: inputDef.frames,
		Fields: inputDef.fields,
	}); err != nil {
		h.Logger.Printf("write status response error: %s", err)
	}
}

// handleDeleteInputDefinition handles DELETE /input-definition request.
func (h *Handler) handleDeleteInputDefinition(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	inputDefName := mux.Vars(r)["input-definition"]

	if err := h.API.DeleteInputDefinition(r.Context(), indexName, inputDefName); err != nil {
		switch err {
		case nil:
			break
		case ErrIndexNotFound:
			fallthrough
		case ErrInputDefinitionNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusNotFound)
		}
		return
	}

	if err := json.NewEncoder(w).Encode(defaultInputDefinitionResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type defaultInputDefinitionResponse struct{}

func (h *Handler) handlePostInput(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	inputDefName := mux.Vars(r)["input-definition"]

	// Decode request.
	var reqs []interface{}
	err := json.NewDecoder(r.Body).Decode(&reqs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err = h.API.WriteInput(r.Context(), indexName, inputDefName, reqs); err != nil {
		switch err {
		case nil:
			break
		case ErrIndexNotFound:
			fallthrough
		case ErrInputDefinitionNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	if err := json.NewEncoder(w).Encode(defaultInputDefinitionResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

func (h *Handler) handlePostClusterResizeSetCoordinator(w http.ResponseWriter, r *http.Request) {
	// Decode request.
	var req setCoordinatorRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "decoding request "+err.Error(), http.StatusBadRequest)
		return
	}

	oldNode, newNode, err := h.API.SetCoordinator(r.Context(), req.ID)
	if err != nil {
		if errors.Cause(err) == ErrNodeIDNotExists {
			http.Error(w, "setting new coordinator: "+err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, "setting new coordinator: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	// Encode response.
	if err := json.NewEncoder(w).Encode(setCoordinatorResponse{
		Old: oldNode,
		New: newNode,
	}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type setCoordinatorRequest struct {
	ID string `json:"id"`
}

type setCoordinatorResponse struct {
	Old *Node `json:"old"`
	New *Node `json:"new"`
}

// handlePostClusterResizeRemoveNode handles POST /cluster/resize/remove-node request.
func (h *Handler) handlePostClusterResizeRemoveNode(w http.ResponseWriter, r *http.Request) {
	// Decode request.
	var req removeNodeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	removeNode, err := h.API.RemoveNode(req.ID)
	if err != nil {
		if errors.Cause(err) == ErrNodeIDNotExists {
			http.Error(w, "removing node: "+err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, "removing node: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(removeNodeResponse{
		Remove: removeNode,
	}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type removeNodeRequest struct {
	ID string `json:"id"`
}

type removeNodeResponse struct {
	Remove *Node `json:"remove"`
}

// handlePostClusterResizeAbort handles POST /cluster/resize/abort request.
func (h *Handler) handlePostClusterResizeAbort(w http.ResponseWriter, r *http.Request) {
	err := h.API.ResizeAbort()
	var msg string
	if err != nil {
		switch errors.Cause(err) {
		case ErrNodeNotCoordinator:
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		case ErrResizeNotRunning:
			msg = err.Error()
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	// Encode response.
	if err := json.NewEncoder(w).Encode(clusterResizeAbortResponse{
		Info: msg,
	}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

type clusterResizeAbortResponse struct {
	Info string `json:"info"`
}

func (h *Handler) handleRecalculateCaches(w http.ResponseWriter, r *http.Request) {
	err := h.API.RecalculateCaches(r.Context())
	if err != nil {
		http.Error(w, "recalculating caches: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetTimeStamp retrieves unix timestamp from Input data.
func GetTimeStamp(data map[string]interface{}, timeField string) (int64, error) {
	tmstamp, ok := data[timeField]
	if !ok {
		return 0, nil
	}

	timestamp, ok := tmstamp.(string)
	if !ok {
		return 0, fmt.Errorf("set-timestamp value must be in time format: YYYY-MM-DD, has: %v", data[timeField])
	}

	v, err := time.Parse(TimeFormat, timestamp)
	if err != nil {
		return 0, err
	}

	return v.Unix(), nil
}

func (h *Handler) handlePostClusterMessage(w http.ResponseWriter, r *http.Request) {
	// Verify that request is only communicating over protobufs.
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	}

	err := h.API.ClusterMessage(r.Context(), r.Body)
	if err != nil {
		// TODO this was the previous behavior, but perhaps not everything is a bad request
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	if err := json.NewEncoder(w).Encode(defaultClusterMessageResponse{}); err != nil {
		h.Logger.Printf("response encoding error: %s", err)
	}
}

func (h *Handler) handleGetID(w http.ResponseWriter, r *http.Request) {
	_, err := w.Write([]byte(h.API.LocalID()))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type defaultClusterMessageResponse struct{}

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
