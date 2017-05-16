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

//go:generate statik -src=./webui

package pilosa

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"reflect"

	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"

	_ "github.com/pilosa/pilosa/statik"
	"github.com/rakyll/statik/fs"
)

// Handler represents an HTTP handler.
type Handler struct {
	Holder        *Holder
	Broadcaster   Broadcaster
	StatusHandler StatusHandler

	// Local hostname & cluster configuration.
	Host    string
	Cluster *Cluster

	Router *mux.Router

	// The execution engine for running queries.
	Executor interface {
		Execute(context context.Context, index string, query *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error)
	}

	// The version to report on the /version endpoint.
	Version string

	// The writer for any logging.
	LogOutput io.Writer
}

// NewHandler returns a new instance of Handler with a default logger.
func NewHandler() *Handler {
	handler := &Handler{
		LogOutput: os.Stderr,
	}
	handler.Router = NewRouter(handler)
	return handler
}

// NewRouter creates a Gorilla Mux http router.
func NewRouter(handler *Handler) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/", handler.handleWebUI).Methods("GET")
	router.HandleFunc("/assets/{file}", handler.handleWebUI).Methods("GET")
	router.HandleFunc("/index", handler.handleGetIndexes).Methods("GET")
	router.HandleFunc("/index/{index}", handler.handleGetIndex).Methods("GET")
	router.HandleFunc("/index/{index}", handler.handlePostIndex).Methods("POST")
	router.HandleFunc("/index/{index}", handler.handleDeleteIndex).Methods("DELETE")
	router.HandleFunc("/index/{index}/attr/diff", handler.handlePostIndexAttrDiff).Methods("POST")
	//router.HandleFunc("/index/{index}/frame", handler.handleGetFrames).Methods("GET") // Not implemented.
	router.HandleFunc("/index/{index}/frame/{frame}", handler.handlePostFrame).Methods("POST")
	router.HandleFunc("/index/{index}/frame/{frame}", handler.handleDeleteFrame).Methods("DELETE")
	router.HandleFunc("/index/{index}/query", handler.handlePostQuery).Methods("POST")
	router.HandleFunc("/index/{index}/frame/{frame}/attr/diff", handler.handlePostFrameAttrDiff).Methods("POST")
	router.HandleFunc("/index/{index}/frame/{frame}/restore", handler.handlePostFrameRestore).Methods("POST")
	router.HandleFunc("/index/{index}/frame/{frame}/time-quantum", handler.handlePatchFrameTimeQuantum).Methods("PATCH")
	router.HandleFunc("/index/{index}/frame/{frame}/views", handler.handleGetFrameViews).Methods("GET")
	router.HandleFunc("/index/{index}/time-quantum", handler.handlePatchIndexTimeQuantum).Methods("PATCH")
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux).Methods("GET")
	router.HandleFunc("/debug/vars", handler.handleExpvar).Methods("GET")
	router.HandleFunc("/export", handler.handleGetExport).Methods("GET")
	router.HandleFunc("/fragment/block/data", handler.handleGetFragmentBlockData).Methods("GET")
	router.HandleFunc("/fragment/blocks", handler.handleGetFragmentBlocks).Methods("GET")
	router.HandleFunc("/fragment/data", handler.handleGetFragmentData).Methods("GET")
	router.HandleFunc("/fragment/data", handler.handlePostFragmentData).Methods("POST")
	router.HandleFunc("/fragment/nodes", handler.handleGetFragmentNodes).Methods("GET")
	router.HandleFunc("/import", handler.handlePostImport).Methods("POST")
	router.HandleFunc("/hosts", handler.handleGetHosts).Methods("GET")
	router.HandleFunc("/schema", handler.handleGetSchema).Methods("GET")
	router.HandleFunc("/slices/max", handler.handleGetSliceMax).Methods("GET")
	router.HandleFunc("/status", handler.handleGetStatus).Methods("GET")
	router.HandleFunc("/version", handler.handleGetVersion).Methods("GET")

	// TODO: Apply MethodNotAllowed statuses to all endpoints.
	// Ideally this would be automatic, as described in this (wontfix) ticket:
	// https://github.com/gorilla/mux/issues/6
	// For now we just do it for the most commonly used handler, /query
	router.HandleFunc("/index/{index}/query", handler.methodNotAllowedHandler).Methods("GET")

	return router
}

func (h *Handler) methodNotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

// ServeHTTP handles an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.Router.ServeHTTP(w, r)
}

func (h *Handler) handleWebUI(w http.ResponseWriter, r *http.Request) {
	// If user is using curl, don't chuck HTML at them
	if strings.HasPrefix(r.UserAgent(), "curl") {
		http.Error(w, "Welcome. Pilosa is running. Visit https://www.pilosa.com/docs/ for more information or try the WebUI by visiting this URL in your browser.", http.StatusNotFound)
		return
	}
	statikFS, err := fs.New()
	if err != nil {
		h.writeQueryResponse(w, r, &QueryResponse{Err: err})
		h.logger().Println("Pilosa WebUI is not available. Please run `make generate-statik` before building Pilosa with `make install`.")
		return
	}
	http.FileServer(statikFS).ServeHTTP(w, r)
}

// handleGetSchema handles GET /schema requests.
func (h *Handler) handleGetSchema(w http.ResponseWriter, r *http.Request) {
	if err := json.NewEncoder(w).Encode(getSchemaResponse{
		Indexes: h.Holder.Schema(),
	}); err != nil {
		h.logger().Printf("write schema response error: %s", err)
	}
}

// handleGetStatus handles GET /status requests.
func (h *Handler) handleGetStatus(w http.ResponseWriter, r *http.Request) {
	status, err := h.StatusHandler.ClusterStatus()
	if err != nil {
		h.logger().Printf("cluster status error: %s", err)
		return
	}
	if err := json.NewEncoder(w).Encode(getStatusResponse{
		Status: status,
	}); err != nil {
		h.logger().Printf("write status response error: %s", err)
	}
}

type getSchemaResponse struct {
	Indexes []*IndexInfo `json:"indexes"`
}

type getStatusResponse struct {
	Status proto.Message `json:"status"`
}

// handlePostQuery handles /query requests.
func (h *Handler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// Parse incoming request.
	req, err := h.readQueryRequest(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		h.writeQueryResponse(w, r, &QueryResponse{Err: err})
		return
	}

	// Build execution options.
	opt := &ExecOptions{
		Remote: req.Remote,
	}

	// Parse query string.
	q, err := pql.NewParser(strings.NewReader(req.Query)).Parse()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		h.writeQueryResponse(w, r, &QueryResponse{Err: err})
		return
	}

	// Execute the query.
	results, err := h.Executor.Execute(r.Context(), indexName, q, req.Slices, opt)
	resp := &QueryResponse{Results: results, Err: err}

	// Fill column attributes if requested.
	if req.ColumnAttrs {
		// Consolidate all column ids across all calls.
		var columnIDs []uint64
		for _, result := range results {
			bm, ok := result.(*Bitmap)
			if !ok {
				continue
			}
			columnIDs = uint64Slice(columnIDs).merge(bm.Bits())
		}

		// Retrieve column attributes across all calls.
		columnAttrSets, err := h.readColumnAttrSets(h.Holder.Index(indexName), columnIDs)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			h.writeQueryResponse(w, r, &QueryResponse{Err: err})
			return
		}
		resp.ColumnAttrSets = columnAttrSets
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
	if err := h.writeQueryResponse(w, r, resp); err != nil {
		h.logger().Printf("write query response error: %s", err)
	}
}

func (h *Handler) handleGetSliceMax(w http.ResponseWriter, r *http.Request) {
	var ms map[string]uint64
	if inverse, _ := strconv.ParseBool(r.URL.Query().Get("inverse")); inverse {
		ms = h.Holder.MaxInverseSlices()
	} else {
		ms = h.Holder.MaxSlices()
	}
	if strings.Contains(r.Header.Get("Accept"), "application/x-protobuf") {
		pb := &internal.MaxSlicesResponse{
			MaxSlices: ms,
		}
		if buf, err := proto.Marshal(pb); err != nil {
			h.logger().Printf("protobuf marshal error: %s", err)
		} else if _, err := w.Write(buf); err != nil {
			h.logger().Printf("stream write error: %s", err)
		}
		return
	}
	json.NewEncoder(w).Encode(sliceMaxResponse{
		MaxSlices: ms,
	})
}

type sliceMaxResponse struct {
	MaxSlices map[string]uint64 `json:"maxSlices"`
}

// handleGetIndexes handles GET /index request.
func (h *Handler) handleGetIndexes(w http.ResponseWriter, r *http.Request) {
	h.handleGetSchema(w, r)
}

// handleGetIndex handles GET /index/<indexname> requests.
func (h *Handler) handleGetIndex(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	index := h.Holder.Index(indexName)
	if index == nil {
		http.Error(w, ErrIndexNotFound.Error(), http.StatusNotFound)
		return
	}

	if err := json.NewEncoder(w).Encode(getIndexResponse{
		map[string]string{"name": index.Name()},
	}); err != nil {
		h.logger().Printf("write response error: %s", err)
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

	// Delete index from the holder.
	if err := h.Holder.DeleteIndex(indexName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send the delete index message to all nodes.
	err := h.Broadcaster.SendSync(
		&internal.DeleteIndexMessage{
			Index: indexName,
		})
	if err != nil {
		h.logger().Printf("problem sending DeleteIndex message: %s", err)
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(deleteIndexResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
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

	// Create index.
	_, err = h.Holder.CreateIndex(indexName, req.Options)
	if err == ErrIndexExists {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send the create index message to all nodes.
	err = h.Broadcaster.SendSync(
		&internal.CreateIndexMessage{
			Index: indexName,
			Meta:  req.Options.Encode(),
		})
	if err != nil {
		h.logger().Printf("problem sending CreateIndex message: %s", err)
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postIndexResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

// handlePatchIndexTimeQuantum handles PATCH /index/time_quantum request.
func (h *Handler) handlePatchIndexTimeQuantum(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// Decode request.
	var req patchIndexTimeQuantumRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate quantum.
	tq, err := ParseTimeQuantum(req.TimeQuantum)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Retrieve index by name.
	index := h.Holder.Index(indexName)
	if index == nil {
		http.Error(w, ErrIndexNotFound.Error(), http.StatusNotFound)
		return
	}

	// Set default time quantum on index.
	if err := index.SetTimeQuantum(tq); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(patchIndexTimeQuantumResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

type patchIndexTimeQuantumRequest struct {
	TimeQuantum string `json:"timeQuantum"`
}

type patchIndexTimeQuantumResponse struct{}

// handlePostIndexAttrDiff handles POST /index/attr/diff requests.
func (h *Handler) handlePostIndexAttrDiff(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]

	// Decode request.
	var req postIndexAttrDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Retrieve index from holder.
	index := h.Holder.Index(indexName)
	if index == nil {
		http.Error(w, ErrIndexNotFound.Error(), http.StatusNotFound)
		return
	}

	// Retrieve local blocks.
	blks, err := index.ColumnAttrStore().Blocks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Read all attributes from all mismatched blocks.
	attrs := make(map[uint64]map[string]interface{})
	for _, blockID := range AttrBlocks(blks).Diff(req.Blocks) {
		// Retrieve block data.
		m, err := index.ColumnAttrStore().BlockData(blockID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Copy to index-wide struct.
		for k, v := range m {
			attrs[k] = v
		}
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postIndexAttrDiffResponse{
		Attrs: attrs,
	}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
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

	// Find index.
	index := h.Holder.Index(indexName)
	if index == nil {
		http.Error(w, ErrIndexNotFound.Error(), http.StatusNotFound)
		return
	}

	// Create frame.
	_, err = index.CreateFrame(frameName, req.Options)
	if err == ErrFrameExists {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send the create frame message to all nodes.
	err = h.Broadcaster.SendSync(
		&internal.CreateFrameMessage{
			Index: indexName,
			Frame: frameName,
			Meta:  req.Options.Encode(),
		})
	if err != nil {
		h.logger().Printf("problem sending CreateFrame message: %s", err)
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postFrameResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
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

	// Find index.
	index := h.Holder.Index(indexName)
	if index == nil {
		if err := json.NewEncoder(w).Encode(deleteIndexResponse{}); err != nil {
			h.logger().Printf("response encoding error: %s", err)
		}
		return
	}

	// Delete frame from the index.
	if err := index.DeleteFrame(frameName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send the delete frame message to all nodes.
	err := h.Broadcaster.SendSync(
		&internal.DeleteFrameMessage{
			Index: indexName,
			Frame: frameName,
		})
	if err != nil {
		h.logger().Printf("problem sending DeleteFrame message: %s", err)
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(deleteFrameResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

type deleteFrameResponse struct{}

// handlePatchFrameTimeQuantum handles PATCH /frame/time_quantum request.
func (h *Handler) handlePatchFrameTimeQuantum(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]

	// Decode request.
	var req patchFrameTimeQuantumRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate quantum.
	tq, err := ParseTimeQuantum(req.TimeQuantum)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Retrieve index by name.
	f := h.Holder.Frame(indexName, frameName)
	if f == nil {
		http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		return
	}

	// Set default time quantum on index.
	if err := f.SetTimeQuantum(tq); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(patchFrameTimeQuantumResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

type patchFrameTimeQuantumRequest struct {
	TimeQuantum string `json:"timeQuantum"`
}

type patchFrameTimeQuantumResponse struct{}

// handleGetFrameViews handles GET /frame/views request.
func (h *Handler) handleGetFrameViews(w http.ResponseWriter, r *http.Request) {
	indexName := mux.Vars(r)["index"]
	frameName := mux.Vars(r)["frame"]

	// Retrieve views.
	f := h.Holder.Frame(indexName, frameName)
	if f == nil {
		http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		return
	}

	// Fetch views.
	views := f.Views()
	names := make([]string, len(views))
	for i := range views {
		names[i] = views[i].Name()
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(getFrameViewsResponse{Views: names}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

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

	// Retrieve index from holder.
	f := h.Holder.Frame(indexName, frameName)
	if f == nil {
		http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		return
	}

	// Retrieve local blocks.
	blks, err := f.RowAttrStore().Blocks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Read all attributes from all mismatched blocks.
	attrs := make(map[uint64]map[string]interface{})
	for _, blockID := range AttrBlocks(blks).Diff(req.Blocks) {
		// Retrieve block data.
		m, err := f.RowAttrStore().BlockData(blockID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Copy to index-wide struct.
		for k, v := range m {
			attrs[k] = v
		}
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postFrameAttrDiffResponse{
		Attrs: attrs,
	}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
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

	// Parse time granularity.
	quantum := TimeQuantum("YMDH")
	if s := q.Get("time_granularity"); s != "" {
		v, err := ParseTimeQuantum(s)
		if err != nil {
			return nil, errors.New("invalid time granularity")
		}
		quantum = v
	}

	return &QueryRequest{
		Query:       query,
		Slices:      slices,
		ColumnAttrs: q.Get("columnAttrs") == "true",
		Quantum:     quantum,
	}, nil
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

	// Convert timestamps to time.Time.
	timestamps := make([]*time.Time, len(req.Timestamps))
	for i, ts := range req.Timestamps {
		if ts == 0 {
			continue
		}
		t := time.Unix(0, ts)
		timestamps[i] = &t
	}

	// Validate that this handler owns the slice.
	if !h.Cluster.OwnsFragment(h.Host, req.Index, req.Slice) {
		mesg := fmt.Sprintf("host does not own slice %s-%s slice:%d", h.Host, req.Index, req.Slice)
		http.Error(w, mesg, http.StatusPreconditionFailed)
		return
	}

	// Find the Index.
	h.logger().Println("importing:", req.Index, req.Frame, req.Slice)
	index := h.Holder.Index(req.Index)
	if index == nil {
		h.logger().Printf("fragment error: index=%s, frame=%s, slice=%d, err=%s", req.Index, req.Frame, req.Slice, ErrIndexNotFound.Error())
		http.Error(w, ErrIndexNotFound.Error(), http.StatusNotFound)
		return
	}

	// Retrieve frame.
	f := index.Frame(req.Frame)
	if f == nil {
		h.logger().Printf("frame error: index=%s, frame=%s, slice=%d, err=%s", req.Index, req.Frame, req.Slice, ErrFrameNotFound.Error())
		http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		return
	}

	// Import into fragment.
	err = f.Import(req.RowIDs, req.ColumnIDs, timestamps)
	if err != nil {
		h.logger().Printf("import error: index=%s, frame=%s, slice=%d, bits=%d, err=%s", req.Index, req.Frame, req.Slice, len(req.ColumnIDs), err)
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

	// Validate that this handler owns the slice.
	if !h.Cluster.OwnsFragment(h.Host, index, slice) {
		mesg := fmt.Sprintf("host does not own slice %s-%s slice:%d", h.Host, index, slice)
		http.Error(w, mesg, http.StatusPreconditionFailed)
		return
	}

	// Find the fragment.
	f := h.Holder.Fragment(index, frame, view, slice)
	if f == nil {
		return
	}

	// Wrap writer with a CSV writer.
	cw := csv.NewWriter(w)

	// Iterate over each bit.
	if err := f.ForEachBit(func(rowID, columnID uint64) error {
		return cw.Write([]string{
			strconv.FormatUint(rowID, 10),
			strconv.FormatUint(columnID, 10),
		})
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Ensure data is flushed.
	cw.Flush()
}

// handleGetFragmentNodes handles /fragment/nodes requests.
func (h *Handler) handleGetFragmentNodes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	index := q.Get("index")

	// Read slice parameter.
	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "slice required", http.StatusBadRequest)
		return
	}

	// Retrieve fragment owner nodes.
	nodes := h.Cluster.FragmentNodes(index, slice)

	// Write to response.
	if err := json.NewEncoder(w).Encode(nodes); err != nil {
		h.logger().Printf("json write error: %s", err)
	}
}

// handleGetFragmentBackup handles GET /fragment/data requests.
func (h *Handler) handleGetFragmentData(w http.ResponseWriter, r *http.Request) {
	// Read slice parameter.
	q := r.URL.Query()
	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "slice required", http.StatusBadRequest)
		return
	}

	// Retrieve fragment from holder.
	f := h.Holder.Fragment(q.Get("index"), q.Get("frame"), q.Get("view"), slice)
	if f == nil {
		http.Error(w, "fragment not found", http.StatusNotFound)
		return
	}

	// Stream fragment to response body.
	if _, err := f.WriteTo(w); err != nil {
		h.logger().Printf("fragment backup error: %s", err)
	}
}

// handlePostFragmentRestore handles POST /fragment/data requests.
func (h *Handler) handlePostFragmentData(w http.ResponseWriter, r *http.Request) {
	// Read slice parameter.
	q := r.URL.Query()
	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "slice required", http.StatusBadRequest)
		return
	}

	// Retrieve frame.
	f := h.Holder.Frame(q.Get("index"), q.Get("frame"))
	if f == nil {
		http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		return
	}

	// Retrieve view.
	view, err := f.CreateViewIfNotExists(q.Get("view"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Retrieve fragment from frame.
	frag, err := view.CreateFragmentIfNotExists(slice)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Read fragment in from request body.
	if _, err := frag.ReadFrom(r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleGetFragmentData handles GET /fragment/block/data requests.
func (h *Handler) handleGetFragmentBlockData(w http.ResponseWriter, r *http.Request) {
	// Read request object.
	var req internal.BlockDataRequest
	if body, err := ioutil.ReadAll(r.Body); err != nil {
		http.Error(w, "ready body error", http.StatusBadRequest)
		return
	} else if err := proto.Unmarshal(body, &req); err != nil {
		http.Error(w, "unmarshal body error", http.StatusBadRequest)
		return
	}

	// Retrieve fragment from holder.
	f := h.Holder.Fragment(req.Index, req.Frame, req.View, req.Slice)
	if f == nil {
		http.Error(w, ErrFragmentNotFound.Error(), http.StatusNotFound)
		return
	}

	// Read data
	var resp internal.BlockDataResponse
	if f != nil {
		resp.RowIDs, resp.ColumnIDs = f.BlockData(int(req.Block))
	}

	// Encode response.
	buf, err := proto.Marshal(&resp)
	if err != nil {
		h.logger().Printf("merge block response encoding error: %s", err)
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

	// Retrieve fragment from holder.
	f := h.Holder.Fragment(q.Get("index"), q.Get("frame"), q.Get("view"), slice)
	if f == nil {
		http.Error(w, "fragment not found", http.StatusNotFound)
		return
	}

	// Retrieve blocks.
	blocks := f.Blocks()

	// Encode response.
	if err := json.NewEncoder(w).Encode(getFragmentBlocksResponse{
		Blocks: blocks,
	}); err != nil {
		h.logger().Printf("block response encoding error: %s", err)
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
	host := q.Get("host")

	// Validate query parameters.
	if host == "" {
		http.Error(w, "host required", http.StatusBadRequest)
		return
	}

	// Create a client for the remote cluster.
	client, err := NewClient(host)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Determine the maximum number of slices.
	maxSlices, err := client.MaxSliceByIndex(r.Context())
	if err != nil {
		http.Error(w, "cannot determine remote slice count: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Retrieve frame.
	f := h.Holder.Frame(indexName, frameName)
	if f == nil {
		http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		return
	}

	// Retrieve list of all views.
	views, err := client.FrameViews(r.Context(), indexName, frameName)
	if err != nil {
		http.Error(w, "cannot retrieve frame views: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Loop over each slice and import it if this node owns it.
	for slice := uint64(0); slice <= maxSlices[indexName]; slice++ {
		// Ignore this slice if we don't own it.
		if !h.Cluster.OwnsFragment(h.Host, indexName, slice) {
			continue
		}

		// Loop over view names.
		for _, view := range views {
			// Create view.
			v, err := f.CreateViewIfNotExists(view)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// Otherwise retrieve the local fragment.
			frag, err := v.CreateFragmentIfNotExists(slice)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// Stream backup from remote node.
			rd, err := client.BackupSlice(r.Context(), indexName, frameName, view, slice)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			} else if rd == nil {
				continue // slice doesn't exist
			}

			// Restore to local frame and always close reader.
			if err := func() error {
				defer rd.Close()
				if _, err := frag.ReadFrom(rd); err != nil {
					return err
				}
				return nil
			}(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
}

// handleGetHosts handles /hosts requests.
func (h *Handler) handleGetHosts(w http.ResponseWriter, r *http.Request) {
	if err := json.NewEncoder(w).Encode(h.Cluster.Nodes); err != nil {
		h.logger().Printf("write version response error: %s", err)
	}
}

// handleGetVersion handles /version requests.
func (h *Handler) handleGetVersion(w http.ResponseWriter, r *http.Request) {
	if err := json.NewEncoder(w).Encode(struct {
		Version string `json:"version"`
	}{
		Version: h.Version,
	}); err != nil {
		h.logger().Printf("write version response error: %s", err)
	}
}

// handleExpvar handles /debug/vars requests.
func (h *Handler) handleExpvar(w http.ResponseWriter, r *http.Request) {
	// Copied from $GOROOT/src/expvar/expvar.go
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

// logger returns a logger for the handler.
func (h *Handler) logger() *log.Logger {
	return log.New(h.LogOutput, "", log.LstdFlags)
}

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

	// Time granularity to use with the timestamp.
	Quantum TimeQuantum

	// If true, indicates that query is part of a larger distributed query.
	// If false, this request is on the originating node.
	Remote bool
}

func decodeQueryRequest(pb *internal.QueryRequest) *QueryRequest {
	req := &QueryRequest{
		Query:       pb.Query,
		Slices:      pb.Slices,
		ColumnAttrs: pb.ColumnAttrs,
		Quantum:     TimeQuantum(pb.Quantum),
		Remote:      pb.Remote,
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
			pb.Results[i].Bitmap = encodeBitmap(result)
		case []Pair:
			pb.Results[i].Pairs = encodePairs(result)
		case uint64:
			pb.Results[i].N = result
		case bool:
			pb.Results[i].Changed = result
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
