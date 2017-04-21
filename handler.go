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

	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

// Handler represents an HTTP handler.
type Handler struct {
	Index *Index

	// Local hostname & cluster configuration.
	Host    string
	Cluster *Cluster

	Router *mux.Router

	// The execution engine for running queries.
	Executor interface {
		Execute(context context.Context, db string, query *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error)
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

func NewRouter(handler *Handler) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/db", handler.handleGetDBs).Methods("GET")
	router.HandleFunc("/db/{db}", handler.handleGetDB).Methods("GET")
	router.HandleFunc("/db/{db}", handler.handlePostDB).Methods("POST")
	router.HandleFunc("/db/{db}", handler.handleDeleteDB).Methods("DELETE")
	router.HandleFunc("/db/{db}/attr/diff", handler.handlePostDBAttrDiff).Methods("POST")
	//router.HandleFunc("/db/{db}/frame", handler.handleGetFrames).Methods("GET") // Not implemented.
	router.HandleFunc("/db/{db}/frame/{frame}", handler.handlePostFrame).Methods("POST")
	router.HandleFunc("/db/{db}/frame/{frame}", handler.handleDeleteFrame).Methods("DELETE")
	router.HandleFunc("/db/{db}/query", handler.handlePostQuery).Methods("POST")
	router.HandleFunc("/db/{db}/frame/{frame}/attr/diff", handler.handlePostFrameAttrDiff).Methods("POST")
	router.HandleFunc("/db/{db}/frame/{frame}/restore", handler.handlePostFrameRestore).Methods("POST")
	router.HandleFunc("/db/{db}/frame/{frame}/time-quantum", handler.handlePatchFrameTimeQuantum).Methods("PATCH")
	router.HandleFunc("/db/{db}/frame/{frame}/views", handler.handleGetFrameViews).Methods("GET")
	router.HandleFunc("/db/{db}/time-quantum", handler.handlePatchDBTimeQuantum).Methods("PATCH")
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux).Methods("GET")
	router.HandleFunc("/debug/vars", handler.handleExpvar).Methods("GET")
	router.HandleFunc("/export", handler.handleGetExport).Methods("GET")
	router.HandleFunc("/fragment/block/data", handler.handleGetFragmentBlockData).Methods("GET")
	router.HandleFunc("/fragment/blocks", handler.handleGetFragmentBlocks).Methods("GET")
	router.HandleFunc("/fragment/data", handler.handleGetFragmentData).Methods("GET")
	router.HandleFunc("/fragment/data", handler.handlePostFragmentData).Methods("POST")
	router.HandleFunc("/fragment/nodes", handler.handleGetFragmentNodes).Methods("GET")
	router.HandleFunc("/import", handler.handlePostImport).Methods("POST")
	router.HandleFunc("/nodes", handler.handleGetNodes).Methods("GET")
	router.HandleFunc("/schema", handler.handleGetSchema).Methods("GET")
	router.HandleFunc("/slices/max", handler.handleGetSliceMax).Methods("GET")
	router.HandleFunc("/version", handler.handleGetVersion).Methods("GET")

	// TODO: Apply MethodNotAllowed statuses to all endpoints.
	// Ideally this would be automatic, as described in this (wontfix) ticket:
	// https://github.com/gorilla/mux/issues/6
	// For now we just do it for the most commonly used handler, /query
	router.HandleFunc("/db/{db}/query", handler.methodNotAllowedHandler).Methods("GET")

	return router
}

func (h *Handler) methodNotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

// ServeHTTP handles an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.Router.ServeHTTP(w, r)
}

// handleGetSchema handles GET /schema requests.
func (h *Handler) handleGetSchema(w http.ResponseWriter, r *http.Request) {
	if err := json.NewEncoder(w).Encode(getSchemaResponse{
		DBs: h.Index.Schema(),
	}); err != nil {
		h.logger().Printf("write schema response error: %s", err)
	}
}

type getSchemaResponse struct {
	DBs []*DBInfo `json:"dbs"`
}

// handlePostQuery handles /query requests.
func (h *Handler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	dbName := mux.Vars(r)["db"]

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
	results, err := h.Executor.Execute(r.Context(), dbName, q, req.Slices, opt)
	resp := &QueryResponse{Results: results, Err: err}

	// Fill profile attributes if requested.
	if req.Profiles {
		// Consolidate all profile ids across all calls.
		var profileIDs []uint64
		for _, result := range results {
			bm, ok := result.(*Bitmap)
			if !ok {
				continue
			}
			profileIDs = uint64Slice(profileIDs).merge(bm.Bits())
		}

		// Retrieve profile attributes across all calls.
		profiles, err := h.readProfiles(h.Index.DB(dbName), profileIDs)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			h.writeQueryResponse(w, r, &QueryResponse{Err: err})
			return
		}
		resp.Profiles = profiles
	}

	// Set appropriate status code, if there is an error.
	if resp.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	// Write response back to client.
	if err := h.writeQueryResponse(w, r, resp); err != nil {
		h.logger().Printf("write query response error: %s", err)
	}
}

func (h *Handler) handleGetSliceMax(w http.ResponseWriter, r *http.Request) {
	var ms map[string]uint64
	if inverse, _ := strconv.ParseBool(r.URL.Query().Get("inverse")); inverse {
		ms = h.Index.MaxInverseSlices()
	} else {
		ms = h.Index.MaxSlices()
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
	}
	json.NewEncoder(w).Encode(sliceMaxResponse{
		MaxSlices: ms,
	})
}

type sliceMaxResponse struct {
	MaxSlices map[string]uint64 `json:"MaxSlices"`
}

// handleGetDBs handles GET /db request.
func (h *Handler) handleGetDBs(w http.ResponseWriter, r *http.Request) {
	h.handleGetSchema(w, r)
}

// handleGetDB handles GET /db/<dbname> requests.
func (h *Handler) handleGetDB(w http.ResponseWriter, r *http.Request) {
	dbName := mux.Vars(r)["db"]
	db := h.Index.DB(dbName)
	if db == nil {
		http.Error(w, ErrDatabaseNotFound.Error(), http.StatusNotFound)
		return
	}

	if err := json.NewEncoder(w).Encode(getDBResponse{
		map[string]string{"name": db.Name()},
	}); err != nil {
		h.logger().Printf("write response error: %s", err)
	}
}

type getDBResponse struct {
	DB map[string]string `json:"db"`
}

type postDBRequest struct {
	Options DBOptions `json:"options"`
}

// Custom Unmarshal JSON to validate request body when creating a new database
func (p *postDBRequest) UnmarshalJSON(b []byte) error {
	validDBOptions := []string{"columnLabel"}
	var data map[string]interface{}
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}
	p.Options = DBOptions{}
	for key, value := range data {
		switch key {
		case "options":
			values, err := validateOptions(data, validDBOptions)
			if err != nil {
				return err
			}
			for k, v := range values {
				switch k {
				case "columnLabel":
					p.Options.ColumnLabel = v.(string)
				}
			}
		default:
			return fmt.Errorf("Unknown key: %v:%v", key, value)
		}
	}
	return nil
}

func validateOptions(data map[string]interface{}, field []string) (map[string]interface{}, error) {
	options, ok := data["options"].(map[string]interface{})
	optionValue := make(map[string]interface{})
	if !ok {
		return nil, errors.New("options is not map[string]interface{}")
	}

	if len(options) == 0 {
		optionValue = nil
	} else {
		for k, v := range options {
			if foundItem(field, k) {
				switch k {
				case "inverseEnabled":
					val, ok := options[k].(bool)
					if !ok {
						return nil, fmt.Errorf("invalid option type %v: {%v:%v}", field, k, v)
					}
					optionValue[k] = val
				default:
					val, ok := options[k].(string)
					if !ok {
						return nil, fmt.Errorf("invalid option %v: {%v:%v}", field, k, v)
					}
					optionValue[k] = val
				}

			} else {
				return nil, fmt.Errorf("invalid key for options {%v:%v}", k, v)
			}
		}
	}
	return optionValue, nil
}

func foundItem(items []string, item string) bool {
	for _, i := range items {
		if item == i {
			return true
		}
	}
	return false
}

type postDBResponse struct{}

// handleDeleteDB handles DELETE /db request.
func (h *Handler) handleDeleteDB(w http.ResponseWriter, r *http.Request) {
	dbName := mux.Vars(r)["db"]

	// Delete database from the index.
	if err := h.Index.DeleteDB(dbName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(deleteDBResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

type deleteDBResponse struct{}

// handlePostDB handles POST /db request.
func (h *Handler) handlePostDB(w http.ResponseWriter, r *http.Request) {
	dbName := mux.Vars(r)["db"]

	// Decode request.
	var req postDBRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create database.
	_, err := h.Index.CreateDB(dbName, req.Options)
	if err == ErrDatabaseExists {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postDBResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

// handlePatchDBTimeQuantum handles PATCH /db/time_quantum request.
func (h *Handler) handlePatchDBTimeQuantum(w http.ResponseWriter, r *http.Request) {
	dbName := mux.Vars(r)["db"]

	// Decode request.
	var req patchDBTimeQuantumRequest
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

	// Retrieve database by name.
	database := h.Index.DB(dbName)
	if database == nil {
		http.Error(w, ErrDatabaseNotFound.Error(), http.StatusNotFound)
		return
	}

	// Set default time quantum on database.
	if err := database.SetTimeQuantum(tq); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(patchDBTimeQuantumResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

type patchDBTimeQuantumRequest struct {
	TimeQuantum string `json:"time_quantum"`
}

type patchDBTimeQuantumResponse struct{}

// handlePostDBAttrDiff handles POST /db/attr/diff requests.
func (h *Handler) handlePostDBAttrDiff(w http.ResponseWriter, r *http.Request) {
	dbName := mux.Vars(r)["db"]

	// Decode request.
	var req postDBAttrDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Retrieve database from index.
	db := h.Index.DB(dbName)
	if db == nil {
		http.Error(w, ErrDatabaseNotFound.Error(), http.StatusNotFound)
		return
	}

	// Retrieve local blocks.
	blks, err := db.ProfileAttrStore().Blocks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Read all attributes from all mismatched blocks.
	attrs := make(map[uint64]map[string]interface{})
	for _, blockID := range AttrBlocks(blks).Diff(req.Blocks) {
		// Retrieve block data.
		m, err := db.ProfileAttrStore().BlockData(blockID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Copy to database-wide struct.
		for k, v := range m {
			attrs[k] = v
		}
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postDBAttrDiffResponse{
		Attrs: attrs,
	}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

type postDBAttrDiffRequest struct {
	Blocks []AttrBlock `json:"blocks"`
}

type postDBAttrDiffResponse struct {
	Attrs map[uint64]map[string]interface{} `json:"attrs"`
}

// handlePostFrame handles POST /frame request.
func (h *Handler) handlePostFrame(w http.ResponseWriter, r *http.Request) {
	dbName := mux.Vars(r)["db"]
	frameName := mux.Vars(r)["frame"]

	// Decode request.
	var req postFrameRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Find database.
	db := h.Index.DB(dbName)
	if db == nil {
		http.Error(w, ErrDatabaseNotFound.Error(), http.StatusNotFound)
		return
	}

	// Create frame.
	_, err := db.CreateFrame(frameName, req.Options)
	if err == ErrFrameExists {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(postFrameResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

// Custom Unmarshal JSON to validate request body when creating a new frame. If there's new FrameOptions,
// adding it to validFrameOptions to make sure the new option is validated, otherwise the request will be failed
func (p *postFrameRequest) UnmarshalJSON(b []byte) error {
	validFrameOptions := []string{"rowLabel", "cacheType", "inverseEnabled"}
	var data map[string]interface{}
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}
	p.Options = FrameOptions{}
	for key, value := range data {
		switch key {
		case "options":
			values, err := validateOptions(data, validFrameOptions)
			if err != nil {
				return err
			}
			for k, v := range values {
				switch k {
				case "rowLabel":
					p.Options.RowLabel = v.(string)
				case "cacheType":
					p.Options.CacheType = v.(string)
				case "inverseEnabled":
					p.Options.InverseEnabled = v.(bool)
				}
			}

		default:
			return fmt.Errorf("Unknown key: {%v:%v}", key, value)
		}
	}
	return nil

}

type postFrameRequest struct {
	Options FrameOptions `json:"options"`
}

type postFrameResponse struct{}

// handleDeleteFrame handles DELETE /frame request.
func (h *Handler) handleDeleteFrame(w http.ResponseWriter, r *http.Request) {
	dbName := mux.Vars(r)["db"]
	frameName := mux.Vars(r)["frame"]

	// Find database.
	db := h.Index.DB(dbName)
	if db == nil {
		if err := json.NewEncoder(w).Encode(deleteDBResponse{}); err != nil {
			h.logger().Printf("response encoding error: %s", err)
		}
		return
	}

	// Delete frame from the database.
	if err := db.DeleteFrame(frameName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode response.
	if err := json.NewEncoder(w).Encode(deleteFrameResponse{}); err != nil {
		h.logger().Printf("response encoding error: %s", err)
	}
}

type deleteFrameResponse struct{}

// handlePatchFrameTimeQuantum handles PATCH /frame/time_quantum request.
func (h *Handler) handlePatchFrameTimeQuantum(w http.ResponseWriter, r *http.Request) {
	dbName := mux.Vars(r)["db"]
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

	// Retrieve database by name.
	f := h.Index.Frame(dbName, frameName)
	if f == nil {
		http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		return
	}

	// Set default time quantum on database.
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
	TimeQuantum string `json:"time_quantum"`
}

type patchFrameTimeQuantumResponse struct{}

// handleGetFrameViews handles GET /frame/views request.
func (h *Handler) handleGetFrameViews(w http.ResponseWriter, r *http.Request) {
	dbName := mux.Vars(r)["db"]
	frameName := mux.Vars(r)["frame"]

	// Retrieve views.
	f := h.Index.Frame(dbName, frameName)
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
	dbName := mux.Vars(r)["db"]
	frameName := mux.Vars(r)["frame"]

	// Decode request.
	var req postFrameAttrDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Retrieve database from index.
	f := h.Index.Frame(dbName, frameName)
	if f == nil {
		http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		return
	}

	// Retrieve local blocks.
	blks, err := f.BitmapAttrStore().Blocks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Read all attributes from all mismatched blocks.
	attrs := make(map[uint64]map[string]interface{})
	for _, blockID := range AttrBlocks(blks).Diff(req.Blocks) {
		// Retrieve block data.
		m, err := f.BitmapAttrStore().BlockData(blockID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Copy to database-wide struct.
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

// readProfiles returns a list of profile objects by id.
func (h *Handler) readProfiles(db *DB, ids []uint64) ([]*Profile, error) {
	if db == nil {
		return nil, nil
	}

	a := make([]*Profile, 0, len(ids))
	for _, id := range ids {
		// Read attributes for profile. Skip profile if empty.
		attrs, err := db.ProfileAttrStore().Attrs(id)
		if err != nil {
			return nil, err
		} else if len(attrs) == 0 {
			continue
		}

		// Append profile with attributes.
		a = append(a, &Profile{ID: id, Attrs: attrs})
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
		Query:    query,
		Slices:   slices,
		Profiles: q.Get("profiles") == "true",
		Quantum:  quantum,
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
	if !h.Cluster.OwnsFragment(h.Host, req.DB, req.Slice) {
		mesg := fmt.Sprintf("host does not own slice %s-%s slice:%d", h.Host, req.DB, req.Slice)
		http.Error(w, mesg, http.StatusPreconditionFailed)
		return
	}

	// Find the DB.
	h.logger().Println("importing:", req.DB, req.Frame, req.Slice)
	db := h.Index.DB(req.DB)
	if db == nil {
		h.logger().Printf("fragment error: db=%s, frame=%s, slice=%d, err=%s", req.DB, req.Frame, req.Slice, ErrDatabaseNotFound.Error())
		http.Error(w, ErrDatabaseNotFound.Error(), http.StatusNotFound)
		return
	}

	// Retrieve frame.
	f := db.Frame(req.Frame)
	if f == nil {
		h.logger().Printf("frame error: db=%s, frame=%s, slice=%d, err=%s", req.DB, req.Frame, req.Slice, ErrFrameNotFound.Error())
		http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		return
	}

	// Import into fragment.
	err = f.Import(req.BitmapIDs, req.ProfileIDs, timestamps)
	if err != nil {
		h.logger().Printf("import error: db=%s, frame=%s, slice=%d, bits=%d, err=%s", req.DB, req.Frame, req.Slice, len(req.ProfileIDs), err)
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
	db, frame, view := q.Get("db"), q.Get("frame"), q.Get("view")

	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "invalid slice", http.StatusBadRequest)
		return
	}

	// Validate that this handler owns the slice.
	if !h.Cluster.OwnsFragment(h.Host, db, slice) {
		mesg := fmt.Sprintf("host does not own slice %s-%s slice:%d", h.Host, db, slice)
		http.Error(w, mesg, http.StatusPreconditionFailed)
		return
	}

	// Find the fragment.
	f := h.Index.Fragment(db, frame, view, slice)
	if f == nil {
		return
	}

	// Wrap writer with a CSV writer.
	cw := csv.NewWriter(w)

	// Iterate over each bit.
	if err := f.ForEachBit(func(bitmapID, profileID uint64) error {
		return cw.Write([]string{
			strconv.FormatUint(bitmapID, 10),
			strconv.FormatUint(profileID, 10),
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
	db := q.Get("db")

	// Read slice parameter.
	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "slice required", http.StatusBadRequest)
		return
	}

	// Retrieve fragment owner nodes.
	nodes := h.Cluster.FragmentNodes(db, slice)

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

	// Retrieve fragment from index.
	f := h.Index.Fragment(q.Get("db"), q.Get("frame"), q.Get("view"), slice)
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
	f := h.Index.Frame(q.Get("db"), q.Get("frame"))
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

	// Retrieve fragment from index.
	f := h.Index.Fragment(req.DB, req.Frame, req.View, req.Slice)
	if f == nil {
		http.Error(w, ErrFragmentNotFound.Error(), http.StatusNotFound)
		return
	}

	// Read data
	var resp internal.BlockDataResponse
	if f != nil {
		resp.BitmapIDs, resp.ProfileIDs = f.BlockData(int(req.Block))
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

	// Retrieve fragment from index.
	f := h.Index.Fragment(q.Get("db"), q.Get("frame"), q.Get("view"), slice)
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
	dbName := mux.Vars(r)["db"]
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
	maxSlices, err := client.MaxSliceByDatabase(r.Context())
	if err != nil {
		http.Error(w, "cannot determine remote slice count: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Retrieve frame.
	f := h.Index.Frame(dbName, frameName)
	if f == nil {
		http.Error(w, ErrFrameNotFound.Error(), http.StatusNotFound)
		return
	}

	// Retrieve list of all views.
	views, err := client.FrameViews(r.Context(), dbName, frameName)
	if err != nil {
		http.Error(w, "cannot retrieve frame views: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Loop over each slice and import it if this node owns it.
	for slice := uint64(0); slice <= maxSlices[dbName]; slice++ {
		// Ignore this slice if we don't own it.
		if !h.Cluster.OwnsFragment(h.Host, dbName, slice) {
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
			rd, err := client.BackupSlice(r.Context(), dbName, frameName, view, slice)
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

// handleGetNodes handles /nodes requests.
func (h *Handler) handleGetNodes(w http.ResponseWriter, r *http.Request) {
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
	// Database to execute query against.
	DB string

	// The query string to parse and execute.
	Query string

	// The slices to include in the query execution.
	// If empty, all slices are included.
	Slices []uint64

	// Return profile attributes, if true.
	Profiles bool

	// Time granularity to use with the timestamp.
	Quantum TimeQuantum

	// If true, indicates that query is part of a larger distributed query.
	// If false, this request is on the originating node.
	Remote bool
}

func decodeQueryRequest(pb *internal.QueryRequest) *QueryRequest {
	req := &QueryRequest{
		Query:    pb.Query,
		Slices:   pb.Slices,
		Profiles: pb.Profiles,
		Quantum:  TimeQuantum(pb.Quantum),
		Remote:   pb.Remote,
	}

	return req
}

// QueryResponse represent a response from a processed query.
type QueryResponse struct {
	// Result for each top-level query call.
	// Can be a Bitmap, Pairs, or uint64.
	Results []interface{}

	// Set of profiles matching IDs returned in Result.
	Profiles []*Profile

	// Error during parsing or execution.
	Err error
}

func (resp *QueryResponse) MarshalJSON() ([]byte, error) {
	var output struct {
		Results  []interface{} `json:"results,omitempty"`
		Profiles []*Profile    `json:"profiles,omitempty"`
		Err      string        `json:"error,omitempty"`
	}
	output.Results = resp.Results
	output.Profiles = resp.Profiles

	if resp.Err != nil {
		output.Err = resp.Err.Error()
	}
	return json.Marshal(output)
}

func encodeQueryResponse(resp *QueryResponse) *internal.QueryResponse {
	pb := &internal.QueryResponse{
		Results:  make([]*internal.QueryResult, len(resp.Results)),
		Profiles: encodeProfiles(resp.Profiles),
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
