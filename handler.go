package pilosa

import (
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa/internal"
	"github.com/umbel/pilosa/pql"
)

// Handler represents an HTTP handler.
type Handler struct {
	Index *Index

	// Local hostname & cluster configuration.
	Host    string
	Cluster *Cluster

	// The execution engine for running queries.
	Executor interface {
		Execute(db string, query *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error)
	}

	// The version to report on the /version endpoint.
	Version string

	// The writer for any logging.
	LogOutput io.Writer
}

// NewHandler returns a new instance of Handler with a default logger.
func NewHandler() *Handler {
	return &Handler{
		Version:   Version,
		LogOutput: os.Stderr,
	}
}

// ServeHTTP handles an HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Handle pprof requests separately.
	if strings.HasPrefix(r.URL.Path, "/debug/pprof") {
		switch r.URL.Path {
		case "/debug/pprof/cmdline":
			pprof.Cmdline(w, r)
		case "/debug/pprof/profile":
			pprof.Profile(w, r)
		case "/debug/pprof/symbol":
			pprof.Symbol(w, r)
		default:
			pprof.Index(w, r)
		}
		return
	}

	// Route API calls to appropriate handler functions.
	t := time.Now()
	switch r.URL.Path {
	case "/schema":
		switch r.Method {
		case "GET":
			h.handleGetSchema(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/query":
		switch r.Method {
		case "POST":
			h.handlePostQuery(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/import":
		switch r.Method {
		case "POST":
			h.handlePostImport(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/slices/max":
		switch r.Method {
		case "GET":
			h.handleGetSliceMax(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/fragment/nodes":
		switch r.Method {
		case "GET":
			h.handleGetFragmentNodes(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/fragment/data":
		switch r.Method {
		case "GET":
			h.handleGetFragmentData(w, r)
		case "POST":
			h.handlePostFragmentData(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/fragment/blocks":
		switch r.Method {
		case "GET":
			h.handleGetFragmentBlocks(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/fragment/block/data":
		switch r.Method {
		case "GET":
			h.handleGetFragmentBlockData(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/frame/restore":
		switch r.Method {
		case "POST":
			h.handlePostFrameRestore(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/version":
		h.handleVersion(w, r)

	case "/debug/vars":
		h.handleExpvar(w, r)
	default:
		http.NotFound(w, r)
	}

	h.logger().Printf("%s %s %.03fs", r.Method, r.URL.String(), time.Since(t).Seconds())
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
	// Parse incoming request.
	req, err := h.readQueryRequest(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		h.writeQueryResponse(w, r, &QueryResponse{Err: err})
		return
	}

	// Build execution options.
	opt := &ExecOptions{
		Timestamp: req.Timestamp,
		Quantum:   req.Quantum,
		Remote:    req.Remote,
	}

	// Parse query string.
	q, err := pql.NewParser(strings.NewReader(req.Query)).Parse()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		h.writeQueryResponse(w, r, &QueryResponse{Err: err})
		return
	}

	// Execute the query.
	results, err := h.Executor.Execute(req.DB, q, req.Slices, opt)
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
		profiles, err := h.readProfiles(h.Index.DB(req.DB), profileIDs)
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

func (h *Handler) handleGetSliceMax(w http.ResponseWriter, r *http.Request) error {
	sm := h.Index.SliceN()
	if strings.Contains(r.Header.Get("Accept"), "application/x-protobuf") {
		pb := &internal.SliceMaxResponse{
			SliceMax: &sm,
		}
		if buf, err := proto.Marshal(pb); err != nil {
			return err
		} else if _, err := w.Write(buf); err != nil {
			return err
		}
		return nil
	}
	return json.NewEncoder(w).Encode(sliceMaxResponse{SliceMax: sm})
}

type sliceMaxResponse struct {
	SliceMax uint64 `json:"SliceMax"`
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

	// Parse timestamp, if available.
	var timestamp *time.Time
	if v := q.Get("timestamp"); v != "" {
		layout := "2006-01-02 15:04:05"
		if strings.Contains(v, "T") {
			layout = "2006-01-02T15:04:05"
		}

		t, err := time.Parse(layout, v)
		if err != nil {
			return nil, errors.New("invalid timestamp")
		}
		timestamp = &t
	}

	// Parse time granularity.
	quantum := YMDH
	if s := q.Get("time_granularity"); s != "" {
		v, err := ParseTimeQuantum(s)
		if err != nil {
			return nil, errors.New("invalid time granularity")
		}
		quantum = v
	}

	return &QueryRequest{
		DB:        q.Get("db"),
		Query:     query,
		Slices:    slices,
		Profiles:  q.Get("profiles") == "true",
		Timestamp: timestamp,
		Quantum:   quantum,
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
	db, frame, slice := req.GetDB(), req.GetFrame(), req.GetSlice()

	// Validate that this handler owns the slice.
	if !h.Cluster.OwnsFragment(h.Host, db, slice) {
		http.Error(w, "host does not own slice", http.StatusPreconditionFailed)
		return
	}

	// Find the correct fragment.
	f, err := h.Index.CreateFragmentIfNotExists(db, frame, slice)
	if err != nil {
		h.logger().Printf("fragment error: db=%s, frame=%s, slice=%d, err=%s", db, frame, slice, err)
		http.Error(w, "fragment error", http.StatusInternalServerError)
		return
	}

	// Import into fragment.
	err = f.Import(req.GetBitmapIDs(), req.GetProfileIDs())
	if err != nil {
		h.logger().Printf("import error: db=%s, frame=%s, slice=%d, bits=%d, err=%s", db, frame, slice, len(req.GetProfileIDs()), err)
	}

	// Marshal response object.
	buf, e := proto.Marshal(&internal.ImportResponse{Err: proto.String(errorString(err))})
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
	f := h.Index.Fragment(q.Get("db"), q.Get("frame"), slice)
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

	// Retrieve fragment from index.
	f, err := h.Index.CreateFragmentIfNotExists(q.Get("db"), q.Get("frame"), slice)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Read fragment in from request body.
	if _, err := f.ReadFrom(r.Body); err != nil {
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
	f := h.Index.Fragment(req.GetDB(), req.GetFrame(), req.GetSlice())
	if f == nil {
		http.Error(w, ErrFragmentNotFound.Error(), http.StatusNotFound)
		return
	}

	// Read data
	var resp internal.BlockDataResponse
	if f != nil {
		resp.BitmapIDs, resp.ProfileIDs = f.BlockData(int(req.GetBlock()))
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
	f := h.Index.Fragment(q.Get("db"), q.Get("frame"), slice)
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
	q := r.URL.Query()
	host := q.Get("host")
	db, frame := q.Get("db"), q.Get("frame")

	// Validate query parameters.
	if host == "" {
		http.Error(w, "host required", http.StatusBadRequest)
		return
	} else if db == "" {
		http.Error(w, "db required", http.StatusBadRequest)
		return
	} else if frame == "" {
		http.Error(w, "frame required", http.StatusBadRequest)
		return
	}

	// Create a client for the remote cluster.
	client, err := NewClient(host)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Determine the maximum number of slices.
	sliceN, err := client.SliceN()
	if err != nil {
		http.Error(w, "cannot determine remote slice count: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Loop over each slice and import it if this node owns it.
	for slice := uint64(0); slice <= sliceN; slice++ {
		// Ignore this slice if we don't own it.
		if !h.Cluster.OwnsFragment(h.Host, db, slice) {
			continue
		}

		// Otherwise retrieve the local fragment.
		f, err := h.Index.CreateFragmentIfNotExists(db, frame, slice)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Stream backup from remote node.
		r, err := client.BackupSlice(db, frame, slice)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else if r == nil {
			continue // slice doesn't exist
		}

		// Restore to local frame and always close reader.
		if err := func() error {
			defer r.Close()
			if _, err := f.ReadFrom(r); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

// handleGetVersion handles /version requests.
func (h *Handler) handleVersion(w http.ResponseWriter, r *http.Request) {
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

	// Timestamp passed into the query.
	Timestamp *time.Time

	// Time granularity to use with the timestamp.
	Quantum TimeQuantum

	// If true, indicates that query is part of a larger distributed query.
	// If false, this request is on the originating node.
	Remote bool
}

func decodeQueryRequest(pb *internal.QueryRequest) *QueryRequest {
	req := &QueryRequest{
		DB:       pb.GetDB(),
		Query:    pb.GetQuery(),
		Slices:   pb.GetSlices(),
		Profiles: pb.GetProfiles(),
		Quantum:  TimeQuantum(pb.GetQuantum()),
		Remote:   pb.GetRemote(),
	}

	if pb.Timestamp != nil {
		t := time.Unix(0, pb.GetTimestamp())
		req.Timestamp = &t
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
			pb.Results[i].N = proto.Uint64(result)
		case bool:
			pb.Results[i].Changed = proto.Bool(result)
		}
	}

	if resp.Err != nil {
		pb.Err = proto.String(resp.Err.Error())
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
