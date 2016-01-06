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
		Execute(db string, query *pql.Query, slices []uint64) (interface{}, error)
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
	t := time.Now()

	switch r.URL.Path {
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
	case "/slices/nodes":
		switch r.Method {
		case "GET":
			h.handleGetSlicesNodes(w, r)
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

// handlePostQuery handles /query requests.
func (h *Handler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	// Parse incoming request.
	db, query, slices, err := h.readQueryRequest(r)

	// h.logger().Printf("%s %s db=%s q=%s slices=%v", r.Method, r.URL.Path, db, query, slices)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		h.writeQueryResponse(w, r, nil, err)
		return
	}

	// Parse query string.
	q, err := pql.NewParser(strings.NewReader(query)).Parse()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		h.writeQueryResponse(w, r, nil, err)
		return
	}

	// Execute the query.
	res, e := h.Executor.Execute(db, q, slices)

	// Set appropriate status code, if there is an error.
	if e != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	// Write response back to client.
	if err := h.writeQueryResponse(w, r, res, e); err != nil {
		h.logger().Printf("write query response error: %s", err)
	}
}

// readQueryRequest parses an query parameters from r.
func (h *Handler) readQueryRequest(r *http.Request) (db, query string, slices []uint64, err error) {
	switch r.Header.Get("Content-Type") {
	case "application/x-protobuf":
		return h.readProtobufQueryRequest(r)
	default:
		return h.readURLQueryRequest(r)
	}
}

// readProtobufQueryRequest parses query parameters in protobuf from r.
func (h *Handler) readProtobufQueryRequest(r *http.Request) (db, query string, slices []uint64, err error) {
	// Slurp the body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}

	// Unmarshal into object.
	var req internal.QueryRequest
	if err = proto.Unmarshal(body, &req); err != nil {
		return
	}

	return req.GetDB(), req.GetQuery(), req.GetSlices(), nil
}

// readURLQueryRequest parses query parameters from URL parameters from r.
func (h *Handler) readURLQueryRequest(r *http.Request) (db, query string, slices []uint64, err error) {
	q := r.URL.Query()

	// Read DB argument.
	db = q.Get("db")

	// Parse query string.
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	query = string(buf)

	// Parse list of slices.
	slices, err = parseUint64Slice(q.Get("slices"))
	if err != nil {
		err = errors.New("invalid slice argument")
		return
	}

	return
}

// writeQueryResponse writes the response from the executor to w.
func (h *Handler) writeQueryResponse(w http.ResponseWriter, r *http.Request, res interface{}, err error) error {
	if strings.Contains(r.Header.Get("Accept"), "application/x-protobuf") {
		return h.writeProtobufQueryResponse(w, res, err)
	}
	return h.writeJSONQueryResponse(w, res, err)
}

// writeProtobufQueryResponse writes the response from the executor to w as protobuf.
func (h *Handler) writeProtobufQueryResponse(w http.ResponseWriter, res interface{}, e error) error {
	var resp internal.QueryResponse

	// Set the result on the appropriate field.
	if res != nil {
		switch res := res.(type) {
		case *Bitmap:
			resp.Bitmap = encodeBitmap(res)
		case Pairs:
			resp.Pairs = encodePairs(res)
		case uint64:
			resp.N = proto.Uint64(res)
		default:
			panic(fmt.Sprintf("invalid query response type: %T", res))
		}
	}

	// Set the error if there is one.
	if e != nil {
		resp.Err = proto.String(e.Error())
	}

	// Encode response.
	buf, err := proto.Marshal(&resp)
	if err != nil {
		return err
	}

	// Write response back to client.
	if _, err := w.Write(buf); err != nil {
		return err
	}

	return nil
}

// writeJSONQueryResponse writes the response from the executor to w as JSON.
func (h *Handler) writeJSONQueryResponse(w http.ResponseWriter, res interface{}, e error) error {
	var o struct {
		Result interface{} `json:"result,omitempty"`
		Error  string      `json:"error,omitempty"`
	}
	o.Result = res

	if e != nil {
		o.Error = e.Error()
	}

	// Otherwise marshal the result as JSON.
	return json.NewEncoder(w).Encode(o)
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
	if !h.Cluster.OwnsSlice(h.Host, slice) {
		http.Error(w, "host does not own slice", http.StatusPreconditionFailed)
		return
	}

	// Find the correct fragment.
	f, err := h.Index.CreateFragmentIfNotExists(db, frame, slice)
	if err != nil {
		h.logger().Printf("fragment error: db=%s, frame=%s, slice=%s, err=%s", db, frame, slice, err)
		http.Error(w, "fragment error", http.StatusInternalServerError)
		return
	}

	// Import into fragment.
	err = f.Import(req.GetBitmapIDs(), req.GetProfileIDs())
	if err != nil {
		h.logger().Printf("import error: db=%s, frame=%s, slice=%s, bits=%d", db, frame, slice, len(req.GetProfileIDs()))
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

// handleGetSlicesNodes handles /slices/nodes requests.
func (h *Handler) handleGetSlicesNodes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Read slice parameter.
	slice, err := strconv.ParseUint(q.Get("slice"), 10, 64)
	if err != nil {
		http.Error(w, "slice required", http.StatusBadRequest)
	}

	// Retrieve slice owner nodes.
	nodes := h.Cluster.SliceNodes(slice)

	// Write to response.
	if err := json.NewEncoder(w).Encode(nodes); err != nil {
		h.logger().Printf("json write error: %s", err)
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
