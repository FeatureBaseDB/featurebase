package pilosa_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/internal"
	"github.com/umbel/pilosa/pql"
)

// Ensure the handler returns "not found" for invalid paths.
func TestHandler_NotFound(t *testing.T) {
	w := httptest.NewRecorder()
	NewHandler().ServeHTTP(w, MustNewHTTPRequest("GET", "/no_such_path", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("invalid status: %d", w.Code)
	}
}

// Ensure the handler can accept URL arguments.
func TestHandler_Query_Args_URL(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		if db != "db0" {
			t.Fatalf("unexpected db: %s", db)
		} else if query.String() != `Count(Bitmap(id=100))` {
			t.Fatalf("unexpected query: %s", query.String())
		} else if !reflect.DeepEqual(slices, []uint64{0, 1}) {
			t.Fatalf("unexpected slices: %+v", slices)
		}
		return uint64(100), nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=db0&slices=0,1", strings.NewReader("Count( Bitmap( 100))")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"result":100}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can accept arguments via protobufs.
func TestHandler_Query_Args_Protobuf(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		if db != "db0" {
			t.Fatalf("unexpected db: %s", db)
		} else if query.String() != `Count(Bitmap(id=100))` {
			t.Fatalf("unexpected query: %s", query.String())
		} else if !reflect.DeepEqual(slices, []uint64{0, 1}) {
			t.Fatalf("unexpected slices: %+v", slices)
		}
		return uint64(100), nil
	}

	// Generate request body.
	reqBody, err := proto.Marshal(&internal.QueryRequest{
		DB:     proto.String("db0"),
		Query:  proto.String("Count(Bitmap(100))"),
		Slices: []uint64{0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Generate protobuf request.
	req := MustNewHTTPRequest("POST", "/query", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/x-protobuf")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
}

// Ensure the handler returns an error when parsing bad arguments.
func TestHandler_Query_Args_Err(t *testing.T) {
	w := httptest.NewRecorder()
	NewHandler().ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=db0&slices=a,b", strings.NewReader("Bitmap(100)")))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"invalid slice argument"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can execute a query with a uint64 response as JSON.
func TestHandler_Query_Uint64_JSON(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		return uint64(100), nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=db0&slices=0,1", strings.NewReader("Count( Bitmap( 100))")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"result":100}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can execute a query with a uint64 response as protobufs.
func TestHandler_Query_Uint64_Protobuf(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		return uint64(100), nil
	}

	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("POST", "/query", strings.NewReader("Count(Bitmap(100))"))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if resp.GetN() != 100 {
		t.Fatalf("unexpected n: %d", resp.GetN())
	}
}

// Ensure the handler can execute a query that returns a bitmap as JSON.
func TestHandler_Query_Bitmap_JSON(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		bm := pilosa.NewBitmap(1, 3, 66, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": 1, "d": true}
		return bm, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=d", strings.NewReader("Bitmap(100)")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"result":{"attrs":{"a":"b","c":1,"d":true},"bits":[1,3,66,65537]}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can execute a query that returns a bitmap with profiles as JSON.
func TestHandler_Query_Bitmap_Profiles_JSON(t *testing.T) {
	idx := NewIndex()
	defer idx.Close()

	// Create database and set profile attributes.
	db, err := idx.CreateDBIfNotExists("d")
	if err != nil {
		t.Fatal(err)
	} else if err := db.ProfileAttrStore().SetAttrs(3, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	} else if err := db.ProfileAttrStore().SetAttrs(66, map[string]interface{}{"y": 123, "z": false}); err != nil {
		t.Fatal(err)
	}

	h := NewHandler()
	h.Index = idx.Index
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		bm := pilosa.NewBitmap(1, 3, 66, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": 1, "d": true}
		return bm, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=d&profiles=true", strings.NewReader("Bitmap(100)")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"result":{"attrs":{"a":"b","c":1,"d":true},"bits":[1,3,66,65537]},"profiles":[{"id":3,"attrs":{"x":"y"}},{"id":66,"attrs":{"y":123,"z":false}}]}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can execute a query that returns a bitmap as protobuf.
func TestHandler_Query_Bitmap_Protobuf(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		bm := pilosa.NewBitmap(1, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": int64(1), "d": true}
		return bm, nil
	}

	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("POST", "/query", strings.NewReader("Bitmap(100)"))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if a := resp.GetBitmap().GetChunks(); len(a) != 2 {
		t.Fatalf("unexpected bitmap chunk length: %d", len(a))
	} else if attrs := resp.GetBitmap().GetAttrs(); len(attrs) != 3 {
		t.Fatalf("unexpected attr length: %d", len(attrs))
	} else if k, v := attrs[0].GetKey(), attrs[0].GetStringValue(); k != "a" || v != "b" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	} else if k, v := attrs[1].GetKey(), attrs[1].GetUintValue(); k != "c" || v != uint64(1) {
		t.Fatalf("unexpected attr[1]: %s=%v", k, v)
	} else if k, v := attrs[2].GetKey(), attrs[2].GetBoolValue(); k != "d" || v != true {
		t.Fatalf("unexpected attr[2]: %s=%v", k, v)
	}
}

// Ensure the handler can execute a query that returns a bitmap with profiles as protobuf.
func TestHandler_Query_Bitmap_Profiles_Protobuf(t *testing.T) {
	idx := NewIndex()
	defer idx.Close()

	// Create database and set profile attributes.
	db, err := idx.CreateDBIfNotExists("d")
	if err != nil {
		t.Fatal(err)
	} else if err := db.ProfileAttrStore().SetAttrs(1, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	}

	h := NewHandler()
	h.Index = idx.Index
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		bm := pilosa.NewBitmap(1, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": int64(1), "d": true}
		return bm, nil
	}

	// Encode request body.
	buf, err := proto.Marshal(&internal.QueryRequest{
		DB:       proto.String("d"),
		Query:    proto.String("Bitmap(100)"),
		Profiles: proto.Bool(true),
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("POST", "/query", bytes.NewReader(buf))
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if a := resp.GetBitmap().GetChunks(); len(a) != 2 {
		t.Fatalf("unexpected bitmap chunk length: %d", len(a))
	} else if attrs := resp.GetBitmap().GetAttrs(); len(attrs) != 3 {
		t.Fatalf("unexpected attr length: %d", len(attrs))
	} else if k, v := attrs[0].GetKey(), attrs[0].GetStringValue(); k != "a" || v != "b" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	} else if k, v := attrs[1].GetKey(), attrs[1].GetUintValue(); k != "c" || v != uint64(1) {
		t.Fatalf("unexpected attr[1]: %s=%v", k, v)
	} else if k, v := attrs[2].GetKey(), attrs[2].GetBoolValue(); k != "d" || v != true {
		t.Fatalf("unexpected attr[2]: %s=%v", k, v)
	}

	if a := resp.GetProfiles(); len(a) != 1 {
		t.Fatalf("unexpected profiles length: %d", len(a))
	} else if a[0].GetID() != 1 {
		t.Fatalf("unexpected id: %d", a[0].GetID())
	} else if len(a[0].GetAttrs()) != 1 {
		t.Fatalf("unexpected profile attr length: %d", len(a))
	} else if k, v := a[0].GetAttrs()[0].GetKey(), a[0].GetAttrs()[0].GetStringValue(); k != "x" || v != "y" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	}
}

// Ensure the handler can execute a query that returns pairs as JSON.
func TestHandler_Query_Pairs_JSON(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		return []pilosa.Pair{
			{Key: 1, Count: 2},
			{Key: 3, Count: 4},
		}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query", strings.NewReader(`TopN(frame=x, n=2)`)))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"result":[{"key":1,"count":2},{"key":3,"count":4}]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can execute a query that returns pairs as protobuf.
func TestHandler_Query_Pairs_Protobuf(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		return []pilosa.Pair{
			{Key: 1, Count: 2},
			{Key: 3, Count: 4},
		}, nil
	}

	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("POST", "/query", strings.NewReader(`TopN(frame=x, n=2)`))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if a := resp.GetPairs(); len(a) != 2 {
		t.Fatalf("unexpected pair length: %d", len(a))
	}
}

// Ensure the handler can return an error as JSON.
func TestHandler_Query_Err_JSON(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		return nil, errors.New("marker")
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query", strings.NewReader(`Bitmap(100)`)))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"marker"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can return an error as protobuf.
func TestHandler_Query_Err_Protobuf(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
		return nil, errors.New("marker")
	}

	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("POST", "/query", strings.NewReader(`TopN(frame=x, n=2)`))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if s := resp.GetErr(); s != `marker` {
		t.Fatalf("unexpected error: %s", s)
	}
}

// Ensure the handler returns "method not allowed" for non-POST queries.
func TestHandler_Query_MethodNotAllowed(t *testing.T) {
	w := httptest.NewRecorder()
	NewHandler().ServeHTTP(w, MustNewHTTPRequest("GET", "/query", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("invalid status: %d", w.Code)
	}
}

// Ensure the handler returns an error if there is a parsing error..
func TestHandler_Query_ErrParse(t *testing.T) {
	h := NewHandler()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=db0&slices=0,1", strings.NewReader("bad_fn(")))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"function not found: bad_fn occurred at line 1, char 1"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can retrieve the version.
func TestHandler_Version(t *testing.T) {
	h := NewHandler()
	h.Version = "1.0.0"

	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("GET", "/version", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if w.Body.String() != `{"version":"1.0.0"}`+"\n" {
		t.Fatalf("unexpected body: %q", w.Body.String())
	}
}

// Ensure the handler can return a list of nodes for a slice.
func TestHandler_Slices_Nodes(t *testing.T) {
	h := NewHandler()
	h.Cluster = NewCluster(3)
	h.Cluster.ReplicaN = 2

	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("GET", "/slices/nodes?slice=0", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if w.Body.String() != `[{"host":"host2"},{"host":"host0"}]`+"\n" {
		t.Fatalf("unexpected body: %q", w.Body.String())
	}
}

// Ensure the handler can return expvars without panicking.
func TestHandler_Expvars(t *testing.T) {
	h := NewHandler()
	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("GET", "/debug/vars", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
}

// Handler represents a test wrapper for pilosa.Handler.
type Handler struct {
	*pilosa.Handler
	Executor HandlerExecutor
}

// NewHandler returns a new instance of Handler.
func NewHandler() *Handler {
	h := &Handler{
		Handler: pilosa.NewHandler(),
	}
	h.Handler.Executor = &h.Executor
	return h
}

// HandlerExecutor is a mock implementing pilosa.Handler.Executor.
type HandlerExecutor struct {
	cluster   *pilosa.Cluster
	ExecuteFn func(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error)
}

func (c *HandlerExecutor) Cluster() *pilosa.Cluster { return c.cluster }

func (c *HandlerExecutor) Execute(db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) (interface{}, error) {
	return c.ExecuteFn(db, query, slices, opt)
}

// Server represents a test wrapper for httptest.Server.
type Server struct {
	*httptest.Server
	Handler *Handler
}

// NewServer returns a test server running on a random port.
func NewServer() *Server {
	s := &Server{
		Handler: NewHandler(),
	}
	s.Server = httptest.NewServer(s.Handler.Handler)
	return s
}

// Host returns the hostname of the running server.
func (s *Server) Host() string {
	u, err := url.Parse(s.URL)
	if err != nil {
		panic(err)
	}
	return u.Host
}

// MustNewHTTPRequest creates a new HTTP request. Panic on error.
func MustNewHTTPRequest(method, urlStr string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		panic(err)
	}
	return req
}
