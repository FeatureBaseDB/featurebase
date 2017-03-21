package pilosa_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

// Ensure the handler returns "not found" for invalid paths.
func TestHandler_NotFound(t *testing.T) {
	w := httptest.NewRecorder()
	NewHandler().ServeHTTP(w, MustNewHTTPRequest("GET", "/no_such_path", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("invalid status: %d", w.Code)
	}
}

// Ensure the handler can return the schema.
func TestHandler_Schema(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	d0 := idx.MustCreateDBIfNotExists("d0", pilosa.DBOptions{})
	d1 := idx.MustCreateDBIfNotExists("d1", pilosa.DBOptions{})

	if _, err := d0.CreateFrameIfNotExists("f1", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := d1.CreateFrameIfNotExists("f0", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := d0.CreateFrameIfNotExists("f0", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	h := NewHandler()
	h.Index = idx.Index
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("GET", "/schema", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"dbs":[{"name":"d0","frames":[{"name":"f0"},{"name":"f1"}]},{"name":"d1","frames":[{"name":"f0"}]}]}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can return the maxslice map.
func TestHandler_MaxSlices(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	idx.MustCreateFragmentIfNotExists("d0", "f0", 1).MustSetBits(30, (1*SliceWidth)+1)
	idx.MustCreateFragmentIfNotExists("d0", "f0", 1).MustSetBits(30, (1*SliceWidth)+2)
	idx.MustCreateFragmentIfNotExists("d0", "f0", 3).MustSetBits(30, (3*SliceWidth)+4)

	idx.MustCreateFragmentIfNotExists("d1", "f1", 0).MustSetBits(40, (0*SliceWidth)+1)
	idx.MustCreateFragmentIfNotExists("d1", "f1", 0).MustSetBits(40, (0*SliceWidth)+2)
	idx.MustCreateFragmentIfNotExists("d1", "f1", 0).MustSetBits(40, (0*SliceWidth)+8)

	h := NewHandler()
	h.Index = idx.Index
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("GET", "/slices/max", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"MaxSlices":{"d0":3,"d1":0}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can accept URL arguments.
func TestHandler_Query_Args_URL(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if db != "db0" {
			t.Fatalf("unexpected db: %s", db)
		} else if query.String() != `Count(Bitmap(id=100))` {
			t.Fatalf("unexpected query: %s", query.String())
		} else if !reflect.DeepEqual(slices, []uint64{0, 1}) {
			t.Fatalf("unexpected slices: %+v", slices)
		}
		return []interface{}{uint64(100)}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=db0&slices=0,1", strings.NewReader("Count( Bitmap( id=100))")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code, w.Body.String())
	} else if body := w.Body.String(); body != `{"results":[100]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can accept arguments via protobufs.
func TestHandler_Query_Args_Protobuf(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if db != "db0" {
			t.Fatalf("unexpected db: %s", db)
		} else if query.String() != `Count(Bitmap(id=100))` {
			t.Fatalf("unexpected query: %s", query.String())
		} else if !reflect.DeepEqual(slices, []uint64{0, 1}) {
			t.Fatalf("unexpected slices: %+v", slices)
		}
		return []interface{}{uint64(100)}, nil
	}

	// Generate request body.
	reqBody, err := proto.Marshal(&internal.QueryRequest{
		DB:     "db0",
		Query:  "Count(Bitmap(id=100))",
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
	NewHandler().ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=db0&slices=a,b", strings.NewReader("Bitmap(id=100)")))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"invalid slice argument"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can execute a query with a uint64 response as JSON.
func TestHandler_Query_Uint64_JSON(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{uint64(100)}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=db0&slices=0,1", strings.NewReader("Count( Bitmap( id=100))")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[100]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can execute a query with a uint64 response as protobufs.
func TestHandler_Query_Uint64_Protobuf(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{uint64(100)}, nil
	}

	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("POST", "/query", strings.NewReader("Count(Bitmap(id=100))"))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if n := resp.Results[0].N; n != 100 {
		t.Fatalf("unexpected n: %d", n)
	}
}

// Ensure the handler can execute a query that returns a bitmap as JSON.
func TestHandler_Query_Bitmap_JSON(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		bm := pilosa.NewBitmap(1, 3, 66, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": 1, "d": true}
		return []interface{}{bm}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=d", strings.NewReader("Bitmap(id=100)")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[{"attrs":{"a":"b","c":1,"d":true},"bits":[1,3,66,1048577]}]}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can execute a query that returns a bitmap with profiles as JSON.
func TestHandler_Query_Bitmap_Profiles_JSON(t *testing.T) {
	idx := NewIndex()
	defer idx.Close()

	// Create database and set profile attributes.
	db, err := idx.CreateDBIfNotExists("d", pilosa.DBOptions{})
	if err != nil {
		t.Fatal(err)
	} else if err := db.ProfileAttrStore().SetAttrs(3, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	} else if err := db.ProfileAttrStore().SetAttrs(66, map[string]interface{}{"y": 123, "z": false}); err != nil {
		t.Fatal(err)
	}

	h := NewHandler()
	h.Index = idx.Index
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		bm := pilosa.NewBitmap(1, 3, 66, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": 1, "d": true}
		return []interface{}{bm}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query?db=d&profiles=true", strings.NewReader("Bitmap(id=100)")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[{"attrs":{"a":"b","c":1,"d":true},"bits":[1,3,66,1048577]}],"profiles":[{"id":3,"attrs":{"x":"y"}},{"id":66,"attrs":{"y":123,"z":false}}]}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can execute a query that returns a bitmap as protobuf.
func TestHandler_Query_Bitmap_Protobuf(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		bm := pilosa.NewBitmap(1, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": int64(1), "d": true}
		return []interface{}{bm}, nil
	}

	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("POST", "/query", strings.NewReader("Bitmap(id=100)"))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if bits := resp.Results[0].Bitmap.Bits; !reflect.DeepEqual(bits, []uint64{1, SliceWidth + 1}) {
		t.Fatalf("unexpected bits: %+v", bits)
	} else if attrs := resp.Results[0].Bitmap.Attrs; len(attrs) != 3 {
		t.Fatalf("unexpected attr length: %d", len(attrs))
	} else if k, v := attrs[0].Key, attrs[0].StringValue; k != "a" || v != "b" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	} else if k, v := attrs[1].Key, attrs[1].UintValue; k != "c" || v != uint64(1) {
		t.Fatalf("unexpected attr[1]: %s=%v", k, v)
	} else if k, v := attrs[2].Key, attrs[2].BoolValue; k != "d" || v != true {
		t.Fatalf("unexpected attr[2]: %s=%v", k, v)
	}
}

// Ensure the handler can execute a query that returns a bitmap with profiles as protobuf.
func TestHandler_Query_Bitmap_Profiles_Protobuf(t *testing.T) {
	idx := NewIndex()
	defer idx.Close()

	// Create database and set profile attributes.
	db, err := idx.CreateDBIfNotExists("d", pilosa.DBOptions{})
	if err != nil {
		t.Fatal(err)
	} else if err := db.ProfileAttrStore().SetAttrs(1, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	}

	h := NewHandler()
	h.Index = idx.Index
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		bm := pilosa.NewBitmap(1, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": int64(1), "d": true}
		return []interface{}{bm}, nil
	}

	// Encode request body.
	buf, err := proto.Marshal(&internal.QueryRequest{
		DB:       "d",
		Query:    "Bitmap(id=100)",
		Profiles: true,
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
	if bits := resp.Results[0].Bitmap.Bits; !reflect.DeepEqual(bits, []uint64{1, SliceWidth + 1}) {
		t.Fatalf("unexpected bits: %+v", bits)
	} else if attrs := resp.Results[0].Bitmap.Attrs; len(attrs) != 3 {
		t.Fatalf("unexpected attr length: %d", len(attrs))
	} else if k, v := attrs[0].Key, attrs[0].StringValue; k != "a" || v != "b" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	} else if k, v := attrs[1].Key, attrs[1].UintValue; k != "c" || v != uint64(1) {
		t.Fatalf("unexpected attr[1]: %s=%v", k, v)
	} else if k, v := attrs[2].Key, attrs[2].BoolValue; k != "d" || v != true {
		t.Fatalf("unexpected attr[2]: %s=%v", k, v)
	}

	if a := resp.Profiles; len(a) != 1 {
		t.Fatalf("unexpected profiles length: %d", len(a))
	} else if a[0].ID != 1 {
		t.Fatalf("unexpected id: %d", a[0].ID)
	} else if len(a[0].Attrs) != 1 {
		t.Fatalf("unexpected profile attr length: %d", len(a))
	} else if k, v := a[0].Attrs[0].Key, a[0].Attrs[0].StringValue; k != "x" || v != "y" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	}
}

// Ensure the handler can execute a query that returns pairs as JSON.
func TestHandler_Query_Pairs_JSON(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{[]pilosa.Pair{
			{Key: 1, Count: 2},
			{Key: 3, Count: 4},
		}}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query", strings.NewReader(`TopN(frame=x, n=2)`)))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[[{"key":1,"count":2},{"key":3,"count":4}]]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can execute a query that returns pairs as protobuf.
func TestHandler_Query_Pairs_Protobuf(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{[]pilosa.Pair{
			{Key: 1, Count: 2},
			{Key: 3, Count: 4},
		}}, nil
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
	} else if a := resp.Results[0].GetPairs(); len(a) != 2 {
		t.Fatalf("unexpected pair length: %d", len(a))
	}
}

// Ensure the handler can return an error as JSON.
func TestHandler_Query_Err_JSON(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return nil, errors.New("marker")
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("POST", "/query", strings.NewReader(`Bitmap(id=100)`)))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"marker"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can return an error as protobuf.
func TestHandler_Query_Err_Protobuf(t *testing.T) {
	h := NewHandler()
	h.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
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
	} else if s := resp.Err; s != `marker` {
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
	} else if body := w.Body.String(); body != `{"error":"expected comma, right paren, or identifier, found \"\" occurred at line 1, char 8"}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can delete a database.
func TestHandler_DB_Delete(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	s := NewServer()
	s.Handler.Index = idx.Index
	defer s.Close()

	// Create database.
	if _, err := idx.CreateDBIfNotExists("d", pilosa.DBOptions{}); err != nil {
		t.Fatal(err)
	}

	// Send request to delete database.
	resp, err := http.DefaultClient.Do(MustNewHTTPRequest("DELETE", s.URL+"/db", strings.NewReader(`{"db":"d"}`)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Verify body response.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if buf, err := ioutil.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	} else if string(buf) != "{}\n" {
		t.Fatalf("unexpected response body: %s", buf)
	}

	// Verify database is gone.
	if idx.DB("d") != nil {
		t.Fatal("expected nil database")
	}
}

// Ensure handler can delete a frame.
func TestHandler_DeleteFrame(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()
	d0 := idx.MustCreateDBIfNotExists("d0", pilosa.DBOptions{})
	if _, err := d0.CreateFrameIfNotExists("f1", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	h := NewHandler()
	h.Index = idx.Index
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("DELETE", "/frame", strings.NewReader(`{"db":"d0","frame":"f1"}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	} else if f := idx.DB("d0").Frame("f1"); f != nil {
		t.Fatal("expected nil frame")
	}
}

// Ensure handler can set the DB time quantum.
func TestHandler_SetDBTimeQuantum(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()
	idx.MustCreateDBIfNotExists("d0", pilosa.DBOptions{})

	h := NewHandler()
	h.Index = idx.Index
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("PATCH", "/db/time_quantum", strings.NewReader(`{"db":"d0","time_quantum":"ymdh"}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	} else if q := idx.DB("d0").TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected time quantum: %s", q)
	}
}

// Ensure handler can set the frame time quantum.
func TestHandler_SetFrameTimeQuantum(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	// Create frame.
	if _, err := idx.MustCreateDBIfNotExists("d0", pilosa.DBOptions{}).CreateFrame("f1", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	h := NewHandler()
	h.Index = idx.Index
	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("PATCH", "/frame/time_quantum", strings.NewReader(`{"db":"d0","frame":"f1","time_quantum":"ymdh"}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	} else if q := idx.DB("d0").Frame("f1").TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected time quantum: %s", q)
	}
}

// Ensure the handler can return data in differing blocks for a database.
func TestHandler_DB_AttrStore_Diff(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	s := NewServer()
	s.Handler.Index = idx.Index
	defer s.Close()

	// Set attributes on the database.
	db, err := idx.CreateDBIfNotExists("d", pilosa.DBOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ProfileAttrStore().SetAttrs(1, map[string]interface{}{"foo": 1, "bar": 2}); err != nil {
		t.Fatal(err)
	} else if err := db.ProfileAttrStore().SetAttrs(100, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	} else if err := db.ProfileAttrStore().SetAttrs(200, map[string]interface{}{"snowman": "☃"}); err != nil {
		t.Fatal(err)
	}

	// Retrieve block checksums.
	blks, err := db.ProfileAttrStore().Blocks()
	if err != nil {
		t.Fatal(err)
	}

	// Remove block #0 and alter block 2's checksum.
	blks = blks[1:]
	blks[1].Checksum = []byte("MISMATCHED_CHECKSUM")

	// Send block checksums to determine diff.
	resp, err := http.Post(
		s.URL+"/db/attr/diff?db=d",
		"application/json",
		strings.NewReader(`{"db":"d", "blocks":`+string(MustMarshalJSON(blks))+`}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Read and validate body.
	if body := string(MustReadAll(resp.Body)); body != `{"attrs":{"1":{"bar":2,"foo":1},"200":{"snowman":"☃"}}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can return data in differing blocks for a frame.
func TestHandler_Frame_AttrStore_Diff(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	s := NewServer()
	s.Handler.Index = idx.Index
	defer s.Close()

	// Set attributes on the database.
	d := idx.MustCreateDBIfNotExists("d", pilosa.DBOptions{})
	f, err := d.CreateFrameIfNotExists("meta", pilosa.FrameOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.BitmapAttrStore().SetAttrs(1, map[string]interface{}{"foo": 1, "bar": 2}); err != nil {
		t.Fatal(err)
	} else if err := f.BitmapAttrStore().SetAttrs(100, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	} else if err := f.BitmapAttrStore().SetAttrs(200, map[string]interface{}{"snowman": "☃"}); err != nil {
		t.Fatal(err)
	}

	// Retrieve block checksums.
	blks, err := f.BitmapAttrStore().Blocks()
	if err != nil {
		t.Fatal(err)
	}

	// Remove block #0 and alter block 2's checksum.
	blks = blks[1:]
	blks[1].Checksum = []byte("MISMATCHED_CHECKSUM")

	// Send block checksums to determine diff.
	resp, err := http.Post(
		s.URL+"/frame/attr/diff?db=d",
		"application/json",
		strings.NewReader(`{"db":"d", "frame":"meta", "blocks":`+string(MustMarshalJSON(blks))+`}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Read and validate body.
	if body := string(MustReadAll(resp.Body)); body != `{"attrs":{"1":{"bar":2,"foo":1},"200":{"snowman":"☃"}}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can backup a fragment and then restore it.
func TestHandler_Fragment_BackupRestore(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	s := NewServer()
	s.Handler.Index = idx.Index
	defer s.Close()

	// Set bits in the index.
	f0 := idx.MustCreateFragmentIfNotExists("d", "f", 0)
	f0.MustSetBits(100, 1, 2, 3)

	// Begin backing up from slice d/f/0.
	resp, err := http.Get(s.URL + "/fragment/data?db=d&frame=f&slice=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Ensure response came back OK.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected backup status code: %d", resp.StatusCode)
	}

	// Create frame.
	if _, err := idx.MustCreateDBIfNotExists("x", pilosa.DBOptions{}).CreateFrame("y", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Restore backup to slice x/y/0.
	if resp, err := http.Post(s.URL+"/fragment/data?db=x&frame=y&slice=0", "application/octet-stream", resp.Body); err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("unexpected restore status code: %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// Verify data is correctly restored.
	f1 := idx.Fragment("x", "y", 0)
	if f1 == nil {
		t.Fatal("fragment x/y/0 not created")
	} else if bits := f1.Bitmap(100).Bits(); !reflect.DeepEqual(bits, []uint64{1, 2, 3}) {
		t.Fatalf("unexpected restored bits: %+v", bits)
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

// Ensure the handler can return a list of nodes for a fragment.
func TestHandler_Fragment_Nodes(t *testing.T) {
	h := NewHandler()
	h.Cluster = NewCluster(3)
	h.Cluster.ReplicaN = 2

	w := httptest.NewRecorder()
	r := MustNewHTTPRequest("GET", "/fragment/nodes?db=X&slice=0", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if w.Body.String() != `[{"host":"host1"},{"host":"host2"}]`+"\n" {
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
	h.Handler.LogOutput = ioutil.Discard
	return h
}

// HandlerExecutor is a mock implementing pilosa.Handler.Executor.
type HandlerExecutor struct {
	cluster   *pilosa.Cluster
	ExecuteFn func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error)
}

func (c *HandlerExecutor) Cluster() *pilosa.Cluster { return c.cluster }

func (c *HandlerExecutor) Execute(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
	return c.ExecuteFn(ctx, db, query, slices, opt)
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

	// Update handler to use hostname.
	s.Handler.Host = s.Host()

	// Create a default cluster on the handler
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()

	return s
}

// Host returns the hostname of the running server.
func (s *Server) Host() string { return MustParseURLHost(s.URL) }

// MustParseURLHost parses rawurl and returns the hostname. Panic on error.
func MustParseURLHost(rawurl string) string {
	u, err := url.Parse(rawurl)
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

// MustMarshalJSON marshals v to JSON. Panic on error.
func MustMarshalJSON(v interface{}) []byte {
	buf, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return buf
}

// MustReadAll reads a reader into a buffer and returns it. Panic on error.
func MustReadAll(r io.Reader) []byte {
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return buf
}

type MessageBin struct {
	Cluster         *pilosa.Cluster
	messageReceived proto.Message
}

func NewMessageBin() *MessageBin {
	return &MessageBin{}
}

func (m *MessageBin) messageHandler(pb proto.Message) error {
	m.messageReceived = pb
	return nil
}

func NewHTTPMessageBin(s *Server, nodes []*pilosa.Node) (*MessageBin, error) {
	ns := pilosa.NewHTTPNodeSet()
	mb := NewMessageBin()
	ns.SetMessageHandler(mb.messageHandler)
	c := pilosa.Cluster{
		Nodes:   nodes,
		NodeSet: ns,
	}
	mb.Cluster = &c
	s.Handler.Cluster = &c
	s.Handler.Messenger = ns

	i, err := c.NodeSet.Join(c.Nodes)
	if i != int(0) {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	return mb, nil
}

// Ensure that an HTTP message sent to the cluster reaches all nodes.
func TestHTTPNodeSet_Base(t *testing.T) {

	// servers
	s1 := NewServer()
	s2 := NewServer()
	s3 := NewServer()
	nodes := []*pilosa.Node{
		{Host: s1.Host()},
		{Host: s2.Host()},
		{Host: s3.Host()},
	}

	// node 1
	mb1, err := NewHTTPMessageBin(s1, nodes)
	if err != nil {
		t.Fatalf("unable to create message bin: %s", err)
	}

	// node2
	mb2, err := NewHTTPMessageBin(s2, nodes)
	if err != nil {
		t.Fatalf("unable to create message bin: %s", err)
	}

	// node3
	mb3, err := NewHTTPMessageBin(s3, nodes)
	if err != nil {
		t.Fatalf("unable to create message bin: %s", err)
	}

	// message
	msg := &internal.CreateSliceMessage{
		DB:    "d",
		Slice: 8,
	}

	// send message
	if err := mb1.Cluster.NodeSet.(pilosa.Messenger).SendMessage(msg, ""); err != nil {
		t.Fatalf("failure sending message: %s", err)
	}

	if !reflect.DeepEqual(mb1.messageReceived, msg) {
		t.Fatalf("unexpected message received by node1: %s", mb1.messageReceived)
	}
	if !reflect.DeepEqual(mb2.messageReceived, msg) {
		t.Fatalf("unexpected message received by node2: %s", mb2.messageReceived)
	}
	if !reflect.DeepEqual(mb3.messageReceived, msg) {
		t.Fatalf("unexpected message received by node3: %s", mb3.messageReceived)
	}
}
