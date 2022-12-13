package http

// import (
// 	"encoding/json"
// 	"io"
// 	"net/http"
// 	"strconv"

	"github.com/gorilla/mux"
	"github.com/featurebasedb/featurebase/v3/dax/snapshotter"
	"github.com/featurebasedb/featurebase/v3/rbf"
)

// func Handler(s *snapshotter.Snapshotter) http.Handler {
// 	svr := &server{
// 		snapshotter: s,
// 	}

// 	router := mux.NewRouter()
// 	router.HandleFunc("/health", svr.getHealth).Methods("GET").Name("GetHealth")
// 	router.HandleFunc("/write-snapshot", svr.postWriteSnapshot).Methods("POST").Name("PostWriteSnapshot")
// 	router.HandleFunc("/read-snapshot", svr.getReadSnapshot).Methods("GET").Name("GetReadSnapshot")
// 	return router
// }

// type server struct {
// 	snapshotter *snapshotter.Snapshotter
// }

// // GET /health
// func (s *server) getHealth(w http.ResponseWriter, r *http.Request) {
// 	w.WriteHeader(http.StatusOK)
// }

// // POST /write-snapshot
// func (s *server) postWriteSnapshot(w http.ResponseWriter, r *http.Request) {
// 	bucket := r.URL.Query().Get("bucket")
// 	if bucket == "" {
// 		http.Error(w, "bucket required", http.StatusBadRequest)
// 		return
// 	}

// 	key := r.URL.Query().Get("key")
// 	if key == "" {
// 		http.Error(w, "key required", http.StatusBadRequest)
// 		return
// 	}

// 	versionArg := r.URL.Query().Get("version")
// 	versionInt64, err := strconv.ParseInt(versionArg, 10, 64)
// 	if err != nil {
// 		http.Error(w, "bad shard", http.StatusBadRequest)
// 		return
// 	}
// 	version := int(versionInt64)

// 	body := r.Body
// 	defer body.Close()

// 	if err := s.snapshotter.Write(bucket, key, version, body); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}

// 	resp := &WriteSnapshotResponse{}

// 	if err := json.NewEncoder(w).Encode(resp); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// }

// type WriteSnapshotResponse struct{}

// // GET /read-snapshot
// func (s *server) getReadSnapshot(w http.ResponseWriter, r *http.Request) {
// 	bucket := r.URL.Query().Get("bucket")
// 	if bucket == "" {
// 		http.Error(w, "bucket required", http.StatusBadRequest)
// 		return
// 	}

// 	key := r.URL.Query().Get("key")
// 	if key == "" {
// 		http.Error(w, "key required", http.StatusBadRequest)
// 		return
// 	}

// 	versionArg := r.URL.Query().Get("version")
// 	versionInt64, err := strconv.ParseInt(versionArg, 10, 64)
// 	if err != nil {
// 		http.Error(w, "bad shard", http.StatusBadRequest)
// 		return
// 	}
// 	version := int(versionInt64)

// 	rc, err := s.snapshotter.Read(bucket, key, version)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	defer rc.Close()

// 	// TODO: is rbf.PageSize a problem here for non-RBF snapshots (i.e. keys)?
// 	// Copy data to response body.
// 	if _, err := io.CopyBuffer(&passthroughWriter{w}, rc, make([]byte, rbf.PageSize)); err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// }

// // passthroughWriter is used to remove non-Writer interfaces from an io.Writer.
// // For example, a writer that implements io.ReaderFrom can change io.Copy() behavior.
// type passthroughWriter struct {
// 	w io.Writer
// }

// func (w *passthroughWriter) Write(p []byte) (int, error) {
// 	return w.w.Write(p)
// }
