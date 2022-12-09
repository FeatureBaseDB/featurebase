package http

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/featurebasedb/featurebase/v3/dax/writelogger"
	"github.com/featurebasedb/featurebase/v3/logger"
)

func Handler(w *writelogger.WriteLogger, logger logger.Logger) http.Handler {
	svr := &server{
		writeLogger: w,
		logger:      logger,
	}

	router := mux.NewRouter()
	router.HandleFunc("/health", svr.getHealth).Methods("GET").Name("GetHealth")
	router.HandleFunc("/append-message", svr.postAppendMessage).Methods("POST").Name("PostAppendMessage")
	router.HandleFunc("/log-reader", svr.postLogReader).Methods("POST").Name("PostLogReader")
	router.HandleFunc("/delete-log", svr.postDeleteLog).Methods("POST").Name("PostDeleteLog")
	return router
}

type server struct {
	writeLogger *writelogger.WriteLogger
	logger      logger.Logger
}

// GET /health
func (s *server) getHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// POST /append-message
func (s *server) postAppendMessage(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	req := AppendMessageRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := s.writeLogger.AppendMessage(req.Bucket, req.Key, req.Version, req.Message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := AppendMessageResponse{}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type AppendMessageRequest struct {
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
	Version int    `json:"version"`
	Message []byte `json:"message"`
}

type AppendMessageResponse struct{}

// POST /log-reader
func (s *server) postLogReader(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	req := LogReaderRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	reader, closer, err := s.writeLogger.LogReader(req.Bucket, req.Key, req.Version)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	defer closer.Close()

	if _, err := io.Copy(w, reader); err != nil {
		s.logger.Printf("error streaming log data: %s", err)
	}
}

type LogReaderRequest struct {
	Bucket  string `json:"bucket"`
	Version int    `json:"version"`
	Key     string `json:"key"`
}

// POST /delete-log
func (s *server) postDeleteLog(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	req := DeleteLogRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.writeLogger.DeleteLog(req.Bucket, req.Key, req.Version); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type DeleteLogRequest struct {
	Bucket  string `json:"bucket"`
	Version int    `json:"version"`
	Key     string `json:"key"`
}
