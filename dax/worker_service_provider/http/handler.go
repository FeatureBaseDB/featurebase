package http

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/worker_service_provider"
	"github.com/gorilla/mux"
)

func Handler(wsp *worker_service_provider.WSP) http.Handler {
	svr := &server{
		wsp: wsp,
	}

	logRequestMiddleWare := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.Contains(r.URL.Path, "/health") {
				wsp.Logger().Debugf("serving %s, %v", r.Method, r.URL)
			}
			next.ServeHTTP(w, r)
		})
	}

	router := mux.NewRouter()
	router.Use(logRequestMiddleWare)
	router.HandleFunc("/health", svr.getHealth).Methods("GET").Name("GetHealth")
	router.HandleFunc("/claim", svr.postClaim).Methods("POST").Name("PostClaim")
	router.HandleFunc("/update", svr.postUpdate).Methods("POST").Name("PostUpdate")
	router.HandleFunc("/drop", svr.postDrop).Methods("POST").Name("PostDrop")

	return router
}

type server struct {
	wsp *worker_service_provider.WSP
}

// GET /health
func (s *server) getHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *server) postClaim(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	req := dax.WorkerService{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := s.wsp.ClaimService(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) postUpdate(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	req := dax.WorkerService{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := s.wsp.UpdateService(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) postDrop(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	req := dax.WorkerService{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := s.wsp.DropService(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
