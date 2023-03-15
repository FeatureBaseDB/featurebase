package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/queryer"
	"github.com/gorilla/mux"
)

func Handler(q *queryer.Queryer) http.Handler {
	svr := &server{
		queryer: q,
	}

	logRequestMiddleWare := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.Contains(r.URL.Path, "/health") {
				q.Logger().Printf("serving %s, %v", r.Method, r.URL)
			}
			next.ServeHTTP(w, r)
		})
	}

	router := mux.NewRouter()
	router.Use(logRequestMiddleWare)
	router.HandleFunc("/health", svr.getHealth).Methods("GET").Name("GetHealth")
	router.HandleFunc("/sql", svr.postSQL).Methods("POST").Name("PostSQL")
	router.HandleFunc("/databases/{databaseID}/sql", svr.postSQL).Methods("POST").Name("PostDatabaseSQL")

	return router
}

type server struct {
	queryer *queryer.Queryer
}

// GET /health
func (s *server) getHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// POST /sql
func (s *server) postSQL(w http.ResponseWriter, r *http.Request) {
	orgID := getOrganizationID(r)
	dbID := dax.DatabaseID(mux.Vars(r)["databaseID"])

	contentType := r.Header.Get("Content-Type")
	switch contentType {
	case "text/plain":
		qdbid := dax.NewQualifiedDatabaseID(orgID, dbID)
		resp, err := s.queryer.QuerySQL(r.Context(), qdbid, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

	case "application/json":
		body := r.Body
		defer body.Close()

		req := SQLRequest{}
		if err := json.NewDecoder(body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if orgID == "" {
			orgID = req.OrganizationID
		}
		if dbID == "" {
			dbID = req.DatabaseID
		}

		qdbid := dax.NewQualifiedDatabaseID(orgID, dbID)
		resp, err := s.queryer.QuerySQL(ctx, qdbid, strings.NewReader(req.SQL))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

	default:
		err := fmt.Errorf("unsupported request content-type '%s'", contentType)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func getOrganizationID(r *http.Request) dax.OrganizationID {
	return dax.OrganizationID(r.Header.Get("OrganizationID"))
}

type SQLRequest struct {
	OrganizationID dax.OrganizationID `json:"org-id"`
	DatabaseID     dax.DatabaseID     `json:"db-id"`
	SQL            string             `json:"sql"`
}
