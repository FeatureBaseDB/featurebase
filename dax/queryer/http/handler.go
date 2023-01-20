package http

import (
	"encoding/json"
	"net/http"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/queryer"
	"github.com/gorilla/mux"
)

func Handler(q *queryer.Queryer) http.Handler {
	svr := &server{
		queryer: q,
	}

	router := mux.NewRouter()
	router.HandleFunc("/health", svr.getHealth).Methods("GET").Name("GetHealth")
	router.HandleFunc("/query", svr.postQuery).Methods("POST").Name("PostQuery")

	// /sql is a subset of /query, added here to provide an easy integration
	// with the FeatureBase cli tool.
	router.HandleFunc("/sql", svr.postSQL).Methods("POST").Name("PostSQL")

	return router
}

type server struct {
	queryer *queryer.Queryer
}

// GET /health
func (s *server) getHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// POST /query
func (s *server) postQuery(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	req := QueryRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	var resp interface{}
	var err error
	qdbid := dax.NewQualifiedDatabaseID(req.OrganizationID, req.DatabaseID)
	if req.SQL != "" {
		resp, err = s.queryer.QuerySQL(ctx, qdbid, req.SQL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		resp, err = s.queryer.QueryPQL(ctx, qdbid, req.Table, req.PQL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// POST /sql
func (s *server) postSQL(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	req := SQLRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	qdbid := dax.NewQualifiedDatabaseID(req.OrganizationID, req.DatabaseID)
	resp, err := s.queryer.QuerySQL(ctx, qdbid, req.SQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type QueryRequest struct {
	OrganizationID dax.OrganizationID `json:"org-id"`
	DatabaseID     dax.DatabaseID     `json:"db-id"`
	Table          dax.TableName      `json:"table-name"`
	PQL            string             `json:"pql"`
	SQL            string             `json:"sql"`
}

type SQLRequest struct {
	OrganizationID dax.OrganizationID `json:"org-id"`
	DatabaseID     dax.DatabaseID     `json:"db-id"`
	SQL            string             `json:"sql"`
}

type QueryResponse interface{}
