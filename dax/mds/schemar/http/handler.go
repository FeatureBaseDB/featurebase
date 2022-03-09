package http

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/schemar"
)

func Handler(s schemar.Schemar) http.Handler {
	svr := &server{
		schemar: s,
	}

	router := mux.NewRouter()
	router.HandleFunc("/health", svr.getHealth).Methods("GET").Name("GetHealth")
	router.HandleFunc("/create-table", svr.postCreateTable).Methods("POST").Name("PostCreateTable")
	router.HandleFunc("/drop-table", svr.postDropTable).Methods("POST").Name("PostDropTable")
	router.HandleFunc("/table", svr.postTable).Methods("POST").Name("PostTable")
	router.HandleFunc("/tables", svr.postTables).Methods("POST").Name("PostTables")
	return router
}

type server struct {
	schemar schemar.Schemar
}

// GET /health
func (s *server) getHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// POST /create-table
func (s *server) postCreateTable(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := &dax.QualifiedTable{}
	if err := json.NewDecoder(body).Decode(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := s.schemar.CreateTable(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := struct{}{}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// POST /drop-table
func (s *server) postDropTable(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := DropTableRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.TableKey.QualifiedTableID()

	err := s.schemar.DropTable(ctx, qtid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := struct{}{}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// // POST /create-field
// func (s *server) postCreateField(w http.ResponseWriter, r *http.Request) {
// 	body := r.Body
// 	defer body.Close()

// 	req := mds.CreateFieldRequest{}
// 	if err := json.NewDecoder(body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}

// 	resp, err := s.mds.CreateField(req)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}

// 	if err := json.NewEncoder(w).Encode(resp); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// }

// // POST /drop-field
// func (s *server) postDropField(w http.ResponseWriter, r *http.Request) {
// 	body := r.Body
// 	defer body.Close()

// 	req := mds.DropFieldRequest{}
// 	if err := json.NewDecoder(body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}

// 	resp, err := s.mds.DropField(req)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}

// 	if err := json.NewEncoder(w).Encode(resp); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// }

// POST /table
func (s *server) postTable(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := TableRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.TableKey.QualifiedTableID()

	resp, err := s.schemar.Table(ctx, qtid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// POST /tables
func (s *server) postTables(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := TablesRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	qual := dax.NewTableQualifier(req.OrganizationID, req.DatabaseID)
	resp, err := s.schemar.Tables(ctx, qual, req.TableIDs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type DropTableRequest struct {
	TableKey dax.TableKey `json:"table-key"`
}

type TableRequest struct {
	TableKey dax.TableKey `json:"table-key"`
}

type TablesRequest struct {
	OrganizationID dax.OrganizationID `json:"org-id"`
	DatabaseID     dax.DatabaseID     `json:"db-id"`
	TableIDs       dax.TableIDs       `json:"table-ids"`
}
