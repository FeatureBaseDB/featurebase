package http

import (
	"encoding/json"
	"net/http"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/gorilla/mux"
)

func Handler(c *controller.Controller) http.Handler {
	server := &server{
		controller: c,
	}

	router := mux.NewRouter()
	router.HandleFunc("/health", server.getHealth).Methods("GET").Name("GetHealth")

	// controller endpoints.
	router.HandleFunc("/create-database", server.postCreateDatabase).Methods("POST").Name("PostCreateDatabase")
	router.HandleFunc("/drop-database", server.postDropDatabase).Methods("POST").Name("PostDropDatabase")
	router.HandleFunc("/database-by-id", server.postDatabaseByID).Methods("POST").Name("PostDatabaseByID")
	router.HandleFunc("/database-by-name", server.postDatabaseByName).Methods("POST").Name("PostDatabaseByName")
	router.HandleFunc("/databases", server.postDatabases).Methods("POST").Name("PostDatabases")
	router.HandleFunc("/database/options", server.patchDatabaseOptions).Methods("PATCH").Name("PatchDatabaseOptions")

	router.HandleFunc("/create-table", server.postCreateTable).Methods("POST").Name("PostCreateTable")
	router.HandleFunc("/drop-table", server.postDropTable).Methods("POST").Name("PostDropTable")
	router.HandleFunc("/create-field", server.postCreateField).Methods("POST").Name("PostCreateField")
	router.HandleFunc("/drop-field", server.postDropField).Methods("POST").Name("PostDropField")
	router.HandleFunc("/table", server.postTable).Methods("POST").Name("PostTable")
	router.HandleFunc("/table-id", server.postTableID).Methods("POST").Name("PostTable")
	router.HandleFunc("/tables", server.postTables).Methods("POST").Name("PostTables")

	router.HandleFunc("/ingest-partition", server.postIngestPartition).Methods("POST").Name("PostIngestPartition")
	router.HandleFunc("/ingest-shard", server.postIngestShard).Methods("POST").Name("PostIngestShard")

	router.HandleFunc("/snapshot", server.postSnapshot).Methods("POST").Name("PostSnapshot")
	router.HandleFunc("/snapshot/shard-data", server.postSnapshotShardData).Methods("POST").Name("PostShapshotShardData")
	router.HandleFunc("/snapshot/table-keys", server.postSnapshotTableKeys).Methods("POST").Name("PostShapshotTableKeys")
	router.HandleFunc("/snapshot/field-keys", server.postSnapshotFieldKeys).Methods("POST").Name("PostShapshotFieldKeys")

	// controller endpoints.
	router.HandleFunc("/register-node", server.postRegisterNode).Methods("POST").Name("PostRegisterNode")
	router.HandleFunc("/register-nodes", server.postRegisterNodes).Methods("POST").Name("PostRegisterNodes")
	router.HandleFunc("/deregister-nodes", server.postDeregisterNodes).Methods("POST").Name("PostDeregisterNodes")
	router.HandleFunc("/check-in-node", server.postCheckInNode).Methods("POST").Name("PostCheckInNode")
	router.HandleFunc("/compute-nodes", server.postComputeNodes).Methods("POST").Name("PostComputeNodes")
	router.HandleFunc("/translate-nodes", server.postTranslateNodes).Methods("POST").Name("PostTranslateNodes")

	// debug endpoints
	router.HandleFunc("/debug/nodes", server.getDebugNodes).Methods("GET").Name("GetDebugNodes")
	router.HandleFunc("/debug/balancer", server.getDebugBalancer).Methods("GET").Name("getDebugBalancer")

	return router
}

type server struct {
	controller *controller.Controller
}

// GET /health
func (s *server) getHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// POST /create-database
func (s *server) postCreateDatabase(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := &dax.QualifiedDatabase{}
	if err := json.NewDecoder(body).Decode(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := s.controller.CreateDatabase(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// POST /drop-database
func (s *server) postDropDatabase(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := dax.QualifiedDatabaseID{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := s.controller.DropDatabase(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// POST /database-by-id
func (s *server) postDatabaseByID(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	qdbid := dax.QualifiedDatabaseID{}
	if err := json.NewDecoder(body).Decode(&qdbid); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.controller.DatabaseByID(ctx, qdbid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// POST /database-by-name
func (s *server) postDatabaseByName(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := &DatabaseByNameRequest{}
	if err := json.NewDecoder(body).Decode(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.controller.DatabaseByName(ctx, req.OrganizationID, req.Name)
	if err != nil {
		http.Error(w, errors.MarshalJSON(err), http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type DatabaseByNameRequest struct {
	OrganizationID dax.OrganizationID `json:"org-id"`
	Name           dax.DatabaseName   `json:"name"`
}

// POST /databases
func (s *server) postDatabases(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := DatabasesRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ids := req.DatabaseIDs

	resp, err := s.controller.Databases(ctx, req.OrganizationID, ids...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type DatabasesRequest struct {
	OrganizationID dax.OrganizationID `json:"org-id"`
	DatabaseIDs    dax.DatabaseIDs    `json:"database-ids"`
	// DatabaseNames     dax.DatabaseNames     `json:"database-names"`
}

// handlePatchDatabaseOptions handles updates to database options.
func (s *server) patchDatabaseOptions(w http.ResponseWriter, r *http.Request) {
	// Decode request.
	var req DatabaseOptionRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.controller.SetDatabaseOption(r.Context(), req.QualifiedDatabaseID, req.Option, req.Value); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// DatabaseOptionRequest represents a change to a database option. The thinking
// is to only support changing one database option at a time to keep the
// implementation sane. At time of writing, only WorkersMin is supported.
type DatabaseOptionRequest struct {
	QualifiedDatabaseID dax.QualifiedDatabaseID `json:"qdbid"`
	Option              string                  `json:"option"`
	Value               string                  `json:"value"`
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

	err := s.controller.CreateTable(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// POST /table
func (s *server) postTable(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	qtid := dax.QualifiedTableID{}
	if err := json.NewDecoder(body).Decode(&qtid); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.controller.TableByID(ctx, qtid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// POST /table-id
func (s *server) postTableID(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := dax.QualifiedTableID{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtbl, err := s.controller.TableByName(ctx, req.QualifiedDatabaseID, req.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	qtid := qtbl.QualifiedID()

	if err := json.NewEncoder(w).Encode(qtid); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// POST /drop-table
func (s *server) postDropTable(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := dax.QualifiedTableID{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := s.controller.DropTable(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// POST /create-field
func (s *server) postCreateField(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := CreateFieldRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.TableKey.QualifiedTableID()

	err := s.controller.CreateField(ctx, qtid, req.Field)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type CreateFieldRequest struct {
	TableKey dax.TableKey `json:"table-key"`
	Field    *dax.Field   `json:"field"`
}

// POST /drop-field
func (s *server) postDropField(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := DropFieldRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.Table

	err := s.controller.DropField(ctx, qtid, req.Field)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type DropFieldRequest struct {
	Table dax.QualifiedTableID `json:"table"`
	Field dax.FieldName        `json:"fields"`
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

	qdbid := dax.NewQualifiedDatabaseID(req.OrganizationID, req.DatabaseID)
	ids := req.TableIDs

	resp, err := s.controller.Tables(ctx, qdbid, ids...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type TablesRequest struct {
	OrganizationID dax.OrganizationID `json:"org-id"`
	DatabaseID     dax.DatabaseID     `json:"db-id"`
	TableIDs       dax.TableIDs       `json:"table-ids"`
	TableNames     dax.TableNames     `json:"table-names"`
}

// POST /ingest-partition
func (s *server) postIngestPartition(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := IngestPartitionRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.Table

	addr, err := s.controller.IngestPartition(ctx, qtid, req.Partition)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := &IngestPartitionResponse{
		Address: addr,
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type IngestPartitionRequest struct {
	Table     dax.QualifiedTableID `json:"table"`
	Partition dax.PartitionNum     `json:"partition"`
}

type IngestPartitionResponse struct {
	Address dax.Address `json:"address"`
}

// POST /ingest-shard
func (s *server) postIngestShard(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := IngestShardRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.Table

	addr, err := s.controller.IngestShard(ctx, qtid, req.Shard)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := &IngestShardResponse{
		Address: addr,
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type IngestShardRequest struct {
	Table dax.QualifiedTableID `json:"table"`
	Shard dax.ShardNum         `json:"shard"`
}

type IngestShardResponse struct {
	Address dax.Address `json:"address"`
}

// POST /snapshot
// High level snapshot endpoint to snapshot everything in a table.
func (s *server) postSnapshot(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := dax.QualifiedTableID{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.controller.SnapshotTable(ctx, req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// POST /snapshot/shard-data
func (s *server) postSnapshotShardData(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := SnapshotShardRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.Table

	if err := s.controller.SnapshotShardData(ctx, qtid, req.Shard); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// SnapshotShardRequest is used to specify the table/shard to snapshot.
type SnapshotShardRequest struct {
	Table dax.QualifiedTableID `json:"table"`
	Shard dax.ShardNum         `json:"shard"`
}

// POST /snapshot/table-keys
func (s *server) postSnapshotTableKeys(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := SnapshotTableKeysRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.Table

	if err := s.controller.SnapshotTableKeys(ctx, qtid, req.Partition); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// SnapshotTableKeysRequest is used to specify the table/partition/keys to
// snapshot.
type SnapshotTableKeysRequest struct {
	Table     dax.QualifiedTableID `json:"table"`
	Partition dax.PartitionNum     `json:"partition"`
}

// POST /snapshot/field-keys
func (s *server) postSnapshotFieldKeys(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := SnapshotFieldKeysRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.Table

	if err := s.controller.SnapshotFieldKeys(ctx, qtid, req.Field); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// SnapshotFieldKeysRequest is used to specify the field/keys to snapshot.
type SnapshotFieldKeysRequest struct {
	Table dax.QualifiedTableID `json:"table"`
	Field dax.FieldName        `json:"field"`
}

// POST /register-node
func (s *server) postRegisterNode(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := RegisterNodeRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	node := &dax.Node{
		Address:   req.Address,
		RoleTypes: req.RoleTypes,
	}

	if err := s.controller.RegisterNode(ctx, node); err != nil {
		http.Error(w, errors.MarshalJSON(err), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type RegisterNodeRequest struct {
	Address dax.Address `json:"address"`

	// RoleTypes allows a registering node to specify which role type(s) it is
	// capable of filling. The controller will not assign a role to this node
	// with a type not included in RoleTypes.
	RoleTypes []dax.RoleType `json:"role-types"`
}

// POST /register-nodes
func (s *server) postRegisterNodes(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := RegisterNodesRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.controller.RegisterNodes(ctx, req.Nodes...); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type RegisterNodesRequest struct {
	Nodes []*dax.Node `json:"nodes"`
}

// POST /deregister-nodes
func (s *server) postDeregisterNodes(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := DeregisterNodesRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.controller.DeregisterNodes(ctx, req.Addresses...); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type DeregisterNodesRequest struct {
	Addresses []dax.Address `json:"addresses"`
}

// POST /check-in-node
func (s *server) postCheckInNode(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := CheckInNodeRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	node := &dax.Node{
		Address:   req.Address,
		RoleTypes: req.RoleTypes,
	}

	if err := s.controller.CheckInNode(ctx, node); err != nil {
		http.Error(w, errors.MarshalJSON(err), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type CheckInNodeRequest struct {
	Address dax.Address `json:"address"`

	// RoleTypes allows a registering node to specify which role type(s) it is
	// capable of filling. The controller will not assign a role to this node
	// with a type not included in RoleTypes.
	RoleTypes []dax.RoleType `json:"role-types"`
}

// POST /compute-nodes
func (s *server) postComputeNodes(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := ComputeNodesRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.Table

	nodes, err := s.controller.ComputeNodes(ctx, qtid, req.Shards)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := ComputeNodesResponse{
		ComputeNodes: nodes,
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (s *server) getDebugNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.controller.DebugNodes(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(nodes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (s *server) getDebugBalancer(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.controller.CurrentState(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(nodes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

// ComputeNodesRequest is used to specify the table/shards to consider in the
// ComputeNodes method call. If IsWrite is true, shards which are not currently
// being managed by the underlying Controller will be added to (registered with)
// the Controller and, if adequate compute is available, will be associated with
// a compute node.
type ComputeNodesRequest struct {
	Table   dax.QualifiedTableID `json:"table"`
	Shards  dax.ShardNums        `json:"shards"`
	IsWrite bool                 `json:"is-write"`
}

// ComputeNodesResponse contains the list of compute nodes returned based on the
// table/shards specified in the ComputeNodeRequest. It's possible that shards
// provided are not included in this response. That might happen if there are
// currently no active compute nodes.
type ComputeNodesResponse struct {
	ComputeNodes []dax.ComputeNode `json:"compute-nodes"`
}

// POST /translate-nodes
func (s *server) postTranslateNodes(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	ctx := r.Context()

	req := TranslateNodesRequest{}
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	qtid := req.Table

	nodes, err := s.controller.TranslateNodes(ctx, qtid, req.Partitions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := TranslateNodesResponse{
		TranslateNodes: nodes,
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// TranslateNodesRequest is used to specify the table/partitions to consider in
// the TranslateNodes method call.
type TranslateNodesRequest struct {
	Table      dax.QualifiedTableID `json:"table"`
	Partitions dax.PartitionNums    `json:"partitions"`
	IsWrite    bool                 `json:"is-write"`
}

// TranslateNodesResponse contains the list of translate nodes returned based on
// the table/partitions specified in the TranslateNodeRequest. It's possible
// that partitions provided are not included in this response. That might happen
// if there are currently no active translate nodes.
type TranslateNodesResponse struct {
	TranslateNodes []dax.TranslateNode `json:"translate-nodes"`
}
