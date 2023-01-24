// Package queryer provides the core query-related structs.
package queryer

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	fbcontext "github.com/featurebasedb/featurebase/v3/context"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/encoding/proto"
	"github.com/featurebasedb/featurebase/v3/errors"
	idkmds "github.com/featurebasedb/featurebase/v3/idk/mds"
	"github.com/featurebasedb/featurebase/v3/logger"
	featurebase_pql "github.com/featurebasedb/featurebase/v3/pql"
	fbproto "github.com/featurebasedb/featurebase/v3/proto"
	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner"
	plannertypes "github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/systemlayer"
	uuid "github.com/satori/go.uuid"
)

// Queryer represents the query layer in a Molecula implementation. The idea is
// that the externally-facing Molecula API would proxy query requests to a pool
// of "Queryer" nodes, which handle incoming query requests.
type Queryer struct {
	mu            sync.RWMutex
	orchestrators map[dax.QualifiedDatabaseID]*qualifiedOrchestrator

	fbClient *featurebase.InternalClient

	controller dax.Controller

	logger logger.Logger
}

// New returns a new instance of Queryer.
func New(cfg Config) *Queryer {
	q := &Queryer{
		controller:    dax.NewNopController(),
		orchestrators: make(map[dax.QualifiedDatabaseID]*qualifiedOrchestrator),
		logger:        logger.NopLogger,
	}

	if cfg.Logger != nil {
		q.logger = cfg.Logger
	}

	return q
}

// Orchestrator gets (or creates) an instance of qualifiedOrchestrator based on
// the provided dax.QualifiedDatabaseID.
func (q *Queryer) Orchestrator(qdbid dax.QualifiedDatabaseID) *qualifiedOrchestrator {
	// Try to get orchestrator under a read lock first.
	if orch := func() *qualifiedOrchestrator {
		q.mu.RLock()
		defer q.mu.RUnlock()
		if orch, ok := q.orchestrators[qdbid]; ok {
			return orch
		}
		return nil
	}(); orch != nil {
		return orch
	}

	// Since we didn't find an orchestrator under a read lock, obtain a write
	// lock and try a read/write.
	q.mu.Lock()
	defer q.mu.Unlock()
	if orch, ok := q.orchestrators[qdbid]; ok {
		return orch
	}

	sapi := newQualifiedSchemaAPI(qdbid, q.controller)

	orch := &orchestrator{
		schema:   sapi,
		trans:    NewMDSTranslator(q.controller),
		topology: &MDSTopology{controller: q.controller},
		// TODO(jaffee) using default http.Client probably bad... need to set some timeouts.
		client: q.fbClient,
		logger: q.logger,
	}

	qorch := newQualifiedOrchestrator(orch, qdbid)
	q.orchestrators[qdbid] = qorch

	return qorch
}

func (q *Queryer) SetController(controller dax.Controller) error {
	q.controller = controller
	return nil
}

func (q *Queryer) Start() error {
	if q.controller == nil {
		return errors.New(errors.ErrUncoded, "queryer requires controller to be configured")
	}

	// fbClient is an instance of internal client. It's used in one place in the
	// orchestrator (o.client.QueryNode()), but in that case, the host is
	// replaces with the actual host (another computer node) to connect to.
	// That's why we set it up with a dummy host here.
	fbClient, err := featurebase.NewInternalClient("fakehostname:8080",
		&http.Client{},
		featurebase.WithSerializer(proto.Serializer{}),
		featurebase.WithPathPrefix("should-not-be-used"),
	)
	if err != nil {
		return errors.Wrap(err, "setting up internal client")
	}
	q.fbClient = fbClient

	return nil
}

func (q *Queryer) QuerySQL(ctx context.Context, qdbid dax.QualifiedDatabaseID, sql string) (*featurebase.WireQueryResponse, error) {
	start := time.Now()

	ret := &featurebase.WireQueryResponse{}

	applyExecutionTime := func() {
		ret.ExecutionTime = time.Since(start).Microseconds()
	}

	applyError := func(e error) {
		ret.Error = e.Error()
		applyExecutionTime()
	}

	// If PQL, run that instead.
	if len(sql) > 0 && sql[0] == '[' {
		if pqlResp, err := q.parseAndQueryPQL(ctx, qdbid, sql); err != nil {
			applyError(errors.Wrap(err, "querying pql"))
			return ret, nil
		} else {
			ret = pqlResp
		}
		applyExecutionTime()

		return ret, nil
	}

	// Create a requestID and add it to the context.
	requestID, err := uuid.NewV4()
	if err != nil {
		applyError(errors.Wrap(err, "creating requestID"))
		return ret, nil
	}
	// put the requestId in the context
	ctx = fbcontext.WithRequestID(ctx, requestID.String())

	st, err := parser.NewParser(strings.NewReader(sql)).ParseStatement()
	if err != nil {
		applyError(errors.Wrap(err, "parsing sql"))
		return ret, nil
	}

	// SchemaAPI
	sapi := newQualifiedSchemaAPI(qdbid, q.controller)

	// Importer
	imp := idkmds.NewImporter(q.controller, qdbid, nil)

	// TODO(tlt): We need a dax-compatible implementation of the SystemAPI.
	sysapi := &featurebase.NopSystemAPI{}

	systemLayer := systemlayer.NewSystemLayer()

	pl := planner.NewExecutionPlanner(q.Orchestrator(qdbid), sapi, sysapi, systemLayer, imp, q.logger, sql)

	planOp, err := pl.CompilePlan(ctx, st)
	if err != nil {
		applyError(errors.Wrap(err, "compiling plan"))
		return ret, nil
	}

	// Get a query iterator.
	iter, err := planOp.Iterator(ctx, nil)
	if err != nil {
		applyError(errors.Wrap(err, "getting iterator"))
		return ret, nil
	}

	// Read schema.
	columns := planOp.Schema()
	schema := featurebase.WireQuerySchema{
		Fields: make([]*featurebase.WireQueryField, len(columns)),
	}
	for i, col := range columns {
		btype, err := dax.BaseTypeFromString(col.Type.BaseTypeName())
		if err != nil {
			applyError(errors.Wrap(err, "getting fieldtype from string"))
			return ret, nil
		}
		schema.Fields[i] = &featurebase.WireQueryField{
			Name:     dax.FieldName(col.ColumnName),
			Type:     strings.ToLower(col.Type.TypeDescription()), // TODO(tlt): remove this once sql3 uses BaseTypes.
			BaseType: btype,
			TypeInfo: col.Type.TypeInfo(),
		}
	}

	// Read rows.
	data := make([][]interface{}, 0)
	var currentRow plannertypes.Row
	for currentRow, err = iter.Next(ctx); err == nil; currentRow, err = iter.Next(ctx) {
		data = append(data, currentRow)
	}
	if err != nil && err != plannertypes.ErrNoMoreRows {
		applyError(errors.Wrap(err, "getting row"))
		return ret, nil
	}

	ret.Schema = schema
	ret.Data = data
	applyExecutionTime()

	return ret, nil
}

func (q *Queryer) parseAndQueryPQL(ctx context.Context, qdbid dax.QualifiedDatabaseID, sql string) (*featurebase.WireQueryResponse, error) {
	var i int
	for i = 1; sql[i] != ']'; i++ {
		if i == len(sql)-1 {
			return nil, errors.Errorf("couldn't parse table name out of '%s'", sql)
		}
	}
	table := sql[1:i]
	query := sql[i+1:]
	fmt.Println("got table/query", table, query)

	return q.queryPQL(ctx, qdbid, dax.TableName(table), query)
}

// convertIndex tries to covert any "index" specified in the call.Args map to a
// TableKeyer. Note, since the Call.CallIndex() method currently only looks for
// strings, we can't just set the value to a TableKeyer; we have to set it to
// the equivalent string and then parse it back out later. A TODO would be to
// modify Call.CallIndex() to be TableKeyer aware. I didn't do that along with
// these changes because I'm not sure if we want to introduce dax types into the
// pql package.
func (q *Queryer) convertIndex(ctx context.Context, qdbid dax.QualifiedDatabaseID, call *featurebase_pql.Call) {
	if index := call.CallIndex(); index != "" {
		qtbl, err := q.controller.TableByName(ctx, qdbid, dax.TableName(index))
		if err != nil {
			return
		}
		call.Args["index"] = string(qtbl.Key())
	}

	// Apply to children.
	for _, child := range call.Children {
		q.convertIndex(ctx, qdbid, child)
	}
}

func (q *Queryer) QueryPQL(ctx context.Context, qdbid dax.QualifiedDatabaseID, table dax.TableName, pql string) (*featurebase.WireQueryResponse, error) {
	start := time.Now()

	ret := &featurebase.WireQueryResponse{}

	applyExecutionTime := func() {
		ret.ExecutionTime = time.Since(start).Microseconds()
	}

	applyError := func(e error) {
		ret.Error = e.Error()
		applyExecutionTime()
	}

	if pqlResp, err := q.queryPQL(ctx, qdbid, table, pql); err != nil {
		applyError(errors.Wrap(err, "querying pql"))
		return ret, nil
	} else {
		ret = pqlResp
	}

	applyExecutionTime()

	return ret, nil
}

func (q *Queryer) queryPQL(ctx context.Context, qdbid dax.QualifiedDatabaseID, table dax.TableName, pql string) (*featurebase.WireQueryResponse, error) {
	// Parse the pql into a pql.Query containing []pql.Call.
	qry, err := featurebase_pql.NewParser(strings.NewReader(pql)).Parse()
	if err != nil {
		return nil, errors.Wrap(err, "parsing pql")
	}
	if len(qry.Calls) != 1 {
		return nil, errors.Errorf("must have exactly 1 query, but got: %+v", qry.Calls)
	}

	// Replace any "index" arguments within the PQL with a TableKey.
	q.convertIndex(ctx, qdbid, qry.Calls[0])

	qtbl, err := q.controller.TableByName(ctx, qdbid, dax.TableName(table))
	if err != nil {
		return nil, errors.Wrap(err, "converting index to qualified table")
	}

	results, err := q.Orchestrator(qdbid).Execute(ctx, qtbl, qry, nil, &featurebase.ExecOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "orchestrator.Execute")
	}
	if len(results.Results) != 1 {
		return nil, errors.Errorf("expected single result but got %+v", results.Results)
	}

	return pqlResultToQueryResult(results.Results[0])
}

func pqlResultToQueryResult(pqlResult interface{}) (*featurebase.WireQueryResponse, error) {
	toTabler, err := server.ToTablerWrapper(pqlResult)
	if err != nil {
		return nil, errors.Wrap(err, "wrapping as type ToTabler")
	}
	table, err := toTabler.ToTable()
	if err != nil {
		return nil, errors.Wrap(err, "ToTable")
	}

	return tableResponseToQueryResult(table)
}

func tableResponseToQueryResult(t *fbproto.TableResponse) (*featurebase.WireQueryResponse, error) {
	qr := &featurebase.WireQueryResponse{
		Schema: featurebase.WireQuerySchema{Fields: make([]*featurebase.WireQueryField, len(t.Headers))},
		Data:   make([][]interface{}, len(t.Rows)),
	}
	for i, ci := range t.Headers {
		qr.Schema.Fields[i] = &featurebase.WireQueryField{
			Name:     dax.FieldName(ci.Name),
			Type:     string(datatypeToBaseType(ci.Datatype)), // TODO(tlt): this doesn't contain typeInfo
			BaseType: datatypeToBaseType(ci.Datatype),
		}
	}

	for i, row := range t.Rows {
		qr.Data[i] = rowToSliceInterface(t.Headers, row)
	}

	return qr, nil
}

func datatypeToBaseType(ciDatatype string) dax.BaseType {
	switch ciDatatype {
	case "string":
		return dax.BaseTypeString
	case "uint64":
		return dax.BaseTypeID
	case "float64":
		// ??
		panic("float64 doesn't have sql3 field type?")
	case "int64":
		return dax.BaseTypeInt
	case "bool":
		return dax.BaseTypeBool
	case "decimal":
		return dax.BaseTypeDecimal
	case "timestamp":
		return dax.BaseTypeTimestamp
	case "[]string":
		return dax.BaseTypeStringSet
	case "[]uint64":
		return dax.BaseTypeIDSet
	// TODO []byte??
	default:
		panic(fmt.Sprintf("unknown ColumnInfo Datatype: %s", ciDatatype))
	}
}

func rowToSliceInterface(header []*fbproto.ColumnInfo, row *fbproto.Row) []interface{} {
	ret := make([]interface{}, len(row.Columns))
	for i, col := range row.Columns {
		switch header[i].Datatype {
		case "string":
			ret[i] = col.GetStringVal()
		case "uint64":
			ret[i] = col.GetUint64Val()
		case "int64":
			ret[i] = col.GetInt64Val()
		case "bool":
			ret[i] = col.GetBoolVal()
		case "[]byte":
			ret[i] = col.GetBlobVal()
		case "[]uint64":
			ret[i] = col.GetUint64ArrayVal()
		case "[]string":
			ret[i] = col.GetStringArrayVal()
		case "float64":
			ret[i] = col.GetFloat64Val()
		case "decimal":
			dec := col.GetDecimalVal()
			ret[i] = featurebase_pql.NewDecimal(dec.Value, dec.Scale)
		case "timestamp":
			ret[i] = col.GetTimestampVal()
		default:
			panic(fmt.Sprintf("don't know how to get value for columninfo datatype %s, val: %+v, type: %[2]T", header[i].Datatype, col.ColumnVal))
		}
	}
	return ret
}
