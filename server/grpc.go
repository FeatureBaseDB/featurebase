// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/logger"
	pb "github.com/pilosa/pilosa/v2/proto"
	vdsm_pb "github.com/pilosa/pilosa/v2/proto/vdsm"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// GRPCHandler contains methods which handle the various gRPC requests.
type GRPCHandler struct {
	api               *pilosa.API
	logger            logger.Logger
	stats             stats.StatsClient
	inspectDeprecated sync.Once
}

func NewGRPCHandler(api *pilosa.API) *GRPCHandler {
	return &GRPCHandler{api: api, logger: logger.NopLogger, stats: stats.NopStatsClient}
}

func (h *GRPCHandler) WithLogger(logger logger.Logger) *GRPCHandler {
	h.logger = logger
	return h
}

func (h *GRPCHandler) WithStats(stats stats.StatsClient) *GRPCHandler {
	h.stats = stats
	return h
}

// errorToStatusError appends an appropriate grpc status code
// to the error (returning it as a status.Error).
func errToStatusError(err error) error {
	if err == nil {
		return status.New(codes.OK, "").Err()
	}

	// Check error string.
	switch cause := errors.Cause(err); cause {
	case pilosa.ErrIndexNotFound,
		pilosa.ErrFieldNotFound,
		pilosa.ErrForeignIndexNotFound,
		pilosa.ErrBSIGroupNotFound:
		return status.Error(codes.NotFound, err.Error())

	case pilosa.ErrIndexExists,
		pilosa.ErrFieldExists,
		pilosa.ErrBSIGroupExists:
		return status.Error(codes.AlreadyExists, err.Error())

	case pilosa.ErrIndexRequired,
		pilosa.ErrFieldRequired,
		pilosa.ErrColumnRequired,
		pilosa.ErrBSIGroupNameRequired,
		pilosa.ErrName,
		pilosa.ErrQueryRequired,
		pilosa.ErrFieldsArgumentRequired,
		pilosa.ErrIntFieldWithKeys,
		pilosa.ErrDecimalFieldWithKeys:
		return status.Error(codes.FailedPrecondition, err.Error())

	case pilosa.ErrInvalidView,
		pilosa.ErrInvalidBSIGroupType,
		pilosa.ErrInvalidBSIGroupValueType,
		pilosa.ErrInvalidCacheType:
		return status.Error(codes.InvalidArgument, err.Error())

	case pilosa.ErrDecimalOutOfRange,
		pilosa.ErrBSIGroupValueTooLow,
		pilosa.ErrBSIGroupValueTooHigh,
		pilosa.ErrInvalidRangeOperation,
		pilosa.ErrInvalidBetweenValue:
		return status.Error(codes.OutOfRange, err.Error())

	case pilosa.ErrQueryTimeout:
		return status.Error(codes.DeadlineExceeded, err.Error())

	case pilosa.ErrQueryCancelled:
		return status.Error(codes.Canceled, err.Error())

	case pilosa.ErrNotImplemented:
		return status.Error(codes.Unimplemented, err.Error())

	case pilosa.ErrAborted:
		return status.Error(codes.Aborted, err.Error())

	case pilosa.ErrClusterDoesNotOwnShard,
		pilosa.ErrResizeNoReplicas,
		pilosa.ErrResizeNotRunning,
		pilosa.ErrNodeNotPrimary,
		pilosa.ErrTooManyWrites,
		pilosa.ErrNodeIDNotExists:
		return status.Error(codes.Internal, err.Error())
	default:
		if _, ok := cause.(pilosa.ConflictError); ok {
			return status.Error(codes.AlreadyExists, err.Error())
		}
	}

	return status.Error(codes.Unknown, err.Error())
}

func (h *GRPCHandler) execSQL(ctx context.Context, queryStr string) (pb.ToRowser, error) {
	h.stats.Count(pilosa.MetricSqlQueries, 1, 1)
	return execSQL(ctx, h.api, h.logger, queryStr)
}

// QuerySQL handles the SQL request and sends RowResponses to the stream.
func (h *GRPCHandler) QuerySQL(req *pb.QuerySQLRequest, stream pb.Pilosa_QuerySQLServer) error {
	start := time.Now()
	results, err := h.execSQL(stream.Context(), req.Sql)
	duration := time.Since(start)
	if err != nil {
		return err
	}

	err = stream.SendHeader(metadata.New(map[string]string{
		"duration": strconv.Itoa(int(duration)),
	}))
	if err != nil {
		return errors.Wrap(err, "sending header")
	}

	err = newDurationRowser(results, duration).ToRows(stream.Send)
	if err != nil {
		return errors.Wrap(err, "streaming result")
	}

	return nil
}

// QuerySQLUnary is a unary-response (non-streaming) version of QuerySQL, returning a TableResponse.
//
// Note regarding QuerySQLUnary and QueryPQLUnary:
// These methods are not ideal, as gRPC responses are payload-length limited to
// 4MB, so in most cases, we would recommend users use the QuerySQL and
// QueryPQL methods, as they stream the response as several small RowResponses.
// The response size limit is configurable on the client size, but we really
// only recommend these methods in the case that the payload is known to be
// quite small (e.g. single counts). These are provided mostly to support gRPC
// Futures, which are used by python-molecula to perform multiple queries
// concurrently. There is additional discussion and historical context here:
// https://github.com/molecula/pilosa/pull/644
func (h *GRPCHandler) QuerySQLUnary(ctx context.Context, req *pb.QuerySQLRequest) (*pb.TableResponse, error) {
	start := time.Now()
	results, err := h.execSQL(ctx, req.Sql)
	if err != nil {
		return nil, err
	}

	var table *pb.TableResponse
	switch results := results.(type) {
	case pb.ToTabler:
		table, err = results.ToTable()
	default:
		table, err = pb.RowsToTable(results, 0)
	}
	if err != nil {
		return nil, err
	}
	duration := time.Since(start)
	table.Duration = int64(duration)
	err = grpc.SendHeader(ctx, metadata.New(map[string]string{
		"duration": strconv.Itoa(int(duration)),
	}))
	if err != nil {
		return nil, errors.Wrap(err, "sending header")
	}

	return table, nil
}

// QueryPQL handles the PQL request and sends RowResponses to the stream.
func (h *GRPCHandler) QueryPQL(req *pb.QueryPQLRequest, stream pb.Pilosa_QueryPQLServer) error {
	query := pilosa.QueryRequest{
		Index: req.Index,
		Query: req.Pql,
	}

	t := time.Now()
	resp, err := h.api.Query(stream.Context(), &query)
	durQuery := time.Since(t)

	// TODO: what about resp.CollumnAttrSets?
	if err != nil {
		return errToStatusError(err)
	} else if len(resp.Results) != 1 {
		// TODO: make a test for this
		return status.Error(codes.InvalidArgument, "QueryPQL handles exactly one query")
	}
	longQueryTime := h.api.LongQueryTime()
	if longQueryTime > 0 && durQuery > longQueryTime {
		h.logger.Printf("GRPC QueryPQL %v %s", durQuery, query.Query)
	}

	rslt := resp.Results[0]
	toRowser, err := ToRowserWrapper(rslt)
	if err != nil {
		return errors.Wrap(err, "wrapping as type ToRowser")
	}

	err = stream.SendHeader(metadata.New(map[string]string{
		"duration": strconv.Itoa(int(durQuery)),
	}))
	if err != nil {
		return errors.Wrap(err, "sending header")
	}

	t = time.Now()
	if err := newDurationRowser(toRowser, durQuery).ToRows(stream.Send); err != nil {
		return errToStatusError(err)
	}
	durFormat := time.Since(t)
	h.stats.Timing(pilosa.MetricGRPCStreamQueryDurationSeconds, durQuery, 0.1)
	h.stats.Timing(pilosa.MetricGRPCStreamFormatDurationSeconds, durFormat, 0.1)
	h.stats.Count(pilosa.MetricPqlQueries, 1, 1)

	return errToStatusError(nil)
}

// QueryPQLUnary is a unary-response (non-streaming) version of QueryPQL, returning a TableResponse.
//
// Note comment above QuerySQLUnary describing the need for the *Unary methods.
func (h *GRPCHandler) QueryPQLUnary(ctx context.Context, req *pb.QueryPQLRequest) (*pb.TableResponse, error) {
	query := pilosa.QueryRequest{
		Index: req.Index,
		Query: req.Pql,
	}

	t := time.Now()
	resp, err := h.api.Query(ctx, &query)
	durQuery := time.Since(t)
	if err != nil {
		return nil, errToStatusError(err)
	} else if len(resp.Results) != 1 {
		return nil, status.Error(codes.InvalidArgument, "QueryPQLUnary handles exactly one query")
	}
	longQueryTime := h.api.LongQueryTime()
	if longQueryTime > 0 && durQuery > longQueryTime {
		h.logger.Printf("GRPC QueryPQLUnary %v %s", durQuery, query.Query)
	}

	rslt := resp.Results[0]
	toTabler, err := ToTablerWrapper(rslt)
	if err != nil {
		return nil, errors.Wrap(err, "wrapping as type ToTabler")
	}

	t = time.Now()
	table, err := toTabler.ToTable()
	if err != nil {
		return nil, errToStatusError(err)
	}
	durFormat := time.Since(t)

	duration := durQuery + durFormat
	table.Duration = int64(duration)
	err = grpc.SendHeader(ctx, metadata.New(map[string]string{
		"duration": strconv.Itoa(int(duration)),
	}))
	if err != nil {
		return nil, errors.Wrap(err, "sending header")
	}

	h.stats.Timing(pilosa.MetricGRPCUnaryQueryDurationSeconds, durQuery, 0.1)
	h.stats.Timing(pilosa.MetricGRPCUnaryFormatDurationSeconds, durFormat, 0.1)
	h.stats.Count(pilosa.MetricPqlQueries, 1, 1)

	return table, errToStatusError(nil)
}

// CreateIndex creates a new Index
func (h *GRPCHandler) CreateIndex(ctx context.Context, req *pb.CreateIndexRequest) (*pb.CreateIndexResponse, error) {
	// Always enable TrackExistence for gRPC-created indexes
	opts := pilosa.IndexOptions{Keys: req.Keys, TrackExistence: true}
	_, err := h.api.CreateIndex(ctx, req.Name, opts)
	if err != nil {
		return nil, errToStatusError(err)
	}
	return &pb.CreateIndexResponse{}, nil
}

// GetIndex returns a single Index given a name
func (h *GRPCHandler) GetIndex(ctx context.Context, req *pb.GetIndexRequest) (*pb.GetIndexResponse, error) {
	schema, err := h.api.Schema(ctx, false)
	if err != nil {
		return nil, errToStatusError(err)
	}

	for _, index := range schema {
		if req.Name == index.Name {
			return &pb.GetIndexResponse{Index: &pb.Index{Name: index.Name}}, nil
		}
	}
	return nil, status.Error(codes.NotFound, fmt.Sprintf("Index with name %s not found", req.Name))
}

// GetIndexes returns a list of all Indexes
func (h *GRPCHandler) GetIndexes(ctx context.Context, req *pb.GetIndexesRequest) (*pb.GetIndexesResponse, error) {
	schema, err := h.api.Schema(ctx, false)
	if err != nil {
		return nil, errToStatusError(err)
	}

	indexes := make([]*pb.Index, len(schema))
	for i, index := range schema {
		indexes[i] = &pb.Index{Name: index.Name}
	}
	return &pb.GetIndexesResponse{Indexes: indexes}, nil
}

// DeleteIndex deletes an Index
func (h *GRPCHandler) DeleteIndex(ctx context.Context, req *pb.DeleteIndexRequest) (*pb.DeleteIndexResponse, error) {
	err := h.api.DeleteIndex(ctx, req.Name)
	if err != nil {
		return nil, errToStatusError(err)
	}
	return &pb.DeleteIndexResponse{}, nil
}

// VDSMGRPCHandler contains methods which handle the various gRPC requests, ported from VDSM.
type VDSMGRPCHandler struct {
	grpcHandler *GRPCHandler
	api         *pilosa.API
	logger      logger.Logger
	stats       stats.StatsClient
}

func NewVDSMGRPCHandler(grpcHandler *GRPCHandler, api *pilosa.API) *VDSMGRPCHandler {
	return &VDSMGRPCHandler{grpcHandler: grpcHandler, api: api, logger: logger.NopLogger, stats: stats.NopStatsClient}
}

func (h *VDSMGRPCHandler) WithLogger(logger logger.Logger) *VDSMGRPCHandler {
	h.logger = logger
	return h
}

func (h *VDSMGRPCHandler) WithStats(stats stats.StatsClient) *VDSMGRPCHandler {
	h.stats = stats
	return h
}

// GetVDSs returns a single VDS given a name
func (h *VDSMGRPCHandler) GetVDS(ctx context.Context, req *vdsm_pb.GetVDSRequest) (*vdsm_pb.GetVDSResponse, error) {
	typedIdOrName := req.GetIdOrName()
	switch idOrName := typedIdOrName.(type) {
	case *vdsm_pb.GetVDSRequest_Id:
		return nil, status.Error(codes.InvalidArgument, "VDS IDs are no longer supported")
	case *vdsm_pb.GetVDSRequest_Name:
		schema, err := h.api.Schema(ctx, false)
		if err != nil {
			return nil, errToStatusError(err)
		}

		for _, index := range schema {
			if idOrName.Name == index.Name {
				return &vdsm_pb.GetVDSResponse{Vds: &vdsm_pb.VDS{Name: index.Name}}, nil
			}
		}
		return nil, status.Error(codes.NotFound, fmt.Sprintf("VDS with name %s not found", idOrName.Name))
	default:
		return nil, status.Error(codes.NotFound, "VDS not found")
	}
}

// GetVDSs returns a list of all VDSs
func (h *VDSMGRPCHandler) GetVDSs(ctx context.Context, req *vdsm_pb.GetVDSsRequest) (*vdsm_pb.GetVDSsResponse, error) {
	schema, err := h.api.Schema(ctx, false)
	if err != nil {
		return nil, errToStatusError(err)
	}

	vdss := make([]*vdsm_pb.VDS, len(schema))
	for i, index := range schema {
		vdss[i] = &vdsm_pb.VDS{Name: index.Name}
	}
	return &vdsm_pb.GetVDSsResponse{Vdss: vdss}, nil
}

// PostVDS creates a new VDS
func (*VDSMGRPCHandler) PostVDS(ctx context.Context, req *vdsm_pb.PostVDSRequest) (*vdsm_pb.PostVDSResponse, error) {
	// Pilosa doesn't implement VDSD files, so this is unimplemented
	return nil, status.Errorf(codes.Unimplemented, "method PostVDS not implemented")
}

// DeleteVDS deletes a VDS
func (h *VDSMGRPCHandler) DeleteVDS(ctx context.Context, req *vdsm_pb.DeleteVDSRequest) (*vdsm_pb.DeleteVDSResponse, error) {
	typedIdOrName := req.GetIdOrName()
	switch idOrName := typedIdOrName.(type) {
	case *vdsm_pb.DeleteVDSRequest_Id:
		return nil, status.Error(codes.InvalidArgument, "VDS IDs are no longer supported")
	case *vdsm_pb.DeleteVDSRequest_Name:
		err := h.api.DeleteIndex(ctx, idOrName.Name)
		if err != nil {
			return nil, errToStatusError(err)
		}
		return &vdsm_pb.DeleteVDSResponse{}, nil
	default:
		return nil, status.Error(codes.NotFound, "")
	}
}

func (h *VDSMGRPCHandler) QuerySQL(req *pb.QuerySQLRequest, srv vdsm_pb.Molecula_QuerySQLServer) error {
	return h.grpcHandler.QuerySQL(req, srv)
}

func (h *VDSMGRPCHandler) QuerySQLUnary(ctx context.Context, req *pb.QuerySQLRequest) (*pb.TableResponse, error) {
	return h.grpcHandler.QuerySQLUnary(ctx, req)
}

func (h *VDSMGRPCHandler) QueryPQL(req *vdsm_pb.QueryPQLRequest, srv vdsm_pb.Molecula_QueryPQLServer) error {
	preq := &pb.QueryPQLRequest{Index: req.Vds, Pql: req.Pql}
	return h.grpcHandler.QueryPQL(preq, srv)
}

func (h *VDSMGRPCHandler) QueryPQLUnary(ctx context.Context, req *vdsm_pb.QueryPQLRequest) (*pb.TableResponse, error) {
	preq := &pb.QueryPQLRequest{Index: req.Vds, Pql: req.Pql}
	return h.grpcHandler.QueryPQLUnary(ctx, preq)
}

func (h *VDSMGRPCHandler) Inspect(req *vdsm_pb.InspectRequest, srv vdsm_pb.Molecula_InspectServer) error {
	preq := &pb.InspectRequest{Index: req.Vds, Columns: req.Records, FilterFields: req.FilterFields, Limit: req.Limit, Offset: req.Offset, Query: req.Query}
	return h.grpcHandler.Inspect(preq, srv)
}

// ResultUint64 is a wrapper around a uint64 result type
// so that we can implement the ToTabler and ToRowser
// interfaces.
type ResultUint64 uint64

// ToTable implements the ToTabler interface.
func (r ResultUint64) ToTable() (*pb.TableResponse, error) {
	return pb.RowsToTable(&r, 1)
}

// ToRows implements the ToRowser interface.
func (r ResultUint64) ToRows(callback func(*pb.RowResponse) error) error {
	return callback(&pb.RowResponse{
		Headers: []*pb.ColumnInfo{{Name: "count", Datatype: "uint64"}},
		Columns: []*pb.ColumnResponse{
			&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(r)}},
		}})
}

// ResultBool is a wrapper around a bool result type
// so that we can implement the ToTabler and ToRowser
// interfaces.
type ResultBool bool

// ToTable implements the ToTabler interface.
func (r ResultBool) ToTable() (*pb.TableResponse, error) {
	return pb.RowsToTable(&r, 1)
}

// ToRows implements the ToRowser interface.
func (r ResultBool) ToRows(callback func(*pb.RowResponse) error) error {
	return callback(&pb.RowResponse{
		Headers: []*pb.ColumnInfo{{Name: "result", Datatype: "bool"}},
		Columns: []*pb.ColumnResponse{
			&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_BoolVal{BoolVal: bool(r)}},
		}})
}

// Normally we wouldn't need this wrapper, but since pilosa returns
// some concrete types for which we can't implement the ToTabler
// interface, we have to check for those here and then wrap them
// with a custom type.
func ToTablerWrapper(result interface{}) (pb.ToTabler, error) {
	toTabler, ok := result.(pb.ToTabler)
	if !ok {
		switch v := result.(type) {
		case []pilosa.GroupCount:
			gc := pilosa.NewGroupCounts("", v...)
			toTabler = gc
		case uint64:
			toTabler = ResultUint64(v)
		case bool:
			toTabler = ResultBool(v)
		default:
			return nil, errors.Errorf("ToTabler interface not implemented by type: %T", result)
		}
	}
	return toTabler, nil
}

// Normally we wouldn't need this wrapper, but since pilosa returns
// some concrete types for which we can't implement the ToRowser
// interface, we have to check for those here and then wrap them
// with a custom type.
func ToRowserWrapper(result interface{}) (pb.ToRowser, error) {
	toRowser, ok := result.(pb.ToRowser)
	if !ok {
		switch v := result.(type) {
		case []pilosa.GroupCount:
			gc := pilosa.NewGroupCounts("", v...)
			toRowser = gc
		case uint64:
			toRowser = ResultUint64(v)
		case bool:
			toRowser = ResultBool(v)
		default:
			return nil, errors.Errorf("ToRowser interface not implemented by type: %T", result)
		}
	}
	return toRowser, nil
}

// durationRowser is a wrapper for pb.ToRowser that can be used to inject a
// duration value into the first record in a stream
type durationRowser struct {
	pb.ToRowser
	duration time.Duration
	once     sync.Once
}

func (r *durationRowser) ToRows(callback func(*pb.RowResponse) error) error {
	cb := func(rr *pb.RowResponse) error {
		r.once.Do(func() {
			rr.Duration = int64(r.duration)
		})
		return callback(rr)
	}
	return r.ToRowser.ToRows(cb)
}

func newDurationRowser(orig pb.ToRowser, duration time.Duration) pb.ToRowser {
	return &durationRowser{
		ToRowser: orig,
		duration: duration,
	}
}

// Inspect handles the inspect request and sends an InspectResponse to the stream.
func (h *GRPCHandler) Inspect(req *pb.InspectRequest, stream pb.Pilosa_InspectServer) error {
	const defaultLimit = 100000

	h.inspectDeprecated.Do(func() {
		h.logger.Printf("DEPRECATED: Inspect is deprecated, please use Extract() instead.")
	})

	index, err := h.api.Index(stream.Context(), req.Index)
	if err != nil {
		return errToStatusError(err)
	}

	// It is okay to pass a nil Tx to field.StringValue(). It will lazily create it.
	var tx pilosa.Tx

	var fields []*pilosa.Field
	for _, field := range index.Fields() {
		// exclude internal fields (starting with "_")
		if strings.HasPrefix(field.Name(), "_") {
			continue
		}
		if len(req.FilterFields) > 0 {
			for _, filter := range req.FilterFields {
				if filter == field.Name() {
					fields = append(fields, field)
					break
				}

			}
		} else {
			fields = append(fields, field)
		}
	}

	if req.Query != "" {
		// Execute the query and use it to select columns.
		if req.Columns != nil && req.Columns.Type != nil {
			l := 0
			switch v := req.Columns.Type.(type) {
			case *pb.IdsOrKeys_Ids:
				l = len(v.Ids.Vals)
			case *pb.IdsOrKeys_Keys:
				l = len(v.Keys.Vals)
			}
			if l > 0 {
				return errors.New("found a list of columns in a query-based inspect call")
			}
		}
		query := pilosa.QueryRequest{
			Index: req.Index,
			Query: req.Query,
		}
		resp, err := h.api.Query(stream.Context(), &query)
		if err != nil {
			return errors.Wrapf(err, "querying for columns with %q", req.Query)
		}
		if len(resp.Results) != 1 {
			return errors.Errorf("expected 1 result for inspect query; got %d from %q", len(resp.Results), req.Query)
		}
		row, ok := resp.Results[0].(*pilosa.Row)
		if !ok {
			return errors.Errorf("incorrect query result type %T for query %q", resp.Results[0], req.Query)
		}
		if len(row.Keys) > 0 {
			req.Columns = &pb.IdsOrKeys{
				Type: &pb.IdsOrKeys_Keys{
					Keys: &pb.StringArray{Vals: row.Keys},
				},
			}
		} else {
			req.Columns = &pb.IdsOrKeys{
				Type: &pb.IdsOrKeys_Ids{
					Ids: &pb.Uint64Array{Vals: row.Columns()},
				},
			}
		}
		if !row.Any() {
			// No columns were matched.
			return nil
		}
	}

	limit := req.Limit
	if limit == 0 {
		limit = defaultLimit
	}
	offset := req.Offset

	if !index.Keys() {
		var cols []uint64
		if req.Columns != nil {
			ints, ok := req.Columns.Type.(*pb.IdsOrKeys_Ids)
			if !ok {
				return errors.New("invalid int columns")
			}
			cols = ints.Ids.Vals
		}
		ci := []*pb.ColumnInfo{
			{Name: "_id", Datatype: "uint64"},
		}
		for _, field := range fields {
			fdt := fieldDataType(field)
			if err != nil {
				return errors.Wrapf(err, "field %s", field.Name())
			}
			ci = append(ci, &pb.ColumnInfo{Name: field.Name(), Datatype: fdt})
		}

		// If Columns is empty, then get the _exists list (via All()),
		// from the index and loop over that instead.
		if len(cols) > 0 {
			// Apply limit/offset to the provided columns.
			if int(offset) >= len(cols) {
				return nil
			}
			end := limit + offset
			if int(end) > len(cols) {
				end = uint64(len(cols))
			}
			cols = cols[offset:end]
		} else {
			// Prevent getting too many records by forcing a limit.
			pql := fmt.Sprintf("All(limit=%d, offset=%d)", limit, offset)
			query := pilosa.QueryRequest{
				Index: req.Index,
				Query: pql,
			}
			resp, err := h.api.Query(stream.Context(), &query)
			if err != nil {
				return errors.Wrapf(err, "querying for all: %s", pql)
			}

			ids, ok := resp.Results[0].(*pilosa.Row)
			if !ok {
				return errors.Wrap(err, "getting results as a row")
			}

			limitedCols := ids.Columns()
			if len(limitedCols) == 0 {
				// If cols is still empty after the limit/offset, then
				// return with no results.
				return nil
			}
			cols = limitedCols
		}

		for _, col := range cols {
			rowResp := &pb.RowResponse{
				Headers: ci,
				Columns: []*pb.ColumnResponse{
					&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: col}},
				},
			}
			ci = nil // only include headers with the first row

			colAdded := 0
			for _, field := range fields {
				// TODO: handle `time` fields
				switch field.Type() {
				case "set":
					pql := fmt.Sprintf("Rows(%s, column=%d)", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(stream.Context(), &query)
					if err != nil {
						return errors.Wrapf(err, "querying rows for set: %s", pql)
					}

					if len(resp.Results) > 0 {
						ids, ok := resp.Results[0].(pilosa.RowIdentifiers)
						if !ok {
							return errors.Wrap(err, "getting row identifiers")
						}

						if len(ids.Keys) > 0 {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringArrayVal{StringArrayVal: &pb.StringArray{Vals: ids.Keys}}})
							colAdded++
						} else if len(ids.Rows) > 0 {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64ArrayVal{Uint64ArrayVal: &pb.Uint64Array{Vals: ids.Rows}}})
							colAdded++
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				case "mutex":
					pql := fmt.Sprintf("Rows(%s, column=%d)", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(stream.Context(), &query)
					if err != nil {
						return errors.Wrap(err, "querying rows for mutex")
					}

					if len(resp.Results) > 0 {
						ids, ok := resp.Results[0].(pilosa.RowIdentifiers)
						if !ok {
							return errors.Wrap(err, "getting row identifiers")
						}

						if len(ids.Keys) == 1 {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: ids.Keys[0]}})
							colAdded++
						} else if len(ids.Rows) == 1 {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: ids.Rows[0]}})
							colAdded++
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				case "int":
					if field.Keys() {
						var value string
						var exists bool
						var err error
						if fi := field.ForeignIndex(); fi != "" {
							// Get the value from the int field.
							pql := fmt.Sprintf("FieldValue(field=%s, column=%d)", field.Name(), col)
							query := pilosa.QueryRequest{
								Index: req.Index,
								Query: pql,
							}
							resp, err := h.api.Query(stream.Context(), &query)
							if err != nil {
								return errors.Wrap(err, "getting int field value for column")
							}

							if len(resp.Results) > 0 {
								valCount, ok := resp.Results[0].(pilosa.ValCount)
								if ok && valCount.Count == 1 {
									vals, err := h.api.TranslateIndexIDs(stream.Context(), fi, []uint64{uint64(valCount.Val)})
									if err != nil {
										return errors.Wrap(err, "getting keys for ids")
									}
									if len(vals) > 0 && vals[0] != "" {
										value = vals[0]
										exists = true
									}
								}
							}
						} else {
							value, exists, err = field.StringValue(tx, col)
							if err != nil {
								return errors.Wrap(err, "getting string field value for column")
							}
						}
						if exists {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: value}})
							colAdded++
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						pql := fmt.Sprintf("FieldValue(field=%s, column=%d)", field.Name(), col)
						query := pilosa.QueryRequest{
							Index: req.Index,
							Query: pql,
						}
						resp, err := h.api.Query(stream.Context(), &query)
						if err != nil {
							return errors.Wrap(err, "getting int field value for column")
						}

						if len(resp.Results) > 0 {
							valCount, ok := resp.Results[0].(pilosa.ValCount)
							if ok && valCount.Count == 1 {
								rowResp.Columns = append(rowResp.Columns,
									&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: valCount.Val}})
								colAdded++
							} else {
								rowResp.Columns = append(rowResp.Columns,
									&pb.ColumnResponse{ColumnVal: nil})
							}
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					}

				case "decimal":
					pql := fmt.Sprintf("FieldValue(field=%s, column=%d)", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(stream.Context(), &query)
					if err != nil {
						return errors.Wrap(err, "getting decimal field value for column")
					}

					if len(resp.Results) > 0 {
						valCount, ok := resp.Results[0].(pilosa.ValCount)
						if ok && valCount.Count == 1 {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_DecimalVal{DecimalVal: &pb.Decimal{Value: valCount.DecimalVal.Value, Scale: valCount.DecimalVal.Scale}}})
							colAdded++
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				case "bool":
					pql := fmt.Sprintf("Rows(%s, column=%d)", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(stream.Context(), &query)
					if err != nil {
						return errors.Wrap(err, "querying rows for bool")
					}

					if len(resp.Results) > 0 {
						ids, ok := resp.Results[0].(pilosa.RowIdentifiers)
						if !ok {
							return errors.Wrap(err, "getting row identifiers")
						}

						if len(ids.Rows) == 1 {
							var bval bool
							if ids.Rows[0] == 1 {
								bval = true
							}
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_BoolVal{BoolVal: bval}})
							colAdded++
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				case "time":
					rowResp.Columns = append(rowResp.Columns,
						&pb.ColumnResponse{ColumnVal: nil})
				}
			}

			// For SQL queries like:
			// SELECT * FROM t WHERE _id=garbageID;
			// we don't want to return any rows.
			// So, check here if we added any columns.
			//
			// Because we don't have keys to translate
			// and _id is an artificial field that's why for query:
			// SELECT _id FROM t WHERE _id=existing-id;
			// we return an empty result.
			//
			// TODO(kuba--): We need to find a way to check here if
			// existing-id is not a garbage.
			//
			// A query which will work here is 'SELECT *' or any query with more columns
			// than just _id.
			if colAdded > 0 {
				if err := stream.Send(rowResp); err != nil {
					return errors.Wrap(err, "sending response to stream")
				}
			}
		}

	} else {
		var cols []string

		if req.Columns != nil {
			switch keys := req.Columns.Type.(type) {
			case *pb.IdsOrKeys_Ids:
				// The default behavior (in api/client/grpc.go) is to
				// send an empty set of Ids even if the index supports
				// keys, so in that case we just need to ignore it.
			case *pb.IdsOrKeys_Keys:
				cols = keys.Keys.Vals
			default:
				return errToStatusError(errors.New("invalid key columns"))
			}
		}

		forceSend := false
		ci := []*pb.ColumnInfo{
			{Name: "_id", Datatype: "string"},
		}
		for _, field := range fields {
			fdt := fieldDataType(field)
			if err != nil {
				return errors.Wrapf(err, "field %s", field.Name())
			}
			ci = append(ci, &pb.ColumnInfo{Name: field.Name(), Datatype: fdt})
		}

		// If Columns is empty, then get the _exists list (via All()),
		// from the index and loop over that instead.
		if len(cols) > 0 {
			// Apply limit/offset to the provided columns.
			if int(offset) >= len(cols) {
				return nil
			}
			end := limit + offset
			if int(end) > len(cols) {
				end = uint64(len(cols))
			}
			cols = cols[offset:end]
			if len(cols) == 1 {
				if id, err := h.api.TranslateIndexKey(stream.Context(), index.Name(), cols[0], false); id != 0 && err == nil {
					forceSend = true
				}
			}
		} else {
			// Prevent getting too many records by forcing a limit.
			pql := fmt.Sprintf("All(limit=%d, offset=%d)", limit, offset)
			query := pilosa.QueryRequest{
				Index: req.Index,
				Query: pql,
			}
			resp, err := h.api.Query(stream.Context(), &query)
			if err != nil {
				return errors.Wrapf(err, "querying for all: %s", pql)
			}

			if len(resp.Results) > 0 {
				ids, ok := resp.Results[0].(*pilosa.Row)
				if !ok {
					return errors.Wrap(err, "getting results as a row")
				}

				limitedCols := ids.Keys
				if len(limitedCols) == 0 {
					// If cols is still empty after the limit/offset, then
					// return with no results.
					return nil
				}
				cols = limitedCols
			} else {
				return errors.Errorf("expected 1 result for inspect query; got %d from %q", len(resp.Results), req.Query)
			}
		}

		for _, col := range cols {
			rowResp := &pb.RowResponse{
				Headers: ci,
				Columns: []*pb.ColumnResponse{
					&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: col}},
				},
			}
			ci = nil // only include headers with the first row

			colAdded := 0
			for _, field := range fields {
				// TODO: handle `time` fields
				switch field.Type() {
				case "set":
					pql := fmt.Sprintf("Rows(%s, column=\"%s\")", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(stream.Context(), &query)
					if err != nil {
						return errors.Wrap(err, "querying set rows(keys)")
					}

					if len(resp.Results) > 0 {
						ids, ok := resp.Results[0].(pilosa.RowIdentifiers)
						if !ok {
							return errors.Wrap(err, "getting row identifiers")
						}

						if len(ids.Keys) > 0 {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringArrayVal{StringArrayVal: &pb.StringArray{Vals: ids.Keys}}})
							colAdded++
						} else if len(ids.Rows) > 0 {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64ArrayVal{Uint64ArrayVal: &pb.Uint64Array{Vals: ids.Rows}}})
							colAdded++
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				case "mutex":
					pql := fmt.Sprintf("Rows(%s, column=\"%s\")", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(stream.Context(), &query)
					if err != nil {
						return errors.Wrap(err, "querying mutex rows(keys)")
					}

					if len(resp.Results) > 0 {
						ids, ok := resp.Results[0].(pilosa.RowIdentifiers)
						if !ok {
							return errors.Wrap(err, "getting row identifiers")
						}

						if len(ids.Keys) == 1 {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: ids.Keys[0]}})
							colAdded++
						} else if len(ids.Rows) == 1 {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: ids.Rows[0]}})
							colAdded++
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				case "int":
					// Translate column key.
					id, err := h.api.TranslateIndexKey(stream.Context(), index.Name(), col, false)
					if err != nil {
						return errors.Wrap(err, "translating column key")
					}

					if field.Keys() {
						var value string
						var exists bool
						var err error
						if fi := field.ForeignIndex(); fi != "" {
							// Get the value from the int field.
							pql := fmt.Sprintf("FieldValue(field=%s, column=%d)", field.Name(), id)
							query := pilosa.QueryRequest{
								Index: req.Index,
								Query: pql,
							}
							resp, err := h.api.Query(stream.Context(), &query)
							if err != nil {
								return errors.Wrap(err, "getting int field value for column")
							}

							if len(resp.Results) > 0 {
								valCount, ok := resp.Results[0].(pilosa.ValCount)
								if ok && valCount.Count == 1 {
									vals, err := h.api.TranslateIndexIDs(stream.Context(), fi, []uint64{uint64(valCount.Val)})
									if err != nil {
										return errors.Wrap(err, "getting keys for ids")
									}
									if len(vals) > 0 && vals[0] != "" {
										value = vals[0]
										exists = true
									}
								} else {
									rowResp.Columns = append(rowResp.Columns,
										&pb.ColumnResponse{ColumnVal: nil})
								}
							} else {
								rowResp.Columns = append(rowResp.Columns,
									&pb.ColumnResponse{ColumnVal: nil})
							}
						} else {
							value, exists, err = field.StringValue(tx, id)
							if err != nil {
								return errors.Wrap(err, "getting string field value for column")
							}
						}
						if exists {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: value}})
							colAdded++
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						pql := fmt.Sprintf("FieldValue(field=%s, column=%d)", field.Name(), id)
						query := pilosa.QueryRequest{
							Index: req.Index,
							Query: pql,
						}
						resp, err := h.api.Query(stream.Context(), &query)
						if err != nil {
							return errors.Wrap(err, "getting int field value for column")
						}

						if len(resp.Results) > 0 {
							valCount, ok := resp.Results[0].(pilosa.ValCount)
							if ok && valCount.Count == 1 {
								rowResp.Columns = append(rowResp.Columns,
									&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: valCount.Val}})
								colAdded++
							} else {
								rowResp.Columns = append(rowResp.Columns,
									&pb.ColumnResponse{ColumnVal: nil})
							}
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					}

				case "decimal":
					pql := fmt.Sprintf("FieldValue(field=%s, column='%s')", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(stream.Context(), &query)
					if err != nil {
						return errors.Wrap(err, "getting decimal field value for column")
					}

					if len(resp.Results) > 0 {
						valCount, ok := resp.Results[0].(pilosa.ValCount)
						if ok && valCount.Count == 1 {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_DecimalVal{DecimalVal: &pb.Decimal{Value: valCount.DecimalVal.Value, Scale: valCount.DecimalVal.Scale}}})
							colAdded++
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				case "bool":
					pql := fmt.Sprintf("Rows(%s, column=\"%s\")", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(stream.Context(), &query)
					if err != nil {
						return errors.Wrap(err, "querying bool rows(keys)")
					}

					if len(resp.Results) > 0 {
						ids, ok := resp.Results[0].(pilosa.RowIdentifiers)
						if !ok {
							return errors.Wrap(err, "getting row identifiers")
						}

						if len(ids.Rows) == 1 {
							var bval bool
							if ids.Rows[0] == 1 {
								bval = true
							}
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_BoolVal{BoolVal: bval}})
							colAdded++
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				default:
					rowResp.Columns = append(rowResp.Columns,
						&pb.ColumnResponse{ColumnVal: nil})
				}
			}

			// For SQL queries like:
			// SELECT _id FROM parent WHERE _id="garbage";
			// we get here without any real columns and fields, and we did not
			// translate any keys. That's why we don't want to send anything back
			// and return fake response like:
			//
			// _id
			// -------
			// <nil>
			// (1 row)
			if colAdded > 0 || forceSend {
				if err := stream.Send(rowResp); err != nil {
					return errors.Wrap(err, "sending response to stream")
				}
			}
		}

	}
	return nil
}

// fieldDataType returns a useful data type (string,
// uint64, bool, etc.) based on the Pilosa field type.
// DO NOT USE THIS IN FUTURE CODE.
// This remains only for backwards-compatability within inspect.
// It does not produce sane results in all scenarios.
func fieldDataType(f *pilosa.Field) string {
	switch f.Type() {
	case "set":
		if f.Keys() {
			return "[]string"
		}
		return "[]uint64"
	case "mutex":
		if f.Keys() {
			return "string"
		}
		return "uint64"
	case "int":
		if f.Keys() {
			return "string"
		}
		return "int64"
	case "decimal":
		return "decimal"
	case "bool":
		return "bool"
	case "time":
		return "int64" // TODO: this is a placeholder
	default:
		panic(fmt.Sprintf("unimplemented fieldDataType: %s", f.Type()))
	}
}

type grpcServer struct {
	api        *pilosa.API
	grpcServer *grpc.Server
	ln         net.Listener
	tlsConfig  *tls.Config

	logger logger.Logger
	stats  stats.StatsClient
}

type grpcServerOption func(s *grpcServer) error

func OptGRPCServerAPI(api *pilosa.API) grpcServerOption {
	return func(s *grpcServer) error {
		s.api = api
		return nil
	}
}

func OptGRPCServerListener(ln net.Listener) grpcServerOption {
	return func(s *grpcServer) error {
		s.ln = ln
		return nil
	}
}

func OptGRPCServerTLSConfig(tlsConfig *tls.Config) grpcServerOption {
	return func(s *grpcServer) error {
		s.tlsConfig = tlsConfig
		return nil
	}
}

func OptGRPCServerLogger(logger logger.Logger) grpcServerOption {
	return func(s *grpcServer) error {
		s.logger = logger
		return nil
	}
}

func OptGRPCServerStats(stats stats.StatsClient) grpcServerOption {
	return func(s *grpcServer) error {
		s.stats = stats
		return nil
	}
}

func (s *grpcServer) Serve() error {
	s.logger.Printf("enabled grpc listening on %s", s.ln.Addr())

	// and start...
	if err := s.grpcServer.Serve(s.ln); err != nil {
		return errors.Wrap(err, "starting grpc server")
	}
	return nil
}

func (s *grpcServer) middleware(origins []string) func(http.Handler) http.Handler {
	httpOriginFunc := grpcweb.WithOriginFunc(func(origin string) bool {
		for _, x := range origins {
			if origin == x {
				return true
			}
		}
		return false
	})

	wrappedGrpc := grpcweb.WrapServer(s.grpcServer, httpOriginFunc)

	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if wrappedGrpc.IsGrpcWebRequest(r) || wrappedGrpc.IsAcceptableGrpcCorsRequest(r) {
				wrappedGrpc.ServeHTTP(w, r)
			} else {
				h.ServeHTTP(w, r)
			}
		})
	}
}

// Stop stops the GRPC server. There's no error because the underlying GRPC
// stuff doesn't report an error.
func (s *grpcServer) Stop() {
	s.grpcServer.Stop()
}

func NewGRPCServer(opts ...grpcServerOption) (*grpcServer, error) {
	server := &grpcServer{
		logger: logger.NopLogger,
	}
	for _, opt := range opts {
		err := opt(server)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}

	gopts := make([]grpc.ServerOption, 0)
	if server.tlsConfig != nil {
		creds := credentials.NewTLS(server.tlsConfig)
		gopts = append(gopts, grpc.Creds(creds))
	}

	// create grpc server
	server.grpcServer = grpc.NewServer(gopts...)
	grpcHandler := NewGRPCHandler(server.api).WithLogger(server.logger).WithStats(server.stats)
	pb.RegisterPilosaServer(server.grpcServer, grpcHandler)
	vdsm_pb.RegisterMoleculaServer(server.grpcServer, NewVDSMGRPCHandler(grpcHandler, server.api).WithLogger(server.logger).WithStats(server.stats))

	// register the server so its services are available to grpc_cli and others
	reflection.Register(server.grpcServer)

	return server, nil
}
