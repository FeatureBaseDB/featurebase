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
	"strings"
	"sync"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/logger"
	pb "github.com/pilosa/pilosa/v2/proto"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
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
	switch errors.Cause(err) {
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
		pilosa.ErrNodeNotCoordinator,
		pilosa.ErrTooManyWrites,
		pilosa.ErrNodeIDNotExists:
		return status.Error(codes.Internal, err.Error())
	}

	return status.Error(codes.Unknown, err.Error())
}

// GetVDSs returns a single VDS given a name
func (h *GRPCHandler) GetVDS(ctx context.Context, req *pb.GetVDSRequest) (*pb.GetVDSResponse, error) {
	// TODO: Return all schema information associated with the VDS.
	// It's obviously not very useful to return the same data as given.
	schema := h.api.Schema(ctx)
	for _, index := range schema {
		if req.Name == index.Name {
			return &pb.GetVDSResponse{Vds: &pb.VDS{Name: index.Name}}, nil
		}
	}
	return nil, status.Error(codes.NotFound, fmt.Sprintf("VDS with name %s not found", req.Name))
}

// GetVDSs returns a list of all VDSs
func (h *GRPCHandler) GetVDSs(ctx context.Context, req *pb.GetVDSsRequest) (*pb.GetVDSsResponse, error) {
	schema := h.api.Schema(ctx)
	vdss := make([]*pb.VDS, len(schema))
	for i, index := range schema {
		vdss[i] = &pb.VDS{Name: index.Name}
	}
	return &pb.GetVDSsResponse{Vdss: vdss}, nil
}

// PostVDS creates a new VDS
func (h *GRPCHandler) PostVDS(ctx context.Context, req *pb.PostVDSRequest) (*pb.PostVDSResponse, error) {
	opts := pilosa.IndexOptions{Keys: req.Keys, TrackExistence: req.TrackExistence}
	_, err := h.api.CreateIndex(ctx, req.Name, opts)
	if err != nil {
		return nil, errToStatusError(err)
	}
	return &pb.PostVDSResponse{}, nil
}

// DeleteVDS deletes a VDS
func (h *GRPCHandler) DeleteVDS(ctx context.Context, req *pb.DeleteVDSRequest) (*pb.DeleteVDSResponse, error) {
	err := h.api.DeleteIndex(ctx, req.Name)
	if err != nil {
		return nil, errToStatusError(err)
	}
	return &pb.DeleteVDSResponse{}, nil
}

func (h *GRPCHandler) execSQL(ctx context.Context, queryStr string) (pb.ToRowser, error) {
	h.stats.Count(pilosa.MetricSqlQueries, 1, 1)
	return execSQL(ctx, h.api, h.logger, queryStr)
}

// QuerySQL handles the SQL request and sends RowResponses to the stream.
func (h *GRPCHandler) QuerySQL(req *pb.QuerySQLRequest, stream pb.Pilosa_QuerySQLServer) error {
	results, err := h.execSQL(stream.Context(), req.Sql)
	if err != nil {
		return err
	}

	err = results.ToRows(stream.Send)
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
	results, err := h.execSQL(ctx, req.Sql)
	if err != nil {
		return nil, err
	}
	if results, ok := results.(pb.ToTabler); ok {
		return results.ToTable()
	}
	return pb.RowsToTable(results, 0)
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

	t = time.Now()
	if err := toRowser.ToRows(stream.Send); err != nil {
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

	h.stats.Timing(pilosa.MetricGRPCUnaryQueryDurationSeconds, durQuery, 0.1)
	h.stats.Timing(pilosa.MetricGRPCUnaryFormatDurationSeconds, durFormat, 0.1)
	h.stats.Count(pilosa.MetricPqlQueries, 1, 1)

	return table, errToStatusError(nil)
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
			toTabler = pilosa.GroupCounts(v)
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
			toRowser = pilosa.GroupCounts(v)
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
						}
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
						}
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
								}
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
					}

				case "time":
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
	mu         sync.Mutex
	grpcServer *grpc.Server
	ln         net.Listener

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

func (s *grpcServer) Serve(tlsConfig *tls.Config) error {
	s.logger.Printf("enabled grpc listening on %s", s.ln.Addr())

	opts := make([]grpc.ServerOption, 0)
	if tlsConfig != nil {
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	// create grpc server
	s.mu.Lock()
	s.grpcServer = grpc.NewServer(opts...)
	pb.RegisterPilosaServer(s.grpcServer, NewGRPCHandler(s.api).WithLogger(s.logger).WithStats(s.stats))

	// register the server so its services are available to grpc_cli and others
	reflection.Register(s.grpcServer)
	s.mu.Unlock()

	// and start...
	if err := s.grpcServer.Serve(s.ln); err != nil {
		return errors.Wrap(err, "starting grpc server")
	}
	return nil
}

// Stop stops the GRPC server. There's no error because the underlying GRPC
// stuff doesn't report an error.
func (s *grpcServer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
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
	return server, nil
}
