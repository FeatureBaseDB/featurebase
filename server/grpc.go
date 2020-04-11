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

// grpcHandler contains methods which handle the various gRPC requests.
type grpcHandler struct {
	api *pilosa.API

	logger logger.Logger

	stats stats.StatsClient
}

// errorToStatusError appends an appropriate grpc status code
// to the error (returning it as a status.Error). It is
// assumed that the input err is non-nil.
func errToStatusError(err error) error {
	// Check error string.
	switch errors.Cause(err) {
	case pilosa.ErrIndexNotFound, pilosa.ErrFieldNotFound:
		return status.Error(codes.NotFound, err.Error())
	}
	// Check error type.
	switch errors.Cause(err).(type) {
	case pilosa.NotFoundError:
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Unknown, err.Error())
}

// QueryPQL handles the PQL request and sends RowResponses to the stream.
func (h grpcHandler) QueryPQL(req *pb.QueryPQLRequest, stream pb.Pilosa_QueryPQLServer) error {
	query := pilosa.QueryRequest{
		Index: req.Index,
		Query: req.Pql,
	}

	t := time.Now()
	resp, err := h.api.Query(context.Background(), &query)
	durQuery := time.Since(t)
	if err != nil {
		return errToStatusError(err)
	}
	longQueryTime := h.api.LongQueryTime()
	if longQueryTime > 0 && durQuery > longQueryTime {
		h.logger.Printf("GRPC QueryPQL %v %s", durQuery, query.Query)
	}

	t = time.Now()
	for row := range makeRows(resp, h.logger) {
		err = stream.Send(row)
		if err != nil {
			return errToStatusError(err)
		}
	}
	durFormat := time.Since(t)
	h.stats.Timing(pilosa.MetricGRPCStreamQueryDurationSeconds, durQuery, 0.1)
	h.stats.Timing(pilosa.MetricGRPCStreamFormatDurationSeconds, durFormat, 0.1)

	return nil
}

// QueryPQLUnary is a unary-response (non-streaming) version of QueryPQL, returning a TableResponse.
func (h grpcHandler) QueryPQLUnary(ctx context.Context, req *pb.QueryPQLRequest) (*pb.TableResponse, error) {
	query := pilosa.QueryRequest{
		Index: req.Index,
		Query: req.Pql,
	}

	t := time.Now()
	resp, err := h.api.Query(context.Background(), &query)
	durQuery := time.Since(t)
	if err != nil {
		return nil, errToStatusError(err)
	}
	longQueryTime := h.api.LongQueryTime()
	if longQueryTime > 0 && durQuery > longQueryTime {
		h.logger.Printf("GRPC QueryPQLUnary %v %s", durQuery, query.Query)
	}

	t = time.Now()
	response := &pb.TableResponse{
		Rows: make([]*pb.Row, 0),
	}
	for row := range makeRows(resp, h.logger) {
		if len(row.Headers) != 0 {
			response.Headers = row.Headers
		}
		response.Rows = append(response.Rows, &pb.Row{Columns: row.Columns})
	}
	durFormat := time.Since(t)
	h.stats.Timing(pilosa.MetricGRPCUnaryQueryDurationSeconds, durQuery, 0.1)
	h.stats.Timing(pilosa.MetricGRPCUnaryFormatDurationSeconds, durFormat, 0.1)

	return response, nil
}

// fieldDataType returns a useful data type (string,
// uint64, bool, etc.) based on the Pilosa field type.
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

// Inspect handles the inspect request and sends an InspectResponse to the stream.
func (h grpcHandler) Inspect(req *pb.InspectRequest, stream pb.Pilosa_InspectServer) error {
	const defaultLimit = 100000

	index, err := h.api.Index(context.Background(), req.Index)
	if err != nil {
		return errToStatusError(err)
	}

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

	limit := req.Limit
	if limit == 0 {
		limit = defaultLimit
	}
	offset := req.Offset

	if !index.Keys() {
		ints, ok := req.Columns.Type.(*pb.IdsOrKeys_Ids)
		if !ok {
			return errors.New("invalid int columns")
		}
		ci := []*pb.ColumnInfo{
			{Name: "_id", Datatype: "uint64"},
		}
		for _, field := range fields {
			ci = append(ci, &pb.ColumnInfo{Name: field.Name(), Datatype: fieldDataType(field)})
		}

		// If Columns is empty, then get the _exists list (via All()),
		// from the index and loop over that instead.
		cols := ints.Ids.Vals
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
			resp, err := h.api.Query(context.Background(), &query)
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

			for _, field := range fields {
				// TODO: handle `time` fields
				switch field.Type() {
				case "set":
					pql := fmt.Sprintf("Rows(%s, column=%d)", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(context.Background(), &query)
					if err != nil {
						return errors.Wrapf(err, "querying rows for set: %s", pql)
					}

					ids, ok := resp.Results[0].(pilosa.RowIdentifiers)
					if !ok {
						return errors.Wrap(err, "getting row identifiers")
					}

					if len(ids.Keys) > 0 {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringArrayVal{StringArrayVal: &pb.StringArray{Vals: ids.Keys}}})
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64ArrayVal{Uint64ArrayVal: &pb.Uint64Array{Vals: ids.Rows}}})
					}

				case "mutex":
					pql := fmt.Sprintf("Rows(%s, column=%d)", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(context.Background(), &query)
					if err != nil {
						return errors.Wrap(err, "querying rows for mutex")
					}

					ids, ok := resp.Results[0].(pilosa.RowIdentifiers)
					if !ok {
						return errors.Wrap(err, "getting row identifiers")
					}

					if len(ids.Keys) == 1 {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: ids.Keys[0]}})
					} else if len(ids.Rows) == 1 {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: ids.Rows[0]}})
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
							intVal, ok, err := field.Value(col)
							if err != nil {
								return errors.Wrap(err, "getting int value")
							} else if ok {
								vals, err := h.api.TranslateIndexIDs(context.Background(), fi, []uint64{uint64(intVal)})
								if err != nil {
									return errors.Wrap(err, "getting keys for ids")
								}
								if len(vals) > 0 && vals[0] != "" {
									value = vals[0]
									exists = true
								}
							}
						} else {
							value, exists, err = field.StringValue(col)
							if err != nil {
								return errors.Wrap(err, "getting string field value for column")
							}
						}
						if exists {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: value}})
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						value, exists, err := field.Value(col)
						if err != nil {
							return errors.Wrap(err, "getting int field value for column")
						} else if exists {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: value}})
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					}

				case "decimal":
					dec, exists, err := field.DecimalValue(col)
					if err != nil {
						return errors.Wrap(err, "getting decimal field value for column")
					} else if exists {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_DecimalVal{DecimalVal: &pb.Decimal{Value: dec.Value, Scale: dec.Scale}}})
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
					resp, err := h.api.Query(context.Background(), &query)
					if err != nil {
						return errors.Wrap(err, "querying rows for bool")
					}

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
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				case "time":
					rowResp.Columns = append(rowResp.Columns,
						&pb.ColumnResponse{ColumnVal: nil})
				}
			}

			if err := stream.Send(rowResp); err != nil {
				return errors.Wrap(err, "sending response to stream")
			}
		}

	} else {
		var cols []string

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

		ci := []*pb.ColumnInfo{
			{Name: "_id", Datatype: "string"},
		}
		for _, field := range fields {
			ci = append(ci, &pb.ColumnInfo{Name: field.Name(), Datatype: fieldDataType(field)})
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
			resp, err := h.api.Query(context.Background(), &query)
			if err != nil {
				return errors.Wrapf(err, "querying for all: %s", pql)
			}

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

		for _, col := range cols {
			rowResp := &pb.RowResponse{
				Headers: ci,
				Columns: []*pb.ColumnResponse{
					&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: col}},
				},
			}
			ci = nil // only include headers with the first row

			for _, field := range fields {
				// TODO: handle `time` fields
				switch field.Type() {
				case "set":
					pql := fmt.Sprintf("Rows(%s, column=\"%s\")", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(context.Background(), &query)
					if err != nil {
						return errors.Wrap(err, "querying set rows(keys)")
					}

					ids, ok := resp.Results[0].(pilosa.RowIdentifiers)
					if !ok {
						return errors.Wrap(err, "getting row identifiers")
					}

					if len(ids.Keys) > 0 {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringArrayVal{StringArrayVal: &pb.StringArray{Vals: ids.Keys}}})
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64ArrayVal{Uint64ArrayVal: &pb.Uint64Array{Vals: ids.Rows}}})
					}

				case "mutex":
					pql := fmt.Sprintf("Rows(%s, column=\"%s\")", field.Name(), col)
					query := pilosa.QueryRequest{
						Index: req.Index,
						Query: pql,
					}
					resp, err := h.api.Query(context.Background(), &query)
					if err != nil {
						return errors.Wrap(err, "querying mutex rows(keys)")
					}

					ids, ok := resp.Results[0].(pilosa.RowIdentifiers)
					if !ok {
						return errors.Wrap(err, "getting row identifiers")
					}

					if len(ids.Keys) == 1 {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: ids.Keys[0]}})
					} else if len(ids.Rows) == 1 {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: ids.Rows[0]}})
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				case "int":
					// Translate column key.
					id, err := h.api.TranslateIndexKey(context.Background(), index.Name(), col)
					if err != nil {
						return errors.Wrap(err, "translating column key")
					}

					if field.Keys() {
						var value string
						var exists bool
						var err error
						if fi := field.ForeignIndex(); fi != "" {
							// Get the value from the int field.
							intVal, ok, err := field.Value(id)
							if err != nil {
								return errors.Wrap(err, "getting int value")
							} else if ok {
								vals, err := h.api.TranslateIndexIDs(context.Background(), fi, []uint64{uint64(intVal)})
								if err != nil {
									return errors.Wrap(err, "getting keys for ids")
								}
								if len(vals) > 0 && vals[0] != "" {
									value = vals[0]
									exists = true
								}
							}
						} else {
							value, exists, err = field.StringValue(id)
							if err != nil {
								return errors.Wrap(err, "getting string field value for column")
							}
						}
						if exists {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: value}})
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					} else {
						value, exists, err := field.Value(id)
						if err != nil {
							return errors.Wrap(err, "getting int field value for column")
						} else if exists {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: value}})
						} else {
							rowResp.Columns = append(rowResp.Columns,
								&pb.ColumnResponse{ColumnVal: nil})
						}
					}

				case "decimal":
					// Translate column key.
					id, err := h.api.TranslateIndexKey(context.Background(), index.Name(), col)
					if err != nil {
						return errors.Wrap(err, "translating column key")
					}

					dec, exists, err := field.DecimalValue(id)
					if err != nil {
						return errors.Wrap(err, "getting decimal field value for column")
					} else if exists {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_DecimalVal{DecimalVal: &pb.Decimal{Value: dec.Value, Scale: dec.Scale}}})
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
					resp, err := h.api.Query(context.Background(), &query)
					if err != nil {
						return errors.Wrap(err, "querying bool rows(keys)")
					}

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
					} else {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: nil})
					}

				case "time":
					rowResp.Columns = append(rowResp.Columns,
						&pb.ColumnResponse{ColumnVal: nil})
				}
			}

			if err := stream.Send(rowResp); err != nil {
				return errors.Wrap(err, "sending response to stream")
			}
		}

	}
	return nil
}

// I think ideally this would be plugged in the executor somewhere
// in order to get some concurrency benefit but we can
// start with the combined response
func makeRows(resp pilosa.QueryResponse, logger logger.Logger) chan *pb.RowResponse {
	results := make(chan *pb.RowResponse)
	go func() {
		var breakLoop bool // Support the "break" inside the switch.
		for _, result := range resp.Results {
			if breakLoop {
				break
			}
			switch r := result.(type) {
			case *pilosa.Row:
				if len(r.Keys) > 0 {
					// Column keys
					ci := []*pb.ColumnInfo{
						{Name: "_id", Datatype: "string"},
					}
					for _, x := range r.Keys {
						results <- &pb.RowResponse{
							Headers: ci,
							Columns: []*pb.ColumnResponse{
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: x}},
							}}
						ci = nil //only send on the first
					}
				} else {
					// Column IDs
					ci := []*pb.ColumnInfo{
						{Name: "_id", Datatype: "uint64"},
					}
					for _, x := range r.Columns() {
						results <- &pb.RowResponse{
							Headers: ci,
							Columns: []*pb.ColumnResponse{
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: x}},
							}}
						ci = nil //only send on the first
					}

					// The following will return roaring segments.
					// This is commented out for now until we decide how we want to use this.
					/*
						// Roaring segments
						ci := []*pb.ColumnInfo{
							// TODO:
							{Name: "shard", Datatype: "uint64"},
							{Name: "segment", Datatype: "roaring"},
						}
						for _, x := range r.Segments() {
							shard, b := x.Raw()
							results <- &pb.RowResponse{
								Headers: ci,
								Columns: []*pb.ColumnResponse{
									&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(shard)}},
									&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_BlobVal{b}},
								}}
							ci = nil //only send on the first
						}
					*/
				}
			case pilosa.PairField:
				if r.Pair.Key != "" {
					results <- &pb.RowResponse{
						Headers: []*pb.ColumnInfo{
							{Name: r.Field, Datatype: "string"},
							{Name: "count", Datatype: "uint64"},
						},
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: r.Pair.Key}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: r.Pair.Count}},
						},
					}
				} else {
					results <- &pb.RowResponse{
						Headers: []*pb.ColumnInfo{
							{Name: r.Field, Datatype: "uint64"},
							{Name: "count", Datatype: "uint64"},
						},
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: r.Pair.ID}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: r.Pair.Count}},
						},
					}
				}
			case *pilosa.PairsField:
				// Determine if the ID has string keys.
				var stringKeys bool
				if len(r.Pairs) > 0 {
					if r.Pairs[0].Key != "" {
						stringKeys = true
					}
				}

				dtype := "uint64"
				if stringKeys {
					dtype = "string"
				}
				ci := []*pb.ColumnInfo{
					{Name: r.Field, Datatype: dtype},
					{Name: "count", Datatype: "uint64"},
				}
				for _, pair := range r.Pairs {
					if stringKeys {
						results <- &pb.RowResponse{
							Headers: ci,
							Columns: []*pb.ColumnResponse{
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: pair.Key}},
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(pair.Count)}},
							},
						}
					} else {
						results <- &pb.RowResponse{
							Headers: ci,
							Columns: []*pb.ColumnResponse{
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(pair.ID)}},
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(pair.Count)}},
							},
						}
					}
					ci = nil //only send on the first
				}
			case []pilosa.GroupCount:
				for i, gc := range r {
					var ci []*pb.ColumnInfo
					if i == 0 {
						for _, fieldRow := range gc.Group {
							if fieldRow.RowKey != "" {
								ci = append(ci, &pb.ColumnInfo{Name: fieldRow.Field, Datatype: "string"})
							} else if fieldRow.Value != nil {
								ci = append(ci, &pb.ColumnInfo{Name: fieldRow.Field, Datatype: "int64"})
							} else {
								ci = append(ci, &pb.ColumnInfo{Name: fieldRow.Field, Datatype: "uint64"})
							}
						}
						ci = append(ci, &pb.ColumnInfo{Name: "count", Datatype: "uint64"})
						ci = append(ci, &pb.ColumnInfo{Name: "sum", Datatype: "int64"})
					}
					rowResp := &pb.RowResponse{
						Headers: ci,
						Columns: []*pb.ColumnResponse{},
					}

					for _, fieldRow := range gc.Group {
						if fieldRow.RowKey != "" {
							rowResp.Columns = append(rowResp.Columns, &pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: fieldRow.RowKey}})
						} else if fieldRow.Value != nil {
							rowResp.Columns = append(rowResp.Columns, &pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: *fieldRow.Value}})
						} else {
							rowResp.Columns = append(rowResp.Columns, &pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: fieldRow.RowID}})
						}
					}
					rowResp.Columns = append(rowResp.Columns,
						&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: gc.Count}},
						&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: gc.Sum}},
					)
					results <- rowResp
				}
			case pilosa.RowIdentifiers:
				if len(r.Keys) > 0 {
					ci := []*pb.ColumnInfo{{Name: r.Field(), Datatype: "string"}}
					for _, key := range r.Keys {
						results <- &pb.RowResponse{
							Headers: ci,
							Columns: []*pb.ColumnResponse{
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: key}},
							}}
						ci = nil
					}
				} else {
					ci := []*pb.ColumnInfo{{Name: r.Field(), Datatype: "uint64"}}
					for _, id := range r.Rows {
						results <- &pb.RowResponse{
							Headers: ci,
							Columns: []*pb.ColumnResponse{
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(id)}},
							}}
						ci = nil
					}
				}
			case uint64:
				ci := []*pb.ColumnInfo{{Name: "count", Datatype: "uint64"}}
				results <- &pb.RowResponse{
					Headers: ci,
					Columns: []*pb.ColumnResponse{
						&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(r)}},
					}}
			case bool:
				ci := []*pb.ColumnInfo{{Name: "result", Datatype: "bool"}}
				results <- &pb.RowResponse{
					Headers: ci,
					Columns: []*pb.ColumnResponse{
						&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_BoolVal{BoolVal: r}},
					}}
			case pilosa.ValCount:
				var ci []*pb.ColumnInfo
				// ValCount can have a decimal, float, or integer value, but
				// not more than one (as of this writing).
				if r.DecimalVal != nil {
					ci = []*pb.ColumnInfo{
						{Name: "value", Datatype: "decimal"},
						{Name: "count", Datatype: "int64"},
					}
					results <- &pb.RowResponse{
						Headers: ci,
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_DecimalVal{DecimalVal: &pb.Decimal{Value: r.DecimalVal.Value, Scale: r.DecimalVal.Scale}}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: r.Count}},
						}}
				} else if r.FloatVal != 0 {
					ci = []*pb.ColumnInfo{
						{Name: "value", Datatype: "float64"},
						{Name: "count", Datatype: "int64"},
					}
					results <- &pb.RowResponse{
						Headers: ci,
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Float64Val{Float64Val: r.FloatVal}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: r.Count}},
						}}
				} else {
					ci = []*pb.ColumnInfo{
						{Name: "value", Datatype: "int64"},
						{Name: "count", Datatype: "int64"},
					}
					results <- &pb.RowResponse{
						Headers: ci,
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: r.Val}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: r.Count}},
						}}
				}
			case pilosa.SignedRow:
				// TODO: address the overflow issue with values outside the int64 range
				ci := []*pb.ColumnInfo{{Name: r.Field(), Datatype: "int64"}}
				negs := r.Neg.Columns()
				for i := len(negs) - 1; i >= 0; i-- {
					results <- &pb.RowResponse{
						Headers: ci,
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: -1 * int64(negs[i])}},
						}}
					ci = nil
				}
				for _, id := range r.Pos.Columns() {
					results <- &pb.RowResponse{
						Headers: ci,
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Int64Val{Int64Val: int64(id)}},
						}}
					ci = nil
				}

			default:
				logger.Printf("unhandled %T\n", r)
				breakLoop = true
			}
		}
		close(results)
	}()
	return results
}

type grpcServer struct {
	api        *pilosa.API
	grpcServer *grpc.Server
	hostPort   string

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

func OptGRPCServerURI(uri *pilosa.URI) grpcServerOption {
	hostport := fmt.Sprintf("%s:%d", uri.Host, uri.Port)
	return func(s *grpcServer) error {
		s.hostPort = hostport
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
	// create listener
	lis, err := net.Listen("tcp", s.hostPort)
	if err != nil {
		return errors.Wrap(err, "creating listener")
	}
	s.logger.Printf("enabled grpc listening on %s", lis.Addr())

	opts := make([]grpc.ServerOption, 0)
	if tlsConfig != nil {
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	// create grpc server
	s.grpcServer = grpc.NewServer(opts...)
	pb.RegisterPilosaServer(s.grpcServer, grpcHandler{api: s.api, logger: s.logger, stats: s.stats})

	// register the server so its services are available to grpc_cli and others
	reflection.Register(s.grpcServer)

	// and start...
	if err := s.grpcServer.Serve(lis); err != nil {
		return errors.Wrap(err, "starting grpc server")
	}
	return nil
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
