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
	"log"
	"net"

	"github.com/pilosa/pilosa/v2"
	pb "github.com/pilosa/pilosa/v2/proto"
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
}

// QueryPQL handles the PQL request and sends RowResponses to the stream.
func (h grpcHandler) QueryPQL(req *pb.QueryPQLRequest, stream pb.Pilosa_QueryPQLServer) error {
	query := pilosa.QueryRequest{
		Index: req.Index,
		Query: req.Pql,
	}
	resp, err := h.api.Query(context.Background(), &query)
	if err != nil {
		return status.Error(codes.Unknown, err.Error())
	}
	for row := range makeRows(resp) {
		err = stream.Send(row)
		if err != nil {
			return status.Error(codes.Unknown, err.Error())
		}
	}

	return nil
}

// Inspect handles the inpspect request and sends an InspectResponse to the stream.
func (h grpcHandler) Inspect(req *pb.InspectRequest, stream pb.Pilosa_InspectServer) error {
	index, err := h.api.Index(context.Background(), req.Index)
	if err != nil {
		return errors.Wrap(err, "getting index")
	}

	var fields []*pilosa.Field
	for _, field := range index.Fields() {
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

	// If there are no matching fields, then don't return any records.
	if len(fields) == 0 {
		return nil
	}

	if ints, ok := req.Columns.Type.(*pb.IdsOrKeys_Ids); ok {
		ci := []*pb.ColumnInfo{
			{Name: "_id", Datatype: "uint64"},
		}
		for _, field := range fields {
			ci = append(ci, &pb.ColumnInfo{Name: field.Name(), Datatype: field.Type()}) // TODO: field.Type likely doesn't align with supported datatypes
		}

		for _, col := range ints.Ids.Vals {
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
				case "decimal":
					value, exists, err := field.FloatValue(col)
					if err != nil {
						return errors.Wrap(err, "getting decimal field value for column")
					} else if exists {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Float64Val{Float64Val: value}})
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
				}
			}

			if err := stream.Send(rowResp); err != nil {
				return errors.Wrap(err, "sending response to stream")
			}
		}

	} else if keys, ok := req.Columns.Type.(*pb.IdsOrKeys_Keys); ok {
		ci := []*pb.ColumnInfo{
			{Name: "_id", Datatype: "string"},
		}
		for _, field := range fields {
			ci = append(ci, &pb.ColumnInfo{Name: field.Name(), Datatype: field.Type()}) // TODO: field.Type likely doesn't align with supported datatypes
		}

		for _, col := range keys.Keys.Vals {
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
					id, err := index.TranslateStore().TranslateKey(col)
					if err != nil {
						return errors.Wrap(err, "translating column key")
					}

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
				case "decimal":
					// Translate column key.
					id, err := index.TranslateStore().TranslateKey(col)
					if err != nil {
						return errors.Wrap(err, "translating column key")
					}

					value, exists, err := field.FloatValue(id)
					if err != nil {
						return errors.Wrap(err, "getting decimal field value for column")
					} else if exists {
						rowResp.Columns = append(rowResp.Columns,
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Float64Val{Float64Val: value}})
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
func makeRows(resp pilosa.QueryResponse) chan *pb.RowResponse {
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
			case pilosa.Pair:
				if r.Key != "" {
					results <- &pb.RowResponse{
						Headers: []*pb.ColumnInfo{
							{Name: "_id", Datatype: "string"},
							{Name: "count", Datatype: "uint64"},
						},
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: r.Key}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: r.Count}},
						},
					}
				} else {
					results <- &pb.RowResponse{
						Headers: []*pb.ColumnInfo{
							{Name: "_id", Datatype: "uint64"},
							{Name: "count", Datatype: "uint64"},
						},
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: r.ID}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: r.Count}},
						},
					}
				}
			case []pilosa.Pair:
				// Determine if the ID has string keys.
				var stringKeys bool
				if len(r) > 0 {
					if r[0].Key != "" {
						stringKeys = true
					}
				}

				dtype := "uint64"
				if stringKeys {
					dtype = "string"
				}
				ci := []*pb.ColumnInfo{
					{Name: "_id", Datatype: dtype},
					{Name: "count", Datatype: "uint64"},
				}
				for _, pair := range r {
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
							} else {
								ci = append(ci, &pb.ColumnInfo{Name: fieldRow.Field, Datatype: "uint64"})
							}
						}
						ci = append(ci, &pb.ColumnInfo{Name: "count", Datatype: "uint64"})
					}
					rowResp := &pb.RowResponse{
						Headers: ci,
						Columns: []*pb.ColumnResponse{},
					}

					for _, fieldRow := range gc.Group {
						if fieldRow.RowKey != "" {
							rowResp.Columns = append(rowResp.Columns, &pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: fieldRow.RowKey}})
						} else {
							rowResp.Columns = append(rowResp.Columns, &pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(fieldRow.RowID)}})
						}
					}
					rowResp.Columns = append(rowResp.Columns, &pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(gc.Count)}})
					results <- rowResp
				}
			case pilosa.RowIdentifiers:
				if len(r.Keys) > 0 {
					ci := []*pb.ColumnInfo{{Name: "_id", Datatype: "string"}}
					for _, key := range r.Keys {
						results <- &pb.RowResponse{
							Headers: ci,
							Columns: []*pb.ColumnResponse{
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: key}},
							}}
						ci = nil
					}
				} else {
					ci := []*pb.ColumnInfo{{Name: "_id", Datatype: "uint64"}}
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
			default:
				log.Printf("unhandled %T\n", r)
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
	return func(h *grpcServer) error {
		h.hostPort = hostport
		return nil
	}
}

func (s *grpcServer) Serve(tlsConfig *tls.Config) error {
	// create listener
	lis, err := net.Listen("tcp", s.hostPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("enabled grpc listening on %s", s.hostPort)

	opts := make([]grpc.ServerOption, 0)
	if tlsConfig != nil {
		creds := credentials.NewTLS(tlsConfig)
		if err != nil {
			log.Fatalf("loading tls: %s\n", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	// create grpc server
	s.grpcServer = grpc.NewServer(opts...)
	pb.RegisterPilosaServer(s.grpcServer, grpcHandler{api: s.api})

	// register the server so its services are available to grpc_cli and others
	reflection.Register(s.grpcServer)

	// and start...
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	return nil
}

func NewGRPCServer(opts ...grpcServerOption) (*grpcServer, error) {
	server := &grpcServer{}
	for _, opt := range opts {
		err := opt(server)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}
	return server, nil
}
