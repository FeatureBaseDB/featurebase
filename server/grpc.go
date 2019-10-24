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
	"fmt"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc/reflection"

	"github.com/pilosa/pilosa/v2"
	pb "github.com/pilosa/pilosa/v2/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcHandler struct {
	api *pilosa.API
}

func makeLabel(group []pilosa.FieldRow) string {
	var b strings.Builder
	for i, f := range group {
		if i == 0 {
			b.WriteString(f.String())
		} else {
			b.WriteString("-" + f.String())
		}

	}
	return b.String()
}

// I think ideally this would be plugged in the executor somewhere
// in order to get some concurrency benefit but we can
// start with the combined response
func makeRows(resp pilosa.QueryResponse) chan *pb.RowResponse {
	results := make(chan *pb.RowResponse)
	go func() {
		for _, result := range resp.Results {
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
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{x}},
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
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_UintVal{x}},
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
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{r.Key}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(r.Count)}},
						},
					}
				} else {
					results <- &pb.RowResponse{
						Headers: []*pb.ColumnInfo{
							{Name: "_id", Datatype: "uint64"},
							{Name: "count", Datatype: "uint64"},
						},
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(r.ID)}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(r.Count)}},
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
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{pair.Key}},
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(pair.Count)}},
							},
						}
					} else {
						results <- &pb.RowResponse{
							Headers: ci,
							Columns: []*pb.ColumnResponse{
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(pair.ID)}},
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(pair.Count)}},
							},
						}
					}
					ci = nil //only send on the first
				}
			case []pilosa.GroupCount:
				ci := []*pb.ColumnInfo{
					{Name: "label", Datatype: "string"},
					{Name: "count", Datatype: "uint64"},
				}
				for _, gc := range r {
					results <- &pb.RowResponse{
						Headers: ci,
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{makeLabel(gc.Group)}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(gc.Count)}},
						}}
					ci = nil //only send on the first
				}
			case pilosa.RowIdentifiers:
				ci := []*pb.ColumnInfo{{Name: "_id", Datatype: "uint64"}}
				for _, id := range r.Rows {
					results <- &pb.RowResponse{
						Headers: ci,
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(id)}},
						}}
					ci = nil
				}
			case uint64:
				ci := []*pb.ColumnInfo{{Name: "count", Datatype: "uint64"}}
				results <- &pb.RowResponse{
					Headers: ci,
					Columns: []*pb.ColumnResponse{
						&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(r)}},
					}}
			default:
				log.Printf("unhandled %T\n", r)
				break
			}
		}
		close(results)
	}()
	return results
}
func (s grpcHandler) QueryPQL(req *pb.QueryPQLRequest, stream pb.Pilosa_QueryPQLServer) error {
	//fmt.Println(req.Pql)
	query := pilosa.QueryRequest{
		Index: req.Index,
		Query: req.Pql,
	}
	resp, err := s.api.Query(context.Background(), &query)
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

func makeItems(p pilosa.RowIdentifiers) *pb.IdsOrKeys {
	if len(p.Keys) == 0 {
		//use Rows
		results := make([]uint64, len(p.Rows))
		for i, id := range p.Rows {
			results[i] = id
		}
		return &pb.IdsOrKeys{Type: &pb.IdsOrKeys_Ids{Ids: &pb.Ids{Vals: results}}}
	}
	results := make([]string, len(p.Keys))
	for i, key := range p.Keys {
		results[i] = key
	}
	return &pb.IdsOrKeys{Type: &pb.IdsOrKeys_Keys{Keys: &pb.Keys{Vals: results}}}
}
func (s grpcHandler) Inspect(req *pb.InspectRequest, stream pb.Pilosa_InspectServer) error {

	schema := s.api.Schema(context.Background())
	var fields []string
	for _, index := range schema {
		if index.Name == req.Index {
			for _, field := range index.Fields {
				if len(req.FilterFields) > 0 {
					for _, filter := range req.FilterFields {
						if filter == field.Name {
							fields = append(fields, field.Name)
							break
						}

					}
				} else {
					fields = append(fields, field.Name)
				}
			}
		}
	}
	ints, ok := req.Columns.Type.(*pb.IdsOrKeys_Ids)
	if ok {
		for _, col := range ints.Ids.Vals {
			ir := &pb.InspectResponse{}
			ir.Fields = append(ir.Fields, &pb.FieldValues{
				Name:   "_id",
				Values: &pb.IdsOrKeys{Type: &pb.IdsOrKeys_Ids{Ids: &pb.Ids{Vals: []uint64{col}}}},
			})
			for _, field := range fields {
				pql := fmt.Sprintf("Rows(%s, column=%d)", field, col)
				query := pilosa.QueryRequest{
					Index: req.Index,
					Query: pql,
				}
				resp, err := s.api.Query(context.Background(), &query)
				if err != nil {
					return err
				}
				var ids *pb.IdsOrKeys
				for _, result := range resp.Results {
					ids = makeItems(result.(pilosa.RowIdentifiers))
				}
				fs := &pb.FieldValues{
					Name:   field,
					Values: ids,
				}
				ir.Fields = append(ir.Fields, fs)
			}
			err := stream.Send(ir)
			if err != nil {
				return err
			}
		}
	} else {
		keys, ok := req.Columns.Type.(*pb.IdsOrKeys_Keys)
		if !ok {
			//not correct type
			return nil
		}
		for _, col := range keys.Keys.Vals {
			ir := &pb.InspectResponse{}
			ir.Fields = append(ir.Fields, &pb.FieldValues{
				Name:   "_id",
				Values: &pb.IdsOrKeys{Type: &pb.IdsOrKeys_Keys{Keys: &pb.Keys{Vals: []string{col}}}},
			})
			for _, field := range fields {
				pql := fmt.Sprintf("Rows(%s, column=\"%s\")", field, col)
				query := pilosa.QueryRequest{
					Index: req.Index,
					Query: pql,
				}
				resp, err := s.api.Query(context.Background(), &query)
				if err != nil {
					return err
				}
				var ids *pb.IdsOrKeys
				for _, result := range resp.Results {
					ids = makeItems(result.(pilosa.RowIdentifiers))
				}
				fs := &pb.FieldValues{
					Name:   field,
					Values: ids,
				}
				ir.Fields = append(ir.Fields, fs)
			}
			stream.Send(ir)
		}
	}
	return nil
}

type grpcServer struct {
	api      *pilosa.API
	hostPort string
}
type grpcServerOption func(s *grpcServer) error

func OptHandlerAPI(api *pilosa.API) grpcServerOption {
	return func(h *grpcServer) error {
		h.api = api
		return nil
	}
}

func OptAddressPort(pilosaURI *pilosa.URI) grpcServerOption {
	hostport := fmt.Sprintf("%s:%d", pilosaURI.Host, pilosaURI.Port+1)
	return func(h *grpcServer) error {
		h.hostPort = hostport
		return nil
	}
}
func (s *grpcServer) Serve() error {
	// create listener
	lis, err := net.Listen("tcp", s.hostPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("enabled grpc listening on %s", s.hostPort)

	// create grpc server
	srv := grpc.NewServer()
	pb.RegisterPilosaServer(srv, grpcHandler{api: s.api})

	// register the server so its services are available to grpc_cli and others
	reflection.Register(srv)

	// and start...
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	return nil
}

func NewGrpcServer(opts ...grpcServerOption) (*grpcServer, error) {
	server := &grpcServer{}
	for _, opt := range opts {
		err := opt(server)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}
	return server, nil
}
