package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/pilosa/pilosa"
	pb "github.com/pilosa/pilosa/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
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
						{Name: "id", Datatype: "string"},
					}
					for _, x := range r.Keys {
						results <- &pb.RowResponse{
							ColumnInfo: ci,
							Columns: []*pb.ColumnResponse{
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{x}},
							}}
						ci = nil //only send on the first
					}
				} else {
					// Roaring segments
					ci := []*pb.ColumnInfo{
						// TODO:
						{Name: "shard", Datatype: "uint64"},
						{Name: "segment", Datatype: "roaring"},
					}
					for _, x := range r.Segments() {
						shard, b := x.Raw()
						results <- &pb.RowResponse{
							ColumnInfo: ci,
							Columns: []*pb.ColumnResponse{
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(shard)}},
								&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_BlobVal{b}},
							}}
						ci = nil //only send on the first
					}
				}
			case pilosa.Pair:
				results <- &pb.RowResponse{
					ColumnInfo: []*pb.ColumnInfo{
						{Name: "id", Datatype: "uint64"},
						{Name: "key", Datatype: "string"},
						{Name: "count", Datatype: "uint64"},
					},
					Columns: []*pb.ColumnResponse{
						&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(r.ID)}},
						&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{r.Key}},
						&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(r.Count)}},
					},
				}
			case []pilosa.Pair:
				ci := []*pb.ColumnInfo{
					{Name: "id", Datatype: "uint64"},
					{Name: "key", Datatype: "string"},
					{Name: "count", Datatype: "uint64"},
				}
				for _, pair := range r {
					results <- &pb.RowResponse{
						ColumnInfo: ci,
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(pair.ID)}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{pair.Key}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(pair.Count)}},
						}}
					ci = nil //only send on the first
				}
			case []pilosa.GroupCount:
				ci := []*pb.ColumnInfo{
					{Name: "label", Datatype: "string"},
					{Name: "count", Datatype: "uint64"},
				}
				for _, gc := range r {
					results <- &pb.RowResponse{
						ColumnInfo: ci,
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_StringVal{makeLabel(gc.Group)}},
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(gc.Count)}},
						}}
					ci = nil //only send on the first
				}
			case pilosa.RowIdentifiers:
				ci := []*pb.ColumnInfo{{Name: "id", Datatype: "uint64"}}
				for _, id := range r.Rows {
					results <- &pb.RowResponse{
						ColumnInfo: ci,
						Columns: []*pb.ColumnResponse{
							&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(id)}},
						}}
					ci = nil
				}
			case uint64:
				ci := []*pb.ColumnInfo{{Name: "count", Datatype: "uint64"}}
				results <- &pb.RowResponse{
					ColumnInfo: ci,
					Columns: []*pb.ColumnResponse{
						&pb.ColumnResponse{ColumnVal: &pb.ColumnResponse_IntVal{int64(r)}},
					}}
			default:
				results <- &pb.RowResponse{
					ErrorMessage: fmt.Sprintf("unhandled %T\n", r),
				}
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
		er := &pb.RowResponse{
			ErrorMessage: err.Error(),
		}
		err = stream.Send(er)
		return errors.Wrap(err, "querying")
	}
	for row := range makeRows(resp) {
		err = stream.Send(row)
		if err != nil {
			return errors.Wrap(err, "sending row to stream")
		}
	}

	return nil
}

func makeItems(p pilosa.RowIdentifiers) []*pb.IdKey {
	if len(p.Keys) == 0 {
		//use Rows
		results := make([]*pb.IdKey, len(p.Rows))
		for i, id := range p.Rows {
			item := &pb.IdKey{Type: &pb.IdKey_Id{Id: int64(id)}}
			results[i] = item
		}
		return results
	}
	results := make([]*pb.IdKey, len(p.Rows))
	for i, key := range p.Keys {
		item := &pb.IdKey{Type: &pb.IdKey_Key{Key: key}}
		results[i] = item
	}
	return results
}
func (s grpcHandler) Inspect(req *pb.InspectRequest, stream pb.Pilosa_InspectServer) error {

	schema := s.api.Schema(context.Background())
	var fields []string
	for _, index := range schema {
		if index.Name == req.Index {
			for _, field := range index.Fields {
				fields = append(fields, field.Name)
			}
		}
	}
	for _, col := range req.Columns {
		ir := &pb.InspectResponse{}
		for _, field := range fields {
			var column string
			if col.GetKey() == "" { //need to figure out the proper way to handle oneof
				column = fmt.Sprintf("%d", col.GetId())
			} else {
				column = fmt.Sprintf("\"%s\"", col.GetKey())
			}
			pql := fmt.Sprintf("Rows(%s, column=%s)", field, column)
			query := pilosa.QueryRequest{
				Index: req.Index,
				Query: pql,
			}
			resp, err := s.api.Query(context.Background(), &query)
			if err != nil {
				return err
			}
			var ids []*pb.IdKey
			for _, result := range resp.Results {
				ids = makeItems(result.(pilosa.RowIdentifiers))
			}
			fs := &pb.FieldSet{
				FieldName: field,
				Items:     ids,
			}
			ir.Set = append(ir.Set, fs)
		}
		stream.Send(ir)
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
