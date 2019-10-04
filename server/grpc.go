package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/pilosa/pilosa"
	pb "github.com/pilosa/pilosa/internal"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type grpchandler struct {
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
// in order to get some concurency benefit but we can
// start with the combined response
func makeRows(resp pilosa.QueryResponse) chan *pb.RowResponse {
	results := make(chan *pb.RowResponse)
	go func() {
		for _, result := range resp.Results {
			switch r := result.(type) {
			case *pilosa.Row:
				ci := []*pb.ColumnInfo{
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
				fmt.Printf("unhandled %T\n", r)
			}
		}
		close(results)
	}()
	return results
}
func (s grpchandler) QueryPQL(pql *pb.QueryPQLRequest, stream pb.Molecula_QueryPQLServer) error {
	fmt.Println(pql.Pql)
	query := pilosa.QueryRequest{
		Index: pql.Vds,
		Query: pql.Pql,
	}
	resp, err := s.api.Query(context.Background(), &query)
	if err != nil {
		return err
	}
	for row := range makeRows(resp) {
		err := stream.Send(row)
		if err != nil {
			return err
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
	// create listiner
	lis, err := net.Listen("tcp", s.hostPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("enabled grpc listening on %s", s.hostPort)

	// create grpc server
	srv := grpc.NewServer()
	pb.RegisterMoleculaServer(srv, grpchandler{api: s.api})

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
