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

package client

import (
	"context"

	pb "github.com/pilosa/pilosa/v2/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type GRPCClient struct {
	conn *grpc.ClientConn
}

func NewGRPCClient(dialTarget string) (*GRPCClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure()) // TODO: consider implementing WithTransportCredentials()
	gconn, err := grpc.Dial(dialTarget, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "creating new grpc client")
	}

	return &GRPCClient{
		conn: gconn,
	}, nil
}

// Close closes any connections the client has opened.
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *GRPCClient) Query(ctx context.Context, index string, pql string) (grpc.ClientStream, error) {
	if c.conn == nil {
		return nil, errors.New("client has not established a grpc connection")
	}

	grpcClient := pb.NewPilosaClient(c.conn)

	stream, err := grpcClient.QueryPQL(ctx, &pb.QueryPQLRequest{
		Index: index,
		Pql:   pql,
	})
	if err != nil {
		return nil, errors.Wrap(err, "getting stream")
	} else if stream == nil {
		return nil, errors.New("could not create stream")
	}

	return stream, err
}

func (c *GRPCClient) Inspect(ctx context.Context, index string, columnIDs []uint64, columnKeys []string, fieldFilters []string) (grpc.ClientStream, error) {
	if c.conn == nil {
		return nil, errors.New("client has not established a grpc connection")
	}

	if len(columnIDs) > 0 && len(columnKeys) > 0 {
		return nil, errors.New("only provide column ids or keys, not both")
	}

	// Convert columns to proto type IdsOrKeys.
	idsOrKeys := &pb.IdsOrKeys{}
	if len(columnKeys) > 0 {
		idsOrKeys.Type = &pb.IdsOrKeys_Keys{Keys: &pb.Keys{Vals: columnKeys}}
	} else {
		idsOrKeys.Type = &pb.IdsOrKeys_Ids{Ids: &pb.Ids{Vals: columnIDs}}
	}

	grpcClient := pb.NewPilosaClient(c.conn)

	stream, err := grpcClient.Inspect(ctx, &pb.InspectRequest{
		Index:        index,
		Columns:      idsOrKeys,
		FilterFields: fieldFilters,
	})
	if err != nil {
		return nil, errors.Wrap(err, "getting stream")
	} else if stream == nil {
		return nil, errors.New("could not create stream")
	}

	return stream, err
}
