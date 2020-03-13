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
	"crypto/tls"
	"log"
	"sync"

	pb "github.com/pilosa/pilosa/v2/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
)

// GRPCClient is a client for working with the gRPC server.
type GRPCClient struct {
	dialTarget string
	tlsConfig  *tls.Config

	mu   sync.RWMutex
	conn *grpc.ClientConn
}

// NewGRPCClient returns a new instance of GRPCClient.
func NewGRPCClient(dialTarget string, tlsConfig *tls.Config) (*GRPCClient, error) {
	c := &GRPCClient{
		dialTarget: dialTarget,
		tlsConfig:  tlsConfig,
	}
	// resetConn sets GRPCClient.conn when it doesn't
	// exist yet.
	if err := c.resetConn(); err != nil {
		return nil, errors.Wrap(err, "setting connection")
	}

	return c, nil
}

// resetConn resets the gRPC client connection. This method
// can also be used to initially set the client connection
// because it only tries to first close the connection if
// the connection already exists.
func (c *GRPCClient) resetConn() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If an existing connection exists, close it first.
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return errors.Wrap(err, "closing existing connection")
		}
	}

	var opts []grpc.DialOption
	if c.tlsConfig != nil {
		creds := credentials.NewTLS(c.tlsConfig)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	var err error
	if c.conn, err = grpc.Dial(c.dialTarget, opts...); err != nil {
		return errors.Wrap(err, "creating new grpc client")
	}

	return nil
}

// Close closes any connections the client has opened.
func (c *GRPCClient) Close() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Conn returns the gRPC client connection. If the connection
// has gone into state `TransientFailure`, this method tries
// to reset the connection and return that new connection.
func (c *GRPCClient) Conn() *grpc.ClientConn {
	c.mu.RLock()
	if c.conn == nil {
		c.mu.RUnlock()
		return nil
	} else if c.conn.GetState() != connectivity.TransientFailure {
		defer c.mu.RUnlock()
		return c.conn
	}
	c.mu.RUnlock()

	if err := c.resetConn(); err != nil {
		// TODO: log this error with logger
		log.Printf("error resetting connection: %s", err)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

// Query returns a stream of RowResponse for the given index and PQL string.
func (c *GRPCClient) Query(ctx context.Context, index string, pql string) (pb.StreamClient, error) {
	conn := c.Conn()

	if conn == nil {
		return nil, errors.New("client has not established a grpc connection")
	}

	grpcClient := pb.NewPilosaClient(conn)

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

// Inspect returns a stream of RowResponse for the given index, columns, and filters.
// It is intended to mimic something like "select [fields] from table where recordID IN (...)".
func (c *GRPCClient) Inspect(ctx context.Context, index string, columnIDs []uint64, columnKeys []string, fieldFilters []string, limit, offset uint64) (pb.StreamClient, error) {
	conn := c.Conn()

	if conn == nil {
		return nil, errors.New("client has not established a grpc connection")
	}

	if len(columnIDs) > 0 && len(columnKeys) > 0 {
		return nil, errors.New("only provide column ids or keys, not both")
	}

	// Convert columns to proto type IdsOrKeys.
	idsOrKeys := &pb.IdsOrKeys{}
	if len(columnKeys) > 0 {
		idsOrKeys.Type = &pb.IdsOrKeys_Keys{Keys: &pb.StringArray{Vals: columnKeys}}
	} else {
		idsOrKeys.Type = &pb.IdsOrKeys_Ids{Ids: &pb.Uint64Array{Vals: columnIDs}}
	}

	grpcClient := pb.NewPilosaClient(conn)

	stream, err := grpcClient.Inspect(ctx, &pb.InspectRequest{
		Index:        index,
		Columns:      idsOrKeys,
		FilterFields: fieldFilters,
		Limit:        limit,
		Offset:       offset,
	})

	if err != nil {
		return nil, errors.Wrap(err, "getting stream")
	} else if stream == nil {
		return nil, errors.New("could not create stream")
	}

	return stream, err
}