// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package server

import (
	"context"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/logger"
	pb "github.com/featurebasedb/featurebase/v3/proto"
	"github.com/featurebasedb/featurebase/v3/sql"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func execSQL(ctx context.Context, api *pilosa.API, logger logger.Logger, queryStr string) (pb.ToRowser, error) {
	mapper := sql.NewMapper()
	mapper.Logger = logger
	query, err := mapper.MapSQL(queryStr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to map SQL")
	}
	var results pb.ToRowser
	switch query.SQLType {
	case sql.SQLTypeSelect:
		handler := sql.NewSelectHandler(api)
		results, err = handler.Handle(ctx, query)
	case sql.SQLTypeShow:
		handler := sql.NewShowHandler(api)
		results, err = handler.Handle(ctx, query)
	case sql.SQLTypeEmpty:
		handler := sql.NewDDLHandler(api)
		results, err = handler.Handle(ctx, query)
	default:
		return nil, status.Errorf(codes.Unimplemented, "query type not supported")
	}
	return results, errors.Wrap(err, "failed to start SQL query")
}
