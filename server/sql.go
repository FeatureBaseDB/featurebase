// Copyright 2020 Pilosa Corp.
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

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/logger"
	pb "github.com/molecula/featurebase/v2/proto"
	"github.com/molecula/featurebase/v2/sql"
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
