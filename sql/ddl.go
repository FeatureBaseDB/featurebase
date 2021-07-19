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

package sql

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v2"
	pproto "github.com/molecula/featurebase/v2/proto"
	"github.com/pkg/errors"
	"vitess.io/vitess/go/vt/sqlparser"
)

// DDLHandler executes  CREATE, ALTER, DROP, RENAME, TRUNCATE or ANALYZE statement.
type DDLHandler struct {
	api *pilosa.API
}

// NewDDLHandler constructor
func NewDDLHandler(api *pilosa.API) *DDLHandler {
	return &DDLHandler{
		api: api,
	}
}

// Handle executes mapped SQL
func (h *DDLHandler) Handle(ctx context.Context, mapped *MappedSQL) (pproto.ToRowser, error) {
	stmt, ok := mapped.Statement.(*sqlparser.DDL)
	if !ok {
		return nil, fmt.Errorf("statement is not type DDL: %T", mapped.Statement)
	}

	switch stmt.Action {
	case sqlparser.DropStr:
		return h.execDropTable(ctx, stmt)

	default:
		return nil, errors.Errorf("unsupported DDL action: %s", stmt.Action)
	}
}

func (h *DDLHandler) execDropTable(ctx context.Context, stmt *sqlparser.DDL) (pproto.ToRowser, error) {
	if n := len(stmt.FromTables); n != 1 {
		return nil, fmt.Errorf("statement can only contain a single drop table, but got: %d", n)
	}

	indexName := stmt.FromTables[0].ToViewName().Name.String()
	if err := h.api.DeleteIndex(ctx, indexName); err != nil {
		return nil, errors.Wrapf(err, "deleting index %s", indexName)
	}
	return pproto.ConstRowser{}, nil
}
