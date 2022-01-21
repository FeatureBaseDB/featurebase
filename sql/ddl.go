// Copyright 2021 Molecula Corp. All rights reserved.
package sql

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3"
	pproto "github.com/molecula/featurebase/v3/proto"
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
