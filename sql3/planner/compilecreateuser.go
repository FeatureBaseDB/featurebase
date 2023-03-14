// // Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strings"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// import (
// 	"context"
// 	"strconv"
// 	"strings"
// 	"time"

// 	pilosa "github.com/featurebasedb/featurebase/v3"
// 	"github.com/featurebasedb/featurebase/v3/dax"
// 	"github.com/featurebasedb/featurebase/v3/pql"
// 	"github.com/featurebasedb/featurebase/v3/sql3"
// 	"github.com/featurebasedb/featurebase/v3/sql3/parser"
// )

// compileCreateUserStatement compiles a CREATE TABLE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileCreateUserStatement(ctx context.Context, stmt *parser.CreateUserStatement) (_ types.PlanOperator, err error) {
	userName := strings.ToLower(parser.IdentName(stmt.Name))
	cop := NewPlanOpCreateUser(p, userName)
	return NewPlanOpQuery(p, cop, p.sql), nil
}
