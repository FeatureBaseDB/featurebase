// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/rbac"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpCreateUser implements the CREATE USER operator
type PlanOpCreateUser struct {
	planner  *ExecutionPlanner
	username string
	warnings []string
}

// Iterator implements types.PlanOperator
func (p *PlanOpCreateUser) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &createUserRowIter{
		planner:  p.planner,
		userName: p.username,
	}, nil
}

func NewPlanOpCreateUser(planner *ExecutionPlanner, username string) *PlanOpCreateUser {
	return &PlanOpCreateUser{
		planner:  planner,
		username: username,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpCreateUser) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpCreateUser) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpCreateUser) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 0 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpCreateUser(p.planner, p.username), nil
}

func (p *PlanOpCreateUser) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["model"] = p.username
	return result
}

func (p *PlanOpCreateUser) String() string {
	return ""
}

func (p *PlanOpCreateUser) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpCreateUser) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	return w
}

type createUserRowIter struct {
	planner  *ExecutionPlanner
	userName string
}

var _ types.RowIterator = (*createUserRowIter)(nil)

func (i *createUserRowIter) Next(ctx context.Context) (types.Row, error) {
	msg := rbac.AddUserMessage{
		Username: i.userName,
	}
	envelope := rbac.UserManagerMessage{
		MessageType: rbac.MAddUser,
		Message:     msg,
	}
	fmt.Printf("Sending message to UserManager...")
	rbac.UserAPI.Send(envelope)
	return nil, types.ErrNoMoreRows
}
