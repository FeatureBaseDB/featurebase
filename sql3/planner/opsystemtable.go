// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

//fb_exec_requests
//	session
//	user
//	start_time
//	end_time
//	status
//	plan
//	wait_type
//	wait_time
//	wait_resource
//	cpu_time
//	elapsed_time
//	reads
//	writes
//	logical_reads
//	row_count

// exclude this file from SonarCloud dupe eval

const (
	fbClusterInfo  = "fb_cluster_info"
	fbClusterNodes = "fb_cluster_nodes"
)

type systemTable struct {
	name   string
	schema types.Schema
}

var systemTables = map[string]*systemTable{
	fbClusterInfo: {
		name: fbClusterInfo,
		schema: types.Schema{
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "name",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "platform",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "platform_version",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "db_version",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "state",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "node_count",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "shard_width",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "replica_count",
				Type:         parser.NewDataTypeInt(),
			},
		},
	},
	fbClusterNodes: {
		name: fbClusterNodes,
		schema: types.Schema{
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "id",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "state",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "uri",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "grpc_uri",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbClusterInfo,
				ColumnName:   "is_primary",
				Type:         parser.NewDataTypeBool(),
			},
		},
	},
}

// PlanOpSystemTable handles system tables
type PlanOpSystemTable struct {
	planner  *ExecutionPlanner
	table    *systemTable
	warnings []string
}

func NewPlanOpSystemTable(p *ExecutionPlanner, table *systemTable) *PlanOpSystemTable {
	return &PlanOpSystemTable{
		planner:  p,
		table:    table,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpSystemTable) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeName()))
	}
	result["_schema"] = ps
	return result
}

func (p *PlanOpSystemTable) String() string {
	return ""
}

func (p *PlanOpSystemTable) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpSystemTable) Warnings() []string {
	return p.warnings
}

func (p *PlanOpSystemTable) Schema() types.Schema {
	return p.table.schema
}

func (p *PlanOpSystemTable) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpSystemTable) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	switch p.table.name {
	case fbClusterInfo:
		return &fbClusterInfoRowIter{
			planner: p.planner,
		}, nil
	case fbClusterNodes:
		return &fbClusterNodesRowIter{
			planner: p.planner,
		}, nil
	default:
		return nil, sql3.NewErrInternalf("unable to find system table '%s'", p.table.name)
	}
}

func (p *PlanOpSystemTable) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) > 0 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpSystemTable(p.planner, p.table), nil
}

type fbClusterInfoRowIter struct {
	planner  *ExecutionPlanner
	rowIndex int
}

var _ types.RowIterator = (*fbClusterInfoRowIter)(nil)

func (i *fbClusterInfoRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.rowIndex < 1 {
		row := []interface{}{
			i.planner.systemAPI.ClusterName(),
			i.planner.systemAPI.PlatformDescription(),
			i.planner.systemAPI.PlatformVersion(),
			i.planner.systemAPI.Version(),
			i.planner.systemAPI.ClusterState(),
			i.planner.systemAPI.ClusterNodeCount(),
			i.planner.systemAPI.ShardWidth(),
			i.planner.systemAPI.ClusterReplicaCount(),
		}
		i.rowIndex += 1
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}

type fbClusterNodesRowIter struct {
	planner *ExecutionPlanner
	result  []pilosa.ClusterNode
}

var _ types.RowIterator = (*fbClusterNodesRowIter)(nil)

func (i *fbClusterNodesRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.result == nil {
		i.result = i.planner.systemAPI.ClusterNodes()
	}

	if len(i.result) > 0 {
		n := i.result[0]
		row := []interface{}{
			n.ID,
			n.State,
			n.URI,
			n.GRPCURI,
			n.IsPrimary,
		}
		// Move to next result element.
		i.result = i.result[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}