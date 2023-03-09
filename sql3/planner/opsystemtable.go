// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"bytes"
	"context"
	"fmt"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// exclude this file from SonarCloud dupe eval

const (
	fbDatabaseInfo        = "fb_database_info"
	fbDatabaseNodes       = "fb_database_nodes"
	fbExecRequests        = "fb_exec_requests"
	fbPerformanceCounters = "fb_performance_counters"

	fbTableDDL = "fb_table_ddl"
)

type systemTable struct {
	name           string
	schema         types.Schema
	requiresFanout bool
}

var systemTables = map[string]*systemTable{
	fbDatabaseInfo: {
		name: fbDatabaseInfo,
		schema: types.Schema{
			&types.PlannerColumn{
				RelationName: fbDatabaseInfo,
				ColumnName:   "id",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseInfo,
				ColumnName:   "name",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseInfo,
				ColumnName:   "platform",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseInfo,
				ColumnName:   "platform_version",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseInfo,
				ColumnName:   "db_version",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseInfo,
				ColumnName:   "state",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseInfo,
				ColumnName:   "node_count",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseInfo,
				ColumnName:   "replica_count",
				Type:         parser.NewDataTypeInt(),
			},
		},
		requiresFanout: false,
	},
	fbDatabaseNodes: {
		name: fbDatabaseNodes,
		schema: types.Schema{
			&types.PlannerColumn{
				RelationName: fbDatabaseNodes,
				ColumnName:   "id",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseNodes,
				ColumnName:   "type",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseNodes,
				ColumnName:   "state",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseNodes,
				ColumnName:   "uri",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseNodes,
				ColumnName:   "grpc_uri",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseNodes,
				ColumnName:   "is_primary",
				Type:         parser.NewDataTypeBool(),
			},
			&types.PlannerColumn{
				RelationName: fbDatabaseNodes,
				ColumnName:   "space_used",
				Type:         parser.NewDataTypeInt(),
			},
		},
		requiresFanout: false,
	},

	fbExecRequests: {
		name: fbExecRequests,
		schema: types.Schema{
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "nodeid",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "request_id",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "user",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "start_time",
				Type:         parser.NewDataTypeTimestamp(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "end_time",
				Type:         parser.NewDataTypeTimestamp(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "status",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "wait_type",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "wait_time",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "wait_resource",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "cpu_time",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "elapsed_time",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "reads",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "writes",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "logical_reads",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "row_count",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "sql",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbExecRequests,
				ColumnName:   "plan",
				Type:         parser.NewDataTypeString(),
			},
		},
		requiresFanout: true,
	},

	fbTableDDL: {
		name: fbTableDDL,
		schema: types.Schema{
			&types.PlannerColumn{
				RelationName: fbTableDDL,
				ColumnName:   "id",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbTableDDL,
				ColumnName:   "name",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbTableDDL,
				ColumnName:   "ddl",
				Type:         parser.NewDataTypeString(),
			},
		},
		requiresFanout: false,
	},

	fbPerformanceCounters: {
		name: fbPerformanceCounters,
		schema: types.Schema{
			&types.PlannerColumn{
				RelationName: fbPerformanceCounters,
				ColumnName:   "nodeid",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbPerformanceCounters,
				ColumnName:   "namespace",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbPerformanceCounters,
				ColumnName:   "subsystem",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbPerformanceCounters,
				ColumnName:   "counter_name",
				Type:         parser.NewDataTypeString(),
			},
			&types.PlannerColumn{
				RelationName: fbPerformanceCounters,
				ColumnName:   "value",
				Type:         parser.NewDataTypeInt(),
			},
			&types.PlannerColumn{
				RelationName: fbPerformanceCounters,
				ColumnName:   "counter_type",
				Type:         parser.NewDataTypeInt(),
			},
		},
		requiresFanout: true,
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
	result["_schema"] = p.Schema().Plan()
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
	case fbDatabaseInfo:
		return &fbDatabaseInfoRowIter{
			planner: p.planner,
		}, nil
	case fbDatabaseNodes:
		return &fbDatabaseNodesRowIter{
			planner: p.planner,
		}, nil
	case fbExecRequests:
		return &fbExecRequestsRowIter{
			planner: p.planner,
		}, nil
	case fbTableDDL:
		return &fbTableDDLRowIter{
			planner: p.planner,
		}, nil
	case fbPerformanceCounters:
		return &fbPerformanceCountersRowIter{
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

type fbDatabaseInfoRowIter struct {
	planner  *ExecutionPlanner
	rowIndex int
}

var _ types.RowIterator = (*fbDatabaseInfoRowIter)(nil)

func (i *fbDatabaseInfoRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.rowIndex < 1 {
		row := []interface{}{
			i.planner.systemAPI.ClusterName(),
			i.planner.systemAPI.ClusterName(),
			i.planner.systemAPI.PlatformDescription(),
			i.planner.systemAPI.PlatformVersion(),
			i.planner.systemAPI.Version(),
			i.planner.systemAPI.ClusterState(),
			i.planner.systemAPI.ClusterNodeCount(),
			i.planner.systemAPI.ClusterReplicaCount(),
		}
		i.rowIndex += 1
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}

type fbDatabaseNodesRowIter struct {
	planner *ExecutionPlanner
	result  []pilosa.ClusterNode
}

var _ types.RowIterator = (*fbDatabaseNodesRowIter)(nil)

func (i *fbDatabaseNodesRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.result == nil {
		i.result = i.planner.systemAPI.ClusterNodes()
	}

	u := i.planner.systemAPI.DataDir()
	spaceUsed, err := pilosa.GetDiskUsage(u)
	if err != nil {
		return nil, err
	}

	if len(i.result) > 0 {
		n := i.result[0]
		row := []interface{}{
			n.ID,
			"types!",
			n.State,
			n.URI,
			n.GRPCURI,
			n.IsPrimary,
			spaceUsed.Usage,
		}
		// Move to next result element.
		i.result = i.result[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}

type fbExecRequestsRowIter struct {
	planner *ExecutionPlanner
	result  []pilosa.ExecutionRequest
}

var _ types.RowIterator = (*fbExecRequestsRowIter)(nil)

func (i *fbExecRequestsRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.result == nil {
		var err error
		i.result, err = i.planner.systemLayerAPI.ExecutionRequests().ListRequests()
		if err != nil {
			return nil, err
		}
	}

	nodeId := i.planner.systemAPI.NodeID()
	if len(i.result) > 0 {
		n := i.result[0]
		row := []interface{}{
			nodeId,
			n.RequestID,
			n.UserID,
			n.StartTime,
			n.EndTime,
			n.Status,
			n.WaitType,
			n.WaitTime.Microseconds(),
			n.WaitResource,
			n.CPUTime.Microseconds(),
			n.ElapsedTime.Microseconds(),
			n.Reads,
			n.Writes,
			n.LogicalReads,
			n.RowCount,
			n.SQL,
			n.Plan,
		}
		// Move to next result element.
		i.result = i.result[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}

type fbTableDDLRow struct {
	id   string
	name string
	ddl  string
}

type fbTableDDLRowIter struct {
	planner *ExecutionPlanner
	result  []*fbTableDDLRow
}

var _ types.RowIterator = (*fbTableDDLRowIter)(nil)

func (i *fbTableDDLRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.result == nil {
		tbls, err := i.planner.schemaAPI.Tables(ctx)
		if err != nil {
			return nil, err
		}

		i.result = make([]*fbTableDDLRow, len(tbls))

		for idx, tbl := range tbls {
			// build the ddl for this table

			var buf bytes.Buffer
			buf.WriteString("create table ")
			fmt.Fprintf(&buf, "%s", tbl.Name)
			buf.WriteString(" (")

			for idx, col := range tbl.Fields {
				if idx > 0 {
					buf.WriteString(", ")
				}
				fmt.Fprintf(&buf, "%s", col.Name)
				dataType := fieldSQLDataType(pilosa.FieldToFieldInfo(col))
				fmt.Fprintf(&buf, " %s", dataType.TypeDescription())

				switch dt := dataType.(type) {
				case *parser.DataTypeID, *parser.DataTypeString:
					if col.Options.CacheType != pilosa.DefaultCacheType && len(col.Options.CacheType) > 0 {
						fmt.Fprintf(&buf, " cachetype %s", col.Options.CacheType)
					}
					if col.Options.CacheSize != pilosa.DefaultCacheSize && col.Options.CacheSize > 0 {
						// if we still have the default, we need to print that out if we have a non-default size
						if col.Options.CacheType == pilosa.DefaultCacheType && len(col.Options.CacheType) > 0 {
							fmt.Fprintf(&buf, " cachetype %s", col.Options.CacheType)
						}
						fmt.Fprintf(&buf, " size %d", col.Options.CacheSize)
					}

				case *parser.DataTypeIDSet, *parser.DataTypeStringSet:
					if col.Options.CacheType != pilosa.DefaultCacheType && len(col.Options.CacheType) > 0 {
						fmt.Fprintf(&buf, " cachetype %s", col.Options.CacheType)
					}
					if col.Options.CacheSize != pilosa.DefaultCacheSize && col.Options.CacheSize > 0 {
						// if we still have the default, we need to print that out if we have a non-default size
						if col.Options.CacheType == pilosa.DefaultCacheType && len(col.Options.CacheType) > 0 {
							fmt.Fprintf(&buf, " cachetype %s", col.Options.CacheType)
						}
						fmt.Fprintf(&buf, " size %d", col.Options.CacheSize)
					}

				case *parser.DataTypeIDSetQuantum, *parser.DataTypeStringSetQuantum:
					if col.Options.CacheType != pilosa.DefaultCacheType && len(col.Options.CacheType) > 0 {
						fmt.Fprintf(&buf, " cachetype %s", col.Options.CacheType)
					}
					if col.Options.CacheSize != pilosa.DefaultCacheSize && col.Options.CacheSize > 0 {
						// if we still have the default, we need to print that out if we have a non-default size
						if col.Options.CacheType == pilosa.DefaultCacheType && len(col.Options.CacheType) > 0 {
							fmt.Fprintf(&buf, " cachetype %s", col.Options.CacheType)
						}
						fmt.Fprintf(&buf, " size %d", col.Options.CacheSize)
					}
					if !col.Options.TimeQuantum.IsEmpty() {
						fmt.Fprintf(&buf, " timequantum '%s'", col.Options.TimeQuantum)
					}
					if col.Options.TTL > 0 {
						fmt.Fprintf(&buf, " ttl '%s'", col.Options.TTL.String())
					}

				case *parser.DataTypeInt:
					minValue, maxValue := pql.MinMax(0)

					min := col.Options.Min
					if !min.EqualTo(minValue) {
						fmt.Fprintf(&buf, " min %d", min.ToInt64(0))
					}

					max := col.Options.Max
					if !max.EqualTo(maxValue) {
						fmt.Fprintf(&buf, " max %d", max.ToInt64(0))
					}

				case *parser.DataTypeDecimal:
					minValue, maxValue := pql.MinMax(dt.Scale)

					min := col.Options.Min
					if !min.EqualTo(minValue) {
						fmt.Fprintf(&buf, " min %v", min)
					}

					max := col.Options.Max
					if !max.EqualTo(maxValue) {
						fmt.Fprintf(&buf, " max %v", max)
					}

				case *parser.DataTypeTimestamp:
					if len(col.Options.TimeUnit) > 0 {
						fmt.Fprintf(&buf, " timeunit '%s'", col.Options.TimeUnit)
					}
					// TODO(pok) how do we get epoch out of col?

				}
			}
			buf.WriteString(");")
			ddl := buf.String()

			i.result[idx] = &fbTableDDLRow{
				id:   string(tbl.Name),
				name: string(tbl.Name),
				ddl:  ddl,
			}
		}
	}

	if len(i.result) > 0 {
		n := i.result[0]
		row := []interface{}{
			n.id,
			n.name,
			n.ddl,
		}
		// Move to next result element.
		i.result = i.result[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}

type fbPerformanceCountersRowIter struct {
	planner *ExecutionPlanner
	result  []pilosa.PerformanceCounter
}

var _ types.RowIterator = (*fbPerformanceCountersRowIter)(nil)

func (i *fbPerformanceCountersRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.result == nil {
		var err error
		i.result, err = pilosa.PerfCounters.ListCounters()
		if err != nil {
			return nil, err
		}
	}

	nodeId := i.planner.systemAPI.NodeID()
	if len(i.result) > 0 {
		n := i.result[0]
		row := []interface{}{
			nodeId,
			n.NameSpace,
			n.SubSystem,
			n.CounterName,
			n.Value,
			n.CounterType,
		}
		// Move to next result element.
		i.result = i.result[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
