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
	"strings"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/pql"
	pproto "github.com/molecula/featurebase/v2/proto"
	"github.com/pkg/errors"
	"vitess.io/vitess/go/vt/sqlparser"
)

// SelectHandler executes SQL select statements
type SelectHandler struct {
	api    *pilosa.API
	router *router
}

// NewSelectHandler constructor
func NewSelectHandler(api *pilosa.API) *SelectHandler {
	return &SelectHandler{
		api:    api,
		router: newRouter(),
	}
}

// Handle executes mapped SQL
func (s *SelectHandler) Handle(ctx context.Context, mapped *MappedSQL) (pproto.ToRowser, error) {
	stmt, ok := mapped.Statement.(*sqlparser.Select)

	if !ok {
		return nil, fmt.Errorf("statement is not type select: %T", mapped.Statement)
	}
	mr, err := s.mapSelect(ctx, stmt, mapped.Mask)
	if err != nil {
		return nil, errors.Wrap(err, "mapping select")
	}
	return s.execMappingResult(ctx, mr, mapped.SQL)
}

func (s *SelectHandler) mapSelect(ctx context.Context, selectStmt *sqlparser.Select, qm QueryMask) (*MappingResult, error) {
	// Get the handler for this query mask.
	handler := s.router.handler(qm)
	if handler == nil {
		return nil, ErrUnsupportedQuery
	}
	indexFunc := func(indexName string) *pilosa.Index {
		idx, err := s.api.Index(ctx, indexName)
		if err != nil {
			return nil
		}
		return idx
	}

	mr, err := handler.Apply(selectStmt, qm, indexFunc)
	if err != nil {
		return nil, errors.Wrap(err, "handling")
	}
	return mr, nil
}

func (s *SelectHandler) execMappingResult(ctx context.Context, mr *MappingResult, sql string) (pproto.ToRowser, error) {
	if mr.Query == "" {
		return nil, errors.New("no pql query created")
	}

	resp, err := s.api.Query(ctx, &pilosa.QueryRequest{Index: mr.IndexName, Query: mr.Query, SQLQuery: sql})
	if err != nil {
		return nil, errors.Wrap(err, "doing pql query")
	}
	res := resp.Results[0]

	var result pproto.ToRowser
	switch res := res.(type) {
	case pproto.ToRowser:
		result = res
	case []pilosa.GroupCount:
		result = pilosa.NewGroupCounts("", res...)
	case uint64:
		result = pproto.ConstRowser{
			{
				Headers: []*pproto.ColumnInfo{
					{
						Name:     "count",
						Datatype: "uint64",
					},
				},
				Columns: []*pproto.ColumnResponse{
					{
						ColumnVal: &pproto.ColumnResponse_Uint64Val{
							Uint64Val: res,
						},
					},
				},
			},
		}
	case bool:
		result = pproto.ConstRowser{
			{
				Headers: []*pproto.ColumnInfo{
					{
						Name:     "result",
						Datatype: "bool",
					},
				},
				Columns: []*pproto.ColumnResponse{
					{
						ColumnVal: &pproto.ColumnResponse_BoolVal{
							BoolVal: res,
						},
					},
				},
			},
		}
	case nil:
		result = pproto.ConstRowser{}

	default:
		return nil, fmt.Errorf("unsupported result type %T", res)
	}

	// Apply reducers.
	for _, reducer := range mr.Reducers {
		result = reducer(result)
	}

	return result, nil
}

type MappingResult struct {
	IndexName    string
	ColumnIDs    []uint64
	ColumnKeys   []string
	FieldFilters []string
	Limit        uint64
	Offset       uint64
	Query        string
	Header       []Column
	Reducers     []func(pproto.ToRowser) pproto.ToRowser
}

func (mr *MappingResult) addReducer(r func(pproto.ToRowser) pproto.ToRowser) {
	mr.Reducers = append(mr.Reducers, r)
}

type SelectProperties struct {
	Index             *pilosa.Index
	Fields            []Column
	Features          selectFeatures
	WherePQL          string
	WhereIDs          []uint64
	WhereKeys         []string
	Offset            uint
	Limit             uint
	GroupByFieldNames []string
	Having            *HavingClause
}

type selectFunc struct {
	funcName FuncName
	field    *pilosa.Field
}

type selectFeatures struct {
	funcs []selectFunc
}

type HavingClause struct {
	Subj string
	Cond pql.Condition
}

type handler interface {
	Apply(*sqlparser.Select, QueryMask, func(string) *pilosa.Index) (*MappingResult, error)
}

// handlerSelectFieldsFromTable: Inspect()
type handlerSelectFieldsFromTableWhere struct{}

func (h handlerSelectFieldsFromTableWhere) Apply(stmt *sqlparser.Select, qm QueryMask, indexFunc func(string) *pilosa.Index) (*MappingResult, error) {
	indexName, err := extractIndexName(stmt)
	if err != nil {
		return nil, errors.Wrapf(err, "extracting index name")
	}
	index := indexFunc(indexName)
	if index == nil {
		return nil, errors.WithMessage(pilosa.ErrIndexNotFound, indexName)
	}

	var whereQuery string
	if qm.HasWhere() {
		whereQuery, err = extractWhere(index, stmt.Where.Expr)
		if err != nil {
			return nil, err
		}
	} else {
		whereQuery = "All()"
	}

	selectFields, _, err := extractSelectFields(index, stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting select fields")
	}

	var fields []string
	for _, fld := range selectFields {
		if _, ok := fld.(*StarColumn); ok {
			pflds := index.Fields()
			fields = []string{"_id"}
			for _, f := range pflds {
				name := f.Name()
				if strings.HasPrefix(name, "_") {
					continue
				}
				fields = append(fields, name)
			}
			break
		}
		fields = append(fields, fld.Name())
	}
	for i, fld := range fields {
		if fld == "_id" && i != 0 {
			return nil, errors.New("_id can only be the first field in a select")
		}
	}

	limit, offset, hasLimit, hasOffset, err := extractLimitOffset(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting limit")
	}

	orderByFlds, orderByDirs, err := extractOrderBy(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting order by")
	}

	mr := &MappingResult{
		IndexName: indexName,
		Header:    selectFields,
	}

	// assign headers
	mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
		return AssignHeaders(result, selectFields...)
	})

	if qm.HasOrderBy() {
		// Sort the results.
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return OrderBy(result, orderByFlds, orderByDirs)
		})

		// Apply the limit and offset after sorting.
		if hasOffset {
			mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
				return OffsetRows(result, offset)
			})
		}
		if hasLimit {
			mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
				return LimitRows(result, limit)
			})
		}
	} else {
		// Apply the limit and offset inside the query.
		switch {
		case hasLimit:
			whereQuery = Limit(whereQuery, limit, offset)
		case hasOffset:
			whereQuery = Offset(whereQuery, offset)
		}
	}

	if len(fields) > 0 && fields[0] == "_id" {
		fields = fields[1:]
	}
	mr.Query = Extract(whereQuery, fields...)

	return mr, nil
}

// handlerSelectDistinctFromTable: Rows, Rows(limit): select distinct fld from tbl
type handlerSelectDistinctFromTable struct{}

func (h handlerSelectDistinctFromTable) Apply(stmt *sqlparser.Select, qm QueryMask, indexFunc func(string) *pilosa.Index) (*MappingResult, error) {
	indexName, err := extractIndexName(stmt)
	if err != nil {
		return nil, errors.Wrapf(err, "extracting index name")
	}
	index := indexFunc(indexName)
	if index == nil {
		return nil, errors.WithMessage(pilosa.ErrIndexNotFound, indexName)
	}

	selectFields, _, err := extractSelectFields(index, stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting select fields")
	}

	fieldCol, ok := selectFields[0].(*FieldColumn)
	if !ok {
		return nil, errors.New("distinct requires a valid field column")
	}

	limit, offset, hasLimit, hasOffset, err := extractLimitOffset(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting limit")
	}

	orderByFlds, orderByDirs, err := extractOrderBy(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting order by")
	}

	// Determine the type of the field needing distinct.
	// If the pilosa field is type int, handle it as a Distinct() query.
	// Otherwise, use Rows()
	// TODO: ensure this works for all field types (bool, time, etc).
	var qo string
	if fieldCol.Field.Type() == pilosa.FieldTypeInt || fieldCol.Field.Type() == pilosa.FieldTypeTimestamp {
		qo = Distinct(fieldCol.Field.Index(), fieldCol.Field.Name())
	} else {
		if !qm.HasOrderBy() && limit > 0 {
			if qo, err = RowsLimit(fieldCol.Field.Name(), int64(limit)); err != nil {
				return nil, errors.Wrap(err, "creating Rows query")
			}
		} else {
			qo = Rows(fieldCol.Field.Name())
		}
	}

	mr := &MappingResult{
		IndexName: indexName,
		Header:    selectFields,
		Query:     qo,
	}

	// Assign headers to the result.
	mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
		return AssignHeaders(result, selectFields...)
	})

	if qm.HasOrderBy() {
		// Sort the result.
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return OrderBy(result, orderByFlds, orderByDirs)
		})
	}

	// Apply the limit and offset after sorting.
	if hasOffset {
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return OffsetRows(result, offset)
		})
	}
	if hasLimit {
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return LimitRows(result, limit)
		})
	}

	return mr, nil
}

// handlerSelectCountFromTableWhere: Count()
type handlerSelectCountFromTableWhere struct{}

func (h handlerSelectCountFromTableWhere) Apply(stmt *sqlparser.Select, qm QueryMask, indexFunc func(string) *pilosa.Index) (*MappingResult, error) {
	var qo string
	var reducers []func(pproto.ToRowser) pproto.ToRowser

	indexName, err := extractIndexName(stmt)
	if err != nil {
		return nil, errors.Wrapf(err, "extracting index name")
	}
	index := indexFunc(indexName)
	if index == nil {
		return nil, errors.WithMessage(pilosa.ErrIndexNotFound, indexName)
	}

	selectFields, features, err := extractSelectFields(index, stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting select fields")
	}

	var wherePQL string
	if stmt.Where != nil {
		wherePQL, err = extractWhere(index, stmt.Where.Expr)
		if err != nil {
			return nil, err
		}
	} else {
		wherePQL = All()
	}

	funcs := features.funcs
	if len(funcs) != 1 {
		return nil, errors.New("handler does not support multiple functions")
	} else if funcs[0].funcName != FuncCount {
		return nil, errors.Errorf("handler expected func: %s", FuncCount)
	}

	if funcs[0].field == nil {
		qo = Count(wherePQL)
	} else {
		// TODO: add the Distinct (for Int fields) here (like we do in handlerSelectDistinctFromTable)
		qo = Rows(funcs[0].field.Name())
		reducers = append(reducers, func(result pproto.ToRowser) pproto.ToRowser {
			return CountRows(result)
		})
	}
	mr := &MappingResult{
		IndexName: indexName,
		Header:    selectFields,
		Query:     qo,
		Reducers:  reducers,
	}

	// Assign headers to the result.
	mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
		return AssignHeaders(result, selectFields...)
	})

	return mr, nil
}

// handlerSelectFuncFromTableWhere: min(), max(), sum(), avg()
type handlerSelectFuncFromTableWhere struct{}

func (h handlerSelectFuncFromTableWhere) Apply(stmt *sqlparser.Select, qm QueryMask, indexFunc func(string) *pilosa.Index) (*MappingResult, error) {
	var qo string

	indexName, err := extractIndexName(stmt)
	if err != nil {
		return nil, errors.Wrapf(err, "extracting index name")
	}
	index := indexFunc(indexName)
	if index == nil {
		return nil, errors.WithMessage(pilosa.ErrIndexNotFound, indexName)
	}

	selectFields, features, err := extractSelectFields(index, stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting select fields")
	}

	funcs := features.funcs
	if len(funcs) != 1 {
		return nil, errors.New("handler does not support multiple functions")
	}

	funcField := funcs[0].field
	if funcField == nil {
		return nil, errors.New("function contains no field")
	}

	var wherePQL string
	if qm.HasWhere() {
		wherePQL, err = extractWhere(index, stmt.Where.Expr)
		if err != nil {
			return nil, err
		}
	}

	limit, offset, hasLimit, hasOffset, err := extractLimitOffset(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting limit offset")
	}

	orderByFlds, orderByDirs, err := extractOrderBy(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting order by")
	}

	switch funcs[0].funcName {
	case FuncMin:
		qo = Min(funcField.Name(), wherePQL)
	case FuncMax:
		qo = Max(funcField.Name(), wherePQL)
	case FuncAvg:
		fallthrough
	case FuncSum:
		qo = Sum(funcField.Name(), wherePQL)
	}

	mr := &MappingResult{
		IndexName: indexName,
		Header:    selectFields,
		Query:     qo,
	}

	// Apply the ValCount function.
	mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
		return ApplyValCountFunc(result, funcs[0].funcName)
	})

	// Assign headers to the result.
	mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
		return AssignHeaders(result, selectFields...)
	})

	if qm.HasOrderBy() {
		// Sort the result.
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return OrderBy(result, orderByFlds, orderByDirs)
		})
	}

	// Apply a limit and offset to the result.
	if hasOffset {
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return OffsetRows(result, offset)
		})
	}
	if hasLimit {
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return LimitRows(result, limit)
		})
	}

	return mr, nil
}

// handlerSelectGroupBy: GroupBy
type handlerSelectGroupBy struct{}

func (h handlerSelectGroupBy) Apply(stmt *sqlparser.Select, qm QueryMask, indexFunc func(string) *pilosa.Index) (*MappingResult, error) {
	var qo string

	indexName, err := extractIndexName(stmt)
	if err != nil {
		return nil, errors.Wrapf(err, "extracting index name")
	}
	index := indexFunc(indexName)
	if index == nil {
		return nil, errors.WithMessage(pilosa.ErrIndexNotFound, indexName)
	}

	selectFields, features, err := extractSelectFields(index, stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting select fields")
	}

	orderByFlds, orderByDirs, err := extractOrderBy(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting order by")
	}

	// If the query can be supported by TopN,
	// i.e. if it's of the form:
	//   select fld, count(fld) as cnt from tbl group by fld order by cnt desc limit 1
	//   select fld, count(fld) as cnt from tbl where fld2=1 group by fld order by cnt desc limit 1
	// then redirect it to handlerSelectIDCountFromTable.
	// Otherwise, handle it as a normal GroupBy query.
	// TODO: this level of inspection on the query needs to be built into
	// an official query planner. The existing solution, which uses very
	// broad masks to route the query to specific handlers, doesn't do
	// this kind of finer-grain inspection of, for example, the order by
	// fields themselves.
	if func() bool {
		if !qm.HasLimit() {
			return false
		}
		if len(orderByFlds) != 1 {
			return false
		}
		if orderByDirs[0] != "desc" {
			return false
		}
		if qm == MustGenerateMask("select fld, count(fld) from tbl group by fld order by cnt limit 1") ||
			qm == MustGenerateMask("select fld, count(fld) from tbl where fld=1 group by fld order by cnt limit 1") {
			// Check that the order-by field is the count field.
			for i := range selectFields {
				if s, ok := selectFields[i].(*FuncColumn); !ok {
					continue
				} else if s.FuncName == FuncCount && s.Alias() == orderByFlds[0] {
					return true
				}
			}
		}
		return false
	}() {
		return handlerSelectIDCountFromTable{}.Apply(stmt, qm, indexFunc)
	}

	groupByFieldNames, err := extractGroupByFieldNames(stmt.GroupBy)
	if err != nil {
		return nil, errors.Wrap(err, "extracting group by fields")
	}

	having, err := extractHavingClause(stmt.Having)
	if err != nil {
		return nil, errors.Wrap(err, "extracting having clause")
	}

	rowsQueries := []string{}
	for _, fieldName := range groupByFieldNames {
		field := index.Field(fieldName)
		rowsQueries = append(rowsQueries, Rows(field.Name()))
	}

	var wherePQL string
	if stmt.Where != nil {
		wherePQL, err = extractWhere(index, stmt.Where.Expr)
		if err != nil {
			return nil, errors.Wrap(err, "extracting where")
		}
	}

	limit, offset, hasLimit, hasOffset, err := extractLimitOffset(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting limit offset")
	}

	// Group by queries can any combination of count() and sum()
	// in the select fields.
	var idxSum int = -1
	funcs := features.funcs
	for i := range funcs {
		switch funcs[i].funcName {
		case FuncSum:
			idxSum = i
		}
	}

	var sumQuery string
	if idxSum >= 0 {
		sumQuery = Sum(funcs[idxSum].field.Name(), "")
	}

	var havingQuery string
	if having != nil {
		havingQuery = fmt.Sprintf("Condition(%s)", having.Cond.StringWithSubj(having.Subj))
	}

	qo, err = GroupByBase(rowsQueries, int64(limit+offset), wherePQL, sumQuery, havingQuery)
	if err != nil {
		return nil, err
	}

	mr := &MappingResult{
		IndexName: indexName,
		Header:    selectFields,
		Query:     qo,
	}

	// Assign headers to the result.
	mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
		return AssignHeaders(result, selectFields...)
	})

	if qm.HasOrderBy() {
		// Sort the result.
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return OrderBy(result, orderByFlds, orderByDirs)
		})
	}

	// Apply a limit and offset to the result.
	if hasOffset {
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return OffsetRows(result, offset)
		})
	}
	if hasLimit {
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return LimitRows(result, limit)
		})
	}

	return mr, nil
}

// handlerSelectIDCountFromTable: TopN
type handlerSelectIDCountFromTable struct{}

func (f handlerSelectIDCountFromTable) Apply(stmt *sqlparser.Select, qm QueryMask, indexFunc func(string) *pilosa.Index) (*MappingResult, error) {
	var qo string

	indexName, err := extractIndexName(stmt)
	if err != nil {
		return nil, errors.Wrapf(err, "extracting index name")
	}
	index := indexFunc(indexName)
	if index == nil {
		return nil, errors.WithMessage(pilosa.ErrIndexNotFound, indexName)
	}

	selectFields, features, err := extractSelectFields(index, stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting select fields")
	}

	var wherePQL string
	if stmt.Where != nil {
		wherePQL, err = extractWhere(index, stmt.Where.Expr)
		if err != nil {
			return nil, errors.Wrap(err, "extracting where")
		}
	}

	limit, offset, hasLimit, hasOffset, err := extractLimitOffset(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting limit offset")
	}

	funcs := features.funcs
	if len(funcs) != 1 {
		return nil, errors.New("handler does not support multiple functions")
	} else if funcs[0].funcName != FuncCount {
		return nil, errors.Errorf("handler expected func: %s", FuncCount)
	}

	if wherePQL == "" {
		qo = TopN(funcs[0].field.Name(), uint64(limit+offset))
	} else {
		qo = RowTopN(funcs[0].field.Name(), uint64(limit+offset), wherePQL)
	}

	mr := &MappingResult{
		IndexName: indexName,
		Header:    selectFields,
		Query:     qo,
	}

	// Assign headers to the result.
	mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
		return AssignHeaders(result, selectFields...)
	})

	// TODO: order by is not implemented on this method because order desc
	// is handled in pilosa TopN. In order to support asc here, we would
	// have to return the entire TopN cache. Instead, we should consider
	// supported something like this in Pilosa itself.

	// Apply a limit and offset to the result.
	if hasOffset {
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return OffsetRows(result, offset)
		})
	}
	if hasLimit {
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return LimitRows(result, limit)
		})
	}

	return mr, nil
}

// handlerSelectJoin: Join/Distinct()
type handlerSelectJoin struct{}

func (h handlerSelectJoin) Apply(stmt *sqlparser.Select, qm QueryMask, indexFunc func(string) *pilosa.Index) (*MappingResult, error) {
	var qo string

	pts, err := extractJoinTables(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting join tables")
	}

	primary := pts.primary()
	secondary := pts.secondary()

	primaryIndexName := primary.name
	primaryIndex := indexFunc(primaryIndexName)
	primaryField := primaryIndex.Field(primary.column.name)

	secondaryIndexName := secondary.name
	secondaryIndex := indexFunc(secondaryIndexName)
	secondaryField := secondaryIndex.Field(secondary.column.name)

	var wheres tableWheres
	if qm.HasWhere() {
		indexes := []*pilosa.Index{primaryIndex, secondaryIndex}
		wheres, err = extractWheres(indexes, pts, stmt.Where.Expr)
		if err != nil {
			return nil, err
		}
	}

	var primaryWhere string
	var secondaryWhere string
	for i, w := range wheres {
		switch w.table.index {
		case primaryIndex:
			primaryWhere = wheres[i].where
		case secondaryIndex:
			secondaryWhere = wheres[i].where
		}
	}

	// Build the Distinct() portion of the query on the secondary.
	var distinctQry string
	if secondaryWhere == "" {
		distinctQry = Distinct(secondaryField.Index(), secondaryField.Name())
	} else {
		distinctQry = RowDistinct(secondaryField.Index(), secondaryField.Name(), secondaryWhere)
	}

	var rowQry string
	if primaryWhere == "" {
		rowQry = Intersect(All(), distinctQry)
	} else {
		_ = primaryField
		rowQry = Intersect(primaryWhere, distinctQry)
	}

	selectFields, _, err := extractSelectFields(primaryIndex, stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting select fields")
	}

	orderByFlds, orderByDirs, err := extractOrderBy(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting order by")
	}

	if qm.HasSelectPart(SelectPartCountStar) {
		qo = Count(rowQry)
	} else {
		qo = rowQry
	}

	limit, offset, hasLimit, hasOffset, err := extractLimitOffset(stmt)
	if err != nil {
		return nil, errors.Wrap(err, "extracting limit")
	}

	mr := &MappingResult{
		IndexName: primaryIndex.Name(),
		Header:    selectFields,
		Query:     qo,
	}

	// Assign headers to the result.
	mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
		return AssignHeaders(result, selectFields...)
	})

	if qm.HasOrderBy() {
		// Sort the result.
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return OrderBy(result, orderByFlds, orderByDirs)
		})
	}

	// Apply a limit and offset to the result.
	if hasOffset {
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return OffsetRows(result, offset)
		})
	}
	if hasLimit {
		mr.addReducer(func(result pproto.ToRowser) pproto.ToRowser {
			return LimitRows(result, limit)
		})
	}

	return mr, nil
}
