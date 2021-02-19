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

package main

import (
	//"io"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"modernc.org/sqlite"
	_ "modernc.org/sqlite"

	//"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/pilosa/pilosa/v2"

	"github.com/pilosa/pilosa/v2/encoding/proto"
	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pkg/errors"
	"github.com/shurcooL/go-goon"
)

func main() {

}

// Sauron sees everything.
// And he converts PQL into SQL.
//
// The PQL is applied to a Pilosa and the SQL
// is applied to a SQL database. Then the
// query results are compared as they are
// returned to the client.
//
type Sauron struct {
	cfg *SauronConfig

	sql  *PQLToSQL
	url  string // "https://..."
	addr string // host:port
	mux  *http.ServeMux
	srv  *http.Server
	cmd  *exec.Cmd

	// target of proxy
	pilosaHttpURL string

	Handler http.Handler
}

type SauronConfig struct {
	Bind       string // host:port
	BindGRPC   string // host:port
	GossipPort string // port
	DataDir    string

	DatabaseName string // for sql database
}

// IndexInfo2 and FieldInfo2 let us look up
// the schema details quickly by using maps
// to index the fields in an index.
type IndexInfo2 struct {
	Name       string                 `json:"name"`
	CreatedAt  int64                  `json:"createdAt,omitempty"`
	Options    pilosa.IndexOptions    `json:"options"`
	Fields     map[string]*FieldInfo2 `json:"fields"`
	ShardWidth uint64                 `json:"shardWidth"`
}

type FieldInfo2 struct {
	Name      string              `json:"name"`
	CreatedAt int64               `json:"createdAt,omitempty"`
	Options   pilosa.FieldOptions `json:"options"`
	Views     []*pilosa.ViewInfo  `json:"views,omitempty"`
	indexInfo *IndexInfo2
}

func (fi *FieldInfo2) GetIndexName() string {
	return fi.indexInfo.Name
}

func (fi *FieldInfo2) GetIndexCreatedAt() int64 {
	return fi.indexInfo.CreatedAt
}

func (fi *FieldInfo2) GetCreatedAt() int64 {
	return fi.CreatedAt
}

func (fi *FieldInfo2) GetName() string {
	return fi.Name
}

type PQLToSQL struct {
	schema pilosa.Schema
	DB     *sql.DB

	dbprefix string // prefixed to all tables

	// map from indexName to *IndexInfo2
	i2f        map[string]*IndexInfo2
	ShardWidth uint64
}

func NewPQLToSQL(dbprefix string) *PQLToSQL {
	return &PQLToSQL{
		dbprefix:   strings.ToLower(dbprefix), // some SQL are not case sensitive, just use lower case.
		i2f:        make(map[string]*IndexInfo2),
		ShardWidth: 1048576,
	}
}

func (ps *PQLToSQL) Start() error {
	db, err := sql.Open("sqlite", fmt.Sprintf("%v.db", ps.dbprefix))
	if err != nil {
		log.Fatal(err)
	}
	ps.DB = db
	return nil
}
func (ps *PQLToSQL) MustRemove() {
	name := fmt.Sprintf("%v.db", ps.dbprefix)
	os.Remove(name)
	//	panicOn(err)
}

func (ps *PQLToSQL) Stop() error {
	return ps.DB.Close()
}
func (ps *PQLToSQL) Store(index, field string, rowIdOrKey interface{}, rowCall []*pql.Call) (bool, error) {
	rowSql, _, _, err := ToSql(rowCall, index)
	panicOn(err)
	insertSql := fmt.Sprintf(`insert into %vλbits select "%v" as field, "%v" as row, column,0 as timestamp from ( %v )`, index, field, rowIdOrKey, rowSql)
	res, err := ps.DB.Exec(insertSql)
	if err != nil {
		return false, err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return aff > 0, nil
}

// convert '-' to 'Θ' since db cannot have '-' in table names,
// but pilosa allows it in index and field names.
func dash2theta(s string) (r string) {
	return strings.Replace(s, "-", "Θ", -1)
}

func theta2dash(s string) (r string) {
	return strings.Replace(s, "Θ", "-", -1)
}
func (ps *PQLToSQL) CreateSchema(jsonBytes []byte) (err error) {
	err = json.Unmarshal(jsonBytes, &ps.schema)
	if err != nil {
		panic(fmt.Sprintf("decoding request as JSON Pilosa schema: %v", err))
	}
	for _, i := range ps.schema.Indexes {
		ps.createDatabase(i.Name)
		for _, f := range i.Fields {
			panicOn(ps.CreateField(i, f))
		}
	}
	return nil
}

func (ps *PQLToSQL) CreateFieldWithOptions(indexName, fieldName string, o fieldOptions) error {

	ii2, ok := ps.i2f[indexName]
	if !ok {
		return fmt.Errorf("index '%v' not found", indexName)
	}

	f2, ok := ii2.Fields[fieldName]
	if !ok {
		f2 = &FieldInfo2{
			Name: fieldName,
			//CreatedAt: f.CreatedAt,
			Options: *(o.ToPilosaFieldOptions()),
			//Views:     f.Views,
			indexInfo: ii2,
		}
		ii2.Fields[fieldName] = f2
	}
	return nil
}

func (ps *PQLToSQL) CreateIndex(indexName string, opts *pilosa.IndexOptions) (err error) {
	ii2, ok := ps.i2f[indexName]
	if !ok {
		ii2 = &IndexInfo2{
			Name: indexName,
			//CreatedAt:  i.CreatedAt,
			Options:    *opts,
			Fields:     make(map[string]*FieldInfo2),
			ShardWidth: 1048576,
		}
		//TODO (twg) figure oiut where to get Shardwidht
		ps.i2f[indexName] = ii2
	}
	return nil
}
func (ps *PQLToSQL) Query(index string, query []*pql.Call) (err error) {
	//	sql, err := ToSQL(query, index)
	//TODO (twg) implement QUery
	return nil
}
func (ps *PQLToSQL) CreateField(i *pilosa.IndexInfo, f *pilosa.FieldInfo) (err error) {
	ii2, ok := ps.i2f[i.Name]
	if !ok {
		ii2 = &IndexInfo2{
			Name:       i.Name,
			CreatedAt:  i.CreatedAt,
			Options:    i.Options,
			Fields:     make(map[string]*FieldInfo2),
			ShardWidth: i.ShardWidth,
		}
		ps.i2f[i.Name] = ii2
	}

	f2, ok := ii2.Fields[f.Name]
	if !ok {
		f2 = &FieldInfo2{
			Name:      f.Name,
			CreatedAt: f.CreatedAt,
			Options:   f.Options,
			Views:     f.Views,
			indexInfo: ii2,
		}
		ii2.Fields[f.Name] = f2
	}
	return nil
}

func (ps *PQLToSQL) createDatabase(index string) (err error) {
	idx_prefix := dash2theta(index) + "λ"
	// Create fields that don't exist.
	sql := fmt.Sprintf("create table %vbits (field string,row,column,timestamp, unique(field,row,column,timestamp) )", idx_prefix)
	_, err = ps.DB.Exec(sql)
	panicOn(err)
	sql = fmt.Sprintf("create table %vbsi (field,column, val bigint,unique(field,column)) ", idx_prefix)
	_, err = ps.DB.Exec(sql)
	panicOn(err)
	sql = fmt.Sprintf(`CREATE VIEW %vcolumns as
	   select distinct column from %vbits
	   UNION
	   select distinct column from %vbsi`, idx_prefix, idx_prefix, idx_prefix)
	_, err = ps.DB.Exec(sql)
	panicOn(err)
	return
}
func (ps *PQLToSQL) GetField(indexName, fieldName string) (*FieldInfo2, error) {
	idx, ok := ps.i2f[indexName]
	if !ok {
		return nil, errors.New(fmt.Sprintf("index not found '%v'", indexName))
	}
	f, ok := idx.Fields[fieldName]
	if !ok {
		return nil, errors.New(fmt.Sprintf("field not found '%v' in index '%v'", fieldName, indexName))
	}
	return f, nil
}

//TODO (twg) below will be moved to its own file later just have it here for quick navigation
type PqlFunction uint8

//debate between this
type ResultType uint8

const (
	UNKNOWN = ResultType(iota)
	BITMAP
	INT
	ARRAY
	GROUPCOUNT
	SIGNEDROW
	BOOL
	PAIRSFIELD
	TABLE
	AGGSQL
)

func getAggregateSql(call *pql.Call, index string) (string, error) {
	fieldName, ok := call.Args["_field"]
	if !ok {
		fieldName, ok = call.Args["field"]
		if !ok {
			return "", errors.New("no _field present")
		}
	}
	var sq string
	if len(call.Children) > 0 {
		//only child should be a bitmap call
		var err error
		sq, _, _, err = ToSql(call.Children, index)
		if err != nil {
			return sq, err
		}
		sq = " and column in (" + sq + ")"

	}
	sqb := fmt.Sprintf(`select %v(val) from %vλbsi where field="%v"%v`, strings.ToLower(call.Name), index, fieldName, sq)
	sql := fmt.Sprintf(`select val,count(*) from %vλbsi where val=(%v)%v`, index, sqb, sq)

	return sql, nil
}

// ToSql takes a pilosa query(ast) and converts it to sql
// assumption: the pql has successfully executed on server
func applyCondition(field string, cond *pql.Condition) (sql string) {

	switch cond.Op {
	case pql.EQ, pql.NEQ, pql.LT, pql.LTE, pql.GT, pql.GTE:
		val, ok := cond.Uint64Value()
		if !ok {
			panic("no value for condition")
		}
		if cond.Op == pql.EQ {
			sql = fmt.Sprintf(`field="%v" and val=%d`, field, val)
		} else if cond.Op == pql.NEQ {
			sql = fmt.Sprintf(`field="%v" and val!=%d`, field, val)
		} else if cond.Op == pql.LT {
			sql = fmt.Sprintf(`field="%v" and val<%d`, field, val)
		} else if cond.Op == pql.LTE {
			sql = fmt.Sprintf(`field="%v" and val<=%d`, field, val)
		} else if cond.Op == pql.GT {
			sql = fmt.Sprintf(`field="%v" and val>%d`, field, val)
		} else if cond.Op == pql.GTE {
			sql = fmt.Sprintf(`field="%v" and val>=%d`, field, val)
		}
	case pql.BETWEEN, pql.BTWN_LT_LTE, pql.BTWN_LTE_LT, pql.BTWN_LT_LT:
		val, ok := cond.Uint64SliceValue()
		if !ok {
			panic("bad value for condition")
		}
		if cond.Op == pql.BETWEEN {
			sql = fmt.Sprintf(`field="%v" and %v>=%d and %v<=%d`, field, "val", val[0], "val", val[1])
		} else if cond.Op == pql.BTWN_LT_LTE {
			sql = fmt.Sprintf(`field="%v" and %v>%d and %v<=%d`, field, "val", val[0], "val", val[1])
		} else if cond.Op == pql.BTWN_LTE_LT {
			sql = fmt.Sprintf(`field="%v" and %v>%d and %v<=%d`, field, "val", val[0], "val", val[0])
		} else if cond.Op == pql.BTWN_LT_LT {
			sql = fmt.Sprintf(`field="%v" and %v>%d and %v<%d`, field, "val", val[0], "val", val[1])
		}
	}
	return
}
func FetchFields(rowsCalls []*pql.Call) ([]string, []interface{}, error) {
	//TODO (twg) date fields in rows?
	results := make([]string, len(rowsCalls))
	columns := make([]interface{}, 0)
	for i, call := range rowsCalls {
		//TODO (twg) Time Fields ie from/to args
		fieldName, ok := call.Args["_field"]
		if !ok {
			fieldName, ok = call.Args["field"]
			if !ok {
				return nil, nil, errors.New("no field for rows")
			}
		}
		results[i] = fieldName.(string)
		col, ok := call.Args["column"]
		if ok {
			s := fmt.Sprintf("f%v.column='%v'", i+1, col)
			columns = append(columns, s)
		}
	}
	return results, columns, nil
}

func ToSql(ast []*pql.Call, index string) (string, ResultType, []interface{}, error) {
	resultType := UNKNOWN
	optional := make([]interface{}, 0)
	if len(ast) == 0 {
		return "", resultType, optional, nil
	}
	stack := make([]string, 0)
	for _, call := range ast {
		switch c := strings.ToLower(call.Name); c {
		case "row":
			var sql string
			if call.HasConditionArg() {
				for k, v := range call.Args {
					csql := applyCondition(k, v.(*pql.Condition))
					sql = fmt.Sprintf("select distinct column from %vλbsi where %v", index, csql)

					break
				}
			} else {
				field, err := call.FieldArg()
				panicOn(err)
				row := call.Args[field]

				t, ok := call.Args["from"]
				var timestampClause string
				if ok {
					clean := strings.Replace(t.(string), "T", " ", 1)
					timestampClause = fmt.Sprintf(` AND timestamp >= "%v"`, clean)
				}
				t, ok = call.Args["to"]
				if ok {
					clean := strings.Replace(t.(string), "T", " ", 1)
					timestampClause += fmt.Sprintf(` AND timestamp < "%v"`, clean)

				}

				sql = fmt.Sprintf(`select distinct column from %vλbits where field="%v" AND row="%v"%v`, index, field, row, timestampClause)

			}
			stack = append(stack, sql)
			resultType = BITMAP
		case "count":
			//should have args and should be top level
			if len(call.Children) == 0 {
				return "", resultType, optional, errors.New("invalid args for  count")
			}
			nestedSQL, _, _, err := ToSql(call.Children, index)
			panicOn(err)
			sql := "select count(*) from( " + nestedSQL + " )"
			stack = append(stack, sql)
			resultType = INT
		case "union", "intersect":
			var sql string
			for _, subcall := range call.Children {
				sq, _, _, err := ToSql([]*pql.Call{subcall}, index)
				panicOn(err)
				if len(sql) == 0 {
					sql = sq
				} else {
					sql += " " + c + " " + sq
				}
			}
			sql = "select column from(" + sql + ")"
			stack = append(stack, sql)
			resultType = BITMAP
		case "difference":
			var sql string
			for _, subcall := range call.Children {
				sq, _, _, err := ToSql([]*pql.Call{subcall}, index)
				panicOn(err)
				if len(sql) == 0 {
					sql = sq
				} else {
					sql += " except " + sq
				}
			}
			sql = "select column from(" + sql + ")"
			stack = append(stack, sql)
			resultType = BITMAP
		case "not":
			//should have args and should be top level
			if len(call.Children) == 0 {
				return "", resultType, optional, errors.New("invalid args for  count")
			}
			nestedSQL, _, _, err := ToSql(call.Children, index)
			panicOn(err)
			sql := fmt.Sprintf("select column from %vλcolumns except %v", index, nestedSQL)
			stack = append(stack, sql)
			resultType = BITMAP
		case "xor":
			var unionSql string
			for _, subcall := range call.Children {
				sq, _, _, err := ToSql([]*pql.Call{subcall}, index)
				panicOn(err)
				if len(unionSql) == 0 {
					unionSql = sq
				} else {
					unionSql += " union " + sq
				}
			}
			unionSql = "select column from(" + unionSql + ")"
			var intersectSql string
			for _, subcall := range call.Children {
				sq, _, _, err := ToSql([]*pql.Call{subcall}, index)
				panicOn(err)
				if len(intersectSql) == 0 {
					intersectSql = sq
				} else {
					intersectSql += " intersect " + sq
				}
			}
			intersectSql = "select column from(" + intersectSql + ")"
			sql := unionSql + " except " + intersectSql
			stack = append(stack, sql)
			resultType = BITMAP

		case "distinct":
			//TODO validate field type, only valid for bsi fields
			fieldName, ok := call.Args["field"]
			if !ok {
				return "", resultType, optional, errors.New("field not specified on distinct call")
			}
			var sql string
			if len(call.Children) > 1 {
				return "", resultType, optional, errors.New(fmt.Sprintf("unexpected args to distinct query:'%v'", call.Children))
			}
			sql = fmt.Sprintf(`select distinct val from %vλbsi where field="%v"`, index, fieldName)
			if len(call.Children) > 0 {
				sq, _, _, err := ToSql([]*pql.Call{call.Children[0]}, index)
				panicOn(err)
				sql += " AND column in (" + sq + ")"
			}
			stack = append(stack, sql)
			resultType = SIGNEDROW
		case "rows":
			//TODO (twg) Time Fields ie from/to args
			fieldName, ok := call.Args["_field"]
			if !ok {
				fieldName, ok = call.Args["field"]
				if !ok {
					return "", resultType, optional, errors.New("no _field present")
				}
			}
			optional = append(optional, fieldName)
			sql := fmt.Sprintf(`select distinct field, row from %vλbits where field="%v"`, index, fieldName)
			col, ok := call.Args["column"]
			if ok {
				sql = fmt.Sprintf("%v and column='%v'", sql, col)
			}
			prev, ok := call.Args["previous"]
			if ok {
				sql = fmt.Sprintf("%v and row>'%v'", sql, prev)
			}
			t, ok := call.Args["from"]
			if ok {
				clean := strings.Replace(t.(string), "T", " ", 1)
				timestampClause := fmt.Sprintf(` AND timestamp >= "%v"`, clean)
				sql += timestampClause
			}
			t, ok = call.Args["to"]
			if ok {
				clean := strings.Replace(t.(string), "T", " ", 1)
				timestampClause := fmt.Sprintf(` AND timestamp < "%v"`, clean)
				sql += timestampClause
			}
			sql += " order by row"
			limit, ok := call.Args["limit"]
			if ok {
				sql = fmt.Sprintf("%v limit %v", sql, limit)
			}
			stack = append(stack, sql)
			resultType = ARRAY
		case "groupby":
			//need to figure out how many fields we will be looking at
			fields, optColumns, err := FetchFields(call.Children)
			panicOn(err)
			sql := "select "
			for i := range fields {
				f := fmt.Sprintf("f%d", i+1)
				if i == 0 {
					sql += fmt.Sprintf(" %s.field, %s.row", f, f)
				} else {

					sql += fmt.Sprintf(",%s.field, %s.row", f, f)
				}
			}
			tname := fmt.Sprintf("%vλbits", index)
			sql += fmt.Sprintf(",count(*) as cnt from %v f1", tname)
			if len(fields) > 1 {
				for i := range fields[1:] {
					f := fmt.Sprintf("f%d", i+2)
					sql += fmt.Sprintf(" inner join %v %v on f1.column = %v.column", tname, f, f)
				}
			}
			sql += " where "
			gb := ""
			for i, field := range fields {
				f := fmt.Sprintf("f%d", i+1)
				if i == 0 {
					sql += fmt.Sprintf("%v.field='%v'", f, field)
					gb += fmt.Sprintf("%v.field,%v.row", f, f)
				} else {
					sql += fmt.Sprintf("and %v.field='%v'", f, field)
					gb += fmt.Sprintf(",%v.field,%v.row", f, f)
				}
			}
			if len(optColumns) > 0 {
				for _, column := range optColumns {
					sql += fmt.Sprintf(" and %v ", column)
				}
			}
			sql += " group by " + gb + " order by cnt desc"

			optional = append(optional, len(fields))

			stack = append(stack, sql)
			resultType = GROUPCOUNT
		case "topn":
			fieldName, ok := call.Args["_field"]
			if !ok {
				fieldName, ok = call.Args["field"]
				if !ok {
					return "", resultType, optional, errors.New("no _field present")
				}
			}
			var sq string
			if len(call.Children) > 0 {
				if len(call.Children) > 1 {
					return "", resultType, optional, errors.New("invalid topn")
				}
				var err error
				sq, _, _, err = ToSql(call.Children, index)
				panicOn(err)
				sq = " and column in (" + sq + ")"

			}
			sql := fmt.Sprintf(`select row,count(*) as cnt from %vλbits where field="%v"%s group by row order by cnt desc`, index, fieldName, sq)

			limit, ok := call.Args["n"]
			if ok {
				sql += fmt.Sprintf(" limit %v", limit)
			}
			stack = append(stack, sql)
			resultType = PAIRSFIELD
		case "topk":
			fieldName, ok := call.Args["_field"]
			if !ok {
				fieldName, ok = call.Args["field"]
				if !ok {
					return "", resultType, optional, errors.New("no _field present")
				}
			}
			var sq string
			filter, ok := call.Args["filter"]
			if ok {
				//assuming this works in pilosa so the filter must be a correct bitmap call
				//ie row/union/intersect/xor/difference
				var err error
				sq, _, _, err = ToSql([]*pql.Call{filter.(*pql.Call)}, index)
				panicOn(err)
				sq = " and column in (" + sq + ")"

			}
			t, ok := call.Args["from"]
			if ok {
				clean := strings.Replace(t.(string), "T", " ", 1)
				timestampClause := fmt.Sprintf(` AND timestamp >= "%v"`, clean)
				sq += timestampClause
			}
			t, ok = call.Args["to"]
			if ok {
				clean := strings.Replace(t.(string), "T", " ", 1)
				timestampClause := fmt.Sprintf(` AND timestamp < "%v"`, clean)
				sq += timestampClause
			}
			sql := fmt.Sprintf(`select row,count(*) as cnt from %vλbits where field="%v"%s group by row order by cnt desc`, index, fieldName, sq)

			limit, ok := call.Args["k"]
			if ok {
				sql += fmt.Sprintf(" limit %v", limit)
			}
			stack = append(stack, sql)
			resultType = PAIRSFIELD
		case "min", "max":
			sql, err := getAggregateSql(call, index)
			panicOn(err)
			stack = append(stack, sql)
			resultType = AGGSQL
		case "sum":
			fieldName, ok := call.Args["_field"]
			if !ok {
				fieldName, ok = call.Args["field"]
				if !ok {
					panic("no _field present")
				}
			}
			var sq string
			if len(call.Children) > 0 {
				//only child should be a bitmap call
				var err error
				sq, _, _, err = ToSql(call.Children, index)
				panicOn(err)
				sq = " and column in (" + sq + ")"

			}
			sql := fmt.Sprintf(`select sum(val),count(*) from %vλbsi where field="%v"%v`, index, fieldName, sq)
			stack = append(stack, sql)
			resultType = AGGSQL
		case "constrow":
			sql := "select column1 as column from (values "
			columns, ok := call.Args["columns"].([]interface{})

			if ok {
				for i, col := range columns {
					if i != 0 {
						sql += ","
					}
					sql += fmt.Sprintf("(%v)", col)

				}
				sql += ")"

			}

			stack = append(stack, sql)
			resultType = BITMAP
		case "all":
			sql := fmt.Sprintf("select column from %vλcolumns", index)
			stack = append(stack, sql)
			resultType = BITMAP
		case "extract":
			// extract is a table response
			bm, _, _, err := ToSql([]*pql.Call{call.Children[0]}, index)
			panicOn(err)
			var rowsPart string
			if len(call.Children) > 1 {
				rows := call.Children[1:]
				rowsPart = "select field,row from("
				for i, row := range rows {
					part, _, _, err1 := ToSql([]*pql.Call{row}, index)
					panicOn(err1)
					if i > 0 {
						rowsPart += "union all select field,row from ("
					}
					rowsPart += part + ")"
				}
			}
			var sql string
			if len(rowsPart) == 0 {
				sql = "select '' as field, '' as row,column from (" + bm + ")"
			} else {
				sql = "select sq.field, sq.row, b.column from ("
				sql += rowsPart + fmt.Sprintf(") sq inner join %vλbits b on sq.field = b.field and sq.row = b.row", index)
				sql += " where b.column in (" + bm + ")"
			}

			stack = append(stack, sql)
			resultType = TABLE
		case "unionrows":
			var rowsPart string
			rowsPart = "select field,row from("
			for i, row := range call.Children {
				part, _, _, err1 := ToSql([]*pql.Call{row}, index)
				panicOn(err1)
				if i > 0 {
					rowsPart += "union all select field,row from ("
				}
				rowsPart += part + ")"
			}
			sql := "select distinct b.column from ("
			sql += rowsPart + fmt.Sprintf(") sq inner join %vλbits b on sq.field = b.field and sq.row = b.row", index)
			stack = append(stack, sql)
			resultType = BITMAP
		case "includescolumn":
			bm, _, _, err := ToSql([]*pql.Call{call.Children[0]}, index)
			panicOn(err)
			col, ok := call.Args["column"]
			if !ok {
				return "", resultType, optional, errors.New("missing column parameter")
			}

			sql := fmt.Sprintf("select count(*)>0 from (select column from(%v) where column='%v')", bm, col)
			stack = append(stack, sql)
			resultType = BOOL
		default:
			goon.Dump(call)
			return "", resultType, optional, errors.New(fmt.Sprintf("unknown call '%v'", c))
		}
	}
	if len(stack) == 0 {
		//not sure if i should error on an NOP
		return "", resultType, optional, nil
	}
	return stack[0], resultType, optional, nil
}

func NewSauron(cfg *SauronConfig) *Sauron {

	addr := "127.0.0.1:8080"
	url := "http://" + addr
	s := &Sauron{
		cfg:  cfg,
		addr: addr,
		url:  url,
		sql:  NewPQLToSQL(cfg.DatabaseName),
		srv: &http.Server{
			Addr:           addr,
			ReadTimeout:    50 * time.Millisecond,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		},
	}
	s.Handler = newRouter(s)
	s.srv.Handler = s.Handler
	return s
}

func (p *Sauron) Start() (urlstring string, err error) {
	started := make(chan struct{})

	panicOn(p.sql.Start())

	// start a pilosa server
	bind := p.cfg.Bind
	dataDir := p.cfg.DataDir
	bindGRPC := p.cfg.BindGRPC
	gossipPort := p.cfg.GossipPort

	p.cmd = exec.Command("pilosa", "server",
		"--bind", bind,
		"--data-dir", dataDir,
		"--bind-grpc", bindGRPC,
		"--gossip.port", gossipPort)
	err = p.cmd.Start()
	panicOn(err)

	// wait for it to be listening
	p.pilosaHttpURL = "http://" + bind
	ok := false
	seconds := 20
	for i := 0; i < seconds; i++ {
		nc, err := net.Dial("tcp", bind)
		if err == nil {
			nc.Close()
			ok = true
			break
		}
		time.Sleep(time.Second)
	}
	if !ok {
		return "", fmt.Errorf("could not contact pilosa server on %v after %v seconds. pid = %v", bind, seconds, p.cmd.Process.Pid)
	}
	go func() {
		close(started)

		err = p.srv.ListenAndServe()
		if err != nil {
			if strings.Contains(err.Error(), "Server closed") {
				return
			}
		}
		panicOn(err)
	}()
	<-started
	return p.url, nil
}

func (p *Sauron) Stop() {
	panicOn(p.sql.Stop())
	err := p.srv.Shutdown(context.Background())
	panicOn(err)
	// Kill pilosa
	if err = p.cmd.Process.Kill(); err != nil {
		vv("failed to kill process: ", err)
	}
}

// GetAvailPort asks the OS for an unused port.
// There's a race here, where the port could be grabbed by someone else
// before the caller gets to Listen on it, but we are only using
// it to find a random port for the test hang debugging.
// Moreover, in practice such races are rare. Just ask for
// it again if the port is taken.
// Uses net.Listen("tcp", ":0") to determine a free port, then
// releases it back to the OS with Listener.Close().
func GetAvailPort() int {
	l, _ := net.Listen("tcp", ":0")
	r := l.Addr()
	l.Close()
	return r.(*net.TCPAddr).Port
}

// newRouter creates a new mux http router.
func newRouter(handler *Sauron) http.Handler {
	router := mux.NewRouter()

	router.HandleFunc("/schema", handler.handleGetSchema).Methods("GET").Name("GetSchema")
	router.HandleFunc("/schema", handler.handlePostSchema).Methods("POST").Name("PostSchema")
	router.HandleFunc("/index/{index}/query", handler.handlePostQuery).Methods("POST").Name("PostQuery")

	router.HandleFunc("/index/{index}", handler.handlePostIndex).Methods("POST").Name("PostIndex")
	router.HandleFunc("/index/{index}/field/", handler.handlePostField).Methods("POST").Name("PostField")
	router.HandleFunc("/index/{index}/field/{field}", handler.handlePostField).Methods("POST").Name("PostField")
	router.HandleFunc("/index/{index}/field/{field}/import-roaring/{shard}", handler.handlePostImportRoaring).Methods("POST").Name("PostImportRoaring")
	router.HandleFunc("/index/{index}/field/{field}/import", handler.handlePostImport).Methods("POST").Name("PostImport")

	return router
}

// handleGetSchema handles GET /schema requests.
func (s *Sauron) handleGetSchema(w http.ResponseWriter, req *http.Request) {
	body, resp, err := s.ForwardHandler(w, req)
	_ = body
	_ = resp
	_ = err

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	respBody, err := ioutil.ReadAll(resp.Body)
	panicOn(err)
	_, err = w.Write(respBody)
	panicOn(err)

	// B) there is no SQL version of get schema.

}

// creates an entire schema from the dumped schema.
// requires completed structures, no defaults applied
func (s *Sauron) handlePostSchema(w http.ResponseWriter, req *http.Request) {
	body, resp, err := s.ForwardHandler(w, req)

	w.WriteHeader(resp.StatusCode)
	respBody, err := ioutil.ReadAll(resp.Body)
	panicOn(err)
	_, err = w.Write(respBody)
	panicOn(err)

	// B) SQL
	panicOn(s.sql.CreateSchema(body))
}

// handlePostField handles /index/{index}/field/{field} requests
func (s *Sauron) handlePostIndex(w http.ResponseWriter, r *http.Request) {
	body, resp, err := s.ForwardHandler(w, r)
	_ = body
	_ = resp
	_ = err
	respBody, err := ioutil.ReadAll(resp.Body)
	panicOn(err)
	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}
	req := struct {
		Options pilosa.IndexOptions
	}{
		Options: pilosa.IndexOptions{
			Keys:           false,
			TrackExistence: true,
		}}
	if len(body) > 0 { //only process if options are present
		err = json.Unmarshal(body, &req)
		panicOn(err)
	}
	err = s.sql.CreateIndex(indexName, &req.Options)
	panicOn(err)
	w.Write(respBody)
}

// handlePostField handles /index/{index}/field/{field} requests
func (s *Sauron) handlePostField(w http.ResponseWriter, r *http.Request) {
	body, resp, err := s.ForwardHandler(w, r)
	_ = body
	_ = resp
	_ = err
	indexName, ok := mux.Vars(r)["index"]
	if !ok {
		http.Error(w, "index name is required", http.StatusBadRequest)
		return
	}

	fieldName, ok := mux.Vars(r)["field"]
	if !ok {
		http.Error(w, "field name is required", http.StatusBadRequest)
		return
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	panicOn(err)
	// Decode request.
	var req postFieldRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	err = dec.Decode(&req)
	if err != io.EOF {
		panicOn(err)
	}
	err = s.sql.CreateFieldWithOptions(indexName, fieldName, req.Options)
	_, err = w.Write(respBody)
	panicOn(err)
}

// handlePostQuery handles /index/{index}/query requests
func (s *Sauron) handlePostQuery(w http.ResponseWriter, req *http.Request) {

	body, resp, err := s.ForwardHandler(w, req)
	pilosaJSON, err := ioutil.ReadAll(resp.Body)
	panicOn(err)
	var pilosaResponse pilosa.QueryResponse // DEBUG
	dec := json.NewDecoder(bytes.NewReader(pilosaJSON))
	err = dec.Decode(&pilosaResponse)
	panicOn(err)
	qreq := &pilosa.QueryRequest{
		Index: mux.Vars(req)["index"],
		Query: string(body),
	}

	oracleResponse, err := s.sql.IssueQuery(qreq)

	if !pilosaResponse.Equals(oracleResponse) {
		//plan is to write out a useful query response indicating a discrepancy
		//TODO (twg) write diff in response somehow
		vv("oracle:")
		prettyJson(*oracleResponse)
		vv("pilosa:")
		prettyJson(pilosaResponse)
		panic("barf")
	}
}
func prettyJson(a pilosa.QueryResponse) {
	src, _ := a.MarshalJSON()
	dst := &bytes.Buffer{}
	if err := json.Indent(dst, src, "", "  "); err != nil {
		panic(err)
	}
	vv("%v", string(dst.Bytes()))
}

func (s *Sauron) FieldInfo(index, field string) (*FieldInfo2, error) {
	idx, ok := s.sql.i2f[index]
	if ok {
		f, ok := idx.Fields[field]
		if ok {
			return f, nil
		}
		return nil, errors.New(fmt.Sprintf("field not in schema '%v'", field))
	}
	return nil, errors.New(fmt.Sprintf("index not in schema '%v'", index))
}
func (s *Sauron) handlePostImport(w http.ResponseWriter, req *http.Request) {
	body, resp, err := s.ForwardHandler(w, req)
	_ = body
	_ = resp
	_ = err
	// Get index and field type to determine how to handle the
	// import data.
	indexName := mux.Vars(req)["index"]
	fieldName := mux.Vars(req)["field"]

	_ = indexName
	_ = fieldName

	// If the clear flag is true, treat the import as clear bits.
	q := req.URL.Query()
	doClear := q.Get("clear") == "true"
	doIgnoreKeyCheck := q.Get("ignoreKeyCheck") == "true"
	_ = doClear
	_ = doIgnoreKeyCheck

	info, err := s.FieldInfo(indexName, fieldName)
	panicOn(err)
	// Unmarshal request based on field type.
	fieldType := info.Options.Type
	if fieldType == pilosa.FieldTypeInt || fieldType == pilosa.FieldTypeDecimal {
		iv := &pilosa.ImportValueRequest{}
		if err := proto.DefaultSerializer.Unmarshal(body, iv); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		//TODO (twg) ignore columnKeys at the moment
		//do importValue
		for i, column := range iv.ColumnIDs {
			if len(iv.Values) > 0 {
				s.sql.Set(indexName, column, fieldName, iv.Values[i], "")
			} else if len(iv.FloatValues) > 0 { //TODO (twg) these may not be reachable
				s.sql.Set(indexName, column, fieldName, iv.FloatValues[i], "") //probably some issues here
			} else if len(iv.StringValues) > 0 {
				s.sql.Set(indexName, column, fieldName, iv.StringValues[i], "")
			}
		}
	} else {
		// Field type: set, time, mutex
		// Marshal into request object.
		ir := &pilosa.ImportRequest{}
		if err := proto.DefaultSerializer.Unmarshal(body, ir); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		//do import
		action := s.sql.Set
		if ir.Clear {
			action = func(index string, column uint64, field string, value interface{}, ts string) (bool, error) {
				return s.sql.Clear(index, column, field, value)
			}
		}
		changed := false
		_ = changed
		for i, column := range ir.ColumnIDs {
			ts := ""
			if len(ir.Timestamps) > 0 {
				t := time.Unix(ir.Timestamps[i], 0)
				ts = t.Format("2006-01-02 15:04:05")
			}
			if len(ir.RowIDs) > 0 {
				change, err := action(indexName, column, fieldName, ir.RowIDs[i], ts)
				panicOn(err)
				if change {
					changed = true
				}
			} else if len(ir.RowKeys) > 0 { //TODO (twg) these may not be reachable
				change, err := action(indexName, column, fieldName, ir.RowKeys[i], ts)
				panicOn(err)
				if change {
					changed = true
				}
			}
		}
	}
}
func (s *Sauron) handlePostImportRoaring(w http.ResponseWriter, req *http.Request) {
	body, resp, err := s.ForwardHandler(w, req)
	_ = body
	_ = resp
	_ = err
	// Get index and field type to determine how to handle the
	// import data.
	indexName := mux.Vars(req)["index"]
	fieldName := mux.Vars(req)["field"]

	irr := &pilosa.ImportRoaringRequest{}
	err = proto.DefaultSerializer.Unmarshal(body, irr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	//do i need the shard?
	urlVars := mux.Vars(req)
	shard, err := strconv.ParseUint(urlVars["shard"], 10, 64)
	if err != nil {
		panicOn(err)
	}
	for v, blob := range irr.Views {
		_ = v //ignore the view for now
		rit, err := roaring.NewRoaringIterator(blob)
		panicOn(err)
		s.sql.ImportRoaring(indexName, fieldName, shard, rit)
	}
}

//end handler
func (s *Sauron) ForwardHandler(w http.ResponseWriter, req *http.Request) (body []byte, resp *http.Response, err error) {
	// TODO (twg) replace all the instances
	// we need to buffer the body if we want to read it here and send it
	// in the request.
	body, err = ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// you can reassign the body if you need to parse it as multipart
	req.Body = ioutil.NopCloser(bytes.NewReader(body))

	// create a new url from the raw RequestURI sent by the client
	url := fmt.Sprintf("%s%s", s.pilosaHttpURL, req.RequestURI)

	proxyReq, err := http.NewRequest(req.Method, url, bytes.NewReader(body))

	// We may want to filter some headers, otherwise we could just use a shallow copy
	// proxyReq.Header = req.Header
	proxyReq.Header = make(http.Header)
	for h, val := range req.Header {
		proxyReq.Header[h] = val
	}

	resp, err = http.DefaultClient.Do(proxyReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	return
}

// ImportRoaring must not be a bsi field..check higher up the stack
func (ps *PQLToSQL) ImportRoaring(index, field string, shard uint64, itr roaring.RoaringIterator) {
	rb := roaring.NewBitmap()
	for itrKey, synthC := itr.NextContainer(); synthC != nil; itrKey, synthC = itr.NextContainer() {
		rb.Put(itrKey, synthC)
	}
	for _, pos := range rb.Slice() {
		row, column := pos/ps.ShardWidth, (shard*ps.ShardWidth)+(pos%ps.ShardWidth)
		//ps.SetWithInt64Field(index, column, field, int64(row))
		ps.Set(index, column, field, int64(row), "")
	}
}

func (ps *PQLToSQL) isBSI(index, field string) (full string, isBSI bool) {
	idx, ok := ps.i2f[index]
	if !ok {
		panic(fmt.Errorf("isBSI: no such index '%v'", index))
	}

	fld, ok := idx.Fields[field]
	if !ok {
		panic(fmt.Errorf("isBSI: no such field '%v'", field))
	}
	if fld.Options.Type == "int" {
		return dash2theta(index) + "λbsi", true
	}
	return dash2theta(index) + "λbits", false
}

// Set is the basic bit operation and value operation as well, depending on the field type
func (ps *PQLToSQL) Set(index string, column uint64, field string, value interface{}, timestamp string) (bool, error) {
	table, isBSI := ps.isBSI(index, field)
	var sql string
	if isBSI {
		sql = fmt.Sprintf("INSERT INTO %v VALUES( '%v','%v', '%v')", table, field, column, value)
	} else {
		//if len(timestamp) == 0 {
		//		sql = fmt.Sprintf("INSERT INTO %v (field,row,column)VALUES( '%v','%v', '%v')", table, field, value, column)
		//	} else {
		//TODO(twg) will need to make the timestamp as granular as the field is configured.
		sql = fmt.Sprintf("INSERT INTO %v (field,row,column,timestamp)VALUES( '%v','%v', '%v','%v')", table, field, value, column, timestamp)
		//	}
	}

	_, err := ps.DB.Exec(sql)
	if err != nil {
		if sqerr, ok := err.(*sqlite.Error); ok {
			if sqerr.Code() == 2067 { //constraint error
				return false, nil
			}
		}
	}
	panicOn(err)
	return true, nil
}

// Clear removes the specified column or value
func (ps *PQLToSQL) Clear(index string, column uint64, field string, value interface{}) (bool, error) {
	table, isBSI := ps.isBSI(index, field)
	var sql string
	if isBSI {
		sql = fmt.Sprintf("delete from %v where field='%v' and column='%v'", table, field, column)
	} else {
		sql = fmt.Sprintf("delete from %v where field='%v' and row='%v' and column='%v'", table, field, value, column)
	}

	res, err := ps.DB.Exec(sql)
	panicOn(err)
	r, err := res.RowsAffected()
	panicOn(err)
	if r == 0 {
		return false, nil
	}
	return true, nil
}

// ClearRow removes entire Row(feature)
func (ps *PQLToSQL) ClearRow(index string, field string, row interface{}) (bool, error) {
	table, isBSI := ps.isBSI(index, field)
	var sql string
	if isBSI {
		panic("bsi can't clear row allowed")
	} else {
		sql = fmt.Sprintf("delete from %v where field='%v' and row='%v' ", table, field, row)
	}

	r, err := ps.DB.Exec(sql)
	panicOn(err)
	n, err := r.RowsAffected()
	return n > 0, err
}

func sliceToString(slc []int64) (r string) {
	if len(slc) == 0 {
		return
	}
	for _, i := range slc {
		r += fmt.Sprintf("%v,", i)
	}
	return r[:len(r)-1]
}

const columnLabel = "col"

func (ps *PQLToSQL) IssueQuery(req *pilosa.QueryRequest) (*pilosa.QueryResponse, error) {
	index := req.Index
	q, err := pql.NewParser(strings.NewReader(req.Query)).Parse()
	if err != nil {
		return nil, errors.Wrap(err, "PQLToSQL.IssueQuery parsing PQL")
	}
	var results pilosa.QueryResponse
	for _, c := range q.Calls {

		if callIndex := c.CallIndex(); callIndex != "" {
			index = callIndex
		}

		// Handle the field arg.
		switch strings.ToLower(c.Name) {
		//handle the write queries
		//TODO (twg) figure out sqlite changes() function to determine number of bits changed
		case "set":
			// Read colID.
			colID, ok, err := c.UintArg("_" + columnLabel)
			if err != nil {
				return nil, fmt.Errorf("reading Set() column: %v", err)
			} else if !ok {
				return nil, fmt.Errorf("Set() column argument '%v' required", columnLabel)
			}
			ts, ok := c.Args["_timestamp"]
			var timestamp string
			if ok {
				timestamp = strings.Replace(ts.(string), "T", " ", 1)
			}

			field, err := c.FieldArg()
			if err == nil {
				rs, err := ps.Set(index, colID, field, c.Args[field], timestamp)
				panicOn(err)
				results.Results = append(results.Results, rs)
			} else {
				return nil, err
			}
		case "clear":
			// Read colID.
			colID, ok, err := c.UintArg("_" + columnLabel)
			if err != nil {
				return nil, fmt.Errorf("reading Clear() column: %v", err)
			} else if !ok {
				return nil, fmt.Errorf("Clear() column argument '%v' required", columnLabel)
			}

			field, err := c.FieldArg()
			if err == nil {
				rs, err := ps.Clear(index, colID, field, c.Args[field])
				if err != nil {
					return nil, fmt.Errorf("Clear() column: %v", err)
				}
				results.Results = append(results.Results, rs)
			} else {
				vv("c.FieldArg() returned '%v'", err)
			}
		case "clearrow":
			field, err := c.FieldArg()
			panicOn(err)
			rs, err := ps.ClearRow(index, field, c.Args[field])
			panicOn(err)
			results.Results = append(results.Results, rs)
		case "store":
			field, err := c.FieldArg()
			panicOn(err)
			rs, err := ps.Store(index, field, c.Args[field], c.Children)
			panicOn(err)
			results.Results = append(results.Results, rs)
		default: //TODO (twg) move ToSQL into main package
			rs, err := ps.readQuery(index, q)
			panicOn(err)
			results.Results = append(results.Results, rs)
		}
	}
	return &results, nil
}
func (ps *PQLToSQL) handleBitmapCall(sql string) (*pilosa.Row, error) {
	rows, err := ps.DB.Query(sql)
	panicOn(err)
	defer rows.Close()
	columns := make([]uint64, 0)
	for rows.Next() {
		//really need a way to know what the results look like
		var column uint64
		rows.Scan(&column)
		columns = append(columns, column)
	}
	row := pilosa.NewRow(columns...)
	return row, nil
}

func (ps *PQLToSQL) handleIntCall(sql string) (count uint64, err error) {
	rows, err := ps.DB.Query(sql)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		//really need a way to know what the results look like
		rows.Scan(&count)
		return
	}
	return
}

type Table struct {
}

func (ps *PQLToSQL) handleSignedRowCall(sql string) (pilosa.SignedRow, error) {
	rows, err := ps.DB.Query(sql)
	panicOn(err)

	var id uint64
	var pos []uint64
	var neg []uint64
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			return pilosa.SignedRow{}, err
		}
		if id >= 0 {
			pos = append(pos, id)
		} else {
			neg = append(neg, id)
		}
	}
	err = rows.Err()
	if err != nil {
		return pilosa.SignedRow{}, err
	}
	posRow := pilosa.NewRow(pos...)
	negRow := pilosa.NewRow(neg...)
	return pilosa.SignedRow{Pos: posRow, Neg: negRow}, nil

}
func (ps *PQLToSQL) handleStringArrayCall(sql string) ([]interface{}, error) {
	rows, err := ps.DB.Query(sql)
	panicOn(err)
	rowKeys := make([]interface{}, 0)
	var field string
	var id string
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&field, &id)
		if err != nil {
			return nil, err
		}
		rowKeys = append(rowKeys, id)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return rowKeys, nil
}

func (ps *PQLToSQL) handleIntArrayCall(sql string) (map[string][]interface{}, error) {
	rows, err := ps.DB.Query(sql)
	panicOn(err)
	rowIDs := make([]interface{}, 0)
	var id uint64
	var field string
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&field, &id)
		if err != nil {
			return nil, err
		}
		rowIDs = append(rowIDs, id)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	m := map[string][]interface{}{"rows": rowIDs, "keys": []interface{}{}}
	return m, nil
}
func (ps *PQLToSQL) handleGroupCountCall(sqlin string, index string, numFields int) (pilosa.SortedGroupCount, error) {
	rows, err := ps.DB.Query(sqlin)
	panicOn(err)
	//groupCounts := make([]pilosa.GroupCount, 0)
	var groupCounts pilosa.SortedGroupCount
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	row := make([]interface{}, len(cols))
	for i := range cols {
		row[i] = new(sql.RawBytes)
	}
	for rows.Next() {
		// need a row*NumFields + cnt
		//err := rows.Scan(&field, &row, &count)
		groupCount := pilosa.GroupCount{}

		err := rows.Scan(row...)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(row)-1; i += 2 {
			ptr := row[i].(*sql.RawBytes)
			fieldName := string(*ptr)
			fr := pilosa.FieldRow{Field: fieldName}
			fi, err := ps.GetField(index, fieldName)
			if err != nil {
				return nil, err
			}
			ptr = row[i+1].(*sql.RawBytes)
			v := string(*ptr)
			if fi.Options.Keys {
				fr.RowKey = v

			} else {
				iv, err := strconv.Atoi(v)
				if err != nil {
					return nil, err
				}
				fr.RowID = uint64(iv)
			}

			groupCount.Group = append(groupCount.Group, fr)

		}
		ptr := row[len(row)-1].(*sql.RawBytes)
		data := string(*ptr) //convert to string
		i, err := strconv.ParseInt(data, 10, 64)
		panicOn(err)
		groupCount.Count = uint64(i)
		groupCounts = append(groupCounts, groupCount)

	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return groupCounts, nil
}
func (ps *PQLToSQL) handlePairsFieldCall(sql, index string) (pilosa.PairsField, error) {
	rows, err := ps.DB.Query(sql)
	panicOn(err)
	var count uint64
	var row string
	defer rows.Close()
	res := pilosa.PairsField{}
	for rows.Next() {
		err := rows.Scan(&row, &count)
		if err != nil {
			return pilosa.PairsField{}, err
		}
		pair := pilosa.Pair{Key: row, Count: count}
		res.Pairs = append(res.Pairs, pair)
	}
	err = rows.Err()
	if err != nil {
		return pilosa.PairsField{}, err
	}
	return res, nil
}

// special structures to deal with Extract.  In the future I would like to use
// pilosa structs, but dealing with keys makes that complicated for now
// note the column is a string, but i only worry about the type when marshalling out for
// the sqlite part is typeless.  Handling keys is almost seemless and not requiring translation
type extractColumns struct {
	column string
	rows   map[int][]interface{}
	parent *sauronExtractedIDMatrix
}

func (eid extractColumns) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf(`{"column": %v,"rows":[`, eid.column))
	rows := make([][]interface{}, len(eid.parent.fields))
	for k, v := range eid.rows {
		rows[k] = v
	}
	for i, grow := range rows {
		if i != 0 {
			buf.WriteByte(44) //write the comma
		}
		isKey := eid.parent.fields[i].isKey
		buf.WriteString(`[`)
		for d, row := range grow {
			if d != 0 {
				buf.WriteByte(44) //write the comma
			}
			if isKey {
				buf.WriteString(fmt.Sprintf(`"%v"`, row))
			} else {
				buf.WriteString(fmt.Sprintf(`%v`, row))
			}

		}
		buf.WriteString(`]`)
	}

	buf.WriteString(`]}`)
	return buf.Bytes(), nil
}

type extractFields struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	isKey bool
}
type sauronExtractedIDMatrix struct {
	columns []extractColumns
	fields  []extractFields
}

func (eid sauronExtractedIDMatrix) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	sort.Slice(eid.columns, func(i, j int) bool {
		return eid.columns[i].column < eid.columns[j].column
	})
	buf.WriteString(`{"columns":[`)
	for i, col := range eid.columns {
		b, err := col.MarshalJSON()
		panicOn(err)
		if i != 0 {
			buf.WriteByte(44) //write the comma
		}
		buf.Write(b)
	}

	buf.WriteString(`],"fields":[`)
	for i, f := range eid.fields {
		if i != 0 {
			buf.WriteByte(44)
		}
		b, err := json.Marshal(f)
		panicOn(err)
		buf.Write(b)
	}
	buf.WriteString(`]}`)
	return buf.Bytes(), nil
}

func (ps *PQLToSQL) handleTableCall(sql, index string) (sauronExtractedIDMatrix, error) {
	rows, err := ps.DB.Query(sql)
	panicOn(err)
	var field string
	var row string
	var column string
	defer rows.Close()
	col := make(map[string]map[string][]interface{})
	fidx := make(map[string]int)
	results := sauronExtractedIDMatrix{}
	counter := 0
	empty := make(map[string][]interface{})
	for rows.Next() {
		err := rows.Scan(&field, &row, &column)
		if err != nil {
			return sauronExtractedIDMatrix{}, err
		}
		if field != "" {
			_, ok := fidx[field]
			if !ok {
				fidx[field] = counter
				counter++
			}
			rows, ok := col[column]
			if !ok {
				rows = make(map[string][]interface{})
			}
			rows[field] = append(rows[field], row)
			//results.Columns = append(results.Columns, pilosa.ExtractedIDColumn{ColumnID: column, Rows: [][]uint64{}})
			col[column] = rows
		} else { //constrow, or no filed info
			col[column] = empty
		}
	}
	err = rows.Err()
	if err != nil {
		return sauronExtractedIDMatrix{}, err
	}
	for c, rowMap := range col {
		idc := extractColumns{rows: make(map[int][]interface{}), parent: &results}
		idc.column = c
		for fld, rows := range rowMap {
			idx := fidx[fld]
			idc.rows[idx] = rows
		}
		results.columns = append(results.columns, idc)
	}
	results.fields = make([]extractFields, len(fidx))
	for fieldName, idx := range fidx {
		fi, _ := ps.GetField(index, fieldName)
		// TODO (twg) hack need to figure out proper way to determine type
		aType := "[]uint64"
		if fi.Options.Keys {
			aType = "[]string"
		}
		results.fields[idx] = extractFields{Name: fieldName, Type: aType, isKey: fi.Options.Keys}
	}

	return results, nil
}

func (ps *PQLToSQL) handleAggSQLCall(sql, index string) (pilosa.ValCount, error) {
	rows, err := ps.DB.Query(sql)
	panicOn(err)
	defer rows.Close()
	results := pilosa.ValCount{}
	var val int64
	var count int64
	for rows.Next() {
		err := rows.Scan(&val, &count)
		if err != nil {
			return pilosa.ValCount{}, err
		}
		results.Count = count
		results.Val = val
		break //shouldn't be anymore rows but being safe
	}
	err = rows.Err()
	if err != nil {
		return pilosa.ValCount{}, err
	}
	return results, nil
}
func (ps *PQLToSQL) handleBoolCall(sql, index string) (bool, error) {
	rows, err := ps.DB.Query(sql)
	panicOn(err)
	defer rows.Close()
	var val bool
	for rows.Next() {
		err := rows.Scan(&val)
		if err != nil {
			return false, err
		}
		break //shouldn't be anymore rows but being safe
	}
	err = rows.Err()
	if err != nil {
		return val, err
	}
	return val, nil
}

func (ps *PQLToSQL) readQuery(index string, query *pql.Query) (interface{}, error) {
	//Row(color=red)
	sql, resultType, optional, err := ToSql(query.Calls, index)
	panicOn(err)
	switch resultType {
	case BITMAP:
		return ps.handleBitmapCall(sql)
	case INT:
		return ps.handleIntCall(sql)
	case SIGNEDROW:
		return ps.handleSignedRowCall(sql)
	case ARRAY:
		fieldName := optional[0].(string)
		info, ok := ps.i2f[index]
		if !ok {
			return nil, errors.New(fmt.Sprintf("index not in schema %v", index))
		}
		field, ok := info.Fields[fieldName]
		if !ok {
			return nil, errors.New(fmt.Sprintf("field not in schema %v", fieldName))
		}
		if field.Options.Keys {
			return ps.handleStringArrayCall(sql)
		} else {
			return ps.handleIntArrayCall(sql)
		}
	case GROUPCOUNT:
		return ps.handleGroupCountCall(sql, index, optional[0].(int))
	case PAIRSFIELD:
		return ps.handlePairsFieldCall(sql, index)
	case TABLE:
		return ps.handleTableCall(sql, index)
	case AGGSQL:
		return ps.handleAggSQLCall(sql, index)
	case BOOL:
		return ps.handleBoolCall(sql, index)
	default:
		vv("Unknown query Type %v", resultType)
	}
	return nil, nil
}

type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// on tables starting with ps.dbprefix will be returned.
func (ps *PQLToSQL) ListIndexFields() (tables []string) {
	for indexName, index := range ps.i2f {
		for fieldname, _ := range index.Fields {

			tables = append(tables, indexName+"/"+fieldname)
		}
	}
	sort.Strings(tables)
	return tables
}
