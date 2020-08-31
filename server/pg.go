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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/logger"
	"github.com/pilosa/pilosa/v2/pg"
	"github.com/pilosa/pilosa/v2/pql"
	pb "github.com/pilosa/pilosa/v2/proto"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// PostgresServer provides a postgres endpoint on pilosa.
type PostgresServer struct {
	api    *pilosa.API
	logger logger.Logger
	eg     errgroup.Group
	s      pg.Server
	stop   context.CancelFunc
}

// NewPostgresServer creates a postgres server.
func NewPostgresServer(api *pilosa.API, logger logger.Logger, tls *tls.Config) *PostgresServer {
	return &PostgresServer{
		api:    api,
		logger: logger,
		s: pg.Server{
			QueryHandler:   NewPostgresHandler(api, logger),
			TypeEngine:     pg.PrimitiveTypeEngine{},
			StartupTimeout: 5 * time.Second,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxStartupSize: 8 * 1024 * 1024,
			Logger:         logger,
			TLSConfig:      tls,
		},
	}
}

// NewPostgresHandler creates a postgres query handler wrapping the pilosa API.
func NewPostgresHandler(api *pilosa.API, logger logger.Logger) pg.QueryHandler {
	return &queryDecodeHandler{
		child: &pilosaQueryHandler{
			api:    api,
			logger: logger,
		},
	}
}

// Start a postgres endpoint at the specified address.
func (s *PostgresServer) Start(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "creating listener")
	}

	s.logger.Printf("serving postgres wire protocol on %s", l.Addr())

	ctx, cancel := context.WithCancel(context.Background())
	s.stop = cancel

	s.eg.Go(func() error { return s.s.Serve(ctx, l) })

	return nil
}

func (s *PostgresServer) Close() error {
	if s == nil {
		return nil
	}

	if s.stop == nil {
		return nil
	}
	s.stop()

	s.logger.Printf("waiting for postgres connections to shut down")

	return s.eg.Wait()
}

type pgPQLQuery struct {
	index string
	query string
}

func (q pgPQLQuery) String() string {
	return fmt.Sprintf("[%s]%s", q.index, q.query)
}

func pgDecodePQL(str string) (q pg.Query, err error) {
	defer func() {
		err = errors.Wrap(err, "not a valid PQL-over-postgres query")
	}()

	if !strings.HasPrefix(str, "[") {
		return nil, errors.New("missing index specification")
	}
	idx := strings.IndexRune(str, ']')
	if idx == -1 {
		return nil, errors.New("unclosed bracket in index specification")
	}

	return pgPQLQuery{
		index: str[1:idx],
		query: str[idx+1:],
	}, nil
}

type pilosaQueryHandler struct {
	api    *pilosa.API
	logger logger.Logger
}

func pgWriteRow(w pg.QueryResultWriter, row *pilosa.Row) error {
	err := w.WriteHeader(pg.ColumnInfo{
		Name: "_id",
		Type: pg.TypeCharoid,
	})
	if err != nil {
		return errors.Wrap(err, "writing result header")
	}

	if row.Keys != nil {
		for _, k := range row.Keys {
			err = w.WriteRowText(k)
			if err != nil {
				return errors.Wrap(err, "writing key")
			}
		}
	} else {
		for _, col := range row.Columns() {
			err = w.WriteRowText(strconv.FormatUint(col, 10))
			if err != nil {
				return errors.Wrap(err, "writing column ID")
			}
		}
	}

	return nil
}

func pgWriteRows(w pg.QueryResultWriter, rows pilosa.RowIdentifiers) error {
	err := w.WriteHeader(pg.ColumnInfo{
		Name: rows.Field(),
		Type: pg.TypeCharoid,
	})
	if err != nil {
		return errors.Wrap(err, "writing result header")
	}

	if rows.Keys != nil {
		for _, k := range rows.Keys {
			err = w.WriteRowText(k)
			if err != nil {
				return errors.Wrap(err, "writing key")
			}
		}
	} else {
		for _, row := range rows.Rows {
			err = w.WriteRowText(strconv.FormatUint(row, 10))
			if err != nil {
				return errors.Wrap(err, "writing row ID")
			}
		}
	}

	return nil
}

func pgFormatVal(val interface{}) string {
	switch val := val.(type) {
	case bool:
		return strconv.FormatBool(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case string:
		return val
	case pql.Decimal:
		return val.String()
	default:
		data, _ := json.Marshal(val)
		return string(data)
	}
}

func pgWriteExtractedTable(w pg.QueryResultWriter, tbl pilosa.ExtractedTable) error {
	headers := make([]pg.ColumnInfo, len(tbl.Fields)+1)
	headers[0] = pg.ColumnInfo{
		Name: "_id",
		Type: pg.TypeCharoid,
	}
	dataHeaders := headers[1:]
	for i, f := range tbl.Fields {
		dataHeaders[i] = pg.ColumnInfo{
			Name: f.Name,
			Type: pg.TypeCharoid,
		}
	}
	err := w.WriteHeader(headers...)
	if err != nil {
		return errors.Wrap(err, "writing result header")
	}

	vals := make([]string, len(headers))
	dataVals := vals[1:]
	for _, col := range tbl.Columns {
		if col.Column.Keyed {
			vals[0] = col.Column.Key
		} else {
			vals[0] = strconv.FormatUint(col.Column.ID, 10)
		}
		for i, v := range col.Rows {
			dataVals[i] = pgFormatVal(v)
		}
		err = w.WriteRowText(vals...)
		if err != nil {
			return errors.Wrap(err, "writing result row")
		}
	}

	return nil
}

func pgWriteGroupCount(w pg.QueryResultWriter, counts []pilosa.GroupCount) error {
	if len(counts) == 0 {
		// Not enough information is available to construct the header.
		// This is a significant flaw in the data type.
		return nil
	}

	headers := make([]pg.ColumnInfo, len(counts[0].Group)+2)
	for i, g := range counts[0].Group {
		headers[i] = pg.ColumnInfo{
			Name: g.Field,
			Type: pg.TypeCharoid,
		}
	}
	headers[len(headers)-2] = pg.ColumnInfo{
		Name: "count",
		Type: pg.TypeCharoid,
	}
	headers[len(headers)-1] = pg.ColumnInfo{
		Name: "sum",
		Type: pg.TypeCharoid,
	}
	err := w.WriteHeader(headers...)
	if err != nil {
		return errors.Wrap(err, "writing result header")
	}

	vals := make([]string, len(headers))
	for _, gc := range counts {
		for j, g := range gc.Group {
			var v string
			switch {
			case g.Value != nil:
				v = strconv.FormatInt(*g.Value, 10)
			case g.RowKey != "":
				v = g.RowKey
			default:
				v = strconv.FormatUint(g.RowID, 10)
			}
			vals[j] = v
		}
		vals[len(vals)-2] = strconv.FormatUint(gc.Count, 10)
		vals[len(vals)-1] = strconv.FormatInt(gc.Sum, 10)

		err := w.WriteRowText(vals...)
		if err != nil {
			return errors.Wrap(err, "writing group count result")
		}
	}

	return nil
}

func pgWriteRowser(w pg.QueryResultWriter, result pb.ToRowser) error {
	var data []string
	return result.ToRows(func(row *pb.RowResponse) error {
		if data == nil {
			headers := make([]pg.ColumnInfo, len(row.Columns))
			for i, h := range row.Headers {
				headers[i] = pg.ColumnInfo{
					Name: h.Name,
					Type: pg.TypeCharoid,
				}
			}
			err := w.WriteHeader(headers...)
			if err != nil {
				return errors.Wrap(err, "writing headers")
			}

			data = make([]string, len(headers))
		}

		for i, col := range row.Columns {
			var v string
			switch col := col.ColumnVal.(type) {
			case nil:
				v = "null"
			case *pb.ColumnResponse_BoolVal:
				v = strconv.FormatBool(col.BoolVal)
			case *pb.ColumnResponse_DecimalVal:
				v = pql.Decimal{
					Value: col.DecimalVal.Value,
					Scale: col.DecimalVal.Scale,
				}.String()
			case *pb.ColumnResponse_Float64Val:
				v = strconv.FormatFloat(col.Float64Val, 'g', -1, 64)
			case *pb.ColumnResponse_Int64Val:
				v = strconv.FormatInt(col.Int64Val, 10)
			case *pb.ColumnResponse_Uint64Val:
				v = strconv.FormatUint(col.Uint64Val, 10)
			case *pb.ColumnResponse_StringVal:
				v = col.StringVal
			case *pb.ColumnResponse_StringArrayVal:
				data, _ := json.Marshal(col.StringArrayVal.Vals)
				v = string(data)
			case *pb.ColumnResponse_Uint64ArrayVal:
				data, _ := json.Marshal(col.Uint64ArrayVal.Vals)
				v = string(data)
			default:
				return errors.Errorf("unable to process value of type %T", col)
			}

			data[i] = v
		}

		return w.WriteRowText(data...)
	})
}

type clientRowser struct {
	pb.StreamClient
}

func (cr *clientRowser) ToRows(f func(*pb.RowResponse) error) error {
	for {
		resp, err := cr.StreamClient.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			return err
		}

		err = f(resp)
		if err != nil {
			return err
		}
	}
}

func pgWriteResult(w pg.QueryResultWriter, result interface{}) error {
	switch result := result.(type) {
	case *pilosa.Row:
		return pgWriteRow(w, result)
	case pilosa.RowIdentifiers:
		return pgWriteRows(w, result)
	case pilosa.ExtractedTable:
		return pgWriteExtractedTable(w, result)
	case []pilosa.GroupCount:
		return pgWriteGroupCount(w, result)
	case pb.ToRowser: // we should avoid protobuf where we can...
		return pgWriteRowser(w, result)
	case pb.StreamClient:
		return pgWriteRowser(w, &clientRowser{result})
	case uint64:
		err := w.WriteHeader(pg.ColumnInfo{
			Name: "count",
			Type: pg.TypeCharoid,
		})
		if err != nil {
			return errors.Wrap(err, "writing headers")
		}

		err = w.WriteRowText(strconv.FormatUint(result, 10))
		if err != nil {
			return errors.Wrap(err, "writing count")
		}

		return nil
	case int64:
		err := w.WriteHeader(pg.ColumnInfo{
			Name: "value",
			Type: pg.TypeCharoid,
		})
		if err != nil {
			return errors.Wrap(err, "writing headers")
		}

		err = w.WriteRowText(strconv.FormatInt(result, 10))
		if err != nil {
			return errors.Wrap(err, "writing count")
		}

		return nil
	case bool:
		err := w.WriteHeader(pg.ColumnInfo{
			Name: "result",
			Type: pg.TypeCharoid,
		})
		if err != nil {
			return errors.Wrap(err, "writing headers")
		}

		err = w.WriteRowText(strconv.FormatBool(result))
		if err != nil {
			return errors.Wrap(err, "writing count")
		}

		return nil
	default:
		return errors.Errorf("result type %T not yet supported", result)
	}
}

func (pqh *pilosaQueryHandler) HandleQuery(ctx context.Context, w pg.QueryResultWriter, q pg.Query) error {
	switch q := q.(type) {
	case pgPQLQuery:
		resp, err := pqh.api.Query(ctx, &pilosa.QueryRequest{
			Index: q.index,
			Query: q.query,
		})
		if err != nil {
			return errors.Wrap(err, "executing query")
		}
		if len(resp.Results) != 1 {
			return errors.Errorf("expected 1 query result but found %d", len(resp.Results))
		}
		return errors.Wrap(pgWriteResult(w, resp.Results[0]), "writing query result")

	case pg.SimpleQuery:
		resp, err := execSQL(ctx, pqh.api, pqh.logger, string(q))
		if err != nil {
			return errors.Wrap(err, "executing query")
		}
		return errors.Wrap(pgWriteResult(w, resp), "writing query result")

	default:
		return errors.Errorf("query type %T not yet supported (query: %s)", q, q)
	}
}

type queryDecodeHandler struct {
	child pg.QueryHandler
}

func (qdh *queryDecodeHandler) HandleQuery(ctx context.Context, w pg.QueryResultWriter, q pg.Query) error {
	switch qv := q.(type) {
	case pg.SimpleQuery:
		if strings.HasPrefix(string(qv), "[") {
			pqlQuery, err := pgDecodePQL(strings.TrimSuffix(string(qv), ";"))
			if err != nil {
				return errors.Wrap(err, "decoding query")
			}
			q = pqlQuery
		}
	}

	return qdh.child.HandleQuery(ctx, w, q)
}
