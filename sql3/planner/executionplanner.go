// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"golang.org/x/sync/errgroup"
)

func isDatabaseNotFoundError(err error) bool {
	return errors.Is(err, dax.ErrDatabaseNameDoesNotExist)
}

func isTableNotFoundError(err error) bool {
	return errors.Is(err, dax.ErrTableNameDoesNotExist)
}

// ExecutionPlanner compiles SQL text into a query plan
type ExecutionPlanner struct {
	executor       pilosa.Executor
	schemaAPI      pilosa.SchemaAPI
	systemAPI      pilosa.SystemAPI
	systemLayerAPI pilosa.SystemLayerAPI
	importer       pilosa.Importer
	logger         logger.Logger
	sql            string
}

func NewExecutionPlanner(executor pilosa.Executor, schemaAPI pilosa.SchemaAPI, systemAPI pilosa.SystemAPI, systemLayerAPI pilosa.SystemLayerAPI, importer pilosa.Importer, logger logger.Logger, sql string) *ExecutionPlanner {
	return &ExecutionPlanner{
		executor:       executor,
		schemaAPI:      newSystemTableDefinitionsWrapper(schemaAPI),
		systemAPI:      systemAPI,
		systemLayerAPI: systemLayerAPI,
		importer:       importer,
		logger:         logger,
		sql:            sql,
	}
}

// CompilePlan takes an AST (parser.Statement) and compiles into a query plan returning the root
// PlanOperator
// The act of compiling includes an analysis step that does semantic analysis of the AST, this includes
// type checking, and sometimes AST rewriting. The compile phase uses the type-checked and rewritten AST
// to produce a query plan.
func (p *ExecutionPlanner) CompilePlan(ctx context.Context, stmt parser.Statement) (types.PlanOperator, error) {
	// call analyze first
	err := p.analyzePlan(ctx, stmt)
	if err != nil {
		return nil, err
	}

	var rootOperator types.PlanOperator
	switch stmt := stmt.(type) {
	case *parser.SelectStatement:
		rootOperator, err = p.compileSelectStatement(stmt, false)
	case *parser.ShowDatabasesStatement:
		rootOperator, err = p.compileShowDatabasesStatement(ctx, stmt)
	case *parser.CopyStatement:
		rootOperator, err = p.compileCopyStatement(stmt)
	case *parser.PredictStatement:
		rootOperator, err = p.compilePredictStatement(ctx, stmt)
	case *parser.ShowTablesStatement:
		rootOperator, err = p.compileShowTablesStatement(ctx, stmt)
	case *parser.ShowColumnsStatement:
		rootOperator, err = p.compileShowColumnsStatement(ctx, stmt)
	case *parser.ShowCreateTableStatement:
		rootOperator, err = p.compileShowCreateTableStatement(ctx, stmt)
	case *parser.CreateDatabaseStatement:
		rootOperator, err = p.compileCreateDatabaseStatement(stmt)
	case *parser.CreateTableStatement:
		rootOperator, err = p.compileCreateTableStatement(ctx, stmt)
	case *parser.CreateViewStatement:
		rootOperator, err = p.compileCreateViewStatement(stmt)
	case *parser.AlterDatabaseStatement:
		rootOperator, err = p.compileAlterDatabaseStatement(ctx, stmt)
	case *parser.AlterTableStatement:
		rootOperator, err = p.compileAlterTableStatement(ctx, stmt)
	case *parser.AlterViewStatement:
		rootOperator, err = p.compileAlterViewStatement(stmt)
	case *parser.DropDatabaseStatement:
		rootOperator, err = p.compileDropDatabaseStatement(ctx, stmt)
	case *parser.DropTableStatement:
		rootOperator, err = p.compileDropTableStatement(ctx, stmt)
	case *parser.DropViewStatement:
		rootOperator, err = p.compileDropViewStatement(ctx, stmt)
	case *parser.DropModelStatement:
		rootOperator, err = p.compileDropModelStatement(stmt)
	case *parser.InsertStatement:
		rootOperator, err = p.compileInsertStatement(ctx, stmt)
	case *parser.BulkInsertStatement:
		rootOperator, err = p.compileBulkInsertStatement(ctx, stmt)
	case *parser.DeleteStatement:
		rootOperator, err = p.compileDeleteStatement(stmt)
	case *parser.CreateModelStatement:
		rootOperator, err = p.compileCreateModelStatement(stmt)
	case *parser.CreateFunctionStatement:
		rootOperator, err = p.compileCreateFunctionStatement(stmt)

	default:
		return nil, sql3.NewErrInternalf("cannot plan statement: %T", stmt)
	}
	// optimize the plan
	if err == nil {
		rootOperator, err = p.optimizePlan(ctx, rootOperator)
	}
	return rootOperator, err
}

func (p *ExecutionPlanner) RehydratePlanOp(ctx context.Context, reader io.Reader) (types.PlanOperator, error) {
	rdr := newWireProtocolParser(p, reader)
	message, err := rdr.nextMessage()
	if err != nil {
		return nil, err
	}
	switch m := message.(type) {
	case *messagePlanOp:
		return m.op, nil
	default:
		return nil, sql3.NewErrInternalf("unexpected message type '%T'", message)
	}
}

func (p *ExecutionPlanner) analyzePlan(ctx context.Context, stmt parser.Statement) error {
	switch stmt := stmt.(type) {
	case *parser.SelectStatement:
		_, err := p.analyzeSelectStatement(ctx, stmt)
		return err
	case *parser.ShowDatabasesStatement:
		return nil
	case *parser.CopyStatement:
		return p.analyzeCopyStatement(ctx, stmt)
	case *parser.PredictStatement:
		return p.analyzePredictStatement(ctx, stmt)
	case *parser.ShowTablesStatement:
		return nil
	case *parser.ShowColumnsStatement:
		return nil
	case *parser.ShowCreateTableStatement:
		return nil
	case *parser.CreateDatabaseStatement:
		return p.analyzeCreateDatabaseStatement(stmt)
	case *parser.CreateTableStatement:
		return p.analyzeCreateTableStatement(stmt)
	case *parser.CreateViewStatement:
		return p.analyzeCreateViewStatement(ctx, stmt)
	case *parser.AlterDatabaseStatement:
		return p.analyzeAlterDatabaseStatement(stmt)
	case *parser.AlterTableStatement:
		return p.analyzeAlterTableStatement(stmt)
	case *parser.AlterViewStatement:
		return p.analyzeAlterViewStatement(ctx, stmt)
	case *parser.DropDatabaseStatement:
		return nil
	case *parser.DropTableStatement:
		return nil
	case *parser.DropViewStatement:
		return nil
	case *parser.DropModelStatement:
		return nil
	case *parser.InsertStatement:
		return p.analyzeInsertStatement(ctx, stmt)
	case *parser.BulkInsertStatement:
		return p.analyzeBulkInsertStatement(ctx, stmt)
	case *parser.DeleteStatement:
		return p.analyzeDeleteStatement(ctx, stmt)
	case *parser.CreateModelStatement:
		return p.analyzeCreateModelStatement(ctx, stmt)
	case *parser.CreateFunctionStatement:
		return p.analyzeCreateFunctionStatement(stmt)

	default:
		return sql3.NewErrInternalf("cannot analyze statement: %T", stmt)
	}
}

type accessType byte

const (
	accessTypeReadData accessType = iota
	accessTypeWriteData
	accessTypeCreateObject
	accessTypeAlterObject
	accessTypeDropObject
)

func (p *ExecutionPlanner) checkAccess(ctx context.Context, objectName string, _ accessType) error {
	return nil
}

type reduceFunc func(ctx context.Context, prev, v types.Rows) (types.Rows, error)

type mapResponse struct {
	node   pilosa.ClusterNode
	result types.Rows
	err    error
}

func (e *ExecutionPlanner) mapReducePlanOp(ctx context.Context, op types.PlanOperator, reduceFn reduceFunc) (result types.Rows, err error) {
	ch := make(chan mapResponse)

	// Wrap context with a cancel to kill goroutines on exit.
	ctx, cancel := context.WithCancel(ctx)
	// Create an errgroup so we can wait for all the goroutines to exit
	eg, ctx := errgroup.WithContext(ctx)

	// After we're done processing, we have to wait for any outstanding
	// functions in the ErrGroup to complete. If we didn't have an error
	// already at that point, we'll report any errors from the ErrGroup
	// instead.
	defer func() {
		cancel()
		errWait := eg.Wait()
		if err == nil {
			err = errWait
		}
	}()

	nodes := e.systemAPI.ClusterNodes()

	// Start mapping across all nodes
	if err = e.mapper(ctx, eg, ch, nodes, op, reduceFn); err != nil {
		return nil, errors.Wrap(err, "starting mapper")
	}

	// Iterate over all map responses and reduce.
	expected := len(nodes)
	done := ctx.Done()
	for expected > 0 {
		select {
		case <-done:
			return nil, ctx.Err()
		case resp := <-ch:
			if resp.err != nil {
				return nil, errors.Wrap(resp.err, "query fanout")
			}
			// if we got a response that we aren't discarding
			// because it's an error, subtract it from our count...
			expected -= 1

			// Reduce value.

			result, err = reduceFn(ctx, result, resp.result)
			if err != nil {
				cancel()
				return nil, err
			}
		}
	}
	// note the deferred Wait above which might override this nil.
	return result, nil
}

func (e *ExecutionPlanner) mapper(ctx context.Context, eg *errgroup.Group, ch chan mapResponse, nodes []pilosa.ClusterNode, op types.PlanOperator, reduceFn reduceFunc) (reterr error) {
	done := ctx.Done()
	// Execute each node in a separate goroutine.
	for _, node := range nodes {
		node := node
		eg.Go(func() error {

			resp := mapResponse{node: node}

			// Send local shards to mapper, otherwise remote exec.
			if node.ID == e.systemAPI.NodeID() {
				iter, err := op.Iterator(ctx, nil)
				if err != nil {
					resp.result = nil
					resp.err = err
				}
				row, err := iter.Next(ctx)
				if err != nil && err != types.ErrNoMoreRows {
					resp.result = nil
					resp.err = err
				}
				if err != types.ErrNoMoreRows {
					for {
						resp.result = append(resp.result, row)
						row, err = iter.Next(ctx)
						if err != nil && err != types.ErrNoMoreRows {
							resp.result = nil
							resp.err = err
						}
						if err == types.ErrNoMoreRows {
							break
						}
					}
				}
			} else {
				results, err := e.remotePlanExec(ctx, node.URI, op)
				resp.result = results
				resp.err = err
			}

			// Return response to the channel.
			select {
			case <-done:
				// If someone just canceled the context
				// arbitrarily, we could end up here with this
				// being the first non-nil error handed to
				// the ErrGroup, in which case, it's the best
				// explanation we have for why everything's
				// stopping.
				return ctx.Err()
			case ch <- resp:
				// If we return a non-nil error from this, the
				// entire errGroup gets canceled. So we don't
				// want to return a non-nil error if mapReduce
				// might try to run another mapper against a
				// different set of nodes. Note that this shouldn't
				// matter; we just sent the error to mapReduce
				// anyway, so it probably cancels the ErrGroup
				// too.
				if resp.err != nil {
					return resp.err
				}
			}
			return nil
		})
		if reterr != nil {
			return reterr // exit early if error occurs when running serially
		}
	}
	return nil
}

func (e *ExecutionPlanner) remotePlanExec(ctx context.Context, addr string, op types.PlanOperator) (types.Rows, error) {
	b, err := writeOp(op)
	if err != nil {
		return nil, err
	}

	// Create HTTP request.
	u := fmt.Sprintf("%s/sql-exec-graph", addr)
	req, err := http.NewRequest("POST", u, bytes.NewReader(b))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	// TODO (pok) internal auth
	//AddAuthToken(ctx, &req.Header)

	req.Header.Set("Content-Length", strconv.Itoa(len(b)))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("User-Agent", "pilosa/"+e.systemAPI.Version())

	// Execute request against the host.
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, sql3.NewErrInternalf("error posting internally: %s", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// we have an error
		return nil, sql3.NewErrInternalf("error posting internally: %d", resp.StatusCode)
	}

	var rows types.Rows
	parser := newWireProtocolParser(e, resp.Body)
	state := 1
	for state <= 2 {
		msg, err := parser.nextMessage()
		if err != nil {
			return nil, err
		}
		switch state {
		case 1:
			switch m := msg.(type) {
			case *messageSchemaInfo:
				parser.schema = m.schema
				state = 2

			case *messageError:
				return nil, m.err

			default:
				return nil, sql3.NewErrInternalf("unexpected token %d", msg.Token())
			}

		case 2:
			switch m := msg.(type) {
			case *messageRow:
				rows = append(rows, m.row)

			case *messageDone:
				// we're done
				state = 3

			case *messageError:
				return nil, m.err

			default:
				return nil, sql3.NewErrInternalf("unexpected token %d", msg.Token())
			}

		}

	}
	return rows, nil
}
