// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pg

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/pg/message"
	"github.com/pkg/errors"
)

// Query is an interface to be implemented by queries.
type Query interface {
	fmt.Stringer
}

// SimpleQuery is a query sent as only a string.
// It has no parameters.
type SimpleQuery string

func (q SimpleQuery) String() string {
	return string(q)
}

// ColumnInfo contains metadata about a column.
type ColumnInfo struct {
	Name    string
	Type    Type
	TableID int32
	FieldID int16
}

// QueryResultWriter is used to write the results of a query back over the connection.
type QueryResultWriter interface {
	// WriteHeader sets the column header information.
	WriteHeader(...ColumnInfo) error

	// WriteRowText sends a row of data in textual format.
	WriteRowText(...string) error

	// Tag assigns a tag to the query.
	// This should be called before the query is completed.
	Tag(tag string)
}

// QueryHandler handles a query.
type QueryHandler interface {
	// HandleQuery executes a query and writes the results back.
	HandleQuery(context.Context, QueryResultWriter, Query) error
	HandleSchema(context.Context, *Portal) error
	Version() string
}

// queryResultWriter implements QueryResultWrtiter over postgres wire protocol.
// The underlying message writer must be flushed by the caller once the query has finished.
type queryResultWriter struct {
	w            message.Writer
	te           TypeEngine
	enc          message.Encoder
	width        int
	wroteHeaders bool
	tag          string
}

func (w *queryResultWriter) WriteHeader(info ...ColumnInfo) error {
	if w.wroteHeaders {
		return errors.New("double-write of query headers")
	}

	// Translate column information into a row description message.
	desc := make([]message.ColumnDescription, len(info))
	for i, c := range info {
		t, err := w.te.TranslateType(c.Type)
		if err != nil {
			return errors.Wrap(err, "translating column type")
		}
		t.Name = c.Name
		t.TableID = c.TableID
		t.FieldID = c.FieldID
		desc[i] = t
	}

	// Encode the row description.
	msg, err := w.enc.RowDescription(desc...)
	if err != nil {
		return errors.Wrap(err, "encoding query header")
	}

	w.wroteHeaders = true
	w.width = len(desc)

	// Write the row description.
	return w.w.WriteMessage(msg)
}

func (w *queryResultWriter) WriteRowText(text ...string) error {
	// Check preconditions of the call.
	switch {
	case !w.wroteHeaders:
		return errors.New("writing rows without headers")
	case len(text) != w.width:
		return errors.Errorf("expected %d columns but found %d", w.width, len(text))
	}

	// Encode the row data as text into a DataRow message.
	msg, err := w.enc.TextRow(text...)
	if err != nil {
		return err
	}

	// Write the data row over the network.
	return w.w.WriteMessage(msg)
}

func (w *queryResultWriter) Tag(tag string) {
	w.tag = tag
}

var _ QueryResultWriter = (*queryResultWriter)(nil)
