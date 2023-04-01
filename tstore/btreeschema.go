// Copyright 2023 Molecula Corp. All rights reserved.

package tstore

import (
	"bytes"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/wireprotocol"
	"github.com/pkg/errors"
)

func (b *BTree) resolveSchema(schema types.Schema) error {
	// get the last schema version
	schemaSchema := types.Schema{
		&types.PlannerColumn{
			ColumnName: "schema",
			Type:       parser.NewDataTypeVarbinary(16384),
		},
	}

	insertSchemaSchema := types.Schema{
		&types.PlannerColumn{
			ColumnName: "_id",
			Type:       parser.NewDataTypeID(),
		},
		&types.PlannerColumn{
			ColumnName: "schema",
			Type:       parser.NewDataTypeVarbinary(16384),
		},
	}

	schemaNode, err := b.fetchNode(b.schemaNode)
	if err != nil {
		return err
	}
	schemaNode.takeReadLatch()
	defer schemaNode.releaseAnyLatch()
	defer b.unpin(schemaNode)

	// get an iterator on the schema and get the last row
	zero := Int(0)
	maxSlot := Int(0xFFFF)
	iter, _, err := b.getReverseIterator(schemaNode, zero, maxSlot, schemaSchema)
	if err != nil {
		return err
	}
	sk, ss, err := iter.Next()
	if err != nil {
		return err
	}
	iter.Dispose()
	// if there isn't any schema, insert this one as the last
	if sk == nil {
		// make a tuple
		schemaBytes, err := wireprotocol.WriteSchema(schema)
		if err != nil {
			return err
		}

		tup := &BTreeTuple{
			TupleSchema: insertSchemaSchema,
			Tuple: types.Row{
				int64(1),
				schemaBytes,
			},
		}

		err = b.insert(HEADER_SLOT_SCHEMA_ROOT, schemaNode, tup, schemaSchema, 1)
		if err != nil {
			return err
		}
		b.schemaVersion = 1
		b.schema = schema
	} else {
		// if there is one, see if it is the same as this one, if it is not, insert this one as the latest
		latestVersion := int(sk.(Int)) // TODO(twg) 2023/04/01 THIS IS NOT CORRECT

		schemaBytes := ss.Tuple[0].([]byte)

		rdr := bytes.NewReader(schemaBytes)
		_, err = wireprotocol.ExpectToken(rdr, wireprotocol.TOKEN_SCHEMA_INFO)
		if err != nil {
			return err
		}

		s, err := wireprotocol.ReadSchema(rdr)
		if err != nil {
			return err
		}

		newSchema := false
		if len(s) != len(schema) {
			newSchema = true
		} else {
			for i, c := range s {
				if schema[i].ColumnName != c.ColumnName || schema[i].Type.BaseTypeName() != c.Type.BaseTypeName() {
					newSchema = true
					break
				}
			}
		}
		if newSchema {
			// TODO(pok) add another schema version
			return errors.Errorf("schema mismatch")
		}
		b.schemaVersion = latestVersion
		b.schema = schema
	}
	return nil
}
