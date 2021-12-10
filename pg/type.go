// Copyright 2021 Molecula Corp. All rights reserved.
package pg

import "github.com/molecula/featurebase/v2/pg/message"

// Type represents a postgres type.
type Type struct {
	// I am not entirely sure what should be in here long term.
	// For now, I am just going to leave it like this.
	Id      int32
	Typelen int16
}

// TypeCharoid is a postgres type for text.
// found in postgres source src/include/catalog/pg_type.h
var TypeCharoid = Type{Id: 18, Typelen: -1}
var TypeNAMEOID = Type{Id: 19, Typelen: 64}
var TypeINT4OID = Type{Id: 23, Typelen: 4}
var TypeTEXTOID = Type{Id: 25, Typelen: -1}
var TypeFLOAT8OID = Type{Id: 701, Typelen: 8}

// TypeEngine is a system for managing types.
// This is necessary for compound types like arrays which need ID generation.
type TypeEngine interface {
	// TranslateType populates a column description with type information.
	TranslateType(Type) (message.ColumnDescription, error)
}

// PrimitiveTypeEngine is a simple type engine that only works on primitive types.
type PrimitiveTypeEngine struct{}

// TranslateType translates a type to a column description.
func (pte PrimitiveTypeEngine) TranslateType(t Type) (message.ColumnDescription, error) {
	var TypeLen int16
	var TypeID int32
	switch t {
	case TypeCharoid:
		TypeID = TypeCharoid.Id
		TypeLen = TypeCharoid.Typelen
	case TypeNAMEOID:
		TypeID = TypeNAMEOID.Id
		TypeLen = TypeNAMEOID.Typelen
	case TypeINT4OID:
		TypeID = TypeINT4OID.Id
		TypeLen = TypeINT4OID.Typelen
	case TypeTEXTOID:
		TypeID = TypeTEXTOID.Id
		TypeLen = TypeTEXTOID.Typelen
	case TypeFLOAT8OID:
		TypeID = TypeFLOAT8OID.Id
		TypeLen = TypeFLOAT8OID.Typelen
	default: // treat like TypeCharoid:
		TypeID = TypeCharoid.Id
		TypeLen = TypeCharoid.Typelen
	}
	return message.ColumnDescription{
		TypeID:       TypeID,
		TypeLen:      TypeLen,
		TypeModifier: -1,
		Mode:         0, // send as text
	}, nil
}
