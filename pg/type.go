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

package pg

import "github.com/molecula/featurebase/v2/pg/message"

// Type represents a postgres type.
type Type struct {
	// I am not entirely sure what should be in here long term.
	// For now, I am just going to leave it like this.
	id int32
}

// TypeCharoid is a postgres type for text.
var TypeCharoid = Type{id: 18}

// TypeData is a type containing raw postgres wire type information.
type TypeData struct {
	TypeID       int32
	TypeLen      int16
	TypeModifier int32
}

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
	return message.ColumnDescription{
		TypeID:       t.id, // just charoid for now; as far as I can tell most implementations do not really use this
		TypeLen:      -1,   // vdsm had 4. . . but the spec says this should be negative
		TypeModifier: -1,
		Mode:         0, // send as text
	}, nil
}
