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
	"fmt"

	"github.com/molecula/featurebase/v2"
	"github.com/pkg/errors"
)

var (
	ErrUnsupportedQuery = errors.New("unsupported query")
)

// TODO: what the difference between this and IDIndexColumn?
type KeyIndexColumn struct {
	Index *pilosa.Index
	text  string
	alias string
}

func NewKeyIndexColumn(index *pilosa.Index, alias string) *KeyIndexColumn {
	return &KeyIndexColumn{
		Index: index,
		text:  ColID,
		alias: alias,
	}
}

func (i *KeyIndexColumn) Source() string {
	return "" // TODO
}
func (i *KeyIndexColumn) Name() string {
	return i.text
}
func (i *KeyIndexColumn) Alias() string {
	if i.alias != "" {
		return i.alias
	}
	return i.Name()
}

type IDIndexColumn struct {
	Index *pilosa.Index
	text  string
	alias string
}

func NewIDIndexColumn(index *pilosa.Index, alias string) *IDIndexColumn {
	return &IDIndexColumn{
		Index: index,
		text:  ColID,
		alias: alias,
	}
}

func (i *IDIndexColumn) Source() string {
	return "" // TODO
}
func (i *IDIndexColumn) Name() string {
	return i.text
}
func (i *IDIndexColumn) Alias() string {
	if i.alias != "" {
		return i.alias
	}
	return i.Name()
}

type FieldColumn struct {
	Field *pilosa.Field
	text  string
	alias string
}

func NewFieldColumn(field *pilosa.Field, alias string) *FieldColumn {
	return &FieldColumn{
		Field: field,
		text:  field.Name(),
		alias: alias,
	}
}

func (f *FieldColumn) Source() string {
	return ""
}
func (f *FieldColumn) Name() string {
	return f.text
}
func (f *FieldColumn) Alias() string {
	if f.alias != "" {
		return f.alias
	}
	return f.Name()
}

type FuncColumn struct {
	Field    *pilosa.Field
	FuncName FuncName
	alias    string
}

func NewFuncColumn(funcName FuncName, field *pilosa.Field, alias string) *FuncColumn {
	return &FuncColumn{
		Field:    field,
		FuncName: funcName,
		alias:    alias,
	}
}

func (f *FuncColumn) Source() string {
	return string(f.FuncName)
}

func (f *FuncColumn) Name() string {
	fieldName := "*"
	if f.Field != nil {
		fieldName = f.Field.Name()
	}
	return fmt.Sprintf("%s(%s)", f.FuncName, fieldName)
}

func (f *FuncColumn) Alias() string {
	if f.alias != "" {
		return f.alias
	}
	return f.Name()
}
