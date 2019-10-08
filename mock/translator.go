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

package mock

import (
	"context"
	"io"

	"github.com/pilosa/pilosa/v2"
)

var _ pilosa.TranslateStore = (*TranslateStore)(nil)

type TranslateStore struct {
	TranslateColumnsToUint64Func func(index string, values []string) ([]uint64, error)
	TranslateColumnToStringFunc  func(index string, values uint64) (string, error)
	TranslateRowsToUint64Func    func(index, field string, values []string) ([]uint64, error)
	TranslateRowToStringFunc     func(index, field string, values uint64) (string, error)
	ReaderFunc                   func(ctx context.Context, off int64) (io.ReadCloser, error)
}

func (s TranslateStore) TranslateColumnsToUint64(index string, values []string) ([]uint64, error) {
	return s.TranslateColumnsToUint64Func(index, values)
}

func (s TranslateStore) TranslateColumnToString(index string, values uint64) (string, error) {
	return s.TranslateColumnToStringFunc(index, values)
}

func (s TranslateStore) TranslateRowsToUint64(index, field string, values []string) ([]uint64, error) {
	return s.TranslateRowsToUint64Func(index, field, values)
}

func (s TranslateStore) TranslateRowToString(index, field string, value uint64) (string, error) {
	return s.TranslateRowToStringFunc(index, field, value)
}

func (s TranslateStore) Reader(ctx context.Context, off int64) (io.ReadCloser, error) {
	return s.ReaderFunc(ctx, off)
}
