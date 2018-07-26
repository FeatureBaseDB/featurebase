package mock

import (
	"context"
	"io"

	"github.com/pilosa/pilosa"
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
