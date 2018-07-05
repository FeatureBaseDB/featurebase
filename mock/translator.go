package mock

import (
	"context"
	"io"

	"github.com/pilosa/pilosa"
)

var _ pilosa.TranslateStore = (*TranslateStore)(nil)

type TranslateStore struct {
	translateColumnsToUint64Func func(index string, values []string) ([]uint64, error)
	translateColumnToStringFunc  func(index string, values uint64) (string, error)
	translateRowsToUint64Func    func(index, frame string, values []string) ([]uint64, error)
	TranslateRowToStringFunc     func(index, frame string, values uint64) (string, error)
	ReaderFunc                   func(ctx context.Context, off int64) (io.ReadCloser, error)
}

func (s TranslateStore) TranslateColumnsToUint64(index string, values []string) ([]uint64, error) {
	return s.translateColumnsToUint64Func(index, values)
}

func (s TranslateStore) TranslateColumnToString(index string, values uint64) (string, error) {
	return s.translateColumnToStringFunc(index, values)
}

func (s TranslateStore) TranslateRowsToUint64(index, frame string, values []string) ([]uint64, error) {
	return s.translateRowsToUint64Func(index, frame, values)
}

func (s TranslateStore) TranslateRowToString(index, frame string, value uint64) (string, error) {
	return s.TranslateRowToStringFunc(index, frame, value)
}

func (s TranslateStore) Reader(ctx context.Context, off int64) (io.ReadCloser, error) {
	return s.ReaderFunc(ctx, off)
}
