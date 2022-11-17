package batch

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto" //nolint:staticcheck
	featurebase "github.com/featurebasedb/featurebase/v3"
	featurebaseproto "github.com/featurebasedb/featurebase/v3/encoding/proto"
	"github.com/featurebasedb/featurebase/v3/pb"
	"github.com/featurebasedb/featurebase/v3/roaring"
	"github.com/pkg/errors"
)

type Importer interface {
	StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*featurebase.Transaction, error)
	FinishTransaction(ctx context.Context, id string) (*featurebase.Transaction, error)
	CreateIndexKeys(ctx context.Context, idx *featurebase.IndexInfo, keys ...string) (map[string]uint64, error)
	CreateFieldKeys(ctx context.Context, index string, field *featurebase.FieldInfo, keys ...string) (map[string]uint64, error)
	ImportRoaringBitmap(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, views map[string]*roaring.Bitmap, clear bool) error
	ImportRoaringShard(ctx context.Context, index string, shard uint64, request *featurebase.ImportRoaringShardRequest) error
	EncodeImportValues(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error)
	EncodeImport(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error)
	DoImport(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, path string, data []byte) error

	StatsTiming(name string, value time.Duration, rate float64)
}

// Ensure type implements interface.
var _ Importer = &nopImporter{}

// NopImporter is an implementation of the Importer interface that doesn't do
// anything.
var NopImporter Importer = &nopImporter{}

type nopImporter struct{}

func (n *nopImporter) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*featurebase.Transaction, error) {
	return nil, nil
}
func (n *nopImporter) FinishTransaction(ctx context.Context, id string) (*featurebase.Transaction, error) {
	return nil, nil
}
func (n *nopImporter) CreateIndexKeys(ctx context.Context, idx *featurebase.IndexInfo, keys ...string) (map[string]uint64, error) {
	return nil, nil
}
func (n *nopImporter) CreateFieldKeys(ctx context.Context, index string, field *featurebase.FieldInfo, keys ...string) (map[string]uint64, error) {
	return nil, nil
}
func (n *nopImporter) ImportRoaringBitmap(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, views map[string]*roaring.Bitmap, clear bool) error {
	return nil
}
func (n *nopImporter) ImportRoaringShard(ctx context.Context, index string, shard uint64, request *featurebase.ImportRoaringShardRequest) error {
	return nil
}
func (n *nopImporter) EncodeImportValues(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error) {
	return "", nil, nil
}
func (n *nopImporter) EncodeImport(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error) {
	return "", nil, nil
}
func (n *nopImporter) DoImport(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, path string, data []byte) error {
	return nil
}

func (n *nopImporter) StatsTiming(name string, value time.Duration, rate float64) {}

// Ensure type implements interface.
var _ Importer = &FeaturebaseImporter{}

// FeaturebaseImporter is a wrapper around featurebase.API, making it a
// batch.Importer.
type FeaturebaseImporter struct {
	*featurebase.API
}

func (f *FeaturebaseImporter) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*featurebase.Transaction, error) {
	return f.API.StartTransaction(ctx, id, timeout, exclusive, false)
}

func (f *FeaturebaseImporter) FinishTransaction(ctx context.Context, id string) (*featurebase.Transaction, error) {
	return f.API.FinishTransaction(ctx, id, false)
}

func (f *FeaturebaseImporter) CreateIndexKeys(ctx context.Context, idx *featurebase.IndexInfo, keys ...string) (map[string]uint64, error) {
	return f.API.CreateIndexKeys(ctx, idx.Name, keys...)
}

func (f *FeaturebaseImporter) CreateFieldKeys(ctx context.Context, index string, field *featurebase.FieldInfo, keys ...string) (map[string]uint64, error) {
	return f.API.CreateFieldKeys(ctx, index, field.Name, keys...)
}

func (f *FeaturebaseImporter) ImportRoaringBitmap(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, views map[string]*roaring.Bitmap, clear bool) error {
	vs := make(map[string][]byte)
	for k, v := range views {
		data := roaring.BitmapsToRoaring([]*roaring.Bitmap{v})
		if len(data) > 0 {
			vs[k] = data
		}
	}
	req := &featurebase.ImportRoaringRequest{
		IndexCreatedAt: 0,
		FieldCreatedAt: field.CreatedAt,
		Clear:          clear,
		Views:          vs,
	}
	return f.API.ImportRoaring(ctx, index, field.Name, shard, false, req)
}

// ImportRoaringShard doesn't technically need to be implemented here, because
// the method on f.API has the same signature and already satisfies the
// interface. But we put this here to avoid possible confusion.
func (f *FeaturebaseImporter) ImportRoaringShard(ctx context.Context, index string, shard uint64, request *featurebase.ImportRoaringShardRequest) error {
	return f.API.ImportRoaringShard(ctx, index, shard, request)
}

// EncodeImportValues is kind of weird. We're trying to mimic what the client
// does here (because the Importer interface was originally based off of the
// client methods). So we end up generating a protobuf-encode byte slice. And we
// don't really use path.
func (f *FeaturebaseImporter) EncodeImportValues(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error) {
	msg := &pb.ImportValueRequest{
		Index:          index,
		IndexCreatedAt: 0,
		Field:          field.Name,
		FieldCreatedAt: field.CreatedAt,
		Shard:          shard,
		ColumnIDs:      ids,
		Values:         vals,
	}
	data, err = proto.Marshal(msg)
	if err != nil {
		return "", nil, errors.Wrap(err, "marshaling ImportValue to protobuf")
	}
	return "", data, nil
}

func (f *FeaturebaseImporter) EncodeImport(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error) {
	msg := &pb.ImportRequest{
		Index:          index,
		IndexCreatedAt: 0,
		Field:          field.Name,
		FieldCreatedAt: field.CreatedAt,
		Shard:          shard,
		RowIDs:         vals,
		ColumnIDs:      ids,
	}
	data, err = proto.Marshal(msg)
	if err != nil {
		return "", nil, errors.Wrap(err, "marshaling Import to protobuf")
	}
	return "", data, nil
}

func (f *FeaturebaseImporter) DoImport(ctx context.Context, index string, field *featurebase.FieldInfo, shard uint64, path string, data []byte) error {
	serializer := featurebaseproto.Serializer{}

	// Unmarshal request based on field type.
	switch field.Options.Type {
	case featurebase.FieldTypeInt, featurebase.FieldTypeDecimal, featurebase.FieldTypeTimestamp:
		// Marshal into request object.
		req := &featurebase.ImportValueRequest{}
		if err := serializer.Unmarshal(data, req); err != nil {
			return errors.Wrap(err, "unmarshaling import value request")
		}

		qcx := f.API.Txf().NewQcx()
		defer qcx.Abort()

		opts := []featurebase.ImportOption{
			featurebase.OptImportOptionsClear(req.Clear),
		}

		if err := f.API.ImportValue(ctx, qcx, req, opts...); err != nil {
			return errors.Wrap(err, "importing import value request")
		}

		if err := qcx.Finish(); err != nil {
			return errors.Wrap(err, "finishing qcx")
		}

	default:
		// Marshal into request object.
		req := &featurebase.ImportRequest{}
		if err := serializer.Unmarshal(data, req); err != nil {
			return errors.Wrap(err, "unmarshaling import request")
		}

		qcx := f.API.Txf().NewQcx()
		defer qcx.Abort()

		opts := []featurebase.ImportOption{
			featurebase.OptImportOptionsClear(req.Clear),
		}
		if len(req.RowIDs) > 0 {
			opts = append(opts, featurebase.OptImportOptionsIgnoreKeyCheck(true))
		}

		if err := f.API.Import(ctx, qcx, req, opts...); err != nil {
			return errors.Wrap(err, "importing import request")
		}

		if err := qcx.Finish(); err != nil {
			return errors.Wrap(err, "finishing qcx")
		}
	}

	return nil
}

func (f *FeaturebaseImporter) StatsTiming(name string, value time.Duration, rate float64) {}
