package client

import (
	"time"

	"github.com/molecula/featurebase/v2/logger"
	"github.com/pkg/errors"
)

// NewIngestAPIBatch creates an alternate implementation of
// RecordBatch which exists to aid in testing the new Ingest API and
// is likely far slower than the Batch.
func NewIngestAPIBatch(client *Client, size int, logger logger.Logger, fields []*Field) *ingestAPIBatch {
	if len(fields) == 0 {
		return nil
	}

	return &ingestAPIBatch{
		client:    client,
		log:       logger,
		fields:    fields,
		keyed:     fields[0].index.Opts().Keys(),
		index:     fields[0].index.Name(),
		batchSize: size,

		recordsK: make(map[string]map[string]interface{}),
		records:  make(map[uint64]map[string]interface{}),
	}
}

type ingestAPIBatch struct {
	client    *Client
	log       logger.Logger
	batchSize int

	fields []*Field
	keyed  bool
	index  string

	// map[recordKey][fieldName]value
	recordsK map[string]map[string]interface{}
	records  map[uint64]map[string]interface{}
}

func (b *ingestAPIBatch) Add(row Row) error {
	if len(row.Clears) > 0 {
		return errors.New("ingest api batch does not support clears")
	}
	values := make(map[string]interface{})
	for i, val := range row.Values {
		field := b.fields[i]
		// val can be string, uint64, int64, []string, []uint64, nil
		// TODO how are null values handled by ingest API? cc @seebs. seems like not well... just don't include a key if null
		// TODO timestamp field might need special handling
		// TODO check that the Row.Clears field is only used for packed bools, and then issue a warning/error (in IDK) if the ingest API mode is used in conjunction w/ packed bools.
		if val == nil {
			continue
		}
		zero := QuantizedTime{}
		if field.Options().Type() == FieldTypeTime && row.Time != zero {
			timeq, err := row.Time.Time()
			if err != nil {
				return errors.Wrap(err, "parsing row time")
			}
			values[field.Name()] = map[string]interface{}{"time": timeq.Format(time.RFC3339), "values": val}
		} else {
			values[field.Name()] = val
		}
	}

	if b.keyed {
		switch rowID := row.ID.(type) {
		case string:
			b.recordsK[rowID] = values
		case []byte:
			b.recordsK[string(rowID)] = values
		default:
			return errors.Errorf("unsupported rowID %v of type %[1]T, must be string, or []byte for keyed index", rowID)
		}
		if len(b.recordsK) >= b.batchSize {
			return ErrBatchNowFull
		}
	} else {
		rowID, ok := row.ID.(uint64)
		if !ok {
			return errors.Errorf("unsupported rowID %v of type %[1]T, must be uint64 for unkeyed index", row.ID)
		}
		b.records[rowID] = values
		if len(b.records) >= b.batchSize {
			return ErrBatchNowFull
		}
	}
	return nil
}

func (b *ingestAPIBatch) Import() error {
	// TODO
	if b.keyed {
		return b.importKeyed()
	}
	return b.importUnkeyed()
}

func (b *ingestAPIBatch) importKeyed() error {
	req := []map[string]interface{}{
		{
			"action":  "set",
			"records": b.recordsK,
		},
	}
	bod, err := b.client.IngestData(b.index, req)
	if err != nil {
		return errors.Wrapf(err, "importKeyed, body: %s", bod)
	}

	for k := range b.recordsK {
		delete(b.recordsK, k)
	}
	return nil
}

func (b *ingestAPIBatch) importUnkeyed() error {
	req := []map[string]interface{}{
		{
			"action":  "set",
			"records": b.records,
		},
	}
	bod, err := b.client.IngestData(b.index, req)
	if err != nil {
		return errors.Wrapf(err, "importKeyed, body: %s", bod)
	}

	for v := range b.records {
		delete(b.records, v)
	}
	return nil
}

func (b *ingestAPIBatch) Len() int {
	if b.keyed {
		return len(b.recordsK)
	}
	return len(b.records)
}

func (b *ingestAPIBatch) Flush() error { return nil }
