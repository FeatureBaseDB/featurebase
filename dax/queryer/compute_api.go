package queryer

import (
	"context"
	"time"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/controller/partitioner"
	"github.com/molecula/featurebase/v3/errors"
)

// Ensure type implements interface.
var _ featurebase.ComputeAPI = &qualifiedComputeAPI{}

type qualifiedComputeAPI struct {
	mds  MDS
	qual dax.TableQualifier
}

func NewQualifiedComputeAPI(qual dax.TableQualifier, mds MDS) *qualifiedComputeAPI {
	return &qualifiedComputeAPI{
		mds:  mds,
		qual: qual,
	}
}

// importer is used to get the Importer based on the provided address. We used
// to maintain a map of different importers (pointers to computers) running
// in-process, but since getting rid of that logic this method is currently just
// a wrapper around NewComputeImporter. I'm leaving it like this for now in case
// it makes sense for this to become a cache of computer clients.
func (c *qualifiedComputeAPI) importer(addr dax.Address) (Importer, error) {
	return NewComputeImporter(addr), nil
}

func (c *qualifiedComputeAPI) Import(ctx context.Context, qcx *featurebase.Qcx, req *featurebase.ImportRequest, opts ...featurebase.ImportOption) error {
	// If the request is empty, return early.
	if len(req.ColumnKeys) == 0 && len(req.ColumnIDs) == 0 {
		return nil
	}

	// Determine if the columns use string keys or not.
	var hasColKeys bool
	if len(req.ColumnKeys) > 0 {
		hasColKeys = true
		if len(req.ColumnIDs) > 0 {
			return errors.Errorf("import request has both column ids and keys")
		}
	}
	// Determine if the rows use string keys or not.
	var hasRowKeys bool
	if len(req.RowKeys) > 0 {
		hasRowKeys = true
		if len(req.RowIDs) > 0 {
			return errors.Errorf("import request has both row ids and keys")
		}
	}

	partitioner := partitioner.NewPartitioner()

	reqPerShard := make(map[uint64]*featurebase.ImportRequest)

	tkey, err := c.indexToQualifiedTableKey(ctx, req.Index)
	if err != nil {
		return errors.Wrap(err, "converting index to qualified table key")
	}
	stkey := string(tkey)

	qtid := tkey.QualifiedTableID()

	qtbl, err := c.mds.Table(ctx, qtid)
	if err != nil {
		return errors.Wrapf(err, "getting table for import: %s", req.Index)
	}

	// Translate column keys.
	if hasColKeys {
		// Get the partitions (and therefore, nodes) responsible for the keys.
		pMap := partitioner.PartitionsForKeys(qtbl.Key(), qtbl.PartitionN, req.ColumnKeys...)

		colIDs := make([]uint64, 0, len(req.ColumnKeys))
		for pNum := range pMap {
			addr, err := c.mds.IngestPartition(ctx, qtid, pNum)
			if err != nil {
				return errors.Wrapf(err, "getting ingest partition: %d", pNum)
			}

			importer, err := c.importer(addr)
			if err != nil {
				return errors.Wrapf(err, "getting importer for address: %s", addr)
			}

			colKeyMap, err := importer.CreateIndexKeys(ctx, stkey, req.ColumnKeys...)
			if err != nil {
				return errors.Wrap(err, "creating index keys")
			}
			for i := range req.ColumnKeys {
				colIDs = append(colIDs, colKeyMap[req.ColumnKeys[i]])
			}
		}
		req.ColumnIDs = colIDs
	}

	// Translate row keys.
	if hasRowKeys {
		addr, err := c.mds.IngestPartition(ctx, qtid, 0)
		if err != nil {
			return errors.Wrapf(err, "getting ingest partition: %d", 0)
		}

		importer, err := c.importer(addr)
		if err != nil {
			return errors.Wrapf(err, "getting importer for address: %s", addr)
		}

		rowKeyMap, err := importer.CreateFieldKeys(ctx, stkey, req.Field, req.RowKeys...)
		if err != nil {
			return errors.Wrap(err, "creating field keys")
		}
		rowIDs := make([]uint64, len(req.RowKeys))
		for i := range req.RowKeys {
			rowIDs[i] = rowKeyMap[req.RowKeys[i]]
		}
		req.RowIDs = rowIDs
	}

	// Loop over the column ids and split them up by shard.
	for ii := range req.ColumnIDs {
		// Determine shard.
		shard := req.ColumnIDs[ii] / featurebase.ShardWidth

		// Get or create the ImportRequest for this shard.
		shardedReq, found := reqPerShard[shard]
		if !found {
			shardedReq = &featurebase.ImportRequest{
				Index:          stkey,
				IndexCreatedAt: req.IndexCreatedAt,
				Field:          req.Field,
				FieldCreatedAt: req.FieldCreatedAt,
				Shard:          shard,
				RowIDs:         []uint64{},
				ColumnIDs:      []uint64{},
				RowKeys:        []string{},
				ColumnKeys:     []string{},
				Timestamps:     []int64{},
				Clear:          req.Clear,
			}
			reqPerShard[shard] = shardedReq
		}

		shardedReq.ColumnIDs = append(shardedReq.ColumnIDs, req.ColumnIDs[ii])
		if len(req.RowIDs) > 0 {
			shardedReq.RowIDs = append(shardedReq.RowIDs, req.RowIDs[ii])
		}
		if len(req.Timestamps) > 0 {
			shardedReq.Timestamps = append(shardedReq.Timestamps, req.Timestamps[ii])
		}
	}

	// Send each of the sharded ImportRequests to the appropriate compute node.
	for shard, req := range reqPerShard {
		addr, err := c.mds.IngestShard(ctx, qtid, dax.ShardNum(shard))
		if err != nil {
			return errors.Wrapf(err, "getting ingest shard: %d", shard)
		}

		importer, err := c.importer(addr)
		if err != nil {
			return errors.Wrapf(err, "getting importer for address: %s", addr)
		}

		importer.Import(ctx, req,
			featurebase.OptImportOptionsClear(req.Clear),
			featurebase.OptImportOptionsIgnoreKeyCheck(true),
		)
	}

	return nil
}

func (c *qualifiedComputeAPI) ImportValue(ctx context.Context, qcx *featurebase.Qcx, req *featurebase.ImportValueRequest, opts ...featurebase.ImportOption) error {
	// If the request is empty, return early.
	if len(req.ColumnKeys) == 0 && len(req.ColumnIDs) == 0 {
		return nil
	}

	// Determine if the columns use string keys or not.
	var hasColKeys bool
	if len(req.ColumnKeys) > 0 {
		hasColKeys = true
		if len(req.ColumnIDs) > 0 {
			return errors.Errorf("import value request has both column ids and keys")
		}
	}

	partitioner := partitioner.NewPartitioner()

	reqPerShard := make(map[uint64]*featurebase.ImportValueRequest)

	tkey, err := c.indexToQualifiedTableKey(ctx, req.Index)
	if err != nil {
		return errors.Wrap(err, "converting index to qualified table key")
	}
	stkey := string(tkey)

	qtid := tkey.QualifiedTableID()

	qtbl, err := c.mds.Table(ctx, qtid)
	if err != nil {
		return errors.Wrapf(err, "getting table for importvalue: %s", req.Index)
	}

	// Translate column keys.
	if hasColKeys {
		// Get the partitions (and therefore, nodes) responsible for the keys.
		pMap := partitioner.PartitionsForKeys(qtbl.Key(), qtbl.PartitionN, req.ColumnKeys...)

		colIDs := make([]uint64, 0, len(req.ColumnKeys))
		for pNum := range pMap {
			addr, err := c.mds.IngestPartition(ctx, qtid, pNum)
			if err != nil {
				return errors.Wrapf(err, "getting ingest partition: %d", pNum)
			}

			importer, err := c.importer(addr)
			if err != nil {
				return errors.Wrapf(err, "getting importer for address: %s", addr)
			}

			colKeyMap, err := importer.CreateIndexKeys(ctx, stkey, req.ColumnKeys...)
			if err != nil {
				return errors.Wrap(err, "creating index keys")
			}
			for i := range req.ColumnKeys {
				colIDs = append(colIDs, colKeyMap[req.ColumnKeys[i]])
			}
		}
		req.ColumnIDs = colIDs
	}

	// Loop over the column ids and split them up by shard.
	for ii := range req.ColumnIDs {
		// Determine shard.
		shard := req.ColumnIDs[ii] / featurebase.ShardWidth

		// Get or create the ImportRequest for this shard.
		shardedReq, found := reqPerShard[shard]
		if !found {
			shardedReq = &featurebase.ImportValueRequest{
				Index:           stkey,
				IndexCreatedAt:  req.IndexCreatedAt,
				Field:           req.Field,
				FieldCreatedAt:  req.FieldCreatedAt,
				Shard:           shard,
				ColumnIDs:       []uint64{},
				ColumnKeys:      []string{},
				Values:          []int64{},
				FloatValues:     []float64{},
				TimestampValues: []time.Time{},
				StringValues:    []string{},
				Clear:           req.Clear,
			}
			reqPerShard[shard] = shardedReq
		}

		shardedReq.ColumnIDs = append(shardedReq.ColumnIDs, req.ColumnIDs[ii])
		if len(req.Values) > 0 {
			shardedReq.Values = append(shardedReq.Values, req.Values[ii])
		}
		// TODO: The following would populate the other value types, but the
		// EncodeImportValues doesn't seem to use this data. So we need to track
		// down how this is being used.
		//
		// if len(req.FloatValues) > 0 {
		// 	shardedReq.FloatValues = append(shardedReq.FloatValues, req.FloatValues[ii])
		// }
		// if len(req.TimestampValues) > 0 {
		// 	shardedReq.TimestampValues = append(shardedReq.TimestampValues, req.TimestampValues[ii])
		// }
		// if len(req.StringValues) > 0 {
		// 	shardedReq.StringValues = append(shardedReq.StringValues, req.StringValues[ii])
		// }
	}

	// Send each of the sharded ImportValueRequests to the appropriate compute
	// node.
	for shard, req := range reqPerShard {
		addr, err := c.mds.IngestShard(ctx, qtid, dax.ShardNum(shard))
		if err != nil {
			return errors.Wrapf(err, "getting ingest shard: %d", shard)
		}

		importer, err := c.importer(addr)
		if err != nil {
			return errors.Wrapf(err, "getting importer for address: %s", addr)
		}

		importer.ImportValue(ctx, req,
			featurebase.OptImportOptionsClear(req.Clear),
			featurebase.OptImportOptionsIgnoreKeyCheck(true),
		)
	}

	return nil
}

func (c *qualifiedComputeAPI) Txf() *featurebase.TxFactory {
	return &featurebase.TxFactory{}
}

func (c *qualifiedComputeAPI) indexToQualifiedTableKey(ctx context.Context, index string) (dax.TableKey, error) {
	qtid, err := c.mds.TableID(ctx, c.qual, dax.TableName(index))
	if err != nil {
		return "", errors.Wrap(err, "converting index to qualified table id")
	}
	return qtid.Key(), nil
}
