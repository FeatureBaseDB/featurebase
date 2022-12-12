package queryer

import (
	"context"
	"net/http"

	pilosa "github.com/molecula/featurebase/v3"
	featurebase_client "github.com/molecula/featurebase/v3/client"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/controller/partitioner"
	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/encoding/proto"
	"github.com/molecula/featurebase/v3/errors"
)

// Ensure type implements interface.
var _ Translator = (*mdsTranslator)(nil)

type mdsTranslator struct {
	noder   dax.Noder
	schemar dax.Schemar
}

func NewMDSTranslator(noder dax.Noder, schemar dax.Schemar) *mdsTranslator {
	return &mdsTranslator{
		noder:   noder,
		schemar: schemar,
	}
}

func fbClient(address dax.Address) (*featurebase_client.Client, error) {
	// Set up a FeatureBase client with address.
	return featurebase_client.NewClient(address.HostPort(),
		featurebase_client.OptClientRetries(2),
		featurebase_client.OptClientTotalPoolSize(1000),
		featurebase_client.OptClientPoolSizePerRoute(400),
		featurebase_client.OptClientPathPrefix(address.Path()),
		//featurebase_client.OptClientStatsClient(m.stats),
	)
}

func (m *mdsTranslator) CreateIndexKeys(ctx context.Context, table string, keys []string) (map[string]uint64, error) {
	tkey := dax.TableKey(table)
	qtid := tkey.QualifiedTableID()

	qtbl, err := m.schemar.TableByID(ctx, qtid)
	if err != nil {
		return nil, errors.Wrap(err, "getting table")
	}

	partitioner := partitioner.NewPartitioner()

	// Get the partitions (and therefore, nodes) responsible for the keys.
	pMap := partitioner.PartitionsForKeys(tkey, qtbl.PartitionN, keys...)

	out := make(map[string]uint64)
	for pNum := range pMap {
		address, err := m.noder.IngestPartition(ctx, qtid, pNum)
		if err != nil {
			return nil, errors.Wrapf(err, "calling ingest-partition on table: %s, partition: %d", table, pNum)
		}

		fbClient, err := fbClient(address)
		if err != nil {
			return nil, errors.Wrap(err, "getting featurebase client")
		}

		idx := featurebase_client.NewIndex(table)

		m, err := fbClient.CreateIndexKeys(idx, pMap[pNum]...)
		if err != nil {
			return nil, errors.Wrapf(err, "creating index keys on index: %s, partition: %d", table, pNum)
		}

		for k, v := range m {
			out[k] = v
		}
	}

	return out, nil
}

func (m *mdsTranslator) CreateFieldKeys(ctx context.Context, table string, field string, keys []string) (map[string]uint64, error) {
	qtid := dax.TableKey(table).QualifiedTableID()
	address, err := m.noder.IngestPartition(ctx, qtid, dax.PartitionNum(0))
	if err != nil {
		return nil, errors.Wrapf(err, "calling ingest-partition on table: %s, partition: %d", table, dax.PartitionNum(0))
	}

	fbClient, err := fbClient(address)
	if err != nil {
		return nil, errors.Wrap(err, "getting featurebase client")
	}

	idx := featurebase_client.NewIndex(table)
	fld := idx.Field(field)

	return fbClient.CreateFieldKeys(fld, keys...)
}

func (m *mdsTranslator) FindIndexKeys(ctx context.Context, table string, keys []string) (map[string]uint64, error) {
	tkey := dax.TableKey(table)
	qtid := tkey.QualifiedTableID()

	qtbl, err := m.schemar.TableByID(ctx, qtid)
	if err != nil {
		return nil, errors.Wrap(err, "getting table")
	}

	partitioner := partitioner.NewPartitioner()

	// Get the partitions (and therefore, nodes) responsible for the keys.
	pMap := partitioner.PartitionsForKeys(tkey, qtbl.PartitionN, keys...)

	pNums := make([]dax.PartitionNum, 0, len(pMap))
	for k := range pMap {
		pNums = append(pNums, k)
	}

	translateNodes, err := m.noder.TranslateNodes(ctx, qtid, pNums...)
	if err != nil {
		return nil, errors.Wrapf(err, "getting translate nodes for partitions on table: %s", table)
	}

	out := make(map[string]uint64)
	for _, tnode := range translateNodes {
		address := tnode.Address

		fbClient, err := fbClient(address)
		if err != nil {
			return nil, errors.Wrap(err, "getting featurebase client")
		}

		idx := featurebase_client.NewIndex(table)

		nodeKeys := []string{}
		for _, pNum := range tnode.Partitions {
			nodeKeys = append(nodeKeys, pMap[pNum]...)
		}

		m, err := fbClient.FindIndexKeys(idx, nodeKeys...)
		if err != nil {
			return nil, errors.Wrapf(err, "finding index keys on index: %s", table)
		}

		for k, v := range m {
			out[k] = v
		}
	}

	return out, nil
}

func (m *mdsTranslator) FindFieldKeys(ctx context.Context, table, field string, keys []string) (map[string]uint64, error) {
	qtid := dax.TableKey(table).QualifiedTableID()
	address, err := m.noder.IngestPartition(ctx, qtid, dax.PartitionNum(0))
	if err != nil {
		return nil, errors.Wrapf(err, "calling ingest-partition on table: %s, partition: %d", table, dax.PartitionNum(0))
	}

	fbClient, err := fbClient(address)
	if err != nil {
		return nil, errors.Wrap(err, "getting featurebase client")
	}

	idx := featurebase_client.NewIndex(table)
	fld := idx.Field(field)

	return fbClient.FindFieldKeys(fld, keys...)
}

func (m *mdsTranslator) TranslateIndexIDs(ctx context.Context, index string, ids []uint64) ([]string, error) {
	idsByPartition := splitIDsByPartition(index, ids, 1<<20) // TODO(jaffee), don't hardcode shardwidth...need to get this from index info
	daxPartitions := make([]dax.PartitionNum, 0)
	for partition := range idsByPartition {
		daxPartitions = append(daxPartitions, partition)
	}

	qtid := dax.TableKey(index).QualifiedTableID()

	nodes, err := m.noder.TranslateNodes(ctx, qtid, daxPartitions...)
	if err != nil {
		return nil, errors.Wrapf(err, "calling translate-nodes on table: %s, partitions: %v", index, daxPartitions)
	}

	// get translation from each node
	idToKey := make(map[uint64]string)
	for _, node := range nodes {
		fbClient, err := fbClient(node.Address)
		if err != nil {
			return nil, errors.Wrap(err, "getting featurebase client")
		}

		reqIDs := make([]uint64, 0)
		for _, partition := range node.Partitions {
			reqIDs = append(reqIDs, idsByPartition[partition]...)
		}

		strings, err := makeTranslateIDsRequest(fbClient, index, "", reqIDs)
		if err != nil {
			return nil, errors.Wrapf(err, "translating on %v", node.Address)
		}
		for i, s := range strings {
			idToKey[reqIDs[i]] = s
		}
	}

	ret := make([]string, len(ids))
	for i, id := range ids {
		ret[i] = idToKey[id]
	}
	return ret, nil
}

func (m *mdsTranslator) TranslateIndexIDSet(ctx context.Context, table string, ids map[uint64]struct{}) (map[uint64]string, error) {
	idList := make([]uint64, 0, len(ids))
	for id := range ids {
		idList = append(idList, id)
	}

	stringList, err := m.TranslateIndexIDs(ctx, table, idList)
	if err != nil {
		return nil, errors.Wrapf(err, "translating index ids on table: %s", table)
	}

	ret := make(map[uint64]string)
	for i, id := range idList {
		ret[id] = stringList[i]
	}
	return ret, nil
}
func (m *mdsTranslator) TranslateFieldIDs(ctx context.Context, table, field string, ids map[uint64]struct{}) (map[uint64]string, error) {
	idList := make([]uint64, 0, len(ids))
	for id := range ids {
		idList = append(idList, id)
	}

	stringList, err := m.TranslateFieldListIDs(ctx, table, field, idList)
	if err != nil {
		return nil, errors.Wrapf(err, "translating field ids on field: %s, %s", table, field)
	}

	ret := make(map[uint64]string)
	for i, id := range idList {
		ret[id] = stringList[i]
	}
	return ret, nil
}
func (m *mdsTranslator) TranslateFieldListIDs(ctx context.Context, index, field string, ids []uint64) ([]string, error) {
	qtid := dax.TableKey(index).QualifiedTableID()
	address, err := m.noder.IngestPartition(ctx, qtid, dax.PartitionNum(0))
	if err != nil {
		return nil, errors.Wrapf(err, "calling ingest-partition on table: %s, partition: %d", index, dax.PartitionNum(0))
	}

	fbClient, err := fbClient(address)
	if err != nil {
		return nil, errors.Wrap(err, "getting featurebase client")
	}

	return makeTranslateIDsRequest(fbClient, index, field, ids)
}

func makeTranslateIDsRequest(fbClient *featurebase_client.Client, table, field string, ids []uint64) ([]string, error) {
	method := "POST"
	path := "/internal/translate/ids"
	headers := map[string]string{
		"Content-Type": "application/x-protobuf",
		"Accept":       "application/x-protobuf",
	}

	req := &pilosa.TranslateIDsRequest{
		Index: table,
		Field: field,
		IDs:   ids,
	}

	ser := proto.Serializer{}

	data, err := ser.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling translate ids request")
	}

	status, body, err := fbClient.HTTPRequest(method, path, data, headers)
	if err != nil {
		return nil, errors.Wrap(err, "http request")
	} else if status != http.StatusOK {
		return nil, errors.Wrapf(err, "http request status code: %d", status)
	}

	idsResp := &pilosa.TranslateIDsResponse{}

	if err := ser.Unmarshal(body, idsResp); err != nil {
		return nil, errors.Wrap(err, "unmarshaling translate ids request")
	}

	return idsResp.Keys, nil
}

func splitIDsByShard(ids []uint64, shardWidth uint64) map[dax.ShardNum][]uint64 {
	ret := make(map[dax.ShardNum][]uint64)
	for _, id := range ids {
		shardIDs, ok := ret[dax.ShardNum(id/shardWidth)]
		if !ok {
			shardIDs = make([]uint64, 0)
		}
		ret[dax.ShardNum(id/shardWidth)] = append(shardIDs, id)
	}
	return ret
}

func splitIDsByPartition(index string, ids []uint64, shardWidth uint64) map[dax.PartitionNum][]uint64 {
	idsByShard := splitIDsByShard(ids, shardWidth)

	partitioner := partitioner.NewPartitioner()

	ret := make(map[dax.PartitionNum][]uint64)
	for shard, ids := range idsByShard {
		// get partition for shard
		// TODO: need to get partitionN from the table, instead of using the default.
		partitionNum := partitioner.ShardToPartition(dax.TableKey(index), shard, disco.DefaultPartitionN)

		ret[partitionNum] = append(ret[partitionNum], ids...)
	}
	return ret
}
