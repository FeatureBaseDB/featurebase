package proto

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/ingest"
	pnet "github.com/molecula/featurebase/v2/net"
	"github.com/molecula/featurebase/v2/pb"
	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/topology"
	"github.com/pkg/errors"
)

// Serializer implements pilosa.Serializer for protobufs.
type Serializer struct {
	RoaringRows bool
}

var DefaultSerializer = Serializer{}
var RoaringSerializer = Serializer{RoaringRows: true}

// Marshal turns pilosa messages into protobuf serialized bytes.
func (s Serializer) Marshal(m pilosa.Message) ([]byte, error) {
	pm := s.encodeToProto(m)
	if pm == nil {
		return nil, errors.New("passed invalid pilosa.Message")
	}
	buf, err := proto.Marshal(pm)
	return buf, errors.Wrap(err, "marshalling")
}

// Unmarshal takes byte slices and protobuf deserializes them into a pilosa Message.
func (s Serializer) Unmarshal(buf []byte, m pilosa.Message) error {
	switch mt := m.(type) {
	case *pilosa.CreateShardMessage:
		msg := &pb.CreateShardMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateShardMessage")
		}
		s.decodeCreateShardMessage(msg, mt)
		return nil
	case *pilosa.CreateIndexMessage:
		msg := &pb.CreateIndexMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateIndexMessage")
		}
		s.decodeCreateIndexMessage(msg, mt)
		return nil
	case *pilosa.DeleteIndexMessage:
		msg := &pb.DeleteIndexMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteIndexMessage")
		}
		s.decodeDeleteIndexMessage(msg, mt)
		return nil
	case *pilosa.CreateFieldMessage:
		msg := &pb.CreateFieldMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateFieldMessage")
		}
		s.decodeCreateFieldMessage(msg, mt)
		return nil
	case *pilosa.DeleteFieldMessage:
		msg := &pb.DeleteFieldMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteFieldMessage")
		}
		s.decodeDeleteFieldMessage(msg, mt)
		return nil
	case *pilosa.DeleteAvailableShardMessage:
		msg := &pb.DeleteAvailableShardMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteAvailableShardMessage")
		}
		s.decodeDeleteAvailableShardMessage(msg, mt)
		return nil
	case *pilosa.CreateViewMessage:
		msg := &pb.CreateViewMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateViewMessage")
		}
		s.decodeCreateViewMessage(msg, mt)
		return nil
	case *pilosa.DeleteViewMessage:
		msg := &pb.DeleteViewMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteViewMessage")
		}
		s.decodeDeleteViewMessage(msg, mt)
		return nil
	case *pilosa.ClusterStatus:
		msg := &pb.ClusterStatus{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ClusterStatus")
		}
		s.decodeClusterStatus(msg, mt)
		return nil
	case *pilosa.ResizeInstruction:
		msg := &pb.ResizeInstruction{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ResizeInstruction")
		}
		s.decodeResizeInstruction(msg, mt)
		return nil
	case *pilosa.ResizeInstructionComplete:
		msg := &pb.ResizeInstructionComplete{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ResizeInstructionComplete")
		}
		s.decodeResizeInstructionComplete(msg, mt)
		return nil
	case *pilosa.NodeStateMessage:
		msg := &pb.NodeStateMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling NodeStateMessage")
		}
		s.decodeNodeStateMessage(msg, mt)
		return nil
	case *pilosa.RecalculateCaches:
		msg := &pb.RecalculateCaches{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling RecalculateCaches")
		}
		s.decodeRecalculateCaches(msg, mt)
		return nil
	case *pilosa.LoadSchemaMessage:
		msg := &pb.LoadSchemaMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling LoadSchemaMessage")
		}
		s.decodeLoadSchemaMessage(msg, mt)
		return nil
	case *pilosa.NodeEvent:
		msg := &pb.NodeEventMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling NodeEvent")
		}
		s.decodeNodeEventMessage(msg, mt)
		return nil
	case *pilosa.NodeStatus:
		msg := &pb.NodeStatus{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling NodeStatus")
		}
		s.decodeNodeStatus(msg, mt)
		return nil
	case *topology.Node:
		msg := &pb.Node{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling Node")
		}
		s.decodeNode(msg, mt)
		return nil
	case *pilosa.QueryRequest:
		msg := &pb.QueryRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling QueryRequest")
		}
		s.decodeQueryRequest(msg, mt)
		return nil
	case *pilosa.QueryResponse:
		msg := &pb.QueryResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling QueryResponse")
		}
		s.decodeQueryResponse(msg, mt)
		return nil
	case *pilosa.ImportRequest:
		msg := &pb.ImportRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportRequest")
		}
		s.decodeImportRequest(msg, mt)
		return nil
	case *pilosa.ImportValueRequest:
		msg := &pb.ImportValueRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportValueRequest")
		}
		s.decodeImportValueRequest(msg, mt)
		return nil
	case *pilosa.ImportRoaringRequest:
		msg := &pb.ImportRoaringRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportRoaringRequest")
		}
		s.decodeImportRoaringRequest(msg, mt)
		return nil
	case *pilosa.ImportResponse:
		msg := &pb.ImportResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportResponse")
		}
		s.decodeImportResponse(msg, mt)
		return nil
	case *pilosa.BlockDataRequest:
		msg := &pb.BlockDataRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling BlockDataRequest")
		}
		s.decodeBlockDataRequest(msg, mt)
		return nil
	case *pilosa.BlockDataResponse:
		msg := &pb.BlockDataResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling BlockDataResponse")
		}
		s.decodeBlockDataResponse(msg, mt)
		return nil
	case *pilosa.TranslateKeysRequest:
		msg := &pb.TranslateKeysRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling TranslateKeysRequest")
		}
		s.decodeTranslateKeysRequest(msg, mt)
		return nil
	case *pilosa.TranslateKeysResponse:
		msg := &pb.TranslateKeysResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling TranslateKeysResponse")
		}
		s.decodeTranslateKeysResponse(msg, mt)
		return nil
	case *pilosa.TranslateIDsRequest:
		msg := &pb.TranslateIDsRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling TranslateIDsRequest")
		}
		s.decodeTranslateIDsRequest(msg, mt)
		return nil
	case *pilosa.TranslateIDsResponse:
		msg := &pb.TranslateIDsResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling TranslateIDsResponse")
		}
		s.decodeTranslateIDsResponse(msg, mt)
		return nil
	case *pilosa.TransactionMessage:
		msg := &pb.TransactionMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling TransactionMessage")
		}
		decodeTransactionMessage(msg, mt)
		return nil
	case *pilosa.AtomicRecord:
		msg := &pb.AtomicRecord{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling AtomicRecord")
		}
		s.decodeAtomicRecord(msg, mt)
		return nil
	case *[]*pilosa.Row:
		msg := &pb.RowMatrix{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling RowMatrix")
		}
		*mt = s.decodeRowMatrix(msg)
		return nil

	case *pilosa.ResizeNodeMessage:
		msg := &pb.ResizeNodeMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ResizeNodeMessage")
		}
		decodeResizeNodeMessage(msg, mt)
		return nil

	case *pilosa.ResizeAbortMessage:
		msg := &pb.ResizeAbortMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ResizeAbortMessage")
		}
		decodeResizeAbortMessage(msg, mt)
		return nil
	case *ingest.ShardedRequest:
		msg := &pb.ShardedIngestRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshalling ShardedRequest")
		}
		req, err := s.decodeShardedIngestRequest(msg)
		if err != nil {
			return err
		}
		*mt = *req
		return nil
	default:
		panic(fmt.Sprintf("unhandled pilosa.Message of type %T: %#v", mt, m))
	}
}

func (s Serializer) encodeToProto(m pilosa.Message) proto.Message {
	switch mt := m.(type) {
	case *pilosa.CreateShardMessage:
		return s.encodeCreateShardMessage(mt)
	case *pilosa.CreateIndexMessage:
		return s.encodeCreateIndexMessage(mt)
	case *pilosa.DeleteIndexMessage:
		return s.encodeDeleteIndexMessage(mt)
	case *pilosa.CreateFieldMessage:
		return s.encodeCreateFieldMessage(mt)
	case *pilosa.DeleteFieldMessage:
		return s.encodeDeleteFieldMessage(mt)
	case *pilosa.DeleteAvailableShardMessage:
		return s.encodeDeleteAvailableShardMessage(mt)
	case *pilosa.CreateViewMessage:
		return s.encodeCreateViewMessage(mt)
	case *pilosa.DeleteViewMessage:
		return s.encodeDeleteViewMessage(mt)
	case *pilosa.ClusterStatus:
		return s.encodeClusterStatus(mt)
	case *pilosa.ResizeInstruction:
		return s.encodeResizeInstruction(mt)
	case *pilosa.ResizeInstructionComplete:
		return s.encodeResizeInstructionComplete(mt)
	case *pilosa.NodeStateMessage:
		return s.encodeNodeStateMessage(mt)
	case *pilosa.RecalculateCaches:
		return s.encodeRecalculateCaches(mt)
	case *pilosa.LoadSchemaMessage:
		return s.encodeLoadSchemaMessage(mt)
	case *pilosa.NodeEvent:
		return s.encodeNodeEventMessage(mt)
	case *pilosa.NodeStatus:
		return s.encodeNodeStatus(mt)
	case *topology.Node:
		return s.encodeNode(mt)
	case *pilosa.QueryRequest:
		return s.encodeQueryRequest(mt)
	case *pilosa.QueryResponse:
		return s.encodeQueryResponse(mt)
	case *pilosa.ImportRequest:
		return s.encodeImportRequest(mt)
	case *pilosa.ImportValueRequest:
		return s.encodeImportValueRequest(mt)
	case *pilosa.ImportRoaringRequest:
		return s.encodeImportRoaringRequest(mt)
	case *pilosa.ImportResponse:
		return s.encodeImportResponse(mt)
	case *pilosa.BlockDataRequest:
		return s.encodeBlockDataRequest(mt)
	case *pilosa.BlockDataResponse:
		return s.encodeBlockDataResponse(mt)
	case *pilosa.TranslateKeysRequest:
		return s.encodeTranslateKeysRequest(mt)
	case *pilosa.TranslateKeysResponse:
		return s.encodeTranslateKeysResponse(mt)
	case *pilosa.TranslateIDsRequest:
		return s.encodeTranslateIDsRequest(mt)
	case *pilosa.TranslateIDsResponse:
		return s.encodeTranslateIDsResponse(mt)
	case *pilosa.TransactionMessage:
		return s.encodeTransactionMessage(mt)
	case *pilosa.AtomicRecord:
		return s.encodeAtomicRecord(mt)
	case *pilosa.ResizeNodeMessage:
		return s.encodeResizeNodeMessage(mt)
	case *pilosa.ResizeAbortMessage:
		return s.encodeResizeAbortMessage(mt)
	case *ingest.ShardedRequest:
		return s.encodeShardedIngestRequest(mt)
	}
	return nil
}

func (s Serializer) encodeBlockDataRequest(m *pilosa.BlockDataRequest) *pb.BlockDataRequest {
	return &pb.BlockDataRequest{
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
		Shard: m.Shard,
		Block: m.Block,
	}
}
func (s Serializer) encodeBlockDataResponse(m *pilosa.BlockDataResponse) *pb.BlockDataResponse {
	return &pb.BlockDataResponse{
		RowIDs:    m.RowIDs,
		ColumnIDs: m.ColumnIDs,
	}
}

func (s Serializer) encodeImportResponse(m *pilosa.ImportResponse) *pb.ImportResponse {
	return &pb.ImportResponse{
		Err: m.Err,
	}
}

func (s Serializer) encodeImportRequest(m *pilosa.ImportRequest) *pb.ImportRequest {
	return &pb.ImportRequest{
		Index:          m.Index,
		Field:          m.Field,
		IndexCreatedAt: m.IndexCreatedAt,
		FieldCreatedAt: m.FieldCreatedAt,
		Shard:          m.Shard,
		RowIDs:         m.RowIDs,
		ColumnIDs:      m.ColumnIDs,
		RowKeys:        m.RowKeys,
		ColumnKeys:     m.ColumnKeys,
		Timestamps:     m.Timestamps,
		Clear:          m.Clear,
	}
}

func (s Serializer) encodeImportValueRequest(m *pilosa.ImportValueRequest) *pb.ImportValueRequest {
	return &pb.ImportValueRequest{
		Index:          m.Index,
		Field:          m.Field,
		IndexCreatedAt: m.IndexCreatedAt,
		FieldCreatedAt: m.FieldCreatedAt,
		Shard:          m.Shard,
		ColumnIDs:      m.ColumnIDs,
		ColumnKeys:     m.ColumnKeys,
		Values:         m.Values,
		FloatValues:    m.FloatValues,
		StringValues:   m.StringValues,
		Clear:          m.Clear,
	}
}

func (s Serializer) encodeImportRoaringRequest(m *pilosa.ImportRoaringRequest) *pb.ImportRoaringRequest {
	views := make([]*pb.ImportRoaringRequestView, len(m.Views))
	i := 0
	for viewName, viewData := range m.Views {
		views[i] = &pb.ImportRoaringRequestView{
			Name: viewName,
			Data: viewData,
		}
		i++
	}
	return &pb.ImportRoaringRequest{
		IndexCreatedAt:  m.IndexCreatedAt,
		FieldCreatedAt:  m.FieldCreatedAt,
		Clear:           m.Clear,
		Action:          m.Action,
		Block:           uint64(m.Block),
		Views:           views,
		UpdateExistence: m.UpdateExistence,
	}
}

func (s Serializer) encodeQueryRequest(m *pilosa.QueryRequest) *pb.QueryRequest {
	r := &pb.QueryRequest{
		Query:         m.Query,
		Shards:        m.Shards,
		Remote:        m.Remote,
		PreTranslated: m.PreTranslated,
		EmbeddedData:  make([]*pb.Row, len(m.EmbeddedData)),
		MaxMemory:     m.MaxMemory,
	}
	for i := range m.EmbeddedData {
		r.EmbeddedData[i] = s.encodeRow(m.EmbeddedData[i])
	}
	return r
}

func (s Serializer) encodeQueryResponse(m *pilosa.QueryResponse) *pb.QueryResponse {
	resp := &pb.QueryResponse{
		Results: make([]*pb.QueryResult, len(m.Results)),
	}

	for i := range m.Results {
		resp.Results[i] = &pb.QueryResult{}

		switch result := m.Results[i].(type) {
		case pilosa.SignedRow:
			resp.Results[i].Type = queryResultTypeSignedRow
			resp.Results[i].SignedRow = s.encodeSignedRow(result)
		case *pilosa.Row:
			resp.Results[i].Type = queryResultTypeRow
			resp.Results[i].Row = s.encodeRow(result)
		case []pilosa.Pair:
			resp.Results[i].Type = queryResultTypePairs
			resp.Results[i].Pairs = s.encodePairs(result)
		case *pilosa.PairsField:
			resp.Results[i].Type = queryResultTypePairsField
			resp.Results[i].PairsField = s.encodePairsField(result)
		case pilosa.ValCount:
			resp.Results[i].Type = queryResultTypeValCount
			resp.Results[i].ValCount = s.encodeValCount(result)
		case uint64:
			resp.Results[i].Type = queryResultTypeUint64
			resp.Results[i].N = result
		case bool:
			resp.Results[i].Type = queryResultTypeBool
			resp.Results[i].Changed = result
		case pilosa.RowIDs:
			resp.Results[i].Type = queryResultTypeRowIDs
			resp.Results[i].RowIDs = result
		case pilosa.ExtractedIDMatrix:
			resp.Results[i].Type = queryResultTypeExtractedIDMatrix
			resp.Results[i].ExtractedIDMatrix = s.endcodeExtractedIDMatrix(result)
		case *pilosa.GroupCounts:
			resp.Results[i].Type = queryResultTypeGroupCounts
			resp.Results[i].GroupCounts = s.encodeGroupCounts(result)
		case pilosa.RowIdentifiers:
			resp.Results[i].Type = queryResultTypeRowIdentifiers
			resp.Results[i].RowIdentifiers = s.encodeRowIdentifiers(result)
		case pilosa.ExtractedTable:
			resp.Results[i].Type = queryResultTypeExtractedTable
			resp.Results[i].ExtractedTable = s.encodeExtractedTable(result)
		case pilosa.Pair:
			resp.Results[i].Type = queryResultTypePair
			resp.Results[i].Pairs = []*pb.Pair{s.encodePair(result)}
		case pilosa.PairField:
			resp.Results[i].Type = queryResultTypePairField
			resp.Results[i].PairField = s.encodePairField(result)
		case []*pilosa.Row:
			resp.Results[i].Type = queryResultTypeRowMatrix
			resp.Results[i].RowMatrix = s.encodeRowMatrix(result)
		case pilosa.DistinctTimestamp:
			resp.Results[i].Type = queryResultTypeDistinctTimestamp
			resp.Results[i].DistinctTimestamp = s.encodeDistinctTimestamp(result)
		case nil:
			resp.Results[i].Type = queryResultTypeNil
		default:
			panic(fmt.Errorf("unknown type: %T", m.Results[i]))
		}
	}

	if m.Err != nil {
		resp.Err = m.Err.Error()
	}

	return resp
}

func (s Serializer) encodeResizeInstruction(m *pilosa.ResizeInstruction) *pb.ResizeInstruction {
	return &pb.ResizeInstruction{
		JobID:              m.JobID,
		Node:               s.encodeNode(m.Node),
		Primary:            s.encodeNode(m.Primary),
		Sources:            s.encodeResizeSources(m.Sources),
		TranslationSources: s.encodeTranslationResizeSources(m.TranslationSources),
		NodeStatus:         s.encodeNodeStatus(m.NodeStatus),
		ClusterStatus:      s.encodeClusterStatus(m.ClusterStatus),
	}
}

func (s Serializer) encodeResizeSources(srcs []*pilosa.ResizeSource) []*pb.ResizeSource {
	new := make([]*pb.ResizeSource, 0, len(srcs))
	for _, src := range srcs {
		new = append(new, s.encodeResizeSource(src))
	}
	return new
}

func (s Serializer) encodeResizeSource(m *pilosa.ResizeSource) *pb.ResizeSource {
	return &pb.ResizeSource{
		Node:  s.encodeNode(m.Node),
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
		Shard: m.Shard,
	}
}

func (s Serializer) encodeTranslationResizeSources(srcs []*pilosa.TranslationResizeSource) []*pb.TranslationResizeSource {
	new := make([]*pb.TranslationResizeSource, 0, len(srcs))
	for _, src := range srcs {
		new = append(new, s.encodeTranslationResizeSource(src))
	}
	return new
}

func (s Serializer) encodeTranslationResizeSource(m *pilosa.TranslationResizeSource) *pb.TranslationResizeSource {
	return &pb.TranslationResizeSource{
		Node:        s.encodeNode(m.Node),
		Index:       m.Index,
		PartitionID: int32(m.PartitionID),
	}
}

func (s Serializer) encodeSchema(m *pilosa.Schema) *pb.Schema {
	return &pb.Schema{
		Indexes: s.encodeIndexInfos(m.Indexes),
	}
}

func (s Serializer) encodeIndexInfos(idxs []*pilosa.IndexInfo) []*pb.Index {
	new := make([]*pb.Index, 0, len(idxs))
	for _, idx := range idxs {
		new = append(new, s.encodeIndexInfo(idx))
	}
	return new
}

func (s Serializer) encodeIndexInfo(idx *pilosa.IndexInfo) *pb.Index {
	return &pb.Index{
		Name:      idx.Name,
		CreatedAt: idx.CreatedAt,
		Options:   s.encodeIndexMeta(&idx.Options),
		Fields:    s.encodeFieldInfos(idx.Fields),
	}
}

func (s Serializer) encodeFieldInfos(fs []*pilosa.FieldInfo) []*pb.Field {
	new := make([]*pb.Field, 0, len(fs))
	for _, f := range fs {
		new = append(new, s.encodeFieldInfo(f))
	}
	return new
}

func (s Serializer) encodeFieldInfo(f *pilosa.FieldInfo) *pb.Field {
	ifield := &pb.Field{
		Name:      f.Name,
		CreatedAt: f.CreatedAt,
		Meta:      s.encodeFieldOptions(&f.Options),
		Views:     make([]string, 0, len(f.Views)),
	}

	for _, viewinfo := range f.Views {
		ifield.Views = append(ifield.Views, viewinfo.Name)
	}
	return ifield
}

func (s Serializer) encodeFieldOptions(o *pilosa.FieldOptions) *pb.FieldOptions {
	if o == nil {
		return nil
	}
	return &pb.FieldOptions{
		Type:         o.Type,
		CacheType:    o.CacheType,
		CacheSize:    o.CacheSize,
		Min:          &pb.Decimal{Value: o.Min.Value, Scale: o.Min.Scale},
		Max:          &pb.Decimal{Value: o.Max.Value, Scale: o.Max.Scale},
		Base:         o.Base,
		Scale:        o.Scale,
		BitDepth:     uint64(o.BitDepth),
		TimeQuantum:  string(o.TimeQuantum),
		TimeUnit:     string(o.TimeUnit),
		Keys:         o.Keys,
		ForeignIndex: o.ForeignIndex,
	}
}

// s.encodeNodes converts a slice of Nodes into its pb.representation.
func (s Serializer) encodeNodes(a []*topology.Node) []*pb.Node {
	other := make([]*pb.Node, len(a))
	for i := range a {
		other[i] = s.encodeNode(a[i])
	}
	return other
}

// s.encodeNode converts a Node into its pb.representation.
func (s Serializer) encodeNode(m *topology.Node) *pb.Node {
	n := m.Clone()
	return &pb.Node{
		ID:      n.ID,
		URI:     s.encodeURI(n.URI),
		State:   string(n.State),
		GRPCURI: s.encodeURI(n.GRPCURI),
	}
}

func (s Serializer) encodeURI(u pnet.URI) *pb.URI {
	return &pb.URI{
		Scheme: u.Scheme,
		Host:   u.Host,
		Port:   uint32(u.Port),
	}
}

func (s Serializer) encodeClusterStatus(m *pilosa.ClusterStatus) *pb.ClusterStatus {
	return &pb.ClusterStatus{
		State:     m.State,
		ClusterID: m.ClusterID,
		Nodes:     s.encodeNodes(m.Nodes),
		Schema:    s.encodeSchema(m.Schema),
	}
}

func (s Serializer) encodeCreateShardMessage(m *pilosa.CreateShardMessage) *pb.CreateShardMessage {
	return &pb.CreateShardMessage{
		Index: m.Index,
		Field: m.Field,
		Shard: m.Shard,
	}
}

func (s Serializer) encodeCreateIndexMessage(m *pilosa.CreateIndexMessage) *pb.CreateIndexMessage {
	return &pb.CreateIndexMessage{
		Index:     m.Index,
		CreatedAt: m.CreatedAt,
		Meta:      s.encodeIndexMeta(&m.Meta),
	}
}

func (s Serializer) encodeIndexMeta(m *pilosa.IndexOptions) *pb.IndexMeta {
	return &pb.IndexMeta{
		Keys:           m.Keys,
		TrackExistence: m.TrackExistence,
	}
}

func (s Serializer) encodeDeleteIndexMessage(m *pilosa.DeleteIndexMessage) *pb.DeleteIndexMessage {
	return &pb.DeleteIndexMessage{
		Index: m.Index,
	}
}

func (s Serializer) encodeCreateFieldMessage(m *pilosa.CreateFieldMessage) *pb.CreateFieldMessage {
	return &pb.CreateFieldMessage{
		Index:     m.Index,
		Field:     m.Field,
		CreatedAt: m.CreatedAt,
		Meta:      s.encodeFieldOptions(m.Meta),
	}
}

func (s Serializer) encodeDeleteFieldMessage(m *pilosa.DeleteFieldMessage) *pb.DeleteFieldMessage {
	return &pb.DeleteFieldMessage{
		Index: m.Index,
		Field: m.Field,
	}
}

func (s Serializer) encodeDeleteAvailableShardMessage(m *pilosa.DeleteAvailableShardMessage) *pb.DeleteAvailableShardMessage {
	return &pb.DeleteAvailableShardMessage{
		Index:   m.Index,
		Field:   m.Field,
		ShardID: m.ShardID,
	}
}

func (s Serializer) encodeCreateViewMessage(m *pilosa.CreateViewMessage) *pb.CreateViewMessage {
	return &pb.CreateViewMessage{
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
	}
}

func (s Serializer) encodeDeleteViewMessage(m *pilosa.DeleteViewMessage) *pb.DeleteViewMessage {
	return &pb.DeleteViewMessage{
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
	}
}

func (s Serializer) encodeResizeInstructionComplete(m *pilosa.ResizeInstructionComplete) *pb.ResizeInstructionComplete {
	return &pb.ResizeInstructionComplete{
		JobID: m.JobID,
		Node:  s.encodeNode(m.Node),
		Error: m.Error,
	}
}

func (s Serializer) encodeNodeStateMessage(m *pilosa.NodeStateMessage) *pb.NodeStateMessage {
	return &pb.NodeStateMessage{
		NodeID: m.NodeID,
		State:  m.State,
	}
}

func (s Serializer) encodeNodeEventMessage(m *pilosa.NodeEvent) *pb.NodeEventMessage {
	return &pb.NodeEventMessage{
		Event: uint32(m.Event),
		Node:  s.encodeNode(m.Node),
	}
}

func (s Serializer) encodeNodeStatus(m *pilosa.NodeStatus) *pb.NodeStatus {
	return &pb.NodeStatus{
		Node:    s.encodeNode(m.Node),
		Indexes: s.encodeIndexStatuses(m.Indexes),
		Schema:  s.encodeSchema(m.Schema),
	}
}

func (s Serializer) encodeIndexStatus(m *pilosa.IndexStatus) *pb.IndexStatus {
	return &pb.IndexStatus{
		Name:      m.Name,
		CreatedAt: m.CreatedAt,
		Fields:    s.encodeFieldStatuses(m.Fields),
	}
}

func (s Serializer) encodeIndexStatuses(a []*pilosa.IndexStatus) []*pb.IndexStatus {
	other := make([]*pb.IndexStatus, len(a))
	for i := range a {
		other[i] = s.encodeIndexStatus(a[i])
	}
	return other
}

func (s Serializer) encodeFieldStatus(m *pilosa.FieldStatus) *pb.FieldStatus {
	return &pb.FieldStatus{
		Name:            m.Name,
		CreatedAt:       m.CreatedAt,
		AvailableShards: m.AvailableShards.Slice(),
	}
}

func (s Serializer) encodeFieldStatuses(a []*pilosa.FieldStatus) []*pb.FieldStatus {
	other := make([]*pb.FieldStatus, len(a))
	for i := range a {
		other[i] = s.encodeFieldStatus(a[i])
	}
	return other
}

func (s Serializer) encodeRecalculateCaches(*pilosa.RecalculateCaches) *pb.RecalculateCaches {
	return &pb.RecalculateCaches{}
}

func (s Serializer) encodeLoadSchemaMessage(*pilosa.LoadSchemaMessage) *pb.LoadSchemaMessage {
	return &pb.LoadSchemaMessage{}
}

func (s Serializer) encodeTranslateKeysRequest(request *pilosa.TranslateKeysRequest) *pb.TranslateKeysRequest {
	return &pb.TranslateKeysRequest{
		Index:       request.Index,
		Field:       request.Field,
		Keys:        request.Keys,
		NotWritable: request.NotWritable,
	}
}

func (s Serializer) encodeTranslateKeysResponse(response *pilosa.TranslateKeysResponse) *pb.TranslateKeysResponse {
	return &pb.TranslateKeysResponse{
		IDs: response.IDs,
	}
}

func (s Serializer) encodeTranslateIDsRequest(request *pilosa.TranslateIDsRequest) *pb.TranslateIDsRequest {
	return &pb.TranslateIDsRequest{
		Index: request.Index,
		Field: request.Field,
		IDs:   request.IDs,
	}
}

func (s Serializer) encodeTranslateIDsResponse(response *pilosa.TranslateIDsResponse) *pb.TranslateIDsResponse {
	return &pb.TranslateIDsResponse{
		Keys: response.Keys,
	}
}

func (s Serializer) encodeTransactionMessage(msg *pilosa.TransactionMessage) *pb.TransactionMessage {
	return &pb.TransactionMessage{
		Action:      msg.Action,
		Transaction: s.encodeTransaction(msg.Transaction),
	}
}

func (s Serializer) encodeAtomicRecord(msg *pilosa.AtomicRecord) *pb.AtomicRecord {
	ar := &pb.AtomicRecord{
		Index: msg.Index,
		Shard: msg.Shard,
	}
	for _, ivr := range msg.Ivr {
		ar.Ivr = append(ar.Ivr, s.encodeImportValueRequest(ivr))
	}
	for _, ir := range msg.Ir {
		ar.Ir = append(ar.Ir, s.encodeImportRequest(ir))
	}
	return ar
}

func (s Serializer) encodeRowMatrix(msg []*pilosa.Row) *pb.RowMatrix {
	rows := make([]*pb.Row, len(msg))
	for i, r := range msg {
		rows[i] = s.encodeRow(r)
	}

	return &pb.RowMatrix{Rows: rows}
}

func (s Serializer) encodeTransaction(trns *pilosa.Transaction) *pb.Transaction {
	if trns == nil {
		return nil
	}
	return &pb.Transaction{
		ID:        trns.ID,
		Active:    trns.Active,
		Exclusive: trns.Exclusive,
		Timeout:   int64(trns.Timeout),
		Deadline:  s.encodeTransactionDeadline(trns.Deadline),
		Stats:     s.encodeTransactionStats(trns.Stats),
	}
}

func (s Serializer) encodeTransactionDeadline(deadline time.Time) int64 {
	if deadline.Year() > 2262 || deadline.Year() < 1678 {
		return 0
	}
	return deadline.UnixNano()
}

func (s Serializer) encodeTransactionStats(stats pilosa.TransactionStats) *pb.TransactionStats {
	return &pb.TransactionStats{}
}

func (s Serializer) encodeShardedIngestRequest(req *ingest.ShardedRequest) *pb.ShardedIngestRequest {
	if req == nil || len(req.Ops) == 0 {
		return &pb.ShardedIngestRequest{}
	}
	out := &pb.ShardedIngestRequest{Ops: make(map[uint64]*pb.ShardIngestOperations, len(req.Ops))}
	for shard, ops := range req.Ops {
		out.Ops[shard] = s.encodeShardIngestOperations(ops)
	}
	return out
}

func (s Serializer) encodeShardIngestOperations(ops []*ingest.Operation) *pb.ShardIngestOperations {
	out := &pb.ShardIngestOperations{}
	for _, op := range ops {
		if op == nil {
			continue
		}
		out.Ops = append(out.Ops, s.encodeShardIngestOperation(op))
	}
	return out
}

func (s Serializer) encodeShardIngestOperation(op *ingest.Operation) *pb.ShardIngestOperation {
	out := &pb.ShardIngestOperation{
		OpType:         op.OpType.String(),
		ClearRecordIDs: op.ClearRecordIDs,
		ClearFields:    op.ClearFields,
		FieldOps:       make(map[string]*pb.FieldOperation, len(op.FieldOps)),
	}
	for k, v := range op.FieldOps {
		if v == nil {
			continue
		}
		out.FieldOps[k] = &pb.FieldOperation{
			RecordIDs: v.RecordIDs,
			Values:    v.Values,
			Signed:    v.Signed,
		}
	}
	return out
}

func (s Serializer) decodeResizeInstruction(ri *pb.ResizeInstruction, m *pilosa.ResizeInstruction) {
	m.JobID = ri.JobID
	m.Node = &topology.Node{}
	s.decodeNode(ri.Node, m.Node)
	m.Primary = &topology.Node{}
	s.decodeNode(ri.Primary, m.Primary)
	m.Sources = make([]*pilosa.ResizeSource, len(ri.Sources))
	s.decodeResizeSources(ri.Sources, m.Sources)
	m.TranslationSources = make([]*pilosa.TranslationResizeSource, len(ri.TranslationSources))
	s.decodeTranslationResizeSources(ri.TranslationSources, m.TranslationSources)
	m.NodeStatus = &pilosa.NodeStatus{}
	s.decodeNodeStatus(ri.NodeStatus, m.NodeStatus)
	m.ClusterStatus = &pilosa.ClusterStatus{}
	s.decodeClusterStatus(ri.ClusterStatus, m.ClusterStatus)
}

func (s Serializer) decodeResizeSources(srcs []*pb.ResizeSource, m []*pilosa.ResizeSource) {
	for i := range srcs {
		m[i] = &pilosa.ResizeSource{}
		s.decodeResizeSource(srcs[i], m[i])
	}
}

func (s Serializer) decodeResizeSource(rs *pb.ResizeSource, m *pilosa.ResizeSource) {
	m.Node = &topology.Node{}
	s.decodeNode(rs.Node, m.Node)
	m.Index = rs.Index
	m.Field = rs.Field
	m.View = rs.View
	m.Shard = rs.Shard
}

func (s Serializer) decodeTranslationResizeSources(srcs []*pb.TranslationResizeSource, m []*pilosa.TranslationResizeSource) {
	for i := range srcs {
		m[i] = &pilosa.TranslationResizeSource{}
		s.decodeTranslationResizeSource(srcs[i], m[i])
	}
}

func (s Serializer) decodeTranslationResizeSource(rs *pb.TranslationResizeSource, m *pilosa.TranslationResizeSource) {
	m.Node = &topology.Node{}
	s.decodeNode(rs.Node, m.Node)
	m.Index = rs.Index
	m.PartitionID = int(rs.PartitionID)
}

func (s Serializer) decodeSchema(sc *pb.Schema, m *pilosa.Schema) {
	m.Indexes = make([]*pilosa.IndexInfo, len(sc.Indexes))
	s.decodeIndexes(sc.Indexes, m.Indexes)
}

func (s Serializer) decodeIndexes(idxs []*pb.Index, m []*pilosa.IndexInfo) {
	for i := range idxs {
		m[i] = &pilosa.IndexInfo{}
		s.decodeIndex(idxs[i], m[i])
	}
}

func (s Serializer) decodeIndex(idx *pb.Index, m *pilosa.IndexInfo) {
	m.Name = idx.Name
	m.CreatedAt = idx.CreatedAt
	m.Options = pilosa.IndexOptions{}
	s.decodeIndexMeta(idx.Options, &m.Options)
	m.Fields = make([]*pilosa.FieldInfo, len(idx.Fields))
	s.decodeFields(idx.Fields, m.Fields)
}

func (s Serializer) decodeFields(fs []*pb.Field, m []*pilosa.FieldInfo) {
	for i := range fs {
		m[i] = &pilosa.FieldInfo{}
		s.decodeField(fs[i], m[i])
	}
}

func (s Serializer) decodeField(f *pb.Field, m *pilosa.FieldInfo) {
	m.Name = f.Name
	m.CreatedAt = f.CreatedAt
	m.Options = pilosa.FieldOptions{}
	s.decodeFieldOptions(f.Meta, &m.Options)
	m.Views = make([]*pilosa.ViewInfo, 0, len(f.Views))
	for _, viewname := range f.Views {
		m.Views = append(m.Views, &pilosa.ViewInfo{Name: viewname})
	}
}

func (s Serializer) decodeFieldOptions(options *pb.FieldOptions, m *pilosa.FieldOptions) {
	m.Type = options.Type
	m.CacheType = options.CacheType
	m.CacheSize = options.CacheSize
	s.decodeDecimal(options.Min, &m.Min)
	s.decodeDecimal(options.Max, &m.Max)
	m.Base = options.Base
	m.Scale = options.Scale
	m.BitDepth = uint64(options.BitDepth)
	m.TimeQuantum = pilosa.TimeQuantum(options.TimeQuantum)
	m.TimeUnit = options.TimeUnit
	m.Keys = options.Keys
	m.ForeignIndex = options.ForeignIndex
}

func (s Serializer) decodeDecimal(d *pb.Decimal, m *pql.Decimal) {
	m.Value = d.Value
	m.Scale = d.Scale
}

func (s Serializer) decodeNodes(a []*pb.Node, m []*topology.Node) {
	for i := range a {
		m[i] = &topology.Node{}
		s.decodeNode(a[i], m[i])
	}
}

func (s Serializer) decodeClusterStatus(cs *pb.ClusterStatus, m *pilosa.ClusterStatus) {
	m.State = cs.State
	m.ClusterID = cs.ClusterID
	m.Nodes = make([]*topology.Node, len(cs.Nodes))
	s.decodeNodes(cs.Nodes, m.Nodes)
	m.Schema = &pilosa.Schema{}
	s.decodeSchema(cs.Schema, m.Schema)
}

func (s Serializer) decodeNode(node *pb.Node, m *topology.Node) {
	m.ID = node.ID
	s.decodeURI(node.URI, &m.URI)
	s.decodeURI(node.GRPCURI, &m.GRPCURI)
	m.State = disco.NodeState(node.State)
}

func (s Serializer) decodeURI(i *pb.URI, m *pnet.URI) {
	m.Scheme = i.Scheme
	m.Host = i.Host
	m.Port = uint16(i.Port)
}

func (s Serializer) decodeCreateShardMessage(pb *pb.CreateShardMessage, m *pilosa.CreateShardMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.Shard = pb.Shard
}

func (s Serializer) decodeCreateIndexMessage(pb *pb.CreateIndexMessage, m *pilosa.CreateIndexMessage) {
	m.Index = pb.Index
	m.CreatedAt = pb.CreatedAt
	m.Meta = pilosa.IndexOptions{}
	s.decodeIndexMeta(pb.Meta, &m.Meta)
}

func (s Serializer) decodeIndexMeta(pb *pb.IndexMeta, m *pilosa.IndexOptions) {
	if pb != nil {
		m.Keys = pb.Keys
		m.TrackExistence = pb.TrackExistence
	}
}

func (s Serializer) decodeDeleteIndexMessage(pb *pb.DeleteIndexMessage, m *pilosa.DeleteIndexMessage) {
	m.Index = pb.Index
}

func (s Serializer) decodeCreateFieldMessage(pb *pb.CreateFieldMessage, m *pilosa.CreateFieldMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.CreatedAt = pb.CreatedAt
	m.Meta = &pilosa.FieldOptions{}
	s.decodeFieldOptions(pb.Meta, m.Meta)
}

func (s Serializer) decodeDeleteFieldMessage(pb *pb.DeleteFieldMessage, m *pilosa.DeleteFieldMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
}

func (s Serializer) decodeDeleteAvailableShardMessage(pb *pb.DeleteAvailableShardMessage, m *pilosa.DeleteAvailableShardMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.ShardID = pb.ShardID
}

func (s Serializer) decodeCreateViewMessage(pb *pb.CreateViewMessage, m *pilosa.CreateViewMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.View = pb.View
}

func (s Serializer) decodeDeleteViewMessage(pb *pb.DeleteViewMessage, m *pilosa.DeleteViewMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.View = pb.View
}

func (s Serializer) decodeResizeInstructionComplete(pb *pb.ResizeInstructionComplete, m *pilosa.ResizeInstructionComplete) {
	m.JobID = pb.JobID
	m.Node = &topology.Node{}
	s.decodeNode(pb.Node, m.Node)
	m.Error = pb.Error
}

func (s Serializer) decodeNodeStateMessage(pb *pb.NodeStateMessage, m *pilosa.NodeStateMessage) {
	m.NodeID = pb.NodeID
	m.State = pb.State
}

func (s Serializer) decodeNodeEventMessage(pb *pb.NodeEventMessage, m *pilosa.NodeEvent) {
	m.Event = pilosa.NodeEventType(pb.Event)
	m.Node = &topology.Node{}
	s.decodeNode(pb.Node, m.Node)
}

func (s Serializer) decodeNodeStatus(pb *pb.NodeStatus, m *pilosa.NodeStatus) {
	m.Node = &topology.Node{}
	m.Indexes = s.decodeIndexStatuses(pb.Indexes)
	m.Schema = &pilosa.Schema{}
	s.decodeSchema(pb.Schema, m.Schema)
}

func (s Serializer) decodeIndexStatuses(a []*pb.IndexStatus) []*pilosa.IndexStatus {
	m := make([]*pilosa.IndexStatus, 0)
	for i := range a {
		m = append(m, &pilosa.IndexStatus{})
		s.decodeIndexStatus(a[i], m[i])
	}
	return m
}

func (s Serializer) decodeIndexStatus(pb *pb.IndexStatus, m *pilosa.IndexStatus) {
	m.Name = pb.Name
	m.CreatedAt = pb.CreatedAt
	m.Fields = s.decodeFieldStatuses(pb.Fields)
}

func (s Serializer) decodeFieldStatuses(a []*pb.FieldStatus) []*pilosa.FieldStatus {
	m := make([]*pilosa.FieldStatus, 0)
	for i := range a {
		m = append(m, &pilosa.FieldStatus{})
		s.decodeFieldStatus(a[i], m[i])
	}
	return m
}

func (s Serializer) decodeFieldStatus(pb *pb.FieldStatus, m *pilosa.FieldStatus) {
	m.Name = pb.Name
	m.CreatedAt = pb.CreatedAt
	m.AvailableShards = roaring.NewBitmap(pb.AvailableShards...)
}

func (s Serializer) decodeRecalculateCaches(pb *pb.RecalculateCaches, m *pilosa.RecalculateCaches) {
}

func (s Serializer) decodeLoadSchemaMessage(pb *pb.LoadSchemaMessage, m *pilosa.LoadSchemaMessage) {
}

func (s Serializer) decodeQueryRequest(pb *pb.QueryRequest, m *pilosa.QueryRequest) {
	m.Query = pb.Query
	m.Shards = pb.Shards
	m.Remote = pb.Remote
	m.EmbeddedData = make([]*pilosa.Row, len(pb.EmbeddedData))
	m.PreTranslated = pb.PreTranslated
	m.MaxMemory = pb.MaxMemory
	for i := range pb.EmbeddedData {
		m.EmbeddedData[i] = s.decodeRow(pb.EmbeddedData[i])
	}
}

func (s Serializer) decodeImportRequest(pb *pb.ImportRequest, m *pilosa.ImportRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.Shard = pb.Shard
	m.RowIDs = pb.RowIDs
	m.ColumnIDs = pb.ColumnIDs
	m.RowKeys = pb.RowKeys
	m.ColumnKeys = pb.ColumnKeys
	m.Timestamps = pb.Timestamps
	m.IndexCreatedAt = pb.IndexCreatedAt
	m.FieldCreatedAt = pb.FieldCreatedAt
	m.Clear = pb.Clear
}

func (s Serializer) decodeImportValueRequest(pb *pb.ImportValueRequest, m *pilosa.ImportValueRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.Shard = pb.Shard
	m.ColumnIDs = pb.ColumnIDs
	m.ColumnKeys = pb.ColumnKeys
	m.Values = pb.Values
	m.FloatValues = pb.FloatValues
	m.StringValues = pb.StringValues
	m.IndexCreatedAt = pb.IndexCreatedAt
	m.FieldCreatedAt = pb.FieldCreatedAt
	m.Clear = pb.Clear
}

func (s Serializer) decodeImportRoaringRequest(pb *pb.ImportRoaringRequest, m *pilosa.ImportRoaringRequest) {
	views := map[string][]byte{}
	for _, view := range pb.Views {
		views[view.Name] = view.Data
	}
	m.Clear = pb.Clear
	m.Action = pb.Action
	m.Block = int(pb.Block)
	m.Views = views
	m.IndexCreatedAt = pb.IndexCreatedAt
	m.FieldCreatedAt = pb.FieldCreatedAt
	m.UpdateExistence = pb.UpdateExistence
}

func (s Serializer) decodeImportResponse(pb *pb.ImportResponse, m *pilosa.ImportResponse) {
	m.Err = pb.Err
}

func (s Serializer) decodeBlockDataRequest(pb *pb.BlockDataRequest, m *pilosa.BlockDataRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.View = pb.View
	m.Shard = pb.Shard
	m.Block = pb.Block
}

func (s Serializer) decodeBlockDataResponse(pb *pb.BlockDataResponse, m *pilosa.BlockDataResponse) {
	m.RowIDs = pb.RowIDs
	m.ColumnIDs = pb.ColumnIDs
}

func (s Serializer) decodeQueryResponse(pb *pb.QueryResponse, m *pilosa.QueryResponse) {
	if pb.Err == "" {
		m.Err = nil
	} else {
		m.Err = errors.New(pb.Err)
	}
	m.Results = make([]interface{}, len(pb.Results))
	s.decodeQueryResults(pb.Results, m.Results)
}

func (s Serializer) decodeQueryResults(pb []*pb.QueryResult, m []interface{}) {
	for i := range pb {
		m[i] = s.decodeQueryResult(pb[i])
	}
}

func (s Serializer) decodeTranslateKeysRequest(pb *pb.TranslateKeysRequest, m *pilosa.TranslateKeysRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.Keys = pb.Keys
	m.NotWritable = pb.NotWritable
}

func (s Serializer) decodeTranslateKeysResponse(pb *pb.TranslateKeysResponse, m *pilosa.TranslateKeysResponse) {
	m.IDs = pb.IDs
}

func (s Serializer) decodeTranslateIDsRequest(pb *pb.TranslateIDsRequest, m *pilosa.TranslateIDsRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.IDs = pb.IDs
}

func (s Serializer) decodeTranslateIDsResponse(pb *pb.TranslateIDsResponse, m *pilosa.TranslateIDsResponse) {
	m.Keys = pb.Keys
}

func decodeTransactionMessage(pb *pb.TransactionMessage, m *pilosa.TransactionMessage) {
	m.Action = pb.Action
	if pb.Transaction == nil {
		m.Transaction = nil
		return
	} else if m.Transaction == nil {
		m.Transaction = &pilosa.Transaction{}
	}
	decodeTransaction(pb.Transaction, m.Transaction)
}

func (s Serializer) decodeAtomicRecord(pb *pb.AtomicRecord, m *pilosa.AtomicRecord) {
	m.Index = pb.Index
	m.Shard = pb.Shard
	m.Ivr = make([]*pilosa.ImportValueRequest, len(pb.Ivr))
	m.Ir = make([]*pilosa.ImportRequest, len(pb.Ir))

	for i, ivr := range pb.Ivr {
		m.Ivr[i] = &pilosa.ImportValueRequest{}
		s.decodeImportValueRequest(ivr, m.Ivr[i])
	}
	for i, ir := range pb.Ir {
		m.Ir[i] = &pilosa.ImportRequest{}
		s.decodeImportRequest(ir, m.Ir[i])
	}
}

func (s Serializer) decodeRowMatrix(pb *pb.RowMatrix) []*pilosa.Row {
	rows := make([]*pilosa.Row, len(pb.Rows))
	for i, r := range pb.Rows {
		rows[i] = s.decodeRow(r)
	}
	return rows
}

func (s Serializer) decodeDistinctTimestamp(pb *pb.DistinctTimestamp) pilosa.DistinctTimestamp {
	return pilosa.DistinctTimestamp{
		Values: pb.Values,
		Name:   pb.Name,
	}
}

func decodeTransaction(pb *pb.Transaction, trns *pilosa.Transaction) {
	trns.ID = pb.ID
	trns.Active = pb.Active
	trns.Exclusive = pb.Exclusive
	trns.Timeout = time.Duration(pb.Timeout)
	trns.Deadline = time.Unix(0, pb.Deadline)
	// TODO: trns.Stats... once it has anything
}

// QueryResult types.
const (
	queryResultTypeNil uint32 = iota
	queryResultTypeRow
	queryResultTypePairs
	queryResultTypePairsField
	queryResultTypeValCount
	queryResultTypeUint64
	queryResultTypeBool
	queryResultTypeRowIDs
	queryResultTypeGroupCounts
	queryResultTypeRowIdentifiers
	queryResultTypePair
	queryResultTypePairField
	queryResultTypeRowMatrix
	queryResultTypeSignedRow
	queryResultTypeExtractedIDMatrix
	queryResultTypeExtractedTable
	queryResultTypeDistinctTimestamp
)

func (s Serializer) decodeQueryResult(pb *pb.QueryResult) interface{} {
	switch pb.Type {
	case queryResultTypeSignedRow:
		return s.decodeSignedRow(pb.SignedRow)
	case queryResultTypeRow:
		return s.decodeRow(pb.Row)
	case queryResultTypePairs:
		return s.decodePairs(pb.Pairs)
	case queryResultTypePairsField:
		return s.decodePairsField(pb.PairsField)
	case queryResultTypeValCount:
		return s.decodeValCount(pb.ValCount)
	case queryResultTypeUint64:
		return pb.N
	case queryResultTypeBool:
		return pb.Changed
	case queryResultTypeNil:
		return nil
	case queryResultTypeRowIDs:
		return pilosa.RowIDs(pb.RowIDs)
	case queryResultTypeRowIdentifiers:
		return s.decodeRowIdentifiers(pb.RowIdentifiers)
	case queryResultTypeGroupCounts:
		return s.decodeGroupCounts(pb.GroupCounts, pb.OldGroupCounts)
	case queryResultTypePair:
		return s.decodePair(pb.Pairs[0])
	case queryResultTypePairField:
		return s.decodePairField(pb.PairField)
	case queryResultTypeExtractedIDMatrix:
		return s.decodeExtractedIDMatrix(pb.ExtractedIDMatrix)
	case queryResultTypeExtractedTable:
		return s.decodeExtractedTable(pb.ExtractedTable)
	case queryResultTypeRowMatrix:
		return s.decodeRowMatrix(pb.RowMatrix)
	case queryResultTypeDistinctTimestamp:
		return s.decodeDistinctTimestamp(pb.DistinctTimestamp)
	}
	panic(fmt.Sprintf("unknown type: %d", pb.Type))
}

// s.decodeRow converts r from its pb.representation.
func (s Serializer) decodeRow(pr *pb.Row) *pilosa.Row {
	if pr == nil {
		return pilosa.NewRow()
	}

	var r *pilosa.Row
	if len(pr.Roaring) > 0 {
		r = pilosa.NewRowFromRoaring(pr.Roaring)
	} else {
		r = pilosa.NewRow()
		for _, v := range pr.Columns {
			r.SetBit(v)
		}
	}
	r.Keys = pr.Keys
	r.Index = pr.Index
	r.Field = pr.Field

	return r
}

func (s Serializer) decodeSignedRow(pr *pb.SignedRow) pilosa.SignedRow {
	if pr == nil {
		return pilosa.SignedRow{}
	}
	r := pilosa.SignedRow{
		Pos: s.decodeRow(pr.Pos),
		Neg: s.decodeRow(pr.Neg),
	}
	return r
}

func (s Serializer) decodeExtractedIDMatrix(m *pb.ExtractedIDMatrix) pilosa.ExtractedIDMatrix {
	cols := make([]pilosa.ExtractedIDColumn, len(m.Columns))
	for i, c := range m.Columns {
		rows := make([][]uint64, len(c.Vals))
		for j, r := range c.Vals {
			rows[j] = r.IDs
		}

		cols[i] = pilosa.ExtractedIDColumn{
			ColumnID: c.ID,
			Rows:     rows,
		}
	}

	return pilosa.ExtractedIDMatrix{
		Fields:  m.Fields,
		Columns: cols,
	}
}

func (s Serializer) decodeExtractedTable(t *pb.ExtractedTable) pilosa.ExtractedTable {
	fields := make([]pilosa.ExtractedTableField, len(t.Fields))
	for i, f := range t.Fields {
		fields[i] = pilosa.ExtractedTableField{
			Name: f.Name,
			Type: f.Type,
		}
	}

	columns := make([]pilosa.ExtractedTableColumn, len(t.Columns))
	for i, c := range t.Columns {
		var col pilosa.KeyOrID
		switch kid := c.KeyOrID.(type) {
		case *pb.ExtractedTableColumn_ID:
			col = pilosa.KeyOrID{
				ID: kid.ID,
			}
		case *pb.ExtractedTableColumn_Key:
			col = pilosa.KeyOrID{
				Keyed: true,
				Key:   kid.Key,
			}
		}

		rows := make([]interface{}, len(c.Values))
		for j, v := range rows {
			var val interface{}
			switch v := v.(type) {
			case *pb.ExtractedTableValue_IDs:
				val = v.IDs.IDs
			case *pb.ExtractedTableValue_Keys:
				val = v.Keys.Keys
			case *pb.ExtractedTableValue_BSIValue:
				val = v.BSIValue
			case *pb.ExtractedTableValue_MutexID:
				val = v.MutexID
			case *pb.ExtractedTableValue_MutexKey:
				val = v.MutexKey
			case *pb.ExtractedTableValue_Bool:
				val = v.Bool
			}

			rows[j] = val
		}

		columns[i] = pilosa.ExtractedTableColumn{
			Column: col,
			Rows:   rows,
		}
	}

	return pilosa.ExtractedTable{
		Fields:  fields,
		Columns: columns,
	}
}

func (s Serializer) decodeRowIdentifiers(a *pb.RowIdentifiers) *pilosa.RowIdentifiers {
	return &pilosa.RowIdentifiers{
		Rows: a.Rows,
		Keys: a.Keys,
	}
}

func (s Serializer) decodeGroupCounts(a *pb.GroupCounts, b []*pb.GroupCount) *pilosa.GroupCounts {
	// Workaround: If we get an old-style "[]*GroupCount", we translate it.
	if a == nil {
		a = &pb.GroupCounts{Aggregate: "", Groups: b}
	}
	other := make([]pilosa.GroupCount, len(a.Groups))
	for i, gc := range a.Groups {
		other[i] = pilosa.GroupCount{
			Group: s.decodeFieldRows(gc.Group),
			Count: gc.Count,
			// note: not renaming the `pb. structure members now
			// to avoid breaking protobuf interactions.
			Agg: gc.Agg,
		}
	}
	return pilosa.NewGroupCounts(a.Aggregate, other...)
}

func (s Serializer) decodeFieldRows(a []*pb.FieldRow) []pilosa.FieldRow {
	other := make([]pilosa.FieldRow, len(a))
	for i := range a {
		fr := a[i]
		other[i].Field = fr.Field
		if fr.Value != nil {
			other[i].Value = &fr.Value.Value
		} else if fr.RowKey == "" {
			other[i].RowID = fr.RowID
		} else {
			other[i].RowKey = fr.RowKey
		}
	}
	return other
}

func (s Serializer) decodePairs(a []*pb.Pair) []pilosa.Pair {
	other := make([]pilosa.Pair, len(a))
	for i := range a {
		other[i] = s.decodePair(a[i])
	}
	return other
}

func (s Serializer) decodePairsField(a *pb.PairsField) *pilosa.PairsField {
	other := &pilosa.PairsField{
		Pairs: make([]pilosa.Pair, len(a.Pairs)),
	}
	for i := range a.Pairs {
		other.Pairs[i] = s.decodePair(a.Pairs[i])
	}
	other.Field = a.Field
	return other
}

func (s Serializer) decodePair(pb *pb.Pair) pilosa.Pair {
	return pilosa.Pair{
		ID:    pb.ID,
		Key:   pb.Key,
		Count: pb.Count,
	}
}

func (s Serializer) decodePairField(pb *pb.PairField) pilosa.PairField {
	return pilosa.PairField{
		Pair: pilosa.Pair{
			ID:    pb.Pair.ID,
			Key:   pb.Pair.Key,
			Count: pb.Pair.Count,
		},
		Field: pb.Field,
	}
}

func (s Serializer) decodeValCount(pb *pb.ValCount) pilosa.ValCount {
	var t time.Time
	t, err := time.Parse(time.RFC3339Nano, pb.TimestampVal)
	if err != nil {
		t = time.Time{}
	}
	return pilosa.ValCount{
		Val:          pb.Val,
		FloatVal:     pb.FloatVal,
		DecimalVal:   s.decodeDecimalStruct(pb.DecimalVal),
		TimestampVal: t,
		Count:        pb.Count,
	}
}

func (s Serializer) decodeDecimalStruct(pb *pb.Decimal) *pql.Decimal {
	if pb == nil {
		return nil
	}
	return &pql.Decimal{
		Value: pb.Value,
		Scale: pb.Scale,
	}
}

func (s Serializer) encodeSignedRow(r pilosa.SignedRow) *pb.SignedRow {
	ir := &pb.SignedRow{
		Pos: s.encodeRow(r.Pos),
		Neg: s.encodeRow(r.Neg),
	}
	return ir
}

func (s Serializer) encodeRow(r *pilosa.Row) *pb.Row {
	if r == nil {
		return &pb.Row{} // Generated proto code doesn't like a nil Row.
	}

	ir := &pb.Row{
		Keys:  r.Keys,
		Index: r.Index,
		Field: r.Field,
	}
	if s.RoaringRows {
		ir.Roaring = r.Roaring()
	} else {
		ir.Columns = r.Columns()
	}
	return ir
}

func (s Serializer) encodeRowIdentifiers(r pilosa.RowIdentifiers) *pb.RowIdentifiers {
	return &pb.RowIdentifiers{
		Rows: r.Rows,
		Keys: r.Keys,
	}
}

func (s Serializer) encodeDistinctTimestamp(d pilosa.DistinctTimestamp) *pb.DistinctTimestamp {
	return &pb.DistinctTimestamp{
		Values: d.Values,
		Name:   d.Name,
	}
}

func (s Serializer) encodeGroupCounts(counts *pilosa.GroupCounts) *pb.GroupCounts {
	groups := counts.Groups()
	result := &pb.GroupCounts{
		Groups:    make([]*pb.GroupCount, len(groups)),
		Aggregate: counts.AggregateColumn(),
	}
	for i, gc := range groups {
		result.Groups[i] = &pb.GroupCount{
			Group: s.encodeFieldRows(gc.Group),
			Count: gc.Count,
			Agg:   gc.Agg,
		}
	}
	return result
}

func (s Serializer) encodeFieldRows(a []pilosa.FieldRow) []*pb.FieldRow {
	other := make([]*pb.FieldRow, len(a))
	for i := range a {
		fr := a[i]
		other[i] = &pb.FieldRow{Field: fr.Field}

		if fr.Value != nil {
			other[i].Value = &pb.Int64{Value: *fr.Value}
		} else if fr.RowKey == "" {
			other[i].RowID = fr.RowID
		} else {
			other[i].RowKey = fr.RowKey
		}
	}
	return other
}

func (s Serializer) endcodeExtractedIDMatrix(m pilosa.ExtractedIDMatrix) *pb.ExtractedIDMatrix {
	cols := make([]*pb.ExtractedIDColumn, len(m.Columns))
	for i, v := range m.Columns {
		vals := make([]*pb.IDList, len(v.Rows))
		for j, f := range v.Rows {
			vals[j] = &pb.IDList{IDs: f}
		}
		cols[i] = &pb.ExtractedIDColumn{
			ID:   v.ColumnID,
			Vals: vals,
		}
	}
	return &pb.ExtractedIDMatrix{
		Fields:  m.Fields,
		Columns: cols,
	}
}

func (s Serializer) encodeExtractedTable(t pilosa.ExtractedTable) *pb.ExtractedTable {
	fields := make([]*pb.ExtractedTableField, len(t.Fields))
	for i, f := range t.Fields {
		fields[i] = &pb.ExtractedTableField{
			Name: f.Name,
			Type: f.Type,
		}
	}

	cols := make([]*pb.ExtractedTableColumn, len(t.Columns))
	for i, c := range t.Columns {
		var col pb.ExtractedTableColumn
		if c.Column.Keyed {
			col.KeyOrID = &pb.ExtractedTableColumn_Key{Key: c.Column.Key}
		} else {
			col.KeyOrID = &pb.ExtractedTableColumn_ID{ID: c.Column.ID}
		}

		rows := make([]*pb.ExtractedTableValue, len(c.Rows))
		for j, v := range c.Rows {
			switch v := v.(type) {
			case []uint64:
				rows[j] = &pb.ExtractedTableValue{
					Value: &pb.ExtractedTableValue_IDs{
						IDs: &pb.IDList{
							IDs: v,
						},
					},
				}
			case []string:
				rows[j] = &pb.ExtractedTableValue{
					Value: &pb.ExtractedTableValue_Keys{
						Keys: &pb.KeyList{
							Keys: v,
						},
					},
				}
			case int64:
				rows[j] = &pb.ExtractedTableValue{
					Value: &pb.ExtractedTableValue_BSIValue{
						BSIValue: v,
					},
				}
			case uint64:
				rows[j] = &pb.ExtractedTableValue{
					Value: &pb.ExtractedTableValue_MutexID{
						MutexID: v,
					},
				}
			case string:
				rows[j] = &pb.ExtractedTableValue{
					Value: &pb.ExtractedTableValue_MutexKey{
						MutexKey: v,
					},
				}
			case bool:
				rows[j] = &pb.ExtractedTableValue{
					Value: &pb.ExtractedTableValue_Bool{
						Bool: v,
					},
				}
			}
		}
		col.Values = rows

		cols[i] = &col
	}

	return &pb.ExtractedTable{
		Fields:  fields,
		Columns: cols,
	}
}

func (s Serializer) encodePairs(a pilosa.Pairs) []*pb.Pair {
	other := make([]*pb.Pair, len(a))
	for i := range a {
		other[i] = s.encodePair(a[i])
	}
	return other
}

func (s Serializer) encodePairsField(a *pilosa.PairsField) *pb.PairsField {
	other := &pb.PairsField{
		Pairs: make([]*pb.Pair, len(a.Pairs)),
	}
	for i := range a.Pairs {
		other.Pairs[i] = s.encodePair(a.Pairs[i])
	}
	other.Field = a.Field
	return other
}

func (s Serializer) encodePair(p pilosa.Pair) *pb.Pair {
	return &pb.Pair{
		ID:    p.ID,
		Key:   p.Key,
		Count: p.Count,
	}
}

func (s Serializer) encodePairField(p pilosa.PairField) *pb.PairField {
	return &pb.PairField{
		Pair:  s.encodePair(p.Pair),
		Field: p.Field,
	}
}

func (s Serializer) encodeValCount(vc pilosa.ValCount) *pb.ValCount {
	return &pb.ValCount{
		Val:          vc.Val,
		FloatVal:     vc.FloatVal,
		DecimalVal:   s.encodeDecimal(vc.DecimalVal),
		Count:        vc.Count,
		TimestampVal: vc.TimestampVal.Format(time.RFC3339Nano),
	}
}

func (s Serializer) encodeDecimal(p *pql.Decimal) *pb.Decimal {
	if p == nil {
		return nil
	}
	return &pb.Decimal{
		Value: p.Value,
		Scale: p.Scale,
	}
}

func (s Serializer) encodeResizeNodeMessage(m *pilosa.ResizeNodeMessage) *pb.ResizeNodeMessage {
	return &pb.ResizeNodeMessage{
		NodeID: m.NodeID,
		Action: m.Action,
	}
}

func (s Serializer) encodeResizeAbortMessage(*pilosa.ResizeAbortMessage) *pb.ResizeAbortMessage {
	return &pb.ResizeAbortMessage{}
}

func decodeResizeNodeMessage(pb *pb.ResizeNodeMessage, m *pilosa.ResizeNodeMessage) {
	m.NodeID = pb.NodeID
	m.Action = pb.Action
}

func decodeResizeAbortMessage(pb *pb.ResizeAbortMessage, m *pilosa.ResizeAbortMessage) {

}

func (s Serializer) decodeShardedIngestRequest(req *pb.ShardedIngestRequest) (*ingest.ShardedRequest, error) {
	if req == nil || len(req.Ops) == 0 {
		return &ingest.ShardedRequest{}, nil
	}
	out := &ingest.ShardedRequest{Ops: make(map[uint64][]*ingest.Operation, len(req.Ops))}
	for shard, ops := range req.Ops {
		var err error
		out.Ops[shard], err = s.decodeShardIngestOperations(ops)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s Serializer) decodeShardIngestOperations(ops *pb.ShardIngestOperations) ([]*ingest.Operation, error) {
	out := []*ingest.Operation{}
	if len(ops.Ops) == 0 {
		return out, nil
	}
	for _, op := range ops.Ops {
		if op == nil {
			continue
		}
		decoded, err := s.decodeShardIngestOperation(op)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func (s Serializer) decodeShardIngestOperation(op *pb.ShardIngestOperation) (*ingest.Operation, error) {
	opType, err := ingest.ParseOpType(op.OpType)
	if err != nil {
		return nil, err
	}
	out := &ingest.Operation{
		OpType:         opType,
		ClearRecordIDs: op.ClearRecordIDs,
		ClearFields:    op.ClearFields,
		FieldOps:       make(map[string]*ingest.FieldOperation, len(op.FieldOps)),
	}
	for k, v := range op.FieldOps {
		out.FieldOps[k] = &ingest.FieldOperation{
			RecordIDs: v.RecordIDs,
			Values:    v.Values,
			Signed:    v.Signed,
		}
	}
	return out, nil
}
