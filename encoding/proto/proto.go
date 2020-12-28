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

package proto

import (
	"fmt"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/internal"
	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
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
		msg := &internal.CreateShardMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateShardMessage")
		}
		s.decodeCreateShardMessage(msg, mt)
		return nil
	case *pilosa.CreateIndexMessage:
		msg := &internal.CreateIndexMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateIndexMessage")
		}
		s.decodeCreateIndexMessage(msg, mt)
		return nil
	case *pilosa.DeleteIndexMessage:
		msg := &internal.DeleteIndexMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteIndexMessage")
		}
		s.decodeDeleteIndexMessage(msg, mt)
		return nil
	case *pilosa.CreateFieldMessage:
		msg := &internal.CreateFieldMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateFieldMessage")
		}
		s.decodeCreateFieldMessage(msg, mt)
		return nil
	case *pilosa.DeleteFieldMessage:
		msg := &internal.DeleteFieldMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteFieldMessage")
		}
		s.decodeDeleteFieldMessage(msg, mt)
		return nil
	case *pilosa.DeleteAvailableShardMessage:
		msg := &internal.DeleteAvailableShardMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteAvailableShardMessage")
		}
		s.decodeDeleteAvailableShardMessage(msg, mt)
		return nil
	case *pilosa.CreateViewMessage:
		msg := &internal.CreateViewMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateViewMessage")
		}
		s.decodeCreateViewMessage(msg, mt)
		return nil
	case *pilosa.DeleteViewMessage:
		msg := &internal.DeleteViewMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteViewMessage")
		}
		s.decodeDeleteViewMessage(msg, mt)
		return nil
	case *pilosa.ClusterStatus:
		msg := &internal.ClusterStatus{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ClusterStatus")
		}
		s.decodeClusterStatus(msg, mt)
		return nil
	case *pilosa.ResizeInstruction:
		msg := &internal.ResizeInstruction{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ResizeInstruction")
		}
		s.decodeResizeInstruction(msg, mt)
		return nil
	case *pilosa.ResizeInstructionComplete:
		msg := &internal.ResizeInstructionComplete{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ResizeInstructionComplete")
		}
		s.decodeResizeInstructionComplete(msg, mt)
		return nil
	case *pilosa.SetCoordinatorMessage:
		msg := &internal.SetCoordinatorMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling SetCoordinatorMessage")
		}
		s.decodeSetCoordinatorMessage(msg, mt)
		return nil
	case *pilosa.UpdateCoordinatorMessage:
		msg := &internal.UpdateCoordinatorMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling UpdateCoordinatorMessage")
		}
		s.decodeUpdateCoordinatorMessage(msg, mt)
		return nil
	case *pilosa.NodeStateMessage:
		msg := &internal.NodeStateMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling NodeStateMessage")
		}
		s.decodeNodeStateMessage(msg, mt)
		return nil
	case *pilosa.RecalculateCaches:
		msg := &internal.RecalculateCaches{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling RecalculateCaches")
		}
		s.decodeRecalculateCaches(msg, mt)
		return nil
	case *pilosa.NodeEvent:
		msg := &internal.NodeEventMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling NodeEvent")
		}
		s.decodeNodeEventMessage(msg, mt)
		return nil
	case *pilosa.NodeStatus:
		msg := &internal.NodeStatus{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling NodeStatus")
		}
		s.decodeNodeStatus(msg, mt)
		return nil
	case *pilosa.Node:
		msg := &internal.Node{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling Node")
		}
		s.decodeNode(msg, mt)
		return nil
	case *pilosa.QueryRequest:
		msg := &internal.QueryRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling QueryRequest")
		}
		s.decodeQueryRequest(msg, mt)
		return nil
	case *pilosa.QueryResponse:
		msg := &internal.QueryResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling QueryResponse")
		}
		s.decodeQueryResponse(msg, mt)
		return nil
	case *pilosa.ImportRequest:
		msg := &internal.ImportRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportRequest")
		}
		s.decodeImportRequest(msg, mt)
		return nil
	case *pilosa.ImportValueRequest:
		msg := &internal.ImportValueRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportValueRequest")
		}
		s.decodeImportValueRequest(msg, mt)
		return nil
	case *pilosa.ImportRoaringRequest:
		msg := &internal.ImportRoaringRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportRoaringRequest")
		}
		s.decodeImportRoaringRequest(msg, mt)
		return nil
	case *pilosa.ImportColumnAttrsRequest:
		msg := &internal.ImportColumnAttrsRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportColumnAttrsRequest")
		}
		s.decodeImportColumnAttrsRequest(msg, mt)
		return nil
	case *pilosa.ImportResponse:
		msg := &internal.ImportResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportResponse")
		}
		s.decodeImportResponse(msg, mt)
		return nil
	case *pilosa.BlockDataRequest:
		msg := &internal.BlockDataRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling BlockDataRequest")
		}
		s.decodeBlockDataRequest(msg, mt)
		return nil
	case *pilosa.BlockDataResponse:
		msg := &internal.BlockDataResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling BlockDataResponse")
		}
		s.decodeBlockDataResponse(msg, mt)
		return nil
	case *pilosa.TranslateKeysRequest:
		msg := &internal.TranslateKeysRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling TranslateKeysRequest")
		}
		s.decodeTranslateKeysRequest(msg, mt)
		return nil
	case *pilosa.TranslateKeysResponse:
		msg := &internal.TranslateKeysResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling TranslateKeysResponse")
		}
		s.decodeTranslateKeysResponse(msg, mt)
		return nil
	case *pilosa.TranslateIDsRequest:
		msg := &internal.TranslateIDsRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling TranslateIDsRequest")
		}
		s.decodeTranslateIDsRequest(msg, mt)
		return nil
	case *pilosa.TranslateIDsResponse:
		msg := &internal.TranslateIDsResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling TranslateIDsResponse")
		}
		s.decodeTranslateIDsResponse(msg, mt)
		return nil
	case *pilosa.TransactionMessage:
		msg := &internal.TransactionMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling TransactionMessage")
		}
		decodeTransactionMessage(msg, mt)
		return nil
	case *pilosa.AtomicRecord:
		msg := &internal.AtomicRecord{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling AtomicRecord")
		}
		s.decodeAtomicRecord(msg, mt)
		return nil
	case *[]*pilosa.Row:
		msg := &internal.RowMatrix{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling RowMatrix")
		}
		*mt = s.decodeRowMatrix(msg)
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
	case *pilosa.SetCoordinatorMessage:
		return s.encodeSetCoordinatorMessage(mt)
	case *pilosa.UpdateCoordinatorMessage:
		return s.encodeUpdateCoordinatorMessage(mt)
	case *pilosa.NodeStateMessage:
		return s.encodeNodeStateMessage(mt)
	case *pilosa.RecalculateCaches:
		return s.encodeRecalculateCaches(mt)
	case *pilosa.NodeEvent:
		return s.encodeNodeEventMessage(mt)
	case *pilosa.NodeStatus:
		return s.encodeNodeStatus(mt)
	case *pilosa.Node:
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
	case *pilosa.ImportColumnAttrsRequest:
		return s.encodeImportColumnAttrsRequest(mt)
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
	}
	return nil
}

func (s Serializer) encodeBlockDataRequest(m *pilosa.BlockDataRequest) *internal.BlockDataRequest {
	return &internal.BlockDataRequest{
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
		Shard: m.Shard,
		Block: m.Block,
	}
}
func (s Serializer) encodeBlockDataResponse(m *pilosa.BlockDataResponse) *internal.BlockDataResponse {
	return &internal.BlockDataResponse{
		RowIDs:    m.RowIDs,
		ColumnIDs: m.ColumnIDs,
	}
}

func (s Serializer) encodeImportResponse(m *pilosa.ImportResponse) *internal.ImportResponse {
	return &internal.ImportResponse{
		Err: m.Err,
	}
}

func (s Serializer) encodeImportRequest(m *pilosa.ImportRequest) *internal.ImportRequest {
	return &internal.ImportRequest{
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

func (s Serializer) encodeImportValueRequest(m *pilosa.ImportValueRequest) *internal.ImportValueRequest {
	return &internal.ImportValueRequest{
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

func (s Serializer) encodeImportRoaringRequest(m *pilosa.ImportRoaringRequest) *internal.ImportRoaringRequest {
	views := make([]*internal.ImportRoaringRequestView, len(m.Views))
	i := 0
	for viewName, viewData := range m.Views {
		views[i] = &internal.ImportRoaringRequestView{
			Name: viewName,
			Data: viewData,
		}
		i++
	}
	return &internal.ImportRoaringRequest{
		IndexCreatedAt: m.IndexCreatedAt,
		FieldCreatedAt: m.FieldCreatedAt,
		Clear:          m.Clear,
		Action:         m.Action,
		Block:          uint64(m.Block),
		Views:          views,
	}
}

func (s Serializer) encodeImportColumnAttrsRequest(m *pilosa.ImportColumnAttrsRequest) *internal.ImportColumnAttrsRequest {
	return &internal.ImportColumnAttrsRequest{
		Index:          m.Index,
		IndexCreatedAt: m.IndexCreatedAt,
		Shard:          m.Shard,
		AttrKey:        m.AttrKey,
		AttrVals:       m.AttrVals,
		ColumnIDs:      m.ColumnIDs,
	}
}

func (s Serializer) encodeQueryRequest(m *pilosa.QueryRequest) *internal.QueryRequest {
	r := &internal.QueryRequest{
		Query:           m.Query,
		Shards:          m.Shards,
		ColumnAttrs:     m.ColumnAttrs,
		Remote:          m.Remote,
		ExcludeRowAttrs: m.ExcludeRowAttrs,
		ExcludeColumns:  m.ExcludeColumns,
		EmbeddedData:    make([]*internal.Row, len(m.EmbeddedData)),
	}
	for i := range m.EmbeddedData {
		r.EmbeddedData[i] = s.encodeRow(m.EmbeddedData[i])
	}
	return r
}

func (s Serializer) encodeQueryResponse(m *pilosa.QueryResponse) *internal.QueryResponse {
	pb := &internal.QueryResponse{
		Results:        make([]*internal.QueryResult, len(m.Results)),
		ColumnAttrSets: s.encodeColumnAttrSets(m.ColumnAttrSets),
	}

	for i := range m.Results {
		pb.Results[i] = &internal.QueryResult{}

		switch result := m.Results[i].(type) {
		case pilosa.SignedRow:
			pb.Results[i].Type = queryResultTypeSignedRow
			pb.Results[i].SignedRow = s.encodeSignedRow(result)
		case *pilosa.Row:
			pb.Results[i].Type = queryResultTypeRow
			pb.Results[i].Row = s.encodeRow(result)
		case []pilosa.Pair:
			pb.Results[i].Type = queryResultTypePairs
			pb.Results[i].Pairs = s.encodePairs(result)
		case *pilosa.PairsField:
			pb.Results[i].Type = queryResultTypePairsField
			pb.Results[i].PairsField = s.encodePairsField(result)
		case pilosa.ValCount:
			pb.Results[i].Type = queryResultTypeValCount
			pb.Results[i].ValCount = s.encodeValCount(result)
		case uint64:
			pb.Results[i].Type = queryResultTypeUint64
			pb.Results[i].N = result
		case bool:
			pb.Results[i].Type = queryResultTypeBool
			pb.Results[i].Changed = result
		case pilosa.RowIDs:
			pb.Results[i].Type = queryResultTypeRowIDs
			pb.Results[i].RowIDs = result
		case pilosa.ExtractedIDMatrix:
			pb.Results[i].Type = queryResultTypeExtractedIDMatrix
			pb.Results[i].ExtractedIDMatrix = s.endcodeExtractedIDMatrix(result)
		case []pilosa.GroupCount:
			pb.Results[i].Type = queryResultTypeGroupCounts
			pb.Results[i].GroupCounts = s.encodeGroupCounts(result)
		case pilosa.RowIdentifiers:
			pb.Results[i].Type = queryResultTypeRowIdentifiers
			pb.Results[i].RowIdentifiers = s.encodeRowIdentifiers(result)
		case pilosa.ExtractedTable:
			pb.Results[i].Type = queryResultTypeExtractedTable
			pb.Results[i].ExtractedTable = s.encodeExtractedTable(result)
		case pilosa.Pair:
			pb.Results[i].Type = queryResultTypePair
			pb.Results[i].Pairs = []*internal.Pair{s.encodePair(result)}
		case pilosa.PairField:
			pb.Results[i].Type = queryResultTypePairField
			pb.Results[i].PairField = s.encodePairField(result)
		case []*pilosa.Row:
			pb.Results[i].Type = queryResultTypeRowMatrix
			pb.Results[i].RowMatrix = s.encodeRowMatrix(result)
		case nil:
			pb.Results[i].Type = queryResultTypeNil
		default:
			panic(fmt.Errorf("unknown type: %T", m.Results[i]))
		}
	}

	if m.Err != nil {
		pb.Err = m.Err.Error()
	}

	return pb
}

func (s Serializer) encodeResizeInstruction(m *pilosa.ResizeInstruction) *internal.ResizeInstruction {
	return &internal.ResizeInstruction{
		JobID:              m.JobID,
		Node:               s.encodeNode(m.Node),
		Coordinator:        s.encodeNode(m.Coordinator),
		Sources:            s.encodeResizeSources(m.Sources),
		TranslationSources: s.encodeTranslationResizeSources(m.TranslationSources),
		NodeStatus:         s.encodeNodeStatus(m.NodeStatus),
		ClusterStatus:      s.encodeClusterStatus(m.ClusterStatus),
	}
}

func (s Serializer) encodeResizeSources(srcs []*pilosa.ResizeSource) []*internal.ResizeSource {
	new := make([]*internal.ResizeSource, 0, len(srcs))
	for _, src := range srcs {
		new = append(new, s.encodeResizeSource(src))
	}
	return new
}

func (s Serializer) encodeResizeSource(m *pilosa.ResizeSource) *internal.ResizeSource {
	return &internal.ResizeSource{
		Node:  s.encodeNode(m.Node),
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
		Shard: m.Shard,
	}
}

func (s Serializer) encodeTranslationResizeSources(srcs []*pilosa.TranslationResizeSource) []*internal.TranslationResizeSource {
	new := make([]*internal.TranslationResizeSource, 0, len(srcs))
	for _, src := range srcs {
		new = append(new, s.encodeTranslationResizeSource(src))
	}
	return new
}

func (s Serializer) encodeTranslationResizeSource(m *pilosa.TranslationResizeSource) *internal.TranslationResizeSource {
	return &internal.TranslationResizeSource{
		Node:        s.encodeNode(m.Node),
		Index:       m.Index,
		PartitionID: int32(m.PartitionID),
	}
}

func (s Serializer) encodeSchema(m *pilosa.Schema) *internal.Schema {
	return &internal.Schema{
		Indexes: s.encodeIndexInfos(m.Indexes),
	}
}

func (s Serializer) encodeIndexInfos(idxs []*pilosa.IndexInfo) []*internal.Index {
	new := make([]*internal.Index, 0, len(idxs))
	for _, idx := range idxs {
		new = append(new, s.encodeIndexInfo(idx))
	}
	return new
}

func (s Serializer) encodeIndexInfo(idx *pilosa.IndexInfo) *internal.Index {
	return &internal.Index{
		Name:      idx.Name,
		CreatedAt: idx.CreatedAt,
		Options:   s.encodeIndexMeta(&idx.Options),
		Fields:    s.encodeFieldInfos(idx.Fields),
	}
}

func (s Serializer) encodeFieldInfos(fs []*pilosa.FieldInfo) []*internal.Field {
	new := make([]*internal.Field, 0, len(fs))
	for _, f := range fs {
		new = append(new, s.encodeFieldInfo(f))
	}
	return new
}

func (s Serializer) encodeFieldInfo(f *pilosa.FieldInfo) *internal.Field {
	ifield := &internal.Field{
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

func (s Serializer) encodeFieldOptions(o *pilosa.FieldOptions) *internal.FieldOptions {
	if o == nil {
		return nil
	}
	return &internal.FieldOptions{
		Type:         o.Type,
		CacheType:    o.CacheType,
		CacheSize:    o.CacheSize,
		Min:          &internal.Decimal{Value: o.Min.Value, Scale: o.Min.Scale},
		Max:          &internal.Decimal{Value: o.Max.Value, Scale: o.Max.Scale},
		Base:         o.Base,
		Scale:        o.Scale,
		BitDepth:     uint64(o.BitDepth),
		TimeQuantum:  string(o.TimeQuantum),
		Keys:         o.Keys,
		ForeignIndex: o.ForeignIndex,
	}
}

// s.encodeNodes converts a slice of Nodes into its internal representation.
func (s Serializer) encodeNodes(a []*pilosa.Node) []*internal.Node {
	other := make([]*internal.Node, len(a))
	for i := range a {
		other[i] = s.encodeNode(a[i])
	}
	return other
}

// s.encodeNode converts a Node into its internal representation.
func (s Serializer) encodeNode(n *pilosa.Node) *internal.Node {
	return &internal.Node{
		ID:            n.ID,
		URI:           s.encodeURI(n.URI),
		IsCoordinator: n.IsCoordinator,
		State:         n.State,
		GRPCURI:       s.encodeURI(n.GRPCURI),
	}
}

func (s Serializer) encodeURI(u pilosa.URI) *internal.URI {
	return &internal.URI{
		Scheme: u.Scheme,
		Host:   u.Host,
		Port:   uint32(u.Port),
	}
}

func (s Serializer) encodeClusterStatus(m *pilosa.ClusterStatus) *internal.ClusterStatus {
	return &internal.ClusterStatus{
		State:     m.State,
		ClusterID: m.ClusterID,
		Nodes:     s.encodeNodes(m.Nodes),
		Schema:    s.encodeSchema(m.Schema),
	}
}

func (s Serializer) encodeCreateShardMessage(m *pilosa.CreateShardMessage) *internal.CreateShardMessage {
	return &internal.CreateShardMessage{
		Index: m.Index,
		Field: m.Field,
		Shard: m.Shard,
	}
}

func (s Serializer) encodeCreateIndexMessage(m *pilosa.CreateIndexMessage) *internal.CreateIndexMessage {
	return &internal.CreateIndexMessage{
		Index:     m.Index,
		CreatedAt: m.CreatedAt,
		Meta:      s.encodeIndexMeta(m.Meta),
	}
}

func (s Serializer) encodeIndexMeta(m *pilosa.IndexOptions) *internal.IndexMeta {
	return &internal.IndexMeta{
		Keys:           m.Keys,
		TrackExistence: m.TrackExistence,
	}
}

func (s Serializer) encodeDeleteIndexMessage(m *pilosa.DeleteIndexMessage) *internal.DeleteIndexMessage {
	return &internal.DeleteIndexMessage{
		Index: m.Index,
	}
}

func (s Serializer) encodeCreateFieldMessage(m *pilosa.CreateFieldMessage) *internal.CreateFieldMessage {
	return &internal.CreateFieldMessage{
		Index:     m.Index,
		Field:     m.Field,
		CreatedAt: m.CreatedAt,
		Meta:      s.encodeFieldOptions(m.Meta),
	}
}

func (s Serializer) encodeDeleteFieldMessage(m *pilosa.DeleteFieldMessage) *internal.DeleteFieldMessage {
	return &internal.DeleteFieldMessage{
		Index: m.Index,
		Field: m.Field,
	}
}

func (s Serializer) encodeDeleteAvailableShardMessage(m *pilosa.DeleteAvailableShardMessage) *internal.DeleteAvailableShardMessage {
	return &internal.DeleteAvailableShardMessage{
		Index:   m.Index,
		Field:   m.Field,
		ShardID: m.ShardID,
	}
}

func (s Serializer) encodeCreateViewMessage(m *pilosa.CreateViewMessage) *internal.CreateViewMessage {
	return &internal.CreateViewMessage{
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
	}
}

func (s Serializer) encodeDeleteViewMessage(m *pilosa.DeleteViewMessage) *internal.DeleteViewMessage {
	return &internal.DeleteViewMessage{
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
	}
}

func (s Serializer) encodeResizeInstructionComplete(m *pilosa.ResizeInstructionComplete) *internal.ResizeInstructionComplete {
	return &internal.ResizeInstructionComplete{
		JobID: m.JobID,
		Node:  s.encodeNode(m.Node),
		Error: m.Error,
	}
}

func (s Serializer) encodeSetCoordinatorMessage(m *pilosa.SetCoordinatorMessage) *internal.SetCoordinatorMessage {
	return &internal.SetCoordinatorMessage{
		New: s.encodeNode(m.New),
	}
}

func (s Serializer) encodeUpdateCoordinatorMessage(m *pilosa.UpdateCoordinatorMessage) *internal.UpdateCoordinatorMessage {
	return &internal.UpdateCoordinatorMessage{
		New: s.encodeNode(m.New),
	}
}

func (s Serializer) encodeNodeStateMessage(m *pilosa.NodeStateMessage) *internal.NodeStateMessage {
	return &internal.NodeStateMessage{
		NodeID: m.NodeID,
		State:  m.State,
	}
}

func (s Serializer) encodeNodeEventMessage(m *pilosa.NodeEvent) *internal.NodeEventMessage {
	return &internal.NodeEventMessage{
		Event: uint32(m.Event),
		Node:  s.encodeNode(m.Node),
	}
}

func (s Serializer) encodeNodeStatus(m *pilosa.NodeStatus) *internal.NodeStatus {
	return &internal.NodeStatus{
		Node:    s.encodeNode(m.Node),
		Indexes: s.encodeIndexStatuses(m.Indexes),
		Schema:  s.encodeSchema(m.Schema),
	}
}

func (s Serializer) encodeIndexStatus(m *pilosa.IndexStatus) *internal.IndexStatus {
	return &internal.IndexStatus{
		Name:      m.Name,
		CreatedAt: m.CreatedAt,
		Fields:    s.encodeFieldStatuses(m.Fields),
	}
}

func (s Serializer) encodeIndexStatuses(a []*pilosa.IndexStatus) []*internal.IndexStatus {
	other := make([]*internal.IndexStatus, len(a))
	for i := range a {
		other[i] = s.encodeIndexStatus(a[i])
	}
	return other
}

func (s Serializer) encodeFieldStatus(m *pilosa.FieldStatus) *internal.FieldStatus {
	return &internal.FieldStatus{
		Name:            m.Name,
		CreatedAt:       m.CreatedAt,
		AvailableShards: m.AvailableShards.Slice(),
	}
}

func (s Serializer) encodeFieldStatuses(a []*pilosa.FieldStatus) []*internal.FieldStatus {
	other := make([]*internal.FieldStatus, len(a))
	for i := range a {
		other[i] = s.encodeFieldStatus(a[i])
	}
	return other
}

func (s Serializer) encodeRecalculateCaches(*pilosa.RecalculateCaches) *internal.RecalculateCaches {
	return &internal.RecalculateCaches{}
}

func (s Serializer) encodeTranslateKeysRequest(request *pilosa.TranslateKeysRequest) *internal.TranslateKeysRequest {
	return &internal.TranslateKeysRequest{
		Index:       request.Index,
		Field:       request.Field,
		Keys:        request.Keys,
		NotWritable: request.NotWritable,
	}
}

func (s Serializer) encodeTranslateKeysResponse(response *pilosa.TranslateKeysResponse) *internal.TranslateKeysResponse {
	return &internal.TranslateKeysResponse{
		IDs: response.IDs,
	}
}

func (s Serializer) encodeTranslateIDsRequest(request *pilosa.TranslateIDsRequest) *internal.TranslateIDsRequest {
	return &internal.TranslateIDsRequest{
		Index: request.Index,
		Field: request.Field,
		IDs:   request.IDs,
	}
}

func (s Serializer) encodeTranslateIDsResponse(response *pilosa.TranslateIDsResponse) *internal.TranslateIDsResponse {
	return &internal.TranslateIDsResponse{
		Keys: response.Keys,
	}
}

func (s Serializer) encodeTransactionMessage(msg *pilosa.TransactionMessage) *internal.TransactionMessage {
	return &internal.TransactionMessage{
		Action:      msg.Action,
		Transaction: s.encodeTransaction(msg.Transaction),
	}
}

func (s Serializer) encodeAtomicRecord(msg *pilosa.AtomicRecord) *internal.AtomicRecord {
	ar := &internal.AtomicRecord{
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

func (s Serializer) encodeRowMatrix(msg []*pilosa.Row) *internal.RowMatrix {
	rows := make([]*internal.Row, len(msg))
	for i, r := range msg {
		rows[i] = s.encodeRow(r)
	}

	return &internal.RowMatrix{Rows: rows}
}

func (s Serializer) encodeTransaction(trns *pilosa.Transaction) *internal.Transaction {
	if trns == nil {
		return nil
	}
	return &internal.Transaction{
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

func (s Serializer) encodeTransactionStats(stats pilosa.TransactionStats) *internal.TransactionStats {
	return &internal.TransactionStats{}
}

func (s Serializer) decodeResizeInstruction(ri *internal.ResizeInstruction, m *pilosa.ResizeInstruction) {
	m.JobID = ri.JobID
	m.Node = &pilosa.Node{}
	s.decodeNode(ri.Node, m.Node)
	m.Coordinator = &pilosa.Node{}
	s.decodeNode(ri.Coordinator, m.Coordinator)
	m.Sources = make([]*pilosa.ResizeSource, len(ri.Sources))
	s.decodeResizeSources(ri.Sources, m.Sources)
	m.TranslationSources = make([]*pilosa.TranslationResizeSource, len(ri.TranslationSources))
	s.decodeTranslationResizeSources(ri.TranslationSources, m.TranslationSources)
	m.NodeStatus = &pilosa.NodeStatus{}
	s.decodeNodeStatus(ri.NodeStatus, m.NodeStatus)
	m.ClusterStatus = &pilosa.ClusterStatus{}
	s.decodeClusterStatus(ri.ClusterStatus, m.ClusterStatus)
}

func (s Serializer) decodeResizeSources(srcs []*internal.ResizeSource, m []*pilosa.ResizeSource) {
	for i := range srcs {
		m[i] = &pilosa.ResizeSource{}
		s.decodeResizeSource(srcs[i], m[i])
	}
}

func (s Serializer) decodeResizeSource(rs *internal.ResizeSource, m *pilosa.ResizeSource) {
	m.Node = &pilosa.Node{}
	s.decodeNode(rs.Node, m.Node)
	m.Index = rs.Index
	m.Field = rs.Field
	m.View = rs.View
	m.Shard = rs.Shard
}

func (s Serializer) decodeTranslationResizeSources(srcs []*internal.TranslationResizeSource, m []*pilosa.TranslationResizeSource) {
	for i := range srcs {
		m[i] = &pilosa.TranslationResizeSource{}
		s.decodeTranslationResizeSource(srcs[i], m[i])
	}
}

func (s Serializer) decodeTranslationResizeSource(rs *internal.TranslationResizeSource, m *pilosa.TranslationResizeSource) {
	m.Node = &pilosa.Node{}
	s.decodeNode(rs.Node, m.Node)
	m.Index = rs.Index
	m.PartitionID = int(rs.PartitionID)
}

func (s Serializer) decodeSchema(sc *internal.Schema, m *pilosa.Schema) {
	m.Indexes = make([]*pilosa.IndexInfo, len(sc.Indexes))
	s.decodeIndexes(sc.Indexes, m.Indexes)
}

func (s Serializer) decodeIndexes(idxs []*internal.Index, m []*pilosa.IndexInfo) {
	for i := range idxs {
		m[i] = &pilosa.IndexInfo{}
		s.decodeIndex(idxs[i], m[i])
	}
}

func (s Serializer) decodeIndex(idx *internal.Index, m *pilosa.IndexInfo) {
	m.Name = idx.Name
	m.CreatedAt = idx.CreatedAt
	m.Options = pilosa.IndexOptions{}
	s.decodeIndexMeta(idx.Options, &m.Options)
	m.Fields = make([]*pilosa.FieldInfo, len(idx.Fields))
	s.decodeFields(idx.Fields, m.Fields)
}

func (s Serializer) decodeFields(fs []*internal.Field, m []*pilosa.FieldInfo) {
	for i := range fs {
		m[i] = &pilosa.FieldInfo{}
		s.decodeField(fs[i], m[i])
	}
}

func (s Serializer) decodeField(f *internal.Field, m *pilosa.FieldInfo) {
	m.Name = f.Name
	m.CreatedAt = f.CreatedAt
	m.Options = pilosa.FieldOptions{}
	s.decodeFieldOptions(f.Meta, &m.Options)
	m.Views = make([]*pilosa.ViewInfo, 0, len(f.Views))
	for _, viewname := range f.Views {
		m.Views = append(m.Views, &pilosa.ViewInfo{Name: viewname})
	}
}

func (s Serializer) decodeFieldOptions(options *internal.FieldOptions, m *pilosa.FieldOptions) {
	m.Type = options.Type
	m.CacheType = options.CacheType
	m.CacheSize = options.CacheSize
	s.decodeDecimal(options.Min, &m.Min)
	s.decodeDecimal(options.Max, &m.Max)
	m.Base = options.Base
	m.Scale = options.Scale
	m.BitDepth = uint(options.BitDepth)
	m.TimeQuantum = pilosa.TimeQuantum(options.TimeQuantum)
	m.Keys = options.Keys
	m.ForeignIndex = options.ForeignIndex
}

func (s Serializer) decodeDecimal(d *internal.Decimal, m *pql.Decimal) {
	m.Value = d.Value
	m.Scale = d.Scale
}

func (s Serializer) decodeNodes(a []*internal.Node, m []*pilosa.Node) {
	for i := range a {
		m[i] = &pilosa.Node{}
		s.decodeNode(a[i], m[i])
	}
}

func (s Serializer) decodeClusterStatus(cs *internal.ClusterStatus, m *pilosa.ClusterStatus) {
	m.State = cs.State
	m.ClusterID = cs.ClusterID
	m.Nodes = make([]*pilosa.Node, len(cs.Nodes))
	s.decodeNodes(cs.Nodes, m.Nodes)
	m.Schema = &pilosa.Schema{}
	s.decodeSchema(cs.Schema, m.Schema)
}

func (s Serializer) decodeNode(node *internal.Node, m *pilosa.Node) {
	m.ID = node.ID
	s.decodeURI(node.URI, &m.URI)
	s.decodeURI(node.GRPCURI, &m.GRPCURI)
	m.IsCoordinator = node.IsCoordinator
	m.State = node.State
}

func (s Serializer) decodeURI(i *internal.URI, m *pilosa.URI) {
	m.Scheme = i.Scheme
	m.Host = i.Host
	m.Port = uint16(i.Port)
}

func (s Serializer) decodeCreateShardMessage(pb *internal.CreateShardMessage, m *pilosa.CreateShardMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.Shard = pb.Shard
}

func (s Serializer) decodeCreateIndexMessage(pb *internal.CreateIndexMessage, m *pilosa.CreateIndexMessage) {
	m.Index = pb.Index
	m.CreatedAt = pb.CreatedAt
	m.Meta = &pilosa.IndexOptions{}
	s.decodeIndexMeta(pb.Meta, m.Meta)
}

func (s Serializer) decodeIndexMeta(pb *internal.IndexMeta, m *pilosa.IndexOptions) {
	if pb != nil {
		m.Keys = pb.Keys
		m.TrackExistence = pb.TrackExistence
	}
}

func (s Serializer) decodeDeleteIndexMessage(pb *internal.DeleteIndexMessage, m *pilosa.DeleteIndexMessage) {
	m.Index = pb.Index
}

func (s Serializer) decodeCreateFieldMessage(pb *internal.CreateFieldMessage, m *pilosa.CreateFieldMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.CreatedAt = pb.CreatedAt
	m.Meta = &pilosa.FieldOptions{}
	s.decodeFieldOptions(pb.Meta, m.Meta)
}

func (s Serializer) decodeDeleteFieldMessage(pb *internal.DeleteFieldMessage, m *pilosa.DeleteFieldMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
}

func (s Serializer) decodeDeleteAvailableShardMessage(pb *internal.DeleteAvailableShardMessage, m *pilosa.DeleteAvailableShardMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.ShardID = pb.ShardID
}

func (s Serializer) decodeCreateViewMessage(pb *internal.CreateViewMessage, m *pilosa.CreateViewMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.View = pb.View
}

func (s Serializer) decodeDeleteViewMessage(pb *internal.DeleteViewMessage, m *pilosa.DeleteViewMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.View = pb.View
}

func (s Serializer) decodeResizeInstructionComplete(pb *internal.ResizeInstructionComplete, m *pilosa.ResizeInstructionComplete) {
	m.JobID = pb.JobID
	m.Node = &pilosa.Node{}
	s.decodeNode(pb.Node, m.Node)
	m.Error = pb.Error
}

func (s Serializer) decodeSetCoordinatorMessage(pb *internal.SetCoordinatorMessage, m *pilosa.SetCoordinatorMessage) {
	m.New = &pilosa.Node{}
	s.decodeNode(pb.New, m.New)
}

func (s Serializer) decodeUpdateCoordinatorMessage(pb *internal.UpdateCoordinatorMessage, m *pilosa.UpdateCoordinatorMessage) {
	m.New = &pilosa.Node{}
	s.decodeNode(pb.New, m.New)
}

func (s Serializer) decodeNodeStateMessage(pb *internal.NodeStateMessage, m *pilosa.NodeStateMessage) {
	m.NodeID = pb.NodeID
	m.State = pb.State
}

func (s Serializer) decodeNodeEventMessage(pb *internal.NodeEventMessage, m *pilosa.NodeEvent) {
	m.Event = pilosa.NodeEventType(pb.Event)
	m.Node = &pilosa.Node{}
	s.decodeNode(pb.Node, m.Node)
}

func (s Serializer) decodeNodeStatus(pb *internal.NodeStatus, m *pilosa.NodeStatus) {
	m.Node = &pilosa.Node{}
	m.Indexes = s.decodeIndexStatuses(pb.Indexes)
	m.Schema = &pilosa.Schema{}
	s.decodeSchema(pb.Schema, m.Schema)
}

func (s Serializer) decodeIndexStatuses(a []*internal.IndexStatus) []*pilosa.IndexStatus {
	m := make([]*pilosa.IndexStatus, 0)
	for i := range a {
		m = append(m, &pilosa.IndexStatus{})
		s.decodeIndexStatus(a[i], m[i])
	}
	return m
}

func (s Serializer) decodeIndexStatus(pb *internal.IndexStatus, m *pilosa.IndexStatus) {
	m.Name = pb.Name
	m.CreatedAt = pb.CreatedAt
	m.Fields = s.decodeFieldStatuses(pb.Fields)
}

func (s Serializer) decodeFieldStatuses(a []*internal.FieldStatus) []*pilosa.FieldStatus {
	m := make([]*pilosa.FieldStatus, 0)
	for i := range a {
		m = append(m, &pilosa.FieldStatus{})
		s.decodeFieldStatus(a[i], m[i])
	}
	return m
}

func (s Serializer) decodeFieldStatus(pb *internal.FieldStatus, m *pilosa.FieldStatus) {
	m.Name = pb.Name
	m.CreatedAt = pb.CreatedAt
	m.AvailableShards = roaring.NewBitmap(pb.AvailableShards...)
}

func (s Serializer) decodeRecalculateCaches(pb *internal.RecalculateCaches, m *pilosa.RecalculateCaches) {
}

func (s Serializer) decodeQueryRequest(pb *internal.QueryRequest, m *pilosa.QueryRequest) {
	m.Query = pb.Query
	m.Shards = pb.Shards
	m.ColumnAttrs = pb.ColumnAttrs
	m.Remote = pb.Remote
	m.ExcludeRowAttrs = pb.ExcludeRowAttrs
	m.ExcludeColumns = pb.ExcludeColumns
	m.EmbeddedData = make([]*pilosa.Row, len(pb.EmbeddedData))
	for i := range pb.EmbeddedData {
		m.EmbeddedData[i] = s.decodeRow(pb.EmbeddedData[i])
	}
}

func (s Serializer) decodeImportRequest(pb *internal.ImportRequest, m *pilosa.ImportRequest) {
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

func (s Serializer) decodeImportValueRequest(pb *internal.ImportValueRequest, m *pilosa.ImportValueRequest) {
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

func (s Serializer) decodeImportRoaringRequest(pb *internal.ImportRoaringRequest, m *pilosa.ImportRoaringRequest) {
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
}

func (s Serializer) decodeImportColumnAttrsRequest(pb *internal.ImportColumnAttrsRequest, m *pilosa.ImportColumnAttrsRequest) {
	m.Index = pb.Index
	m.IndexCreatedAt = pb.IndexCreatedAt
	m.Shard = pb.Shard
	m.AttrKey = pb.AttrKey
	m.AttrVals = pb.AttrVals
	m.ColumnIDs = pb.ColumnIDs
}

func (s Serializer) decodeImportResponse(pb *internal.ImportResponse, m *pilosa.ImportResponse) {
	m.Err = pb.Err
}

func (s Serializer) decodeBlockDataRequest(pb *internal.BlockDataRequest, m *pilosa.BlockDataRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.View = pb.View
	m.Shard = pb.Shard
	m.Block = pb.Block
}

func (s Serializer) decodeBlockDataResponse(pb *internal.BlockDataResponse, m *pilosa.BlockDataResponse) {
	m.RowIDs = pb.RowIDs
	m.ColumnIDs = pb.ColumnIDs
}

func (s Serializer) decodeQueryResponse(pb *internal.QueryResponse, m *pilosa.QueryResponse) {
	m.ColumnAttrSets = make([]*pilosa.ColumnAttrSet, len(pb.ColumnAttrSets))
	s.decodeColumnAttrSets(pb.ColumnAttrSets, m.ColumnAttrSets)
	if pb.Err == "" {
		m.Err = nil
	} else {
		m.Err = errors.New(pb.Err)
	}
	m.Results = make([]interface{}, len(pb.Results))
	s.decodeQueryResults(pb.Results, m.Results)
}

func (s Serializer) decodeColumnAttrSets(pb []*internal.ColumnAttrSet, m []*pilosa.ColumnAttrSet) {
	for i := range pb {
		m[i] = &pilosa.ColumnAttrSet{}
		s.decodeColumnAttrSet(pb[i], m[i])
	}
}

func (s Serializer) decodeColumnAttrSet(pb *internal.ColumnAttrSet, m *pilosa.ColumnAttrSet) {
	m.ID = pb.ID
	m.Key = pb.Key
	m.Attrs = s.decodeAttrs(pb.Attrs)
}

func (s Serializer) decodeQueryResults(pb []*internal.QueryResult, m []interface{}) {
	for i := range pb {
		m[i] = s.decodeQueryResult(pb[i])
	}
}

func (s Serializer) decodeTranslateKeysRequest(pb *internal.TranslateKeysRequest, m *pilosa.TranslateKeysRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.Keys = pb.Keys
	m.NotWritable = pb.NotWritable
}

func (s Serializer) decodeTranslateKeysResponse(pb *internal.TranslateKeysResponse, m *pilosa.TranslateKeysResponse) {
	m.IDs = pb.IDs
}

func (s Serializer) decodeTranslateIDsRequest(pb *internal.TranslateIDsRequest, m *pilosa.TranslateIDsRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.IDs = pb.IDs
}

func (s Serializer) decodeTranslateIDsResponse(pb *internal.TranslateIDsResponse, m *pilosa.TranslateIDsResponse) {
	m.Keys = pb.Keys
}

func decodeTransactionMessage(pb *internal.TransactionMessage, m *pilosa.TransactionMessage) {
	m.Action = pb.Action
	if pb.Transaction == nil {
		m.Transaction = nil
		return
	} else if m.Transaction == nil {
		m.Transaction = &pilosa.Transaction{}
	}
	decodeTransaction(pb.Transaction, m.Transaction)
}

func (s Serializer) decodeAtomicRecord(pb *internal.AtomicRecord, m *pilosa.AtomicRecord) {
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

func (s Serializer) decodeRowMatrix(pb *internal.RowMatrix) []*pilosa.Row {
	rows := make([]*pilosa.Row, len(pb.Rows))
	for i, r := range pb.Rows {
		rows[i] = s.decodeRow(r)
	}
	return rows
}

func decodeTransaction(pb *internal.Transaction, trns *pilosa.Transaction) {
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
)

func (s Serializer) decodeQueryResult(pb *internal.QueryResult) interface{} {
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
		return s.decodeGroupCounts(pb.GroupCounts)
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
	}
	panic(fmt.Sprintf("unknown type: %d", pb.Type))
}

// s.decodeRow converts r from its internal representation.
func (s Serializer) decodeRow(pr *internal.Row) *pilosa.Row {
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
	r.Attrs = s.decodeAttrs(pr.Attrs)
	r.Keys = pr.Keys
	r.Index = pr.Index
	r.Field = pr.Field

	return r
}

func (s Serializer) decodeSignedRow(pr *internal.SignedRow) pilosa.SignedRow {
	if pr == nil {
		return pilosa.SignedRow{}
	}
	r := pilosa.SignedRow{
		Pos: s.decodeRow(pr.Pos),
		Neg: s.decodeRow(pr.Neg),
	}
	return r
}

func (s Serializer) decodeAttrs(pb []*internal.Attr) map[string]interface{} {
	m := make(map[string]interface{}, len(pb))
	for i := range pb {
		key, value := s.decodeAttr(pb[i])
		m[key] = value
	}
	return m
}

const (
	attrTypeString = 1
	attrTypeInt    = 2
	attrTypeBool   = 3
	attrTypeFloat  = 4
)

func (s Serializer) decodeAttr(attr *internal.Attr) (key string, value interface{}) {
	switch attr.Type {
	case attrTypeString:
		return attr.Key, attr.StringValue
	case attrTypeInt:
		return attr.Key, attr.IntValue
	case attrTypeBool:
		return attr.Key, attr.BoolValue
	case attrTypeFloat:
		return attr.Key, attr.FloatValue
	default:
		return attr.Key, nil
	}
}

func (s Serializer) decodeExtractedIDMatrix(m *internal.ExtractedIDMatrix) pilosa.ExtractedIDMatrix {
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

func (s Serializer) decodeExtractedTable(t *internal.ExtractedTable) pilosa.ExtractedTable {
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
		case *internal.ExtractedTableColumn_ID:
			col = pilosa.KeyOrID{
				ID: kid.ID,
			}
		case *internal.ExtractedTableColumn_Key:
			col = pilosa.KeyOrID{
				Keyed: true,
				Key:   kid.Key,
			}
		}

		rows := make([]interface{}, len(c.Values))
		for j, v := range rows {
			var val interface{}
			switch v := v.(type) {
			case *internal.ExtractedTableValue_IDs:
				val = v.IDs.IDs
			case *internal.ExtractedTableValue_Keys:
				val = v.Keys.Keys
			case *internal.ExtractedTableValue_BSIValue:
				val = v.BSIValue
			case *internal.ExtractedTableValue_MutexID:
				val = v.MutexID
			case *internal.ExtractedTableValue_MutexKey:
				val = v.MutexKey
			case *internal.ExtractedTableValue_Bool:
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

func (s Serializer) decodeRowIdentifiers(a *internal.RowIdentifiers) *pilosa.RowIdentifiers {
	return &pilosa.RowIdentifiers{
		Rows: a.Rows,
		Keys: a.Keys,
	}
}

func (s Serializer) decodeGroupCounts(a []*internal.GroupCount) []pilosa.GroupCount {
	other := make([]pilosa.GroupCount, len(a))
	for i := range a {
		other[i] = pilosa.GroupCount{
			Group: s.decodeFieldRows(a[i].Group),
			Count: a[i].Count,
			Sum:   a[i].Sum,
		}
	}
	return other
}

func (s Serializer) decodeFieldRows(a []*internal.FieldRow) []pilosa.FieldRow {
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

func (s Serializer) decodePairs(a []*internal.Pair) []pilosa.Pair {
	other := make([]pilosa.Pair, len(a))
	for i := range a {
		other[i] = s.decodePair(a[i])
	}
	return other
}

func (s Serializer) decodePairsField(a *internal.PairsField) *pilosa.PairsField {
	other := &pilosa.PairsField{
		Pairs: make([]pilosa.Pair, len(a.Pairs)),
	}
	for i := range a.Pairs {
		other.Pairs[i] = s.decodePair(a.Pairs[i])
	}
	other.Field = a.Field
	return other
}

func (s Serializer) decodePair(pb *internal.Pair) pilosa.Pair {
	return pilosa.Pair{
		ID:    pb.ID,
		Key:   pb.Key,
		Count: pb.Count,
	}
}

func (s Serializer) decodePairField(pb *internal.PairField) pilosa.PairField {
	return pilosa.PairField{
		Pair: pilosa.Pair{
			ID:    pb.Pair.ID,
			Key:   pb.Pair.Key,
			Count: pb.Pair.Count,
		},
		Field: pb.Field,
	}
}

func (s Serializer) decodeValCount(pb *internal.ValCount) pilosa.ValCount {
	return pilosa.ValCount{
		Val:        pb.Val,
		FloatVal:   pb.FloatVal,
		DecimalVal: s.decodeDecimalStruct(pb.DecimalVal),
		Count:      pb.Count,
	}
}

func (s Serializer) decodeDecimalStruct(pb *internal.Decimal) *pql.Decimal {
	if pb == nil {
		return nil
	}
	return &pql.Decimal{
		Value: pb.Value,
		Scale: pb.Scale,
	}
}

func (s Serializer) encodeColumnAttrSets(a []*pilosa.ColumnAttrSet) []*internal.ColumnAttrSet {
	other := make([]*internal.ColumnAttrSet, len(a))
	for i := range a {
		other[i] = s.encodeColumnAttrSet(a[i])
	}
	return other
}

func (s Serializer) encodeColumnAttrSet(set *pilosa.ColumnAttrSet) *internal.ColumnAttrSet {
	return &internal.ColumnAttrSet{
		ID:    set.ID,
		Key:   set.Key,
		Attrs: s.encodeAttrs(set.Attrs),
	}
}

func (s Serializer) encodeSignedRow(r pilosa.SignedRow) *internal.SignedRow {
	ir := &internal.SignedRow{
		Pos: s.encodeRow(r.Pos),
		Neg: s.encodeRow(r.Neg),
	}
	return ir
}

func (s Serializer) encodeRow(r *pilosa.Row) *internal.Row {
	if r == nil {
		return nil
	}

	ir := &internal.Row{
		Keys:  r.Keys,
		Attrs: s.encodeAttrs(r.Attrs),
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

func (s Serializer) encodeRowIdentifiers(r pilosa.RowIdentifiers) *internal.RowIdentifiers {
	return &internal.RowIdentifiers{
		Rows: r.Rows,
		Keys: r.Keys,
		//Attrs:   s.encodeAttrs(r.Attrs),
	}
}

func (s Serializer) encodeGroupCounts(counts []pilosa.GroupCount) []*internal.GroupCount {
	result := make([]*internal.GroupCount, len(counts))
	for i := range counts {
		result[i] = &internal.GroupCount{
			Group: s.encodeFieldRows(counts[i].Group),
			Count: counts[i].Count,
			Sum:   counts[i].Sum,
		}
	}
	return result
}

func (s Serializer) encodeFieldRows(a []pilosa.FieldRow) []*internal.FieldRow {
	other := make([]*internal.FieldRow, len(a))
	for i := range a {
		fr := a[i]
		other[i] = &internal.FieldRow{Field: fr.Field}

		if fr.Value != nil {
			other[i].Value = &internal.Int64{Value: *fr.Value}
		} else if fr.RowKey == "" {
			other[i].RowID = fr.RowID
		} else {
			other[i].RowKey = fr.RowKey
		}
	}
	return other
}

func (s Serializer) endcodeExtractedIDMatrix(m pilosa.ExtractedIDMatrix) *internal.ExtractedIDMatrix {
	cols := make([]*internal.ExtractedIDColumn, len(m.Columns))
	for i, v := range m.Columns {
		vals := make([]*internal.IDList, len(v.Rows))
		for j, f := range v.Rows {
			vals[j] = &internal.IDList{IDs: f}
		}
		cols[i] = &internal.ExtractedIDColumn{
			ID:   v.ColumnID,
			Vals: vals,
		}
	}
	return &internal.ExtractedIDMatrix{
		Fields:  m.Fields,
		Columns: cols,
	}
}

func (s Serializer) encodeExtractedTable(t pilosa.ExtractedTable) *internal.ExtractedTable {
	fields := make([]*internal.ExtractedTableField, len(t.Fields))
	for i, f := range t.Fields {
		fields[i] = &internal.ExtractedTableField{
			Name: f.Name,
			Type: f.Type,
		}
	}

	cols := make([]*internal.ExtractedTableColumn, len(t.Columns))
	for i, c := range t.Columns {
		var col internal.ExtractedTableColumn
		if c.Column.Keyed {
			col.KeyOrID = &internal.ExtractedTableColumn_Key{Key: c.Column.Key}
		} else {
			col.KeyOrID = &internal.ExtractedTableColumn_ID{ID: c.Column.ID}
		}

		rows := make([]*internal.ExtractedTableValue, len(c.Rows))
		for j, v := range c.Rows {
			switch v := v.(type) {
			case []uint64:
				rows[j] = &internal.ExtractedTableValue{
					Value: &internal.ExtractedTableValue_IDs{
						IDs: &internal.IDList{
							IDs: v,
						},
					},
				}
			case []string:
				rows[j] = &internal.ExtractedTableValue{
					Value: &internal.ExtractedTableValue_Keys{
						Keys: &internal.KeyList{
							Keys: v,
						},
					},
				}
			case int64:
				rows[j] = &internal.ExtractedTableValue{
					Value: &internal.ExtractedTableValue_BSIValue{
						BSIValue: v,
					},
				}
			case uint64:
				rows[j] = &internal.ExtractedTableValue{
					Value: &internal.ExtractedTableValue_MutexID{
						MutexID: v,
					},
				}
			case string:
				rows[j] = &internal.ExtractedTableValue{
					Value: &internal.ExtractedTableValue_MutexKey{
						MutexKey: v,
					},
				}
			case bool:
				rows[j] = &internal.ExtractedTableValue{
					Value: &internal.ExtractedTableValue_Bool{
						Bool: v,
					},
				}
			}
		}
		col.Values = rows

		cols[i] = &col
	}

	return &internal.ExtractedTable{
		Fields:  fields,
		Columns: cols,
	}
}

func (s Serializer) encodePairs(a pilosa.Pairs) []*internal.Pair {
	other := make([]*internal.Pair, len(a))
	for i := range a {
		other[i] = s.encodePair(a[i])
	}
	return other
}

func (s Serializer) encodePairsField(a *pilosa.PairsField) *internal.PairsField {
	other := &internal.PairsField{
		Pairs: make([]*internal.Pair, len(a.Pairs)),
	}
	for i := range a.Pairs {
		other.Pairs[i] = s.encodePair(a.Pairs[i])
	}
	other.Field = a.Field
	return other
}

func (s Serializer) encodePair(p pilosa.Pair) *internal.Pair {
	return &internal.Pair{
		ID:    p.ID,
		Key:   p.Key,
		Count: p.Count,
	}
}

func (s Serializer) encodePairField(p pilosa.PairField) *internal.PairField {
	return &internal.PairField{
		Pair:  s.encodePair(p.Pair),
		Field: p.Field,
	}
}

func (s Serializer) encodeValCount(vc pilosa.ValCount) *internal.ValCount {
	return &internal.ValCount{
		Val:        vc.Val,
		FloatVal:   vc.FloatVal,
		DecimalVal: s.encodeDecimal(vc.DecimalVal),
		Count:      vc.Count,
	}
}

func (s Serializer) encodeDecimal(p *pql.Decimal) *internal.Decimal {
	if p == nil {
		return nil
	}
	return &internal.Decimal{
		Value: p.Value,
		Scale: p.Scale,
	}
}

func (s Serializer) encodeAttrs(m map[string]interface{}) []*internal.Attr {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	a := make([]*internal.Attr, len(keys))
	for i := range keys {
		a[i] = s.encodeAttr(keys[i], m[keys[i]])
	}
	return a
}

// s.encodeAttr converts a key/value pair into an Attr internal representation.
func (s Serializer) encodeAttr(key string, value interface{}) *internal.Attr {
	pb := &internal.Attr{Key: key}
	switch value := value.(type) {
	case string:
		pb.Type = attrTypeString
		pb.StringValue = value
	case float64:
		pb.Type = attrTypeFloat
		pb.FloatValue = value
	case uint64:
		pb.Type = attrTypeInt
		pb.IntValue = int64(value)
	case int64:
		pb.Type = attrTypeInt
		pb.IntValue = value
	case bool:
		pb.Type = attrTypeBool
		pb.BoolValue = value
	}
	return pb
}
