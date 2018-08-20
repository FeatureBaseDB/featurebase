package proto

import (
	"fmt"
	"sort"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pkg/errors"
)

// Serializer implements pilosa.Serializer for protobufs.
type Serializer struct{}

// Marshal turns pilosa messages into protobuf serialized bytes.
func (Serializer) Marshal(m pilosa.Message) ([]byte, error) {
	pm := encodeToProto(m)
	if pm == nil {
		return nil, errors.New("passed invalid pilosa.Message")
	}
	buf, err := proto.Marshal(pm)
	return buf, errors.Wrap(err, "marshalling")
}

// Unmarshal takes byte slices and protobuf deserializes them into a pilosa Message.
func (Serializer) Unmarshal(buf []byte, m pilosa.Message) error {
	switch mt := m.(type) {
	case *pilosa.CreateShardMessage:
		msg := &internal.CreateShardMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateShardMessage")
		}
		decodeCreateShardMessage(msg, mt)
		return nil
	case *pilosa.CreateIndexMessage:
		msg := &internal.CreateIndexMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateIndexMessage")
		}
		decodeCreateIndexMessage(msg, mt)
		return nil
	case *pilosa.DeleteIndexMessage:
		msg := &internal.DeleteIndexMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteIndexMessage")
		}
		decodeDeleteIndexMessage(msg, mt)
		return nil
	case *pilosa.CreateFieldMessage:
		msg := &internal.CreateFieldMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateFieldMessage")
		}
		decodeCreateFieldMessage(msg, mt)
		return nil
	case *pilosa.DeleteFieldMessage:
		msg := &internal.DeleteFieldMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteFieldMessage")
		}
		decodeDeleteFieldMessage(msg, mt)
		return nil
	case *pilosa.CreateViewMessage:
		msg := &internal.CreateViewMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling CreateViewMessage")
		}
		decodeCreateViewMessage(msg, mt)
		return nil
	case *pilosa.DeleteViewMessage:
		msg := &internal.DeleteViewMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling DeleteViewMessage")
		}
		decodeDeleteViewMessage(msg, mt)
		return nil
	case *pilosa.ClusterStatus:
		msg := &internal.ClusterStatus{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ClusterStatus")
		}
		decodeClusterStatus(msg, mt)
		return nil
	case *pilosa.ResizeInstruction:
		msg := &internal.ResizeInstruction{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ResizeInstruction")
		}
		decodeResizeInstruction(msg, mt)
		return nil
	case *pilosa.ResizeInstructionComplete:
		msg := &internal.ResizeInstructionComplete{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ResizeInstructionComplete")
		}
		decodeResizeInstructionComplete(msg, mt)
		return nil
	case *pilosa.SetCoordinatorMessage:
		msg := &internal.SetCoordinatorMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling SetCoordinatorMessage")
		}
		decodeSetCoordinatorMessage(msg, mt)
		return nil
	case *pilosa.UpdateCoordinatorMessage:
		msg := &internal.UpdateCoordinatorMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling UpdateCoordinatorMessage")
		}
		decodeUpdateCoordinatorMessage(msg, mt)
		return nil
	case *pilosa.NodeStateMessage:
		msg := &internal.NodeStateMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling NodeStateMessage")
		}
		decodeNodeStateMessage(msg, mt)
		return nil
	case *pilosa.RecalculateCaches:
		msg := &internal.RecalculateCaches{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling RecalculateCaches")
		}
		decodeRecalculateCaches(msg, mt)
		return nil
	case *pilosa.NodeEvent:
		msg := &internal.NodeEventMessage{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling NodeEvent")
		}
		decodeNodeEventMessage(msg, mt)
		return nil
	case *pilosa.NodeStatus:
		msg := &internal.NodeStatus{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling NodeStatus")
		}
		decodeNodeStatus(msg, mt)
		return nil
	case *pilosa.Node:
		msg := &internal.Node{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling Node")
		}
		decodeNode(msg, mt)
		return nil
	case *pilosa.QueryRequest:
		msg := &internal.QueryRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling QueryRequest")
		}
		decodeQueryRequest(msg, mt)
		return nil
	case *pilosa.QueryResponse:
		msg := &internal.QueryResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling QueryResponse")
		}
		decodeQueryResponse(msg, mt)
		return nil
	case *pilosa.ImportRequest:
		msg := &internal.ImportRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportRequest")
		}
		decodeImportRequest(msg, mt)
		return nil
	case *pilosa.ImportValueRequest:
		msg := &internal.ImportValueRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportValueRequest")
		}
		decodeImportValueRequest(msg, mt)
		return nil
	case *pilosa.ImportResponse:
		msg := &internal.ImportResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling ImportResponse")
		}
		decodeImportResponse(msg, mt)
		return nil
	case *pilosa.BlockDataRequest:
		msg := &internal.BlockDataRequest{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling BlockDataRequest")
		}
		decodeBlockDataRequest(msg, mt)
		return nil
	case *pilosa.BlockDataResponse:
		msg := &internal.BlockDataResponse{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return errors.Wrap(err, "unmarshaling BlockDataResponse")
		}
		decodeBlockDataResponse(msg, mt)
		return nil
	default:
		panic(fmt.Sprintf("unhandled pilosa.Message of type %T: %#v", mt, m))
	}
}

func encodeToProto(m pilosa.Message) proto.Message {
	switch mt := m.(type) {
	case *pilosa.CreateShardMessage:
		return encodeCreateShardMessage(mt)
	case *pilosa.CreateIndexMessage:
		return encodeCreateIndexMessage(mt)
	case *pilosa.DeleteIndexMessage:
		return encodeDeleteIndexMessage(mt)
	case *pilosa.CreateFieldMessage:
		return encodeCreateFieldMessage(mt)
	case *pilosa.DeleteFieldMessage:
		return encodeDeleteFieldMessage(mt)
	case *pilosa.CreateViewMessage:
		return encodeCreateViewMessage(mt)
	case *pilosa.DeleteViewMessage:
		return encodeDeleteViewMessage(mt)
	case *pilosa.ClusterStatus:
		return encodeClusterStatus(mt)
	case *pilosa.ResizeInstruction:
		return encodeResizeInstruction(mt)
	case *pilosa.ResizeInstructionComplete:
		return encodeResizeInstructionComplete(mt)
	case *pilosa.SetCoordinatorMessage:
		return encodeSetCoordinatorMessage(mt)
	case *pilosa.UpdateCoordinatorMessage:
		return encodeUpdateCoordinatorMessage(mt)
	case *pilosa.NodeStateMessage:
		return encodeNodeStateMessage(mt)
	case *pilosa.RecalculateCaches:
		return encodeRecalculateCaches(mt)
	case *pilosa.NodeEvent:
		return encodeNodeEventMessage(mt)
	case *pilosa.NodeStatus:
		return encodeNodeStatus(mt)
	case *pilosa.Node:
		return encodeNode(mt)
	case *pilosa.QueryRequest:
		return encodeQueryRequest(mt)
	case *pilosa.QueryResponse:
		return encodeQueryResponse(mt)
	case *pilosa.ImportRequest:
		return encodeImportRequest(mt)
	case *pilosa.ImportValueRequest:
		return encodeImportValueRequest(mt)
	case *pilosa.ImportResponse:
		return encodeImportResponse(mt)
	case *pilosa.BlockDataRequest:
		return encodeBlockDataRequest(mt)
	case *pilosa.BlockDataResponse:
		return encodeBlockDataResponse(mt)
	}
	return nil
}

func encodeBlockDataRequest(m *pilosa.BlockDataRequest) *internal.BlockDataRequest {
	return &internal.BlockDataRequest{
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
		Shard: m.Shard,
		Block: m.Block,
	}
}
func encodeBlockDataResponse(m *pilosa.BlockDataResponse) *internal.BlockDataResponse {
	return &internal.BlockDataResponse{
		RowIDs:    m.RowIDs,
		ColumnIDs: m.ColumnIDs,
	}
}

func encodeImportResponse(m *pilosa.ImportResponse) *internal.ImportResponse {
	return &internal.ImportResponse{
		Err: m.Err,
	}
}

func encodeImportRequest(m *pilosa.ImportRequest) *internal.ImportRequest {
	return &internal.ImportRequest{
		Index:      m.Index,
		Field:      m.Field,
		Shard:      m.Shard,
		RowIDs:     m.RowIDs,
		ColumnIDs:  m.ColumnIDs,
		RowKeys:    m.RowKeys,
		ColumnKeys: m.ColumnKeys,
		Timestamps: m.Timestamps,
	}
}

func encodeImportValueRequest(m *pilosa.ImportValueRequest) *internal.ImportValueRequest {
	return &internal.ImportValueRequest{
		Index:      m.Index,
		Field:      m.Field,
		Shard:      m.Shard,
		ColumnIDs:  m.ColumnIDs,
		ColumnKeys: m.ColumnKeys,
		Values:     m.Values,
	}
}

func encodeQueryRequest(m *pilosa.QueryRequest) *internal.QueryRequest {
	return &internal.QueryRequest{
		Query:           m.Query,
		Shards:          m.Shards,
		ColumnAttrs:     m.ColumnAttrs,
		Remote:          m.Remote,
		ExcludeRowAttrs: m.ExcludeRowAttrs,
		ExcludeColumns:  m.ExcludeColumns,
	}
}

func encodeQueryResponse(m *pilosa.QueryResponse) *internal.QueryResponse {
	pb := &internal.QueryResponse{
		Results:        make([]*internal.QueryResult, len(m.Results)),
		ColumnAttrSets: encodeColumnAttrSets(m.ColumnAttrSets),
	}

	for i := range m.Results {
		pb.Results[i] = &internal.QueryResult{}

		switch result := m.Results[i].(type) {
		case *pilosa.Row:
			pb.Results[i].Type = queryResultTypeRow
			pb.Results[i].Row = encodeRow(result)
		case []pilosa.Pair:
			pb.Results[i].Type = queryResultTypePairs
			pb.Results[i].Pairs = encodePairs(result)
		case pilosa.ValCount:
			pb.Results[i].Type = queryResultTypeValCount
			pb.Results[i].ValCount = encodeValCount(result)
		case uint64:
			pb.Results[i].Type = queryResultTypeUint64
			pb.Results[i].N = result
		case bool:
			pb.Results[i].Type = queryResultTypeBool
			pb.Results[i].Changed = result
		case nil:
			pb.Results[i].Type = queryResultTypeNil
		}
	}

	if m.Err != nil {
		pb.Err = m.Err.Error()
	}

	return pb
}

func encodeResizeInstruction(m *pilosa.ResizeInstruction) *internal.ResizeInstruction {
	return &internal.ResizeInstruction{
		JobID:         m.JobID,
		Node:          encodeNode(m.Node),
		Coordinator:   encodeNode(m.Coordinator),
		Sources:       encodeResizeSources(m.Sources),
		Schema:        encodeSchema(m.Schema),
		ClusterStatus: encodeClusterStatus(m.ClusterStatus),
	}
}

func encodeResizeSources(srcs []*pilosa.ResizeSource) []*internal.ResizeSource {
	new := make([]*internal.ResizeSource, 0, len(srcs))
	for _, src := range srcs {
		new = append(new, encodeResizeSource(src))
	}
	return new
}

func encodeResizeSource(m *pilosa.ResizeSource) *internal.ResizeSource {
	return &internal.ResizeSource{
		Node:  encodeNode(m.Node),
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
		Shard: m.Shard,
	}
}

func encodeSchema(m *pilosa.Schema) *internal.Schema {
	return &internal.Schema{
		Indexes: encodeIndexInfos(m.Indexes),
	}
}

func encodeIndexInfos(idxs []*pilosa.IndexInfo) []*internal.Index {
	new := make([]*internal.Index, 0, len(idxs))
	for _, idx := range idxs {
		new = append(new, encodeIndexInfo(idx))
	}
	return new
}

func encodeIndexInfo(idx *pilosa.IndexInfo) *internal.Index {
	return &internal.Index{
		Name:   idx.Name,
		Fields: encodeFieldInfos(idx.Fields),
	}
}

func encodeFieldInfos(fs []*pilosa.FieldInfo) []*internal.Field {
	new := make([]*internal.Field, 0, len(fs))
	for _, f := range fs {
		new = append(new, encodeFieldInfo(f))
	}
	return new
}

func encodeFieldInfo(f *pilosa.FieldInfo) *internal.Field {
	ifield := &internal.Field{
		Name:  f.Name,
		Meta:  encodeFieldOptions(&f.Options),
		Views: make([]string, 0, len(f.Views)),
	}

	for _, viewinfo := range f.Views {
		ifield.Views = append(ifield.Views, viewinfo.Name)
	}
	return ifield
}

func encodeFieldOptions(o *pilosa.FieldOptions) *internal.FieldOptions {
	if o == nil {
		return nil
	}
	return &internal.FieldOptions{
		Type:        o.Type,
		CacheType:   o.CacheType,
		CacheSize:   o.CacheSize,
		Min:         o.Min,
		Max:         o.Max,
		TimeQuantum: string(o.TimeQuantum),
		Keys:        o.Keys,
	}
}

// encodeNodes converts a slice of Nodes into its internal representation.
func encodeNodes(a []*pilosa.Node) []*internal.Node {
	other := make([]*internal.Node, len(a))
	for i := range a {
		other[i] = encodeNode(a[i])
	}
	return other
}

// encodeNode converts a Node into its internal representation.
func encodeNode(n *pilosa.Node) *internal.Node {
	return &internal.Node{
		ID:            n.ID,
		URI:           encodeURI(n.URI),
		IsCoordinator: n.IsCoordinator,
	}
}

func encodeURI(u pilosa.URI) *internal.URI {
	return &internal.URI{
		Scheme: u.Scheme,
		Host:   u.Host,
		Port:   uint32(u.Port),
	}
}

func encodeClusterStatus(m *pilosa.ClusterStatus) *internal.ClusterStatus {
	return &internal.ClusterStatus{
		State:     m.State,
		ClusterID: m.ClusterID,
		Nodes:     encodeNodes(m.Nodes),
	}
}

func encodeCreateShardMessage(m *pilosa.CreateShardMessage) *internal.CreateShardMessage {
	return &internal.CreateShardMessage{
		Index: m.Index,
		Shard: m.Shard,
	}
}

func encodeCreateIndexMessage(m *pilosa.CreateIndexMessage) *internal.CreateIndexMessage {
	return &internal.CreateIndexMessage{
		Index: m.Index,
		Meta:  encodeIndexMeta(m.Meta),
	}
}

func encodeIndexMeta(m *pilosa.IndexOptions) *internal.IndexMeta {
	return &internal.IndexMeta{
		Keys: m.Keys,
	}
}

func encodeDeleteIndexMessage(m *pilosa.DeleteIndexMessage) *internal.DeleteIndexMessage {
	return &internal.DeleteIndexMessage{
		Index: m.Index,
	}
}

func encodeCreateFieldMessage(m *pilosa.CreateFieldMessage) *internal.CreateFieldMessage {
	return &internal.CreateFieldMessage{
		Index: m.Index,
		Field: m.Field,
		Meta:  encodeFieldOptions(m.Meta),
	}
}

func encodeDeleteFieldMessage(m *pilosa.DeleteFieldMessage) *internal.DeleteFieldMessage {
	return &internal.DeleteFieldMessage{
		Index: m.Index,
		Field: m.Field,
	}
}

func encodeCreateViewMessage(m *pilosa.CreateViewMessage) *internal.CreateViewMessage {
	return &internal.CreateViewMessage{
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
	}
}

func encodeDeleteViewMessage(m *pilosa.DeleteViewMessage) *internal.DeleteViewMessage {
	return &internal.DeleteViewMessage{
		Index: m.Index,
		Field: m.Field,
		View:  m.View,
	}
}

func encodeResizeInstructionComplete(m *pilosa.ResizeInstructionComplete) *internal.ResizeInstructionComplete {
	return &internal.ResizeInstructionComplete{
		JobID: m.JobID,
		Node:  encodeNode(m.Node),
		Error: m.Error,
	}
}

func encodeSetCoordinatorMessage(m *pilosa.SetCoordinatorMessage) *internal.SetCoordinatorMessage {
	return &internal.SetCoordinatorMessage{
		New: encodeNode(m.New),
	}
}

func encodeUpdateCoordinatorMessage(m *pilosa.UpdateCoordinatorMessage) *internal.UpdateCoordinatorMessage {
	return &internal.UpdateCoordinatorMessage{
		New: encodeNode(m.New),
	}
}

func encodeNodeStateMessage(m *pilosa.NodeStateMessage) *internal.NodeStateMessage {
	return &internal.NodeStateMessage{
		NodeID: m.NodeID,
		State:  m.State,
	}
}

func encodeNodeEventMessage(m *pilosa.NodeEvent) *internal.NodeEventMessage {
	return &internal.NodeEventMessage{
		Event: uint32(m.Event),
		Node:  encodeNode(m.Node),
	}
}

func encodeNodeStatus(m *pilosa.NodeStatus) *internal.NodeStatus {
	return &internal.NodeStatus{
		Node:      encodeNode(m.Node),
		MaxShards: &internal.MaxShards{Standard: m.MaxShards},
		Schema:    encodeSchema(m.Schema),
	}
}

func encodeRecalculateCaches(*pilosa.RecalculateCaches) *internal.RecalculateCaches {
	return &internal.RecalculateCaches{}
}

func decodeResizeInstruction(ri *internal.ResizeInstruction, m *pilosa.ResizeInstruction) {
	m.JobID = ri.JobID
	m.Node = &pilosa.Node{}
	decodeNode(ri.Node, m.Node)
	m.Coordinator = &pilosa.Node{}
	decodeNode(ri.Coordinator, m.Coordinator)
	m.Sources = make([]*pilosa.ResizeSource, len(ri.Sources))
	decodeResizeSources(ri.Sources, m.Sources)
	m.Schema = &pilosa.Schema{}
	decodeSchema(ri.Schema, m.Schema)
	m.ClusterStatus = &pilosa.ClusterStatus{}
	decodeClusterStatus(ri.ClusterStatus, m.ClusterStatus)
}

func decodeResizeSources(srcs []*internal.ResizeSource, m []*pilosa.ResizeSource) {
	for i := range srcs {
		m[i] = &pilosa.ResizeSource{}
		decodeResizeSource(srcs[i], m[i])
	}
}

func decodeResizeSource(rs *internal.ResizeSource, m *pilosa.ResizeSource) {
	m.Node = &pilosa.Node{}
	decodeNode(rs.Node, m.Node)
	m.Index = rs.Index
	m.Field = rs.Field
	m.View = rs.View
	m.Shard = rs.Shard
}

func decodeSchema(s *internal.Schema, m *pilosa.Schema) {
	m.Indexes = make([]*pilosa.IndexInfo, len(s.Indexes))
	decodeIndexes(s.Indexes, m.Indexes)
}

func decodeIndexes(idxs []*internal.Index, m []*pilosa.IndexInfo) {
	for i := range idxs {
		m[i] = &pilosa.IndexInfo{}
		decodeIndex(idxs[i], m[i])
	}
}

func decodeIndex(idx *internal.Index, m *pilosa.IndexInfo) {
	m.Name = idx.Name
	m.Fields = make([]*pilosa.FieldInfo, len(idx.Fields))
	decodeFields(idx.Fields, m.Fields)
}

func decodeFields(fs []*internal.Field, m []*pilosa.FieldInfo) {
	for i := range fs {
		m[i] = &pilosa.FieldInfo{}
		decodeField(fs[i], m[i])
	}
}

func decodeField(f *internal.Field, m *pilosa.FieldInfo) {
	m.Name = f.Name
	m.Options = pilosa.FieldOptions{}
	decodeFieldOptions(f.Meta, &m.Options)
	m.Views = make([]*pilosa.ViewInfo, 0, len(f.Views))
	for _, viewname := range f.Views {
		m.Views = append(m.Views, &pilosa.ViewInfo{Name: viewname})
	}
}

func decodeFieldOptions(options *internal.FieldOptions, m *pilosa.FieldOptions) {
	m.Type = options.Type
	m.CacheType = options.CacheType
	m.CacheSize = options.CacheSize
	m.Min = options.Min
	m.Max = options.Max
	m.TimeQuantum = pilosa.TimeQuantum(options.TimeQuantum)
	m.Keys = options.Keys
}

func decodeNodes(a []*internal.Node, m []*pilosa.Node) {
	for i := range a {
		m[i] = &pilosa.Node{}
		decodeNode(a[i], m[i])
	}
}

func decodeClusterStatus(cs *internal.ClusterStatus, m *pilosa.ClusterStatus) {
	m.State = cs.State
	m.ClusterID = cs.ClusterID
	m.Nodes = make([]*pilosa.Node, len(cs.Nodes))
	decodeNodes(cs.Nodes, m.Nodes)
}

func decodeNode(node *internal.Node, m *pilosa.Node) {
	m.ID = node.ID
	decodeURI(node.URI, &m.URI)
	m.IsCoordinator = node.IsCoordinator
}

func decodeURI(i *internal.URI, m *pilosa.URI) {
	m.Scheme = i.Scheme
	m.Host = i.Host
	m.Port = uint16(i.Port)
}

func decodeCreateShardMessage(pb *internal.CreateShardMessage, m *pilosa.CreateShardMessage) {
	m.Index = pb.Index
	m.Shard = pb.Shard
}

func decodeCreateIndexMessage(pb *internal.CreateIndexMessage, m *pilosa.CreateIndexMessage) {
	m.Index = pb.Index
	m.Meta = &pilosa.IndexOptions{}
	decodeIndexMeta(pb.Meta, m.Meta)
}

func decodeIndexMeta(pb *internal.IndexMeta, m *pilosa.IndexOptions) {
	m.Keys = pb.Keys
}

func decodeDeleteIndexMessage(pb *internal.DeleteIndexMessage, m *pilosa.DeleteIndexMessage) {
	m.Index = pb.Index
}

func decodeCreateFieldMessage(pb *internal.CreateFieldMessage, m *pilosa.CreateFieldMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.Meta = &pilosa.FieldOptions{}
	decodeFieldOptions(pb.Meta, m.Meta)
}

func decodeDeleteFieldMessage(pb *internal.DeleteFieldMessage, m *pilosa.DeleteFieldMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
}

func decodeCreateViewMessage(pb *internal.CreateViewMessage, m *pilosa.CreateViewMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.View = pb.View
}

func decodeDeleteViewMessage(pb *internal.DeleteViewMessage, m *pilosa.DeleteViewMessage) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.View = pb.View
}

func decodeResizeInstructionComplete(pb *internal.ResizeInstructionComplete, m *pilosa.ResizeInstructionComplete) {
	m.JobID = pb.JobID
	m.Node = &pilosa.Node{}
	decodeNode(pb.Node, m.Node)
	m.Error = pb.Error
}

func decodeSetCoordinatorMessage(pb *internal.SetCoordinatorMessage, m *pilosa.SetCoordinatorMessage) {
	m.New = &pilosa.Node{}
	decodeNode(pb.New, m.New)
}

func decodeUpdateCoordinatorMessage(pb *internal.UpdateCoordinatorMessage, m *pilosa.UpdateCoordinatorMessage) {
	m.New = &pilosa.Node{}
	decodeNode(pb.New, m.New)
}

func decodeNodeStateMessage(pb *internal.NodeStateMessage, m *pilosa.NodeStateMessage) {
	m.NodeID = pb.NodeID
	m.State = pb.State
}

func decodeNodeEventMessage(pb *internal.NodeEventMessage, m *pilosa.NodeEvent) {
	m.Event = pilosa.NodeEventType(pb.Event)
	m.Node = &pilosa.Node{}
	decodeNode(pb.Node, m.Node)
}

func decodeNodeStatus(pb *internal.NodeStatus, m *pilosa.NodeStatus) {
	m.Node = &pilosa.Node{}
	decodeNode(pb.Node, m.Node)
	m.MaxShards = pb.MaxShards.Standard
	m.Schema = &pilosa.Schema{}
	decodeSchema(pb.Schema, m.Schema)
}

func decodeRecalculateCaches(pb *internal.RecalculateCaches, m *pilosa.RecalculateCaches) {}

func decodeQueryRequest(pb *internal.QueryRequest, m *pilosa.QueryRequest) {
	m.Query = pb.Query
	m.Shards = pb.Shards
	m.ColumnAttrs = pb.ColumnAttrs
	m.Remote = pb.Remote
	m.ExcludeRowAttrs = pb.ExcludeRowAttrs
	m.ExcludeColumns = pb.ExcludeColumns
}

func decodeImportRequest(pb *internal.ImportRequest, m *pilosa.ImportRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.Shard = pb.Shard
	m.RowIDs = pb.RowIDs
	m.ColumnIDs = pb.ColumnIDs
	m.RowKeys = pb.RowKeys
	m.ColumnKeys = pb.ColumnKeys
	m.Timestamps = pb.Timestamps
}

func decodeImportValueRequest(pb *internal.ImportValueRequest, m *pilosa.ImportValueRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.Shard = pb.Shard
	m.ColumnIDs = pb.ColumnIDs
	m.ColumnKeys = pb.ColumnKeys
	m.Values = pb.Values
}

func decodeImportResponse(pb *internal.ImportResponse, m *pilosa.ImportResponse) {
	m.Err = pb.Err
}

func decodeBlockDataRequest(pb *internal.BlockDataRequest, m *pilosa.BlockDataRequest) {
	m.Index = pb.Index
	m.Field = pb.Field
	m.View = pb.View
	m.Shard = pb.Shard
	m.Block = pb.Block
}

func decodeBlockDataResponse(pb *internal.BlockDataResponse, m *pilosa.BlockDataResponse) {
	m.RowIDs = pb.RowIDs
	m.ColumnIDs = pb.ColumnIDs
}

func decodeQueryResponse(pb *internal.QueryResponse, m *pilosa.QueryResponse) {
	m.ColumnAttrSets = make([]*pilosa.ColumnAttrSet, len(pb.ColumnAttrSets))
	decodeColumnAttrSets(pb.ColumnAttrSets, m.ColumnAttrSets)
	if pb.Err == "" {
		m.Err = nil
	} else {
		m.Err = errors.New(pb.Err)
	}
	m.Results = make([]interface{}, len(pb.Results))
	decodeQueryResults(pb.Results, m.Results)

}

func decodeColumnAttrSets(pb []*internal.ColumnAttrSet, m []*pilosa.ColumnAttrSet) {
	for i := range pb {
		m[i] = &pilosa.ColumnAttrSet{}
		decodeColumnAttrSet(pb[i], m[i])
	}
}

func decodeColumnAttrSet(pb *internal.ColumnAttrSet, m *pilosa.ColumnAttrSet) {
	m.ID = pb.ID
	m.Key = pb.Key
	m.Attrs = decodeAttrs(pb.Attrs)
}

func decodeQueryResults(pb []*internal.QueryResult, m []interface{}) {
	for i := range pb {
		m[i] = decodeQueryResult(pb[i])
	}
}

// QueryResult types.
const (
	queryResultTypeNil uint32 = iota
	queryResultTypeRow
	queryResultTypePairs
	queryResultTypeValCount
	queryResultTypeUint64
	queryResultTypeBool
)

func decodeQueryResult(pb *internal.QueryResult) interface{} {
	switch pb.Type {
	case queryResultTypeRow:
		return decodeRow(pb.Row)
	case queryResultTypePairs:
		return decodePairs(pb.Pairs)
	case queryResultTypeValCount:
		return decodeValCount(pb.ValCount)
	case queryResultTypeUint64:
		return pb.N
	case queryResultTypeBool:
		return pb.Changed
	case queryResultTypeNil:
		return nil
	}
	panic(fmt.Sprintf("unknown type: %d", pb.Type))
}

// DecodeRow converts r from its internal representation.
func decodeRow(pr *internal.Row) *pilosa.Row {
	if pr == nil {
		return nil
	}

	r := pilosa.NewRow()
	r.Attrs = decodeAttrs(pr.Attrs)
	r.Keys = pr.Keys
	for _, v := range pr.Columns {
		r.SetBit(v)
	}
	return r
}

func decodeAttrs(pb []*internal.Attr) map[string]interface{} {
	m := make(map[string]interface{}, len(pb))
	for i := range pb {
		key, value := decodeAttr(pb[i])
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

func decodeAttr(attr *internal.Attr) (key string, value interface{}) {
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

func decodePairs(a []*internal.Pair) []pilosa.Pair {
	other := make([]pilosa.Pair, len(a))
	for i := range a {
		other[i] = decodePair(a[i])
	}
	return other
}

func decodePair(pb *internal.Pair) pilosa.Pair {
	return pilosa.Pair{
		ID:    pb.ID,
		Key:   pb.Key,
		Count: pb.Count,
	}
}

func decodeValCount(pb *internal.ValCount) pilosa.ValCount {
	return pilosa.ValCount{
		Val:   pb.Val,
		Count: pb.Count,
	}
}

func encodeColumnAttrSets(a []*pilosa.ColumnAttrSet) []*internal.ColumnAttrSet {
	other := make([]*internal.ColumnAttrSet, len(a))
	for i := range a {
		other[i] = encodeColumnAttrSet(a[i])
	}
	return other
}

func encodeColumnAttrSet(set *pilosa.ColumnAttrSet) *internal.ColumnAttrSet {
	return &internal.ColumnAttrSet{
		ID:    set.ID,
		Key:   set.Key,
		Attrs: encodeAttrs(set.Attrs),
	}
}

func encodeRow(r *pilosa.Row) *internal.Row {
	if r == nil {
		return nil
	}

	return &internal.Row{
		Columns: r.Columns(),
		Keys:    r.Keys,
		Attrs:   encodeAttrs(r.Attrs),
	}
}

func encodePairs(a pilosa.Pairs) []*internal.Pair {
	other := make([]*internal.Pair, len(a))
	for i := range a {
		other[i] = encodePair(a[i])
	}
	return other
}

func encodePair(p pilosa.Pair) *internal.Pair {
	return &internal.Pair{
		ID:    p.ID,
		Key:   p.Key,
		Count: p.Count,
	}
}

func encodeValCount(vc pilosa.ValCount) *internal.ValCount {
	return &internal.ValCount{
		Val:   vc.Val,
		Count: vc.Count,
	}
}

func encodeAttrs(m map[string]interface{}) []*internal.Attr {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	a := make([]*internal.Attr, len(keys))
	for i := range keys {
		a[i] = encodeAttr(keys[i], m[keys[i]])
	}
	return a
}

// encodeAttr converts a key/value pair into an Attr internal representation.
func encodeAttr(key string, value interface{}) *internal.Attr {
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
