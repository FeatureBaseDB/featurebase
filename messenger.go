package pilosa

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"reflect"

	"golang.org/x/sync/errgroup"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// Messenger represents an internal message handler.
type Messenger struct {

	// Broker handles Send/Receive Messages.
	Broker MessageBroker

	Index *Index

	// Local hostname & cluster configuration.
	Host    string
	Cluster *Cluster

	// The writer for any logging.
	LogOutput io.Writer
}

// NewMessenger returns a new instance of Messenger with a default logger.
func NewMessenger() *Messenger {
	return &Messenger{
		Broker:    NopMessageBroker,
		LogOutput: os.Stderr,
	}
}

func (m *Messenger) SendMessage(pb proto.Message, method string) error {
	if m.Broker == nil {
		return errors.New("Messenger.Broker is not defined.")
	}
	return m.Broker.Send(pb, method)
}
func (m *Messenger) ReceiveMessage(pb proto.Message) error {
	return m.handleMessage(pb)
}

// handleMessage handles protobuf Messages sent to nodes in the cluster.
func (m *Messenger) handleMessage(pb proto.Message) error {
	switch obj := pb.(type) {
	case *internal.CreateSliceMessage:
		d := m.Index.DB(obj.DB)
		if d == nil {
			return fmt.Errorf("Local DB not found: %s", obj.DB)
		}
		d.SetRemoteMaxSlice(obj.Slice)
	case *internal.CreateDBMessage:
		opt := DBOptions{ColumnLabel: obj.Meta.ColumnLabel}
		_, err := m.Index.CreateDB(obj.DB, opt)
		if err != nil {
			return err
		}
	case *internal.DeleteDBMessage:
		fmt.Println("DELETE:", obj.DB)
		if err := m.Index.DeleteDB(obj.DB); err != nil {
			return err
		}
	case *internal.CreateFrameMessage:
		db := m.Index.DB(obj.DB)
		opt := FrameOptions{RowLabel: obj.Meta.RowLabel}
		_, err := db.CreateFrame(obj.Frame, opt)
		if err != nil {
			return err
		}
	case *internal.DeleteFrameMessage:
		db := m.Index.DB(obj.DB)
		if err := db.DeleteFrame(obj.Frame); err != nil {
			return err
		}
	}
	return nil
}

// LocalState returns the state of the local node as well as the
// index (dbs/frames) according to the local node.
// In a gossip implementation, memberlist.Delegate.LocalState() uses this.
// It seems odd to have this as part of Messenger, but with the
// exception of Server, it's currenntly the only object with access
// to the necessary information (Host, Index, Cluster).
func (m *Messenger) LocalState() (proto.Message, error) {
	if m.Index == nil {
		return nil, errors.New("Messenger.Index is nil.")
	}
	return &internal.NodeState{
		Host:  m.Host,
		State: "OK", // TODO: make this work, pull from m.Cluster.Node
		DBs:   encodeDBs(m.Index.DBs()),
	}, nil
}

// HandleRemoteState receives incoming NodeState from remote nodes.
func (m *Messenger) HandleRemoteState(pb proto.Message) error {
	return m.mergeRemoteState(pb.(*internal.NodeState))
}

func (m *Messenger) mergeRemoteState(ns *internal.NodeState) error {
	// TODO: update some node state value in the cluster (it should be in cluster.node i guess)

	// Create databases that don't exist.
	for _, db := range ns.DBs {
		opt := DBOptions{
			ColumnLabel: db.Meta.ColumnLabel,
			TimeQuantum: TimeQuantum(db.Meta.TimeQuantum),
		}
		d, err := m.Index.CreateDBIfNotExists(db.Name, opt)
		if err != nil {
			return err
		}
		// Create frames that don't exist.
		for _, f := range db.Frames {
			opt := FrameOptions{
				RowLabel:    f.Meta.RowLabel,
				TimeQuantum: TimeQuantum(f.Meta.TimeQuantum),
				CacheSize:   f.Meta.CacheSize,
			}
			_, err := d.CreateFrameIfNotExists(f.Name, opt)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

//////////////////////////////////////////////////////////////////

// MessageBroker is an interface for handling incoming/outgoing messages.
type MessageBroker interface {
	Send(pb proto.Message, method string) error
	Receive(pb proto.Message) error
}

//////////////////////////////////////////////////////////////////

func init() {
	NopMessageBroker = &nopMessageBroker{}
}

var NopMessageBroker MessageBroker

// nopMessageBroker represents a MessageBroker that doesn't do anything.
type nopMessageBroker struct{}

func (c *nopMessageBroker) Send(pb proto.Message, method string) error {
	fmt.Println("NOPMessageBroker: Send")
	return nil
}
func (c *nopMessageBroker) Receive(pb proto.Message) error {
	fmt.Println("NOPMessageBroker: Receive")
	return nil
}

//////////////////////////////////////////////////////////////////

// HTTPMessageBroker represents a NodeSet that broadcasts messages over HTTP.
type HTTPMessageBroker struct {
	messenger *Messenger
}

// NewHTTPMessageBroker returns a new instance of HTTPMessageBroker.
func NewHTTPMessageBroker() *HTTPMessageBroker {
	return &HTTPMessageBroker{}
}

// Send sends a protobuf message to all nodes simultaneously.
// It waits for all nodes to respond before the function returns (and returns any errors).
func (h *HTTPMessageBroker) Send(pb proto.Message, method string) error {
	// Marshal the pb to []byte
	buf, err := MarshalMessage(pb)
	if err != nil {
		return err
	}

	nodes, err := h.nodes()
	if err != nil {
		return err
	}

	var g errgroup.Group
	for _, n := range nodes {
		// Don't send the message to the local node.
		if n.Host == h.messenger.Host {
			continue
		}
		node := n
		g.Go(func() error {
			return h.sendNodeMessage(node, buf)
		})
	}
	return g.Wait()
}

// Receive is called when a node receives a message.
func (h *HTTPMessageBroker) Receive(pb proto.Message) error {
	if err := h.messenger.ReceiveMessage(pb); err != nil {
		return err
	}
	return nil
}

func (h *HTTPMessageBroker) nodes() ([]*Node, error) {
	if h.messenger == nil {
		return nil, errors.New("HTTPMessageBroker has no reference to Messenger.")
	}
	nodeset, ok := h.messenger.Cluster.NodeSet.(*HTTPNodeSet)
	if !ok {
		return nil, errors.New("NodeSet cannot be caste to HTTPNodeSet.")
	}
	return nodeset.Nodes(), nil
}

func (h *HTTPMessageBroker) sendNodeMessage(node *Node, msg []byte) error {
	var client *http.Client
	client = http.DefaultClient

	// Create HTTP request.
	req, err := http.NewRequest("POST", (&url.URL{
		Scheme: "http",
		Host:   node.Host,
		Path:   "/message",
	}).String(), bytes.NewReader(msg))
	if err != nil {
		return err
	}

	// Require protobuf encoding.
	req.Header.Set("Content-Type", "application/x-protobuf")

	// Send request to remote node.
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read response into buffer.
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	// Check status code.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status: code=%d, err=%s", resp.StatusCode, body)
	}

	return nil
}

//////////////////////////////////////////////////////////////////

const (
	MessageTypeCreateSlice = 1
	MessageTypeCreateDB    = 2
	MessageTypeDeleteDB    = 3
	MessageTypeCreateFrame = 4
	MessageTypeDeleteFrame = 5
)

func MarshalMessage(m proto.Message) ([]byte, error) {
	var typ uint8
	switch obj := m.(type) {
	case *internal.CreateSliceMessage:
		typ = MessageTypeCreateSlice
	case *internal.CreateDBMessage:
		typ = MessageTypeCreateDB
	case *internal.DeleteDBMessage:
		typ = MessageTypeDeleteDB
	case *internal.CreateFrameMessage:
		typ = MessageTypeCreateFrame
	case *internal.DeleteFrameMessage:
		typ = MessageTypeDeleteFrame
	default:
		return nil, fmt.Errorf("message type not implemented for marshalling: %s", reflect.TypeOf(obj))
	}
	buf, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	return append([]byte{typ}, buf...), nil
}

func UnmarshalMessage(buf []byte) (proto.Message, error) {
	typ, buf := buf[0], buf[1:]

	var m proto.Message
	switch typ {
	case MessageTypeCreateSlice:
		m = &internal.CreateSliceMessage{}
	case MessageTypeCreateDB:
		m = &internal.CreateDBMessage{}
	case MessageTypeDeleteDB:
		m = &internal.DeleteDBMessage{}
	case MessageTypeCreateFrame:
		m = &internal.CreateFrameMessage{}
	case MessageTypeDeleteFrame:
		m = &internal.DeleteFrameMessage{}
	default:
		return nil, fmt.Errorf("invalid message type: %d", typ)
	}

	if err := proto.Unmarshal(buf, m); err != nil {
		return nil, err
	}
	return m, nil
}
