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

	// Broker handles Send
	Broker MessageBroker

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

//////////////////////////////////////////////////////////////////

// MessageBroker is an interface for handling incoming/outgoing messages.
type MessageBroker interface {
	Send(pb proto.Message, method string) error
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

//////////////////////////////////////////////////////////////////

// HTTPMessageBroker represents a NodeSet that broadcasts messages over HTTP.
type HTTPMessageBroker struct {
	server *Server
}

// NewHTTPMessageBroker returns a new instance of HTTPMessageBroker.
func NewHTTPMessageBroker(s *Server) *HTTPMessageBroker {
	return &HTTPMessageBroker{server: s}
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
		if n.Host == h.server.Host {
			continue
		}
		node := n
		g.Go(func() error {
			return h.sendNodeMessage(node, buf)
		})
	}
	return g.Wait()
}

func (h *HTTPMessageBroker) nodes() ([]*Node, error) {
	if h.server == nil {
		return nil, errors.New("HTTPMessageBroker has no reference to Server.")
	}
	nodeset, ok := h.server.Cluster.NodeSet.(*HTTPNodeSet)
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
