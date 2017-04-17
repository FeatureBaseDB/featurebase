package pilosa

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"

	"golang.org/x/sync/errgroup"

	"net"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// Broadcaster is an interface for handling incoming/outgoing messages.
type Broadcaster interface {
	SendSync(pb proto.Message) error
	SendAsync(pb proto.Message) error
}

func init() {
	NopBroadcaster = &nopBroadcaster{}
}

var NopBroadcaster Broadcaster

// nopBroadcaster represents a Broadcaster that doesn't do anything.
type nopBroadcaster struct{}

// SendSync A no-op implemenetation of Broadcaster SendSync method.
func (c *nopBroadcaster) SendSync(pb proto.Message) error {
	fmt.Println("NOPBroadcaster: SendSync") // TODO remove or log properly?
	return nil
}

// SendAsync A no-op implemenetation of Broadcaster SendAsync method.
func (c *nopBroadcaster) SendAsync(pb proto.Message) error {
	fmt.Println("NOPBroadcaster: SendAsync") // TODO remove or log properly?
	return nil
}

//////////////////////////////////////////////////////////////////

// HTTPBroadcaster represents a NodeSet that broadcasts messages over HTTP.
type HTTPBroadcaster struct {
	server       *Server
	internalPort string
}

// NewHTTPBroadcaster returns a new instance of HTTPBroadcaster.
func NewHTTPBroadcaster(s *Server, internalPort string) *HTTPBroadcaster {
	return &HTTPBroadcaster{server: s}
}

// SendSync sends a protobuf message to all nodes simultaneously.
// It waits for all nodes to respond before the function returns (and returns any errors).
func (h *HTTPBroadcaster) SendSync(pb proto.Message) error {
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

// SendAsync exists to implement the Broadcaster interface, but just calls
// SendSync.
func (h *HTTPBroadcaster) SendAsync(pb proto.Message) error {
	return h.SendSync(pb)
}

func (h *HTTPBroadcaster) nodes() ([]*Node, error) {
	if h.server == nil {
		return nil, errors.New("HTTPBroadcaster has no reference to Server.")
	}
	nodeset, ok := h.server.Cluster.NodeSet.(*HTTPNodeSet)
	if !ok {
		return nil, errors.New("NodeSet cannot be caste to HTTPNodeSet.")
	}
	return nodeset.Nodes(), nil
}

func (h *HTTPBroadcaster) sendNodeMessage(node *Node, msg []byte) error {
	var client *http.Client
	client = http.DefaultClient

	host, _, err := net.SplitHostPort(node.Host)

	// Create HTTP request.
	req, err := http.NewRequest("POST", (&url.URL{
		Scheme: "http",
		Host:   host + ":" + h.internalPort,
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
