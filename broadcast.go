package pilosa

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/gogo/protobuf/proto"
)

// BroadcastHandler is the interface for the pilosa object which knows how to
// handle broadcast messages. (Hint: this is implemented by pilosa.Server)
type BroadcastHandler interface {
	ReceiveMessage(pb proto.Message) error
}

// BroadcastReceiver is the interface for the object which will listen for and
// decode broadcast messages before passing them to pilosa to handle. The
// implementation of this could be an http server which listens for messages,
// gets the protobuf payload, and then passes it to
// BroadcastHandler.ReceiveMessage.
type BroadcastReceiver interface {
	// Start starts listening for broadcast messages - it should return
	// immediately, spawning a goroutine if necessary.
	Start(BroadcastHandler) error
}

type nopBroadcastReceiver struct{}

func (n *nopBroadcastReceiver) Start(b BroadcastHandler) error { return nil }

var NopBroadcastReceiver = &nopBroadcastReceiver{}

type HTTPBroadcastReceiver struct {
	port      string
	handler   BroadcastHandler
	logOutput io.Writer
}

func NewHTTPBroadcastReceiver(port string, logOutput io.Writer) *HTTPBroadcastReceiver {
	return &HTTPBroadcastReceiver{
		port:      port,
		logOutput: logOutput,
	}
}

func (rec *HTTPBroadcastReceiver) Start(b BroadcastHandler) error {
	rec.handler = b
	go func() {
		err := http.ListenAndServe(":"+rec.port, rec)
		if err != nil {
			fmt.Fprintf(rec.logOutput, "Error listening on %v for HTTPBroadcastReceiver: %v\n", ":"+rec.port, err)
		}
	}()
	return nil
}

func (rec *HTTPBroadcastReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	}

	// Read entire body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Unmarshal message to specific proto type.
	m, err := UnmarshalMessage(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := rec.handler.ReceiveMessage(m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
