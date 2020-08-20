// Copyright 2020 Pilosa Corp.
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

package message

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Reader reads messages.
type Reader interface {
	ReadMessage() (Message, error)
}

// Writer writes messages.
type Writer interface {
	WriteMessage(Message) error
	Flush() error
}

// WireReader reads messages in Postgres wire protocol format.
type WireReader struct {
	buf     []byte
	r       *bufio.Reader
	scratch [4]byte
}

// ReadMessage reads a single message off of the wire.
// The returned message is only valid until the next read call, as the data buffer may be re-used.
func (r *WireReader) ReadMessage() (Message, error) {
	t, err := r.r.ReadByte()
	if err != nil {
		return Message{}, err
	}

	_, err = r.r.Read(r.scratch[:])
	if err != nil {
		return Message{}, err
	}

	len := binary.BigEndian.Uint32(r.scratch[:4])
	if len < 4 {
		return Message{}, errors.New("invalid message length")
	}
	len -= 4

	if cap(r.buf) < int(len) {
		r.buf = make([]byte, len)
	} else {
		r.buf = r.buf[:len]
	}
	_, err = io.ReadFull(r.r, r.buf)
	if err != nil {
		return Message{}, err
	}

	return Message{
		Type: Type(t),
		Data: r.buf,
	}, nil
}

var _ Reader = (*WireReader)(nil)

// NewWireReader returns a message reader that reads postgres wire protocol format.
func NewWireReader(r *bufio.Reader) *WireReader {
	return &WireReader{r: r}
}

// ErrMessageTooBig is an error indicating that a message is to big to be sent or recieved.
var ErrMessageTooBig = errors.New("message is too big")

// WireWriter writes messages in Postgres wire protocol.
type WireWriter struct {
	w       *bufio.Writer
	scratch [4]byte
}

// WriteMessage writes a message onto the wire.
func (w *WireWriter) WriteMessage(message Message) error {
	if uint(len(message.Data))+4 >= 1<<31 {
		return ErrMessageTooBig
	}

	err := w.w.WriteByte(byte(message.Type))
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(w.scratch[:], uint32(len(message.Data))+4)
	_, err = w.w.Write(w.scratch[:])
	if err != nil {
		return err
	}

	_, err = w.w.Write(message.Data)
	return err
}

// Flush writes any buffered data to the underlying stream.
func (w *WireWriter) Flush() error {
	return w.w.Flush()
}

var _ Writer = (*WireWriter)(nil)

// NewWireWriter returns a message writer that writes in postgres wire protocol format.
func NewWireWriter(w *bufio.Writer) *WireWriter {
	return &WireWriter{w: w}
}

type DumpWriter struct {
	Writer
	Out io.Writer
}

func (w *DumpWriter) WriteMessage(msg Message) error {
	fmt.Fprintf(w.Out, "write type %s: %x %q\n", string(msg.Type), msg.Data, string(msg.Data))
	return w.Writer.WriteMessage(msg)
}

type DumpReader struct {
	Reader
	Out io.Writer
}

func (r *DumpReader) ReadMessage() (Message, error) {
	msg, err := r.Reader.ReadMessage()
	if err == nil {
		fmt.Fprintf(r.Out, "read type %s: %x %q\n", string(msg.Type), msg.Data, string(msg.Data))
	}
	return msg, err
}
