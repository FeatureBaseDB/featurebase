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

package internal

import (
	"io"
	"io/ioutil"

	"github.com/gogo/protobuf/proto"
)

//go:generate protoc --gofast_out=. private.proto
//go:generate protoc --gofast_out=. public.proto

type Request proto.Message

type Response proto.Message

// Encoder encodes messages to a writer.
type Encoder struct {
	w io.Writer
}

// NewEncoder returns a new instance of Encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

// Encode marshals m into bytes and writes them to r.
func (enc *Encoder) Encode(pb proto.Message) error {
	buf, err := proto.Marshal(pb)
	if err != nil {
		return err
	}

	if _, err := enc.w.Write(buf); err != nil {
		return err
	}

	return nil
}

// Decoder decodes messages from a reader.
type Decoder struct {
	r io.Reader
}

// NewDecoder returns a new instance of Decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// Decode reads all bytes from the reader and unmarshals them into pb.
func (dec *Decoder) Decode(pb proto.Message) error {
	buf, err := ioutil.ReadAll(dec.r)
	if err != nil {
		return err
	}

	return proto.Unmarshal(buf, pb)
}
