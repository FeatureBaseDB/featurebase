// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pb

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
