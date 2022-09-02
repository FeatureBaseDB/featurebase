// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/pkg/errors"
)

// GobSerializer represents a Serializer that uses gob encoding. This is only
// used in tests; there's really no reason to use this instead of the proto
// serializer except that, as it's currently implemented, the proto serializer
// can't be used in internal tests (i.e test in the pilosa package) because the
// proto package imports the pilosa package, so it would result in circular
// imports. We really need all the pilosa types to be in a sub-package of
// pilosa, so that both proto and pilosa can import them without resulting in
// circular imports.
var GobSerializer Serializer = &gobSerializer{}

type gobSerializer struct{}

// Marshal is a gob-encoded implementation of the Serializer Marshal method.
func (s *gobSerializer) Marshal(msg Message) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(msg); err != nil {
		return nil, errors.Wrap(err, "gob encoding message")
	}
	return buf.Bytes(), nil
}

// Unmarshal is a gob-encoded implementation of the Serializer Unmarshal method.
func (s *gobSerializer) Unmarshal(b []byte, m Message) error {
	switch mt := m.(type) {
	case *CreateIndexMessage, *CreateFieldMessage:
		dec := gob.NewDecoder(bytes.NewReader(b))
		err := dec.Decode(mt)
		if err != nil {
			return errors.Wrapf(err, "decoding %T", mt)
		}
		return nil
	default:
		panic(fmt.Sprintf("unhandled Message of type %T: %#v", mt, m))
	}
}
