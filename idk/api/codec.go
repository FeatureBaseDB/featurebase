package api

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

// newJSONCodec creates a codec to stream a JSON array of objects, each of which represents a record.
// This returns an error if the data stream does not contain the start of a valid JSON array.
func newJSONCodec(r io.Reader) (*jsonCodec, error) {
	// Create a JSON decoder.
	d := json.NewDecoder(r)

	// Enable the json.Number type in parsed output.
	// This allows us to retain precision on numbers, rather than converting them to floating point.
	d.UseNumber()

	// Do not allow unknown fields.
	d.DisallowUnknownFields()

	// Read the opening bracket of the array.
	token, err := d.Token()
	if err != nil {
		if err == io.EOF {
			// The input included nothing (except maybe whitespace).
			// This is not expected.
			err = io.ErrUnexpectedEOF
		}
		return nil, errors.Wrap(err, "parsing start of JSON data")
	}
	switch token {
	case json.Delim('['):
		// This is the start of the array of records.

	case json.Delim('{'):
		// The input is a JSON object instead of an array.
		// This seems like an easy user error, so provide a descriptive error.
		return nil, errors.New("expected an array of records but found an object")

	default:
		// This is a number or something.
		return nil, errors.Errorf("expected array of records but got %v", token)
	}

	return &jsonCodec{d: d}, nil
}

type record struct {
	PrimaryKey interface{}            `json:"primary-key"`
	Values     map[string]interface{} `json:"values"`
}

type jsonCodec struct {
	d *json.Decoder
}

func (c *jsonCodec) Next() (record, error) {
	if !c.d.More() {
		// There are no more records in the stream.
		// Read the ']'.
		_, err := c.d.Token()
		if err != nil {
			if err == io.EOF {
				// If the closing bracket is missing, then the stream was terminated prematurely.
				err = io.ErrUnexpectedEOF
			}
			return record{}, errors.Wrap(err, "parsing end of record array")
		}

		// There are no more records.
		return record{}, io.EOF
	}

	// Parse the record.
	var r record
	err := c.d.Decode(&r)
	if err != nil {
		return record{}, errors.Wrap(err, "parsing record")
	}

	return r, nil
}
