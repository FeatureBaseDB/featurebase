package api

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONCodec(t *testing.T) {
	t.Parallel()

	// Ensure that an empty input is handled gracefully.
	t.Run("MissingInput", func(t *testing.T) {
		t.Parallel()

		_, err := newJSONCodec(strings.NewReader(``))
		assert.EqualError(t, err, "parsing start of JSON data: unexpected EOF")
	})

	// Ensure that something other than an array is handled gracefully.
	t.Run("Object", func(t *testing.T) {
		t.Parallel()

		_, err := newJSONCodec(strings.NewReader(`{}`))
		assert.EqualError(t, err, "expected an array of records but found an object")
	})

	// Ensure that an incomplete array is handled gracefully.
	t.Run("IncompleteArray", func(t *testing.T) {
		t.Parallel()

		codec, err := newJSONCodec(strings.NewReader(`[`))
		if !assert.NoError(t, err) {
			return
		}

		_, err = codec.Next()
		assert.EqualError(t, err, "parsing end of record array: unexpected EOF")
	})

	// Test normal behavior.
	t.Run("Normal", func(t *testing.T) {
		t.Parallel()

		expect := []record{
			{
				PrimaryKey: json.Number("2"),
				Values: map[string]interface{}{
					"int":     json.Number("1"),
					"decimal": json.Number("2.01"),
					"string":  "h",
					"idset": []interface{}{
						json.Number("1"),
						json.Number("2"),
						json.Number("4"),
					},
					"stringset": []interface{}{"plugh"},
				},
			},
			{
				PrimaryKey: "h",
				Values:     map[string]interface{}{},
			},
		}
		codec, err := newJSONCodec(strings.NewReader(`[
			{
				"primary-key": 2,
				"values": {
					"int": 1,
					"decimal": 2.01,
					"string": "h",
					"idset": [1, 2, 4],
					"stringset": ["plugh"]
				}
			},
			{
				"primary-key": "h",
				"values": {}
			}
		]`))
		if !assert.NoError(t, err) {
			return
		}

		out, err := decodeAll(codec)
		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, expect, out)
	})
}

func decodeAll(c *jsonCodec) ([]record, error) {
	var out []record
	for {
		rec, err := c.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		out = append(out, rec)
	}

	return out, nil
}
