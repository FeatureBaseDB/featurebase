package pilosa

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Test custom UnmarshalJSON for postDBRequest object
func TestPostDBRequestUnmarshalJSON(t *testing.T) {
	tests := []struct {
		json     string
		expected postDBRequest
		err      string
	}{
		{json: `{"options": {}}`, expected: postDBRequest{Options: DBOptions{}}},
		{json: `{"options": 4}`, err: "options is not map[string]interface{}"},
		{json: `{"option": {}}`, err: "Unknown key: option:map[]"},
		{json: `{"options": {"columnLabel": "test"}}`, expected: postDBRequest{Options: DBOptions{ColumnLabel: "test"}}},
		{json: `{"options": {"columnLabl": "test"}}`, err: "invalid key for options {columnLabl:test}"},
	}
	for _, test := range tests {
		actual := &postDBRequest{}
		err := json.Unmarshal([]byte(test.json), actual)

		if err != nil {
			if test.err == "" || test.err != err.Error() {
				t.Errorf("expected error: %v, but got result: %v", test.err, err)
			}
		} else {
			if test.err != "" {
				t.Errorf("expected error: %v, but got no error", test.err)
			}
		}

		if test.err == "" {
			if !reflect.DeepEqual(*actual, test.expected) {
				t.Errorf("expected: %v, but got: %v", test.expected, *actual)
			}
		}

	}
}

// Test custom UnmarshalJSON for postFrameRequest object
func TestPostFrameRequestUnmarshalJSON(t *testing.T) {
	tests := []struct {
		json     string
		expected postFrameRequest
		err      string
	}{
		{json: `{"options": {}}`, expected: postFrameRequest{Options: FrameOptions{}}},
		{json: `{"options": 4}`, err: "options is not map[string]interface{}"},
		{json: `{"option": {}}`, err: "Unknown key: {option:map[]}"},
		{json: `{"options": {"rowLabel": "test"}}`, expected: postFrameRequest{Options: FrameOptions{RowLabel: "test"}}},
		{json: `{"options": {"rowLabl": "test"}}`, err: "invalid key for options {rowLabl:test}"},
	}
	for _, test := range tests {
		actual := &postFrameRequest{}
		err := json.Unmarshal([]byte(test.json), actual)
		if err != nil {
			if test.err == "" || test.err != err.Error() {
				t.Errorf("expected error: %v, but got result: %v", test.err, err)
			}
		}

		if test.err == "" {
			if !reflect.DeepEqual(*actual, test.expected) {
				t.Errorf("expected: %v, but got: %v", test.expected, *actual)
			}
		}

	}
}
