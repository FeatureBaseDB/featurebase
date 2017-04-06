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
		{json: `{"db": "d", "options": {}}`, expected: postDBRequest{DB: "d", Options: DBOptions{}}},
		{json: `{"db": 1, "options": {}}`, err: "db required and must be a string"},
		{json: `{"db": "d", "options": 4}`, err: "options is not map[string]interface{}"},
		{json: `{"db": "d", "option": {}}`, err: "Unknown key: option:map[]"},
		{json: `{"db": "d", "options": {"columnLabel": "test"}}`, expected: postDBRequest{DB: "d", Options: DBOptions{ColumnLabel: "test"}}},
		{json: `{"db": "d", "options": {"columnLabl": "test"}}`, err: "invalid key for options {columnLabl:test}"},
		{json: `{"db": "d", "options": {"columnLabel": "////"}}`, err: "invalid columnLabel value: ////"},
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
		{json: `{"db": "d", "frame":"f",  "options": {}}`, expected: postFrameRequest{DB: "d", Frame: "f", Options: FrameOptions{}}},
		{json: `{"db": "d", "options": {}}`, err: "db required and must be a string"},
		{json: `{"db": "d", "frame":"f", "options": 4}`, err: "options is not map[string]interface{}"},
		{json: `{"db": "d", "frame":"f", "option": {}}`, err: "Unknown key: {option:map[]}"},
		{json: `{"db": "d", "frame":"f", "options": {"rowLabel": "test"}}`, expected: postFrameRequest{DB: "d", Frame: "f", Options: FrameOptions{RowLabel: "test"}}},
		{json: `{"db": "d", "frame":"f", "options": {"rowLabl": "test"}}`, err: "invalid key for options {rowLabl:test}"},
		{json: `{"db": "d", "frame":"f", "options": {"rowLabel": "////"}}`, err: "invalid rowLabel value: ////"},
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
