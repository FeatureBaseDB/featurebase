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

package pilosa

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Test custom UnmarshalJSON for postIndexRequest object
func TestPostIndexRequestUnmarshalJSON(t *testing.T) {
	tests := []struct {
		json     string
		expected postIndexRequest
		err      string
	}{
		{json: `{"options": {}}`, expected: postIndexRequest{Options: IndexOptions{}}},
		{json: `{"options": 4}`, err: "options is not map[string]interface{}"},
		{json: `{"option": {}}`, err: "Unknown key: option:map[]"},
		{json: `{"options": {"badKey": "test"}}`, err: "Unknown key: badKey:test"},
	}
	for _, test := range tests {
		actual := &postIndexRequest{}
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
		{json: `{"option": {}}`, err: "Unknown key: option:map[]"},
		{json: `{"options": {"badKey": "test"}}`, err: "Unknown key: badKey:test"},
		{json: `{"options": {"inverseEnabled": true}}`, expected: postFrameRequest{Options: FrameOptions{InverseEnabled: true}}},
		{json: `{"options": {"inverseEnabled": true, "cacheType": "type"}}`, expected: postFrameRequest{Options: FrameOptions{InverseEnabled: true, CacheType: "type"}}},
		{json: `{"options": {"inverse": true, "cacheType": "type"}}`, err: "Unknown key: inverse:true"},
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
