// Copyright 2021 Molecula Corp. All rights reserved.
package http

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/pql"
)

// Test custom UnmarshalJSON for postIndexRequest object
func TestPostIndexRequestUnmarshalJSON(t *testing.T) {
	tests := []struct {
		json     string
		expected postIndexRequest
		err      string
	}{
		{json: `{"options": {}}`, expected: postIndexRequest{Options: pilosa.IndexOptions{TrackExistence: true}}},
		{json: `{"options": {"trackExistence": false}}`, expected: postIndexRequest{Options: pilosa.IndexOptions{TrackExistence: false}}},
		{json: `{"options": {"keys": true}}`, expected: postIndexRequest{Options: pilosa.IndexOptions{Keys: true, TrackExistence: true}}},
		{json: `{"options": 4}`, err: "options is not map[string]interface{}"},
		{json: `{"option": {}}`, err: "unknown key: option:map[]"},
		{json: `{"options": {"badKey": "test"}}`, err: "unknown key: badKey:test"},
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
				t.Errorf("expected: %v, but got: %v for JSON: %s", test.expected, *actual, test.json)
			}
		}
	}
}

// Test custom UnmarshalJSON for postFieldRequest object
func TestPostFieldRequestUnmarshalJSON(t *testing.T) {
	foo := "foo"
	tests := []struct {
		json     string
		expected postFieldRequest
		err      string
	}{
		{json: `{"options": {}}`, expected: postFieldRequest{}},
		{json: `{"options": 4}`, err: "json: cannot unmarshal number"},
		{json: `{"option": {}}`, err: `json: unknown field "option"`},
		{json: `{"options": {"badKey": "test"}}`, err: `json: unknown field "badKey"`},
		{json: `{"options": {"inverseEnabled": true}}`, err: `json: unknown field "inverseEnabled"`},
		{json: `{"options": {"cacheType": "foo"}}`, expected: postFieldRequest{Options: fieldOptions{CacheType: &foo}}},
		{json: `{"options": {"inverse": true, "cacheType": "foo"}}`, err: `json: unknown field "inverse"`},
	}
	for i, test := range tests {
		actual := &postFieldRequest{}
		dec := json.NewDecoder(bytes.NewReader([]byte(test.json)))
		dec.DisallowUnknownFields()
		err := dec.Decode(actual)
		if err != nil {
			if test.err == "" || !strings.HasPrefix(err.Error(), test.err) {
				t.Errorf("test %d: expected error: %v, but got result: %v", i, test.err, err)
			}
		}

		if test.err == "" {
			if !reflect.DeepEqual(*actual, test.expected) {
				t.Errorf("test %d: expected: %v, but got: %v", i, test.expected, *actual)
			}
		}
	}
}

func stringPtr(s string) *string {
	return &s
}

func decimalPtr(d pql.Decimal) *pql.Decimal {
	return &d
}

// Test fieldOption validation.
func TestFieldOptionValidation(t *testing.T) {
	timeQuantum := pilosa.TimeQuantum("YMD")
	defaultCacheSize := uint32(pilosa.DefaultCacheSize)
	tests := []struct {
		json     string
		expected postFieldRequest
		err      string
	}{
		// FieldType: Set
		{json: `{"options": {}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:      pilosa.FieldTypeSet,
			CacheType: stringPtr(pilosa.DefaultCacheType),
			CacheSize: &defaultCacheSize,
		}}},
		{json: `{"options": {"type": "set"}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:      pilosa.FieldTypeSet,
			CacheType: stringPtr(pilosa.DefaultCacheType),
			CacheSize: &defaultCacheSize,
		}}},
		{json: `{"options": {"type": "set", "cacheType": "lru"}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:      pilosa.FieldTypeSet,
			CacheType: stringPtr("lru"),
			CacheSize: &defaultCacheSize,
		}}},
		{json: `{"options": {"type": "set", "min": 0}}`, err: "min does not apply to field type set"},
		{json: `{"options": {"type": "set", "max": 100}}`, err: "max does not apply to field type set"},
		{json: `{"options": {"type": "set", "timeQuantum": "YMD"}}`, err: "timeQuantum does not apply to field type set"},

		// FieldType: Int
		{json: `{"options": {"type": "int"}}`, err: "min is required for field type int"},
		{json: `{"options": {"type": "int", "min": 0}}`, err: "max is required for field type int"},
		{json: `{"options": {"type": "int", "min": 0, "max": 1001}}`, expected: postFieldRequest{Options: fieldOptions{
			Type: pilosa.FieldTypeInt,
			Min:  decimalPtr(pql.NewDecimal(0, 0)),
			Max:  decimalPtr(pql.NewDecimal(1001, 0)),
		}}},
		{json: `{"options": {"type": "int", "min": 0, "max": 1000, "cacheType": "ranked"}}`, err: "cacheType does not apply to field type int"},
		{json: `{"options": {"type": "int", "min": 0, "max": 1000, "cacheSize": 1000}}`, err: "cacheSize does not apply to field type int"},
		{json: `{"options": {"type": "int", "min": 0, "max": 1000, "timeQuantum": "YMD"}}`, err: "timeQuantum does not apply to field type int"},

		// FieldType: Time
		{json: `{"options": {"type": "time"}}`, err: "timeQuantum is required for field type time"},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD"}}`, expected: postFieldRequest{Options: fieldOptions{
			Type:        pilosa.FieldTypeTime,
			TimeQuantum: &timeQuantum,
		}}},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD", "min": 0}}`, err: "min does not apply to field type time"},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD", "max": 1000}}`, err: "max does not apply to field type time"},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD", "cacheType": "ranked"}}`, err: "cacheType does not apply to field type time"},
		{json: `{"options": {"type": "time", "timeQuantum": "YMD", "cacheSize": 1000}}`, err: "cacheSize does not apply to field type time"},
	}
	for i, test := range tests {
		actual := &postFieldRequest{}
		dec := json.NewDecoder(bytes.NewReader([]byte(test.json)))
		dec.DisallowUnknownFields()
		err := dec.Decode(actual)
		if err != nil {
			t.Errorf("test %d: %v", i, err)
		}

		// Validate field options.
		if err := actual.Options.validate(); err != nil {
			if test.err == "" || test.err != err.Error() {
				t.Errorf("test %d: expected error: %v, but got result: %v", i, test.err, err)
			}
		}

		if test.err == "" {
			if !reflect.DeepEqual(*actual, test.expected) {
				t.Errorf("test %d: expected: %v, but got: %v", i, test.expected, *actual)
			}
		}
	}
}
