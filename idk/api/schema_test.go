package api

import (
	"strings"
	"testing"

	pilosaclient "github.com/molecula/featurebase/v3/client"
	"github.com/stretchr/testify/assert"
)

func TestApplyToPilosa(t *testing.T) {
	client, err := pilosaclient.NewClient("pilosa:10101")
	if !assert.NoError(t, err) {
		return
	}
	t.Cleanup(func() {
		err := client.Close()
		assert.NoError(t, err)
	})

	schema, err := client.Schema()
	if !assert.NoError(t, err) {
		return
	}
	if schema.HasIndex("apitestingest_unkeyed") {
		err := client.DeleteIndexByName("apitestingest_unkeyed")
		if !assert.NoError(t, err) {
			return
		}
	}

	schema, err = client.Schema()
	if !assert.NoError(t, err) {
		return
	}

	unkeyedIdx := schema.Index("apitestingest_unkeyed", pilosaclient.OptIndexTrackExistence(true))

	err = client.SyncSchema(schema)
	if !assert.NoError(t, err) {
		return
	}
	t.Cleanup(func() {
		err := client.DeleteIndex(unkeyedIdx)
		assert.NoError(t, err)
	})

	type FieldOpt struct { //nolint:unused
		TimeQuantum            string `json:"time-quantum,omitempty"`
		CacheSize              int    `json:"cache-size,omitempty"`
		CacheType              string `json:"cache-type,omitempty"`
		EnforceMutualExclusion bool   `json:"enforce-mutual-exclusion,omitempty"`
		Scale                  int    `json:"scale,omitempty"`
		Epoch                  string `json:"epoch,omitempty"`
		Unit                   string `json:"unit,omitempty"`
		TTL                    string `json:"ttl,omitempty"`
	}

	tests := []struct {
		name        string
		schemaField schemaField
		expErr      string
		expOption   string
	}{
		{
			name: "id_ttl",
			schemaField: schemaField{
				FieldName:    "id_ttl",
				FieldType:    "id",
				FieldOptions: FieldOpt{"YMD", 0, "", false, 0, "", "", "11s"},
			},
			expErr:    "",
			expOption: `"{"options":{"noStandardView":false,"timeQuantum":"YMD","ttl":"11s","type":"time"}}"}`,
		},
		{
			name: "id_bad_ttl",
			schemaField: schemaField{
				FieldName:    "id_bad_ttl",
				FieldType:    "id",
				FieldOptions: FieldOpt{"YMD", 0, "", false, 0, "", "", "bad-ttl"},
			},
			expErr:    "unable to parse TTL",
			expOption: `"{"options":{"type":"set"}}"}`,
		},
		{
			name: "id_ttl_without_time_quantum",
			schemaField: schemaField{
				FieldName:    "id_ttl_without_time_quantum",
				FieldType:    "id",
				FieldOptions: FieldOpt{"", 0, "", false, 0, "", "", "12s"},
			},
			expErr:    "",
			expOption: `options: "{"options":{"type":"set"}}"}`,
		},
		{
			name: "string_ttl",
			schemaField: schemaField{
				FieldName:    "string_ttl",
				FieldType:    "string",
				FieldOptions: FieldOpt{"YMD", 0, "", false, 0, "", "", "13s"},
			},
			expErr:    "",
			expOption: `"{"options":{"noStandardView":false,"timeQuantum":"YMD","ttl":"13s","type":"time"}}"}`,
		},
		{
			name: "string_bad_ttl",
			schemaField: schemaField{
				FieldName:    "string_bad_ttl",
				FieldType:    "string",
				FieldOptions: FieldOpt{"YMD", 0, "", false, 0, "", "", "bad-ttl"},
			},
			expErr:    "unable to parse TTL",
			expOption: `"{"options":{"type":"set"}}"}`,
		},
		{
			name: "string_ttl_without_time_quantum",
			schemaField: schemaField{
				FieldName:    "string_ttl_without_time_quantum",
				FieldType:    "string",
				FieldOptions: FieldOpt{"", 0, "", false, 0, "", "", "14s"},
			},
			expErr:    "",
			expOption: `options: "{"options":{"type":"set"}}"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := (test.schemaField).applyToPilosa(unkeyedIdx)
			if err != nil && !strings.Contains(err.Error(), test.expErr) {
				t.Errorf("expected error: '%s', got: '%s'", test.expErr, err.Error())
			}

			if !strings.Contains(unkeyedIdx.String(), test.expOption) {
				t.Errorf("expected option: '%v', got: '%v'", test.expOption, unkeyedIdx.String())
			}
		})
	}
}
