package kafka

import (
	"reflect"
	"strings"
	"testing"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/go-avro/avro"
)

func TestIdkSchemaToAvroRecordSchema(t *testing.T) {

	tests := []struct {
		name            string
		field           idk.Field
		fields          []idk.Field
		schemaField     []*avro.SchemaField
		expRecordSchema string
		expErr          string
	}{
		{
			name: "string field ttl",
			fields: []idk.Field{
				idk.StringField{
					NameVal:     "a",
					DestNameVal: "a",
					Quantum:     "YMD",
					TTL:         "11s",
				},
			},
			schemaField: []*avro.SchemaField{
				{
					Name: "a",
					Type: &avro.UnionSchema{
						Types: []avro.Schema{
							&avro.NullSchema{},
							&avro.StringSchema{
								Properties: map[string]interface{}{
									"cacheSize": "0",
									"cacheType": "",
									"mutex":     false,
									"quantum":   "YMD",
									"ttl":       "11s",
								},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			name: "string field bad ttl",
			fields: []idk.Field{
				idk.StringField{
					NameVal:     "b",
					DestNameVal: "b",
					Quantum:     "YMD",
					TTL:         "bad-ttl",
				},
			},
			expRecordSchema: "bad-ttl-no-recordSchema",
			expErr:          "unable to parse TTL",
		},
		{
			name: "string field ttl without quantum",
			fields: []idk.Field{
				idk.StringField{
					NameVal:     "c",
					DestNameVal: "c",
					TTL:         "12s",
				},
			},
			schemaField: []*avro.SchemaField{
				{
					Name: "c",
					Type: &avro.UnionSchema{
						Types: []avro.Schema{
							&avro.NullSchema{},
							&avro.StringSchema{
								Properties: map[string]interface{}{
									"cacheSize": "50000",
									"cacheType": "ranked",
									"mutex":     false,
									"quantum":   "",
									"ttl":       "0s",
								},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			name: "id field ttl",
			fields: []idk.Field{
				idk.IDField{
					NameVal:     "d",
					DestNameVal: "d",
					Quantum:     "YMD",
					TTL:         "13s",
				},
			},
			schemaField: []*avro.SchemaField{
				{
					Name: "d",
					Type: &avro.UnionSchema{
						Types: []avro.Schema{
							&avro.NullSchema{},
							&avro.LongSchema{
								Properties: map[string]interface{}{
									"cacheSize": "0",
									"cacheType": "",
									"mutex":     false,
									"quantum":   "YMD",
									"ttl":       "13s",
									"fieldType": "id",
								},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			name: "id field ttl without quantum",
			fields: []idk.Field{
				idk.IDField{
					NameVal:     "e",
					DestNameVal: "e",
					TTL:         "14s",
				},
			},
			schemaField: []*avro.SchemaField{
				{
					Name: "e",
					Type: &avro.UnionSchema{
						Types: []avro.Schema{
							&avro.NullSchema{},
							&avro.LongSchema{
								Properties: map[string]interface{}{
									"cacheSize": "50000",
									"cacheType": "ranked",
									"mutex":     false,
									"quantum":   "",
									"ttl":       "0s",
									"fieldType": "id",
								},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			name: "string array field ttl",
			fields: []idk.Field{
				idk.StringArrayField{
					NameVal:     "f",
					DestNameVal: "f",
					Quantum:     "YMD",
					TTL:         "15s",
				},
			},
			schemaField: []*avro.SchemaField{
				{
					Name: "f",
					Type: &avro.UnionSchema{
						Types: []avro.Schema{
							&avro.NullSchema{},
							&avro.ArraySchema{
								Items: &avro.StringSchema{
									Properties: map[string]interface{}{
										"cacheSize": "0",
										"cacheType": "",
										"quantum":   "YMD",
										"ttl":       "15s",
									},
								},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			name: "string array field ttl without quantum",
			fields: []idk.Field{
				idk.StringArrayField{
					NameVal:     "g",
					DestNameVal: "g",
					TTL:         "16s",
				},
			},
			schemaField: []*avro.SchemaField{
				{
					Name: "g",
					Type: &avro.UnionSchema{
						Types: []avro.Schema{
							&avro.NullSchema{},
							&avro.ArraySchema{
								Items: &avro.StringSchema{
									Properties: map[string]interface{}{
										"cacheSize": "50000",
										"cacheType": "ranked",
										"quantum":   "",
										"ttl":       "0s",
									},
								},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			name: "id array field ttl",
			fields: []idk.Field{
				idk.IDArrayField{
					NameVal:     "h",
					DestNameVal: "h",
					Quantum:     "YMD",
					TTL:         "17s",
				},
			},
			schemaField: []*avro.SchemaField{
				{
					Name: "h",
					Type: &avro.UnionSchema{
						Types: []avro.Schema{
							&avro.NullSchema{},
							&avro.ArraySchema{
								Items: &avro.LongSchema{
									Properties: map[string]interface{}{
										"cacheSize": "0",
										"cacheType": "",
										"quantum":   "YMD",
										"ttl":       "17s",
									},
								},
							},
						},
					},
				},
			},
			expErr: "",
		},
		{
			name: "id array field ttl without quantum",
			fields: []idk.Field{
				idk.IDArrayField{
					NameVal:     "i",
					DestNameVal: "i",
					TTL:         "18s",
				},
			},
			schemaField: []*avro.SchemaField{
				{
					Name: "i",
					Type: &avro.UnionSchema{
						Types: []avro.Schema{
							&avro.NullSchema{},
							&avro.ArraySchema{
								Items: &avro.LongSchema{
									Properties: map[string]interface{}{
										"cacheSize": "50000",
										"cacheType": "ranked",
										"quantum":   "",
										"ttl":       "0s",
									},
								},
							},
						},
					},
				},
			},
			expErr: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testRecordSchema := &avro.RecordSchema{
				Name:      "idk_datagen",
				Namespace: "idk.datagen",
				Doc:       "idk-datagen",
			}
			if test.expRecordSchema == "bad-ttl-no-recordSchema" {
				testRecordSchema = nil
			} else {
				testRecordSchema.Fields = test.schemaField
			}

			field, err := idkSchemaToAvroRecordSchema(test.fields)
			if !reflect.DeepEqual(field, testRecordSchema) {
				t.Errorf("expected field: '%v', got: '%v'", testRecordSchema, field)
			}
			if err != nil && !strings.Contains(err.Error(), test.expErr) {
				t.Errorf("expected error: '%s', got: '%s'", test.expErr, err.Error())
			}
		})
	}
}
