package pilosa_test

import (
	"encoding/json"
	"testing"

	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/stretchr/testify/assert"
)

func TestSQL(t *testing.T) {
	t.Run("SQLResponse", func(t *testing.T) {
		t.Run("Error", func(t *testing.T) {
			body := `{"error": "bad"}`
			sqlResponse := &featurebase.WireQueryResponse{}
			err := json.Unmarshal([]byte(body), sqlResponse)
			assert.NoError(t, err)
			assert.Equal(t, "bad", sqlResponse.Error)
		})

		t.Run("Typed", func(t *testing.T) {
			body := `{
				"schema":{
					"fields":[
						{"name":"an_int","type":"int","base-type":"int"},
						{"name":"a_decimal","type":"decimal","base-type":"decimal","type-info":{"scale":2}},
						{"name":"a_stringset","type":"stringset","base-type":"stringset"},
						{"name":"an_idset","type":"idset","base-type":"idset"},
						{"name":"a_bool","type":"bool","base-type":"bool"},
						{"name":"a_string","type":"string","base-type":"string"},
						{"name":"an_id","type":"id","base-type":"id"}
					]
				},
				"data":[
					[1, 12.34, ["foo", "bar"], [4,5], true, "foobar", 8]
				],
				"error":"",
				"warnings":null
			}`
			sqlResponse := &featurebase.WireQueryResponse{}
			//err := json.Unmarshal([]byte(body), sqlResponse)
			err := sqlResponse.UnmarshalJSONTyped([]byte(body), true)
			assert.NoError(t, err)

			row0 := sqlResponse.Data[0]

			assert.Equal(t, int64(1), row0[0])
			assert.Equal(t, pql.NewDecimal(1234, 2), row0[1])
			assert.Equal(t, featurebase.StringSet([]string{"foo", "bar"}), row0[2])
			assert.Equal(t, featurebase.IDSet([]int64{4, 5}), row0[3])
			assert.Equal(t, true, row0[4])
			assert.Equal(t, "foobar", row0[5])
			assert.Equal(t, int64(8), row0[6])

			// Check the set stringers.
			assert.Equal(t, "['foo', 'bar']", row0[2].(featurebase.StringSet).String())
			assert.Equal(t, "[4, 5]", row0[3].(featurebase.IDSet).String())
		})

		t.Run("Untyped", func(t *testing.T) {
			body := `{
				"schema":{
					"fields":[
						{"name":"an_int","type":"int","base-type":"int"},
						{"name":"a_decimal","type":"decimal","base-type":"decimal","type-info":{"scale":2}},
						{"name":"a_stringset","type":"stringset","base-type":"stringset"},
						{"name":"an_idset","type":"idset","base-type":"idset"},
						{"name":"a_bool","type":"bool","base-type":"bool"},
						{"name":"a_string","type":"string","base-type":"string"},
						{"name":"an_id","type":"id","base-type":"id"}
					]
				},
				"data":[
					[1, 12.34, ["foo", "bar"], [4,5], true, "foobar", 8]
				],
				"error":"",
				"warnings":null
			}`
			sqlResponse := &featurebase.WireQueryResponse{}
			err := json.Unmarshal([]byte(body), sqlResponse)
			assert.NoError(t, err)

			row0 := sqlResponse.Data[0]

			assert.Equal(t, int64(1), row0[0])
			assert.Equal(t, pql.NewDecimal(1234, 2), row0[1])
			assert.Equal(t, []string{"foo", "bar"}, row0[2])
			assert.Equal(t, []int64{4, 5}, row0[3])
			assert.Equal(t, true, row0[4])
			assert.Equal(t, "foobar", row0[5])
			assert.Equal(t, int64(8), row0[6])
		})
	})
}
