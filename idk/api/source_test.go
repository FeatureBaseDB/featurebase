package api

import (
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	pilosaclient "github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/stretchr/testify/assert"
)

func TestIngest(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

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
	unkeyedIdx.Field("idset")
	unkeyedIdx.Field("id", pilosaclient.OptFieldTypeMutex(pilosaclient.CacheTypeNone, 0))
	unkeyedIdx.Field("stringset", pilosaclient.OptFieldKeys(true))
	unkeyedIdx.Field("string", pilosaclient.OptFieldKeys(true), pilosaclient.OptFieldTypeMutex(pilosaclient.CacheTypeNone, 0))
	//unkeyedIdx.Field("bool", pilosaclient.OptFieldTypeBool())
	unkeyedIdx.Field("int", pilosaclient.OptFieldTypeInt())
	unkeyedIdx.Field("decimal", pilosaclient.OptFieldTypeDecimal(2))
	//unkeyedIdx.Field("timestamp", pilosaclient.OptFieldTypeTimestamp(time.Unix(1, 0).UTC(), "s"))
	keyedIdx := schema.Index("apitestingest_keyed", pilosaclient.OptIndexTrackExistence(true), pilosaclient.OptIndexKeys(true))
	keyedIdx.Field("idset")
	keyedIdx.Field("id", pilosaclient.OptFieldTypeMutex(pilosaclient.CacheTypeNone, 0))
	keyedIdx.Field("stringset", pilosaclient.OptFieldKeys(true))
	keyedIdx.Field("string", pilosaclient.OptFieldKeys(true), pilosaclient.OptFieldTypeMutex(pilosaclient.CacheTypeNone, 0))
	//keyedIdx.Field("bool", pilosaclient.OptFieldTypeBool())
	keyedIdx.Field("int", pilosaclient.OptFieldTypeInt())
	keyedIdx.Field("decimal", pilosaclient.OptFieldTypeDecimal(2))
	//keyedIdx.Field("timestamp", pilosaclient.OptFieldTypeTimestamp(time.Unix(1, 0).UTC(), "s"))

	err = client.SyncSchema(schema)
	if !assert.NoError(t, err) {
		return
	}
	t.Cleanup(func() {
		err := client.DeleteIndex(unkeyedIdx)
		assert.NoError(t, err)
		err = client.DeleteIndex(keyedIdx)
		assert.NoError(t, err)
	})

	m := *idk.NewMain()
	m.PilosaHosts = []string{"pilosa:10101"}
	m.PilosaGRPCHosts = []string{"pilosa:20101"}
	m.Verbose = true
	m.Pprof = ""
	m.Stats = ""
	m.PackBools = ""

	t.Run("Unkeyed", func(t *testing.T) {
		t.Parallel()

		err := IngestJSON(unkeyedIdx, m, strings.NewReader(`[
			{
				"primary-key": 0,
				"values": {
					"idset": [5, 7],
					"id": 4,
					"stringset": ["a"],
					"string": "h",
					"int": -13,
					"decimal": 1.01
				}
			},
			{
				"primary-key": 1,
				"values": {
					"idset": 6,
					"id": 8,
					"stringset": "b",
					"decimal": "1.02"
				}
			},
			{
				"primary-key": 2,
				"values": {}
			}
		]`))
		if !assert.NoError(t, err) {
			return
		}

		// Unfortunately the go-pilosa client is still very behind.
		resp, err := http.Post("http://pilosa:10101/index/apitestingest_unkeyed/query", "text/plain", strings.NewReader("Extract(All(), Rows(idset), Rows(id), Rows(stringset), Rows(string), Rows(int), Rows(decimal))"))
		if !assert.NoError(t, err) {
			return
		}
		defer resp.Body.Close()
		defer io.Copy(ioutil.Discard, resp.Body) //nolint: errcheck
		if !assert.Equal(t, 200, resp.StatusCode) {
			body, _ := io.ReadAll(resp.Body)
			t.Logf("request error: %s", body)
			return
		}
		data, err := io.ReadAll(resp.Body)
		if !assert.NoError(t, err) {
			return
		}

		expect := `{
			"results": [
				{
					"fields": [
						{"name":"idset","type":"[]uint64"},
						{"name":"id","type":"uint64"},
						{"name":"stringset","type":"[]string"},
						{"name":"string","type":"string"},
						{"name":"int","type":"int64"},
						{"name":"decimal","type":"decimal"}
					],
					"columns": [
						{"column":0,"rows":[[5,7],4,["a"],"h",-13,1.01]},
						{"column":1,"rows":[[6],8,["b"],null,null,1.02]},
						{"column":2,"rows":[[],null,[],null,null,null]}
					]
				}
			]
		}`
		assert.JSONEq(t, expect, string(data))
	})
	t.Run("Keyed", func(t *testing.T) {
		t.Parallel()

		err := IngestJSON(keyedIdx, m, strings.NewReader(`[
			{
				"primary-key": "a",
				"values": {
					"idset": [5, 7],
					"id": 4,
					"stringset": ["a"],
					"string": "h",
					"int": -13,
					"decimal": 1.01
				}
			},
			{
				"primary-key": "b",
				"values": {
					"idset": 6,
					"id": 8,
					"stringset": "b",
					"decimal": "1.02"
				}
			},
			{
				"primary-key": "c",
				"values": {}
			}
		]`))
		if !assert.NoError(t, err) {
			return
		}

		// Unfortunately the go-pilosa client is still very behind.
		resp, err := http.Post("http://pilosa:10101/index/apitestingest_keyed/query", "text/plain", strings.NewReader("Extract(All(), Rows(idset), Rows(id), Rows(stringset), Rows(string), Rows(int), Rows(decimal))"))
		if !assert.NoError(t, err) {
			return
		}
		defer resp.Body.Close()
		defer io.Copy(ioutil.Discard, resp.Body) //nolint: errcheck
		if !assert.Equal(t, 200, resp.StatusCode) {
			body, _ := io.ReadAll(resp.Body)
			t.Logf("request error: %s", body)
			return
		}
		data, err := io.ReadAll(resp.Body)
		if !assert.NoError(t, err) {
			return
		}

		expect := `{
			"results": [
				{
					"fields": [
						{"name":"idset","type":"[]uint64"},
						{"name":"id","type":"uint64"},
						{"name":"stringset","type":"[]string"},
						{"name":"string","type":"string"},
						{"name":"int","type":"int64"},
						{"name":"decimal","type":"decimal"}
					],
					"columns": [
						{"column":"a","rows":[[5,7],4,["a"],"h",-13,1.01]},
						{"column":"b","rows":[[6],8,["b"],null,null,1.02]},
						{"column":"c","rows":[[],null,[],null,null,null]}
					]
				}
			]
		}`
		assert.JSONEq(t, expect, string(data))
	})
}
