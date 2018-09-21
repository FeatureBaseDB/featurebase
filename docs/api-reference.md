+++
title = "API Reference"
weight = 10
nav = []
+++


## API Reference

### List all index schemas

`GET /index`

Returns the schema of all indexes in JSON.

``` request
curl -XGET localhost:10101/index
```
``` response
{"indexes":[{"name":"user","fields":[{"name":"event","options":{"type":"time","timeQuantum":"YMD","keys":false}}]}]}
```

### List index schema

`GET /index/<index-name>`

Returns the schema of the specified index in JSON.

``` request
curl -XGET localhost:10101/index/user
```
``` response
{"name":"user","fields":[{"name":"event","options":{"type":"time","timeQuantum":"YMD","keys":false}}]}
```

### Create index

`POST /index/<index-name>`

Creates an index with the given name.

``` request
curl -XPOST localhost:10101/index/user
```
``` response
{"success":true}
```

### Remove index

`DELETE /index/index-name`

Removes the given index.

``` request
curl -XDELETE localhost:10101/index/user
```
``` response
{"success":true}
```

### Query index

`POST /index/<index-name>/query`

Sends a [query](../query-language/) to the Pilosa server with the given index. The request body is UTF-8 encoded text and response body is in JSON by default.

``` request
curl localhost:10101/index/user/query \
     -X POST \
     -d 'Row(language=5)'
```
``` response
{"results":[{"attrs":{},"columns":[100]}]}
```

In order to send protobuf binaries in the request and response, set `Content-Type` and `Accept` headers to: `application/x-protobuf`.

The response doesn't include column attributes by default. To return them, set the `columnAttrs` query argument to `true`.

The query is executed for all [shards](../data-model/#shard) by default. To use specified shards only, set the `shards` query argument to a comma-separated list of slice indices.

``` request
curl "localhost:10101/index/user/query?columnAttrs=true&shards=0,1" \
     -X POST \
     -d 'Row(language=5)'
```
``` response
{
  "results":[{"attrs":{},"columns":[100]}],
  "columnAttrs":[{"id":100,"attrs":{"name":"Klingon"}}]
}
```

By default, all bits and attributes (*for `Row` queries only*) are returned. In order to suppress returning bits, set `excludeBits` query argument to `true`; to suppress returning attributes, set `excludeAttrs` query argument to `true`.

### Create field

`POST /index/<index-name>/field/<field-name>`

Creates a field in the given index with the given name.

The request payload is in JSON, and may contain the `options` field. The `options` field is a JSON object which must contain a `type` along with the corresponding configuration options. 

* `set`
    * `cacheType` (string): [ranked](../data-model/#ranked) or [LRU](../data-model/#lru) caching on this field. Default is `ranked`.
    * `cacheSize` (int): Number of rows to keep in the cache. Default is 50,000.
* `int`
    * `min` (int): Minimum integer value allowed for the field.
    * `max` (int): Maximum integer value allowed for the field.
* `bool`
    * (boolean fields take no arguments)
* `time`
    * `timeQuantum` (string): [Time Quantum](../data-model/#time-quantum) for this field.
* `mutex`
    * `cacheType` (string): [ranked](../data-model/#ranked) or [LRU](../data-model/#lru) caching on this field. Default is `ranked`.
    * `cacheSize` (int): Number of rows to keep in the cache. Default is 50,000.

The following example creates an `int` field called "quantity" capable of storing values from -1000 to 2000:

``` request
curl localhost:10101/index/user/field/quantity \
     -X POST \
     -d '{"options": {"type": "int", "min": -1000, "max":2000}}'
```
``` response
{"success":true}
```

Integer fields are stored as n-bit range-encoded values. Pilosa supports 63-bit, signed integers with values between `min` and `max`.

``` request
curl localhost:10101/index/user/field/language -X POST
```
``` response
{"success":true}
```

``` request
curl localhost:10101/index/repository/field/stats \
    -X POST \
    -d '{"fields": [{"name": "pullrequests", "type": "int", "min": 0, "max": 1000000}]}'
```
``` response
{"success":true}
```

### Remove field

`DELETE /index/<index-name>/field/<field-name>`

Removes the given field.

``` request
curl -XDELETE localhost:10101/index/user/field/language
```
``` response
{"success":true}
```

### Get version

`GET /version`

Returns the version of the Pilosa server.

``` request
curl -XGET localhost:10101/version
```
``` response
{"version":"v0.6.0"}
```

### Recalculate Caches

`POST /recalculate-caches`

Recalculates the caches on demand. The cache is recalculated every 10
seconds by default. This endpoint can be used to recalculate the cache
before the 10 second interval. This should probably only be used in
integration tests and not in a typical production workflow. Note that
in a multi-node cluster, the cache is only recalculated on the node
that receives the request.

``` request
curl -XPOST localhost:10101/recalculate-caches
```

Response: `204 No Content`

