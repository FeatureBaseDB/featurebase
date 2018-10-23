+++
title = "API Reference"
weight = 10
nav = []
+++


## API Reference

### List all index schemas

`GET /index`

Is equivalent to `GET /schema` and returns the same response.

### List index schema

`GET /index/<index-name>`

Returns the schema of the specified index in JSON.

``` request
curl -XGET localhost:10101/index/user
```
``` response
{
    "fields": [
        {
            "name": "event",
            "options": {
                "keys": false,
                "timeQuantum": "YMD",
                "type": "time"
            }
        }
    ],
    "name": "user",
    "options": {
        "keys": false,
        "trackExistence": true
    }
}
```

### Create index

`POST /index/<index-name>`

Creates an index with the given name.

The request payload is in JSON, and may contain the `options` field. The `options` field is a JSON object with the following options:

* `keys` (bool): Enables using column keys instead of column IDs.
* `trackExistence` (bool): Enables or disables existence tracking on the index. Required for [Not](../query-language/#not) queries. It is `true` by default.

``` request
curl -XPOST localhost:10101/index/user -d '{"options":{"keys":true}}'
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
{
    "results": [
        {
            "attrs": {},
            "columns": [
                100
            ]
        }
    ]
}
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
    "columnAttrs": [
        {
            "attrs": {
                "name": "Klingon"
            },
            "id": 100
        }
    ],
    "results": [
        {
            "attrs": {},
            "columns": [
                100
            ]
        }
    ]
}
```

By default, all bits and attributes (*for `Row` queries only*) are returned. In order to suppress returning bits, set `excludeBits` query argument to `true`; to suppress returning attributes, set `excludeAttrs` query argument to `true`.

### Import Data

`POST /index/<index-name>/field/<field-name>/import`

Supports high-rate data ingest to a particular shard of a particular field. The
official client libraries use this endpoint for their import functionality - it
is not usually necessary to use this endpoint directly. See the documentation for
imports for
<a href="https://github.com/pilosa/go-pilosa/blob/master/docs/imports-exports.md">Go</a>,
<a href="https://github.com/pilosa/java-pilosa/blob/master/docs/imports.md">Java</a>,
and <a href="https://github.com/pilosa/python-pilosa/tree/master/docs/imports.md">Python</a>.

The request payload is protobuf encoded with the following schema. The RowKeys
and/or ColumnKeys fields are used if the pilosa field or index are configured
for keys respectively. Otherwise, the RowIDs and ColumnIDs fields are used. They
must have the same number of items, and each index into those two lists
represents a particular bit to be set. Timestamps are optional, but if they
exist must also contain the same number of items as rows and columns. The
column IDs must all be in the shard specified in the request.

```
message ImportRequest {
	string Index = 1;
	string Field = 2;
	uint64 Shard = 3;
	repeated uint64 RowIDs = 4;
	repeated uint64 ColumnIDs = 5;
	repeated string RowKeys = 7;
	repeated string ColumnKeys = 8;
	repeated int64 Timestamps = 6;
}
```


### Create field

`POST /index/<index-name>/field/<field-name>`

Creates a field in the given index with the given name.

The request payload is in JSON, and may contain the `options` field. The `options` field is a JSON object which must contain a `type`:

* `type` (string): Sets the field type and type options.
* `keys` (bool): Enables using column keys instead of column IDs (optional).

Valid `type`s and correspondonding options are listed below:

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

### List all index schemas

`GET /schema`

Returns the schema of all indexes in JSON.

``` request
curl -XGET localhost:10101/index
```
``` response
{
    "indexes": [
        {
            "fields": [
                {
                    "name": "event",
                    "options": {
                        "keys": false,
                        "timeQuantum": "YMD",
                        "type": "time"
                    }
                },
                {
                    "name": "language",
                    "options": {
                        "cacheSize": 50000,
                        "cacheType": "ranked",
                        "keys": false,
                        "type": "set"
                    }
                }
            ],
            "name": "user",
            "options": {
                "keys": false,
                "trackExistence": true
            }
        }
    ]
}
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

### Get status

`GET /status`

Returns the status of the cluster.

```request
curl -XGET localhost:10101/status
```
```response
{
    "localID": "d3369125-29d8-4305-a351-b4474d14a542",
    "nodes": [
        {
            "id": "d3369125-29d8-4305-a351-b4474d14a542",
            "isCoordinator": true,
            "uri": {
                "host": "localhost",
                "port": 10101,
                "scheme": "http"
            }
        }
    ],
    "state": "NORMAL"
}
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
