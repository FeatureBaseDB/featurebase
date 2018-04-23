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
{"indexes":[{"name":"user","frames":[{"name":"collab"}]}]}
```

### List index schema

`GET /index/<index-name>`

Returns the schema of the specified index in JSON.

``` request
curl -XGET localhost:10101/index/user
```
``` response
{"index":{"name":"user"}, "frames":[{"name":"collab"}]}]}
```

### Create index

`POST /index/<index-name>`

Creates an index with the given name.

``` request
curl -XPOST localhost:10101/index/user
```
``` response
{}
```

### Remove index

`DELETE /index/index-name`

Removes the given index.

``` request
curl -XDELETE localhost:10101/index/user
```
``` response
{}
```

### Query index

`POST /index/<index-name>/query`

Sends a query to the Pilosa server with the given index. The request body is UTF-8 encoded text and response body is in JSON by default.

``` request
curl localhost:10101/index/user/query \
     -X POST \
     -d 'Bitmap(frame="language", rowID=5)'
```
``` response
{"results":[{"attrs":{},"bits":[100]}]}
```

In order to send protobuf binaries in the request and response, set `Content-Type` and `Accept` headers to: `application/x-protobuf`.

The response doesn't include column attributes by default. To return them, set the `columnAttrs` query argument to `true`.

The query is executed for all [slices](../data-model/#slice) by default. To use specified slices only, set the `slices` query argument to a comma-separated list of slice indices.

``` request
curl "localhost:10101/index/user/query?columnAttrs=true&slices=0,1" \
     -X POST \
     -d 'Bitmap(frame="language", rowID=5)'
```
``` response
{
  "results":[{"attrs":{},"bits":[100]}],
  "columnAttrs":[{"id":100,"attrs":{"name":"Klingon"}}]
}
```

By default, all bits and attributes (*for `Bitmap` queries only*) are returned. In order to suppress returning bits, set `excludeBits` query argument to `true`; to suppress returning attributes, set `excludeAttrs` query argument to `true`.

### Create frame

`POST /index/<index-name>/frame/<frame-name>`

Creates a frame in the given index with the given name.

The request payload is in JSON, and may contain the `options` field. The `options` field is a JSON object which may contain the following fields:

* `timeQuantum` (string): [Time Quantum](../data-model/#time-quantum) for this frame.
* `inverseEnabled` (boolean): Enables [the inverted view](../data-model/#inverse) for this frame if `true`.
* `cacheType` (string): [ranked](../data-model/#ranked) or [LRU](../data-model/#lru) caching on this frame. Default is `lru`.
* `cacheSize` (int): Number of rows to keep in the cache. Default 50,000.
* `rangeEnabled` (boolean): DEPRECATED - has no effect, will be removed. All frames support BSI fields.
* `fields` (array): List of range-encoded [fields](../data-model/#bsi-range-encoding).

Each individual `field` contains the following:

* `name` (string): Field name.
* `type` (string): Field type, currently only "int" is supported.
* `min` (int): Minimum value allowed for this field.
* `max` (int): Maximum value allowed for this field.

Integer fields are stored as n-bit range-encoded values. Pilosa supports 63-bit, signed integers with values between `min` and `max`.

``` request
curl localhost:10101/index/user/frame/language \
     -X POST \
     -d '{"options": {"inverseEnabled": true}}'
```
``` response
{}
```

``` request
curl localhost:10101/index/repository/frame/stats \
    -X POST \
    -d '{"fields": [{"name": "pullrequests", "type": "int", "min": 0, "max": 1000000}]}'
```
``` response
{}
```

### Remove frame

`DELETE /index/<index-name>/frame/<frame-name>`

Removes the given frame.

``` request
curl -XDELETE localhost:10101/index/user/frame/language
```
``` response
{}
```

### Create Field

`POST /index/<index-name>/frame/<frame-name>/field/<field-name>`

Creates a new field to store integer values in the given frame.

The request payload is JSON, and it must contain the fields `type`, `min`, `max`.
* `type` (string): Field type, currently only "int" is supported.
* `min` (int): Minimum value allowed for this field.
* `max` (int): Maximum value allowed for this field.

``` request
curl localhost:10101/index/repository/frame/stats/field/pullrequests \
    -X POST \
    -d '{"type": "int", "min": 0, "max": 1000000}'
```
``` response
{}
```

### List hosts

`GET /hosts`

Returns the hosts in the cluster.

``` request
curl -XGET localhost:10101/hosts
```
``` response
[{"host":":10101"}]
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

Response: `204 No Content`

