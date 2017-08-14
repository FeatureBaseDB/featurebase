+++
title = "API Reference"
weight = 10
nav = []
+++


## API Reference

### List all index schemas

`GET /index`

Returns the schema of all indexes in JSON.

Request:
```
curl -XGET localhost:10101/index
```

Response:
```
{"indexes":[{"name":"user","frames":[{"name":"collab"}]}]}
```

### List index schema

`GET /index/<index-name>`

Returns the schema of the specified index in JSON.

Request:
```
curl -XGET localhost:10101/index/user
```

Response:
```
{"index":{"name":"user"}, "frames":[{"name":"collab"}]}]}
```

### Create index

`POST /index/<index-name>`

Creates an index with the given name.

The request payload is in JSON, and may contain the `options` field. The `options` field is a JSON object which may contain the following fields:

* `columnLabel` (string): column label of the index.

Request:
```
curl localhost:10101/index/user \
     -X POST \
     -d '{"options": {"columnLabel": "user_id"}}'
```

Response:
```
{}
```

### Remove index

`DELETE /index/index-name`

Removes the given index.

Request:
```
curl -XDELETE localhost:10101/index/user
```

Response:
```
{}
```

### Query index

`POST /index/<index-name>/query`

Sends a query to the Pilosa server with the given index. The request body is UTF-8 encoded text and response body is in JSON by default.

Request:
```
curl localhost:10101/index/user/query \
     -X POST \
     -d 'Bitmap(frame="language", id=5)'
```

Response:
```
{"results":[{"attrs":{},"bits":[100]}]}
```

In order to send protobuf binaries in the request and response, set `Content-Type` and `Accept` headers to: `application/x-protobuf`.

The response doesn't include column attributes by default. To return them, set the `columnAttrs` query argument to `true`.

The query is executed for all [slices](../data-model#slice) by default. To use specified slices only, set the `slices` query argument to a comma-separated list of slice indices.

Request:
```
curl "localhost:10101/index/user/query?columnAttrs=true&slices=0,1" \
     -X POST \
     -d 'Bitmap(frame="language", id=5)'
```
Response:
```
{
  "results":[{"attrs":{},"bits":[100]}],
  "columnAttrs":[{"id":100,"attrs":{"name":"Klingon"}}]
}
```

### Change index time quantum

`PATCH /index/<index-name>/time-quantum`

Changes the time quantum for the given index. This endpoint should be called at most once right after creating a database.

The payload is in JSON with the format: `{"timeQuantum": "${TIME_QUANTUM}"}`. Valid time quantum values are:

* (Empty string)
* Y: year
* M: month
* D: day
* H: hour
* YM: year and month
* MD: month and day
* DH: day and hour
* YMD: year, month and day
* MDH: month, day and hour
* YMDH: year, month, day and hour

Request:
```
curl localhost:10101/index/user/time-quantum \
     -X POST \
     -d '{"timeQuantum": "YM"}'
```

Response:
```
{}
```

### Create frame

`POST /index/<index-name>/frame/<frame-name>`

Creates a frame in the given index with the given name.

The request payload is in JSON, and may contain the `options` field. The `options` field is a JSON object which may contain the following fields:

* `rowLabel` (string): Row label of the frame.
* `timeQuantum` (string): [Time Quantum]({{< ref "data-model.md#time-quantum" >}}) for this frame.
* `inverseEnabled` (boolean): Enables [the inverted view]({{< ref "data-model.md#inverse" >}}) for this frame if `true`.
* `cacheType` (string): [ranked]({{< ref "data-model.md#ranked" >}}) or [LRU]({{< ref "data-model.md#lru" >}}) caching on this frame. Default is `lru`.
* `cacheSize` (int): Number of rows to keep in the cache. Default 50,000.

Request:
```
curl localhost:10101/index/user/frame/language \
     -X POST \
     -d '{"options": {"rowLabel": "language_id"}}'
```

Response:
```
{}
```

### Remove frame

`DELETE /index/<index-name>/frame/<frame-name>`

Removes the given frame.

Request:
```
curl -XDELETE localhost:10101/index/user/frame/language
```

Response:
```
{}
```

### Change frame time quantum

`PATCH /index/<index-name>/frame/<frame-name>/time-quantum`

Changes the time quantum for the given frame. This endpoint should be called at most once right after creating a frame.

The payload is in JSON with the format: `{"timeQuantum": "${TIME_QUANTUM}"}`. Valid time quantum values are:

* (Empty string)
* Y: year
* M: month
* D: day
* H: hour
* YM: year and month
* MD: month and day
* DH: day and hour
* YMD: year, month and day
* MDH: month, day and hour
* YMDH: year, month, day and hour

Request:
```
curl localhost:10101/index/user/frame/language/time-quantum \
     -X POST \
     -d '{"timeQuantum": "YM"}'
```

Response:
```
{}
```

### Create input definition

`POST /index/<index-name>/input-definition/<input-definition-name>`
 
Creates an input definition in the given index with the given name.
 
The request payload is JSON, and it must contain the fields `frames` and `fields`. `frames` is an array of frames used within this input definition.  Each frame must contain a `name` and may contain the following options:
 
* `rowLabel` (string): Row label of the frame.
* `timeQuantum` (string): [Time Quantum]({{< ref "data-model.md#time-quantum" >}}) for this frame.
* `inverseEnabled` (boolean): Enables [the inverted view]({{< ref "data-model.md#inverse" >}}) for this frame if `true`.
* `cacheType` (string): [ranked]({{< ref "data-model.md#ranked" >}}) or [LRU]({{< ref "data-model.md#lru" >}}) caching on this frame. Default is `lru`.
* `cacheSize` (int): Number of rows to keep in the cache. Default 50,000.
 
The `fields` array contains a series of JSON objects describing how to process each field received in the input data.  Each `field` object must contain a `name` which maps to the source JSON field name.  One field must be defined at the `primaryKey`.  The `primarykey` source field name must equal the column label for the `Index`, and its value must be an unsigned integer which maps directly to a columnID in Pilosa.

* `name`    (string): Maps the source data field to actions that process the field's corresponding value.
* `actions` (array): List of actions that will process the field's value.
 
The `action` describes how the field value will be processed.  Each `action` may contain:

* `frame` (string): The Frame that will contain this action's set bits.
* `rowid` (int): The action can use this as a pre-defined SetBit rowID.  The user is required to ensure this ID does not overlap with other rows in use per frame.
* `valueDestination` (string): The mapping rule used for this data.
    - `value-to-row`: The value should be an integer and will map directly to a RowID.
    - `single-row-boolean`: If the value is true set a bit using the `rowid`.
    - `mapping`: Map the value to a RowID in the `valueMap`.
* `valueMap` (object): string and integer pairs used to map field values to RowID's.
 
Request:
```
curl localhost:10101/index/user/input-definition/stargazer-input \
    -X POST \
    -d '{
            "frames":[
                {
                    "name": "language",
                    "options": {"rowLabel": "language_id"}
                }
            ],
            "fields":[
                {
                    "name": "repo_id",
                    "primaryKey":true
                },
                {
                    "name": "language_id",
                    "actions":[
                        {
                            "frame": "language",
                            "valueDestination": "mapping",
                            "valueMap": {
                                "Go": 5,
                                "Python": 17,
                                "C++": 10
                            }
                        }
                    ]
                }
            ]
        }'
```
 
Response:
```
{}
```

### Get input definition

`GET /index/<index-name>/input-definition/<input-definition-name>`

Returns the given input definition as JSON.

Request:
```
curl -XGET localhost:10101/index/user/input-definition/stargazer-input
```

Response:
```
{"frames":[{"name":"language","options":{"rowLabel":"language_id"}}],"fields":[{"name":"repo_id","primaryKey":true},{"name":"language_id","actions":[{"frame":"language","valueDestination":"mapping","valueMap":{"Go":5,"Python":17,"C++":10}}]}]}
```

### Remove input definition

`DELETE /index/<index-name>/input-definition/<input-definition-name>`

Removes the given input definition.

Request:
```
curl -XDELETE localhost:10101/index/user/input-definition/stargazer-input
```

Response:
```
{}
```

### Process input data

`POST /index/<index-name>/input/<input-definition-name>`

Processes the JSON payload using the given input definition.

The request payload is a JSON array of objects containing one field for the primary key that corresponds to the column label, and additional fields that will be handled by corresponding actions in the input definition.

Request:
```
curl localhost:10101/index/user/input/stargazer-input \
     -X POST \
     -d '[{"language_id": "Go", "repo_id": 92274475}]'
```

Response:
```
{}
```

### List hosts

`GET /hosts`

Returns the hosts in the cluster.

Request:
```
curl -XGET localhost:10101/hosts
```

Response:
```
[{"host":":10101","internalHost":""}]
```

### Get version

`GET /version`

Returns the version of the Pilosa server.

Request:
```
curl -XGET localhost:10101/version
```

Response:
```
{"version":"v0.5.0"}
```

