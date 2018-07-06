+++
title = "PDK"
weight = 11
nav = [
    "Examples and Executables",
    "Library",
]
+++

## PDK

The [Pilosa Dev Kit](https://github.com/pilosa/pdk) contains executables, examples, and Go libraries to help you use Pilosa effectively.

### Examples and Executables
Running `pdk -h` will give the most up to date list of all the tools and examples that PDK provides. We'll cover a few of the more important ones here.

#### Kafka
`pdk kafka` reads either JSON or Avro encoded records from Kafka (using the
Confluent Schema Registry in the case of Avro), and indexes them in Pilosa. Each
record from Kafka is assigned a Pilosa column, and each value in a record is
assigned a row or field. Pilosa field names are built from the "path" through
the record to arrive at that field. For example:

```json
{
  "name": "jill",
  "favorite_foods": ["corn chips", "chipotle dip"],
  "location": {
    "city": "Austin",
    "state": "Texas",
    "latitude": 3754,
    "longitude": 4526
  },
  "active": true,
  "age": 27
}
```

This JSON object would result in the following Pilosa schema:

| Field          | Type   | Min |        Max |   Size |
|----------------|--------|-----|------------|--------|
| name           | ranked |     |            | 100000 |
| favorite_foods | ranked |     |            | 100000 |
| default        | ranked |     |            | 100000 |
| age            | int    |   0 | 2147483647 |        |
| location       | ranked |     |            |   1000 |
| latitude       | int    |   0 | 2147483647 |        |
| longitude      | int    |   0 | 2147483647 |        |
| location-city  | ranked |     |            | 100000 |
| location-state | ranked |     |            | 100000 |

All set fields are created as ranked fields by default, with the cache size 
listed above. Integer fields are created with a minimum size of zero and a 
fixed maximum of 2147483647. Field names are a dash-separated concatenation of
all key values in the path - you can see this with fields like location-city.


Most of the options to `pdk kafka` are self-explanatory (kafka hosts, pilosa hosts,
kafka topics, kafka group, etc.), but there are a few options that give some
control over the way data is indexed, and ingestion performance.

* `--batch-size`: The batch size controls how many set bits or values are batched up to be imported *per field*. So for fields that have one value per record, you have to wait for `batch-size` records to come through before you'll see the data indexed in Pilosa. Fields like `favorite_foods` which can have multiple values could be indexed sooner.
* `--framer.collapse`: This is a list of strings which will be removed from the field names created by dash-concatentating all names in the JSON path to a value. E.G. if "location" were listed in `framer.collapse`, then there would be fields named "city" and "state" rather than "location-city" and "location-state".
* `--framer.ignore`: This allows you to skip indexing on any path containing these strings. If you have a field like email address or some other unique ID, you might not want to index it.
* `--subject-path`: If nothing is passed for this option, then each record will be assigned a unique sequential column ID. If `subject-path` is specified, then the value at this path in the record will be mapped to a column ID. If the same value appears in another record, the same column ID will be used.
* `--proxy`: The PDK ingests data, but also keeps a mapping for string values to row IDs, and from subjects to column ids. Because of this, querying Pilosa directly may not be useful, since it only returns integer row and column ids. The PDK will start a proxy server which intercepts requests to Pilosa using strings for row and column ids, and translates them to the integers that Pilosa understands. It will also translate responses so that (e.g.) a TopN query will return `{"results":[[{"Key":"chipotle dip","Count":1},{"Key":"corn chips","Count":1}]]}`. By default, the mapping is stored in an embedded leveldb.


### Library

For now, the [Godocs](https://godoc.org/github.com/pilosa/pdk) have the most up to date library documentation.

