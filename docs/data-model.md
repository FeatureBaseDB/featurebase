+++
title = "Data Model"
weight = 5
nav = [
    "Overview",
    "Index",
    "Column",
    "Row",
    "Field",
    "Time Quantum",
    "Attribute",
    "Shard",
    "View",
]
+++

## Data Model

### Overview

The central component of Pilosa's data model is a boolean matrix. Each cell in the matrix is a single bit; if the bit is set, it indicates that a relationship exists between that particular row and column.

Rows and columns can represent anything (they could even represent the same set of things as in a [bigraph](https://en.wikipedia.org/wiki/Bigraph)). Pilosa can associate arbitrary key/value pairs (referred to as attributes) to rows and columns, but queries and storage are optimized around the core matrix.

Pilosa lays out data first in rows, so queries which get all the set bits in one or many rows, or compute a combining operation—such as Intersect or Union—on multiple rows, are the fastest. Pilosa categorizes rows into different *fields* and quickly retrieves the top rows in a field sorted by the number of columns set in each row.

Please note that Pilosa is most performant when row and column IDs are sequential starting from 0. You can deviate from this to some degree, but setting a bit with column ID 2<sup>63</sup> on a single-node cluster, for example, will not work well due to memory limitations.

![basic data model diagram](/img/docs/data-model.svg)
*Basic data model diagram*

### Index

The purpose of the Index is to represent a data namespace. You cannot perform cross-index queries.

### Column

Column ids are sequential, increasing integers and they are common to all Fields within an Index. A single column often corresponds to a record in a relational table, although other configurations are possible, and sometimes preferable.

### Row

Row ids are sequential, increasing integers namespaced to each Field within an Index.

### Field

Fields are used to segment rows within an index, for example to define different functional groups. A Pilosa field might correspond to a single field in a relational table, where each row in a standard Pilosa field represents a single possible value of the relational field. Similarly, an integer field could represent all possible integer values of a relational field.

#### Relational Analogy

The Pilosa index is a flexible structure; it can represent any sort of high-cardinality binary matrix. We have explored a number of modeling patterns in Pilosa use cases; one accessible example is a direct analogy to the relational model, summarized here.

Entities:

 Relational  | Pilosa
-------------|----------------------------------------------
 Database    | N/A *(internal: Holder)*
 Table       | Index
 Row         | Column
 Column      | Field
 Value       | Row
 Value (int) | Field.Value (see [BSI](#bsi-range-encoding))

Simple queries:

 Relational                                    | Pilosa
-----------------------------------------------|------------------------------------
 `select ID from People where Name = 'Bob'`    | `Row(Name="Bob")`
 `select ID from People where Age > 30`        | `Range(Age > 30)`
 `select ID from People where Member = true`   | `Row(Member=0)`

Note that `Row(Member=0)` selects all entities with a bit set in row 0 of the Member field. We could just as well use row 1 to store this, in which case we would use `Row(Member=1)`, which looks a bit more intuitive. In the relational model, joins are often necessary. Because Pilosa supports extremely high cardinality in both rows and columns, many types of joins are accomplished with basic Pilosa queries across multiple fields. For example, this SQL join:

```sql
select AVG(p.Age) from People p
inner join PersonCar pc on pc.PersonID=p.ID
inner join Cars c on pc.CarID=c.ID
where c.Make = 'Ford'
```

can be accomplished with a Pilosa query like this (note that [Sum](../query-language/#sum) returns a json object containing both the sum and count, from which the average is easily computed):

```pql
Sum(Row(Car-Make="Ford"), field=Age)
```

This is one major component of Pilosa's ability to combine relationships from multiple data stores.

#### Ranked

Ranked Fields maintain a sorted cache of column counts by Row ID (yielding the top rows by columns with a bit set in each). This cache facilitates the TopN query. The cache size defaults to 50,000 and can be set at Field creation.

![ranked field diagram](/img/docs/field-ranked.svg)
*Ranked field diagram*

#### LRU

The LRU cache maintains the most recently accessed Rows.

![lru field diagram](/img/docs/field-lru.svg)
*LRU field diagram*

### Time Quantum

Setting a time quantum on a field creates extra views which allow Range queries down to the time interval specified. For example, if the time quantum is set to `YMD`, Range queries down to the granularity of a day are supported.

### Attribute

Attributes are arbitrary key/value pairs that can be associated with either rows or columns. This metadata is stored in a separate BoltDB data structure.

Column-level attributes are common across an index. That is, each column attribute applies to all bits in the corresponding column, across all fields in an index. Row attributes apply to all bits in the corresponding row.

### Shard

Indexes are segmented into groups of columns called shards (previously known as slices). Each shard contains a fixed number of columns, which is the ShardWidth. ShardWidth is a constant that can only be modified at compile time, and before ingesting data. The default value is 2<sup>20</sup>.

Query operations run in parallel, and they are evenly distributed across a cluster via a consistent hash algorithm.

### Field Type

Upon creation, fields are configured to be of a certain type. Pilosa supports the following field types: `set`, `int`, `bool`, `time`, and `mutex`.

#### Set

Set is the default field type in Pilosa. Set fields represent a standard, binary matrix of rows and columns where each row key represents a possible field value. The following example creates a `set` field called "info" with a ranked cache containing up to 100,000 records.

``` request
curl localhost:10101/index/repository/field/info \
     -X POST \
     -d '{"options": {"type": "set", "cacheType": "ranked", "cacheSize":100000}}'
```
``` response
{"success":true}
```

#### Int
Fields of type `int` are used to store integer values. Integer fields share the same columns as the other fields in the index, but values for the field must be integers that fall between the `min` and `max` values specified when creating the field. The following example creates an `int` field called "quantity" capable of storing values from -1000 to 2000:

``` request
curl localhost:10101/index/repository/field/quantity \
     -X POST \
     -d '{"options": {"type": "int", "min": -1000, "max":2000}}'
```
``` response
{"success":true}
```

##### BSI Range-Encoding

Bit-Sliced Indexing (BSI) is the storage method Pilosa uses to represent multi-bit integers in a bitmap index. Integers are stored as n-bit, range-encoded bit-sliced indexes of base-2, along with an additional row indicating "not null". This means that a 16-bit integer will require 17 rows: one for each 0-bit of the 16 bit-slice components (the 1-bit does not need to be stored because with range-encoding the highest bit position is always 1) and one for the non-null row. Pilosa can evaluate `Range`, `Min`, `Max`, and `Sum` queries on these BSI integers. The result of a `Sum` query includes a count, which can be used to compute an average with no other overhead.

Internally Pilosa stores each BSI `field` as a `view`. The rows of the `view` contain the base-2 representations of the integer values. Pilosa manages the base-2 offset and translation that efficiently packs the integer value within the minimum set of rows.

For example, the following `Set()` queries executed against BSI fields will result in the data described in the diagram below:

```
Set(1, A=1)
Set(2, A=2)
Set(3, A=3)
Set(4, A=7)
Set(2, B=1)
Set(3, B=6)
```

![BSI field diagram](/img/docs/field-bsi.svg)
*BSI field diagram*

Check out this [blog post](/blog/range-encoded-bitmaps/) for some more details about BSI in Pilosa.

#### Time

Time fields are similar to `set` fields, but in addition to row and column information, they also store a per-bit time value down to a defined granularity. The following example creates a `time` field called "event" which stores timestamp information down to a day granularity.

``` request
curl localhost:10101/index/repository/field/event \
     -X POST \
     -d '{"options": {"type": "time", "timeQuantum": "YMD"}}'
```
``` response
{"success":true}
```

With `time` fields, data views are generated for each of the defined time segments. For example, for a field with a time quantum of `YMD`, the following `Set()` queries will result in the data described in the diagram below:

```
Set(3, A=8, 2017-05-18T00:00)
Set(3, A=8, 2017-05-19T00:00)
```

![time quantum field diagram](/img/docs/field-time-quantum.svg)
*Time quantum fueld diagram*

#### Mutex

Mutex fields are similar to `set` fields, with the distinction of requiring the row value for each column to be mutually exclusive. In other words, each column can only have a single value for the field. If the field value for a column is updated on a `mutex` field, then the previous field value for that column will be cleared. This field type is like a field in an RDBMS table where every record contains a single value for a particular field.

#### Boolean

A boolean field is similar to a `mutex` field tracking only two values: `true` and `false`. Boolean fields do not maintain a sorted cache, nor do they support key values.
