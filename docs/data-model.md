+++
title = "Data Model"
weight = 5
nav = [
    "Overview",
    "Index",
    "Field",
    "Time Quantum",
]
+++

## Data Model

### Overview

The central component of Pilosa's data model is the index. An index can be thought of as a boolean matrix in which each cell in the matrix is a single bit; if the bit is set, it indicates that a relationship exists between that particular row and column.

Rows and columns can represent anything. They could represent two differnt sets of things as in a [bigraph](https://en.wikipedia.org/wiki/Bigraph) or the same set things. Pilosa can associate arbitrary key/value pairs (referred to as attributes) to rows and columns, but queries and storage are optimized around the core matrix.


Pilosa categorizes rows, or bitmaps, into *fields* and supports queries which operate on bitmaps across one or more fields. Boolean operations such as the union, intersection, or difference between bitmaps are extremely performant. Pilosa can also quickly retrieve the top bitmaps in a field sorted by the number of columns, or records, set in each bitmap.

<div class="note">
    <font size="5"><b>Shard</b></font>
    <p>Indexes are segmented into groups of records called shards, which are 2<sup>20</sup> bits wide by default. Each shard contains data for all fields and all bitmaps within each field, but just the portion of each bitmap which is relevant to the shard. The width is controlled by a compile-time constant code called `ShardWidth`.</p>
    <p>Query operations run concurrently across all shards, and they are distributed across a cluster via consistent hashing.</p>
</div>

Please note that Pilosa is most performant when row and column IDs are sequential starting from 0. You can deviate from this to some degree, but setting a bit with column ID 2<sup>63</sup> on a single-node cluster, for example, will not work well due to memory limitations.

![basic data model diagram](/img/docs/data-model.png)
*Basic data model diagram*

##### Relational Analogy

The Pilosa index is a flexible structure; it can represent any sort of high-cardinality binary matrix. We have explored a number of modeling patterns in Pilosa use cases; one accessible example is a direct analogy to the relational model, summarized here.

Entities:

 Relational  | Pilosa
-------------|----------------------------------------------
 Database    | N/A *(internal: Holder)*
 Table       | Index
 Row         | Column (or Record)
 Column      | Field
 Value       | Row (or Bitmap)
 Value (int) | Field.Value (see [BSI](#bsi-range-encoding))

Simple queries:

 Relational                                    | Pilosa
-----------------------------------------------|------------------------------------
 `select ID from People where Name = 'Bob'`    | `Row(Name="Bob")`
 `select ID from People where Age > 30`        | `Row(Age > 30)`
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

##### Row (or Bitmap)

Rows, or bitmaps are the horizantal sets of bits in the matrix. They are namespaced to each field within an index and are represented by a Bitmap, which is why they are referred to as bitmaps. Row IDs are sequential, increasing integers that usually represent features of the column subject.

##### Column (or Record)

Columns, or records, are the vertical sets of bits in the matrix. They are grouped into sets of 2<sup>20</sup> called Shards. Column IDs are sequential, increasing integers that are common to all fields within an index and represent different types of some item of interest. A single record often corresponds to a record in a relational table, although other configurations are possible, and sometimes preferable.

##### Attribute

Attributes are arbitrary key/value pairs that can be associated with either bitmaps or records. This metadata is stored in a separate BoltDB data structure.

Record-level attributes are common across an index. That is, each record attribute applies to all bits in the corresponding record, across all fields in an index. Bitmap attributes apply to all bits in the corresponding bitmap.

### Index

An index is a top level container in Pilosa, analogous to a database in a Relational Database Management System (RDBMS). It's purpose is to represent a data namespace for the Pilosa Bitmap. Pilosa stores the index in a format called Roaring, which both compresses the bitmaps and allows for efficient computation on them. By using Roaring, Pilosa is able to use Roaring's unary *count* operation and the binary operations *intersect*, *union*, *difference*, and *XOR* within an index when making queries. Cross-index queries are currently unavailable, but check back soon as Pilosa is constantly updating.

### Field

Fields are used to segment bitmaps into different categories to organize the data for quering. They are created based on the user's need, so an index may have one field or it may have hundreds. Row IDs are namespaced by field such that the same row ID in a different field refers to a different row. For example, row 1 in field 1 is not the same as row 1 in field 2. Fields also maintain a cache with a default size of 50,000. Their cache type can be set to either the ranked or least recently used (LRU) setting upon creation of the field. A ranked field maintains a list of row IDs sorted in descending order by the number of bits set in the bitmap.

![ranked field diagram](/img/docs/field-ranked.png)
*Ranked field diagram*

A LRU field maintains a similar list of row IDs sorted by the most recently accessed.

![lru field diagram](/img/docs/field-lru.png)
*LRU field diagram*

Fields can also be one of five types.

#### Field Type

Upon creation, fields are configured to be of a certain type. Pilosa supports the following field types: `set`, `int`, `bool`, `time`, and `mutex`.

#### Set

Set is the default field type in Pilosa. Set fields represent a standard, binary matrix of bitmaps and records where each row key represents a possible field value. The following example creates a `set` field called "info" with a ranked cache containing up to 100,000 records.

``` request
curl localhost:10101/index/repository/field/info \
     -X POST \
     -d '{"options": {"type": "set", "cacheType": "ranked", "cacheSize":100000}}'
```
``` response
{"success":true}
```

#### Int
Fields of type `int` are used to store integer values. Integer fields share the same records as the other fields in the index. The following example creates an `int` field called "quantity" capable of storing values from -1000 to 2000:

``` request
curl localhost:10101/index/repository/field/quantity \
     -X POST \
     -d '{"options": {"type": "int", "min": -1000, "max":2000}}'
```
``` response
{"success":true}
```

##### BSI Range-Encoding

Bit-Sliced Indexing (BSI) is the storage method Pilosa uses to represent multi-bit integers in a bitmap index. Integers are stored as n-bit, range-encoded bit-sliced indexes of base-2, along with an additional bitmap indicating "not null". This means that a 16-bit integer will require 17 bitmaps: one for each 0-bit of the 16 bit-slice components (the 1-bit does not need to be stored because with range-encoding the highest bit position is always 1) and one for the non-null bitmap. Pilosa can evaluate `Row`, `Min`, `Max`, and `Sum` queries on these BSI integers. The result of a `Sum` query includes a count, which can be used to compute an average with no other overhead.

Internally Pilosa stores each BSI `field` as a `view`. The bitmaps of the `view` contain the base-2 representations of the integer values. Pilosa manages the base-2 offset and translation that efficiently packs the integer value within the minimum set of bitmaps.

For example, the following `Set()` queries executed against BSI fields will result in the data described in the diagram below:

```
Set(1, A=1)
Set(2, A=2)
Set(3, A=3)
Set(4, A=7)
Set(2, B=1)
Set(3, B=6)
```

![BSI field diagram](/img/docs/field-bsi.png)
*BSI field diagram*

Check out this [blog post](/blog/range-encoded-bitmaps/) for some more details about BSI in Pilosa.

<div class="note">
    <font size="5"><b>BSI Deprecated Format</b></font>
    <p>The original implementation of BSI required a fixed bit depth when creating fields because the existence bit was written to the bit above the highest bit. The second version of BSI moves the existence bit to the beginning, adds a negative bit as the second bit, and shifts all remaining bits up by two.</p>
    <p>Pilosa automatically converts all old data to the new format on startup, however, this can cause issues when upgrading Pilosa and then reverting back to an old version. This documentation section exists as a record for anyone who experiences unusual behavior in BSI between versions.</p>
</div>


#### Time

Time fields are similar to `set` fields, but in addition to bitmap and record information, they also store a per-bit time value down to a defined granularity. The following example creates a `time` field called "event" which stores timestamp information down to a day granularity.

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

![time quantum field diagram](/img/docs/field-time-quantum.png)
*Time quantum field diagram*

Setting a time quantum on a field creates extra views which allow ranged Row queries down to the time interval specified. For example, if the time quantum is set to `YMD`, ranged Row queries down to the granularity of a day are supported.


#### Mutex

Mutex fields are similar to `set` fields, with the distinction of requiring the bitmap value for each record to be mutually exclusive. In other words, each record can only have a single value for the field. If the field value for a record is updated on a `mutex` field, then the previous field value for that record will be cleared. This field type is like a field in an RDBMS table where every record contains a single value for a particular field.

#### Boolean

A boolean field is similar to a `mutex` field tracking only two values: `true` and `false`. Boolean fields do not maintain a sorted cache, nor do they support key values.