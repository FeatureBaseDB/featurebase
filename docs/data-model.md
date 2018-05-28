+++
title = "Data Model"
weight = 5
nav = [
    "Overview",
    "Index",
    "Column",
    "Row",
    "Frame",
    "Time Quantum",
    "Attribute",
    "Slice",
    "View",
]
+++

## Data Model

### Overview

The central component of Pilosa's data model is a boolean matrix. Each cell in the matrix is a single bit - if the bit is set, it indicates that a relationship exists between that particular row and column.

Rows and columns can represent anything (they could even represent the same set of things - a [bigraph](https://en.wikipedia.org/wiki/Bigraph)). Pilosa can associate arbitrary key/value pairs (referred to as attributes) to rows and columns, but queries and storage are optimized around the core matrix.

Pilosa lays out data first in rows, so queries which get all the set bits in one or many rows, or compute a combining operation on multiple rows such as Intersect or Union are the fastest. Pilosa categorizes rows into different *frames* and quickly retrieves the top rows in a frame sorted by the number of bits set in each row.

Please note that Pilosa is most performant when row and column IDs are sequential starting from 0. You can deviate from this to some degree, but setting a bit with column ID 2<sup>63</sup> on a single-node cluster, for example, will not work well due to memory limitations.

![basic data model diagram](/img/docs/data-model.svg)
*Basic data model diagram*

### Index

The purpose of the Index is to represent a data namespace. You cannot perform cross-index queries.

### Column

Column ids are sequential increasing integers and are common to all Frames within an Index. A single column often corresponds to a record in a relational table, although other configurations are possible, and sometimes preferable.

### Row

Row ids are sequential increasing integers namespaced to each Frame within an Index.

### Frame

Frames are used to segment rows within an index, for example to define different functional groups. A frame might correspond to a single field in a relational table, where each row in a standard frame represents a single possible value of the field. Similarly, a frame with BSI values could represent all possible integer values of a field .

#### Relational Analogy

The Pilosa index is a flexible structure; it can represent any sort of high-cardinality binary matrix. We have explored a number of modeling patterns in Pilosa use cases; one accessible example is a direct analogy to the relational model, summarized here.

Entities:

 Relational  | Pilosa
-------------|----------------------------------------------
 Database    | N/A *(internal: Holder)*
 Table       | Index
 Row         | Column
 Column      | Frame
 Value       | Row
 Value (int) | Field.Value (see [BSI](#bsi-range-encoding))

Simple queries:

 Relational                                  | Pilosa
---------------------------------------------|------------------------------------
 `select ID from People where Name = 'Bob'`  | `Bitmap(frame=Name, row=[Bob])`
 `select ID from People where Age > 30`      | `Range(frame=Default, Age > 30)`
 `select ID from People where Member = true` | `Bitmap(frame=Member, row=[true])`

In the relational model, joins are often necessary. Because Pilosa supports extremely high cardinality in both rows and columns, many types of joins are accomplished with basic Pilosa queries across multiple frames. For example, this SQL join:

```sql
select AVG(p.Age) from People p
inner join PersonCar pc on pc.PersonID=p.ID
inner join Cars c on pc.CarID=c.ID
where c.Make = 'Ford'
```

can be accomplished with a Pilosa query like this (note that [Sum](../query-language/#sum) returns a json object containing both the sum and count, from which the average is easily computed):

```pql
Sum(Bitmap(frame="Car-Make", row=[Ford]), frame=Default, field=Age)
```

This is one major component of Pilosa's ability to combine relationships from multiple data stores.

#### Ranked

Ranked Frames maintain a sorted cache of column counts by Row ID (yielding the top rows by columns with a bit set in each). This cache facilitates the TopN query. The cache size defaults to 50,000 and can be set at Frame creation.

![ranked frame diagram](/img/docs/frame-ranked.svg)
*Ranked frame diagram*

#### LRU

The LRU cache maintains the most recently accessed Rows.

![lru frame diagram](/img/docs/frame-lru.svg)
*LRU frame diagram*

### Time Quantum

Setting a time quantum on a frame creates extra views which allow Range queries down to the time interval specified. For example - if the time quantum is set to `YMD`, Range queries down to the granularity of a day are supported.

### Attribute

Attributes are arbitrary key/value pairs that can be associated with either rows or columns. This metadata is stored in a separate BoltDB data structure.

Column-level attributes are common across an index. That is, each column attribute applies to all bits in the corresponding column, across all frames in an index. Row attributes apply to all bits in the corresponding row.

### Slice

Indexes are sharded into groups of columns called Slices. Each Slice contains a fixed number of columns, which is the SliceWidth. SliceWidth is a constant that can only be modified at compile time, and before ingesting data. The default value is 2<sup>20</sup>.

Query operations run in parallel, and they are evenly distributed across a cluster via a consistent hash algorithm.

### View

Views represent the various data layouts within a Frame. The primary View is called Standard, and it contains the typical Row and Column data. Time-based Views are automatically generated for each time quantum. Views are internally managed by Pilosa, and never exposed directly via the API.

#### Standard

The standard View contains the same Row/Column format as the input data. 

#### Time Quantums

If a Frame has a time quantum, then Views are generated for each of the defined time segments. For example, for a frame with a time quantum of `YMD`, the following `SetBit()` queries will result in the data described in the diagram below:

```
SetBit(frame="A", row=8, col=3, timestamp="2017-05-18T00:00")
SetBit(frame="A", row=8, col=3, timestamp="2017-05-19T00:00")
```

![time quantum frame diagram](/img/docs/frame-time-quantum.svg)
*Time quantum frame diagram*

#### BSI Range-Encoding

Bit-Sliced Indexing (BSI) is the storage method Pilosa uses to represent multi-bit integers in a bitmap index. Integers are stored as n-bit, range-encoded bit-sliced indexes of base-2, along with an additional bitmap indicating "not null". This means that a 16-bit integer will require 17 bitmaps: one for each 0-bit of the 16 bit-slice components (the 1-bit does not need to be stored because with range-encoding the highest bit position is always 1) and one for the non-null bitmap. Pilosa can evaluate `Range`, `Min`, `Max`, and `Sum` queries on these BSI integers. The result of a `Sum` query includes a count, which can be used to compute an average with no other overhead.

Internally Pilosa stores each BSI `field` as a `view` within a `frame`. The rows of the `view` contain the base-2 representations of the integer values. Pilosa manages the base-2 offset and translation that efficiently packs the integer value within the minimum set of rows.

For example, the following `SetFieldValue()` queries will result in the data described in the diagram below:

```
SetFieldValue(col=1, frame="A", field0=1)
SetFieldValue(col=2, frame="A", field0=2)
SetFieldValue(col=3, frame="A", field0=3)
SetFieldValue(col=4, frame="A", field0=7)
SetFieldValue(col=2, frame="A", field1=1)
SetFieldValue(col=3, frame="A", field1=6)
```

![BSI frame diagram](/img/docs/frame-bsi.svg)
*BSI frame diagram*

Check out this [blog post](/blog/range-encoded-bitmaps/) for some more details about BSI in Pilosa.
