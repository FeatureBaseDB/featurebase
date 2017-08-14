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

Rows and columns can represent anything (they could even represent the same set of things). Pilosa can associate arbitrary key/value pairs (referred to as attributes) to rows and columns, but queries and storage are optimized around the core matrix.

Pilosa lays out data first in rows, so queries which get all the set bits in one or many rows, or compute a combining operation on multiple rows such as Intersect or Union are the fastest. Pilosa also has the ability to categorize rows into different "frames" and quickly retrieve the top rows in a frame sorted by the number of bits set in each row.

![data model diagram](/img/docs/data-model.svg)

### Index

The purpose of the Index is to represent a data namespace. You cannot perform cross-index queries.  Column-level attributes are global to the Index.

### Column

Column ids are sequential increasing integers and are common to all Frames within an Index.

### Row

Row ids are sequential increasing integers namespaced to each Frame within an Index.

### Frame

Frames are used to segment and define different functional characteristics within your entire index.  You can think of a Frame as a table-like data partition within your Index.

Row attributes are namespaced at the Frame level.

#### Ranked

Ranked Frames maintain a sorted cache of column counts by Row ID (yielding the top rows by columns with a bit set in each). This cache facilitates the TopN query.  The cache size defaults to 50,000 and can be set at Frame creation.

![ranked frame diagram](/img/docs/frame-ranked.svg)

#### LRU

The LRU cache maintains the most recently accessed Rows.

![lru frame diagram](/img/docs/frame-lru.svg)

### Time Quantum

Setting a time quantum on a frame creates extra indices which allow Range queries down to the interval specified. For example - if the time quantum is set to `YMD`, Range queries down to the granularity of a day are supported. 

### Attribute

Attributes are arbitrary key/value pairs that can be associated to both rows or columns.  This metadata is stored in a separate BoltDB data structure. 

### Slice

Indexes are sharded into groups of columns called Slices - each Slice contains a fixed number of columns which is the SliceWidth.

Columns are sharded on a preset width, and each shard is referred to as a Slice.  Slices are operated on in parallel, and they are evenly distributed across a cluster via a consistent hash algorithm.

### View

Views represent the various data layouts within a Frame. The primary View is called Standard, and it contains the typical Row and Column data. The Inverse View contains the same data with the axes inverted.Time-based Views are automatically generated for each time quantum. Views are internally managed by Pilosa, and never exposed directly via the API. This simplifies the functional interface from the physical data representation.

#### Standard

The standard View contains the same Row/Column format as the input data. 

#### Inverse

The Inverse View contains the same data with the Row and Column swapped.

For example, the following `SetBit()` queries will result in the data described in the illustration below:
```
SetBit(frame="A", rowID=8, columnID=3)
SetBit(frame="A", rowID=11, columnID=3)
SetBit(frame="A", rowID=19, columnID=5)
```

![inverse frame diagram](/img/docs/frame-inverse.svg)

#### Time Quantums

If a Frame has a time quantum, then Views are generated for each of the defined time segments. For example, for a frame with a time quantum of `YMD`, the following `SetBit()` queries will result in the data described in the illustration below:

```
SetBit(frame="A", rowID=8, columnID=3, timestamp="2017-05-18T00:00")
SetBit(frame="A", rowID=8, columnID=3, timestamp="2017-05-19T00:00")
```

![time quantum frame diagram](/img/docs/frame-time-quantum.svg)
