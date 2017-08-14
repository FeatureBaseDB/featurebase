+++
title = "Glossary"
weight = 14
nav = []
+++

## Glossary

<strong id="index">Index:</strong> Indexes are the top level container in Pilosa - similar to a database in an RDBMS. Queries cannot operate across multiple indexes.

<strong id="column">Column:</strong> Columns are the fundamental horizontal data axis within Pilosa.  Columns are global to all Frames within a Index.

<strong id="row">Row:</strong> Rows are the fundamental vertical data axis within Pilosa.  They are namespaced to each Frame within a Index.

<strong id="bit">Bit:</strong> A bit is the intersection of a Row and Column.

<strong id="bitmap">Bitmap:</strong> The on-disk and in-memory representation of a Row.

<strong id="roaring-bitmap">Roaring Bitmap:</strong> [Roaring Bitmap](http://roaringbitmap.org) is the compressed bitmap format which Pilosa uses.

<strong id="attribute">Attribute:</strong> Attributes can be associated to both rows and columns.  This metadata is kept separately from the core binary matrix in a BoltDB store.

<strong id="pql">PQL:</strong> Pilosa Query Language

<strong id="index">Index:</strong> The Index represents a data namespace.

<strong id="frame">Frame:</strong> Frames are used to segment rows into different categories - row ids are namespaced by frame such that the same row id in a different frame refers to a different row. For Ranked frames, rows are kept in sorted order within the frame. 

<strong id="view">View:</strong> Views separate the different data layouts within a Frame. The two primary views are Standard and Inverse which represent the typical row/column data and its inverse respectively. Time based Frame Views are automatically generated for each time quantum. Views are internally managed by Pilosa, and never exposed directly via the API. This simplifies the functional interface by separating it from the physical data representation.

<strong id="fragment">Fragment:</strong> A Fragment is the intersection of a frame and slice in an index.

<strong id="slice">Slice:</strong> Columns are sharded on a preset width. Each shard is referred to as a Slice in Pilosa. Slices are operated on in parallel and are evenly distributed across the cluster via a consistent hash.

<strong id="slicewidth">SliceWidth:</strong> This is the default number of columns in a slice.

<strong id="maxslice">MaxSlice:</strong> The total number of slices allocated to handle current set of columns.  This value is important for all nodes to efficiently distribute queries.

<strong id="anti-entropy">Anti-entropy:</strong> A periodic process that compares each slice and its replicas across the cluster to repair inconsistencies.

<strong id="node">Node:</strong> An individual running instance of Pilosa server which belongs to a cluster.  

<strong id="cluster">Cluster:</strong> A cluster consists of one or more nodes which share a cluster configuration. The cluster also defines how data is replicated throughout and how internode communication is coordinated. Pilosa does not have a leader node, all data is evenly distributed, and any node can respond to queries.

<strong id="topn">TopN:</strong> Given a Frame and/or RowID this query returns the ordered set of RowID's by the number of columns that have a bit set in that row.

<strong id="tanimoto">Tanimoto:</strong> Used for similarity queries on Pilosa data. The Tanimoto Coefficient is the ratio of the intersecting set to the union set as the measure of similarity. 

<strong id="protobuf">Protobuf:</strong>: [Protocol Buffers](https://developers.google.com/protocol-buffers/) is a binary serialization format which Pilosa uses for internal messages, and can be used by clients as an alternative to JSON.

<strong id="toml">TOML:</strong> We use [TOML](https://github.com/toml-lang/toml) for our configuration file format.

<strong id="jump-consistent-hash">Jump Consistent Hash:</strong> A fast, minimal memory, consistent hash algorithm that evenly distributes the workload even when the number of buckets changes.
https://arxiv.org/pdf/1406.2294v1.pdf

<strong id="partition">Partition:</strong> The consistent hash is compiled with a maximum number of partitions or locations on the unit circle that keys are mapped to. Partitions are then evenly mapped to physical nodes. To add nodes to the cluster you simply need to remap the partitions, and associated data across the new cluster topography.

<strong id="replica">Replica:</strong> A copy of a [fragment] on a different host from the original. The "cluster.replicas" configuration parameter determines how many replicas of a fragment exist in the cluster (including the original, so a value of 1 means no extra copies are made).
