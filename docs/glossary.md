+++
title = "Glossary"
weight = 14
nav = []
+++

## Glossary

<strong id="anti-entropy">[Anti-entropy](../configuration/#anti-entropy-interval):</strong> A periodic process that compares each [slice](#slice) and its [replicas](#replica) across the [cluster](#cluster) to repair inconsistencies.

<strong id="attribute">[Attribute](../data-model/#attribute):</strong> Attributes can be associated to both [rows](#row) and [columns](#column). This metadata is kept separately from the core binary matrix in a [BoltDB](https://github.com/boltdb/bolt) store.

<strong id="bit">[Bit](../data-model/#overview):</strong> Bits are the fundamental unit of data in Pilosa. A bit lives in a [frame](#frame), at the intersection of a [row](#row) and [column](#column).

<strong id="bitmap">[Bitmap](../data-model/#overview):</strong> The on-disk and in-memory representation of a [row](#row). Implemented with [Roaring](#roaring-bitmap).

<strong id="cluster">Cluster:</strong> A cluster consists of one or more [nodes](#node) which share a cluster configuration. The cluster also defines how data is [replicated](#replica) throughout and how internode communication is coordinated. Pilosa does not have a leader node, all data is evenly distributed, and any node can respond to queries.

<strong id="column">[Column](../data-model/#column):</strong> Columns are the fundamental horizontal data axis within Pilosa. Columns are global to all [frames](#frame) within an [index](#index).

<strong id="fragment">Fragment:</strong> A Fragment is the intersection of a [frame](#frame) and a [slice](#slice) in an [index](#index).

<strong id="frame">[Frame](../data-model/#frame):</strong> Frames are used to group [rows](#row) into different categories. `RowID`s are namespaced by frame such that the same `RowID` in a different frame refers to a different row. For [ranked](#topn) frames, rows are kept in sorted order within the frame.

<strong id="index">[Index](../data-model/#index):</strong> An Index is a top level container in Pilosa, analogous to a database in an RDBMS. Queries cannot operate across multiple indexes.

<strong id="jump-consistent-hash">[Jump Consistent Hash](https://arxiv.org/pdf/1406.2294v1.pdf):</strong> A fast, minimal memory, consistent hash algorithm that evenly distributes the workload even when the number of buckets changes.

<strong id="maxslice">MaxSlice:</strong> The total number of [slices](#slice) allocated to handle the current set of [columns](#columns). This value is important for all [nodes](#node) to efficiently distribute queries.

<strong id="node">Node:</strong> An individual running instance of Pilosa server which belongs to a [cluster](#cluster).

<strong id="partition">Partition:</strong> The [consistent hash](#jump-consistent-hash) maps keys to partitions (or locations on the unit circle), based on a preset maximum number of partitions (256 by default). Partitions are then evenly mapped to physical [nodes](#node). To add nodes to the [cluster](#cluster), the partitions must be remapped, and data is then associated across the new cluster topology.

<strong id="pql">[PQL](../query-language/):</strong> Pilosa Query Language.

<strong id="protobuf">[Protobuf](https://developers.google.com/protocol-buffers/):</strong> Protocol Buffers is a binary serialization format which Pilosa uses for internal messages, and can be used by clients as an alternative to JSON.

<strong id="replica">[Replica](../configuration/#cluster-replicas):</strong> A copy of a [fragment](#fragment) on a different [node](#node) than the original. The `cluster.replicas` configuration parameter determines how many replicas of a fragment exist in the cluster. This includes the original, so a value of 1 means no extra copies are made.

<strong id="roaring-bitmap">[Roaring Bitmap](http://roaringbitmap.org):</strong> the compressed bitmap format which Pilosa uses to [implement bitmaps](../architecture/#roaring-bitmap-storage-format), for both storage and logical query operations.

<strong id="row">[Row](../data-model/#row):</strong> Rows are the fundamental vertical data axis within Pilosa. They are namespaced to each [frame](#frame) within an [index](#index). Represented as a [bitmap][#bitmap].

<strong id="slice">[Slice](../data-model/#slice):</strong> [Columns](#column) are sharded on a preset [width](#slicewidth). Each shard is referred to as a slice in Pilosa. Slices are operated on in parallel and are evenly distributed across the cluster via a [consistent hash](#jump-consistent-hash).

<strong id="slicewidth">SliceWidth:</strong> This is the number of [columns](#column) in a [slice](#slice). By default, 2<sup>20</sup> or about one million.

<strong id="tanimoto">[Tanimoto](../examples/#chemical-similarity-search):</strong> Used for similarity queries on Pilosa data. The [Tanimoto Coefficient](https://en.wikipedia.org/wiki/Jaccard_index#Tanimoto_similarity_and_distance) between two [bitmaps](#bitmap) A and B is the ratio of the size of their intersection to the size of their union (|A∩B|/|A∪B|).

<strong id="toml">[TOML](https://github.com/toml-lang/toml):</strong> the language used for Pilosa's [configuration file](../configuration).

<strong id="topn">TopN:</strong> A [PQL](#pql) query that returns a list of `RowID`s, sorted by the count of [bits](#bit) set in the [row](#row), within a specified [frame](#frame).

<strong id="view">[View](../data-model/#view):</strong> Views separate the different data layouts within a [Frame](#frame). The two primary views are standard and inverse which represent the typical [row](#row)/[column](#column) data and its inverse respectively (an [inverted index](https://en.wikipedia.org/wiki/Inverted_index), or a matrix transpose). Time based frame views are automatically generated for each time quantum. Views are internally managed by Pilosa, and never exposed directly via the API. This simplifies the functional interface by separating it from the physical data representation.
