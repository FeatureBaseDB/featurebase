+++
title = "Glossary"
weight = 14
nav = []
+++

## Glossary

<strong id="anti-entropy">[Anti-entropy](../configuration/#anti-entropy-interval):</strong> A periodic process that compares each [shard](#shard) and its [replicas](#replica) across the [cluster](#cluster) to repair inconsistencies.

<strong id="attribute">[Attribute](../data-model/#attribute):</strong> Attributes can be associated to both [rows](#row) and [columns](#column). This metadata is kept separately from the core binary matrix in a [BoltDB](https://github.com/boltdb/bolt) store.

<strong id="bit">[Bit](../data-model/#overview):</strong> Bits are the fundamental unit of data in Pilosa. A bit lives in a [field](#field), at the intersection of a [row](#row) and [column](#column).

<strong id="bitmap">[Bitmap](../data-model/#overview):</strong> The on-disk and in-memory representation of a [row](#row). Implemented with [Roaring](#roaring-bitmap).

<strong id="bsi">[BSI](../data-model/#bsi-range-encoding)</strong> Bit-sliced indexing is the method Pilosa uses to represent multi-bit integers. Integer values are stored in `int` [fields](#field), and can be used for [Range](#range-bsi), [Min](#min), [Max](#max), and [Sum](#sum) queries.

<strong id="cluster">Cluster:</strong> A cluster consists of one or more [nodes](#node) which share a cluster configuration. The cluster also defines how data is [replicated](#replica) and how internode communication is coordinated. Pilosa does not have a leader node, all data is evenly distributed, and any node can respond to queries.

<strong id="column">[Column](../data-model/#column):</strong> Columns are the fundamental horizontal data axis within Pilosa. Columns are global to all [fields](#field) within an [index](#index).

<strong id="fragment">Fragment:</strong> A Fragment is the intersection of a [field](#field) and a [shard](#shard) in an [index](#index).

<strong id="field">[Field](../data-model/#field):</strong> Fields are used to group [rows](#row) into different categories. Row IDs are namespaced by field such that the same row ID in a different field refers to a different row. For [ranked](#topn) fields, rows are kept in sorted order within the field. Fields are one of four types: set, [int](#bsi), bool, time, and mutex. For more information, see [data model](../data-model/) and [Creating fields](../api-reference/#create-field).

<strong id="frame">[Frame](../data-model/#field):</strong> Prior to Pilosa 1.0, fields were known as frames.

<strong id="gossip">[Gossip](https://en.wikipedia.org/wiki/Gossip_protocol):</strong> A protocol used by Pilosa for internal communication.

<strong id="index">[Index](../data-model/#index):</strong> An Index is a top level container in Pilosa, analogous to a database in an RDBMS. Queries cannot operate across multiple indexes.

<strong id="jump-consistent-hash">[Jump Consistent Hash](https://arxiv.org/pdf/1406.2294v1.pdf):</strong> A fast, minimal memory, consistent hash algorithm that evenly distributes the workload even when the number of buckets changes.

<strong id="max">[Max](../query-language/#max):</strong> A [PQL](#pql) query that returns the maximum integer value stored in an [integer](#bsi) [field](#field).

<strong id="maxshard">MaxShard:</strong> The total number of [shards](#shard) allocated to handle the current set of [columns](#column). This value is important for all [nodes](#node) to efficiently distribute queries. MaxShard is zero-indexed, so if an index contains six shards, its MaxShard will be 5.

<strong id="min">[Min](../query-language/#min):</strong> A [PQL](#pql) query that returns the minimum integer value stored in an [integer](#bsi) [field](#field).

<strong id="node">Node:</strong> An individual running instance of Pilosa server which belongs to a [cluster](#cluster).

<strong id="partition">Partition:</strong> The [consistent hash](#jump-consistent-hash) maps keys to partitions (or locations on the unit circle), based on a preset maximum number of partitions. Partitions are then evenly mapped to physical [nodes](#node). To add nodes to the [cluster](#cluster), the partitions must be remapped, and data is then associated across the new cluster topology. `DefaultPartitionN` is 256. It can be modified, but only at compile time, and before ingesting any data.

<strong id="pql">[PQL](../query-language/):</strong> Pilosa Query Language.

<strong id="protobuf">[Protobuf](https://developers.google.com/protocol-buffers/):</strong> Protocol Buffers is a binary serialization format which Pilosa uses for internal messages, and can be used by clients as an alternative to JSON.

<strong id="range">[Range](../query-language/#range-queries):</strong>: A [PQL](#pql) query that returns bits based on comparison to timestamps, set according to the [time quantum](#time-quantum).

<strong id="range-bsi">[Range (BSI)](../query-language/#range-bsi):</strong>: A [PQL](#pql) query that returns bits based on comparison to integers stored in [BSI](#bsi) [fields](#field).

<strong id="replica">[Replica](../configuration/#cluster-replicas):</strong> A copy of a [fragment](#fragment) on a different [node](#node) than the original. The `cluster.replicas` configuration parameter determines how many replicas of a fragment exist in the cluster. This includes the original, so a value of 1 means no extra copies are made.

<strong id="roaring-bitmap">[Roaring Bitmap](http://roaringbitmap.org):</strong> the compressed bitmap format which Pilosa uses to [implement bitmaps](../architecture/#roaring-bitmap-storage-format), for both storage and logical query operations.

<strong id="row">[Row](../data-model/#row):</strong> Rows are the fundamental vertical data axis within Pilosa. They are namespaced to each [field](#field) within an [index](#index). Represented as a [Bitmap](#bitmap).

<strong id="slice">[Slice](../data-model/#shard):</strong> Prior to Pilosa 1.0, shards were known as slices.

<strong id="shard">[Shard](../data-model/#shard):</strong> [Columns](#column) are [sharded](https://en.wikipedia.org/wiki/Shard_(database_architecture)) on a preset [width](#shardwidth). Shards are operated on in parallel and are evenly distributed across the cluster via a [consistent hash](#jump-consistent-hash).

<strong id="shardwidth">ShardWidth:</strong> This is the number of [columns](#column) in a [shard](#shard). `ShardWidth` defaults to 2<sup>20</sup> or about one million. It can be modified, but only at compile time, and before ingesting any data.

<strong id="sum">[Sum](../query-language/#sum):</strong> A [PQL](#pql) query that returns the sum of integers stored in an [integer](#bsi) [field](#field).

<strong id="time-quantum">[Time quantum](../data-model/#time-quantum):</strong> Defines the granularity to be used for [Range](#range) queries on time [fields](#field).

<strong id="toml">[TOML](https://github.com/toml-lang/toml):</strong> the language used for Pilosa's [configuration file](../configuration/).

<strong id="topn">[TopN](../query-language/#topn):</strong> A [PQL](#pql) query that returns a list of rows, sorted by the count of [columns](#column) set in the [row](#row), within a specified [field](#field).

<strong id="view">[View](../data-model/#view):</strong> Views separate the different data layouts within a [Field](#field). The primary view is standard, which represents the typical [row](#row)/[column](#column) data. Time based field views are automatically generated for each [time quantum](#time-quantum). Views are internally managed by Pilosa, and never exposed directly via the API. This simplifies the functional interface by separating it from the physical data representation.
