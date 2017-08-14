+++
title = "Administration"
weight = 13
nav = [
    "Installing in production",
    "Imports and Exports",
    "Versioning",
    "Backup/restore",
]
+++

## Administration Guide

### Installing in production

#### Hardware

Pilosa is a standalone, compiled Go application, so there is no need to worry about running and configuring a Java VM. Pilosa can run on very small machines and works well with even a medium sized dataset on a personal laptop. If you are reading this section, you are likely ready to deploy a cluster of Pilosa servers handling very large datasets or high velocity data. These are guidelines for running a cluster; specific needs may differ.

#### Memory

Pilosa holds all row/column bitmap data in main memory. While this data is compressed more than a typical database, available memory is a primary concern.  In a production environment, we recommend choosing hardware with a large amount of memory >= 64GB.  Prefer a small number of hosts with lots of memory per host over a larger number with less memory each. Larger clusters tend to be less efficient overall due to increased inter-node communication.

#### CPUs

Pilosa is a concurrent application written in Go and can take full advantage of multicore machines. The main unit of parallelism is the slice, so a single query will only use a number of cores up to the number of slices stored on that host. Multiple queries can still take advantage of multiple cores as well though, so tuning in this area is dependent on the expected workload.

#### Disk

Even though the main dataset is in memory Pilosa does back up to disk frequently.  We recommend SSDs--especially if you have a write heavy application.

#### Network

Pilosa is designed to be a distributed application, with data replication shared across the cluster.  As such every write and read needs to communicate with several nodes.  Therefore fast internode communication is essential. If using a service like AWS we recommend that all node exist in the same region and availability zone.  The inherent latency of spreading a Pilosa cluster across physical regions it not usually worth the redundancy protection.  Since Pilosa is designed to be an Indexing service there already should be a system of record, or ability to rebuild a Cluster quickly from backups.

#### Overview

While Pilosa does have some high system requirements it is not a best practice to set up a cluster with the fewest, largest machines available.  You want an evenly distributed load across several nodes in a cluster to easily recover from a single node failure, and have the resource capacity to handle a missing node until it's repaired or replaced.   Nor is it advisable to have many small machines.  The internode network traffic will become a bottleneck.  You can always add nodes later, but that does require some down time.

### Open File Limits

Pilosa requires a large number of open files to support its memory-mapped file storage system. Most operating systems put limits on the maximum number of files that may be opened concurrently by a process. On Linux systems, this limit is controlled by a utility called [ulimit](https://ss64.com/bash/ulimit.html). Pilosa will automatically attempt to raise the limit to `262144` during startup, but it may fail due to access limitations. If you see errors related to open file limits when starting Pilosa, it is recommended that you run `sudo ulimit -n 262144` before starting Pilosa.

On Mac OS X, `ulimit` does not behave predictably. [This blog post](https://blog.dekstroza.io/ulimit-shenanigans-on-osx-el-capitan/) contains information about setting open file limits in OS X.

### Importing and Exporting Data

#### Importing

The import API expects a csv of RowID,ColumnID's.

When importing large datasets remember it is much faster to pre sort the data by RowID and then by ColumnID in ascending order. You can use the `--sort` flag to do that. Also, avoid querying Pilosa until the import is complete, otherwise you will experience inconsistent results.

```
pilosa import --sort -d project -f stargazer project-stargazer.csv
```

#### Exporting

Exporting Data to csv can be performed on a live instance of Pilosa. You need to specify the Index, Frame, and View(default is standard). The API also expects the slice number, but the `pilosa export` sub command will export all slices within a Frame. The data will be in csv format RowID,ColumnID and sorted by column ID.
```
curl "http://localhost:10101/export?index=repository&frame=stargazer&slice=0&view=standard" \
     --header "Accept: text/csv"
```

### Versioning

Pilosa follows [Semantic Versioning](http://semver.org/).

MAJOR.MINOR.PATCH:

* MAJOR version when you make incompatible API changes,
* MINOR version when you add functionality in a backwards-compatible manner, and
* PATCH version when you make backwards-compatible bug fixes.

#### PQL versioning

The Pilosa server should support PQL versioning using HTTP headers. On each request, the client should send a Content-Type header and an Accept header. The server should respond with a Content-Type header that matches the client Accept header. The server should also optionally respond with a Warning header if a PQL version is in a deprecation period, or an HTTP 400 error if a PQL version is no longer supported.

#### Upgrading

When upgrading, upgrade clients first, followed by server for all Minor and Patch level changes.

### Backup/restore

Pilosa continuously writes out the in-memory bitmap data to disk.  This data is organized by Index->Frame->Views->Fragment->numbered slice files.  These data files can be routinely backed up to restore nodes in a cluster.

Depending on the size of your data you have two options.  For a small dataset you can rely on the periodic anti-entropy sync process to replicate existing data back to this node.

For larger datasets and to make this process faster you could copy the relevant data files from the other nodes to the new one before startup.

Note: This will only work when the replication factor is >= 2

#### Using Index Sync

- Shutdown the cluster.
- Modify config file to replace existing node address with new node.
- Restart all nodes in the cluster.
- Wait for auto Index sync to replicate data from existing nodes to new node.

#### Copying data files manually

- To accomplish this goal you will 1st need:
  - List of all Indexes on your cluster
  - List of all frames in your Indexes
  - Max slice per Index, listed in the /status endpoint
- With this information you can query the `/fragment/nodes` endpoint and iterate over each slice
- Using the list of slices owned by this node you will then need to manually:
  - setup a directory structure similar to the other nodes with a path for each Index/Frame
  - copy each owned slice for an existing node to this new node
- Modify the cluster config file to replace the previous node address with the new node address.
- Restart the cluster
- Wait for the 1st sync (10 minutes) to validate Index connections

#### Metrics

Pilosa can be configured to emit metrics pertaining to its internal processes in one of two formats: Expvar or StatsD. Metric recording is disabled by default.
The metrics configuration options are: 

  - Host to receive events
  - Polling interval for runtime metrics
  - Metric type (StatsD, Expvar).

##### Tags
StatsD Tags adhere to the DataDog format (key:value), and we tag the following:

- NodeID
- Index
- Frame
- View
- Slice

##### Events
We currently track the following events

<strong id="index">Index:</strong> The creation of a new Index.

<strong id="frame">Frame:</strong> The creation of a new Frame.

<strong id="maxSlice">MaxSlice:</strong> The Creation of a new Slice.

<strong id="setbit">SetBit:</strong> Count of set bits.

<strong id="clearbit">ClearBit:</strong> Count of cleared bits.

<strong id="importbit">ImportBit:</strong> During a bulk data import this represents the count of bits created.

<strong id="setrowattrs">SetRowAttrs:</strong> Count of Attributes set per row.

<strong id="setcollumnattrs">SetColumnAttrs:</strong> Count of Attributes set per collumn.

<strong id="bitmap">Bitmap:</strong> Count of Bitmap queries.

<strong id="topn">TopN:</strong> Count of TopN queries.

<strong id="union">Union:</strong> Count of Union queries.

<strong id="intersection">Intersection:</strong> Count of Intersection queries.

<strong id="difference">Difference:</strong> Count of Difference queries.

<strong id="count">Count:</strong> Count of Count queries.

<strong id="range">Range:</strong> Count of Range queries.

<strong id="snapshot">Snapshot:</strong> Event count when the snapshot process is triggered.

<strong id="blockrepair">BlockRepair:</strong> Count of data blocks that were out of sync and repaired.

<strong id="garbage_collection">Garbage Collection:</strong> Event count when Garbage Collection occurs.

<strong id="goroutines">Goroutines:</strong> Number of running Goroutines.

<strong id="openfiles">OpenFiles:</strong> Number of open file handles associated with running Pilosa process ID.
