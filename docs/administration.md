+++
title = "Administration"
weight = 13
nav = [
    "Installing in production",
    "Importing and Exporting Data",
    "Versioning",
    "Resizing the Cluster",
    "Backup/restore",
]
+++

## Administration Guide

### Installing in production

#### Hardware

Pilosa is a standalone, compiled Go application, so there is no need to worry about running and configuring a Java VM. Pilosa can run on very small machines and works well with even a medium sized dataset on a personal laptop. If you are reading this section, you are likely ready to deploy a cluster of Pilosa servers handling very large datasets or high velocity data. These are guidelines for running a cluster; specific needs may differ.

#### Memory

Pilosa holds all row/column bitmap data in main memory. While this data is compressed more than a typical database, available memory is a primary concern. In a production environment, we recommend choosing hardware with a large amount of memory >= 64GB. Prefer a small number of hosts with lots of memory per host over a larger number with less memory each. Larger clusters tend to be less efficient overall due to increased inter-node communication.

#### CPUs

Pilosa is a concurrent application written in Go and can take full advantage of multicore machines. The main unit of parallelism is the [shard](../data-model/#shard), so a single query will only use a number of cores up to the number of shards stored on that host. Multiple queries can still take advantage of multiple cores as well, so tuning in this area is dependent upon the expected workload.

#### Disk

Even though the main dataset is in memory Pilosa backs up to disk frequently. We recommend SSDs—especially if you have a write-heavy application.

#### Network

Pilosa is designed to be a distributed application, with data replication replicated across the cluster. As such, every write and read needs to communicate with several nodes. Therefore fast internode communication is essential. If using a service like AWS we recommend that all nodes exist in the same region and availability zone. The inherent latency of spreading a Pilosa cluster across physical regions is not usually worth the redundancy protection. Since Pilosa is designed to be an indexing service there should already be a system of record, or ability to rebuild a cluster quickly from backups.

#### Overview

While Pilosa does have some high system requirements it is not a best practice to set up a cluster with the fewest, largest machines available. You want an evenly distributed load across several nodes in a cluster to easily recover from a single node failure, and have the resource capacity to handle a missing node until it's repaired or replaced. Nor is it advisable to have many small machines, as the internode network traffic will become a bottleneck. You can always add nodes later, but that does require some down time.

### Open File Limits

Pilosa requires a large number of open files to support its memory-mapped file storage system. Most operating systems put limits on the maximum number of files that may be opened concurrently by a process. On Linux systems, this limit is controlled by a utility called [ulimit](https://ss64.com/bash/ulimit.html). Pilosa will automatically attempt to raise the limit to `262144` during startup, but it may fail due to access limitations. If you see errors related to open file limits when starting Pilosa, it is recommended that you run `sudo ulimit -n 262144` before starting Pilosa.

On Mac OS X, `ulimit` does not behave predictably. [This blog post](https://blog.dekstroza.io/ulimit-shenanigans-on-osx-el-capitan/) contains information about setting open file limits in OS X.

### Importing and Exporting Data

#### Importing

The import API expects a csv of the format `Row,Column`.

When importing large datasets remember it is much faster to pre sort the data by row ID and then by column ID in ascending order. You can use the `--sort` flag to do that. Also, avoid querying Pilosa until the import is complete, otherwise you will experience inconsistent results.

```
pilosa import --sort -i project -f stargazer project-stargazer.csv
```

##### Importing Integer Values

If you are using [integer](../data-model/#bsi-range-encoding) field values, the CSV file should be in the format `Column,Value`.

```
pilosa import -i project -f stargazer-counts project-stargazer-counts.csv
```

##### Importing Boolean Values

If you are using a [boolean](../data-model/#boolean) field, the CSV file should be in the format `Boolean,Value`, where `Boolean` is either `0` (false) or `1` (true).

For example, importing a file with the following contents will result in columns 3 and 9 being set in the `false` row, and columns 1, 2, 4, and 8 being set in the `true` row.
```
0,3
0,9
1,1
1,2
1,4
1,8
```

<div class="note">
    <p>Note that you must first create a field. View <a href="../api-reference/#create-field">Create Field</a> for more details. The `-e` flag can create the necessary schema when using a field of type "set".</p>
</div>

#### Exporting

Exporting data to csv can be performed on a live instance of Pilosa. You need to specify the index and the field. The API also expects the shard number, but the `pilosa export` sub command will export all shards within a field. The data will be in csv format `Row,Column` and sorted by column.
```request
curl "http://localhost:10101/export?index=repository&field=stargazer&shard=0" \
     --header "Accept: text/csv"
```
```response
2,10
2,30
3,426
4,2
...
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

To upgrade Pilosa:

1. First, upgrade the [client libraries](../client-libraries/) you are using in your application. Generally, a client version `X` will be compatible with the Pilosa server version `X` and earlier. For example, `python-pilosa 0.9.0` is compatible with both `pilosa 0.8.0` and `pilosa 0.9.0`.
2. Next, download the latest release from our [installation page](/docs/latest/installation/) or from the [release page on Github](https://github.com/pilosa/pilosa/releases).
3. Shut down the Pilosa cluster.
4. Make a backup of the [data directory](../configuration/#data-dir) on each cluster node.
5. Upgrade the Pilosa server binaries and any configuration changes. See the following sections on any version-specific changes you must make.
6. Start Pilosa. It is recommended to start the cluster coordinator node first, followed by any other nodes.

##### Version 0.9

Pilosa v0.9 introduces a few compatibility changes that need to be addressed.

**Configuration changes**: These changes need to occur before starting Pilosa v0.9:

1. Cluster-resize capability eliminates the `hosts` setting. Now, cluster membership is determined by gossip. This is only a factor if you are running Pilosa as a cluster.
2. Gossip-based cluster membership requires you to set a single cluster node as a [coordinator](../configuration/#cluster-coordinator). Make sure only a single node has the `cluster.coordinator` flag set.
3. `gossip.seed` has been renamed [`gossip.seeds`](../configuration/#gossip-seeds) and takes multiple items. It is recommended that at least two nodes are specified as gossip seeds.

**Data directory changes**: These changes need to occur while the cluster is shut down, before starting Pilosa v0.9:

Pilosa v0.9 adds two new files to the data directory, an `.id` file and a `.topology` file. Due to the way Pilosa internally shards indices, upgrading a Pilosa cluster will result in data loss if an existing cluster is brought up without these files. New clusters will generate them automatically, but you may migrate an existing cluster by using a tool we called [`topology-generator`](https://github.com/pilosa/upgrade-utils/tree/master/v0.9/topology-generator):

1. Observe the `cluster.hosts` configuration value in Pilosa v0.8. The ordering of the nodes in the config file is significant, as it determines shard ownership. Pilosa v0.9 uses UUIDs for each node, and the ordering is alphabetical.
2. Install the `topology-generator`: `go get github.com/pilosa/upgrade-utils/v0.9/topology-generator`.
3. Run the `topology-generator`. There are two arguments: the number of nodes and the output directory. For this example, we'll assume a 3-node cluster and place the files in the current working directory: `topology-generator 3 .`.
4. This tool will generate a file, `topology`, and multiple id files, called `nodeX.id`, X being the node index position.
5. Copy the file `topology` into the data directories of every node in the cluster, naming it `.topology` (note the dot), e.g. `cp topology ~/.pilosa/.topology` or `scp topology node1:.pilosa/.topology`.
6. Copy the node ID files into the respective node data directories. For example, `node0.id` will be placed on the first node in the `cluster.hosts` list, with the name `.id`. For example: `scp node0.id node0:.pilosa/.id`. Again, it is very important that the ordering you give the nodes with these IDs matches the ordering you had in your existing `cluster.hosts` setting.

**Application changes**:

1. Row and column labels were deprecated in Pilosa v0.8, and removed in Pilosa v0.9. Make sure that your application does not attempt to use a custom row or column label, as they are no longer supported.
2. If your application relies on the implicit creation of [time quantums](../glossary/#time-quantum) by inheriting the time-quantum setting of the index, you must begin explicitly enabling the time quantum per-field, as index-level time-quantums have been removed.
3. Inverse fields have been deprecated, removed from docs, and will be unsupported in the next release.

### Resizing the Cluster

If you need to increase (or decrease) the capacity of a Pilosa server, you can add or remove nodes to a running cluster at any time. Note that you can only add or remove one node at a time; if you attempt to add multiple nodes at once, those requests will be enqueued and processed serially. Also note that during any resize process, the cluster goes into state `RESIZING` during which all read/write requests are denied. When the cluster returns to state `NORMAL` then read/write operations can resume. The amount of time that the cluster stays in state `RESIZING` depends on the amount of data that needs to be moved during the resize process.

#### Adding a Node

You can add a new, empty node to an existing cluster by starting `pilosa server` on the new node with the correct configuration options. Specifically, you must specify the [cluster coordinator](../configuration/#cluster-coordinator) to be the same as the coordinator on the existing nodes. You must also specify at least one valid [gossip seed](../configuration/#gossip-seeds) (preferably multiple for redundancy). When the new node starts, the coordinator node will receive a `nodeJoin` event indicating that a new node is joining the cluster. At this point, the coordinator will put the cluster into state `RESIZING` and kick off a resize job that instructs all of the nodes in the cluster how to rebalance data to accomodate the additional capacity of the new node. Once the resize job is complete, the coordinator will put the cluster back to state `NORMAL` and ensure that the new node is included in future queries.

If the node is being added to a cluster which contains no data (for example, during startup of a new cluster), the coordinator will bypass the `RESIZING` state and allow the node to join the cluster immediately.

#### Removing a Node

In order to  remove a node from a cluster, your cluster must be configured to have a [cluster replicas](../configuration/#cluster-replicas) value of at least 2; if you're removing a node that no longer exists (for example a node that has died), there must be at least one additional replica of the data owned by the dead node in order for the cluster to correctly rebalance itself.

To remove node `localhost:10102` from a cluster having coordinator `localhost:10101`, first determine the ID of the node to be removed. If the node to be removed is still available, you can find the ID by issuing a `/status` request to the node. The node's ID is in the `localID` field:
``` request
curl localhost:10101/status
```
``` response
{
    "state":"NORMAL",
    "nodes":[
        {"id":"24824777-62ec-4151-9fbd-67e4676e317d","uri":{"scheme":"http","host":"localhost","port":10101}}
        {"id":"40a891fa-243b-4d71-ae24-4f5c78a0f4b1","uri":{"scheme":"http","host":"localhost","port":10102}}
        {"id":"9fab09cc-3c26-4202-9622-d167c84684d9","uri":{"scheme":"http","host":"localhost","port":10103}}
    ],
    "localID": "40a891fa-243b-4d71-ae24-4f5c78a0f4b1"
}
```

If the node to be removed is no longer available, you can get the IDs of the nodes in the cluster by issuing a `/status` request to any available node:
``` request
curl localhost:10101/status
```
``` response
{
    "state":"NORMAL",
    "nodes":[
        {"id":"24824777-62ec-4151-9fbd-67e4676e317d","uri":{"scheme":"http","host":"localhost","port":10101}}
        {"id":"40a891fa-243b-4d71-ae24-4f5c78a0f4b1","uri":{"scheme":"http","host":"localhost","port":10102}}
        {"id":"9fab09cc-3c26-4202-9622-d167c84684d9","uri":{"scheme":"http","host":"localhost","port":10103}}
    ],
    "localID": "40a891fa-243b-4d71-ae24-4f5c78a0f4b1"
}
```

Once you have the ID of the node that you want to remove from the cluster, issue the following request:
```
curl localhost:10101/cluster/resize/remove-node \
     -X POST \
     -d '{"id": "40a891fa-243b-4d71-ae24-4f5c78a0f4b1"}'
```
At this point, the coordinator will put the cluster into state `RESIZING` and kick off a resize job that instructs all of the nodes in the cluster how to rebalance data to accomodate the reduced capacity of the cluster. Once the resize job is complete, the coordinator will put the cluster back to state `NORMAL` and ensure that the removed node is no longer included in future queries.

Note that you can't directly remove the coordinator node. If you need to remove the coordinator node from the cluster, you must first [make one of the other nodes the coordinator](#changing-the-coordinator).

#### Aborting a Resize Job

If at any point you need to abort an active resize job, you can issue a `POST` request to the `/cluster/resize/abort` endpoint on the coordinator node.
For example, if your coordinator node is `localhost:10101`, then you can run:
```
curl localhost:10101/cluster/resize/abort -X POST
```
This will immediately abort the resize job and return the cluster to state `NORMAL`. Because data is never removed from a node during a resize job (only once a resize job has successfully completed), aborting a resize job will return the cluster back to the state it was in before the resize began.

#### Changing the Coordinator

In order to assign a different node to be the coordinator, you can issue a `/cluster/resize/set-coordinator` request to any node in the cluster. The payload should indicate the ID of the node to be made coordinator.
```
curl localhost:10101/cluster/resize/set-coordinator \
     -X POST \
     -d '{"id": "9fab09cc-3c26-4202-9622-d167c84684d9"}'
```

### Backup/restore

Pilosa continuously writes out the in-memory bitmap data to disk. This data is organized by Index->Field->Views->Fragment->numbered shard files. These data files can be routinely backed up to restore nodes in a cluster.

Depending on the size of your data you have two options. For a small dataset you can rely on the periodic anti-entropy sync process to replicate existing data back to this node.

For larger datasets and to make this process faster you could copy the relevant data files from the other nodes to the new one before startup.

Note: This will only work when the replication factor is >= 2

#### Using Index Sync

- Shutdown the cluster.
- Modify config file to replace existing node address with new node.
- Restart all nodes in the cluster.
- Wait for auto Index sync to replicate data from existing nodes to new node.

#### Copying data files manually

- To accomplish this you will first need:
  - List of all indexes on your cluster
  - List of all fields in your indexes
  - Max shard per index, listed in the `/internal/shards/max` endpoint
- With this information you can query the `/internal/fragment/nodes` endpoint and iterate over each shard
- Using the list of shards owned by this node you will then need to manually:
  - setup a directory structure similar to the other nodes with a path for each Index/Field
  - copy each owned shard for an existing node to this new node
- Modify the cluster config file to replace the previous node address with the new node address.
- Restart the cluster
- Wait for the first sync (10 minutes) to validate Index connections

### Diagnostics

Each Pilosa cluster is configured by default to share anonymous usage details with Pilosa Corp. These metrics allow us to understand how Pilosa is used by the community and improve the technology to suit your needs. Diagnostics are sent to Pilosa every hour. Each of the metrics are detailed below as well as opt-out instructions.

- **Version:** Version string of the build.
- **Host:** Host URI.
- **Cluster:** List of nodes in the cluster.
- **NumNodes:** Number of nodes in the cluster.
- **NumCPU:** Number of cores per node
- **BSIEnabled:** Bit Sliced Index Fields in use.
- **TimeQuantumEnabled:** Time Quantum Fields in use.
- **NumIndexes:** Number of indexes in the Cluster.
- **NumFields:** Number of fields in the Cluster.
- **NumShards:** Number of shards in the Cluster.
- **NumViews:** Number of views in the Cluster.
- **OpenFiles:** Open file handle count.
- **GoRoutines:** Go routine count.
 
You can opt-out of the Pilosa diagnostics reporting by setting the command line configuration option `--metric.diagnostics=false`, the `PILOSA_METRIC_DIAGNOSTICS` environment variable, or the TOML configuration file `[metric]` `diagnostics` option.

### Metrics

Pilosa can be configured to emit metrics pertaining to its internal processes in one of two formats: Expvar or StatsD. Metric recording is disabled by default.
The metrics configuration options are: 

  - [Host](../configuration/#metric-host): specify host that receives metric events
  - [Poll Interval](../configuration/#metric-poll-interval): specify polling interval for runtime metrics
  - [Service](../configuration/#metric-service): declare type StatsD or Expvar

#### Tags
StatsD Tags adhere to the DataDog format (key:value), and we tag the following:

- NodeID
- Index
- Field
- View
- Shard

#### Events
We currently track the following events

- **Index:** The creation of a new index.
- **Field:** The creation of a new field.
- **MaxShard:** The creation of a new Shard.
- **SetBit:** Count of set bits.
- **ClearBit:** Count of cleared bits.
- **ImportBit:** During a bulk data import this represents the count of bits created.
- **SetRowAttrs:** Count of attributes set per row.
- **SetColumnAttrs:** Count of attributes set per column.
- **Bitmap:** Count of Bitmap queries.
- **TopN:** Count of TopN queries.
- **Union:** Count of Union queries.
- **Intersection:** Count of Intersection queries.
- **Difference:** Count of Difference queries.
- **Xor:** Count of Xor queries.
- **Not:** Count of Not queries.
- **Count:** Count of Count queries.
- **Range:** Count of Range queries.
- **Snapshot:** Event count when the snapshot process is triggered.
- **BlockRepair:** Count of data blocks that were out of sync and repaired.
- **GarbageCollection:** Event count when garbage collection occurs.
- **Goroutines:** Number of running goroutines.
- **OpenFiles:** Number of open file handles associated with running Pilosa process ID.
