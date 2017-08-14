+++
title = "FAQ"
weight = 15
nav = []
+++

## FAQ

### What is Pilosa?

Pilosa is an in-memory, distributed index that is layered over persistent storage. It supports fast ad-hoc queries and segmentation. Pilosa does not require the underlying data to be moved, rather it can be populated in conjunction with data writes, or it can be backfilled asynchronously from any other data store or event processing system. This allows Pilosa to support sub-second queries against very large underlying data sets.

### Is Pilosa a database?

Pilosa is not a database in the traditional sense. While Pilosa does store data (both in-memory as well as persisted to disk), it wouldn't typically be used as a primary data store. Instead, one would likely use Pilosa as an index of the data stored in a traditional database or in a data warehouse.

### Where does Pilosa fit in my stack?

Pilosa sits on top of a data store or multiple data stores.  
How is Pilosa different than Elasticsearch since they are both indexes?
Elasticsearch is a search engine based on Lucene, and is therefore very good at indexing and searching large volumes of unstructured text. As it matures, Elasticsearch has continued to move into the analytics space, but its core data object is still the "document". Pilosa is specifically designed to index structured data and improve query speed. By representing data as the relationship between objects, and then storing those relationships in bitmaps, Pilosa can very efficiently search and compare many millions of data points while still maintaining a small memory footprint.

### How do I get my data into Pilosa?

There are typically two methods for getting data into Pilosa: importing large batches of data from an existing data set, and continuously updating Pilosa as data is added or updated.

In the first case, one would use the `pilosa import` command to bulk load structured data into Pilosa. In order to improve this process, one can use the Pilosa Development Kit (PDK) to map structured data in the original data set onto the Pilosa schema.

For the case where data is continually mutating, one would apply a parallel data writer at the point at which data is written to the persistent data store. This new writer would simultaneously write to Pilosa. An example use case would be one where Kafka was employed as the message broker in your data pipeline, you could introduce an additional Kafka consumer to read from the message log and write mutated data to Pilosa.

### What languages can I use with it?

There is currently client support for Go, Python, and Java. If you want to use Pilosa with a different language, you can access Pilosa via the Pilosa API.

### Do you query Pilosa using SQL?

One can access Pilosa directly via the terminal using the Pilosa Query Language (PQL), but a typical implementation would use one of the Pilosa client libraries to integrate with an existing codebase. There is currently client support for Go, Python, and Java.


### Replication on each node?

Pilosa supports a replication factor greater than or equal to one. When replication is configured to be greater than one, then all mutations will be replicated to additional nodes in the cluster. For example, in a five-node cluster consisting of nodes A-B-C-D-E and with replication factor of three, then a write to node B will result in data being written to nodes B, C, and D. If the replication factor is greater than the number of nodes in the cluster, the data will be replicated to every node in the cluster only once.
