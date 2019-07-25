+++
title = "Architecture"
weight = 6
nav = [
    "Overview",
    "Pilosa Request",
    "Pilosa Interface",
    "Pilosa",
    "Pilosa Roaring"
]
+++
 
## Architecture

Pilosa is deisgned to compute real time, complex queries on huge data stores without resorting to pre-computation or approximation. It speacializes in speed, accuracy, and horizantal scalability.

Pilosa can be run as a single server, or `Node`, or it can be run in conjunction with several other nodes in a `Cluster`. For very large data sets, Pilosa works best when run in a cluster. The cluster can have multiple nodes, which are able to distribute the data load evenly and communicate with each other to perform queries. As a result, the Pilosa cluster is able to handle a lot more data and still maintain a high degree of accuracy. When setting up a cluster, nodes can be added or removed, and Pilosa will automatically redistribute the data across the existing nodes.

### Overview

A Pilosa server can be broken down into 4 basic components: the Pilosa Request, the Pilosa Interface, Pilosa, and Pilosa Roaring. In general, the Pilosa Request accepts requests from the user and passes them to the Pilosa Interface. The Pilosa Interface decides how to handle the request based on the type of request and can send the request to either another node or Pilosa. Pilosa handles the request, either by answering it in the case of a query or by carrying out the requested action in the case of a `set` command or import. Pilosa then returns either an answer or a `success` response to the Pilosa Request through the Pilosa Interface to be sent back to the user.

![Overview Diagram](/img/docs/PilosaOverview.svg)
*Pilosa Overview diagram*

Pilosa can store arbitrary keys/values, called attributes, in association with any row or column. An attribute can be a `string`, `integer`, `boolean`, or `float`.

### Pilosa Request

All interactions with Pilosa begin with the `Server Package`. The `Server Package` sends and receives messages between Pilosa and the user, helping the two to communicate with each other. When the user sends a query, for example, the `Server Package` accepts the request, then sends it to the `HTTP Package`. In the `HTTP Package`, the request is decoded to extract the PQL message Pilosa can use from the HTTP format and then the message is sent to the `Handler`. Once the message is in the `Handler`, it can be sent to the Pilosa Interface and then onto Pilosa. Once the query has been completed, the Pilosa Interface will then respond to the `Handler` with the answer to the query and the `Handler` will then give the `Server Package` the query response the user will receive.

![Pilosa Request Diagram](/img/docs/PilosaRequest.svg)
*Pilosa Request diagram*

### Pilosa Interface

From the Pilosa Request, the `API` takes over. The `API`, or the Application Programming Interface, determines how to handle incoming requests. If the `API` receives an import request, it sends the data down the red path in the diagram. If the `API` receives a query request, it sends the data down the green path in the diagram. As the instance of Pilosa in the Node changes, the `API` sends updates to `Broadcast` to notify the other nodes.

![Pilosa Interface Diagram](/img/docs/PilosaInterface.svg)
*Pilosa Interface diagram*

##### Imports
In the case of an import, the `API` will send the import data down the red arrow in the diagram to the three-way fork. If the data isn't in the rowID, columnID format the `API` expects or the rowID and columnID haven't been decoded, the data goes to the `Translator` for decoding. If the Field or Index the data is set to import to is not present in the `Node`, the data is sent to the `Client`, where it is sent out to the other nodes in the `Cluster` to find the appropriate node with the correct Field and Index. If the data doesn't require row or column translation and the `Node` has the correct Field and Index, the `API` sends the data straight into Pilosa.

##### Queries

In the case of a query, the `API` will send the request down the green path in the diagram to the `Parser`, where the query is extracted from the Pilosa Query Language (PQL). The query is then sent to the `Executor`, which decides how to handle the query. If the query requests a specifc row or column and the rowID or the columnID hasn't been decoded, the query is sent to the `Translator`, where the rowID and columnID are identified. If the `Node` doesn't have enough information to satisfy the query, the `Executor` sends the query to the `Client`, where it is sent out to the other nodes in the `Cluster` to find the node that can satisfy the query. If the query doesn't require row or column translation and the `Node` can satisfy the query, the `Executor` sends the query into Pilosa.

##### Broadcast and Client

`Broadcast` and `Client` are both able to communicate with other nodes in the `Cluster` through the different `APIs` in each node. However, they each complete a different task. `Broadcast` is responsible for keeping all the nodes up-to-date with the latest schema. If a field is created or destroyed, data is imported, or a node is added, it is the job of `Broadcast` to make all the other nodes in the `Cluster` aware of the change. `Client` is responsible for finding the correct node to carry out queries.

### Pilosa

Every request that enters Pilosa starts in the `Holder`, which is the top level container for Pilosa. As the `Holder` sends requests through Pilosa, it creates `Snapshots`. From the `Holder`, a request is sent to the `Index`, which is a container for Pilosa `Fields`. The `Index` decides which `Fields` to query and how their responses relate. Cross-Field queries, such as Count, Intersect, Union, Difference, and XOR, are handled by the `Index`. The `Field` is, in a nutshell, a segment of rows used to categorize the data. A `Field` takes a request and decides how to answer it. Each `Field` contains one or more `Views`, which store the `Field` to allow for quick quering. When there are multiple `Views`, they are usually recording the `Field's` timestamp. For every degree of granularity of a timestamp, there is at least one additional `View`. Any query dealing with time will first find the correct `View`. A `Field` is also broken up into `Fragments`. A `Fragment` is the intersection of the `Field` with a shard, or a set of 2<sup>20</sup> columns. Each `Fragment` in a `Field` can access a `Cache` and at least one `Row`. A `Cache` contains data considering the ranked form of a `Field's` data. A TopN query, for example, will access the `Cache` to return a response. `Rows` contain all the organized data. All row queries and imports end up in the `Row` to be answered or stored. Each `Row` is stored as a Roaring Bitmap in Pilosa Roaring.

![Pilosa Diagram](/img/docs/PilosaArch.svg)
*Pilosa Diagram*

##### SnapShot

A `Snapshot` is requested by the `Holder` and carried out by the `Fragment`. A `Snapshot` is exactly what it sounds like, a snapshot of the instance of Pilosa at any given time. It is used to store information about Pilosa onto disk.

### Pilosa Roaring

Pilosa Roaring is the storage method for Pilosa. When making an import, the data is stored in Pilosa Roaring. The journey into storage begins with `Roaring`, where each row is converted into a roaring bitmap. From `Roaring`, the bitmaps are sent to `Containers Slice`, where they are formatted and prepared for storage. The prepared bitmaps are then sent into `Container Stash`, where they are stored to memory ready for querying. When making a query, the answer is extracted from Pilosa Roaring by directly querying `Container Stash` memory. When a snapshot is made, `Roaring` sends the data to `Containers Btree` for formatting before being sent to `Container Stash` for storage on disk.

![Roaring Diagram](/img/docs/PilosaRoaring.svg)
*Pilosa Roaring Overview Diagram*

#### Roaring bitmap

Bitmaps are persisted to disk using a file format very similar to the [Roaring Bitmap format spec](https://github.com/RoaringBitmap/RoaringFormatSpec). Pilosa's format uses 64-bit IDs, so it is not binary-compatible with the spec. Some parts of the format are simpler, and an additional section is included. Specific differences include:

* The cookie is always bytes 0-3; the container count is always bytes 4-7, never bytes 2-3.
* The cookie includes file format version in bytes 2-3 (currently equal to zero).
* The descriptive header includes, for each container, a 64-bit key, a 16-bit cardinality, and a 16-bit container type (which only uses two bits now). This makes the runFlag bitset unnecessary. This is in contrast to the spec, which stores a 16-bit key and a 16-bit cardinality.
* The offset header section is always included.
* RLE runs are serialized as [start, last], not [start, length].
* After the container storage section is an operation log, of unspecified length.

![roaring file format diagram](/img/docs/pilosa-roaring-storage-diagram.png)
*Pilosa Roaring storage format diagram*

All values are little-endian. The first two bytes of the cookie is 12348, to reflect incompatibility with the spec, which uses 12346 or 12347. Container types are NOT inferred from their cardinality as in the spec. Instead, the container type is read directly from the descriptive header.

Check out this [blog post](/blog/adding-rle-support/) for some more details about Roaring in Pilosa.

### Resources

For more information about Pilosa, please see the following:

* [Setting up a Secure Cluster](https://www.pilosa.com/docs/latest/tutorials/)

* [Data-Model](https://www.pilosa.com/docs/latest/data-model/)

* [Code Base](https://github.com/pilosa/pilosa)

* [Pilosa Whitepaper](https://www.pilosa.com/pdf/PILOSA%20-%20Technical%20White%20Paper.pdf)

* [Administration Guide](https://www.pilosa.com/docs/latest/administration/)