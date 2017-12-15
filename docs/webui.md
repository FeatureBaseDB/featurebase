+++
title = "WebUI"
weight = 9
nav = [
    "Console",
    "Cluster Admin",
]
+++

## WebUI

The Pilosa server comes packaged with in-browser WebUI.  When you run a local Pilosa server on the default host, you can access it at [localhost:10101](http://localhost:10101)
This can be used for constructing queries and viewing the cluster status.

### Console

The [Console view](http://localhost:10101/#console) allows you to enter [PQL](../query-language) queries and run them against your locally running server.  First you must select an Index with the Select index dropdown.

Each query's result will be displayed in the Output section along with the query time. 

The Console will keep a record of each query and its result with the latest query on top.

![console](/img/docs/webui-console.png)

In addition to standard PQL, the console supports a few special commands, prefixed with `:`.

- `:create index <indexname>`
- `:delete index <indexname>`
- `:use <indexname>`
- `:create frame <framename>`
- `:delete frame <framename>`

Frame creation also supports options like `timeQuantum` or `inverseEnabled`. When creating a new frame, add options by using the keys documented in [API reference](../api-reference).

- `:create index <indexname> timeQuantum=YM`
- `:create frame <framename> inverseEnabled=true cacheSize=10000`


### Cluster Admin

Use the [Cluster Admin tab](http://localhost:10101/#admin) to view the current status of your cluster.  This contains information on each node in the cluster, plus the list of Indexes and Frames.
