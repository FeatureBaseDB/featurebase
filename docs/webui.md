+++
title = "WebUI"
weight = 9
nav = [
    "Console",
    "Cluster Admin",
]
+++

## WebUI

A web-based app called Pilosa WebUI is available in a separate package. This can be used for constructing queries and viewing the cluster status.

### Installation

Releases are [available on Github](https://github.com/pilosa/webui/releases) as well as on [Homebrew](https://brew.sh/) for Mac.

Installing on a Mac with Homebrew is simple; just run:

```
brew install pilosa-webui
```

You may also build from source by checking out the [repo on Github](https://github.com/pilosa/webui) and running:

```
make install
```

### Console

The Console view allows you to enter [PQL](../query-language/) queries and run them against your locally running server. First you must select an Index with the Select index dropdown.

Each query's result will be displayed in the Output section along with the query time. 

The Console will keep a record of each query and its result with the latest query on top.

![webUI console screenshot](/img/docs/webui-console.png)
*WebUI console screenshot*

In addition to standard PQL, the console supports a few special commands, prefixed with `:`.

- `:create index <indexname>`
- `:delete index <indexname>`
- `:use <indexname>`
- `:create field <fieldname>`
- `:delete field <fieldname>`

Field creation also supports options like `timeQuantum`. When creating a new field, add options by using the keys documented in [API reference](../api-reference/#create-field).

- `:create field <fieldname> cacheSize=10000`


### Cluster Admin

Use the Cluster Admin tab to view the current status of your cluster. This contains information on each node in the cluster, plus the list of Indexes and Fields.
