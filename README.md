# pilosa

Pilosa is a bitmap index database.


## Getting Started

Pilosa requires Go 1.7 or greater.

You can download the source by running `go get`:

```sh
$ go get github.com/pilosa/pilosa
```

Now you can install the `pilosa` binary:

```sh
$ go install github.com/pilosa/pilosa/cmd/...
```

Now run a single pilosa node with the default configuration:

```sh
pilosa
```

If you would like to quickly create a multi-node pilosa cluster, see the `pilosactl create` documentation.

## Configuration

You can specify a configuration by setting the `-config` flag when running `pilosa`.


```sh
pilosa -config custom-config-file.cfg
```

The config file uses the [TOML](https://github.com/toml-lang/toml) configuration file format,
and should look like:

```
data-dir = "/tmp/pil0"
host = "127.0.0.1:15000"

[cluster]
replicas = 2

[[cluster.node]]
host = "127.0.0.1:15000"

[[cluster.node]]
host = "127.0.0.1:15001"
```

The first two configuration options will be unique to each node in the cluster:

`data-dir`: directory in which data is stored to disk

`host`: IP and port of the pilosa node

The remaining configuration options should be the same on every node in the cluster.

`replicas`: the number of replicas within the cluster

`[[cluster.node]]`: specifies each node within the cluster

## Usage

You can interact with Pilosa via HTTP requests to the host:port on which you have Pilosa running.
The following examples illustrate how to do this using `curl` with a Pilosa cluster running on
127.0.0.1 port 15000.

Return the version of Pilosa:
```sh
$ curl "http://127.0.0.1:15000/version"
```

Return a list of all databases and frames in the index:
```sh
$ curl "http://127.0.0.1:15000/schema"
```


### Queries

Queries to Pilosa require sending a POST request where the query itself is sent as POST data.
You specify the database on which to perform the query with a URL argument `db=database-name`.

A query sent to database `exampleDB` will have the following format:

```sh
$ curl -X POST "http://127.0.0.1:15000/query?db=exampleDB" -d 'Query()'
```

The `Query()` object referenced above should be made up of one or more of the query types listed below.
So for example, a SetBit() query would look like this:
```sh
$ curl -X POST "http://127.0.0.1:15000/query?db=exampleDB" -d 'SetBit(id=10, frame="foo", profileID=1)'
```

Query results have the format `{"results":[]}`, where `results` is a list of results for each `Query()`. This
means that you can provide multiple `Query()` objects with each HTTP request and `results` will contain
the results of all of the queries.

```sh
$ curl -X POST "http://127.0.0.1:15000/query?db=exampleDB" -d 'Query() Query() Query()'
```

---
#### SetBit()
```
SetBit(id=10, frame="foo", profileID=1)
```
A return value of `{"results":[true]}` indicates that the bit was toggled from 0 to 1.
A return value of `{"results":[false]}` indicates that the bit was already set to 1 and therefore nothing changed.

SetBit accepts an optional `timestamp` field:
```
SetBit(id=10, frame=f, profileID=2, timestamp="2016-12-11T10:09:07")
```

---
#### ClearBit()
```
ClearBit(id=10, frame="foo", profileID=1)
```
A return value of `{"results":[true]}` indicates that the bit was toggled from 1 to 0.
A return value of `{"results":[false]}` indicates that the bit was already set to 0 and therefore nothing changed.

---
#### SetBitmapAttrs()
```
SetBitmapAttrs(id=10, frame="foo", category=123, color="blue", happy=true)
```
Returns `{"results":[null]}`

---
#### SetProfileAttrs()
---
```
SetProfileAttrs(id=10, category=123, color="blue", happy=true)
```

Returns `{"results":[null]}`

---
#### Bitmap()
```
Bitmap(id=10, frame="foo")
```
Returns `{"results":[{"attrs":{"category":123,"color":"blue","happy":true},"bits":[1,2]}]}` where `attrs` are the
attributes set using `SetBitmapAttrs()` and `bits` are the bits set using `SetBit()`.

In order to return profile attributes attached to the profiles of a bitmap, add `&profiles=true` to the query string. Sample response:
```
{"results":[{"attrs":{},"bits":[10]}],"profiles":[{"id":10,"attrs":{"category":123,"color":"blue","happy":true}}]}
```

---
#### Union()
```
Union(Bitmap(id=10, frame="foo"), Bitmap(id=20, frame="foo")))
```
Returns a result set similar to that of a `Bitmap()` query, only the `attrs` dictionary will be empty: `{"results":[{"attrs":{},"bits":[1,2]}]}`.
Note that a `Union()` query can be nested within other queries anywhere that you would otherwise provide a `Bitmap()`.

---
#### Intersect()
```
Intersect(Bitmap(id=10, frame="foo"), Bitmap(id=20, frame="foo")))
```
Returns a result set similar to that of a `Bitmap()` query, only the `attrs` dictionary will be empty: `{"results":[{"attrs":{},"bits":[1]}]}`.
Note that an `Intersect()` query can be nested within other queries anywhere that you would otherwise provide a `Bitmap()`.

---
#### Difference()
```
Difference(Bitmap(id=10, frame="foo"), Bitmap(id=20, frame="foo")))
```
`Difference()` represents all of the bits that are set in the first `Bitmap()` but are not set in the second `Bitmap()`.  It returns a result set similar to that of a `Bitmap()` query, only the `attrs` dictionary will be empty: `{"results":[{"attrs":{},"bits":[2]}]}`.
Note that a `Difference()` query can be nested within other queries anywhere that you would otherwise provide a `Bitmap()`.

---
#### Count()
```
Count(Bitmap(id=10, frame="foo"))
```
Returns the count of the number of bits set in `Bitmap()`: `{"results":[28]}`

---
#### Range()
```
Range(id=10, frame="foo", start="1970-01-01T00:00", end="2000-01-02T03:04")
```

---
#### TopN()
```
TopN(frame="bar", n=20)
```
Returns the top 20 Bitmaps from frame `bar`.

```
TopN(Bitmap(id=10, frame="foo"), frame="bar", n=20)
```
Returns the top 20 Bitmaps from `bar` sorted by the count of bits in the intersection with `Bitmap(id=10)`.


```
TopN(Bitmap(id=10, frame="foo"), frame="bar", n=20, field="category", [81,82])
```

Returns the top 20 Bitmaps from `bar`in attribute `category` with values `81 or
82` sorted by the count of bits in the intersection with `Bitmap(id=10)`.

## Development

### Updating dependencies

To update dependencies, you'll need to install [Glide][].

Then add the new dependencies in your project:

```sh
$ glide get github.com/foo/bar
```

### Protobuf

If you update protobuf (pilosa/internal/internal.proto), then you need to run `go generate`
```sh
$ go generate
```

### Version

In order to set the version number, compile Pilosa with the following argument:
```sh
$ go install --ldflags="-X main.Version=1.0.0"
```

[Glide]: http://glide.sh/

## Pilosactl

Pilosactl contains a suite of tools for interacting with pilosa. Run `pilosactl` for an overview of commands, and `pilosactl <command> -h` for specific information on that command.

### Create

`pilosactl create` is used to create pilosa clusters. It has a number of options for controlling how the cluster is configured, what hosts it is on, and even the ability to build the pilosa binary locally and copy it to each cluster node automatically. To start pilosa on remote hosts, you only need `ssh` access to those hosts. See `pilosactl create -h` for a full list of options.

Examples:

Create a 5 node cluster locally (using 5 different ports), with a replication factor of 2.
```
pilosactl create \
  -serverN 5 \
  -replicaN 2
```

Create a cluster on 3 remote hosts - all logs will come to local stderr, pilosa binary must be available on remote hosts. The ssh user on the remote hosts needs to be the same as your local user. Otherwise use the `ssh-user` option.
```
pilosactl create \
  -hosts="node1.example.com:15000,node2.example.com:15000,node3.example.com:15000"
```

Create a cluster on 3 remote hosts running OSX, but build the binary locally and copy it up. Stream the stderr of each node to a separate local log file.
```
pilosactl create \
  -hosts="mac1.example.com:15000,mac2.example.com:15000,mac3.example.com:15000" \
  -copy-binary \
  -goos=darwin \
  -goarch=amd64 \
  -log-file-prefix=clusterlogs
```

### Bagent

`pilosactl bagent` is what you want if you just want to run a simple benchmark against an existing cluster. Running it with no arguments will print some help, including the set of subcommands that it may be passed. Calling a subcommand with `-h'` will print the options for that subcommand. The `agent-num` flag can be passed an integer which can change the behavior the benchmarks that are run. This is useful when multiple invocations of the same benchmark are made by the `bspawn` command - they can each (for example) set different bits even though they all have the same arguments.

E.G.
```
pilosactl bagent \
  -hosts="localhost:15000,localhost:15001" \
  import -h
```

Multiple subcommands and their arguments may be concatenated at the command line and they will be run serially. This is useful (i.e.) for importing a bunch of data, and then executing queries against it.

This will generate and import a bunch of data, and then execute random queries against it.

```
pilosactl bagent \
  -hosts="localhost:15000,localhost:15001" \
  import -max-bits-per-map=10000 \
  random-query -iterations 100
```

### Bspawn
`pilosactl bspawn` allows you to automate the creation of clusters and the running of complex benchmarks which span multiple benchmark agents against them. It has a number of options which are described by `pilosactl bspawn` with no arguments, and also takes a config file which describes the Benchmark itself - this file is described below.

#### Configuration Format

The configuration file is a json object with the top level key `benchmarks`. This contains a list of objects each of which represents a `bagent` command (the `args` key) that will be run some number of times concurrently (the `num` key), and a `name` which should describe the overall effect that command. An example is below.
```json
{
    "benchmarks": [
        {
            "num": 3,
            "name": "set-diags",
            "args": ["diagonal-set-bits", "-iterations", "30000", "-client-type", "round_robin"]
        },
        {
            "num": 2,
            "name": "rand-plus-zipf",
            "args": ["random-set-bits", "-iterations", "20000", "zipf", "-iterations", "100"]
        }
    ]
}
```

All of the benchmarks, and agents are run concurrently. Each agent will be passed an `agent-num` which can modify the behavior in a way that is benchmark specific. See the documentation for each benchmark to see how `agent-num` changes its behavior.
