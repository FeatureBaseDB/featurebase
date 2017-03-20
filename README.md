# pilosa

Pilosa is a bitmap index database.

[![Build Status](https://travis-ci.com/pilosa/pilosa.svg?token=Peb4jvQ3kLbjUEhpU5aR&branch=master)](https://travis-ci.com/pilosa/pilosa)

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
pilosa server
```

## Configuration

Running just `pilosa` will show a list of available subcommands. `pilosa help
<command>` will show usage information and the available flags for the command.

Any flag can be specified at the command line, in an environment variables,
and/or in a toml config file. The environment variable for any flag is that
flag, upper cased, prefixed with `PILOSA_`, and with any dashes converted to
underscores. For example, if you would specify `--cluster.poll-interval=30s` at
the command line, you would set `PILOSA_CLUSTER.POLL_INTERVAL=30s` in the
environment. For the configuration file, a dot in a flag denotes nesting with in
the config file. See the example config file below for examples of this.

You can specify a configuration by setting the `--config` flag when running
`pilosa`.


```sh
pilosa server --config custom-config-file.cfg
```

The config file uses the [TOML](https://github.com/toml-lang/toml) configuration file format,
and should look like:

```
data-dir = "/tmp/pil0"
bind = "127.0.0.1:10101"

[cluster]
  poll-interval = "2m0s"
  replicas = 2
  hosts = [
    "127.0.0.1:10101",
    "127.0.0.1:10102",
  ]

[anti-entropy]
  interval = "10m0s"

[profile]
  cpu = "/home/mycpuprofile"
  cpu-time = "30s"
```

You can generate a template config file with default values with:

```sh
pilosa config
```

The first two configuration options will be unique to each node in the cluster:

`data-dir`: directory in which data is stored to disk

`bind`: IP and port that pilosa will listen on

The remaining configuration options should be the same on every node in the cluster.

`[cluster] replicas`: the number of replicas within the cluster

`[cluster] hosts`: specifies each node within the cluster

`[cluster] poll-interval`: TODO

`[anti-entropy] interval`: TODO

There are also some profiling options for debugging and performance tuning - these don't need to be the same across the cluster and are mostly useful for doing Pilosa development.

`[profile] cpu`: Path at which to store cpu profiling data which will be taken when pilosa starts.

`[profile] cpu-time`: Amount of time for which to collect cpu profiling data at startup.

## Docker

You can create a Pilosa container using `make docker` or equivalently:
```
docker build -t pilosa:latest .
```

You can run a temporary container using:
```
docker run -it --rm --name pilosa -p 10101:10101 pilosa:latest
```

When you click `Ctrl+C` to stop the container, the container and the data in the container will be erased. You can leave out `--rm` flag to keep the data in the container. See [Docker documentation](https://docs.docker.com) for other options.

## Usage

You can interact with Pilosa via HTTP requests to the host:port on which you have Pilosa running.
The following examples illustrate how to do this using `curl` with a Pilosa cluster running on
127.0.0.1 port 10101.

Return the version of Pilosa:
```sh
$ curl "http://127.0.0.1:10101/version"
```

Return a list of all databases and frames in the index:
```sh
$ curl "http://127.0.0.1:10101/schema"
```

### Database and Frame Schema

Before running a query, the corresponding database and frame must be created. Note that database and frame names can contain only lower case letters, numbers, dash (`-`), underscore (`_`) and dot (`.`).

You can create the database `sample-db` using:

```sh
$ curl -XPOST "http://127.0.0.1:10101/db" \
    -d '{"db": "sample-db"}'
```

Optionally, you can specify the column label on database creation:

```sh
$ curl -XPOST "http://127.0.0.1:10101/db" \
    -d '{"db": "sample-db", "columnLabel": "user"}'
```

The frame `collaboration` may be created using the following call:

```sh
$ curl -XPOST "http://127.0.0.1:10101/frame" \
    -d '{"db": "sample-db", "frame": "collaboration"}'
```

It is possible to specify the frame row label on frame creation:

```sh
$ curl -XPOST "http://127.0.0.1:10101/frame" \
    -d '{"db": "sample-db", "frame": "collaboration"}, "options": {"rowLabel": "project"}}'
```

### Queries

Queries to Pilosa require sending a POST request where the query itself is sent as POST data.
You specify the database on which to perform the query with a URL argument `db=database-name`.

In this section, we assume both the database `sample-db` with column label `user` and the frame `collaboration` with row label `project` was created.

A query sent to database `sample-db` will have the following format:

```sh
$ curl -X POST "http://127.0.0.1:10101/query?db=sample-db" -d 'Query()'
```

The `Query()` object referenced above should be made up of one or more of the query types listed below.
So for example, a SetBit() query would look like this:
```sh
$ curl -X POST "http://127.0.0.1:10101/query?db=sample-db" -d 'SetBit(project=10, frame="collaboration", user=1)'
```

Query results have the format `{"results":[]}`, where `results` is a list of results for each `Query()`. This
means that you can provide multiple `Query()` objects with each HTTP request and `results` will contain
the results of all of the queries.

```sh
$ curl -X POST "http://127.0.0.1:10101/query?db=sample-db" -d 'Query() Query() Query()'
```

---
#### SetBit()
```
SetBit(project=10, frame="collaboration", user=1)
```
A return value of `{"results":[true]}` indicates that the bit was toggled from 0 to 1.
A return value of `{"results":[false]}` indicates that the bit was already set to 1 and therefore nothing changed.

SetBit accepts an optional `timestamp` field:
```
SetBit(project=10, frame="collaboration", user=2, timestamp="2016-12-11T10:09:07")
```

---
#### ClearBit()
```
ClearBit(project=10, frame="collaboration", user=1)
```
A return value of `{"results":[true]}` indicates that the bit was toggled from 1 to 0.
A return value of `{"results":[false]}` indicates that the bit was already set to 0 and therefore nothing changed.

---
#### SetBitmapAttrs()
```
SetBitmapAttrs(project=10, frame="collaboration", stars=123, url="http://projects.pilosa.com/10", active=true)
```
Returns `{"results":[null]}`

---
#### SetProfileAttrs()
---
```
SetProfileAttrs(user=10, friends=123, username="mrpi", active=true)
```

Returns `{"results":[null]}`

---
#### Bitmap()
```
Bitmap(project=10, frame="collaboration")
```
Returns `{"results":[{"attrs":{"stars":123, "url":"http://projects.pilosa.com/10", "active":true},"bits":[1,2]}]}` where `attrs` are the
attributes set using `SetBitmapAttrs()` and `bits` are the bits set using `SetBit()`.

In order to return profile attributes attached to the profiles of a bitmap, add `&profiles=true` to the query string. Sample response:
```
{"results":[{"attrs":{},"bits":[10]}],"profiles":[{"user":10,"attrs":{"friends":123, "username":"mrpi", "active":true}}]}
```

---
#### Union()
```
Union(Bitmap(project=10, frame="collaboration"), Bitmap(project=20, frame="collaboration")))
```
Returns a result set similar to that of a `Bitmap()` query, only the `attrs` dictionary will be empty: `{"results":[{"attrs":{},"bits":[1,2]}]}`.
Note that a `Union()` query can be nested within other queries anywhere that you would otherwise provide a `Bitmap()`.

---
#### Intersect()
```
Intersect(Bitmap(project=10, frame="collaboration"), Bitmap(project=20, frame="collaboration")))
```
Returns a result set similar to that of a `Bitmap()` query, only the `attrs` dictionary will be empty: `{"results":[{"attrs":{},"bits":[1]}]}`.
Note that an `Intersect()` query can be nested within other queries anywhere that you would otherwise provide a `Bitmap()`.

---
#### Difference()
```
Difference(Bitmap(project=10, frame="collaboration"), Bitmap(project=20, frame="collaboration")))
```
`Difference()` represents all of the bits that are set in the first `Bitmap()` but are not set in the second `Bitmap()`.  It returns a result set similar to that of a `Bitmap()` query, only the `attrs` dictionary will be empty: `{"results":[{"attrs":{},"bits":[2]}]}`.
Note that a `Difference()` query can be nested within other queries anywhere that you would otherwise provide a `Bitmap()`.

---
#### Count()
```
Count(Bitmap(project=10, frame="collaboration"))
```
Returns the count of the number of bits set in `Bitmap()`: `{"results":[28]}`

---
#### Range()
```
Range(project=10, frame="collaboration", start="1970-01-01T00:00", end="2000-01-02T03:04")
```

---
#### TopN()
```
TopN(frame="geo")
```
Returns all Bitmaps in the cache from frame `geo` sorted by the count of bits.

```
TopN(frame="geo", n=20)
```
Returns the top 20 Bitmaps from frame `geo`.

```
TopN(Bitmap(project=10, frame="collaboration"), frame="geo", n=20)
```
Returns the top 20 Bitmaps from `geo` sorted by the count of bits in the intersection with `Bitmap(project=10)`.

```
TopN(Bitmap(project=10, frame="collaboration"), frame="geo", n=20, field="category", [81,82])
```
Returns the top 20 Bitmaps from `geo`in attribute `category` with values `81 or
82` sorted by the count of bits in the intersection with `Bitmap(project=10)`.

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
