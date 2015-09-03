# pilosa

Pilosa is a bitmap index database.


## Getting Started

Pilosa uses the new Go 1.5 vendoring experiment. To enable this, you'll need
to export an environment variable in your `.profile`:

```sh
export GO15VENDOREXPERIMENT=1
```

Then you can download the source by running `go get`:

```sh
$ go get github.com/umbel/pilosa
```

Now you can install the `pilosa` binary:

```sh
$ go install github.com/umbel/pilosa/...
```

Pilosa requires that [etcd][] is running locally for coordinating the cluster:

```sh
# In another terminal window
$ etcd
```

Now run `pilosa` with the default configuration:

```sh
pilosa
```

This setup assumes that cassandra and etcd are running locally and that
cassandra keyspace pilosa has been setup as describe in the file
`index/storage_cass.go`.

If you don't want backend storage you can comment out storage_backend in the
config file and it will just operate out of memory.

[etcd]: https://github.com/coreos/etcd


## Development

### Updating dependencies

To update dependencies, you'll need to install [godep][]:

```sh
$ go get -u github.com/tools/godep
```

Then save the dependencies in your project:

```sh
$ godep save ./...
```

*Make sure you have set the `GO15VENDOREXPERIMENT` environment variable!*

[godep]: https://github.com/tools/godep


### Running tests

Because of a bug in Go 1.5, you'll need to run `make test` to exclude tests
the the `vendor/` directory.


