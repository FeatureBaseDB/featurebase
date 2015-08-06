# pilosa

Pilosa is a bitmap index database.


## Getting Started

To download the source, run `go get`:

```sh
$ go get github.com/umbel/pilosa
```

You can update the dependencies to specific versions using [`depman`][depman]:

```sh
$ go get github.com/vube/depman
```

And then run `depman` from the project directory:

```sh
$ cd $GOPATH/src/github.com/umbel/pilosa
$ depman
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
PILOSA_CONFIG=default.yaml pilosa
```

This setup assumes that cassandra and etcd are running locally and that
cassandra keyspace pilosa has been setup as describe in the file
`index/storage_cass.go`.

If you don't want backend storage you can comment out storage_backend in the
yaml file and it will just operate out of memory.

[etcd]: https://github.com/coreos/etcd
[depman]: https://github.com/vube/depman

