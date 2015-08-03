# pilosa

requires etcd to be running on the local machine
https://github.com/coreos/etcd

in order to build, just clone run depman in the project directory then launch with the following command line

PILOSA_CONFIG=default.yaml ../bin/pilosa-cruncher

That assumes that cassandra and etcd are running locally and that cassandra keyspace pilosa has been setup as describe in the file 
index/storage_cass.go

If you don't want backend storage you can comment out storage_backend in the yaml file and it will just operate out of memory

