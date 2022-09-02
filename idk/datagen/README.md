# Datagen Tool

## Help Usage

```sh
Usage of datagen:
  -c, --concurrency int             Number of concurrent sources and indexing routines to launch. (default 1)
      --dry-run                     Dry run - just flag parsing.
  -e, --end-at uint                 ID at which to stop generating records.
      --kafka.batch-size int        Number of records to generate before sending them to Kafka all at once. Generally, larger means better throughput and more memory usage. (default 1000)
      --kafka.hosts strings         Comma separated list of host:port pairs for Kafka. (default [])
      --kafka.registry-url string   Location of Confluent Schema Registry. Must start with 'https://' if you want to use TLS.
      --kafka.subject string        Kafka schema subject.
      --kafka.topic string          Kafka topic to post to.
      --pilosa.batch-size int       Number of records to read before indexing all of them at once. Generally, larger means better throughput and more memory usage. 1,048,576 might be a good number.
      --pilosa.cache-length uint    Number of batches of ID mappings to cache. (default 64)
      --pilosa.hosts strings        Comma separated list of host:port pairs for Pilosa. (default [])
      --pilosa.index string         Name of Pilosa index.
      --seed int                    Seed to use for any random number generation.
  -s, --source string               Source generator type. Running datagen with no arguments will list the available source types.
  -b, --start-from uint             ID at which to start generating records.
  -t, --target string               Destination for the generated data: [kafka, pilosa]. (default "pilosa")
      --track-progress              Periodically print status updates on how many records have been sourced.
```

## Example Usage

The following command will create 100 records in Pilosa index (starting at ID 0 and ending at ID 99)
in the `equipment` index using the `equipment` data generator.

```sh
datagen --source=equipment --pilosa.index=equipment --end-at=99
```

## Adding New Sources

TODO: redo README (or delete?)

If you're looking to add a new Source to datagen, the best thing to do is use the special "custom" datagen source (`datagen --source=custom --custom-config=somefile.yaml`) and write a `somefile.yaml` which describes the data you want to generate. An example can be found in `datagen/testdata/custom.yaml`, and there are some more in the molecula/technical-validation repo.
