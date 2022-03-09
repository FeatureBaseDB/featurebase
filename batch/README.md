# batch

The `batch` package provides a standard tool set for batching records in a way
that is most performant for ingesting those records into FeatureBase. The main
implementation is `Batch` (which can be initated with the `NewBatch()`
function). The `NewBatch()` function takes an `Importer` which contains all of
 the methods required to interact with FeatureBase; these include methods for
 doing string/id translation as well as for importing shards of data.

 IDK uses the `batch` package internally. Another example where the `batch`
 package is used in the `sql3` package. When an "INSERT INTO" statement is
 executed, the SQL engine uses a `Batch` to do key translation and build import
 batches prior to doing the final import.
## Integration tests

To run the tests, you will need to install the following dependencies:

1. [Docker](https://docs.docker.com/install/)
2. [Docker Compose](https://docs.docker.com/compose/install/)

In addition to these dependancies, you will need to be added to the molecula [Gitlab](https://registry.gitlab.com/molecula) account.

First start the test environment. This is a docker-compose environment that includes featurebase.

    make startup

To build and run the integration tests, run:

    make test-run-local

Then to shut down the test environment, run:

    make shutdown

The previous command is equivalent to running the following:

    make startup
    sleep 30 # wait for services to come up
    make test-run
    make shutdown

To run an individual test, you can run the command directly using docker-compose. Note that you must run `docker-compose build batch-test` for docker to run the latest code. Modify the following as needed:

    make startup
    docker-compose build batch-test
    docker-compose run batch-test /usr/local/go/bin/go test -count=1 -mod=vendor -run=TestCmdMainOne .
