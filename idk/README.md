# idk: Ingest Development Kit

## Integration tests

To run the tests, you will need to install the following dependencies:

1. [Docker](https://docs.docker.com/install/)
2. [Docker Compose](https://docs.docker.com/compose/install/)
3. [Certstrap](https://github.com/square/certstrap)

In addition to these dependancies, you will need to be added to the moleculacorp [Dockerhub](https://hub.docker.com/orgs/moleculacorp) account.

First start the test environment. This is a docker-compose environment that includes pilosa and a confluent kafka stack. Run the following to start those services:

    make startup

To build and run the integration tests, run:

    make test-run

Then to shut down the test environment, run:

    make shutdown

You can run all of the previous commands by calling test-all:

    make test-all

The previous command is equivalent to running the following:

    make startup
    sleep 30 # wait for services to come up
    make test-run
    make shutdown

To run an individual test, you can run the command directly using docker-compose. Note that you must run `docker-compose build idk-test` for docker to run the latest code. Modify the following as needed:

    make startup
    docker-compose build idk-test
    docker-compose run idk-test /usr/local/go/bin/go test -count=1 -mod=vendor -run=TestCmdMainOne ./kafka

To shutdown and reset the environment:

    make clean

## Running dependencies locally (rather than in docker) "make test-local"

This is for running the tests locally and *not* in Docker... so you
have to be running a bunch of stuff natively on your machine.

Run Pilosa with default config:

	pilosa server

Run another pilosa like

	pilosa server --config=pilosa-sec-test.conf

which will run Pilosa with TLS using certs in testenv. (`make testenv` first if you haven't).

You also need to be running the Confluent stack which, after you've
installed it from Confluent's site might look something like:

	export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_66.jdk/Contents/Home
	confluent local destroy && confluent local start schema-registry

Or it might not! You may not need the first line, but if you have the
wrong Java version by default, that's how you set it. The second line
may change depending on what version of the confluent stack you get. According to

	confluent version

I'm running:

	Version:     v0.212.0
	Git Ref:     2b04985

Use the test-local make target:

	make test-local

This sets a number of environment variables (which it prints when you
run it), and should set them correctly if you follow the instructions
above, but if you're running things on non-default ports you may need
to tweak them.

## CSV Ingester

1. make sure you're running Pilosa (localhost:10101 for these instructions)

molecula-consumer-csv --primary-key-fields=asset_tag -i sample-index --files sample.csv

    asset_tag__String,fan_time__RecordTime_2006-01-02,fan_val__String_F_YMD
    ABCD,2019-01-02,70%
    ABCD,2019-01-03,20%
    ABCD,2019-01-04,30%
    BEDF,2019-01-02,70%
    BEDF,2019-01-05,90%
    BEDF,2019-01-08,10%
    BEDF,2019-01-08,20%
    ABCD,2019-01-30,40%

## Datagen
Datagen is an internal command-line tool to generate various application-specific datasets, and ingest them directly into Pilosa. After running `make install`, run `datagen` with no arguments to see a list of available "sources".

When running Datagen with local Docker stacks, make sure to add individual docker stacks to the `/etc/hosts` file:

    127.0.0.1 kafka
    127.0.0.1 pilosa
    127.0.0.1 <docker stack>

## ODBC Support

By default, the SQL ingester is not built with ODBC support.
This is because it uses CGO with extra dependencies, and the resulting binaries are not portable.
In order to build with ODBC support, it is necessary to install `unixODBC` to the system.
Then run:
```
make bin/molecula-consumer-sql-odbc
```

Different Linux distros will store certain libraries in different locations.
ODBC uses dynamic library loading to handle drivers, so full static linking of dependencies is not possible.
It is therefore necessary to build on a system with the same distro and the same software versions as the target machine.
