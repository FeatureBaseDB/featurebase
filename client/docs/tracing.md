# Tracing

Pilosa client supports distributed tracing via the [OpenTracing](https://opentracing.io/) API.

In order to use a tracer with Go-Pilosa, you should:
1. Create the tracer,
2. Pass the `OptClientOption(tracer)` to `NewClient`.

In this document, we will be using the [Jaeger](https://www.jaegertracing.io) tracer, but OpenTracing has support for [other tracing systems](https://opentracing.io/docs/supported-tracers/).

## Running the Pilosa Server

Let's run a temporary Pilosa container:

    $ docker run -it --rm -p 10101:10101 pilosa/pilosa:v1.2.0

Check that you can access Pilosa:

    $ curl localhost:10101
    Welcome. Pilosa is running. Visit https://www.pilosa.com/docs/ for more information.

## Running the Jaeger Server

Let's run a Jaeger Server container:

    $ docker run -it --rm -p 5775:5775/udp -p 16686:16686 jaegertracing/all-in-one:latest
    ...<title>Jaeger UI</title>...

## Writing the Sample Code

The sample code depdends on the Jaeger Go client, so let's install it first:

    $ go get -u github.com/uber/jaeger-client-go/

Save the following sample code as `gopilosa-tracing.go`:
```go
package main

import (
	"log"
	"time"

	"github.com/pilosa/pilosa/v2/client"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

func main() {
	// Create the tracer.
	cfg := config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans:            true,
			BufferFlushInterval: 1 * time.Second,
			// Jaeger Server address
			LocalAgentHostPort:  "127.0.0.1:5775",
		},
	}
	tracer, closer, err := cfg.New(
		"go_pilosa_test",
		config.Logger(jaeger.StdLogger),
	)

	// Don't forget to close the tracer.
	defer closer.Close()

	// Create the client, and pass the tracer.
	cli, err := client.NewClient(":10101", pilosa.OptClientTracer(tracer))
	if err != nil {
		log.Fatal(err)
	}

	// Read the schema from the server.
	// This should create a trace on the Jaeger server.
	schema, err := cli.Schema()
	if err != nil {
		log.Fatal(err)
	}

	// Create and sync the sample schema.
	// This should create a trace on the Jaeger server.
	myIndex := schema.Index("my-index")
	myField := myIndex.Field("my-field")
	err = cli.SyncSchema(schema)
	if err != nil {
		log.Fatal(err)
	}

	// Run a query on Pilosa.
	// This should create a trace on the Jaeger server.
	_, err = cli.Query(myField.Set(1, 1000))
	if err != nil {
		log.Fatal(err)
	}
}
```

## Checking the Tracing Data

Run the sample code:

    $ go run gopilosa-tracing.go


* Open http://localhost:16686 in your web browser to visit Jaeger UI.
* Click on the *Search* tab and select `go_pilosa_test` in the *Service* dropdown on the right.
* Click on *Find Traces* button at the bottom left.
* You should see a couple of traces, such as: `Client.Query`, `Client.CreateField`, `Client.Schema`, etc.
