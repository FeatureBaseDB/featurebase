# Go Client for Pilosa

Go client for Pilosa high performance distributed index.

## Usage

If you have the pilosa repo in your `GOPATH`,
you can import the library in your code using:

```go
import "github.com/pilosa/pilosa/v2/client"
```


### Quick overview

Assuming [Pilosa](https://github.com/pilosa/pilosa) server is running at `localhost:10101` (the default):

```go
package main

import (
	"fmt"

	"github.com/pilosa/pilosa/v2/client"
)

func main() {
	// Create the default client
	cli := client.DefaultClient()

	// Retrieve the schema
	schema, err := cli.Schema()

	// Create an Index object
	myindex := schema.Index("myindex")

	// Create a Field object
	myfield := myindex.Field("myfield")

	// make sure the index and the field exists on the server
	err := cli.SyncSchema(schema)

	// Send a Set query. If err is non-nil, response will be nil.
	response, err := cli.Query(myfield.Set(5, 42))

	// Send a Row query. If err is non-nil, response will be nil.
	response, err = cli.Query(myfield.Row(5))

	// Get the result
	result := response.Result()
	// Act on the result
	if result != nil {
		columns := result.Row().Columns
		fmt.Println("Got columns: ", columns)
	}

	// You can batch queries to improve throughput
	response, err = cli.Query(myindex.BatchQuery(
		myfield.Row(5),
		myfield.Row(10)))
	if err != nil {
		fmt.Println(err)
	}

	for _, result := range response.Results() {
		// Act on the result
		fmt.Println(result.Row().Columns)
	}
}
```

## Documentation

### Data Model and Queries

See: [Data Model and Queries](docs/data-model-queries.md)

### Executing Queries

See: [Server Interaction](docs/server-interaction.md)

### Other Documentation

* [Tracing](docs/tracing.md)
