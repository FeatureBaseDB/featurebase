+++
title = "Client Libraries"
weight = 12
nav = [
    "Go",
    "Python",
    "Java",
]
+++

## Client Libraries

This section contains example code for client libraries in several languages. Please remember that when modeling your data in Pilosa, it is best to keep row and column ids sequential. It is best to avoid using the output of a hash or randomly distributed ids with Pilosa.

### Go

You can find the Go client library for Pilosa at our [Go Pilosa Repository](https://github.com/pilosa/go-pilosa). Check out its [README](https://github.com/pilosa/go-pilosa/blob/master/README.md) for more information and installation instructions.

We are going to use the index you have created in the [Getting Started](../getting-started/) section. Before carrying on, make sure that example index is created, sample stargazer data is imported and Pilosa server is running on the default address: `http://localhost:10101`.

Error handling has been omitted in the example below for brevity.

```go
package main

import (
	"fmt"

	"github.com/pilosa/go-pilosa"
)

func main() {
	// We will just use the default client which assumes the server is at http://localhost:10101
	client := pilosa.DefaultClient()

	// Let's load the schema from the server.
	// Note that, for this example the schema should be created beforehand
	// and the stargazer data should be imported.
	// See the Getting Started repository: https://github.com/pilosa/getting-started/
	schema, err := client.Schema()
	if err != nil {
		// Most calls will return an error value.
		// You should handle them appropriately.
		// We will just terminate the program in this case.
		// Error handling was left out for brevity in the rest of the code.
		panic(err)
	}

	// We need to refer to indexes and fields before we can use them in a query.
	repository := schema.Index("repository")
	stargazer := repository.Field("stargazer")
	language := repository.Field("language")

	var response *pilosa.QueryResponse

	// Which repositories did user 14 star:
	response, _ = client.Query(stargazer.Row(14))
	fmt.Println("User 14 starred: ", response.Result().Row().Columns)

	// What are the top 5 languages in the sample data?
	response, err = client.Query(language.TopN(5))
	languageIDs := []uint64{}
	for _, item := range response.Result().CountItems() {
		languageIDs = append(languageIDs, item.ID)
	}
	fmt.Println("Top 5 languages: ", languageIDs)

	// Which repositories were starred by both user 14 and 19:
	response, _ = client.Query(
		repository.Intersect(
			stargazer.Row(14),
			stargazer.Row(19)))
	fmt.Println("Both user 14 and 19 starred:", response.Result().Row().Columns)

	// Which repositories were starred by user 14 or 19:
	response, _ = client.Query(
		repository.Union(
			stargazer.Row(14),
			stargazer.Row(19)))
	fmt.Println("User 14 or 19 starred:", response.Result().Row().Columns)

	// Which repositories were starred by user 14 or 19 and were written in language 1:
	response, _ = client.Query(
		repository.Intersect(
			repository.Union(
				stargazer.Row(14),
				stargazer.Row(19),
			),
			language.Row(1)))
	fmt.Println("User 14 or 19 starred, written in language 1:", response.Result().Row().Columns)

	// Set user 99999 as a stargazer for repository 77777?
	client.Query(stargazer.Set(99999, 77777))
}
```

Running the above program should produce output like this:
```
User 14 starred:  [1 2 3 362 368 391 396 409 416 430 436 450 454 460 461 464 466 469 470 483 484 486 490 491 503 504 514]
Top 5 languages:  [5 1 4 9 13]
Both user 14 and 19 starred: [2 3 362 396 416 461 464 466 470 486]
User 14 or 19 starred: [1 2 3 361 362 368 376 377 378 382 386 388 391 396 398 400 409 411 412 416 426 428 430 435 436 450 452 453 454 456 460 461 464 465 466 469 470 483 484 486 487 489 490 491 500 503 504 505 512 514]
User 14 or 19 starred, written in language 1: [1 2 362 368 382 386 416 426 435 456 461 483 500 503 504 514]
```

### Python

You can find the Python client library for Pilosa at our [Python Pilosa Repository](https://github.com/pilosa/python-pilosa). Check out its [README](https://github.com/pilosa/python-pilosa/blob/master/README.md) or [readthedocs](https://pilosa.readthedocs.io/en/latest/) for more information and installation instructions.

We are going to use the index you have created in the [Getting Started](../getting-started/) section. Before carrying on, make sure that example index is created, sample stargazer data is imported and Pilosa server is running on the default address: `http://localhost:10101`.

Error handling has been omitted in the example below for brevity.

```python
from __future__ import print_function
from pilosa import Index, Client, PilosaError, TimeQuantum

# We will just use the default client which assumes the server is at http://localhost:10101
client = Client()

# Let's load the schema from the server.
# Note that, for this example the schema should be created beforehand
# and the stargazer data should be imported.
# See the Getting Started repository: https://github.com/pilosa/getting-started/

# Let's create Index and Field objects, which will contain the settings
# for the corresponding indexes and fields.
try:
    schema = client.schema()
except PilosaError as e:
    # Most calls will raise an exception on errors.
    # You should handle them appropriately.
    # We will just terminate the program in this case.
    raise SystemExit(e)

# We need to refer to indexes and fields before we can use them in a query.
repository = schema.index("repository")
stargazer = repository.field("stargazer")
language = repository.field("language")

# Which repositories did user 8 star:
repository_ids = client.query(stargazer.row(14)).result.row.columns
print("User 8 starred: ", repository_ids)

# What are the top 5 languages in the sample data:
top_languages = client.query(language.topn(5)).result.count_items
print("Top 5 languages: ", [item.id for item in top_languages])

# Which repositories were starred by both user 14 and 19:
query = repository.intersect(
    stargazer.row(14),
    stargazer.row(19)
)
mutually_starred = client.query(query).result.row.columns
print("Both user 14 and 19 starred:", mutually_starred)

# Which repositories were starred by user 14 or 19:
query = repository.union(
    stargazer.row(14),
    stargazer.row(19)
)
either_starred = client.query(query).result.row.columns
print("User 14 or 19 starred:", either_starred)

# Which repositories were starred by user 14 or 19 and were written in language 1:
query = repository.intersect(
    repository.union(
        stargazer.row(14),
        stargazer.row(19)
    ),
    language.row(1)
)
mutually_starred = client.query(query).result.row.columns
print("User 14 or 19 starred, written in language 1:", mutually_starred)

# Set user 99999 as a stargazer for repository 77777
client.query(stargazer.set(99999, 77777))
```

Running the above program should produce output like this:
```
('User 8 starred: ', [1L, 2L, 3L, 362L, 368L, 391L, 396L, 409L, 416L, 430L, 436L, 450L, 454L, 460L, 461L, 464L, 466L, 469L, 470L, 483L, 484L, 486L, 490L, 491L, 503L, 504L, 514L])
('Top 5 languages: ', [5L, 1L, 4L, 9L, 13L])
('Both user 14 and 19 starred:', [2L, 3L, 362L, 396L, 416L, 461L, 464L, 466L, 470L, 486L])
('User 14 or 19 starred:', [1L, 2L, 3L, 361L, 362L, 368L, 376L, 377L, 378L, 382L, 386L, 388L, 391L, 396L, 398L, 400L, 409L, 411L, 412L, 416L, 426L, 428L, 430L, 435L, 436L, 450L, 452L, 453L, 454L, 456L, 460L, 461L, 464L, 465L, 466L, 469L, 470L, 483L, 484L, 486L, 487L, 489L, 490L, 491L, 500L, 503L, 504L, 505L, 512L, 514L])
('User 14 or 19 starred, written in language 1:', [1L, 2L, 362L, 368L, 382L, 386L, 416L, 426L, 435L, 456L, 461L, 483L, 500L, 503L, 504L, 514L])
```

### Java

You can find the Java client library for Pilosa at our [Java Pilosa Repository](https://github.com/pilosa/java-pilosa). Check out its [README](https://github.com/pilosa/java-pilosa/blob/master/README.md) for more information and installation instructions.

We are going to use the index you have created in the [Getting Started](../getting-started/) section. Before carrying on, make sure that example index is created, sample stargazer data is imported and Pilosa server is running on the default address: `http://localhost:10101`.

Error handling has been omitted in the example below for brevity.

```java
import com.pilosa.client.*;
import com.pilosa.client.orm.*;
import com.pilosa.client.exceptions.PilosaException;

import java.util.ArrayList;
import java.util.List;

public class StarTrace {
    public static void main(String[] args) {
        // We will just use the default client which assumes the server is at http://localhost:10101
        PilosaClient client = PilosaClient.defaultClient();

        // Let's load the schema from the server.
        Schema schema;
        try {
            schema = client.readSchema();
        }
        catch (PilosaException ex) {
            // Most calls will return an error value.
            // You should handle them appropriately.
            // We will just terminate the program in this case.
            throw new RuntimeException(ex);
        }

        // We need to refer to indexes and fields before we can use them in a query.
        Index repository = schema.index("repository");
        Field stargazer = repository.field("stargazer");
        Field language = repository.field("language");

        QueryResponse response;
        QueryResult result;
        PqlQuery query;
        List<Long> repositoryIDs;

        // Which repositories did user 14 star:
        response = client.query(stargazer.row(14));
        repositoryIDs = response.getResult().getRow().getColumns();
        System.out.println("User 14 starred: " + repositoryIDs);

        // What are the top 5 languages in the sample data:
        response = client.query(language.topN(5));
        List<CountResultItem> top_languages = response.getResult().getCountItems();
        List<Long> languageIDs = new ArrayList<Long>();
        for (CountResultItem item : top_languages) {
            languageIDs.add(item.getID());
        }

        System.out.println("Top Languages: " +languageIDs);

        // Which repositories were starred by both user 14 and 19:
        query = repository.intersect(
                stargazer.row(14),
                stargazer.row(19)
        );
        response = client.query(query);
        repositoryIDs = response.getResult().getRow().getColumns();
        System.out.println("Both user 14 and 19 starred: " + repositoryIDs);

        // Which repositories were starred by user 14 or 19:
        query = repository.union(
                stargazer.row(14),
                stargazer.row(19)
        );
        response = client.query(query);
        repositoryIDs = response.getResult().getRow().getColumns();
        System.out.println("User 14 or 19 starred: " + repositoryIDs);

        // Which repositories were starred by user 14 or 19 and were written in language 1:
        query = repository.intersect(
                repository.union(
                        stargazer.row(14),
                        stargazer.row(19)
                ),
                language.row(1)
        );
        response = client.query(query);
        repositoryIDs = response.getResult().getRow().getColumns();
        System.out.println("User 14 or 19 starred, written in language 1: " + repositoryIDs);

        // Set user 99999 as a stargazer for repository 77777:
        client.query(stargazer.set(99999, 77777));
    }
}
```

Running the above program should produce output like this:
```
User 14 starred: [1, 2, 3, 362, 368, 391, 396, 409, 416, 430, 436, 450, 454, 460, 461, 464, 466, 469, 470, 483, 484, 486, 490, 491, 503, 504, 514]
Top Languages: [5, 1, 4, 9, 13]
Both user 14 and 19 starred: [2, 3, 362, 396, 416, 461, 464, 466, 470, 486]
User 14 or 19 starred: [1, 2, 3, 361, 362, 368, 376, 377, 378, 382, 386, 388, 391, 396, 398, 400, 409, 411, 412, 416, 426, 428, 430, 435, 436, 450, 452, 453, 454, 456, 460, 461, 464, 465, 466, 469, 470, 483, 484, 486, 487, 489, 490, 491, 500, 503, 504, 505, 512, 514]
User 14 or 19 starred, written in language 1: [1, 2, 362, 368, 382, 386, 416, 426, 435, 456, 461, 483, 500, 503, 504, 514]
```
