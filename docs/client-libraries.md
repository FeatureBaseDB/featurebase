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


### Go

You can find the Go client library for Pilosa at our [Go Pilosa Repository](https://github.com/pilosa/go-pilosa). Check out its [README](https://github.com/pilosa/go-pilosa/blob/master/README.md) for more information and installation instructions.

We are going to use the index you have created in the [Getting Started](../getting-started) section. Before carrying on, make sure that example index is created, sample stargazer data is imported and Pilosa server is running on the default address: `http://localhost:10101`.

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

	// We need to refer to indexes and frames before we can use them in a query.
	repository, _ := schema.Index("repository")
	stargazer, _ := repository.Frame("stargazer")
	language, _ := repository.Frame("language")

	var response *pilosa.QueryResponse

	// Which repositories did user 14 star:
	response, _ = client.Query(stargazer.Bitmap(14))
	fmt.Println("User 14 starred: ", response.Result().Bitmap.Bits)

	// What are the top 5 languages in the sample data?
	response, err = client.Query(language.TopN(5))
	languageIDs := []uint64{}
	for _, item := range response.Result().CountItems {
		languageIDs = append(languageIDs, item.ID)
	}
	fmt.Println("Top 5 languages: ", languageIDs)

	// Which repositories were starred by both user 14 and 19:
	response, _ = client.Query(
		repository.Intersect(
			stargazer.Bitmap(14),
			stargazer.Bitmap(19)))
	fmt.Println("Both user 14 and 19 starred:", response.Result().Bitmap.Bits)

	// Which repositories were starred by user 14 or 19:
	response, _ = client.Query(
		repository.Union(
			stargazer.Bitmap(14),
			stargazer.Bitmap(19)))
	fmt.Println("User 14 or 19 starred:", response.Result().Bitmap.Bits)

	// Which repositories were starred by user 14 or 19 and were written in language 1:
	response, _ = client.Query(
		repository.Intersect(
			repository.Union(
				stargazer.Bitmap(14),
				stargazer.Bitmap(19),
			),
			language.Bitmap(1)))
	fmt.Println("User 14 or 19 starred, written in language 1:", response.Result().Bitmap.Bits)

	// Set user 99999 as a stargazer for repository 77777?
	client.Query(stargazer.SetBit(99999, 77777))
}
```

### Python

You can find the Python client library for Pilosa at our [Python Pilosa Repository](https://github.com/pilosa/python-pilosa). Check out its [README](https://github.com/pilosa/python-pilosa/blob/master/README.md) for more information and installation instructions.

We are going to use the index you have created in the [Getting Started](../getting-started) section. Before carrying on, make sure that example index is created, sample stargazer data is imported and Pilosa server is running on the default address: `http://localhost:10101`.

Error handling has been omitted in the example below for brevity.

```python
from pilosa import Index, Client, PilosaError, TimeQuantum

# We will just use the default client which assumes the server is at http://localhost:10101
client = Client()

# Let's load the schema from the server.
# Note that, for this example the schema should be created beforehand
# and the stargazer data should be imported.
# See the Getting Started repository: https://github.com/pilosa/getting-started/

# Let's create Index and Frame objects, which will contain the settings
# for the corresponding indexes and frames.
try:
    schema = client.schema()
except PilosaError as e:
    # Most calls will raise an exception on errors.
    # You should handle them appropriately.
    # We will just terminate the program in this case.
    raise SystemExit(e)

# We need to refer to indexes and frames before we can use them in a query.
repository = schema.index("repository")
stargazer = repository.frame("stargazer")
language = repository.frame("language")

# Which repositories did user 8 star:
repository_ids = client.query(stargazer.bitmap(14)).result.bitmap.bits
print("User 8 starred: ", repository_ids)

# What are the top 5 languages in the sample data:
top_languages = client.query(language.topn(5)).result.count_items
print("Top 5 languages: ", [item.id for item in top_languages])

# Which repositories were starred by both user 14 and 19:
query = repository.intersect(
    stargazer.bitmap(14),
    stargazer.bitmap(19)
)
mutually_starred = client.query(query).result.bitmap.bits
print("Both user 14 and 19 starred:", mutually_starred)

# Which repositories were starred by user 14 or 19:
query = repository.union(
    stargazer.bitmap(14),
    stargazer.bitmap(19)
)
either_starred = client.query(query).result.bitmap.bits
print("User 14 or 19 starred:", either_starred)

# Which repositories were starred by user 14 or 19 and were written in language 1:
query = repository.intersect(
    repository.union(
        stargazer.bitmap(14),
        stargazer.bitmap(19)
    ),
    language.bitmap(1)
)
mutually_starred = client.query(query).result.bitmap.bits
print("User 14 or 19 starred, written in language 1:", mutually_starred)

# Set user 99999 as a stargazer for repository 77777
client.query(stargazer.setbit(99999, 77777))
```

### Java

You can find the Java client library for Pilosa at our [Java Pilosa Repository](https://github.com/pilosa/java-pilosa). Check out its [README](https://github.com/pilosa/java-pilosa/blob/master/README.md) for more information and installation instructions.

We are going to use the index you have created in the [Getting Started](../getting-started) section. Before carrying on, make sure that example index is created, sample stargazer data is imported and Pilosa server is running on the default address: `http://localhost:10101`.

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

        // We need to refer to indexes and frames before we can use them in a query.
        Index repository = schema.index("repository");
        Frame stargazer = repository.frame("stargazer");
        Frame language = repository.frame("language");

        QueryResponse response;
        QueryResult result;
        PqlQuery query;
        List<Long> repositoryIDs;

        // Which repositories did user 14 star:
        response = client.query(stargazer.bitmap(14));
        repositoryIDs = response.getResult().getBitmap().getBits();
        System.out.println("User 14 starred: " + repositoryIDs);

        // What are the top 5 languages in the sample data:
        response = client.query(language.topN(5));
        List<CountResultItem> top_languages = response.getResult().getCountItems();
        List<Long> languageIDs = new ArrayList<>();
        for (CountResultItem item : top_languages) {
            languageIDs.add(item.getID());
        }

        System.out.println("Top Languages: " +languageIDs);

        // Which repositories were starred by both user 14 and 19:
        query = repository.intersect(
                stargazer.bitmap(14),
                stargazer.bitmap(19)
        );
        response = client.query(query);
        repositoryIDs = response.getResult().getBitmap().getBits();
        System.out.println("Both user 14 and 19 starred: " + repositoryIDs);

        // Which repositories were starred by user 14 or 19:
        query = repository.union(
                stargazer.bitmap(14),
                stargazer.bitmap(19)
        );
        response = client.query(query);
        repositoryIDs = response.getResult().getBitmap().getBits();
        System.out.println("User 14 or 19 starred: " + repositoryIDs);

        // Which repositories were starred by user 14 or 19 and were written in language 1:
        query = repository.intersect(
                repository.union(
                        stargazer.bitmap(14),
                        stargazer.bitmap(19)
                ),
                language.bitmap(1)
        );
        response = client.query(query);
        repositoryIDs = response.getResult().getBitmap().getBits();
        System.out.println("User 14 or 19 starred, written in language 1: " + repositoryIDs);

        // Set user 99999 as a stargazer for repository 77777:
        client.query(stargazer.setBit(99999, 77777));
    }
}
```
