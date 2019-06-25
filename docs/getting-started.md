+++
title = "Getting Started"
weight = 3
nav = [
     "Starting Pilosa",
     "Sample Project",
     "What's Next?",
]
+++

## Getting Started

Pilosa supports an HTTP interface which uses JSON by default.
Any HTTP tool can be used to interact with the Pilosa server. The examples in this documentation will use [curl](https://curl.haxx.se/) which is available by default on many UNIX-like systems including Linux and MacOS. Windows users can download curl [here](https://curl.haxx.se/download.html).

<div class="note">
    <p>Note that Pilosa server requires a high limit for open files. Check the documentation of your system to see how to increase it in case you hit that limit. See <a href="/docs/administration/#open-file-limits">Open File Limits</a> for more details.</p>
</div>

### Starting Pilosa

Follow the steps in the [Installation](../installation/) document to install Pilosa.
Execute the following in a terminal to run Pilosa with the default configuration (Pilosa will be available at [localhost:10101](http://localhost:10101)):
```
pilosa server
```
If you are using the Docker image, you can run an ephemeral Pilosa container on the default address using the following command:
```
docker run -it --rm --name pilosa -p 10101:10101 pilosa/pilosa:latest
```

Let's make sure Pilosa is running:
``` request
curl localhost:10101/status
```
``` response
{"state":"NORMAL","nodes":[{"id":"91715a50-7d50-4c54-9a03-873801da1cd1","uri":{"scheme":"http","host":"localhost","port
":10101},"isCoordinator":true}],"localID":"91715a50-7d50-4c54-9a03-873801da1cd1"}
```

### Sample Project

In order to better understand Pilosa's capabilities, we will create a sample project called "Star Trace" containing information about 1,000 popular Github repositories which have "go" in their name. The Star Trace index will include data points such as programming language, tags, and stargazers—people who have starred a project.

Although Pilosa doesn't keep the data in a tabular format, we still use the terms "columns" and "rows" when describing the data model. We put the primary objects in columns, and the properties of those objects in rows. For example, the Star Trace project will contain an index called "repository" which contains columns representing Github repositories, and rows representing properties like programming languages and tags. We can better organize the rows by grouping them into sets called Fields. So the "repository" index might have a "languages" field as well as a "tags" field. You can learn more about indexes and fields in the [Data Model](../data-model/) section of the documentation.

#### Create the Environment

While we can create indexes and query directly in the terminal, it is more conventional to do so in a client library. Pilosa supports Go, Java, and Python, though you will have to install the library for compatibility.

For Go users, open a terminal (one other than the one running pilosa) and download the library in your `GOPATH` using:
``` 
go get github.com/pilosa/go-pilosa
```

For Java users, add the following dependency in your `pom.xml`:
```
<dependencies>
    <dependency>
        <groupId>com.pilosa</groupId>
        <artifactId>pilosa-client</artifactId>
        <version>1.3.1</version>
    </dependency>
</dependencies>
```

For Python users, open a terminal (one other than the one running pilosa) and install the library using:
```
pip install pilosa
```

For simplicity, we reccomend that you create a separate folder for this project. In the terminal, create a new folder as follows:
```
mkdir GettingStarted
cd GettingStarted
```

In this folder, we will download two CSV files to provide data to our fields later on. Download the stargazer.csv and language.csv files here:
```
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/stargazer.csv
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/language.csv
```

We will also create a file called StarTrace.go (for Go users), StarTrace.java (for Java users), or StarTrace.py (for Python users) as follows:
```
touch StarTrace.go
```
This file will be used in the following section.

#### Create the Schema

Note:
If at any time you want to verify the data structure, you can request the schema as follows:

``` request
curl localhost:10101/schema
```
``` response
{"indexes":null}
```
##### Go Users

Before we can import data or run queries, we need to create our indexes and the fields within them. Let's create the repository index first. Copy the following into the StarTrace.go file:
``` 
package main

import (
	"bytes"
	"fmt"
	"github.com/pilosa/go-pilosa"
	"github.com/pilosa/go-pilosa/csv"
	"io/ioutil"
	"log"
)

func main() {
	// Create the Schema
	client := pilosa.DefaultClient()
	schema, _ := client.Schema()
	repository := schema.Index("repository")
	// This is where the field will go later
	err := client.SyncSchema(schema)
	if err != nil {
		log.Fatal(err)
	}
}
```
The index name must be 64 characters or less, start with a letter, and consist only of lowercase alphanumeric characters or `_-`. The same goes for field names.

Let's create the `stargazer` field which has user IDs of stargazers as its rows:
```
	stargazer := repository.Field("stargazer")
```

Next up is the `language` field, which will contain IDs for programming languages:
```
	language := repository.Field("language")
```

Your StarTrace.go file should look like:
```
package main

import (
	"bytes"
	"fmt"
	"github.com/pilosa/go-pilosa"
	"github.com/pilosa/go-pilosa/csv"
	"io/ioutil"
	"log"
)

func main() {
	// Create the Schema
	client := pilosa.DefaultClient()
	schema, _ := client.Schema()
	repository := schema.Index("repository")
	stargazer := repository.Field("stargazer")
	language := repository.Field("language")
	err := client.SyncSchema(schema)
	if err != nil {
		log.Fatal(err)
	}
}
```

##### Java and Python Users

<div class="note">
    <p>Java and Python support will be uploaded shortly.
</div>

#### Import Data From CSV Files

Now that we have our index and our fields, we can import the data we downloaded earlier and soon be making our own queries.

##### Go Users

First, we will load our data into the `stargazer` field:
```
	stargazerFile, err := ioutil.ReadFile("stargazer.csv")
	if err != nil {
		log.Fatal(err)
	}
	format := "2006-01-02T15:04"
	iterator = csv.NewColumnIteratorWithTimestampFormat(csv.RowIDColumnID, bytes.NewReader(stargazerFile), format)
	err = client.ImportField(stargazer, iterator)
	if err != nil {
		log.Fatal(err)
	}
```
Since our `stargazer` data contains time stamps, which represent the time users starred repos, we will be using the `csv.NewColumnIterator` function that is built into the go-pilosa import. For more information on imports in go-pilosa, please see the go-pilosa [site](https://github.com/pilosa/go-pilosa/blob/master/docs/imports-exports.md). Time quantum is the resolution of the time we want to use and is defined by the `format` variable.

Next, we will load our data into the `language` field:
```
	languageFile, err := ioutil.ReadFile("language.csv")
	if err != nil {
		log.Fatal(err)
	}
	iterator := csv.NewColumnIterator(csv.RowIDColumnID, bytes.NewReader(languageFile))
	err = client.ImportField(language, iterator)
	if err != nil {
		log.Fatal(err)
	}
```
The `language` is a `set` field, but since the default field type is `set`, we didn't need to specify it.

##### Java and Python Users

<div class="note">
    <p>Java and Python support will be uploaded shortly.
</div>

<div class="note">
    <p>If you are using a Docker container for Pilosa (with name `pilosa`), you should instead copy the `*.csv` file into the container and then import them:
```
docker cp stargazer.csv pilosa:/stargazer.csv
docker exec -it pilosa /pilosa import -i repository -f stargazer /stargazer.csv
docker cp language.csv pilosa:/language.csv
docker exec -it pilosa /pilosa import -i repository -f language /language.csv
```
</div>

Note that both the user IDs and the repository IDs were remapped to sequential integers in the data files, they don't correspond to actual Github IDs anymore. You can check out [languages.txt](https://github.com/pilosa/getting-started/blob/master/languages.txt) to see the mapping for languages.

#### Make Some Queries

Now that we have a working schema, we can query it.

##### Go Users

Which repositories did user 14 star:
``` request
	response, err := client.Query(stargazer.Row(14))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Row Query: ", response.Result().Row().Columns)
```
``` response
Row Query:  [1 2 3 362 368 391 396 409 416 430 436 450 454 460 461 464 466 469 470 483 484 486 490 491 503 504 514]
```

What are the top 5 languages in the sample data:
``` request
	response, err = client.Query(language.TopN(5))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("TopN Query: ", response.Result().CountItems())
```
``` response
TopN Query:  [{5  119} {1  50} {4  48} {9  31} {13  25}]
```

Which repositories were starred by user 14 and 19:
``` request
	response, err = client.Query(repository.Intersect(stargazer.Row(14), stargazer.Row(19)))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Stargazer Intersect Query: ", response.Result().Row().Columns)
```
``` response
Stargazer Intersect Query:  [2 3 362 396 416 461 464 466 470 486]
```

Which repositories were starred by user 14 or 19:
``` request
	response, err = client.Query(repository.Union(stargazer.Row(14), stargazer.Row(19)))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Union Query: ", response.Result().Row().Columns)
```
``` response
Union Query:  [1 2 3 361 362 368 376 377 378 382 386 388 391 396 398 400 409 411 412 416 426 428 430 435 436 450 452 453 454 456 460 461 464 465 466 469 470 483 484 486 487 489 490 491 500 503 504 505 512 514]
```

Which repositories were starred by user 14 and 19 and also were written in language 1:
``` request
	response, err = client.Query(repository.Intersect(stargazer.Row(14), stargazer.Row(19), language.Row(1)))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Stargazer and Language Intersect Query: ", response.Result().Row().Columns)
```
``` response
Stargazer and Language Intersect Query:  [2 362 416 461]
```

Set user 99999 as a stargazer for repository 77777:
``` request
	client.Query(stargazer.Set(99999, 77777))
	response, err = client.Query(stargazer.Row(99999))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Set Query: ", response.Result().Row().Columns)
```
``` response
Set Query:  [77777]
```

Please note that while user ID 99999 may not be sequential with the other column IDs, it is still a relatively low number. 
Don't try to use arbitrary 64-bit integers as column or row IDs in Pilosa - this will lead to problems such as poor performance and out of memory errors.

For more information about Query Language, please see [Data Model and Queries](https://github.com/pilosa/go-pilosa/blob/master/docs/data-model-queries.md) and [Server Interaction](https://github.com/pilosa/go-pilosa/blob/master/docs/server-interaction.md)

##### Java and Python Users

<div class="note">
    <p>Java and Python support will be uploaded shortly.
</div>

### What's Next?

You can jump to [Data Model](../data-model/) for an in-depth look at Pilosa's data model, or [Query Language](../query-language/) for more details about **PQL**, the query language of Pilosa. Check out the [Examples](../examples/) page for example implementations of real world use cases for Pilosa. Ready to get going in your favorite language? Have a peek at our small but expanding set of official [Client Libraries](../client-libraries/).
