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
Any HTTP tool can be used to interact with the Pilosa server. The examples in this documentation will use curl which is available by default on many UNIX-like systems including Linux and MacOS. However, the best way to interface with the Pilosa server is through one of our three client libraries. Pilosa currently supports [Go](https://github.com/pilosa/go-pilosa), [Java](https://github.com/pilosa/java-pilosa), and [Python](https://github.com/pilosa/python-pilosa).

<div class="note">
    <p>Note that Pilosa server requires a high limit for open files. Check the documentation of your system to see how to increase it in case you hit that limit. See <a href="/docs/administration/#open-file-limits">Open File Limits</a> for more details.</p>
</div>

### Starting Pilosa

Follow the steps in the [Installation](../installation/) document to install Pilosa.
Execute the following in a terminal to run Pilosa with the default configuration (Pilosa will be available at [localhost:10101](http://localhost:10101)):
```
pilosa server
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

In order to better understand Pilosa's capabilities, we will create a sample project called "Star Trace" containing information about 1,000 popular Github repositories which have "go" in their name. The Star Trace index will include data points such as programming language and stargazers—people who have starred a project.

Although Pilosa doesn't keep the data in a tabular format, we still use the terms "columns" and "rows" when describing the data model. We put the primary objects in columns, and the properties of those objects in rows. For example, the Star Trace project will contain an index called "repository" which contains columns representing Github repositories, and rows representing properties like programming languages and stargazers. We can better organize the rows by grouping them into sets called Fields. So the "repository" index might have a "languages" field as well as a "stargazers" field. You can learn more about indexes and fields in the [Data Model](../data-model/) section of the documentation.

Pilosa supports curl (or any HTTP tool), Go, Java, and Python. In this project, we will walk you through how to use each one to best communicate with the Pilosa server.

Note: If at any time you want to verify the data structure, you can request the schema as follows:

``` request
curl localhost:10101/schema
```
``` response
{"indexes":null}
```

#### Using Curl

Note: This is not the recommended way to interact with Pilosa, but it is the fastest way to see the efficiency of Pilosa.

##### Create the Schema

Before we can import data or run queries, we need to create our indexes and the fields within them. Let's create the `repository` index first:
``` request
curl localhost:10101/index/repository -X POST
```
``` response
{"success":true}
```
The index name must be 64 characters or less, start with a letter, and consist only of lowercase alphanumeric characters or `_-`. The same goes for field names.

Let's create the `stargazer` field which has user IDs of stargazers as its rows:
``` request
curl localhost:10101/index/repository/field/stargazer \
     -X POST \
     -d '{"options": {"type": "time", "timeQuantum": "YMD"}}'
```
``` response
{"success":true}
```

Since our data contains time stamps which represent the time users starred repos, we set the field type to `time`. Time quantum is the resolution of the time we want to use, and we set it to `YMD` (year, month, day) for `stargazer`.

Next up is the `language` field, which will contain IDs for programming languages:
``` request
curl localhost:10101/index/repository/field/language \
     -X POST
```
``` response
{"success":true}
```

The `language` is a `set` field, but since the default field type is `set`, we didn't specify it in field options.

##### Import Data From CSV Files

Download the `stargazer.csv` and `language.csv` files here:

```
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/stargazer.csv
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/language.csv
```

Run the following commands to import the data into Pilosa:

```
pilosa import -i repository -f stargazer stargazer.csv
pilosa import -i repository -f language language.csv
```

Note that both the user IDs and the repository IDs were remapped to sequential integers in the data files, they don't correspond to actual Github IDs anymore. You can check out [languages.txt](https://github.com/pilosa/getting-started/blob/master/languages.txt) to see the mapping for languages.

##### Make Some Queries

Which repositories did user 14 star:
``` request
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'Row(stargazer=14)'
```
``` response
{
    "results":[
        {
            "attrs":{},
            "columns":[1,2,3,362,368,391,396,409,416,430,436,450,454,460,461,464,466,469,470,483,484,486,490,491,503,504,514]
        }
    ]
}
```

What are the top 5 languages in the sample data:
``` request
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'TopN(language, n=5)'
```
``` response
{
    "results":[
        [
            {"id":5,"count":119},
            {"id":1,"count":50},
            {"id":4,"count":48},
            {"id":9,"count":31},
            {"id":13,"count":25}
        ]
    ]
}
```

Which repositories were starred by user 14 and 19:
``` request
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'Intersect(
            Row(stargazer=14), 
            Row(stargazer=19)
        )'
```
``` response
{
    "results":[
        {
            "attrs":{},
            "columns":[2,3,362,396,416,461,464,466,470,486]
        }
    ]
}
```

Which repositories were starred by user 14 or 19:
``` request
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'Union(
            Row(stargazer=14), 
            Row(stargazer=19)
        )'
```
``` response
{
    "results":[
        {
            "attrs":{},
            "columns":[1,2,3,361,362,368,376,377,378,382,386,388,391,396,398,400,409,411,412,416,426,428,430,435,436,450,452,453,454,456,460,461,464,465,466,469,470,483,484,486,487,489,490,491,500,503,504,505,512,514]
        }
    ]
}
```

Which repositories were starred by user 14 and 19 and also were written in language 1:
``` request
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'Intersect(
            Row(stargazer=14), 
            Row(stargazer=19),
            Row(language=1)
        )'
```
``` response
{
    "results":[
        {
            "attrs":{},
            "columns":[2,362,416,461]
        }
    ]
}
```

Set user 99999 as a stargazer for repository 77777:
``` request
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'Set(77777, stargazer=99999)'
```
``` response
{"results":[true]}
```

Please note that while user ID 99999 may not be sequential with the other column IDs, it is still a relatively low number. 
Don't try to use arbitrary 64-bit integers as column or row IDs in Pilosa - this will lead to problems such as poor performance and out of memory errors.

#### Using Go

Pilosa requires Go 1.12 or higher. It is also recommended that you have a code editor downloaded.

##### Create the Environment

In order to communicate with Pilosa through your Go code, you must have a "translator," which is go-pilosa. To install go-pilosa, open a terminal (one other than the one running Pilosa) and download the library in your `GOPATH` using:
``` 
go get github.com/pilosa/go-pilosa
```

To contain the Getting Started project in one place, we will create a new folder as follows:
```
mkdir GettingStarted && cd GettingStarted
```

In this folder, we will download two CSV files to provide data to our fields later on. Download the `stargazer.csv` and `language.csv` files here:
```
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/stargazer.csv
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/language.csv
```

We will also create a file called `StarTrace.go` as follows:
```
touch StarTrace.go
```
This file will be used in the following sections.

##### Create the Schema

Before we can import data or run queries, we need to create our schema. Go-pilosa is implemented by importing `github.com/pilosa/go-pilosa` and its ability to read csv files is implemented by importing 'github.com/pilosa/go-pilosa/csv`. The first steps to creating the schema are creating a client which will communicate our schema to Pilosa, creating a schema which will contain our indexes and fields, and syncing with Pilosa. This is all done in the `StarTrace.go` file:
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
	// This is where the index will go later
	// This is where the fields will go later
	err := client.SyncSchema(schema)
	if err != nil {
		log.Fatal(err)
	}
}
```

Next, let's create the `repository` index:
```
	repository := schema.Index("repository")
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

Your `StarTrace.go` file should look like:
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

##### Import Data From CSV Files

Now that we have our index and our fields, we can import the data we downloaded earlier and be on our way to making our own queries.

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
Since our `stargazer` data contains time stamps, which represent the time users starred repos, we will be using the `csv.NewColumnIteratorWithTimeStampFormat` function that is built into the go-pilosa import. This function takes the format of the csv files (`csv.RowIDColumnID`), an `io.Reader` (`bytes.NewReader(stargazerFile)`), and the time quantum format (`format`) and translates the csv file into a format Pilosa can read. Time quantum is the resolution of the time we want to use.

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
Since our `language` data doesn't contain time stamps, we will use the `csv.NewColumnIterator` function in place of `csv.NewColumnIteratorWithTimeStampFormat`.

Note that both the user IDs and the repository IDs were remapped to sequential integers in the data files, they don't correspond to actual Github IDs anymore. You can check out [languages.txt](https://github.com/pilosa/getting-started/blob/master/languages.txt) to see the mapping for languages.

For more information on imports in go-pilosa, please see the go-pilosa [site](https://github.com/pilosa/go-pilosa/blob/master/docs/imports-exports.md).

##### Make Some Queries

Now that we have a working schema, we can query it.

Which repositories did user 14 star:
``` request
response, err := client.Query(stargazer.Row(14))
if err != nil {
	log.Fatal(err)
}
fmt.Println("User 14 starred: ", response.Result().Row().Columns)
```
``` response
User 14 starred:  [1 2 3 362 368 391 396 409 416 430 436 450 454 460 461 464 466 469 470 483 484 486 490 491 503 504 514]
```

What are the top 5 languages in the sample data:
``` request
response, err = client.Query(language.TopN(5))
if err != nil {
	log.Fatal(err)
}
fmt.Println("Top Languages: ", response.Result().CountItems())
```
``` response
Top Languages:  [{5  119} {1  50} {4  48} {9  31} {13  25}]
```

Which repositories were starred by user 14 and 19:
``` request
response, err = client.Query(repository.Intersect(stargazer.Row(14), stargazer.Row(19)))
if err != nil {
	log.Fatal(err)
}
fmt.Println("Both user 14 and 19 starred: ", response.Result().Row().Columns)
```
``` response
Both user 14 and 19 starred:  [2 3 362 396 416 461 464 466 470 486]
```

Which repositories were starred by user 14 or 19:
``` request
response, err = client.Query(repository.Union(stargazer.Row(14), stargazer.Row(19)))
if err != nil {
	log.Fatal(err)
}
fmt.Println("User 14 or 19 starred: ", response.Result().Row().Columns)
```
``` response
User 14 or 19 starred:  [1 2 3 361 362 368 376 377 378 382 386 388 391 396 398 400 409 411 412 416 426 428 430 435 436 450 452 453 454 456 460 461 464 465 466 469 470 483 484 486 487 489 490 491 500 503 504 505 512 514]
```

Which repositories were starred by user 14 and 19 and also were written in language 1:
``` request
response, err = client.Query(repository.Intersect(stargazer.Row(14), stargazer.Row(19), language.Row(1)))
if err != nil {
	log.Fatal(err)
}
fmt.Println("Both user 14 and 19 starred and were written in language 1: ", response.Result().Row().Columns)
```
``` response
Both user 14 and 19 starred and were written in language 1:  [2 362 416 461]
```

Set user 99999 as a stargazer for repository 77777:
``` request
client.Query(stargazer.Set(99999, 77777))
response, err = client.Query(stargazer.Row(99999))
if err != nil {
	log.Fatal(err)
}
fmt.Println("Set user 99999 as a stargazer for repository 77777")
```
``` response
Set user 99999 as a stargazer for repository 77777
```

Please note that while user ID 99999 may not be sequential with the other column IDs, it is still a relatively low number. 
Don't try to use arbitrary 64-bit integers as column or row IDs in Pilosa - this will lead to problems such as poor performance and out of memory errors.

For more information about go-pilosa, please see our Go client library at [go-pilosa](https://github.com/pilosa/go-pilosa) or checkout the go-pilosa [Data Model and Queries](https://github.com/pilosa/go-pilosa/blob/master/docs/data-model-queries.md) section for more query options.

#### Using Java

Pilosa requires Java 8 or higher and Maven 3 or higher. It is also recommended that you have a code editor downloaded.

##### Create the Environment

To contain the Getting Started project in one place, we will create a new folder as follows:
```
mkdir GettingStarted && cd GettingStarted
```

In this folder, we will download two CSV files to provide data to our fields later on. Download the `stargazer.csv` and `language.csv` files here:
```
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/stargazer.csv
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/language.csv
```

We will now create the java directory that will contain our `pom.xml` file and then import the `pom.xml` file:
```
mkdir startrace && cd startrace
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/java/startrace/pom.xml
```

For this specific project, the `pom.xml` file needs to be edited. The file can be edited by typing `nano pom.xml` directly into the terminal or simply using your code editing software. The following needs to be changed:
```
<dependencies>
     <dependency>
         <groupId>com.pilosa</groupId>
         <artifactId>pilosa-client</artifactId>
         <version>**1.3.1**</version>
     </dependency>
</dependencies>

<!-- Build an executable JAR -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-jar-plugin</artifactId>
    <version>3.0.2</version>
    <configuration>
        <archive>
            <manifest>
                <addClasspath>true</addClasspath>
                <classpathPrefix>lib/</classpathPrefix>
                <mainClass>**main.java.StarTrace**</mainClass>
            </manifest>
        </archive>
    </configuration>
</plugin>
```

We will now create the java directory that will contain our `StarTrace.java` file and create the `StarTrace.java` file:
```
mkdir src && cd src
mkdir main && cd main
mkdir java && cd java
touch StarTrace.go
```

This file will be used in the following sections.

##### Create the Schema

Before we can import data or run queries, we need to create our schema. The following imports implement the java-pilosa:
```
import com.pilosa.client.PilosaClient;
import com.pilosa.client.QueryResponse;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.orm.*;
import com.pilosa.client.csv.FileRecordIterator;
import com.pilosa.client.TimeQuantum;
```
The first steps to creating the schema are creating a client which will communicate our schema to Pilosa, creating a schema which will contain our indexes and fields, and syncing with Pilosa. This is all done in the `StarTrace.java` file:
```
package main.java;

import com.pilosa.client.PilosaClient;
import com.pilosa.client.QueryResponse;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.orm.*;
import com.pilosa.client.csv.FileRecordIterator; 
import com.pilosa.client.TimeQuantum;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class StarTrace {
    public static void main(String []args) throws IOException {
        // Create the Schema
        PilosaClient client = PilosaClient.defaultClient();
        Schema schema = client.readSchema();
        // This is were the index will go later
	// This is were the fields will go later
        client.syncSchema(schema);
    }
}
```

Next, let's create the `repository` index:
```
	Index repository = schema.index("repository");
```
The index name must be 64 characters or less, start with a letter, and consist only of lowercase alphanumeric characters or `_-`. The same goes for field names.

Let's create the `stargazer` field which has user IDs of stargazers as its rows:
```
	FieldOptions stargazerOptions = FieldOptions.builder()
            .fieldTime(TimeQuantum.YEAR_MONTH_DAY)
            .build();
        Field stargazer = repository.field("stargazer", stargazerOptions);
```
Since our data contains time stamps which represent the time users starred repos, we set the field type to `time` using `fieldTime()`. Time quantum is the resolution of the time we want to use, and we set it to `YEAR_MONTH-DAY` for `stargazer`.

Next up is the `language` field, which will contain IDs for programming languages:
```
	Field language = repository.field("language");
```
The `language` field is a `set` field, but since the default field type is `set`, we don't need to specify it

Your `StarTrace.java` file should look like:
```
package main.java;

import com.pilosa.client.PilosaClient;
import com.pilosa.client.QueryResponse;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.orm.*;
import com.pilosa.client.csv.FileRecordIterator; 
import com.pilosa.client.TimeQuantum;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class StarTrace {
    public static void main(String []args) throws IOException {
        // Create the Schema
        PilosaClient client = PilosaClient.defaultClient();
        Schema schema = client.readSchema();
        Index repository = schema.index("repository");

        FieldOptions stargazerOptions = FieldOptions.builder()
            .fieldTime(TimeQuantum.YEAR_MONTH_DAY)
            .build();
        Field stargazer = repository.field("stargazer", stargazerOptions);
        
        Field language = repository.field("language");
        client.syncSchema(schema);
    }
}
```

##### Import Data From CSV Files

Now that we have our index and our fields, we can import the data we downloaded earlier and be on our way to making our own queries.

First, we will load our data into the `stargazer` field:
```
	SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm");
        FileRecordIterator iterator = FileRecordIterator.fromPath("stargazer.csv", stargazer, timestampFormat);
        client.importField(stargazer, iterator);
```
Due to the time aspect of the `stargazer` csv file, we have to specify the time stamp format in the `fromPath` function. We set the variable `timestampFormat` to the format present in the csv file using the function `SimpleDateFormat()` and call the variable in the `fromPath` function, which will take the csv file name, the field name, and the time stamp format and translate the csv file into a format Pilosa can read.

Next, we will load our data into the `language` field:
```
	iterator = FileRecordIterator.fromPath("language.csv", language);
        client.importField(language, iterator);
```
Since our `language` data doesn't have a time aspect, the time stamp format doesn't need to be specified.

Note that both the user IDs and the repository IDs were remapped to sequential integers in the data files, they don't correspond to actual Github IDs anymore. You can check out [languages.txt](https://github.com/pilosa/getting-started/blob/master/languages.txt) to see the mapping for languages.

For more information on imports in java-pilosa, please see the java-pilosa [site](https://github.com/pilosa/java-pilosa/blob/master/docs/imports.md).

##### Make Some Queries

Now that we have a working schema, we can query it.

Which repositories did user 14 star:
``` request
QueryResponse response = client.query(stargazer.row(14));
System.out.println("User 14 starred: " + response.getResult().getRow().getColumns());
```
``` response
User 14 starred: [1, 2, 3, 362, 368, 391, 396, 409, 416, 430, 436, 450, 454, 460, 461, 464, 466, 469, 470, 483, 484, 486, 490, 491, 503, 504, 514]
```

What are the top 5 languages in the sample data:
``` request
response = client.query(language.topN(5));
System.out.println("Top Languages: " + response.getResult().getCountItems());
```
``` response
Top Languages: [CountResultItem(id=5, count=119), CountResultItem(id=1, count=50), CountResultItem(id=4, count=48), CountResultItem(id=9, count=31), CountResultItem(id=13, count=25)]
```

Which repositories were starred by user 14 and 19:
``` request
response = client.query(repository.intersect(stargazer.row(14), stargazer.row(19)));
System.out.println("Both user 14 and 19 starred: " + response.getResult().getRow().getColumns());
```
``` response
Both user 14 and 19 starred: [2, 3, 362, 396, 416, 461, 464, 466, 470, 486]
```

Which repositories were starred by user 14 or 19:
``` request
response = client.query(repository.union(stargazer.row(14), stargazer.row(19)));
System.out.println("User 14 or 19 starred: " + response.getResult().getRow().getColumns());
```
``` response
User 14 or 19 starred: [1, 2, 3, 361, 362, 368, 376, 377, 378, 382, 386, 388, 391, 396, 398, 400, 409, 411, 412, 416, 426, 428, 430, 435, 436, 450, 452, 453, 454, 456, 460, 461, 464, 465, 466, 469, 470, 483, 484, 486, 487, 489, 490, 491, 500, 503, 504, 505, 512, 514]
```

Which repositories were starred by user 14 and 19 and also were written in language 1:
``` request
response = client.query(repository.intersect(stargazer.row(14), stargazer.row(19), language.row(1)));
System.out.println("Both user 14 and 19 starred and were written in language 1: " + response.getResult().getRow().getColumns());
```
``` response
Both user 14 and 19 starred and were written in language 1: [2, 362, 416, 461]
```

Set user 99999 as a stargazer for repository 77777:
``` request
client.query(stargazer.set(99999, 77777));
System.out.println("Set user 99999 as a stargazer for repository 77777");
```
``` response
Set user 99999 as a stargazer for repository 77777
```

Please note that while user ID 99999 may not be sequential with the other column IDs, it is still a relatively low number. 
Don't try to use arbitrary 64-bit integers as column or row IDs in Pilosa - this will lead to problems such as poor performance and out of memory errors.

For more information about java-pilosa, please see our Java client library at [java-pilosa](https://github.com/pilosa/java-pilosa) or checkout the java-pilosa [Data Model and Queries](https://github.com/pilosa/java-pilosa/blob/master/docs/data-model-queries.md) section for more query options.

#### Python Users

Pilosa requires Python 2.7 or higher or Python 3.4 or higher. It is also recommended that you have a code editor downloaded.

##### Create the Environment

To contain the Getting Started project in one place, we will create a new folder as follows:
```
mkdir GettingStarted && cd GettingStarted
```
In this folder, we will download two CSV files to provide data to our fields later on. Download the `stargazer.csv` and `language.csv` files here:
```
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/stargazer.csv
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/language.csv
```
We will also download two text files. One is the `requirements.txt` that will install python-pilosa later on and the other is `languages.txt` which will provide context to the `language` field.
```
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/python/requirements.txt
curl -O https://raw.githubusercontent.com/pilosa/getting-started/master/language.txt
```
We will now create the python environment:
```
python3 -m venv startrace
```

Next, we activate the python environment we created and install the requirements (and python-pilosa):
```
source startrace/bin/activate
pip install -r requirements.txt
```
We will also create a file called `StarTrace.py` as follows:
```
touch StarTrace.py
```
This file will be used in the following sections.

##### Create the Schema

Before we can import data or run queries, we need to create our schema. The following imports implement the python-pilosa. This is all done in the `StarTrace.py` file:
```
import pilosa
from pilosa import Client, Index, TimeQuantum
from pilosa.imports import csv_column_reader, csv_row_id_column_id
```

The first steps to creating the schema are creating a client which will communicate our schema to Pilosa, creating a schema which will contain our indexes and fields, and syncing with Pilosa:
``` 
from __future__ import print_function

import os
import sys
import time
import pilosa

from pilosa import Client, Index, TimeQuantum
from pilosa.imports import csv_column_reader, csv_row_id_column_id

try:
	# Python 2.7 and 3
	from io import StringIO
except ImportError:
	# Python 2.6 and 2.7
	from StringIO import StringIO

# Create the Schema
client = pilosa.Client()
schema = client.schema()
# This is where the index will go later
# This is where the fields will go later
client.sync_schema(schema)
```
Next, let's create the `repository` index:
```
repository = schema.index("repository")
```
The index name must be 64 characters or less, start with a letter, and consist only of lowercase alphanumeric characters or `_-`. The same goes for field names.

Let's create the `stargazer` field which has user IDs of stargazers as its rows:
```
stargazer = repository.field("stargazer", time_quantum=pilosa.TimeQuantum.YEAR_MONTH_DAY)
```
Since our data contains time stamps which represent the time users starred repos, we establish the time aspect by using `time_quantum`. Time quantum is the resolution of the time we want to use, and we set it to `YEAR_MONTH-DAY` for `stargazer`.

Next up is the `language` field, which will contain IDs for programming languages:
```
language = repository.field("language")
```
The `language` field is a `set` field, but since the defualt field is `set`, we didn't need to specify any options.

Your `StarTrace.py` file should look like:
```
from __future__ import print_function

import os
import sys
import time
import pilosa

from pilosa import Client, Index, TimeQuantum
from pilosa.imports import csv_column_reader, csv_row_id_column_id

try:
        # Python 2.7 and 3
        from io import StringIO
except ImportError:
        # Python 2.6 and 2.7
        from StringIO import StringIO

# Create the Schema
client = pilosa.Client()
schema = client.schema()
repository = schema.index("repository")
stargazer = repository.field("stargazer", time_quantum=pilosa.TimeQuantum.YEAR_MONTH_DAY)
language = repository.field("language")
client.sync_schema(schema)
```

##### Import Data From CSV Files

Now that we have our index and our fields, we can import the data we downloaded earlier and be on our way to making our own queries.

First, we will load our data into the `stargazer` field:
```
time_func = lambda s: int(time.mktime(time.strptime(s, "%Y-%m-%dT%H:%M")))
with open("stargazer.csv") as f:
    stargazer_reader = csv_column_reader(f, timefunc=time_func)
    client.import_field(stargazer, stargazer_reader)
```
Due to the time aspect of the `stargazer` csv file, we have to specify the time stamp format in the `csv_column_reader` function. We set the variable `time_func` to the format present in the csv file and call it in the `csv_column_reader` function, which will take the csv file and the time stamp format and translate the csv file into a format Pilosa can read

Next, we will load our data into the `language` field:
```
with open("language.csv") as f:
    language_reader = csv_column_reader(f, csv_row_id_column_id)
    client.import_field(language, language_reader)
```

The `language` is a `set` field, but since the default field type is `set`, we didn't need to specify it.

For more information on imports in python-pilosa, please see the python-pilosa [site](https://github.com/pilosa/python-pilosa/blob/master/docs/imports.md).

Note that both the user IDs and the repository IDs were remapped to sequential integers in the data files, they don't correspond to actual Github IDs anymore. You can check out [languages.txt](https://github.com/pilosa/getting-started/blob/master/languages.txt) to see the mapping for languages.

##### Make Some Queries

Now that we have a working schema, we can query it.

Which repositories did user 14 star:
``` request
response = client.query(stargazer.row(14))
print("User 14 starred: ", response.result.row.columns)
```
``` response
User 14 starred:  [1, 2, 3, 362, 368, 391, 396, 409, 416, 430, 436, 450, 454, 460, 461, 464, 466, 469, 470, 483, 484, 486, 490, 491, 503, 504, 514]
```

What are the top 5 languages in the sample data:
``` request
def load_language_names():
    with open("languages.txt") as f:
        return [line.strip() for line in f]

def print_topn(items):
    lines = ["\t{i}. {s[0]}: {s[1]} stars".format(s=s, i=i + 1) for i, s in enumerate(items)]
    print("\n".join(lines))

language_names = load_language_names()
top_languages = client.query(language.topn(5)).result.count_items
language_items = [(language_names[item.id], item.count) for item in top_languages]
print("Top languages: ")
print_topn(language_items)
```
``` response
Top languages: 
        1. Go: 119 stars
        2. Shell: 50 stars
        3. Makefile: 48 stars
        4. HTML: 31 stars
        5. JavaScript: 25 stars
```

Which repositories were starred by user 14 and 19:
``` request
repsonse = client.query(repository.intersect(stargazer.row(14), stargazer.row(19)))
print("Both user 14 and 19 starred: ", response.result.row.columns)
```
``` resposne
Both user 14 and 19 starred:  [1, 2, 3, 362, 368, 391, 396, 409, 416, 430, 436, 450, 454, 460, 461, 464, 466, 469, 470, 483, 484, 486, 490, 491, 503, 504, 514]
```

Which repositories were starred by user 14 or 19:
``` request
response = client.query(repository.union(stargazer.row(14), stargazer.row(19)))
print("User 14 or 19 starred: ", response.result.row.columns)
```
``` response
User 14 or 19 starred:  [1, 2, 3, 361, 362, 368, 376, 377, 378, 382, 386, 388, 391, 396, 398, 400, 409, 411, 412, 416, 426, 428, 430, 435, 436, 450, 452, 453, 454, 456, 460, 461, 464, 465, 466, 469, 470, 483, 484, 486, 487, 489, 490, 491, 500, 503, 504, 505, 512, 514]
```

Which repositories were starred by user 14 and 19 and also were written in language 1:
``` request
response = client.query(repository.intersect(stargazer.row(14), stargazer.row(19), language.row(1)))
print("Both user 14 and 19 starred and were written in language 1: ", response.result.row.columns)
```
``` response
Both user 14 and 19 starred and were written in language 1:  [2, 362, 416, 461]
```

Set user 99999 as a stargazer for repository 77777:
``` request
client.query(stargazer.set(99999, 77777))
print("Set user 99999 as a stargazer for repository 77777")
```
``` response
Set user 99999 as a stargazer for repository 77777
```

Please note that while user ID 99999 may not be sequential with the other column IDs, it is still a relatively low number. 
Don't try to use arbitrary 64-bit integers as column or row IDs in Pilosa - this will lead to problems such as poor performance and out of memory errors.

For more information about python-pilosa, please see our Python client library at [python-pilosa](https://github.com/pilosa/python-pilosa) or checkout the python-pilosa [Data Model and Queries](https://github.com/pilosa/python-pilosa/blob/master/docs/data-model-queries.md) section for more query options.

### What's Next?

You can jump to [Data Model](../data-model/) for an in-depth look at Pilosa's data model, or [Query Language](../query-language/) for more details about **PQL**, the query language of Pilosa. Check out the [Examples](../examples/) page for example implementations of real world use cases for Pilosa. Ready to get going in your favorite language? Have a peek at our small but expanding set of official [Client Libraries](../client-libraries/).
