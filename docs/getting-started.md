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

#### Create the Schema

Note:
If at any time you want to verify the data structure, you can request the schema as follows:

``` request
curl localhost:10101/schema
```
``` response
{"indexes":null}
```

Before we can import data or run queries, we need to create our indexes and the fields within them. Let's create the repository index first:
``` request
curl localhost:10101/index/repository -X POST
```
``` response
{"success":true}
```

Let's create the `stargazer` field which has user IDs of stargazers as its rows:
``` request
curl localhost:10101/index/repository/field/stargazer \
     -X POST \
     -d '{"options": {"type": "time", "timeQuantum": "YMD"}}'
```
``` response
{"success":true}
```

Since our data contains time stamps whcih represent the time users starred repos, we set the field type to `time`. Time quantum is the resolution of the time we want to use, and we set it to `YMD` (year, month, day) for `stargazer`.

Next up is the `language` field, which will contain IDs for programming languages:
``` request
curl localhost:10101/index/repository/field/language \
     -X POST
```
``` response
{"success":true}
```

The `language` is a `set` field, but since the default field type is `set`, we didn't specify it in field options.

#### Import Data From CSV Files

<div class="note">
    <p>For demonstration purposes, we're using Pilosa's built in utility to import specially formatted CSV files. For more general usage, see how the various client libraries expose the bulk import functionality in <a href="https://github.com/pilosa/go-pilosa/blob/master/docs/imports-exports.md">Go</a>, <a href="https://github.com/pilosa/java-pilosa/blob/master/docs/imports.md">Java</a>, and <a href="https://github.com/pilosa/python-pilosa/tree/master/docs/imports.md">Python</a>. </p>
</div>


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

If you are using a Docker container for Pilosa (with name `pilosa`), you should instead copy the `*.csv` file into the container and then import them:
```
docker cp stargazer.csv pilosa:/stargazer.csv
docker exec -it pilosa /pilosa import -i repository -f stargazer /stargazer.csv
docker cp language.csv pilosa:/language.csv
docker exec -it pilosa /pilosa import -i repository -f language /language.csv
```

Note that both the user IDs and the repository IDs were remapped to sequential integers in the data files, they don't correspond to actual Github IDs anymore. You can check out [languages.txt](https://github.com/pilosa/getting-started/blob/master/languages.txt) to see the mapping for languages.

#### Make Some Queries

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



### What's Next?

You can jump to [Data Model](../data-model/) for an in-depth look at Pilosa's data model, or [Query Language](../query-language/) for more details about **PQL**, the query language of Pilosa. Check out the [Examples](../examples/) page for example implementations of real world use cases for Pilosa. Ready to get going in your favorite language? Have a peek at our small but expanding set of official [Client Libraries](../client-libraries/).
