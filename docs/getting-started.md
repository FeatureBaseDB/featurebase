+++
title = "Getting Started"
+++

## Getting Started

Pilosa supports an HTTP interface which uses JSON by default.
Any HTTP tool can be used to interact with the Pilosa server. The examples in this documentation will use [curl](https://curl.haxx.se/) which is available by default on many UNIX-like systems including Linux and MacOS. Windows users can download curl [here](https://curl.haxx.se/download.html).

<div class="note">
    <p>Note that Pilosa server requires a high limit for open files. Check the documentation of your system to see how to increase it in case you hit that limit.</p>
</div>

### Starting Pilosa

Follow the steps in the [Install]({{< ref "installation.md" >}}) document to install Pilosa.
Execute the following in a terminal to run Pilosa with the default configuration (Pilosa will be available at `localhost:10101`):
```
pilosa server
```
If you are using the Docker image, you can run an ephemeral Pilosa container on the default address using the following command:
```
docker run -it --rm --name pilosa -p 10101:10101 pilosa/pilosa:latest
```

Let's make sure Pilosa is running:
```
curl localhost:10101/status
```

Which should output: `{"status":{"Nodes":[{"Host":":10101","State":"UP"}]}}`

### Sample Project

In order to better understand Pilosa's capabilities, we will create a sample project called "Star Trace" containing information about the top 1,000 most recently updated Github repositories which have "go" in their name. The Star Trace index will include data points such as programming language, tags, and stargazers—people who have starred a project.

Although Pilosa doesn't keep the data in a tabular format, we still use the terms "columns" and "rows" when describing the data model. We put the primary objects in columns, and the properties of those objects in rows. For example, the Star Trace project will contain an index called "repository" which contains columns representing Github repositories, and rows representing properties like programming languages and tags. We can better organize the rows by grouping them into sets called Frames. So the "repository" index might have a "languages" frame as well as a "tags" frame. You can learn more about indexes and frames in the [Data Model](../data-model) section of the documentation.

#### Create the Schema

Note:
The queries in this section which are used to set up the indexes in Pilosa just the empty object on success: `{}` - if you would like to verify that a query worked as you expected, you can request the schema as follows:
```
curl localhost:10101/schema
{"indexes":null}
```

Before we can import data or run queries, we need to create our indexes and the frames within them. Let's create the repository index first:
```
curl localhost:10101/index/repository \
     -X POST \
     -d '{"options": {"columnLabel": "repo_id"}}'
```

Repository IDs are the main focus of the `repository` index, so we chose `repo_id` as the column label.

Let's create the `stargazer` frame which has user IDs of stargazers as its rows:
```
curl localhost:10101/index/repository/frame/stargazer \
     -X POST \
     -d '{"options": {"rowLabel": "stargazer_id", 
                      "timeQuantum": "YMD",
                      "inverseEnabled": true}}'
```

Since our data contains time stamps for the time users starred repos, we set the *time quantum* for the `stargazer` frame in the options as well. Time quantum is the resolution of the time we want to use, and we set it to `YMD` (year, month, day) for `stargazer`.

We set `inverseEnabled` to `true` in order to allow queries over columns as well as rows.

Next up is the `language` frame, which will contain IDs for programming languages:
```
curl localhost:10101/index/repository/frame/language \
     -X POST \
     -d '{"options": {"rowLabel": "language_id",
                      "inverseEnabled": true}}'
```

#### Create the Schema Using an Input Definition

Input definitions allow users to define a schema based on their data and to provide data to Pilosa in
a more standard format like JSON. Once an input definition is created, we can send data to Pilosa as JSON, and as long as the data adheres to the definition, Pilosa will internally perform all
of the appropriate mutations.

Before creating a schema, let's create the repository index first:

```
curl localhost:10101/index/repository \
     -X POST \
     -d '{"options": {"columnLabel": "repo_id"}}'
```
Then we can send the following input definition as JSON to Pilosa. The sample input defintion schema for the "Star Trace" project is at [Pilosa Getting Started repository](https://github.com/pilosa/getting-started), `input-definition.json` file

```
curl localhost:10101/index/repository/input-definition/stargazer \
     -X POST \
     -d '{
            "frames": [
                 {
                     "name": "language", 
                     "options": { 
                         "inverseEnabled": true, 
                         "timeQuantum": "YMD"
                     }
                 }, 
                 {
                     "name": "stargazer", 
                     "options": {
                         "inverseEnabled": true, 
                         "timeQuantum": "YMD"
                     }
                 }
             ],
             "fields": [
                 {
                     "name": "repo_id", 
                     "primaryKey": true
                 }, 
                 {
                     "actions": [
                         {
                             "frame": "language", 
                             "valueDestination": "mapping", 
                             "valueMap": {
                                 "C": 7, 
                                 "C#": 27, 
                                 "Go": 5, 
                                 "Java": 21, 
                                 "JavaScript": 13, 
                                 "Python": 17, 
                             }
                         }
                     ], 
                     "name": "language_id"
                 }, 
                 {
                     "actions": [
                         {
                             "frame": "stargazer", 
                             "valueDestination": "value-to-row"
                         }
                     ], 
                     "name": "stargazer_id"
                 }
             ] 
         }'
```

Instead of creating a `stargazer` frame and a `language` frame individually like above, we can create multiple frames in one input definition.
We can also set `repo_id` for multiple frames at the same time by providing field actions. There are three options for valueDestination:

 - value-to-row: The value for this field is used as the `rowID`.
 - single-row-boolean: The value must be a boolean, and this specifies `SetBit()` or `ClearBit()`, a `rowID` must be specified for this destination type.
 - mapping: The value for this field is used to lookup a `rowID` in a map. A valueMap is required for this destination type.


#### Import Data Using an Input Definition

The sample data for the "Star Trace" project is at [Pilosa Getting Started repository](https://github.com/pilosa/getting-started). 

If you import data using an input definition, download the `json-input.json` file in that repo, then run the following request using the input definition created above:

```
curl localhost:10101/index/repository/input/stargazer \
     -X POST \
     -d '[
             {
                 "language_id": "Go", 
                 "repo_id": 91720568, 
                 "stargazer_id": 513114
             }, 
             {
                 "language_id": "Python", 
                 "repo_id": 95122322
             }'
         ]
```

As defined in the input definition, field name `language_id` maps language to a corresponding id defined in `valueMap` and sets the appropriate bit in the `language` frame.  The value corresponding to field name `stargazer_id` is added to the `stargazer` frame as rowID.
The data input above is equivalent to the following `SetBit()` operations:

```
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'SetBit(frame="stargazer", repo_id=91720568, stargazer_id=513114)
         SetBit(frame="language", repo_id=91720568, language_id=5)
         SetBit(frame="language", repo_id=95122322, language_id=17)
     '
```

#### Import Data From CSV Files

If you import data using csv files and without input defintion, download the `stargazer.csv` and `language.csv` files in that repo.

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

Note that, both the user IDs and the repository IDs were remapped to sequential integers in the data files, they don't correspond to actual Github IDs anymore. You can check out `language.txt` to see the mapping for languages.

#### Make Some Queries

<div class="note">
    <p>Note the Pilosa server comes with a <a href="../webui/">WebUI</a> for constructing queries in a browser. In local development, it is available at <a href="http://localhost:10101">localhost:10101</a>.</p>
</div>

Which repositories did user 14 star:
```
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'Bitmap(frame="stargazer", stargazer_id=14)'
```

What are the top 5 languages in the sample data:
```
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'TopN(frame="language", n=5)'
```

Which repositories were starred by user 14 and 19:
```
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'Intersect(Bitmap(frame="stargazer", stargazer_id=14), Bitmap(frame="stargazer", stargazer_id=19))'
```

Which repositories were starred by user 14 or 19:
```
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'Union(Bitmap(frame="stargazer", stargazer_id=14), Bitmap(frame="stargazer", stargazer_id=19))'
```

Which repositories were starred by user 14 and 19 and also were written in language 1:
```
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'Intersect(Bitmap(frame="stargazer", stargazer_id=14), Bitmap(frame="stargazer", stargazer_id=19), Bitmap(frame="language", language_id=1))'
```

Set user 99999 as a stargazer for repository 77777:
```
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'SetBit(frame="stargazer", repo_id=77777, stargazer_id=99999)'
```

### What's Next?

You can jump to [Data Model](../data-model/) for an in-depth look at Pilosa's data model, or [Query Language](../query-language/) for more details about **PQL**, the query language of Pilosa. Check out the [Tutorials](../tutorials/) for example implementations of real world use cases for Pilosa. Ready to get going in your favorite language? Have a peek at our small but expanding set of official [Client Libraries](../client-libraries/).
