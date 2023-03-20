# FeatureBase

This is FeatureBase, a crazy fast database/analytics engine based on [B-trees](https://en.wikipedia.org/wiki/B-tree) and [Roaring Bitmaps](https://roaringbitmap.org/).

FeatureBase is written in Go and supports SQL.

## License

FeatureBase is released under the Apache 2.0 Open Source license.

## Getting Started

### Build FeatureBase Server with Docker (Optional)

To build Featurebase from source with Docker, run the following from a prompt:

```
docker build -t featurebasedb/featurebase:latest .
```

### Run FeatureBase from Docker

To run FeatureBase from Docker, enter the following from a prompt:

```
docker run -p 10101:10101 featurebasedb/featurebase:latest
```

Don't have Docker? Get started here: [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/)

### Create a Database and Insert Data

To create a sample database, run the following:

```
curl -X POST http://0.0.0.0:10101/sql -d 'CREATE TABLE planets (_id id, planet_name string, moons stringset, diameter_in_km int);'
```

To insert data into the `planets` database, do the following:

```
curl -X POST http://0.0.0.0:10101/sql -d "INSERT INTO planets VALUES (1, 'Mercury', [''], 4879), (2, 'Venus', [''], 12104), (3, 'Earth', ['Moon'], 12742);" 
```

You may insert as many records as you like in a single POST.

### Query Data

To query the data in a FeatureBase database, do the following:

```
curl -X POST http://0.0.0.0:10101/sql -d "SELECT * from planets;" | python3 -m json.tool
```

The output should be as follows:

```
{
    "schema": {
        "fields": [
            {
                "name": "_id",
                "type": "id",
                "base-type": "id",
                "type-info": null
            },
            {
                "name": "planet_name",
                "type": "string",
                "base-type": "string",
                "type-info": null
            },
            {
                "name": "moons",
                "type": "stringset",
                "base-type": "stringset",
                "type-info": null
            },
            {
                "name": "diameter_in_km",
                "type": "int",
                "base-type": "int",
                "type-info": null
            }
        ]
    },
    "data": [
        [
            1,
            "Mercury",
            null,
            4879
        ],
        [
            2,
            "Venus",
            null,
            12104
        ],
        [
            3,
            "Earth",
            [
                "Moon"
            ],
            12742
        ]
    ],
    "execution-time": 1006
}
```


## Community and Support

Join us on [Discord](https://discord.gg/featurefirstai).


