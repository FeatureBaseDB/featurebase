+++
title = "Examples"
weight = 4
nav = [
    "Transportation",
]
+++

## Examples

### Transportation

#### Introduction

New York City released an extremely detailed data set of over 1 billion taxi rides taken in the city - this data has become a popular target for analysis by tech bloggers and has been very well studied. For this reason, we thought it would be interesting to import this data to Pilosa in order to compare with other data stores and techniques on the exact same data set.

Transportation in general is a compelling use case for Pilosa as it often involves multiple disparate data sources, as well as high rate, real time, and extremely large amounts of data (particularly if one wants to draw reasonable conclusions).

We've written a tool to help import the NYC taxi data into Pilosa - this tool is part of the [PDK](../pdk/) (Pilosa Development Kit), and takes advantage of a number of reusable modules that may help you import other data as well. Follow along and we'll explain the whole process step by step.

After initial setup, the PDK import tool does everything we need to define a Pilosa schema, map data to bitmaps accordingly, and import it into Pilosa.

#### Data Model

The NYC taxi data is comprised of a number of csv files listed here: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml. These data files have around 20 columns, about half of which are relevant to the benchmark queries we're looking at:

* Distance: miles, floating point
* Fare: dollars, floating point
* Number of passengers: integer
* Dropoff location: latitude and longitude, floating point
* Pickup location: latitude and longitude, floating point
* Dropoff time: timestamp
* Pickup time: timestamp

We import these fields, creating one or more Pilosa fields from each of them:

field	|mapping
------------|---------------------
cab_type	|direct map of enum int → row ID
dist_miles	|round(dist) → row ID
total_amount_dollars	|round(dist) → row ID
passenger_count	|direct map of integer value → row ID
drop_grid_id	|(lat, lon) → 100x100 rectangular grid → cell ID
drop_year	|year(timestamp) → row ID
drop_month	|month(timestamp) → row ID
drop_day	|day(timestamp) → row ID
drop_time	|time of day mapped to one of 48 half-hour buckets
pickup_grid_id	|(lat, lon) → 100x100 rectangular grid → cell ID
pickup_year	|year(timestamp) → row ID
pickup_month	|month(timestamp) → row ID
pickup_day	|day(timestamp) → row ID
pickup_time	|time of day mapped to one of 48 half-hour buckets → row ID

We also created two extra fields that represent the duration and average speed of each ride:

field	|mapping
--------------------|-------------
duration_minutes	|round(drop_timestamp - pickup_timestamp) → row ID
speed_mph	|round(dist_miles / (drop_timestamp - pickup_timestamp)) → row ID

#### Mapping

Each column that we want to use must be mapped to a combination of fields and row IDs according to some rule. There are many ways to approach this mapping, and the taxi dataset gives us a good overview of possibilities.

##### 0 columns → 1 field

**cab_type**: contains one row for each type of cab. Each column, representing one ride, has a bit set in exactly one row of this field. The mapping is a simple enumeration, for example yellow=0, green=1, etc. The values of the bits in this field are determined by the source of the data. That is, we're importing data from several disparate sources: NYC yellow taxi cabs, NYC green taxi cabs, and Uber cars. For each source, the single row to be set in the cab_type field is constant.

##### 1 column → 1 field

The following three fields are mapped in a simple direct way from single columns of the original data.

**dist_miles:** each row represents rides of a certain distance. The mapping is simple: as an example, row 1 represents rides with a distance in the interval [0.5, 1.5]. That is, we round the floating point value of distance to an integer, and use that as the row ID directly. Generally, the mapping from a floating point value to a row ID could be arbitrary. The rounding mapping is concise to implement, which simplifies importing and analysis. As an added bonus, it's human-readable. We'll see this pattern used several times.

In PDK parlance, we define a Mapper, which is simply a function that returns integer row IDs. PDK has a number of predefined mappers that can be described with a few parameters. One of these is LinearFloatMapper, which applies a linear function to the input, and casts it to an integer, so the rounding is handled implicitly. In code:
```go
lfm := pdk.LinearFloatMapper{
    Min: -0.5,
    Max: 3600.5,
    Res: 3601,
}
```

`Min` and `Max` define the linear function, and `Res` determines the maximum allowed value for the output row ID - we chose these values to produce a “round to nearest integer” behavior. Other predefined mappers have their own specific parameters, usually two or three.

This mapper function is the core operation, but we need a few other pieces to define the overall process, which is encapsulated in the ColumnMapper object. This object defines which field(s) of the input data source to use (`Fields`), how to parse them (`Parsers`), what mapping to use (`Mapper`), and the name of the field to use (`Field`). <!-- TODO update so this makes sense -->
```go
pdk.ColumnMapper{
    Field:   "dist_miles",
    Mapper:  lfm,
    Parsers: []pdk.Parser{pdk.FloatParser{}},
    Fields:  []int{fields["trip_distance"]},
},
```

These same objects are represented in the JSON definition file:
```go
{
    "Fields": {
        "Trip_distance": 10
    },
    "Mappers": [
        {
            "Name": "lfm0",
            "Min": -0.5,
            "Max": 3600.5,
            "Res": 3600
        }
    ],
    "ColumnMappers": [
        {
            "Field": "dist_miles",
            "Mapper": {
                "Name": "lfm0"
            },
            "Parsers": [
                {"Name": "FloatParser"}
            ],
            "Fields": "Trip_distance"
        }
    ]
}
```

Here, we define a list of Mappers, each including a name, which we use to refer to the mapper later, in the list of ColumnMappers. We can also do this with Parsers, but a few simple Parsers that need no configuration are available by default. We also have a list of Fields, which is simply a map of field names (in the source data) to column indices (in Pilosa). We use these names in the ColumnMapper definitions to keep things human-readable.

**total_amount_dollars:** Here we use the rounding mapping again, so each row represents rides with a total cost that rounds to the row's ID. The ColumnMapper definition is very similar to the previous one.

**passenger_count:** This column contains small integers, so we use one of the simplest possible mappings: the column value is the row ID.

##### 1 column → multiple fields

When working with a composite data type like a timestamp, there are plenty of mapping options. In this case, we expect to see interesting periodic trends, so we want to encode the cyclic components of time in a way that allows us to look at them independently during analysis.

We do this by storing time data in four separate fields for each timestamp: one each for the year, month, day, and time of day. The first three are mapped directly. For example, a ride with a date of 2015/06/24 will have a bit set in row 2015 of field "year", row 6 of field "month", and row 24 of field "day".

We might continue this pattern with hours, minutes, and seconds, but we don't have much use for that level of precision here, so instead we use a "bucketing" approach. That is, we pick a resolution (30 minutes), divide the day into buckets of that size, and create a row for each one. So a ride with a time of 6:45AM has a bit set in row 13 of field "time_of_day".

We do all of this for each timestamp of interest, one for pickup time and one for dropoff time. That gives us eight total fields for two timestamps: pickup_year, pickup_month, pickup_day, pickup_time, drop_year, drop_month, drop_day, drop_time.

##### Multiple columns → 1 field

The ride data also contains geolocation data: latitude and longitude for both pickup and dropoff. We just want to be able to produce a rough overview heatmap of ride locations, so we use a grid mapping. We divide the area of interest into a 100x100 grid in latitude-longitude space, label each cell in this grid with a single integer, and use that integer as the row ID.

We do all of this for each location of interest, one for pickup and one for dropoff. That gives us two fields for two locations: pickup_grid_id, drop_grid_id.

Again, there are many mapping options for location data. For example, we might convert to a different coordinate system, apply a projection, or aggregate locations into real-world regions such as neighborhoods. Here, the simple approach is sufficient.

##### Complex mappings

We also anticipate looking for trends in ride duration and speed, so we want to capture this information during the import process. For the field `duration_minutes`, we compute a row ID as `round((drop_timestamp - pickup_timestamp).minutes)`. For the field `speed_mph`, we compute row ID as `round(dist_miles / (drop_timestamp - pickup_timestamp).minutes)`. These mapping calculations are straightforward, but because they require arithmetic operations on multiple columns, they are a bit too complex to capture in the basic mappers available in PDK. Instead, we define custom mappers to do the work:
```go
durm := pdk.CustomMapper{
    Func: func(fields ...interface{}) interface{} {
        start := fields[0].(time.Time)
        end := fields[1].(time.Time)
        return end.Sub(start).Minutes()
    },
    Mapper: lfm,
}
```

#### Import process

After designing this schema and mapping, we capture it in a JSON definition file that can be read by the PDK import tool. Running `pdk taxi` runs the import based on the information in this file. For more details, see the [PDK](../pdk/) section, or check out the [code](https://github.com/pilosa/pdk/tree/master/usecase/taxi) itself.

#### Queries

Now we can run some example queries.

Count per cab type can be retrieved, sorted, with a single PQL call.

```request
TopN(cab_type)
```
```response
{"results":[[{"id":1,"count":1992943},{"id":0,"count":7057}]]}
```

High traffic location IDs can be retrieved with a similar call. These IDs correspond to latitude, longitude pairs, which can be recovered from the mapping that generates the IDs.

```request
TopN(pickup_grid_id)
```
```response
{"results":[[{"id":5060,"count":40620},{"id":4861,"count":38145},{"id":4962,"count":35268},...]]}
```

Average of `total_amount` per `passenger_count` can be computed with some postprocessing. We use a small number of `TopN` calls to retrieve counts of rides by passenger_count, then use those counts to compute an average.

```python
import pilosa

client = pilosa.Client()
schema = client.schema()
taxi = schema.index("taxi")
passenger_count = taxi.field("passenger_count")
total_amount_dollars = taxi.field("total_amount_dollars")

queries = []
pcounts = range(10)
for i in pcounts:
    queries.append(total_amount_dollars.topn(passenger_count.row(i))
query = taxi.batch_query(**queries)
results = client.query(query)
resp = requests.post(qurl, data=queries)

average_amounts = []
for pcount, result in zip(pcounts, resp.results):
    wsum = sum([r.count * r.id for r in result.count_items])
    count = sum([r.count for r in result.count_items])
    average_amounts.append(float(wsum)/count)
```

<div class="note">
Note that the <a href="../data-model/#bsi-range-encoding">BSI</a>-powered <a href="../query-language/#sum">Sum</a> query now provides an alternative approach to this kind of query.
</div>

<!-- Disabled until we have the time to update the Jupyter notebook --YT
For more examples and details, see this [ipython notebook](https://github.com/pilosa/notebooks/blob/master/taxi-use-case.ipynb).
-->
