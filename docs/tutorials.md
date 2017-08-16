+++
title = "Tutorials"
weight = 4
nav = [
    "Transportation",
    "Chemical similarity search",
]
+++

## Tutorials

### Transportation

#### Introduction

New York City released an extremely detailed data set of over 1 billion taxi rides taken in the city - this data has become a popular target for analysis by tech bloggers and has been very well studied. For this reason, we thought it would be interesting to import this data to Pilosa in order to compare with other data stores and techniques on the exact same data set.

Transportation in general is a compelling use case for Pilosa as it often involves multiple disparate data sources, as well as high rate, real time, and extremely large amounts of data (particularly if one wants to draw reasonable conclusions).

We've written a tool to help import the NYC taxi data into Pilosa - this tool is part of the [PDK](../pdk) (Pilosa Development Kit), and takes advantage of a number of reusable modules that may help you import other data as well. Follow along and we'll explain the whole process step by step.

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

We import these fields, creating one or more Pilosa frames from each of them:

frame	|mapping
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

We also created two extra frames that represent the duration and average speed of each ride:

frame	|mapping
--------------------|-------------
duration_minutes	|round(drop_timestamp - pickup_timestamp) → row ID
speed_mph	|round(dist_miles / (drop_timestamp - pickup_timestamp)) → row ID

#### Mapping

Each column that we want to use must be mapped to a combination of frames and row IDs according to some rule. There are many ways to approach this mapping, and the taxi dataset gives us a good overview of possibilities.

##### 0 columns → 1 frame

cab_type: contains one row for each type of cab. Each column, representing one ride, has a bit set in exactly one row of this frame. The mapping is a simple enumeration, for example yellow=0, green=1, etc. The values of the bits in this frame are determined by the source of the data. That is, we're importing data from several disparate sources: NYC yellow taxi cabs, NYC green taxi cabs, and Uber cars. For each source, the single row to be set in the cab_type frame is constant.

##### 1 column → 1 frame

The following three frames are mapped in a simple direct way from single columns of the original data.

dist_miles: each row represents rides of a certain distance. The mapping is simple: as an example, row 1 represents rides with a distance in the interval [0.5, 1.5]. That is, we round the floating point value of distance to an integer, and use that as the row ID directly. Generally, the mapping from a floating point value to a row ID could be arbitrary. The rounding mapping is concise to implement, which simplifies importing and analysis. As an added bonus, it's human-readable. We'll see this pattern used several times.

In PDK parlance, we define a Mapper, which is simply a function that returns integer row IDs. PDK has a number of predefined mappers that can be described with a few parameters. One of these is LinearFloatMapper, which applies a linear function to the input, and casts it to an integer, so the rounding is handled implicitly. In code:
```go
lfm := pdk.LinearFloatMapper{
    Min: -0.5,
    Max: 3600.5,
    Res: 3601,
}
```

`Min` and `Max` define the linear function, and `Res` determines the maximum allowed value for the output row ID - we chose these values to produce a “round to nearest integer” behavior. Other predefined mappers have their own specific parameters, usually two or three.

This mapper function is the core operation, but we need a few other pieces to define the overall process, which is encapsulated in the BitMapper object. This object defines which field(s) of the input data source to use (`Fields`), how to parse them (`Parsers`), what mapping to use (`Mapper`), and the name of the frame to use (`Frame`).
```go
pdk.BitMapper{
    Frame:   "dist_miles",
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
    "BitMappers": [
        {
            "Frame": "dist_miles",
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

Here, we define a list of Mappers, each including a name, which we use to refer to the mapper later, in the list of BitMappers. We can also do this with Parsers, but a few simple Parsers that need no configuration are available by default. We also have a list of Fields, which is simply a map of field names to column indices. We use these names in the BitMapper definitions to keep things human-readable.

**total_amount_dollars:** Here we use the rounding mapping again, so each row represents rides with a total cost that rounds to the row's ID. The BitMapper definition is very similar to the previous one.

**passenger_count:** This column contains small integers, so we use one of the simplest possible mappings: the column value is the row ID.

##### 1 column → multiple frames

When working with a composite data type like a timestamp, there are plenty of mapping options. In this case, we expect to see interesting periodic trends, so we want to encode the cyclic components of time in a way that allows us to look at them independently during analysis.

We do this by storing time data in four separate frames for each timestamp: one each for the year, month, day, and time of day. The first three are mapped directly. For example, a ride with a date of 2015/06/24 will have a bit set in row 2015 of frame "year", row 6 of frame "month", and row 24 of frame "day". 

We might continue this pattern with hours, minutes, and seconds, but we don't have much use for that level of precision here, so instead we use a "bucketing" approach. That is, we pick a resolution (30 minutes), divide the day into buckets of that size, and create a row for each one. So a ride with a time of 6:45AM has a bit set in row 13 of frame "time_of_day".

We do all of this for each timestamp of interest, one for pickup time and one for dropoff time. That gives us eight total frames for two timestamps: pickup_year, pickup_month, pickup_day, pickup_time, drop_year, drop_month, drop_day, drop_time.

##### Multiple columns → 1 frame

The ride data also contains geolocation data: latitude and longitude for both pickup and dropoff. We just want to be able to produce a rough overview heatmap of ride locations, so we use a grid mapping. We divide the area of interest into a 100x100 grid in latitude-longitude space, label each cell in this grid with a single integer, and use that integer as the row ID.

We do all of this for each location of interest, one for pickup and one for dropoff. That gives us two frames for two locations: pickup_grid_id, drop_grid_id.

Again, there are many mapping options for location data. For example, we might convert to a different coordinate system, apply a projection, or aggregate locations into real-world regions such as neighborhoods. Here, the simple approach is sufficient.

##### Complex mappings

We also anticipate looking for trends in ride duration and speed, so we want to capture this information during the import process. For the frame `duration_minutes`, we compute a row ID as `round((drop_timestamp - pickup_timestamp).minutes)`. For the frame `speed_mph`, we compute row ID as `round(dist_miles / (drop_timestamp - pickup_timestamp).minutes)`. These mapping calculations are straightforward, but because they require arithmetic operations on multiple columns, they are a bit too complex to capture in the basic mappers available in PDK. Instead, we define custom mappers to do the work:
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

After designing this schema and mapping, we capture it in a JSON definition file that can be read by the PDK import tool. Running `pdk taxi` runs the import based on the information in this file. See [PDK](../pdk) for more details on this process.

#### Queries

Now we can run some example queries.

Count per cab type can be retrieved, sorted, with a single PQL call.

```
TopN(frame=cab_type)
```

High traffic location IDs can be retrieved with a similar call. These IDs correspond to latitude, longitude pairs, which can be recovered from the mapping that generates the IDs.

```
TopN(frame=pickup_grid_id)
```

Average of total_amount per passenger_count can be computed with some postprocessing. We use a small number of `TopN` calls to retrieve counts of rides by passenger_count, then use those counts to compute an average.

```python
queries = ''
pcounts = range(10)
for i in pcounts:
    queries += "TopN(Bitmap(id=%d, frame='passenger_count'), frame=total_amount_dollars)" % i
resp = requests.post(qurl, data=queries)

average_amounts = []
for pcount, topn in zip(pcounts, resp.json()['results']):
    wsum = sum([r['count'] * r['key'] for r in topn])
    count = sum([r['count'] for r in topn])
    average_amounts.append(float(wsum)/count)
```

For more examples and details, see this [ipython notebook](https://github.com/alanbernstein/pilosa-notebooks/blob/master/taxi-use-case.ipynb).

### Chemical similarity search

#### Overview

The notion of chemical similarity (or molecular similarity) plays an important role in predicting the properties of chemical compounds, designing chemicals with a predefined set of properties, and—especially—conducting drug design studies. All of these are accomplished by screening large indexes containing structures of available or potentially available chemicals.

We'd like to use Pilosa to search through millions of molecules and find those most similar to a given molecule. There are examples where --- tried to solve this chemical similarity search problem using other indexes (MongoDB, PostgreSQL), so it will be interesting to compare those results to Pilosa using the same data set.

Calculation of the similarity of any two molecules is achieved by comparing their molecular fingerprints. These fingerprints are comprised of structural information about the molecule which has been encoded as a series of bits. The most commonly used algorithm to calculate the similarity is the Tanimoto coefficient.
```
T(A,B)= Intersect(A,B) / (Count(A) + Count(B) - Intersect(A,B))
```

A and B are sets of fingerprint bits on in the fingerprints of molecule A and molecule B. AB is the set of common bits of fingerprints of both molecule A and B. The Tanimoto coefficient ranges from 0 when the fingerprints have no bits in common, to 1 when the fingerprints are identical.

All source code to calculate tanimoto for molecule fingerprint using Pilosa is available in a Github repository https://github.com/pilosa/chem-usecase

#### Data model

We use the latest ChEMBL release chembl_22.sdf for test data. Each molecule in the SD file gives us the canonical isomeric SMILES (Simplified molecular-input line-entry system) and chembl_id. 

Because Pilosa store information as a series of bits, we use RDKit in Python to convert molecules from their SMILES encoding to Morgan fingerprints, which are arrays of “on” bit positions.

Given a SMILES encoded molecule and a similarity threshold, we want to retrieve all molecule ids (or SMILES) that have a similarity percentage greater than or equal to the similarity threshold. For example, given a molecule with:
```
SMILES = "IC=C1/CCC(C(=O)O1)c2cccc3ccccc23"
threshold = 90
```

return the set of molecules that have at least a 90% similarity with the given molecule.

The Inverse view swaps the rows and columns automatically to enable queries over either the chembl_id or fingerprint.

Standard View is used to calculate similarity
```
Index: mole
    View: Standard
        Col: chembl_id
            Frame: fingerprint
                Row: position_id ("on" bit positions of a fingerprint)
```

Inverse View is used for finding chembl_id based on given SMILES. 
From a given SMILES, we use RDKit to convert it to fingerprints with "on" bit position. From "on" bit positions,  we can search a list of chembl_ids that match the bit positions. To choose the right chembl_id, we need another query to Standard View then choose the right chembl_id which has the length that matches the given fingerprint's length after using RDKit to convert SMILES to fingerprint.
```
Index: mole
    View: Inverse
        Col: position_id ("on" bit positions of a fingerprint)
            Frame: fingerprint
                Row: chembl_id
```

After retrieving chembl_id from the Inverse View, we can use the Tanimoto coefficient to compare chembl_id with the entire data set of molecules. The result of this comparison is the list of `chembl_id`s that have a Tanimoto coefficient greater than the given threshold.

#### Import process

To import data into Pilosa, we need to get chembl_id and SMILES from SD files, convert SMILES to Morgan fingerprints, and then write chembl_id and fingerprint to Pilosa. The fastest way is to extracted chembl_id and SMILES from SD file to csv file, then use the `pilosa import` command to import the csv file into Pilosa. Since chembl_id in the SD file is always paired with CHEMBL, e.g CHEMBL6329, and because Pilosa doesn't support string keys, we will ignore CHEMBL and instead use chembl_id as an integer key.

For the `mole` index, each row in the csv file has the format 'chembl_id, position_id' by running the following command from Chem-usecase:
```
python import_from_sdf.py -p <path_to_sdf_file> -file id_fingerprint.csv
```


First, follow the instruction in the [getting started]({{< ref "getting-started.md" >}}) guide to run a Pilosa server. Then create the indexes and frames according to the schemas outlined in the Data Model section above.
The option cacheSize should be set as amount of chembl_id to calculate effectively for the whole data set, so we need to calculate amount of chembl_id. We have total 1678393 chembl_id (it will displayed after import_from_sdf.py script running), then the cacheSize should be >= 1678393
```
curl localhost:10101/index/mole \
     -X POST \
     -d '{"options": {"columnLabel": "position_id"}}'

curl localhost:10101/index/mole/frame/fingerprint \
     -X POST \
     -d '{"options": {"rowLabel": "chembl_id", "inverseEnabled": true, "cacheSize": 2000000, "cacheType": "ranked"}}'

```

Run the following commands to import the csv data into the `mole` index:
```
pilosa import -d mole -f fingerprint id_fingerprint.csv
```

#### Queries

Get chembl_id from a given SMILES:
```
python get_mol_fr_smile.py -s "I\C=C/1\CCC(C(=O)O1)c2cccc3ccccc23"
```

Return chembl_id = 6223. This script uses Pilosa’s Intersection query to get all chemlb_id that have positions are on, which following these steps:

* Convert SMILES to fingerprint bit "on" positions

    ```python
    from rdkit import Chem
    from rdkit.Chem import AllChem
    mol=Chem.MolFromSmiles("I\C=C/1\CCC(C(=O)O1)c2cccc3ccccc23")
    fp = list(AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=4096).GetOnBits())
    ```
        
* Query all chembl_id that have all "on" positions from the inverse view, return list of chembl_id

    ```python
    bit_maps = ["Bitmap(position_id=%s, frame=%s, inversed=%s)" % (f, frame, True) for f in fp]
    bitmap_string = ', '.join(bit_maps)
    intersection = "Intersect(%s)" % bitmap_string
    mole_ids = requests.post("http://%s/index/%s/query" % (host, db), data=intersection).json()["results"][0]["bits"]
    ```

* From list of chembl_id, query all "on" position from mol index, if the length of array of "on" position is matched to len(fp) then return that chembl_id, otherwise the given SMILES does not exist.

    ```python    
    for m in mole_ids:
        mol = requests.post("http://%s/index/%s/query" % (host, db), data="Bitmap(chembl_id=%s, frame=%s)" % (m, frame)).json()["results"][0]["bits"]
        existed_mol = False
        if len(mol) == len(fp):
            found = m
            existed_mol = True
            break
    ```

Retrieve molecule_ids that have similarity with SMILES="I\C=C/1\CCC(C(=O)O1)c2cccc3ccccc23" and similarity threshold = 70%
```
python similar.py -s "I\C=C/1\CCC(C(=O)O1)c2cccc3ccccc23" -t 70
```

Return chembl_id = [6223, 269758, 6206, 6228]. This script uses Pilosa’s TopN query to get all chemlb_id that have position is on, which following these steps:

* Get chembl_id from a SMILES (steps discussed above)

* Query Pilosa’s TopN to get list of similarity chembl_id
    ```python
    query_string = 'TopN(Bitmap(chembl_id=6223, frame="fingerprint"), frame="fingerprint", n=2000000, tanimotoThreshold=70)'
    topn = requests.post("http://127.0.0.1:10101/index/mol/query" , data=query_string)
    ```

#### Benchmark

To run benchmark for specific chembl_id for different similarity threshold at percentage of [50, 70, 75, 80, 85, 90], run following command:
```
python benchmarks.py -id 6223
```

As Matt Swain’s blog post also did a great job using mongoDB for chemical similarity search, we compared benchmark on 500000 molecules between mongoDB aggregation framework with Pilosa.

Both using the same molecule, Morgan fingerprint folded to fixed lengths of 4096 bits and were run on a MacBook Pro with a 2.8 GHz 2-core Intel Core i7 processor, memory of 16 GB 1600 MHz DDR3, single host cluster
