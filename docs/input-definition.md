+++
title = "Input Definition"
weight = 8
nav = [
    "Create the Schema",
    "Import Data",
]
+++

## Input Definition
This document builds on the data import concepts introduced in [Getting Started](../getting-started/).  
Here we will demonstrate creating the index's schema and data definition.  Then using this definition to import JSON data.

### Create the Schema

Input definitions allow users to define a schema based on their data and to provide data to Pilosa in a more standard format like JSON. Once an input definition is created, we can send data to Pilosa as JSON, and as long as the data adheres to the definition, Pilosa will internally perform all of the appropriate mutations.

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
                 },
                 {
                      "actions": [
                          {
                              "frame": "stargazer", 
                              "valueDestination": "set-timestamp"
                          }
                      ], 
                      "name": "time_value
                  }
             ] 
         }'
```

Instead of creating a `stargazer` frame and a `language` frame individually like in [Getting Started](../getting-started/), we can create multiple frames in one input definition.
We can also set `repo_id` for multiple frames at the same time by providing field actions. There are three options for valueDestination:

 - value-to-row: The value for this field is used as the `rowID`.
 - single-row-boolean: The value must be a boolean, and this specifies `SetBit()` or `ClearBit()`, a `rowID` must be specified for this destination type.
 - mapping: The value for this field is used to lookup a `rowID` in a map. A valueMap is required for this destination type.
 - set-timestamp: The value for this field is used to lookup timestamp and set timestamp for the whole frame

### Import Data

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
                 "time_value": "2017-05-18T20:40"
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
        'SetBit(frame="stargazer", repo_id=91720568, stargazer_id=513114, timestamp="2017-05-18T20:40")
         SetBit(frame="language", repo_id=91720568, language_id=5)
         SetBit(frame="language", repo_id=95122322, language_id=17)
     '
```
