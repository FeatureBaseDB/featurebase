# FeatureBase

## Pilosa is now FeatureBase

As of September 7, 2022, the Pilosa project is now FeatureBase. The core of the project remains the same: FeatureBase is the first real-time distributed database built entirely on bitmaps. (More information about updated capabilities and improvements below.)

FeatureBase delivers low-latency query results, regardless of throughput or query volumes, on fresh data with extreme efficiency. It works because bitmaps are faster, simpler, and far more I/O efficient than traditional column-oriented data formats. With FeatureBase, you can ingest data from batch data sources (e.g. S3, CSV, Snowflake, BigQuery, etc.) and/or streaming data sources (e.g. Kafka/Confluent, Kinesis, Pulsar).

For more information about FeatureBase, please visit [www.featurebase.com][HomePage].

## Getting Started

### Build FeatureBase Server from source

0. Install go. Ensure that your shell's search path includes the go/bin directory.
1. Clone the FeatureBase repository (or download as zip).
2. In the featurebase directory, run `make install` to compile the FeatureBase server binary. By default, it will be installed in the go/bin directory.
3. In the idk directory, run `make install` to compile the ingester binaries. By default, they will be installed in the go/bin directory.
4. Run `featurebase server --handler.allowed-origins=http://localhost:3000` to run FeatureBase server with default settings (learn more about configuring FeatureBase at the link below). The `--handler.allowed-origins` parameter allows the standalone web UI to talk to the server; this can be omitted if the web UI is not needed.
5. Run `curl localhost:10101/status` to verify the server is running and accessible.

### Ingest Data and Query

1. Run 
```
molecula-consumer-csv \
    --index repository \
    --header "language__ID_F,project_id__ID_F" \
    --id-field project_id \
    --batch-size 1000 \
    --files example.csv
```

This will ingest the `example.csv` file into a FeatureBase table called `repository`. If the table does not exist, it will be automatically created. Learn more about [ingesting data into FeatureBase][Ingest]

2. Query your data. 
```
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'Row(example=5)'
```
Learn about supported [SQL][SQL], native [Pilosa Query Language (PQL)][PQL].

### Data Model

Because FeatureBase is built on bitmaps, there is bit of a learning curve to grasp how your data is represented. 
[Learn about Data Modeling][DataModel].

### More Information

[Installation][Install]

[Configuration][Config]

## Community

You can email us at community@featurebase.com and [learn more about contributing](https://github.com/FeatureBaseDB/featurebase/blob/master/OPENSOURCE.md).

Chat with us: [https://discord.gg/FBn2vEp7Na][Discord]

## What's Changed Since the Pilosa Days? 

A lot has changed since the days of Pilosa. This list highlights some new capabilities included in FeatureBase. We have also made significant improvements to the performance, scalability, and stability of the FeatureBase product. 

* Query Languages: FeatureBase supports Pilosa Query Language (PQL), as well as SQL
* Stream and Batch Ingest: Combine real-time data streams with batch historical data and act on it within milliseconds.
* Mutable: Perform inserts, updates, and deletes at scale, in real time and on-the-fly. This is key for meeting data compliance requirements, and for reflecting the constantly-changing nature of high-volume data.
* Multi-Valued Set Fields: Store multiple comma-delimited values within a single field while *increasing* query performance of counts, TopKs, etc.
* Time Quantums: Setting a time quantum on a field creates extra views which allow ranged Row queries down to the time interval specified. For example, if the time quantum is set to YMD, ranged Row queries down to the granularity of a day are supported.
* RBF storage backend: this is a new compressed bitmap format which improves performance in a number of ways: ACID support on a per shard basis, prevents issues with the number of open files, reduces memory allocation and lock contention for reads, provides more consistent garbage collection, and allows backups to run concurrently with writes. However, because of this change, Pilosa backup files cannot be restored into FeatureBase.

## License

FeatureBase is licensed under the [Apache License, Version 2.0][License]

[Community]: http://www.featurebase.com/community?utm_campaign=Open%20Source&utm_source=GitHub
[Config]: https://docs.featurebase.com/docs/community/com-config/old-config-flags/?utm_campaign=Open%20Source&utm_source=GitHub
[DataModel]: https://docs.featurebase.com/docs/concepts/overview-data-modeling/?utm_campaign=Open%20Source&utm_source=GitHub
[Discord]: https://discord.gg/FBn2vEp7Na
[HomePage]: http://featurebase.com?utm_campaign=Open%20Source&utm_source=GitHub
[Ingest]: https://docs.featurebase.com/docs/community/com-ingest/old-ingesters/?utm_campaign=Open%20Source&utm_source=GitHub
[Install]: https://docs.featurebase.com/docs/community/com-home/#install-featurebase-community?utm_campaign=Open%20Source&utm_source=GitHub
[License]: http://www.apache.org/licenses/LICENSE-2.0
[PQL]: https://docs.featurebase.com/docs/pql-guide/pql-home/?utm_campaign=Open%20Source&utm_source=GitHub
[SQL]: https://docs.featurebase.com/docs/sql-guide/sql-guide-home/?utm_campaign=Open%20Source&utm_source=GitHub
