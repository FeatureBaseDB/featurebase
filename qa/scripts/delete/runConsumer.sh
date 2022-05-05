#!/bin/bash
set -e
set +x

runConsumer(){
    echo "Started running molecula-consumer-kafka-static on consumer node"
    START_CONSUMER=$(date +%s%N) # in nano seconds

    # start molecula consumer kafka static - read messages from kafka server and ingest into FeatureBase 
    ./molecula-consumer-kafka-static --pilosa-hosts=$DATANODE --kafka-hosts $INGESTNODE0:9092 --index $INDEX --primary-key-fields uuid --header ./schema.json --batch-size $BATCH_SIZE --topics $TOPIC --max-msgs $INGEST_COUNT --concurrency $CONCURRENCY &

    # wait for all background processes to finish
    for job in `jobs -p`
    do
        wait $job 
    done

    END_CONSUMER=$(date +%s%N) # in nano seconds
    DURATION_CONSUMER=$(($(($END_CONSUMER-$START_CONSUMER))/1000000000/60)) # convert from ns to min

    # send metrics to featurebase 
    echo -n "consumer_duration_min:$DURATION_CONSUMER|g|#test_name:$TEST_NAME" | nc -4u -w1 127.0.0.1 8125

    echo "finished consumer - duration: $DURATION_CONSUMER min"   
}


DATANODE=$1
shift

INGESTNODE0=$1
shift 

INGEST_COUNT=$1
shift

BATCH_SIZE=$1
shift

CONCURRENCY=$1
shift

INDEX=$1
shift

TOPIC=$1
shift 

TEST_NAME=$1
shift 

runConsumer